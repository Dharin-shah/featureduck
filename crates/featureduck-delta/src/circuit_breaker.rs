//! Circuit Breaker for Delta Storage Operations
//!
//! This module provides a circuit breaker pattern to prevent cascading failures
//! when Delta Lake storage becomes unavailable or degraded.
//!
//! ## States
//!
//! The circuit breaker has three states:
//! - **Closed**: Normal operation, requests flow through
//! - **Open**: Circuit is tripped, requests fail fast without attempting storage
//! - **HalfOpen**: Testing if storage is recovered (allows limited requests)
//!
//! ## Configuration
//!
//! - `failure_threshold`: Number of failures before opening circuit (default: 5)
//! - `success_threshold`: Successes in HalfOpen to close circuit (default: 3)
//! - `timeout`: Duration circuit stays open before testing (default: 30s)
//!
//! ## Example
//!
//! ```rust,ignore
//! use featureduck_delta::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
//!
//! let config = CircuitBreakerConfig::default();
//! let breaker = CircuitBreaker::new("delta_storage", config);
//!
//! // Before making a storage call
//! if !breaker.allow_request() {
//!     return Err(Error::CircuitOpen("Storage unavailable"));
//! }
//!
//! // After the call
//! match result {
//!     Ok(_) => breaker.record_success(),
//!     Err(_) => breaker.record_failure(),
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests flow through
    Closed,
    /// Circuit tripped - requests fail fast
    Open,
    /// Testing recovery - limited requests allowed
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "CLOSED"),
            CircuitState::Open => write!(f, "OPEN"),
            CircuitState::HalfOpen => write!(f, "HALF_OPEN"),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    pub failure_threshold: u32,

    /// Number of successes in HalfOpen state to close circuit
    pub success_threshold: u32,

    /// How long circuit stays open before transitioning to HalfOpen
    pub timeout: Duration,

    /// Maximum requests allowed in HalfOpen state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a strict configuration for critical operations
    pub fn strict() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 5,
            timeout: Duration::from_secs(60),
            half_open_max_requests: 1,
        }
    }

    /// Create a lenient configuration for resilient operations
    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 2,
            timeout: Duration::from_secs(15),
            half_open_max_requests: 5,
        }
    }
}

/// Internal state tracking
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    half_open_requests: u32,
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            half_open_requests: 0,
        }
    }
}

/// Circuit breaker for protecting storage operations
///
/// Thread-safe implementation using atomic operations and RwLock
pub struct CircuitBreaker {
    /// Name of the protected resource (for logging)
    name: String,

    /// Configuration
    config: CircuitBreakerConfig,

    /// Internal state (protected by RwLock for thread-safety)
    state: RwLock<CircuitBreakerState>,

    /// Total request count (for metrics)
    total_requests: AtomicU64,

    /// Total failures (for metrics)
    total_failures: AtomicU64,

    /// Times circuit was opened (for metrics)
    times_opened: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(name: &str, config: CircuitBreakerConfig) -> Self {
        info!(
            "Creating circuit breaker '{}' with config: failure_threshold={}, success_threshold={}, timeout={}s",
            name, config.failure_threshold, config.success_threshold, config.timeout.as_secs()
        );

        Self {
            name: name.to_string(),
            config,
            state: RwLock::new(CircuitBreakerState::default()),
            total_requests: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            times_opened: AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(name: &str) -> Self {
        Self::new(name, CircuitBreakerConfig::default())
    }

    /// Check if a request should be allowed
    ///
    /// Returns true if the request can proceed, false if it should fail fast
    pub fn allow_request(&self) -> bool {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        let mut state = self.state.write().unwrap();

        match state.state {
            CircuitState::Closed => {
                debug!("Circuit '{}' is CLOSED, allowing request", self.name);
                true
            }
            CircuitState::Open => {
                // Check if timeout has passed
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= self.config.timeout {
                        info!(
                            "Circuit '{}' timeout elapsed, transitioning to HALF_OPEN",
                            self.name
                        );
                        state.state = CircuitState::HalfOpen;
                        state.half_open_requests = 0;
                        state.success_count = 0;
                        // Allow this request as the first HalfOpen request
                        state.half_open_requests += 1;
                        return true;
                    }
                }
                debug!(
                    "Circuit '{}' is OPEN, rejecting request (timeout remaining: {:?})",
                    self.name,
                    state
                        .last_failure_time
                        .map(|t| self.config.timeout.saturating_sub(t.elapsed()))
                );
                false
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in HalfOpen state
                if state.half_open_requests < self.config.half_open_max_requests {
                    state.half_open_requests += 1;
                    debug!(
                        "Circuit '{}' is HALF_OPEN, allowing request ({}/{})",
                        self.name, state.half_open_requests, self.config.half_open_max_requests
                    );
                    true
                } else {
                    debug!(
                        "Circuit '{}' is HALF_OPEN but max requests reached, rejecting",
                        self.name
                    );
                    false
                }
            }
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let mut state = self.state.write().unwrap();

        match state.state {
            CircuitState::Closed => {
                // Reset failure count on success
                if state.failure_count > 0 {
                    debug!(
                        "Circuit '{}' success, resetting failure count from {}",
                        self.name, state.failure_count
                    );
                    state.failure_count = 0;
                }
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                debug!(
                    "Circuit '{}' HalfOpen success ({}/{})",
                    self.name, state.success_count, self.config.success_threshold
                );

                // Check if we can close the circuit
                if state.success_count >= self.config.success_threshold {
                    info!(
                        "Circuit '{}' recovered, transitioning from HALF_OPEN to CLOSED",
                        self.name
                    );
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.last_failure_time = None;
                    state.half_open_requests = 0;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
                debug!(
                    "Circuit '{}' received success while OPEN (unexpected)",
                    self.name
                );
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);

        let mut state = self.state.write().unwrap();

        match state.state {
            CircuitState::Closed => {
                state.failure_count += 1;
                debug!(
                    "Circuit '{}' failure ({}/{})",
                    self.name, state.failure_count, self.config.failure_threshold
                );

                // Check if we should open the circuit
                if state.failure_count >= self.config.failure_threshold {
                    warn!(
                        "Circuit '{}' failure threshold reached, transitioning from CLOSED to OPEN",
                        self.name
                    );
                    state.state = CircuitState::Open;
                    state.last_failure_time = Some(Instant::now());
                    self.times_opened.fetch_add(1, Ordering::Relaxed);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in HalfOpen immediately reopens the circuit
                warn!(
                    "Circuit '{}' failure in HALF_OPEN state, reopening circuit",
                    self.name
                );
                state.state = CircuitState::Open;
                state.last_failure_time = Some(Instant::now());
                state.success_count = 0;
                state.half_open_requests = 0;
                self.times_opened.fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::Open => {
                // Update last failure time
                state.last_failure_time = Some(Instant::now());
                debug!("Circuit '{}' failure while already OPEN", self.name);
            }
        }
    }

    /// Get current circuit state
    pub fn state(&self) -> CircuitState {
        self.state.read().unwrap().state
    }

    /// Get circuit statistics
    pub fn stats(&self) -> CircuitStats {
        let state = self.state.read().unwrap();
        CircuitStats {
            name: self.name.clone(),
            state: state.state,
            failure_count: state.failure_count,
            success_count: state.success_count,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            times_opened: self.times_opened.load(Ordering::Relaxed),
        }
    }

    /// Reset the circuit breaker to closed state
    ///
    /// Use with caution - typically for testing or manual recovery
    pub fn reset(&self) {
        info!("Circuit '{}' manually reset to CLOSED", self.name);
        let mut state = self.state.write().unwrap();
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.last_failure_time = None;
        state.half_open_requests = 0;
    }
}

/// Statistics about circuit breaker state
#[derive(Debug, Clone)]
pub struct CircuitStats {
    pub name: String,
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub total_requests: u64,
    pub total_failures: u64,
    pub times_opened: u64,
}

impl std::fmt::Display for CircuitStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Circuit '{}': state={}, failures={}/{}, total_requests={}, times_opened={}",
            self.name,
            self.state,
            self.failure_count,
            self.total_failures,
            self.total_requests,
            self.times_opened
        )
    }
}

/// Error type for circuit breaker operations
#[derive(Debug, Clone)]
pub struct CircuitOpenError {
    pub circuit_name: String,
    pub message: String,
}

impl std::fmt::Display for CircuitOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Circuit '{}' is open: {}",
            self.circuit_name, self.message
        )
    }
}

impl std::error::Error for CircuitOpenError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let breaker = CircuitBreaker::with_defaults("test");
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request());
    }

    #[test]
    fn test_circuit_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_secs(30),
            half_open_max_requests: 1,
        };
        let breaker = CircuitBreaker::new("test", config);

        // Record failures
        for _ in 0..3 {
            assert!(breaker.allow_request());
            breaker.record_failure();
        }

        // Circuit should now be open
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request()); // Should reject
    }

    #[test]
    fn test_circuit_transitions_to_half_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 1,
            timeout: Duration::from_millis(50), // Short timeout for testing
            half_open_max_requests: 1,
        };
        let breaker = CircuitBreaker::new("test", config);

        // Trip the circuit
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        assert_eq!(breaker.state(), CircuitState::Open);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        // Should transition to HalfOpen
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_closes_after_successes_in_half_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(50),
            half_open_max_requests: 5,
        };
        let breaker = CircuitBreaker::new("test", config);

        // Trip the circuit
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        // Transition to HalfOpen and record successes
        assert!(breaker.allow_request());
        breaker.record_success();
        assert!(breaker.allow_request());
        breaker.record_success();

        // Should be closed now
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_reopens_on_half_open_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(50),
            half_open_max_requests: 5,
        };
        let breaker = CircuitBreaker::new("test", config);

        // Trip the circuit
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        // Transition to HalfOpen
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Record failure - should reopen
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new("test", config);

        // Record 2 failures (not enough to trip)
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        // Record a success - should reset failure count
        breaker.allow_request();
        breaker.record_success();

        // Now record 2 more failures - still not enough
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        // Circuit should still be closed
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_max_requests_limit() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 5,
            timeout: Duration::from_millis(50),
            half_open_max_requests: 2,
        };
        let breaker = CircuitBreaker::new("test", config);

        // Trip the circuit
        breaker.allow_request();
        breaker.record_failure();

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        // First two requests should be allowed in HalfOpen
        assert!(breaker.allow_request()); // 1
        assert!(breaker.allow_request()); // 2

        // Third request should be rejected
        assert!(!breaker.allow_request());
    }

    #[test]
    fn test_stats_tracking() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new("test", config);

        // Make some requests
        for _ in 0..5 {
            breaker.allow_request();
            breaker.record_success();
        }

        for _ in 0..3 {
            breaker.allow_request();
            breaker.record_failure();
        }

        let stats = breaker.stats();
        assert_eq!(stats.total_requests, 8);
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.times_opened, 1);
    }

    #[test]
    fn test_reset_clears_state() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new("test", config);

        // Trip the circuit
        breaker.allow_request();
        breaker.record_failure();
        breaker.allow_request();
        breaker.record_failure();

        assert_eq!(breaker.state(), CircuitState::Open);

        // Reset
        breaker.reset();

        // Should be closed and allow requests
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let breaker = Arc::new(CircuitBreaker::with_defaults("concurrent_test"));
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent operations
        for _ in 0..10 {
            let breaker_clone = Arc::clone(&breaker);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    if breaker_clone.allow_request() {
                        if rand_bool() {
                            breaker_clone.record_success();
                        } else {
                            breaker_clone.record_failure();
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should not panic and stats should be consistent
        let stats = breaker.stats();
        assert!(stats.total_requests > 0);
    }

    // Helper function for random bool in test
    fn rand_bool() -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        nanos.is_multiple_of(2)
    }
}
