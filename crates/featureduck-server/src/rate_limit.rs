//! Rate limiting middleware for FeatureDuck server
//!
//! This module provides rate limiting to protect the server from abuse
//! and ensure fair resource usage.
//!
//! ## Rate Limiting Strategies
//!
//! 1. **Per-IP**: Limits requests from each IP address
//! 2. **Per-API-Key**: Limits requests per authenticated API key
//! 3. **Global**: Overall server request limit
//!
//! ## Configuration
//!
//! Rate limits are configured via:
//! - Environment variables: `FEATUREDUCK_RATE_LIMIT_*`
//! - Config file: `rate_limit` section in YAML
//!
//! ## Example
//!
//! ```bash
//! # Set rate limit to 100 requests per second
//! export FEATUREDUCK_RATE_LIMIT_RPS=100
//! export FEATUREDUCK_RATE_LIMIT_BURST=20
//! ```

use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per second
    pub requests_per_second: u32,
    /// Burst size (allows temporary spikes)
    pub burst_size: u32,
    /// Whether rate limiting is enabled
    pub enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 1000, // 1000 RPS default
            burst_size: 100,           // Allow bursts of 100
            enabled: true,
        }
    }
}

impl RateLimitConfig {
    /// Create a new rate limit config
    #[allow(dead_code)] // Public API for programmatic configuration
    pub fn new(requests_per_second: u32, burst_size: u32) -> Self {
        Self {
            requests_per_second,
            burst_size,
            enabled: true,
        }
    }

    /// Create from environment variables
    ///
    /// Reads:
    /// - `FEATUREDUCK_RATE_LIMIT_RPS`: Requests per second (default: 1000)
    /// - `FEATUREDUCK_RATE_LIMIT_BURST`: Burst size (default: 100)
    /// - `FEATUREDUCK_RATE_LIMIT_ENABLED`: Enable/disable (default: true)
    pub fn from_env() -> Self {
        let requests_per_second = std::env::var("FEATUREDUCK_RATE_LIMIT_RPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let burst_size = std::env::var("FEATUREDUCK_RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let enabled = std::env::var("FEATUREDUCK_RATE_LIMIT_ENABLED")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true);

        Self {
            requests_per_second,
            burst_size,
            enabled,
        }
    }

    /// Create a disabled rate limit config
    #[allow(dead_code)] // Public API for testing and special deployments
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Rate limiter type alias for the global rate limiter
pub type GlobalRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Create a rate limiter from config
pub fn create_rate_limiter(config: &RateLimitConfig) -> Arc<GlobalRateLimiter> {
    let quota = Quota::per_second(
        NonZeroU32::new(config.requests_per_second).unwrap_or(NonZeroU32::new(1000).unwrap()),
    )
    .allow_burst(NonZeroU32::new(config.burst_size).unwrap_or(NonZeroU32::new(100).unwrap()));

    Arc::new(RateLimiter::direct(quota))
}

/// Rate limit state for the middleware
#[derive(Clone)]
pub struct RateLimitState {
    /// The rate limiter
    pub limiter: Arc<GlobalRateLimiter>,
    /// Configuration
    pub config: RateLimitConfig,
}

impl RateLimitState {
    /// Create a new rate limit state
    pub fn new(config: RateLimitConfig) -> Self {
        let limiter = create_rate_limiter(&config);
        Self { limiter, config }
    }

    /// Create from environment
    pub fn from_env() -> Self {
        let config = RateLimitConfig::from_env();
        Self::new(config)
    }

    /// Check if rate limit allows the request
    pub fn check(&self) -> Result<(), RateLimitError> {
        if !self.config.enabled {
            return Ok(());
        }

        match self.limiter.check() {
            Ok(_) => Ok(()),
            Err(_) => Err(RateLimitError {
                retry_after_secs: 1, // Approximate
            }),
        }
    }
}

/// Rate limit error
#[derive(Debug)]
pub struct RateLimitError {
    /// Seconds until the client can retry
    pub retry_after_secs: u64,
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rate limit exceeded. Retry after {} seconds.",
            self.retry_after_secs
        )
    }
}

impl std::error::Error for RateLimitError {}

/// Axum middleware for rate limiting
///
/// ## Usage
///
/// ```rust,ignore
/// use axum::{Router, middleware};
/// use featureduck_server::rate_limit::{rate_limit_middleware, RateLimitState};
///
/// let state = RateLimitState::from_env();
/// let app = Router::new()
///     .route("/v1/features/online", post(handler))
///     .layer(middleware::from_fn_with_state(
///         state,
///         rate_limit_middleware
///     ));
/// ```
pub async fn rate_limit_middleware(
    axum::extract::State(state): axum::extract::State<RateLimitState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, RateLimitResponse> {
    // Check rate limit
    if let Err(e) = state.check() {
        tracing::warn!(
            path = %request.uri().path(),
            "Rate limit exceeded"
        );
        return Err(RateLimitResponse {
            retry_after_secs: e.retry_after_secs,
        });
    }

    Ok(next.run(request).await)
}

/// Rate limit response (429 Too Many Requests)
pub struct RateLimitResponse {
    pub retry_after_secs: u64,
}

impl axum::response::IntoResponse for RateLimitResponse {
    fn into_response(self) -> axum::response::Response {
        use axum::http::{header, StatusCode};

        let body = serde_json::json!({
            "code": "RATE_LIMITED",
            "message": format!("Rate limit exceeded. Retry after {} seconds.", self.retry_after_secs),
            "retry_after_secs": self.retry_after_secs
        });

        (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, self.retry_after_secs.to_string())],
            axum::Json(body),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert_eq!(config.requests_per_second, 1000);
        assert_eq!(config.burst_size, 100);
        assert!(config.enabled);
    }

    #[test]
    fn test_rate_limit_config_disabled() {
        let config = RateLimitConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_rate_limit_state_allows_requests() {
        let config = RateLimitConfig::new(100, 10);
        let state = RateLimitState::new(config);

        // Should allow multiple requests within limit
        for _ in 0..10 {
            assert!(state.check().is_ok());
        }
    }

    #[test]
    fn test_rate_limit_state_blocks_excess() {
        let config = RateLimitConfig::new(1, 1); // Very low limit: 1 RPS with burst of 1
        let state = RateLimitState::new(config);

        // First request should succeed (consumes the 1 token)
        assert!(state.check().is_ok());

        // Eventually should be blocked after burst is exhausted
        // With 1 RPS and burst of 1, we can make at most 1 request per second
        // The burst replenishes at 1 per second, so rapid requests will be blocked
        let mut blocked = false;
        for _ in 0..10 {
            if state.check().is_err() {
                blocked = true;
                break;
            }
        }
        assert!(blocked, "Rate limiter should block excess requests");
    }

    #[test]
    fn test_rate_limit_disabled_allows_all() {
        let config = RateLimitConfig::disabled();
        let state = RateLimitState::new(config);

        // Should allow unlimited requests when disabled
        for _ in 0..1000 {
            assert!(state.check().is_ok());
        }
    }
}
