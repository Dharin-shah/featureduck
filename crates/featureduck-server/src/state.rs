//! Application state shared across all HTTP handlers
//!
//! This module defines the `AppState` struct that holds all the shared
//! resources needed by the HTTP handlers (database connections, configuration, etc.).
//!
//! ## Design Pattern: Dependency Injection
//!
//! Rather than using global variables, we pass state explicitly to each handler.
//! This makes the code:
//! - Easier to test (can inject mock state)
//! - Easier to reason about (explicit dependencies)
//! - Thread-safe (Axum requires state to be Clone + Send + Sync)

use std::sync::Arc;

/// Shared application state
///
/// This struct contains everything that HTTP handlers need to access.
/// It's wrapped in an Arc (atomic reference counter) so it can be cloned
/// cheaply and shared across threads.
///
/// ## Milestone 0
/// For now this is just a placeholder. In future milestones we'll add:
/// - DuckDB connection pool
/// - Feature registry
/// - Storage connector
/// - Configuration
///
/// ## Thread Safety
/// All fields must be thread-safe (Send + Sync) because Axum runs handlers
/// on a thread pool. We use Arc for shared ownership and interior mutability
/// patterns (Mutex, RwLock) only when necessary.
#[derive(Clone)]
pub struct AppState {
    /// Inner state wrapped in Arc for cheap cloning
    inner: Arc<AppStateInner>,
}

/// The actual state fields
///
/// This is separate from AppState so we can wrap it in Arc once
/// and clone the Arc instead of cloning all fields.
struct AppStateInner {
    // TODO Milestone 1: Add DuckDB connection pool
    // pool: DuckDBPool,
    
    // TODO Milestone 1: Add feature registry
    // registry: Arc<FeatureRegistry>,
    
    // TODO Milestone 1: Add storage connector
    // connector: Arc<dyn StorageConnector>,
    
    /// Server start time (for uptime reporting)
    start_time: std::time::Instant,
}

impl AppState {
    /// Creates a new AppState with default values
    ///
    /// For Milestone 0, this just initializes the start time.
    /// Future milestones will take configuration and create real connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = AppState::new();
    /// ```
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                start_time: std::time::Instant::now(),
            }),
        }
    }

    /// Returns the server uptime in seconds
    ///
    /// This is useful for health checks and monitoring.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let uptime = state.uptime();
    /// println!("Server has been running for {} seconds", uptime);
    /// ```
    pub fn uptime(&self) -> u64 {
        self.inner.start_time.elapsed().as_secs()
    }
}

// Implement Default trait for convenience
impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_creation() {
        let state = AppState::new();
        // Uptime should be very small (we just created it)
        assert!(state.uptime() < 1);
    }

    #[test]
    fn test_state_is_cloneable() {
        let state1 = AppState::new();
        let state2 = state1.clone();
        
        // Both should report same uptime (they share the same Arc)
        assert_eq!(state1.uptime(), state2.uptime());
    }

    #[tokio::test]
    async fn test_state_uptime_increases() {
        let state = AppState::new();
        let uptime1 = state.uptime();
        
        // Wait a bit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        let uptime2 = state.uptime();
        // Uptime should have increased (even if just by 0 seconds due to rounding)
        assert!(uptime2 >= uptime1);
    }
}
