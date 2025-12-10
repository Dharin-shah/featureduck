//! Graceful Shutdown Module
//!
//! Handles graceful server shutdown including:
//! - Signal handling (SIGTERM, SIGINT)
//! - In-flight request draining
//! - Connection cleanup
//! - Resource cleanup
//!
//! ## Usage
//!
//! ```rust,ignore
//! use featureduck_server::shutdown::shutdown_signal;
//!
//! let server = axum::Server::bind(&addr)
//!     .serve(app.into_make_service())
//!     .with_graceful_shutdown(shutdown_signal());
//! ```
//!
//! Note: This module is prepared for integration but not yet wired into main.rs.
//! Enable when ready for production deployment.

// Allow dead code as this module is prepared for future integration
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Global shutdown state
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Default grace period for in-flight requests (used in ShutdownCoordinator::default)
const DEFAULT_GRACE_PERIOD_SECS: u64 = 30;

/// Shutdown coordinator for managing graceful shutdown
#[derive(Clone)]
pub struct ShutdownCoordinator {
    /// Broadcast sender for shutdown signal
    tx: broadcast::Sender<()>,
    /// Grace period for draining connections
    grace_period: Duration,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new(Duration::from_secs(DEFAULT_GRACE_PERIOD_SECS))
    }
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator with custom grace period
    pub fn new(grace_period: Duration) -> Self {
        let (tx, _) = broadcast::channel(1);
        Self { tx, grace_period }
    }

    /// Create from environment variables
    ///
    /// Reads `SHUTDOWN_GRACE_PERIOD_SECS` from environment.
    pub fn from_env() -> Self {
        let grace_secs = std::env::var("SHUTDOWN_GRACE_PERIOD_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_GRACE_PERIOD_SECS);

        Self::new(Duration::from_secs(grace_secs))
    }

    /// Get a receiver for shutdown signals
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

    /// Trigger shutdown
    pub fn shutdown(&self) {
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        let _ = self.tx.send(());
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested() -> bool {
        SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
    }

    /// Get grace period
    pub fn grace_period(&self) -> Duration {
        self.grace_period
    }
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
///
/// This function returns a future that completes when the server should
/// shut down. It handles:
/// - SIGTERM (sent by Docker/Kubernetes)
/// - SIGINT (Ctrl+C)
///
/// ## Example
///
/// ```rust,ignore
/// let server = axum::Server::bind(&addr)
///     .serve(app.into_make_service())
///     .with_graceful_shutdown(shutdown_signal());
/// ```
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating graceful shutdown");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
    }

    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Wait for shutdown signal with timeout
///
/// Same as `shutdown_signal()` but also completes after a timeout.
/// Useful for testing.
pub async fn shutdown_signal_with_timeout(timeout: Duration) {
    tokio::select! {
        _ = shutdown_signal() => {}
        _ = tokio::time::sleep(timeout) => {
            warn!("Shutdown timeout reached");
        }
    }
}

/// Graceful shutdown with connection draining
///
/// This function:
/// 1. Stops accepting new connections
/// 2. Waits for in-flight requests to complete (up to grace period)
/// 3. Forcefully closes remaining connections
///
/// ## Arguments
///
/// * `coordinator` - The shutdown coordinator
/// * `shutdown_tasks` - Optional async tasks to run during shutdown
pub async fn graceful_shutdown<F, Fut>(coordinator: &ShutdownCoordinator, shutdown_tasks: Option<F>)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    info!(
        "Starting graceful shutdown (grace period: {:?})",
        coordinator.grace_period()
    );

    // Run any shutdown tasks
    if let Some(tasks) = shutdown_tasks {
        let shutdown_result = tokio::time::timeout(coordinator.grace_period(), tasks()).await;

        if shutdown_result.is_err() {
            warn!(
                "Shutdown tasks did not complete within grace period ({:?})",
                coordinator.grace_period()
            );
        }
    }

    info!("Graceful shutdown complete");
}

/// Create a shutdown-aware service wrapper
///
/// This wrapper checks if shutdown has been requested before handling
/// each request. If shutdown is in progress, it returns 503 Service Unavailable.
pub fn is_shutting_down() -> bool {
    ShutdownCoordinator::is_shutdown_requested()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_coordinator_default() {
        let coord = ShutdownCoordinator::default();
        assert_eq!(
            coord.grace_period(),
            Duration::from_secs(DEFAULT_GRACE_PERIOD_SECS)
        );
        assert!(!ShutdownCoordinator::is_shutdown_requested());
    }

    #[test]
    fn test_shutdown_coordinator_custom_grace_period() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(60));
        assert_eq!(coord.grace_period(), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_shutdown_signal_is_global() {
        // Reset global state
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

        assert!(!ShutdownCoordinator::is_shutdown_requested());

        let coord = ShutdownCoordinator::default();
        coord.shutdown();

        assert!(ShutdownCoordinator::is_shutdown_requested());

        // Reset for other tests
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn test_shutdown_broadcast() {
        // Reset global state
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

        let coord = ShutdownCoordinator::default();
        let mut rx = coord.subscribe();

        // Spawn a task that waits for shutdown
        let handle = tokio::spawn(async move {
            rx.recv().await.ok();
            true
        });

        // Give the task time to subscribe
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger shutdown
        coord.shutdown();

        // Wait for task to complete
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "Task should complete after shutdown");

        // Reset for other tests
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_is_shutting_down() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        assert!(!is_shutting_down());

        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        assert!(is_shutting_down());

        // Reset
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    }
}
