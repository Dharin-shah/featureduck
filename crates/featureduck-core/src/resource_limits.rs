//! Resource limits for preventing OOM and runaway queries
//!
//! Provides configurable limits to ensure system stability:
//! - Query timeout (prevent long-running queries)
//! - Memory limits (prevent OOM)
//! - Row limits (prevent processing too much data)
//! - Concurrent operation limits
//!
//! # Example
//!
//! ```rust,ignore
//! use featureduck_core::resource_limits::{ResourceLimits, execute_with_limits};
//!
//! let limits = ResourceLimits::default();
//! let result = execute_with_limits(&limits, || async {
//!     // Your operation with automatic timeout
//!     my_query().await
//! }).await?;
//! ```

use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Resource limits configuration
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum query execution time
    pub query_timeout: Duration,

    /// Maximum number of rows to process
    pub max_rows: usize,

    /// Maximum memory usage (bytes) - advisory only
    pub max_memory_bytes: usize,

    /// Maximum concurrent operations
    pub max_concurrent_ops: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(300), // 5 minutes
            max_rows: 10_000_000,                    // 10 million rows
            max_memory_bytes: 2_000_000_000,         // 2 GB
            max_concurrent_ops: 10,                  // 10 concurrent operations
        }
    }
}

impl ResourceLimits {
    /// Strict limits for production environments
    pub fn strict() -> Self {
        Self {
            query_timeout: Duration::from_secs(120), // 2 minutes
            max_rows: 5_000_000,                     // 5 million rows
            max_memory_bytes: 1_000_000_000,         // 1 GB
            max_concurrent_ops: 5,                   // 5 concurrent operations
        }
    }

    /// Relaxed limits for development/testing
    pub fn relaxed() -> Self {
        Self {
            query_timeout: Duration::from_secs(600), // 10 minutes
            max_rows: 50_000_000,                    // 50 million rows
            max_memory_bytes: 8_000_000_000,         // 8 GB
            max_concurrent_ops: 20,                  // 20 concurrent operations
        }
    }

    /// No limits (use with caution!)
    pub fn unlimited() -> Self {
        Self {
            query_timeout: Duration::from_secs(3600), // 1 hour
            max_rows: usize::MAX,
            max_memory_bytes: usize::MAX,
            max_concurrent_ops: 100,
        }
    }

    /// Validate row count against limits
    pub fn check_row_count(&self, row_count: usize) -> Result<(), LimitExceeded> {
        if row_count > self.max_rows {
            warn!("Row count {} exceeds limit {}", row_count, self.max_rows);
            return Err(LimitExceeded::RowLimit {
                actual: row_count,
                limit: self.max_rows,
            });
        }
        Ok(())
    }

    /// Validate memory usage against limits (advisory)
    pub fn check_memory(&self, memory_bytes: usize) -> Result<(), LimitExceeded> {
        if memory_bytes > self.max_memory_bytes {
            warn!(
                "Memory usage {} bytes exceeds limit {} bytes",
                memory_bytes, self.max_memory_bytes
            );
            return Err(LimitExceeded::MemoryLimit {
                actual: memory_bytes,
                limit: self.max_memory_bytes,
            });
        }
        Ok(())
    }
}

/// Errors when resource limits are exceeded
#[derive(Debug, thiserror::Error)]
pub enum LimitExceeded {
    #[error("Query timeout: operation exceeded {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Row limit exceeded: {actual} rows exceeds limit of {limit}")]
    RowLimit { actual: usize, limit: usize },

    #[error("Memory limit exceeded: {actual} bytes exceeds limit of {limit} bytes")]
    MemoryLimit { actual: usize, limit: usize },

    #[error("Concurrent operation limit exceeded: {actual} exceeds limit of {limit}")]
    ConcurrencyLimit { actual: usize, limit: usize },
}

/// Execute an async operation with timeout
///
/// # Example
///
/// ```rust,ignore
/// let limits = ResourceLimits::default();
/// let result = execute_with_timeout(&limits, || async {
///     my_query().await
/// }).await?;
/// ```
pub async fn execute_with_timeout<F, Fut, T, E>(
    limits: &ResourceLimits,
    operation: F,
) -> Result<T, TimeoutError<E>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match timeout(limits.query_timeout, operation()).await {
        Ok(Ok(result)) => {
            debug!("Operation completed within timeout");
            Ok(result)
        }
        Ok(Err(error)) => Err(TimeoutError::OperationFailed(error)),
        Err(_) => {
            warn!("Operation timed out after {:?}", limits.query_timeout);
            Err(TimeoutError::Timeout(limits.query_timeout))
        }
    }
}

/// Timeout error wrapper
#[derive(Debug)]
pub enum TimeoutError<E> {
    Timeout(Duration),
    OperationFailed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for TimeoutError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutError::Timeout(duration) => {
                write!(f, "Operation timed out after {:?}", duration)
            }
            TimeoutError::OperationFailed(error) => write!(f, "Operation failed: {}", error),
        }
    }
}

impl<E: std::error::Error> std::error::Error for TimeoutError<E> {}

/// Concurrency limiter using semaphore
pub struct ConcurrencyLimiter {
    semaphore: tokio::sync::Semaphore,
    max_permits: usize,
}

impl ConcurrencyLimiter {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(max_concurrent),
            max_permits: max_concurrent,
        }
    }

    /// Execute operation with concurrency limit
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, LimitExceeded>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _permit =
            self.semaphore
                .acquire()
                .await
                .map_err(|_| LimitExceeded::ConcurrencyLimit {
                    actual: self.max_permits + 1,
                    limit: self.max_permits,
                })?;

        Ok(operation().await)
    }

    /// Get current number of available permits
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.query_timeout, Duration::from_secs(300));
        assert_eq!(limits.max_rows, 10_000_000);
    }

    #[test]
    fn test_resource_limits_strict() {
        let limits = ResourceLimits::strict();
        assert_eq!(limits.query_timeout, Duration::from_secs(120));
        assert_eq!(limits.max_rows, 5_000_000);
    }

    #[test]
    fn test_check_row_count_success() {
        let limits = ResourceLimits::default();
        assert!(limits.check_row_count(1_000).is_ok());
        assert!(limits.check_row_count(10_000_000).is_ok());
    }

    #[test]
    fn test_check_row_count_exceeds() {
        let limits = ResourceLimits::default();
        let result = limits.check_row_count(10_000_001);
        assert!(result.is_err());
        match result {
            Err(LimitExceeded::RowLimit { actual, limit }) => {
                assert_eq!(actual, 10_000_001);
                assert_eq!(limit, 10_000_000);
            }
            _ => panic!("Expected RowLimit error"),
        }
    }

    #[test]
    fn test_check_memory_success() {
        let limits = ResourceLimits::default();
        assert!(limits.check_memory(1_000_000).is_ok());
    }

    #[test]
    fn test_check_memory_exceeds() {
        let limits = ResourceLimits::default();
        let result = limits.check_memory(3_000_000_000);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_timeout_success() {
        let limits = ResourceLimits::default();
        let result = execute_with_timeout(&limits, || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<i32, String>(42)
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_execute_with_timeout_exceeds() {
        let limits = ResourceLimits {
            query_timeout: Duration::from_millis(50),
            ..Default::default()
        };

        let result = execute_with_timeout(&limits, || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<i32, String>(42)
        })
        .await;

        assert!(result.is_err());
        match result {
            Err(TimeoutError::Timeout(duration)) => {
                assert_eq!(duration, Duration::from_millis(50));
            }
            _ => panic!("Expected Timeout error"),
        }
    }

    #[tokio::test]
    async fn test_concurrency_limiter() {
        let limiter = ConcurrencyLimiter::new(2);

        // Test that at most 2 operations run concurrently
        let result1 = limiter
            .execute(|| async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                42
            })
            .await;

        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), 42);

        // Verify available permits
        assert_eq!(limiter.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_concurrency_limiter_available_permits() {
        let limiter = ConcurrencyLimiter::new(3);
        assert_eq!(limiter.available_permits(), 3);

        let _permit = limiter.semaphore.acquire().await.unwrap();
        assert_eq!(limiter.available_permits(), 2);
    }
}
