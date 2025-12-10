//! Retry logic with exponential backoff
//!
//! Provides robust error recovery for transient failures such as:
//! - Network timeouts
//! - Delta Lake OCC (Optimistic Concurrency Control) conflicts
//! - Temporary resource unavailability
//! - Rate limiting
//!
//! # Features
//!
//! - Exponential backoff with jitter
//! - Configurable max retries and delays
//! - Retry only on retryable errors
//! - Circuit breaker pattern support (future)
//!
//! # Example
//!
//! ```rust,ignore
//! use featureduck_core::retry::{RetryPolicy, retry_async};
//!
//! let policy = RetryPolicy::default();
//! let result = retry_async(&policy, || async {
//!     // Your fallible operation
//!     my_delta_write().await
//! }).await?;
//! ```

use std::time::Duration;
use tracing::{debug, warn};

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: usize,

    /// Initial backoff delay
    pub initial_delay: Duration,

    /// Maximum backoff delay (caps exponential growth)
    pub max_delay: Duration,

    /// Multiplier for exponential backoff (typically 2.0)
    pub backoff_multiplier: f64,

    /// Add random jitter to prevent thundering herd (0.0-1.0)
    pub jitter_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl RetryPolicy {
    /// Aggressive retry policy for critical operations
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.2,
        }
    }

    /// Conservative retry policy for non-critical operations
    pub fn conservative() -> Self {
        Self {
            max_retries: 2,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }

    /// Calculate backoff delay for a given attempt
    pub fn backoff_delay(&self, attempt: usize) -> Duration {
        let base_delay =
            self.initial_delay.as_millis() as f64 * self.backoff_multiplier.powi(attempt as i32);

        let capped_delay = base_delay.min(self.max_delay.as_millis() as f64);

        // Add jitter: random value between (1 - jitter) and (1 + jitter)
        let jitter = 1.0 + (rand::random::<f64>() * 2.0 - 1.0) * self.jitter_factor;
        let final_delay = (capped_delay * jitter) as u64;

        Duration::from_millis(final_delay)
    }
}

/// Retry error - wraps the original error with retry metadata
#[derive(Debug)]
pub struct RetryError<E> {
    pub error: E,
    pub attempts: usize,
    pub total_delay: Duration,
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Operation failed after {} attempts (total delay: {:?}): {}",
            self.attempts, self.total_delay, self.error
        )
    }
}

impl<E: std::error::Error> std::error::Error for RetryError<E> {}

/// Determine if an error is retryable
///
/// Override this function to customize retry logic for specific error types.
pub fn is_retryable(error: &crate::Error) -> bool {
    match error {
        // Storage errors (network, I/O) are retryable
        crate::Error::StorageError(e) => {
            let error_str = e.to_string().to_lowercase();
            // Network and transient errors are retryable
            error_str.contains("network")
                || error_str.contains("connection")
                || error_str.contains("timeout")
                || error_str.contains("temporary")
                || error_str.contains("retry")
                || error_str.contains("unavailable")
        }

        // OCC conflicts in Delta Lake are retryable
        crate::Error::InvalidInput(msg) if msg.contains("conflict") => true,
        crate::Error::InvalidInput(msg) if msg.contains("concurrent") => true,
        crate::Error::InvalidInput(msg) if msg.contains("version mismatch") => true,
        crate::Error::InvalidInput(msg) if msg.contains("optimistic") => true,

        // Timeout errors are retryable
        crate::Error::InvalidInput(msg) if msg.contains("timeout") => true,
        crate::Error::InvalidInput(msg) if msg.contains("timed out") => true,

        // Resource temporarily unavailable
        crate::Error::InvalidInput(msg) if msg.contains("busy") => true,
        crate::Error::InvalidInput(msg) if msg.contains("locked") => true,

        // Everything else is not retryable
        _ => false,
    }
}

/// Retry an async operation with exponential backoff
///
/// # Arguments
///
/// * `policy` - Retry policy configuration
/// * `operation` - Async function to retry
///
/// # Returns
///
/// `Ok(T)` on success, `Err(RetryError<E>)` after exhausting retries
///
/// # Example
///
/// ```rust,ignore
/// let result = retry_async(&RetryPolicy::default(), || async {
///     my_fallible_operation().await
/// }).await?;
/// ```
pub async fn retry_async<F, Fut, T, E>(
    policy: &RetryPolicy,
    mut operation: F,
) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempts = 0;
    let mut total_delay = Duration::from_secs(0);

    loop {
        attempts += 1;

        match operation().await {
            Ok(result) => {
                if attempts > 1 {
                    debug!(
                        "Operation succeeded after {} attempts (total delay: {:?})",
                        attempts, total_delay
                    );
                }
                return Ok(result);
            }
            Err(error) => {
                if attempts > policy.max_retries {
                    warn!(
                        "Operation failed after {} attempts (total delay: {:?}): {}",
                        attempts, total_delay, error
                    );
                    return Err(RetryError {
                        error,
                        attempts,
                        total_delay,
                    });
                }

                let delay = policy.backoff_delay(attempts - 1);
                total_delay += delay;

                debug!(
                    "Operation failed (attempt {}/{}), retrying after {:?}: {}",
                    attempts,
                    policy.max_retries + 1,
                    delay,
                    error
                );

                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));
    }

    #[test]
    fn test_retry_policy_aggressive() {
        let policy = RetryPolicy::aggressive();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.initial_delay, Duration::from_millis(50));
    }

    #[test]
    fn test_backoff_delay_exponential() {
        let policy = RetryPolicy {
            max_retries: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0, // No jitter for predictable testing
        };

        let delay0 = policy.backoff_delay(0);
        let delay1 = policy.backoff_delay(1);
        let delay2 = policy.backoff_delay(2);

        // With multiplier 2.0: 100ms, 200ms, 400ms
        assert_eq!(delay0, Duration::from_millis(100));
        assert_eq!(delay1, Duration::from_millis(200));
        assert_eq!(delay2, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_delay_capped() {
        let policy = RetryPolicy {
            max_retries: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };

        let delay10 = policy.backoff_delay(10);
        // Should be capped at max_delay
        assert_eq!(delay10, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_retry_async_succeeds_first_try() {
        let policy = RetryPolicy::default();
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_async(&policy, || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let mut count = counter.lock().unwrap();
                *count += 1;
                Ok::<i32, String>(42)
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(*counter.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_retry_async_succeeds_after_retries() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_async(&policy, || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let mut count = counter.lock().unwrap();
                *count += 1;
                if *count < 3 {
                    Err("Temporary failure".to_string())
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(*counter.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_retry_async_exhausts_retries() {
        let policy = RetryPolicy {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let result = retry_async(&policy, || {
            let counter = Arc::clone(&counter_clone);
            async move {
                let mut count = counter.lock().unwrap();
                *count += 1;
                Err::<i32, String>("Permanent failure".to_string())
            }
        })
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.attempts, 3); // max_retries + 1
        assert_eq!(*counter.lock().unwrap(), 3);
    }

    #[test]
    fn test_is_retryable() {
        // Storage errors with network keywords are retryable
        assert!(is_retryable(&crate::Error::StorageError(anyhow::anyhow!(
            "Network timeout"
        ))));
        assert!(is_retryable(&crate::Error::StorageError(anyhow::anyhow!(
            "Connection refused"
        ))));
        assert!(is_retryable(&crate::Error::StorageError(anyhow::anyhow!(
            "Temporary failure"
        ))));

        // OCC conflict errors are retryable
        assert!(is_retryable(&crate::Error::InvalidInput(
            "OCC conflict detected".to_string()
        )));
        assert!(is_retryable(&crate::Error::InvalidInput(
            "version mismatch in Delta Lake".to_string()
        )));
        assert!(is_retryable(&crate::Error::InvalidInput(
            "optimistic concurrency violation".to_string()
        )));

        // Timeout errors are retryable
        assert!(is_retryable(&crate::Error::InvalidInput(
            "Operation timeout".to_string()
        )));
        assert!(is_retryable(&crate::Error::InvalidInput(
            "Request timed out".to_string()
        )));

        // Resource busy errors are retryable
        assert!(is_retryable(&crate::Error::InvalidInput(
            "Database is busy".to_string()
        )));
        assert!(is_retryable(&crate::Error::InvalidInput(
            "Table is locked".to_string()
        )));

        // Validation errors are NOT retryable
        assert!(!is_retryable(&crate::Error::InvalidInput(
            "Invalid column name".to_string()
        )));

        // Not implemented is NOT retryable
        assert!(!is_retryable(&crate::Error::NotImplemented(
            "Feature not available".to_string()
        )));

        // Storage errors without retry keywords are NOT retryable
        assert!(!is_retryable(&crate::Error::StorageError(anyhow::anyhow!(
            "Permanent disk failure"
        ))));
    }
}
