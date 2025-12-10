//! Observability module for metrics and tracing
//!
//! This module provides production-grade observability through:
//! - **Prometheus metrics** - Latency histograms, counters, gauges
//! - **Structured logging** - JSON logs with context
//! - **Distributed tracing** - Request tracking across services
//!
//! ## Metrics Exposed:
//! - `featureduck_write_duration_seconds` - Feature write latency histogram
//! - `featureduck_write_rows_total` - Total rows written counter
//! - `featureduck_write_errors_total` - Write errors counter
//! - `featureduck_read_duration_seconds` - Feature read latency histogram
//! - `featureduck_read_rows_total` - Total rows read counter
//! - `featureduck_read_errors_total` - Read errors counter
//! - `featureduck_materialization_duration_seconds` - Materialization latency
//! - `featureduck_duckdb_query_duration_seconds` - DuckDB query execution time
//!
//! ## Usage:
//! ```rust,no_run
//! use featureduck_delta::observability::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Record write latency
//! let _timer = time_write_operation();
//! // ... perform write operation ...
//! drop(_timer); // Automatically records duration
//!
//! // Count rows written
//! let row_count = 1000;
//! increment_write_rows(row_count);
//! # Ok(())
//! # }
//! ```

use metrics::{counter, gauge, histogram};
use std::time::Instant;

/// Initialize metrics subsystem
///
/// This should be called once at application startup.
/// Note: With the metrics crate, metrics are registered lazily on first use,
/// so this is just a placeholder for future initialization logic.
pub fn init_metrics() {
    // Metrics will be registered on first use
    // No explicit initialization needed for metrics crate
}

// ==================== Write Metrics ====================

/// Timer for write operations (auto-records on drop)
pub struct WriteTimer {
    start: Instant,
}

impl Default for WriteTimer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl WriteTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Drop for WriteTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!("featureduck_write_duration_seconds", duration);
    }
}

/// Start timing a write operation
pub fn time_write_operation() -> WriteTimer {
    WriteTimer::new()
}

/// Increment total rows written
pub fn increment_write_rows(count: usize) {
    counter!("featureduck_write_rows_total", count as u64);
}

/// Increment write errors
pub fn increment_write_errors() {
    counter!("featureduck_write_errors_total", 1);
}

// ==================== Read Metrics ====================

/// Timer for read operations (auto-records on drop)
pub struct ReadTimer {
    start: Instant,
}

impl Default for ReadTimer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl ReadTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Drop for ReadTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!("featureduck_read_duration_seconds", duration);
    }
}

/// Start timing a read operation
pub fn time_read_operation() -> ReadTimer {
    ReadTimer::new()
}

/// Increment total rows read
pub fn increment_read_rows(count: usize) {
    counter!("featureduck_read_rows_total", count as u64);
}

/// Increment read errors
pub fn increment_read_errors() {
    counter!("featureduck_read_errors_total", 1);
}

// ==================== Materialization Metrics ====================

/// Timer for materialization operations (auto-records on drop)
pub struct MaterializationTimer {
    start: Instant,
}

impl Default for MaterializationTimer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl MaterializationTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Drop for MaterializationTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!("featureduck_materialization_duration_seconds", duration);
    }
}

/// Start timing a materialization operation
pub fn time_materialization(_feature_view: &str) -> MaterializationTimer {
    // Note: Label support will be added when we upgrade metrics crate
    MaterializationTimer::new()
}

// ==================== DuckDB Query Metrics ====================

/// Timer for DuckDB query operations (auto-records on drop)
pub struct QueryTimer {
    start: Instant,
}

impl Default for QueryTimer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl QueryTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!("featureduck_duckdb_query_duration_seconds", duration);
    }
}

/// Start timing a DuckDB query
pub fn time_query() -> QueryTimer {
    QueryTimer::new()
}

// ==================== Connection Pool Metrics ====================

/// Update active connections gauge
pub fn set_active_connections(count: usize) {
    gauge!("featureduck_active_connections", count as f64);
}

/// Update idle connections gauge
pub fn set_idle_connections(count: usize) {
    gauge!("featureduck_idle_connections", count as f64);
}

// ==================== Resource Metrics ====================

/// Update memory usage gauge (in bytes)
pub fn set_memory_usage(bytes: usize) {
    gauge!("featureduck_memory_bytes", bytes as f64);
}

// ==================== Connection Pool Metrics ====================

/// Timer for connection pool wait time (auto-records on drop)
pub struct PoolWaitTimer {
    start: Instant,
}

impl Default for PoolWaitTimer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl PoolWaitTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Drop for PoolWaitTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        histogram!("featureduck_pool_wait_duration_seconds", duration);
    }
}

/// Start timing connection pool wait
pub fn time_pool_wait() -> PoolWaitTimer {
    PoolWaitTimer::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_write_timer() {
        let _timer = time_write_operation();
        thread::sleep(Duration::from_millis(10));
        drop(_timer);
        // Timer automatically records on drop
    }

    #[test]
    fn test_increment_counters() {
        increment_write_rows(100);
        increment_write_errors();
        increment_read_rows(50);
        increment_read_errors();
    }

    #[test]
    fn test_materialization_timer() {
        let _timer = time_materialization("user_features");
        thread::sleep(Duration::from_millis(10));
        drop(_timer);
    }

    #[test]
    fn test_set_gauges() {
        set_active_connections(5);
        set_idle_connections(10);
        set_memory_usage(1024 * 1024 * 100); // 100MB
    }

    #[test]
    fn test_pool_wait_timer() {
        let _timer = time_pool_wait();
        thread::sleep(Duration::from_millis(5));
        drop(_timer);
        // Timer automatically records on drop
    }
}
