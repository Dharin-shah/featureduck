//! Prometheus metrics for FeatureDuck server
//!
//! Tracks:
//! - Request latency (P50, P95, P99)
//! - Request counts (success/error)
//! - Feature read/write operations
//! - Cache hit rates
//! - Active connections

use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge, Encoder, HistogramVec,
    IntCounterVec, IntGauge, TextEncoder,
};

lazy_static! {
    /// HTTP request latency histogram
    pub static ref HTTP_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_http_request_duration_seconds",
        "HTTP request latency in seconds",
        &["method", "endpoint", "status"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .unwrap();

    /// HTTP request counter
    pub static ref HTTP_REQUEST_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_http_requests_total",
        "Total HTTP requests",
        &["method", "endpoint", "status"]
    )
    .unwrap();

    /// Feature read operations
    pub static ref FEATURE_READS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_feature_reads_total",
        "Total feature read operations",
        &["feature_view", "status"]
    )
    .unwrap();

    /// Feature write operations
    pub static ref FEATURE_WRITES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_feature_writes_total",
        "Total feature write operations",
        &["feature_view", "status"]
    )
    .unwrap();

    /// Feature read latency
    pub static ref FEATURE_READ_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_feature_read_duration_seconds",
        "Feature read latency in seconds",
        &["feature_view"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap();

    /// Feature write latency
    pub static ref FEATURE_WRITE_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_feature_write_duration_seconds",
        "Feature write latency in seconds",
        &["feature_view"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .unwrap();

    /// Rows processed counter
    pub static ref ROWS_PROCESSED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_rows_processed_total",
        "Total rows processed",
        &["feature_view", "operation"]
    )
    .unwrap();

    /// Active connections gauge
    pub static ref ACTIVE_CONNECTIONS: IntGauge = register_int_gauge!(
        "featureduck_active_connections",
        "Number of active HTTP connections"
    )
    .unwrap();

    /// Registry operations
    pub static ref REGISTRY_OPERATIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_registry_operations_total",
        "Total registry operations",
        &["operation", "status"]
    )
    .unwrap();

    /// Cache hit/miss counter
    pub static ref CACHE_OPERATIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_cache_operations_total",
        "Cache operations (hit/miss)",
        &["cache_type", "result"]
    )
    .unwrap();

    /// Error counter by type
    pub static ref ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_errors_total",
        "Total errors by type",
        &["error_type", "component"]
    )
    .unwrap();

    /// Feature materialization counter
    pub static ref FEATURE_MATERIALIZATIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_feature_materializations_total",
        "Total feature materializations",
        &["feature_view", "status"]
    )
    .unwrap();

    /// Feature materialization duration histogram
    pub static ref FEATURE_MATERIALIZATION_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_feature_materialization_duration_seconds",
        "Feature materialization duration in seconds",
        &["feature_view"],
        vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    )
    .unwrap();

    /// Feature materialization rows processed histogram
    pub static ref FEATURE_MATERIALIZATION_ROWS: HistogramVec = register_histogram_vec!(
        "featureduck_feature_materialization_rows_processed",
        "Rows processed during materialization",
        &["feature_view"],
        vec![10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0]
    )
    .unwrap();

    /// Delta table size gauge
    pub static ref DELTA_TABLE_SIZE_BYTES: IntGauge = register_int_gauge!(
        "featureduck_delta_table_size_bytes",
        "Size of Delta Lake tables in bytes"
    )
    .unwrap();

    /// Registry query duration histogram
    pub static ref REGISTRY_QUERY_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_registry_query_duration_seconds",
        "Registry query duration in seconds",
        &["operation"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap();

    /// DuckDB query duration histogram
    pub static ref DUCKDB_QUERY_DURATION: HistogramVec = register_histogram_vec!(
        "featureduck_duckdb_query_duration_seconds",
        "DuckDB query duration in seconds",
        &["feature_view"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    )
    .unwrap();

    /// Retry attempts counter
    pub static ref RETRY_ATTEMPTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "featureduck_retry_attempts_total",
        "Total retry attempts by operation and outcome",
        &["operation", "outcome"]
    )
    .unwrap();

    /// Retry delay histogram
    pub static ref RETRY_DELAY_SECONDS: HistogramVec = register_histogram_vec!(
        "featureduck_retry_delay_seconds",
        "Retry backoff delay in seconds",
        &["operation"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    )
    .unwrap();

    /// Resource limit violations counter
    pub static ref RESOURCE_LIMIT_VIOLATIONS: IntCounterVec = register_int_counter_vec!(
        "featureduck_resource_limit_violations_total",
        "Total resource limit violations",
        &["limit_type", "operation"]
    )
    .unwrap();

    /// Operation duration with timeout tracking
    pub static ref OPERATION_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "featureduck_operation_duration_seconds",
        "Operation duration in seconds (includes timeout violations)",
        &["operation", "status"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
    )
    .unwrap();
}

/// Export metrics in Prometheus text format
pub fn export_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

/// Middleware timer for tracking request duration
#[allow(dead_code)] // Public API for request timing in middleware
pub struct RequestTimer {
    start: std::time::Instant,
    method: String,
    endpoint: String,
}

impl RequestTimer {
    #[allow(dead_code)] // Public API for request timing
    pub fn new(method: String, endpoint: String) -> Self {
        ACTIVE_CONNECTIONS.inc();
        Self {
            start: std::time::Instant::now(),
            method,
            endpoint,
        }
    }

    #[allow(dead_code)] // Public API for request timing
    pub fn observe(self, status: u16) {
        let duration = self.start.elapsed().as_secs_f64();
        let status_str = status.to_string();

        HTTP_REQUEST_DURATION
            .with_label_values(&[&self.method, &self.endpoint, &status_str])
            .observe(duration);

        HTTP_REQUEST_TOTAL
            .with_label_values(&[&self.method, &self.endpoint, &status_str])
            .inc();

        ACTIVE_CONNECTIONS.dec();
    }
}

/// Helper function to record materialization metrics
#[allow(dead_code)] // Public API for orchestration layer
pub fn record_materialization(
    feature_view: &str,
    duration_seconds: f64,
    rows_processed: usize,
    success: bool,
) {
    let status = if success { "success" } else { "failed" };

    FEATURE_MATERIALIZATIONS_TOTAL
        .with_label_values(&[feature_view, status])
        .inc();

    FEATURE_MATERIALIZATION_DURATION
        .with_label_values(&[feature_view])
        .observe(duration_seconds);

    FEATURE_MATERIALIZATION_ROWS
        .with_label_values(&[feature_view])
        .observe(rows_processed as f64);
}

/// Helper function to record registry query metrics
#[allow(dead_code)] // Public API for registry instrumentation
pub fn record_registry_query(operation: &str, duration_seconds: f64) {
    REGISTRY_QUERY_DURATION
        .with_label_values(&[operation])
        .observe(duration_seconds);
}

/// Helper function to record DuckDB query metrics
#[allow(dead_code)] // Public API for DuckDB instrumentation
pub fn record_duckdb_query(feature_view: &str, duration_seconds: f64) {
    DUCKDB_QUERY_DURATION
        .with_label_values(&[feature_view])
        .observe(duration_seconds);
}

/// Helper function to update Delta table size
#[allow(dead_code)] // Public API for storage monitoring
pub fn update_delta_table_size(size_bytes: i64) {
    DELTA_TABLE_SIZE_BYTES.set(size_bytes);
}

/// Helper function to record retry attempt
#[allow(dead_code)] // Public API for retry instrumentation
pub fn record_retry_attempt(operation: &str, outcome: &str, delay_seconds: f64) {
    RETRY_ATTEMPTS_TOTAL
        .with_label_values(&[operation, outcome])
        .inc();

    RETRY_DELAY_SECONDS
        .with_label_values(&[operation])
        .observe(delay_seconds);
}

/// Helper function to record resource limit violation
#[allow(dead_code)] // Public API for resource limit monitoring
pub fn record_limit_violation(limit_type: &str, operation: &str) {
    RESOURCE_LIMIT_VIOLATIONS
        .with_label_values(&[limit_type, operation])
        .inc();
}

/// Helper function to record operation duration with status
#[allow(dead_code)] // Public API for operation timing
pub fn record_operation_duration(operation: &str, duration_seconds: f64, success: bool) {
    let status = if success { "success" } else { "timeout" };
    OPERATION_DURATION_SECONDS
        .with_label_values(&[operation, status])
        .observe(duration_seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_export() {
        HTTP_REQUEST_TOTAL
            .with_label_values(&["GET", "/health", "200"])
            .inc();

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_http_requests_total"));
        assert!(metrics.contains("GET"));
    }

    #[test]
    fn test_request_timer() {
        let timer = RequestTimer::new("POST".to_string(), "/api/features".to_string());
        std::thread::sleep(std::time::Duration::from_millis(10));
        timer.observe(200);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_http_request_duration_seconds"));
    }

    #[test]
    fn test_feature_metrics() {
        FEATURE_READS_TOTAL
            .with_label_values(&["user_features", "success"])
            .inc();

        FEATURE_WRITES_TOTAL
            .with_label_values(&["user_features", "success"])
            .inc();

        ROWS_PROCESSED_TOTAL
            .with_label_values(&["user_features", "read"])
            .inc_by(100);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_feature_reads_total"));
        assert!(metrics.contains("featureduck_feature_writes_total"));
        assert!(metrics.contains("featureduck_rows_processed_total"));
    }

    #[test]
    fn test_materialization_metrics() {
        record_materialization("user_features", 1.5, 1000, true);
        record_materialization("product_features", 2.0, 5000, false);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_feature_materializations_total"));
        assert!(metrics.contains("featureduck_feature_materialization_duration_seconds"));
        assert!(metrics.contains("featureduck_feature_materialization_rows_processed"));
        assert!(metrics.contains("user_features"));
        assert!(metrics.contains("product_features"));
    }

    #[test]
    fn test_registry_metrics() {
        record_registry_query("get_feature_view", 0.01);
        record_registry_query("list_feature_views", 0.05);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_registry_query_duration_seconds"));
        assert!(metrics.contains("get_feature_view"));
        assert!(metrics.contains("list_feature_views"));
    }

    #[test]
    fn test_duckdb_metrics() {
        record_duckdb_query("user_features", 0.5);
        record_duckdb_query("product_features", 1.2);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_duckdb_query_duration_seconds"));
        assert!(metrics.contains("user_features"));
        assert!(metrics.contains("product_features"));
    }

    #[test]
    fn test_delta_table_size() {
        update_delta_table_size(1024 * 1024 * 100); // 100MB

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_delta_table_size_bytes"));
    }

    #[test]
    fn test_retry_metrics() {
        record_retry_attempt("write_features", "retryable_error", 0.1);
        record_retry_attempt("write_features", "success_after_retry", 0.2);
        record_retry_attempt("write_features", "retryable_error", 0.4);
        record_retry_attempt("write_features", "success_after_retry", 0.8);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_retry_attempts_total"));
        assert!(metrics.contains("featureduck_retry_delay_seconds"));
        assert!(metrics.contains("write_features"));
    }

    #[test]
    fn test_resource_limit_metrics() {
        record_limit_violation("timeout", "materialize");
        record_limit_violation("memory", "query");
        record_limit_violation("rows", "materialize");

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_resource_limit_violations_total"));
        assert!(metrics.contains("timeout"));
        assert!(metrics.contains("memory"));
        assert!(metrics.contains("rows"));
    }

    #[test]
    fn test_operation_duration_metrics() {
        record_operation_duration("materialize", 45.5, true);
        record_operation_duration("materialize", 120.0, false);
        record_operation_duration("query", 2.5, true);

        let metrics = export_metrics().expect("Failed to export metrics");
        assert!(metrics.contains("featureduck_operation_duration_seconds"));
        assert!(metrics.contains("success"));
        assert!(metrics.contains("timeout"));
    }
}
