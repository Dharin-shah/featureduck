//! Integration tests for Prometheus metrics endpoint

use axum::http::StatusCode;
use featureduck_server::{api, metrics};

#[tokio::test]
async fn test_metrics_endpoint_success() {
    // Given: Record some metrics
    metrics::HTTP_REQUEST_TOTAL
        .with_label_values(&["GET", "/health", "200"])
        .inc();

    metrics::FEATURE_READS_TOTAL
        .with_label_values(&["user_features", "success"])
        .inc();

    // When: Request metrics endpoint
    let response = api::metrics().await;

    // Then: Should return 200 OK with Prometheus format
    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    // Verify Prometheus format
    assert!(body_str.contains("# HELP"));
    assert!(body_str.contains("# TYPE"));
    assert!(body_str.contains("featureduck_http_requests_total"));
    assert!(body_str.contains("featureduck_feature_reads_total"));
}

#[tokio::test]
async fn test_metrics_contain_all_expected_metrics() {
    // Given: Record at least one value for each metric type
    metrics::HTTP_REQUEST_DURATION
        .with_label_values(&["GET", "/test", "200"])
        .observe(0.1);
    metrics::HTTP_REQUEST_TOTAL
        .with_label_values(&["GET", "/test", "200"])
        .inc();
    metrics::FEATURE_READS_TOTAL
        .with_label_values(&["test", "success"])
        .inc();
    metrics::FEATURE_WRITES_TOTAL
        .with_label_values(&["test", "success"])
        .inc();
    metrics::FEATURE_READ_DURATION
        .with_label_values(&["test"])
        .observe(0.01);
    metrics::FEATURE_WRITE_DURATION
        .with_label_values(&["test"])
        .observe(0.1);
    metrics::ROWS_PROCESSED_TOTAL
        .with_label_values(&["test", "read"])
        .inc();
    metrics::ACTIVE_CONNECTIONS.set(5);
    metrics::REGISTRY_OPERATIONS_TOTAL
        .with_label_values(&["get", "success"])
        .inc();
    metrics::CACHE_OPERATIONS_TOTAL
        .with_label_values(&["query", "hit"])
        .inc();
    metrics::ERRORS_TOTAL
        .with_label_values(&["test_error", "test_component"])
        .inc();

    // When: Request metrics
    let response = api::metrics().await;
    let (_, body) = response.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    // Then: Should contain all expected metric types
    let expected_metrics = vec![
        "featureduck_http_request_duration_seconds",
        "featureduck_http_requests_total",
        "featureduck_feature_reads_total",
        "featureduck_feature_writes_total",
        "featureduck_feature_read_duration_seconds",
        "featureduck_feature_write_duration_seconds",
        "featureduck_rows_processed_total",
        "featureduck_active_connections",
        "featureduck_registry_operations_total",
        "featureduck_cache_operations_total",
        "featureduck_errors_total",
    ];

    for metric in expected_metrics {
        assert!(body_str.contains(metric), "Missing metric: {}", metric);
    }
}

#[tokio::test]
async fn test_request_timer() {
    // Given: Create a request timer
    let timer = metrics::RequestTimer::new("POST".to_string(), "/api/test".to_string());

    // Then: Active connections should increase
    let metrics_before = metrics::export_metrics().unwrap();
    assert!(metrics_before.contains("featureduck_active_connections"));

    // When: Timer is dropped (observe called)
    timer.observe(200);

    // Then: Metrics should be recorded
    let metrics_after = metrics::export_metrics().unwrap();
    assert!(metrics_after.contains("featureduck_http_request_duration_seconds"));
    assert!(metrics_after.contains("featureduck_http_requests_total"));
}

#[tokio::test]
async fn test_feature_operation_metrics() {
    // Given: Record feature operations
    metrics::FEATURE_READS_TOTAL
        .with_label_values(&["test_features", "success"])
        .inc();

    metrics::FEATURE_WRITES_TOTAL
        .with_label_values(&["test_features", "success"])
        .inc();

    metrics::ROWS_PROCESSED_TOTAL
        .with_label_values(&["test_features", "read"])
        .inc_by(1000);

    metrics::FEATURE_READ_DURATION
        .with_label_values(&["test_features"])
        .observe(0.025);

    // When: Export metrics
    let metrics_str = metrics::export_metrics().unwrap();

    // Then: All metrics should be present
    assert!(metrics_str.contains("featureduck_feature_reads_total"));
    assert!(metrics_str.contains("featureduck_feature_writes_total"));
    assert!(metrics_str.contains("featureduck_rows_processed_total"));
    assert!(metrics_str.contains("featureduck_feature_read_duration_seconds"));
    assert!(metrics_str.contains("test_features"));
}

#[tokio::test]
async fn test_error_tracking_metrics() {
    // Given: Record some errors
    metrics::ERRORS_TOTAL
        .with_label_values(&["storage_error", "delta_connector"])
        .inc();

    metrics::ERRORS_TOTAL
        .with_label_values(&["validation_error", "api"])
        .inc_by(3);

    // When: Export metrics
    let metrics_str = metrics::export_metrics().unwrap();

    // Then: Error metrics should be tracked
    assert!(metrics_str.contains("featureduck_errors_total"));
    assert!(metrics_str.contains("storage_error"));
    assert!(metrics_str.contains("validation_error"));
}
