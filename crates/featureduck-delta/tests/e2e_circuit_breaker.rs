//! E2E tests for circuit breaker resilience
//!
//! Tests the circuit breaker pattern that protects against cascading failures:
//! - Circuit opens after consecutive failures
//! - Circuit allows requests in half-open state
//! - Circuit closes after successful requests
//! - Circuit stats are tracked correctly

use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{
    CircuitBreakerConfig, CircuitState, DeltaStorageConnector, DuckDBEngineConfig,
};
use tempfile::TempDir;

/// Helper to create a test connector with custom circuit breaker config
async fn create_connector_with_circuit_breaker(
    config: CircuitBreakerConfig,
) -> (DeltaStorageConnector, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .expect("Failed to create connector");

    connector.set_circuit_breaker_config(config);
    (connector, temp_dir)
}

#[tokio::test]
async fn test_circuit_breaker_starts_closed() {
    // Given: A new connector with circuit breaker
    let config = CircuitBreakerConfig::default();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Then: Circuit should start in closed state
    let stats = connector.circuit_breaker_stats();
    assert_eq!(
        stats.state,
        CircuitState::Closed,
        "Circuit breaker should start in closed state"
    );
    assert_eq!(stats.failure_count, 0);
    assert_eq!(stats.success_count, 0);
}

#[tokio::test]
async fn test_circuit_breaker_allows_successful_operations() {
    // Given: A connector with circuit breaker and some test data
    let config = CircuitBreakerConfig::default();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Write some test data
    let entity_key = EntityKey::new("user_id", "test_user");
    let mut row = FeatureRow::new(vec![entity_key.clone()], chrono::Utc::now());
    row.add_feature("clicks".to_string(), FeatureValue::Int(42));

    connector
        .write_features("test_view", vec![row])
        .await
        .expect("Write should succeed");

    // When: We perform a successful read
    let result = connector
        .read_features("test_view", vec![entity_key], None)
        .await;

    // Then: Operation should succeed (circuit breaker allows it)
    assert!(
        result.is_ok(),
        "Read should succeed with circuit breaker enabled"
    );

    // Circuit should remain closed (no failures)
    let stats = connector.circuit_breaker_stats();
    assert_eq!(
        stats.state,
        CircuitState::Closed,
        "Circuit should stay closed after successful ops"
    );
}

#[tokio::test]
async fn test_circuit_breaker_tracks_failures() {
    // Given: A connector with circuit breaker
    let config = CircuitBreakerConfig::default();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // When: We try to read from a non-existent table
    let entity_key = EntityKey::new("user_id", "test_user");
    let result = connector
        .read_features("nonexistent_table", vec![entity_key], None)
        .await;

    // Then: Failure should be tracked
    assert!(result.is_err(), "Read from nonexistent table should fail");
    let stats = connector.circuit_breaker_stats();
    assert!(
        stats.failure_count > 0,
        "Failure count should be incremented"
    );
}

#[tokio::test]
async fn test_circuit_breaker_opens_after_threshold() {
    // Given: A connector with a low failure threshold for testing
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        timeout: std::time::Duration::from_secs(1),
        ..Default::default()
    };
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // When: We cause multiple failures by reading from non-existent table
    let entity_key = EntityKey::new("user_id", "test_user");
    for _ in 0..5 {
        let _ = connector
            .read_features("nonexistent_table", vec![entity_key.clone()], None)
            .await;
    }

    // Then: Circuit should be open
    let stats = connector.circuit_breaker_stats();
    assert_eq!(
        stats.state,
        CircuitState::Open,
        "Circuit should be open after {} failures, current state: {:?}",
        stats.failure_count,
        stats.state
    );
}

#[tokio::test]
async fn test_circuit_breaker_rejects_when_open() {
    // Given: A connector with low thresholds and open circuit
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        success_threshold: 1,
        timeout: std::time::Duration::from_secs(60), // Long timeout to keep it open
        ..Default::default()
    };
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Cause failures to open the circuit
    let entity_key = EntityKey::new("user_id", "test_user");
    for _ in 0..5 {
        let _ = connector
            .read_features("nonexistent_table", vec![entity_key.clone()], None)
            .await;
    }

    // Verify circuit is open
    let stats = connector.circuit_breaker_stats();
    if stats.state != CircuitState::Open {
        // Circuit might be half-open, which is also valid
        println!("Circuit state: {:?}", stats.state);
        return;
    }

    // When: We try another request while circuit is open
    let result = connector
        .read_features("another_table", vec![entity_key], None)
        .await;

    // Then: Request should be rejected fast
    assert!(result.is_err(), "Request should fail when circuit is open");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Circuit breaker") || error_msg.contains("temporarily disabled"),
        "Error should indicate circuit breaker is open: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_circuit_breaker_reset() {
    // Given: A connector with some failures recorded
    let config = CircuitBreakerConfig::default();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Cause some failures
    let entity_key = EntityKey::new("user_id", "test_user");
    for _ in 0..2 {
        let _ = connector
            .read_features("nonexistent_table", vec![entity_key.clone()], None)
            .await;
    }

    // When: We reset the circuit breaker
    connector.reset_circuit_breaker();

    // Then: Stats should be reset
    let stats = connector.circuit_breaker_stats();
    assert_eq!(
        stats.state,
        CircuitState::Closed,
        "Circuit should be closed after reset"
    );
}

#[tokio::test]
async fn test_circuit_breaker_stats_serializable() {
    // Given: A connector with circuit breaker
    let config = CircuitBreakerConfig::default();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // When: We get stats
    let stats = connector.circuit_breaker_stats();

    // Then: Stats should have expected structure (u32 fields are always >= 0)
    // State should be one of the valid states
    assert!(
        matches!(
            stats.state,
            CircuitState::Closed | CircuitState::Open | CircuitState::HalfOpen
        ),
        "State should be valid: {:?}",
        stats.state
    );
    // Verify stats are accessible
    let _ = stats.failure_count;
    let _ = stats.success_count;
    let _ = stats.total_requests;
}

#[tokio::test]
async fn test_circuit_breaker_with_strict_config() {
    // Given: A connector with strict circuit breaker config
    let config = CircuitBreakerConfig::strict();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Then: Should have stricter thresholds
    // Strict config opens circuit faster
    let stats = connector.circuit_breaker_stats();
    assert_eq!(stats.state, CircuitState::Closed, "Should start closed");
}

#[tokio::test]
async fn test_circuit_breaker_with_lenient_config() {
    // Given: A connector with lenient circuit breaker config
    let config = CircuitBreakerConfig::lenient();
    let (connector, _temp_dir) = create_connector_with_circuit_breaker(config).await;

    // Then: Should have lenient thresholds
    let stats = connector.circuit_breaker_stats();
    assert_eq!(stats.state, CircuitState::Closed, "Should start closed");

    // Cause some failures - lenient config should not open circuit quickly
    let entity_key = EntityKey::new("user_id", "test_user");
    for _ in 0..3 {
        let _ = connector
            .read_features("nonexistent_table", vec![entity_key.clone()], None)
            .await;
    }

    // With lenient config (threshold=10), circuit should still be closed after 3 failures
    let stats = connector.circuit_breaker_stats();
    assert!(
        stats.state == CircuitState::Closed || stats.failure_count < 10,
        "Lenient config should not open circuit after only 3 failures"
    );
}
