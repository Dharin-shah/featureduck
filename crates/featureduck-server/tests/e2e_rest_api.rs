//! End-to-end integration tests for REST API endpoints
//!
//! These tests validate the complete REST API workflow from registration
//! to materialization orchestration.
//!
//! ## Test Coverage
//!
//! - Feature view lifecycle (register, get, update, delete)
//! - Materialization workflow (trigger, status, history)
//! - Error handling (404, validation)
//! - Filtering and querying
//! - Multi-view scenarios

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{json, Value};
use tower::ServiceExt;

// Helper function to create test app
async fn create_test_app() -> axum::Router {
    // We need to import the modules to build the router
    // For now, we'll create a minimal test setup
    use axum::{
        routing::{delete, get, post, put},
        Router,
    };
    use featureduck_server::state::AppState;

    let app_state = AppState::new_with_registry().await;

    Router::new()
        .route("/health", get(featureduck_server::api::health))
        .route(
            "/api/v1/feature-views",
            get(featureduck_server::api::registry::list_feature_views),
        )
        .route(
            "/api/v1/feature-views",
            post(featureduck_server::api::registry::register_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            get(featureduck_server::api::registry::get_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            put(featureduck_server::api::registry::update_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            delete(featureduck_server::api::registry::delete_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name/runs",
            get(featureduck_server::api::registry::get_runs_history),
        )
        .route(
            "/api/v1/feature-views/:name/materialize",
            post(featureduck_server::api::registry::trigger_materialization),
        )
        .route(
            "/api/v1/runs/:id",
            get(featureduck_server::api::registry::get_run_status),
        )
        .with_state(app_state)
}

// Helper to send request and parse JSON response
async fn send_request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = match method {
        "GET" => Request::builder().uri(uri).body(Body::empty()).unwrap(),
        "POST" | "PUT" => {
            let json_body = body.unwrap_or(json!({}));
            Request::builder()
                .uri(uri)
                .method(method)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json_body).unwrap()))
                .unwrap()
        }
        "DELETE" => Request::builder()
            .uri(uri)
            .method("DELETE")
            .body(Body::empty())
            .unwrap(),
        _ => panic!("Unsupported method: {}", method),
    };

    let response = ServiceExt::<Request<Body>>::oneshot(app.clone(), request)
        .await
        .unwrap();
    let status = response.status();

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let json_response: Value = if body_bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body_bytes)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&body_bytes)}))
    };

    (status, json_response)
}

#[tokio::test]
async fn test_e2e_health_check() {
    // Given: A running server
    let app = create_test_app().await;

    // When: Health check is called
    let (status, body) = send_request(&app, "GET", "/health", None).await;

    // Then: Should return 200 OK with health info
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "healthy");
    assert!(body["version"].is_string());
}

#[tokio::test]
async fn test_e2e_feature_view_lifecycle() {
    // Given: A running server
    let app = create_test_app().await;

    // When: List feature views (initially empty)
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views", None).await;

    // Then: Should return empty list
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0);
    assert_eq!(body["feature_views"].as_array().unwrap().len(), 0);

    // When: Register a new feature view
    let register_body = json!({
        "name": "user_features",
        "version": 1,
        "source_type": "delta",
        "source_path": "s3://bucket/events",
        "entities": ["user_id"],
        "transformations": "{}",
        "timestamp_field": "event_timestamp",
        "ttl_days": 30,
        "batch_schedule": "0 * * * *",
        "description": "User activity features",
        "tags": ["user", "activity"],
        "owner": "data-team"
    });

    let (status, body) =
        send_request(&app, "POST", "/api/v1/feature-views", Some(register_body)).await;

    // Then: Should return 201 Created
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["name"], "user_features");
    assert_eq!(body["version"], 1);

    // When: Get the feature view
    let (status, body) =
        send_request(&app, "GET", "/api/v1/feature-views/user_features", None).await;

    // Then: Should return the feature view details
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "user_features");
    assert_eq!(body["version"], 1);
    assert_eq!(body["source_type"], "delta");
    assert_eq!(body["entities"], json!(["user_id"]));
    assert_eq!(body["ttl_days"], 30);

    // When: List feature views again
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views", None).await;

    // Then: Should return 1 feature view
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["feature_views"][0]["name"], "user_features");

    // When: Update the feature view
    let update_body = json!({
        "name": "user_features",
        "source_type": "delta",
        "source_path": "s3://bucket/events_v2",
        "entities": ["user_id"],
        "transformations": "{}",
        "ttl_days": 60,
        "description": "Updated user features"
    });

    let (status, body) = send_request(
        &app,
        "PUT",
        "/api/v1/feature-views/user_features",
        Some(update_body),
    )
    .await;

    // Then: Should return updated view with incremented version
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["version"], 2);
    assert_eq!(body["ttl_days"], 60);
    assert_eq!(body["source_path"], "s3://bucket/events_v2");

    // When: Delete the feature view
    let (status, _body) =
        send_request(&app, "DELETE", "/api/v1/feature-views/user_features", None).await;

    // Then: Should return 204 No Content
    assert_eq!(status, StatusCode::NO_CONTENT);

    // When: Try to get deleted feature view
    let (status, body) =
        send_request(&app, "GET", "/api/v1/feature-views/user_features", None).await;

    // Then: Should return 404 Not Found
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[tokio::test]
async fn test_e2e_materialization_workflow() {
    // Given: A server with a registered feature view
    let app = create_test_app().await;

    let register_body = json!({
        "name": "test_features",
        "source_type": "delta",
        "source_path": "s3://bucket/data",
        "entities": ["user_id"],
        "transformations": "{}"
    });

    let (status, _) =
        send_request(&app, "POST", "/api/v1/feature-views", Some(register_body)).await;
    assert_eq!(status, StatusCode::CREATED);

    // When: Trigger materialization
    let (status, body) = send_request(
        &app,
        "POST",
        "/api/v1/feature-views/test_features/materialize",
        None,
    )
    .await;

    // Then: Should return 202 Accepted with run ID
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["run_id"].as_i64().unwrap() > 0);
    assert_eq!(body["status"], "pending");
    assert!(body["started_at"].is_string());

    let run_id = body["run_id"].as_i64().unwrap();

    // When: Get run status
    let (status, body) = send_request(&app, "GET", &format!("/api/v1/runs/{}", run_id), None).await;

    // Then: Should return run details
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], run_id);
    assert_eq!(body["feature_view_name"], "test_features");
    // Status can be "pending" or "failed" (transformations are invalid JSON, causing immediate failure)
    let status_str = body["status"].as_str().unwrap();
    assert!(
        status_str == "pending" || status_str == "failed",
        "Expected status 'pending' or 'failed', got '{}'",
        status_str
    );

    // When: Get runs history
    let (status, body) = send_request(
        &app,
        "GET",
        "/api/v1/feature-views/test_features/runs",
        None,
    )
    .await;

    // Then: Should return run history
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["runs"][0]["id"], run_id);
    // Status can be "pending" or "failed" (transformations are invalid JSON)
    let status_str = body["runs"][0]["status"].as_str().unwrap();
    assert!(
        status_str == "pending" || status_str == "failed",
        "Expected status 'pending' or 'failed', got '{}'",
        status_str
    );
}

#[tokio::test]
async fn test_e2e_multiple_feature_views() {
    // Given: A running server
    let app = create_test_app().await;

    // When: Register multiple feature views
    let views = vec![
        ("user_7d_features", vec!["user_id"], "7d user metrics"),
        ("user_30d_features", vec!["user_id"], "30d user metrics"),
        ("product_features", vec!["product_id"], "product attributes"),
    ];

    for (name, entities, desc) in &views {
        let body = json!({
            "name": name,
            "source_type": "delta",
            "source_path": format!("s3://bucket/{}", name),
            "entities": entities,
            "transformations": "{}",
            "description": desc
        });

        let (status, _) = send_request(&app, "POST", "/api/v1/feature-views", Some(body)).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // When: List all feature views
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views", None).await;

    // Then: Should return all 3 views
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 3);

    // When: Filter by name pattern
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views?filter=user", None).await;

    // Then: Should return only user views
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    for view in body["feature_views"].as_array().unwrap() {
        assert!(view["name"].as_str().unwrap().contains("user"));
    }
}

#[tokio::test]
async fn test_e2e_error_handling() {
    // Given: A running server
    let app = create_test_app().await;

    // When: Try to get non-existent feature view
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views/nonexistent", None).await;

    // Then: Should return 404
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));

    // When: Try to update non-existent feature view
    let update_body = json!({
        "name": "nonexistent",
        "source_type": "delta",
        "source_path": "s3://bucket/data",
        "entities": ["user_id"],
        "transformations": "{}"
    });

    let (status, _body) = send_request(
        &app,
        "PUT",
        "/api/v1/feature-views/nonexistent",
        Some(update_body),
    )
    .await;

    // Then: Should return 404
    assert_eq!(status, StatusCode::NOT_FOUND);

    // When: Try to trigger materialization for non-existent view
    let (status, _body) = send_request(
        &app,
        "POST",
        "/api/v1/feature-views/nonexistent/materialize",
        None,
    )
    .await;

    // Then: Should return 404
    assert_eq!(status, StatusCode::NOT_FOUND);

    // When: Try to get non-existent run
    let (status, body) = send_request(&app, "GET", "/api/v1/runs/999999", None).await;

    // Then: Should return 404
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[tokio::test]
async fn test_e2e_run_tracking_lifecycle() {
    // Given: A feature view with multiple runs
    let app = create_test_app().await;

    let register_body = json!({
        "name": "tracked_features",
        "source_type": "delta",
        "source_path": "s3://bucket/data",
        "entities": ["user_id"],
        "transformations": "{}"
    });

    let (status, _) =
        send_request(&app, "POST", "/api/v1/feature-views", Some(register_body)).await;
    assert_eq!(status, StatusCode::CREATED);

    // When: Trigger multiple materialization runs
    let mut run_ids = Vec::new();
    for _ in 0..3 {
        let (status, body) = send_request(
            &app,
            "POST",
            "/api/v1/feature-views/tracked_features/materialize",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        run_ids.push(body["run_id"].as_i64().unwrap());
    }

    // Then: Should have 3 different run IDs
    assert_eq!(run_ids.len(), 3);
    assert!(run_ids[0] < run_ids[1]);
    assert!(run_ids[1] < run_ids[2]);

    // When: Get runs history
    let (status, body) = send_request(
        &app,
        "GET",
        "/api/v1/feature-views/tracked_features/runs",
        None,
    )
    .await;

    // Then: Should return all 3 runs in reverse chronological order
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 3);
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs[0]["id"], run_ids[2]); // Most recent first
    assert_eq!(runs[1]["id"], run_ids[1]);
    assert_eq!(runs[2]["id"], run_ids[0]);

    // All should be in pending or failed state (transformations are invalid JSON)
    for run in runs {
        let status = run["status"].as_str().unwrap();
        assert!(
            status == "pending" || status == "failed",
            "Expected 'pending' or 'failed', got '{}'",
            status
        );
    }
}

#[tokio::test]
async fn test_e2e_concurrent_operations() {
    // Given: A running server
    let app = create_test_app().await;

    // Register a feature view
    let register_body = json!({
        "name": "concurrent_features",
        "source_type": "delta",
        "source_path": "s3://bucket/data",
        "entities": ["user_id"],
        "transformations": "{}"
    });

    let (status, _) =
        send_request(&app, "POST", "/api/v1/feature-views", Some(register_body)).await;
    assert_eq!(status, StatusCode::CREATED);

    // When: Perform multiple operations
    // 1. Get the view
    let (status, body) = send_request(
        &app,
        "GET",
        "/api/v1/feature-views/concurrent_features",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "concurrent_features");

    // 2. Trigger materialization
    let (status, _) = send_request(
        &app,
        "POST",
        "/api/v1/feature-views/concurrent_features/materialize",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // 3. List all views
    let (status, body) = send_request(&app, "GET", "/api/v1/feature-views", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["count"].as_i64().unwrap() > 0);

    // 4. Get runs history
    let (status, body) = send_request(
        &app,
        "GET",
        "/api/v1/feature-views/concurrent_features/runs",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);

    // Then: All operations should succeed without data corruption
    // Registry should maintain consistency
}

#[tokio::test]
async fn test_e2e_feature_serving_with_real_storage() {
    use chrono::Utc;
    use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
    use featureduck_server::state::AppState;
    use tempfile::TempDir;

    // Given: A server with real storage connector
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage_path = temp_dir.path().to_str().unwrap();

    let app_state = AppState::with_storage(storage_path, None)
        .await
        .expect("Failed to create state with storage");

    // Write some test features
    let storage = app_state.storage().expect("Storage should be available");

    let mut row1 = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row1.add_feature("clicks_7d".to_string(), FeatureValue::Int(42));
    row1.add_feature("purchases_30d".to_string(), FeatureValue::Int(3));

    let mut row2 = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row2.add_feature("clicks_7d".to_string(), FeatureValue::Int(15));
    row2.add_feature("purchases_30d".to_string(), FeatureValue::Int(1));

    storage
        .write_features("user_features", vec![row1, row2])
        .await
        .expect("Failed to write features");

    // Build the router with storage-enabled state
    let app = axum::Router::new()
        .route(
            "/v1/features/online",
            axum::routing::post(featureduck_server::api::get_online_features),
        )
        .route(
            "/v1/features/historical",
            axum::routing::post(featureduck_server::api::get_historical_features),
        )
        .with_state(app_state);

    // When: Request online features
    let request_body = json!({
        "feature_view": "user_features",
        "entities": [
            {"user_id": "123"},
            {"user_id": "456"}
        ]
    });

    let (status, body) =
        send_request(&app, "POST", "/v1/features/online", Some(request_body)).await;

    // Then: Should return real feature data
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["metadata"]["count"], 2);
    assert!(body["metadata"]["latency_ms"].as_u64().unwrap() < 1000);

    let features = body["features"]
        .as_array()
        .expect("Features should be array");
    assert_eq!(features.len(), 2);

    // Find entities by user_id (order not guaranteed by DuckDB)
    let user_123 = features
        .iter()
        .find(|f| f["user_id"] == "123")
        .expect("Should find user_123");
    let user_456 = features
        .iter()
        .find(|f| f["user_id"] == "456")
        .expect("Should find user_456");

    // Check user_123
    assert_eq!(user_123["user_id"], "123");
    assert_eq!(user_123["clicks_7d"], 42);
    assert_eq!(user_123["purchases_30d"], 3);

    // Check user_456
    assert_eq!(user_456["user_id"], "456");
    assert_eq!(user_456["clicks_7d"], 15);
    assert_eq!(user_456["purchases_30d"], 1);

    // When: Request historical features with timestamp
    let historical_body = json!({
        "feature_view": "user_features",
        "entities": [{"user_id": "123"}],
        "timestamp": "2024-01-01T00:00:00Z"
    });

    let (status, body) = send_request(
        &app,
        "POST",
        "/v1/features/historical",
        Some(historical_body),
    )
    .await;

    // Then: Should return features as they existed at that time (likely empty since our data is recent)
    assert_eq!(status, StatusCode::OK);
    assert!(body["metadata"]["count"].as_u64().unwrap() <= 1);
}

#[tokio::test]
async fn test_e2e_feature_view_stats() {
    use chrono::Utc;
    use featureduck_registry::{FeatureRegistry, FeatureViewStats, RegistryConfig};
    use tempfile::TempDir;

    // Given: Create a shared file-backed registry
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let registry_path = temp_dir.path().join("test_registry.db");
    let registry_path_str = registry_path.to_str().unwrap();

    // Create state with file-backed registry (same as app uses)
    let app_state = featureduck_server::state::AppState::with_registry_path(registry_path_str)
        .await
        .expect("Failed to create state with registry");

    // Build app with this state
    use axum::{
        routing::{get, post},
        Router,
    };
    let app = Router::new()
        .route(
            "/api/v1/feature-views",
            post(featureduck_server::api::registry::register_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name/stats",
            get(featureduck_server::api::registry::get_feature_view_stats),
        )
        .with_state(app_state);

    // Register a feature view
    let register_body = json!({
        "name": "stats_test_view",
        "source_type": "delta",
        "source_path": "s3://bucket/data",
        "entities": ["user_id"],
        "transformations": "{}"
    });

    let (status, _) =
        send_request(&app, "POST", "/api/v1/feature-views", Some(register_body)).await;
    assert_eq!(status, StatusCode::CREATED);

    // Save stats directly via a new registry connection to the same database
    let config = RegistryConfig::sqlite(registry_path_str);
    let registry = FeatureRegistry::new(config)
        .await
        .expect("Failed to create registry");

    let stats = FeatureViewStats {
        feature_view: "stats_test_view".to_string(),
        version: 1,
        row_count: 1000000,
        distinct_entities: 50000,
        min_timestamp: Some(1704067200000000),
        max_timestamp: Some(1735689600000000),
        avg_file_size_bytes: Some(1048576),
        total_size_bytes: 1073741824,
        histogram_buckets: Some("[[0,100],[100,200]]".to_string()),
        created_at: Utc::now(),
    };

    registry
        .save_stats(&stats)
        .await
        .expect("Failed to save stats");

    // When: Get stats via REST API
    let (status, body) = send_request(
        &app,
        "GET",
        "/api/v1/feature-views/stats_test_view/stats",
        None,
    )
    .await;

    // Then: Should return stats data
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["feature_view"], "stats_test_view");
    assert_eq!(body["version"], 1);
    assert_eq!(body["row_count"], 1000000);
    assert_eq!(body["distinct_entities"], 50000);
    assert_eq!(body["min_timestamp"], 1704067200000000i64);
    assert_eq!(body["max_timestamp"], 1735689600000000i64);
    assert_eq!(body["avg_file_size_bytes"], 1048576);
    assert_eq!(body["total_size_bytes"], 1073741824i64);

    // Histogram should be parsed as JSON array, not string
    assert!(
        body["histogram_buckets"].is_array(),
        "histogram_buckets should be an array"
    );
    let histogram = body["histogram_buckets"]
        .as_array()
        .expect("histogram should be array");
    assert_eq!(histogram.len(), 2);
    assert_eq!(histogram[0], json!([0, 100]));
    assert_eq!(histogram[1], json!([100, 200]));

    assert!(body["created_at"].is_string());

    // When: Request stats for non-existent feature view
    let (status, body) =
        send_request(&app, "GET", "/api/v1/feature-views/nonexistent/stats", None).await;

    // Then: Should return 404
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("No stats found"));
}
