//! E2E tests for Registry REST API
//!
//! Tests the full registry API endpoints:
//! - Feature view CRUD
//! - Materialization triggers
//! - Run history
//! - Stats

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::{delete, get, post, put},
    Router,
};
use featureduck_server::state::AppState;
use serde_json::{json, Value};
use tower::ServiceExt;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Create a test app with registry routes
async fn create_registry_app() -> Router {
    let state = AppState::new_with_registry().await;

    Router::new()
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
        .with_state(state)
}

/// Helper to create a feature view JSON body
fn create_feature_view_body(name: &str) -> Value {
    json!({
        "name": name,
        "source_type": "delta",
        "source_path": "s3://test-bucket/features",
        "entities": ["user_id"],
        "transformations": "{}",
        "ttl_days": 30,
        "batch_schedule": "0 * * * *",
        "description": "Test feature view",
        "tags": ["test", "e2e"],
        "owner": "test_user"
    })
}

// ============================================================================
// Feature View CRUD Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_list_feature_views_empty() {
    let app = create_registry_app().await;

    let request = Request::builder()
        .uri("/api/v1/feature-views")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["count"], 0);
    assert!(json["feature_views"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_e2e_register_feature_view() {
    let app = create_registry_app().await;

    let body = create_feature_view_body("test_view");

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["name"], "test_view");
    assert_eq!(json["version"], 1);
    assert!(json["created_at"].is_string());
}

#[tokio::test]
async fn test_e2e_get_feature_view() {
    let app = create_registry_app().await;

    // Register first
    let body = create_feature_view_body("get_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Then get
    let request = Request::builder()
        .uri("/api/v1/feature-views/get_test_view")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["name"], "get_test_view");
    assert_eq!(json["source_type"], "delta");
    assert_eq!(json["entities"][0], "user_id");
}

#[tokio::test]
async fn test_e2e_get_nonexistent_feature_view() {
    let app = create_registry_app().await;

    let request = Request::builder()
        .uri("/api/v1/feature-views/nonexistent")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_e2e_update_feature_view() {
    let app = create_registry_app().await;

    // Register first
    let body = create_feature_view_body("update_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Then update
    let updated_body = json!({
        "name": "update_test_view",
        "source_type": "delta",
        "source_path": "s3://updated-bucket/features",
        "entities": ["user_id", "product_id"],
        "transformations": "{\"updated\": true}",
        "ttl_days": 60,
        "description": "Updated feature view"
    });

    let request = Request::builder()
        .method("PUT")
        .uri("/api/v1/feature-views/update_test_view")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&updated_body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Version should be incremented
    assert_eq!(json["version"], 2);
    assert_eq!(json["source_path"], "s3://updated-bucket/features");
}

#[tokio::test]
async fn test_e2e_delete_feature_view() {
    let app = create_registry_app().await;

    // Register first
    let body = create_feature_view_body("delete_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Delete
    let request = Request::builder()
        .method("DELETE")
        .uri("/api/v1/feature-views/delete_test_view")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify deleted
    let request = Request::builder()
        .uri("/api/v1/feature-views/delete_test_view")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_e2e_list_multiple_feature_views() {
    let app = create_registry_app().await;

    // Register multiple views
    for i in 1..=3 {
        let body = create_feature_view_body(&format!("list_test_view_{}", i));
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/feature-views")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();
        let _ = app.clone().oneshot(request).await.unwrap();
    }

    // List all
    let request = Request::builder()
        .uri("/api/v1/feature-views")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["count"], 3);
}

// ============================================================================
// Materialization Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_trigger_materialization() {
    let app = create_registry_app().await;

    // Register first
    let body = create_feature_view_body("materialize_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Trigger materialization
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views/materialize_test_view/materialize")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["run_id"].as_i64().unwrap() > 0);
    assert_eq!(json["status"], "pending");
}

#[tokio::test]
async fn test_e2e_trigger_materialization_nonexistent() {
    let app = create_registry_app().await;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views/nonexistent/materialize")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_e2e_get_runs_history() {
    let app = create_registry_app().await;

    // Register view
    let body = create_feature_view_body("history_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Trigger multiple materializations
    for _ in 0..3 {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/feature-views/history_test_view/materialize")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(request).await.unwrap();
    }

    // Get history
    let request = Request::builder()
        .uri("/api/v1/feature-views/history_test_view/runs")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["count"], 3);
    assert_eq!(json["runs"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_e2e_get_run_status() {
    let app = create_registry_app().await;

    // Register view
    let body = create_feature_view_body("run_status_test_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // Trigger materialization and get run_id
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views/run_status_test_view/materialize")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let run_id = json["run_id"].as_i64().unwrap();

    // Wait a bit for status update
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Get run status
    let request = Request::builder()
        .uri(format!("/api/v1/runs/{}", run_id))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["id"], run_id);
    assert_eq!(json["feature_view_name"], "run_status_test_view");
}

// ============================================================================
// Validation Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_register_missing_required_fields() {
    let app = create_registry_app().await;

    // Missing required fields
    let body = json!({
        "name": "incomplete_view"
        // Missing source_type, source_path, entities, transformations
    });

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    // Should fail validation
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn test_e2e_register_duplicate_name() {
    let app = create_registry_app().await;

    // Register first
    let body = create_feature_view_body("duplicate_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Register again (should update/overwrite per registry semantics)
    let body = create_feature_view_body("duplicate_view");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/feature-views")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    // Registry allows re-registration (upsert)
    assert_eq!(response.status(), StatusCode::CREATED);
}

// ============================================================================
// Filter Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_list_with_filter() {
    let app = create_registry_app().await;

    // Register views with different prefixes
    for name in ["user_features", "user_metrics", "product_features"] {
        let body = create_feature_view_body(name);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/feature-views")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();
        let _ = app.clone().oneshot(request).await.unwrap();
    }

    // Filter by "user" prefix
    let request = Request::builder()
        .uri("/api/v1/feature-views?filter=user")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Should only return user_features and user_metrics
    assert_eq!(json["count"], 2);
}
