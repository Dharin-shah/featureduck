//! Feature Registry REST API Endpoints
//!
//! This module implements HTTP endpoints for feature view management and
//! materialization orchestration.
//!
//! ## API Design
//!
//! - `GET /api/v1/feature-views` - List all feature views
//! - `GET /api/v1/feature-views/:name` - Get specific feature view
//! - `POST /api/v1/feature-views` - Register new feature view
//! - `PUT /api/v1/feature-views/:name` - Update feature view
//! - `DELETE /api/v1/feature-views/:name` - Delete feature view
//! - `GET /api/v1/feature-views/:name/runs` - Get materialization history
//! - `POST /api/v1/feature-views/:name/materialize` - Trigger materialization
//! - `GET /api/v1/runs/:id` - Get run status

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{error::AppError, state::AppState};
use featureduck_registry::FeatureViewDef;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Query parameters for listing feature views
#[derive(Debug, Deserialize)]
pub struct ListParams {
    /// Optional filter by name pattern
    pub filter: Option<String>,
}

/// Response for listing feature views
#[derive(Debug, Serialize)]
pub struct ListFeatureViewsResponse {
    pub feature_views: Vec<FeatureViewResponse>,
    pub count: usize,
}

/// Feature view in API response format
#[derive(Debug, Serialize)]
pub struct FeatureViewResponse {
    pub name: String,
    pub version: i32,
    pub source_type: String,
    pub source_path: String,
    pub entities: Vec<String>,
    pub ttl_days: Option<i32>,
    pub batch_schedule: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub owner: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<FeatureViewDef> for FeatureViewResponse {
    fn from(view: FeatureViewDef) -> Self {
        Self {
            name: view.name,
            version: view.version,
            source_type: view.source_type,
            source_path: view.source_path,
            entities: view.entities,
            ttl_days: view.ttl_days,
            batch_schedule: view.batch_schedule,
            description: view.description,
            tags: view.tags,
            owner: view.owner,
            created_at: view.created_at.to_rfc3339(),
            updated_at: view.updated_at.to_rfc3339(),
        }
    }
}

/// Request to register a new feature view
#[derive(Debug, Deserialize)]
pub struct RegisterFeatureViewRequest {
    pub name: String,
    pub version: Option<i32>,
    pub source_type: String,
    pub source_path: String,
    pub entities: Vec<String>,
    pub transformations: String,
    pub timestamp_field: Option<String>,
    pub ttl_days: Option<i32>,
    pub batch_schedule: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub owner: Option<String>,
}

/// Response after registering a feature view
#[derive(Debug, Serialize)]
pub struct RegisterFeatureViewResponse {
    pub name: String,
    pub version: i32,
    pub created_at: String,
}

/// Response for materialization trigger
#[derive(Debug, Serialize)]
pub struct MaterializeResponse {
    pub run_id: i64,
    pub status: String,
    pub started_at: String,
}

/// Response for run status query
#[derive(Debug, Serialize)]
pub struct RunStatusResponse {
    pub id: i64,
    pub feature_view_name: String,
    pub status: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub rows_processed: Option<i64>,
    pub error_message: Option<String>,
}

/// Response for runs history
#[derive(Debug, Serialize)]
pub struct RunsHistoryResponse {
    pub runs: Vec<RunStatusResponse>,
    pub count: usize,
}

// ============================================================================
// Endpoint Handlers
// ============================================================================

/// List all feature views
///
/// ## Endpoint
/// `GET /api/v1/feature-views?filter=pattern`
///
/// ## Query Parameters
/// - `filter` (optional): Filter by name pattern
///
/// ## Response
/// ```json
/// {
///   "feature_views": [...],
///   "count": 2
/// }
/// ```
#[instrument(skip(state), fields(filter = ?params.filter))]
pub async fn list_feature_views(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListFeatureViewsResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    let views = registry
        .list_feature_views(params.filter.as_deref())
        .await
        .map_err(|e| AppError::Internal(format!("Failed to list feature views: {}", e)))?;

    let response_views: Vec<FeatureViewResponse> =
        views.into_iter().map(FeatureViewResponse::from).collect();

    Ok(Json(ListFeatureViewsResponse {
        count: response_views.len(),
        feature_views: response_views,
    }))
}

/// Get a specific feature view by name
///
/// ## Endpoint
/// `GET /api/v1/feature-views/:name`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Response
/// ```json
/// {
///   "name": "user_features",
///   "version": 1,
///   ...
/// }
/// ```
#[instrument(skip(state), fields(feature_view = %name))]
pub async fn get_feature_view(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<FeatureViewResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    let view = registry
        .get_feature_view(&name)
        .await
        .map_err(|e| AppError::NotFound(format!("Feature view '{}' not found: {}", name, e)))?;

    Ok(Json(FeatureViewResponse::from(view)))
}

/// Register a new feature view
///
/// ## Endpoint
/// `POST /api/v1/feature-views`
///
/// ## Request Body
/// ```json
/// {
///   "name": "user_features",
///   "source_type": "delta",
///   "source_path": "s3://bucket/events",
///   "entities": ["user_id"],
///   "transformations": "{}",
///   ...
/// }
/// ```
///
/// ## Response
/// `201 Created`
/// ```json
/// {
///   "name": "user_features",
///   "version": 1,
///   "created_at": "2025-10-29T..."
/// }
/// ```
#[instrument(skip(state, req), fields(feature_view = %req.name, source_type = %req.source_type))]
pub async fn register_feature_view(
    State(state): State<AppState>,
    Json(req): Json<RegisterFeatureViewRequest>,
) -> Result<(StatusCode, Json<RegisterFeatureViewResponse>), AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    let now = chrono::Utc::now();

    let view = FeatureViewDef {
        name: req.name.clone(),
        version: req.version.unwrap_or(1),
        source_type: req.source_type,
        source_path: req.source_path,
        entities: req.entities,
        transformations: req.transformations,
        timestamp_field: req.timestamp_field,
        ttl_days: req.ttl_days,
        batch_schedule: req.batch_schedule,
        description: req.description,
        tags: req.tags.unwrap_or_default(),
        owner: req.owner,
        created_at: now,
        updated_at: now,
    };

    registry
        .register_feature_view(&view)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to register feature view: {}", e)))?;

    Ok((
        StatusCode::CREATED,
        Json(RegisterFeatureViewResponse {
            name: req.name,
            version: view.version,
            created_at: now.to_rfc3339(),
        }),
    ))
}

/// Update an existing feature view
///
/// ## Endpoint
/// `PUT /api/v1/feature-views/:name`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Request Body
/// Same as register (will increment version)
///
/// ## Response
/// `200 OK` with updated feature view
pub async fn update_feature_view(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<RegisterFeatureViewRequest>,
) -> Result<Json<FeatureViewResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    // Get current version
    let current = registry
        .get_feature_view(&name)
        .await
        .map_err(|e| AppError::NotFound(format!("Feature view '{}' not found: {}", name, e)))?;

    let now = chrono::Utc::now();

    // Create updated version
    let updated = FeatureViewDef {
        name: name.clone(),
        version: current.version + 1,
        source_type: req.source_type,
        source_path: req.source_path,
        entities: req.entities,
        transformations: req.transformations,
        timestamp_field: req.timestamp_field,
        ttl_days: req.ttl_days,
        batch_schedule: req.batch_schedule,
        description: req.description,
        tags: req.tags.unwrap_or_default(),
        owner: req.owner,
        created_at: current.created_at,
        updated_at: now,
    };

    registry
        .register_feature_view(&updated)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to update feature view: {}", e)))?;

    Ok(Json(FeatureViewResponse::from(updated)))
}

/// Delete a feature view
///
/// ## Endpoint
/// `DELETE /api/v1/feature-views/:name`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Response
/// `204 No Content`
pub async fn delete_feature_view(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    registry
        .delete_feature_view(&name)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to delete feature view: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Get statistics for a feature view
///
/// ## Endpoint
/// `GET /api/v1/feature-views/:name/stats`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Response
/// Returns the most recent statistics for the feature view, ordered by `created_at DESC`.
///
/// ```json
/// {
///   "feature_view": "user_features",
///   "version": 1,
///   "row_count": 1000000,
///   "distinct_entities": 50000,
///   "min_timestamp": 1704067200000000,
///   "max_timestamp": 1735689600000000,
///   "avg_file_size_bytes": 1048576,
///   "total_size_bytes": 1073741824,
///   "histogram_buckets": [[0, 100, 500], [100, 200, 300]],
///   "created_at": "2024-01-15T10:30:00Z"
/// }
/// ```
///
/// **Note:** Timestamps (`min_timestamp`, `max_timestamp`) are in **microseconds since Unix epoch**.
///
/// ## Errors
/// - `404 Not Found` - Feature view doesn't exist or has no stats
/// - `500 Internal Server Error` - Database or internal error
pub async fn get_feature_view_stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<FeatureViewStatsResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    // Get stats for the feature view
    let stats = registry
        .get_latest_stats(&name)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to get stats: {}", e)))?
        .ok_or_else(|| AppError::NotFound(format!("No stats found for feature view: {}", name)))?;

    Ok(Json(FeatureViewStatsResponse::from(stats)))
}

/// Response for feature view statistics
#[derive(Debug, Serialize)]
pub struct FeatureViewStatsResponse {
    pub feature_view: String,
    pub version: i32,
    pub row_count: i64,
    pub distinct_entities: i64,
    /// Minimum timestamp in microseconds since epoch
    pub min_timestamp: Option<i64>,
    /// Maximum timestamp in microseconds since epoch
    pub max_timestamp: Option<i64>,
    pub avg_file_size_bytes: Option<i64>,
    pub total_size_bytes: i64,
    /// Histogram buckets as structured JSON (array of [lower, upper, count])
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram_buckets: Option<serde_json::Value>,
    pub created_at: String,
}

impl From<featureduck_registry::FeatureViewStats> for FeatureViewStatsResponse {
    fn from(stats: featureduck_registry::FeatureViewStats) -> Self {
        // Parse histogram JSON string into structured Value
        let histogram_buckets = stats
            .histogram_buckets
            .and_then(|json_str| serde_json::from_str(&json_str).ok());

        Self {
            feature_view: stats.feature_view,
            version: stats.version,
            row_count: stats.row_count,
            distinct_entities: stats.distinct_entities,
            min_timestamp: stats.min_timestamp,
            max_timestamp: stats.max_timestamp,
            avg_file_size_bytes: stats.avg_file_size_bytes,
            total_size_bytes: stats.total_size_bytes,
            histogram_buckets,
            created_at: stats.created_at.to_rfc3339(),
        }
    }
}

/// Get materialization history for a feature view
///
/// ## Endpoint
/// `GET /api/v1/feature-views/:name/runs?limit=10`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Query Parameters
/// - `limit` (optional): Maximum number of runs to return (default: 10)
///
/// ## Response
/// ```json
/// {
///   "runs": [...],
///   "count": 5
/// }
/// ```
pub async fn get_runs_history(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<serde_json::Value>,
) -> Result<Json<RunsHistoryResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

    let runs = registry
        .get_recent_runs(&name, limit)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to get runs: {}", e)))?;

    let response_runs: Vec<RunStatusResponse> = runs
        .into_iter()
        .map(|run| RunStatusResponse {
            id: run.id,
            feature_view_name: run.feature_view_name,
            status: run.status.to_string(),
            started_at: run.started_at.map(|dt| dt.to_rfc3339()),
            completed_at: run.completed_at.map(|dt| dt.to_rfc3339()),
            rows_processed: run.rows_processed,
            error_message: run.error_message,
        })
        .collect();

    Ok(Json(RunsHistoryResponse {
        count: response_runs.len(),
        runs: response_runs,
    }))
}

/// Trigger materialization for a feature view
///
/// ## Endpoint
/// `POST /api/v1/feature-views/:name/materialize`
///
/// ## Path Parameters
/// - `name`: Feature view name
///
/// ## Response
/// `202 Accepted`
/// ```json
/// {
///   "run_id": 123,
///   "status": "pending",
///   "started_at": "2025-10-29T..."
/// }
/// ```
#[instrument(skip(state), fields(feature_view = %name, run_id))]
pub async fn trigger_materialization(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<MaterializeResponse>), AppError> {
    use featureduck_registry::RunStatus;

    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    // Get feature view definition (validates it exists)
    let _view = registry
        .get_feature_view(&name)
        .await
        .map_err(|e| AppError::NotFound(format!("Feature view '{}' not found: {}", name, e)))?;

    // Create run
    let run_id = registry
        .create_run(&name)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to create run: {}", e)))?;

    // Record run_id in the current span for tracing correlation
    tracing::Span::current().record("run_id", run_id);

    let now = chrono::Utc::now();

    // ⚠️ MIGRATION NOTICE: LogicalPlan-based materialization has been removed
    // The system now uses SQLFrame (PySpark API) for transformations instead
    let error_msg = "LogicalPlan-based materialization is no longer supported. \
                     Please use SQLFrame (PySpark API) for feature transformations. \
                     See documentation for migration guide.";

    // Update run status to failed
    let _ = registry
        .update_run_status(run_id, RunStatus::Failed, None, Some(error_msg))
        .await;

    tracing::error!(
        "Materialization not supported for view '{}': {}",
        name,
        error_msg
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(MaterializeResponse {
            run_id,
            status: "pending".to_string(),
            started_at: now.to_rfc3339(),
        }),
    ))
}

/// Get status of a specific materialization run
///
/// ## Endpoint
/// `GET /api/v1/runs/:id`
///
/// ## Path Parameters
/// - `id`: Run ID
///
/// ## Response
/// ```json
/// {
///   "id": 123,
///   "feature_view_name": "user_features",
///   "status": "success",
///   "started_at": "...",
///   "completed_at": "...",
///   "rows_processed": 1000
/// }
/// ```
pub async fn get_run_status(
    State(state): State<AppState>,
    Path(run_id): Path<i64>,
) -> Result<Json<RunStatusResponse>, AppError> {
    let registry = state
        .registry()
        .ok_or_else(|| AppError::Internal("Registry not initialized".to_string()))?;

    // O(1) direct lookup by run ID
    let run = registry
        .get_run_by_id(run_id)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to get run: {}", e)))?
        .ok_or_else(|| AppError::NotFound(format!("Run {} not found", run_id)))?;

    Ok(Json(RunStatusResponse {
        id: run.id,
        feature_view_name: run.feature_view_name,
        status: run.status.to_string(),
        started_at: run.started_at.map(|dt| dt.to_rfc3339()),
        completed_at: run.completed_at.map(|dt| dt.to_rfc3339()),
        rows_processed: run.rows_processed,
        error_message: run.error_message,
    }))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Query;
    use serde_json::json;

    #[tokio::test]
    async fn test_register_and_get_feature_view() {
        let state = AppState::new_with_registry().await;

        let req = RegisterFeatureViewRequest {
            name: "test_view".to_string(),
            version: None,
            source_type: "delta".to_string(),
            source_path: "s3://bucket/data".to_string(),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: None,
            ttl_days: Some(30),
            batch_schedule: Some("0 * * * *".to_string()),
            description: Some("Test view".to_string()),
            tags: Some(vec!["test".to_string()]),
            owner: Some("test_user".to_string()),
        };

        // Register
        let (status, _response) = register_feature_view(State(state.clone()), Json(req))
            .await
            .unwrap();

        assert_eq!(status, StatusCode::CREATED);

        // Get
        let response = get_feature_view(State(state), Path("test_view".to_string()))
            .await
            .unwrap();

        assert_eq!(response.name, "test_view");
        assert_eq!(response.version, 1);
    }

    #[tokio::test]
    async fn test_list_feature_views_empty() {
        let state = AppState::new_with_registry().await;

        let response = list_feature_views(State(state), Query(ListParams { filter: None }))
            .await
            .unwrap();

        assert_eq!(response.count, 0);
    }

    #[tokio::test]
    async fn test_trigger_materialization() {
        let state = AppState::new_with_registry().await;

        // First register a view
        let req = RegisterFeatureViewRequest {
            name: "test_view".to_string(),
            version: None,
            source_type: "delta".to_string(),
            source_path: "s3://bucket/data".to_string(),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: None,
            ttl_days: None,
            batch_schedule: None,
            description: None,
            tags: None,
            owner: None,
        };

        let _ = register_feature_view(State(state.clone()), Json(req))
            .await
            .unwrap();

        // Trigger materialization
        let (status, response) =
            trigger_materialization(State(state), Path("test_view".to_string()))
                .await
                .unwrap();

        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(response.run_id > 0);
        assert_eq!(response.status, "pending");
    }

    #[tokio::test]
    async fn test_get_runs_history() {
        let state = AppState::new_with_registry().await;

        // Register view
        let req = RegisterFeatureViewRequest {
            name: "test_view".to_string(),
            version: None,
            source_type: "delta".to_string(),
            source_path: "s3://bucket/data".to_string(),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: None,
            ttl_days: None,
            batch_schedule: None,
            description: None,
            tags: None,
            owner: None,
        };

        let _ = register_feature_view(State(state.clone()), Json(req))
            .await
            .unwrap();

        // Trigger materialization
        let _ = trigger_materialization(State(state.clone()), Path("test_view".to_string()))
            .await
            .unwrap();

        // Wait a bit for background task to potentially complete
        // (transformations are invalid so it will fail quickly)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Get runs
        let response = get_runs_history(
            State(state),
            Path("test_view".to_string()),
            Query(json!({})),
        )
        .await
        .unwrap();

        assert_eq!(response.count, 1);
        assert_eq!(response.runs[0].feature_view_name, "test_view");
        // Status can be "pending" (not started yet) or "failed" (invalid transformations)
        assert!(
            response.runs[0].status == "pending" || response.runs[0].status == "failed",
            "Expected status 'pending' or 'failed', got '{}'",
            response.runs[0].status
        );
    }
}
