//! Sync endpoints for Delta Lake → Online Store sync
//!
//! These endpoints trigger sync jobs to populate the online store
//! from the offline store (Delta Lake).
//!
//! ## Endpoints
//!
//! - `POST /v1/admin/sync` - Trigger sync for a feature view

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::{error::Result, state::AppState};

/// Request to sync a feature view to the online store
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields read when online feature is enabled
pub struct SyncRequest {
    /// Name of the feature view to sync
    pub feature_view: String,

    /// Path to the Delta Lake table (optional, will use registry if not provided)
    #[serde(default)]
    pub delta_path: Option<String>,

    /// Entity columns (required for sync)
    pub entity_columns: Vec<String>,

    /// Optional TTL for features in seconds
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// Sync response with metrics
#[derive(Debug, Serialize)]
pub struct SyncResponse {
    /// Status of the sync
    pub status: String,
    /// Number of rows synced
    pub rows_synced: usize,
    /// Duration of sync in milliseconds
    pub duration_ms: u64,
    /// Rows per second
    pub rows_per_sec: f64,
    /// Online store type
    pub store_type: String,
}

/// Trigger sync from Delta Lake to online store
///
/// This endpoint syncs the latest features from the offline store (Delta Lake)
/// to the online store (Redis/PostgreSQL) for low-latency serving.
///
/// ## Endpoint
/// `POST /v1/admin/sync`
///
/// ## Request Body
/// ```json
/// {
///   "feature_view": "user_features",
///   "delta_path": "s3://bucket/features/user_features",
///   "entity_columns": ["user_id"],
///   "ttl_seconds": 86400
/// }
/// ```
///
/// ## Response
/// ```json
/// {
///   "status": "success",
///   "rows_synced": 100000,
///   "duration_ms": 1234,
///   "rows_per_sec": 81037.0,
///   "store_type": "redis"
/// }
/// ```
#[cfg(feature = "online")]
pub async fn sync_to_online_store(
    State(state): State<AppState>,
    Json(request): Json<SyncRequest>,
) -> Result<Json<SyncResponse>> {
    use featureduck_online::{sync_to_online, SyncConfig};

    let online_store = state.online_store().ok_or_else(|| {
        crate::error::AppError::BadRequest(
            "Online store not configured. Enable online feature and configure Redis/PostgreSQL."
                .to_string(),
        )
    })?;

    // Get delta path from request or registry
    let delta_path = if let Some(path) = request.delta_path {
        path
    } else if let Some(registry) = state.registry() {
        let view = registry
            .get_feature_view(&request.feature_view)
            .await
            .map_err(|e| {
                crate::error::AppError::NotFound(format!(
                    "Feature view '{}' not found in registry: {}",
                    request.feature_view, e
                ))
            })?;
        view.source_path
    } else {
        return Err(crate::error::AppError::BadRequest(
            "delta_path is required when registry is not configured".to_string(),
        ));
    };

    let entity_columns: Vec<&str> = request.entity_columns.iter().map(|s| s.as_str()).collect();

    let config = SyncConfig {
        ttl: request
            .ttl_seconds
            .map(|s| std::time::Duration::from_secs(s)),
        ..Default::default()
    };

    tracing::info!(
        feature_view = %request.feature_view,
        delta_path = %delta_path,
        entity_columns = ?entity_columns,
        store_type = online_store.store_type(),
        "Starting sync to online store"
    );

    let result = sync_to_online(
        online_store.as_ref(),
        &delta_path,
        &request.feature_view,
        &entity_columns,
        config,
    )
    .await
    .map_err(|e| crate::error::AppError::Internal(format!("Sync failed: {}", e)))?;

    Ok(Json(SyncResponse {
        status: "success".to_string(),
        rows_synced: result.rows_synced,
        duration_ms: result.duration.as_millis() as u64,
        rows_per_sec: result.rows_per_sec,
        store_type: online_store.store_type().to_string(),
    }))
}

/// Placeholder for when online feature is not enabled
#[cfg(not(feature = "online"))]
pub async fn sync_to_online_store(
    State(_state): State<AppState>,
    Json(_request): Json<SyncRequest>,
) -> Result<Json<SyncResponse>> {
    Err(crate::error::AppError::BadRequest(
        "Online store feature not enabled. Rebuild server with --features online".to_string(),
    ))
}
