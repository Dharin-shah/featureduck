//! HTTP API handlers
//!
//! This module contains all the HTTP endpoint handlers.
//! Each handler is an async function that:
//! 1. Receives a request (with validation via Axum extractors)
//! 2. Processes the request (business logic)
//! 3. Returns a response (or error)
//!
//! ## Modules
//!
//! - `registry`: Feature registry management endpoints (M3)
//! - Root module: Feature serving endpoints (M0-M2)

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use featureduck_core::{EntityKey, StorageConnector};
use serde::{Deserialize, Serialize};

use crate::{error::Result, metrics, state::AppState};

// ============================================================================
// Response Limits - Prevent large responses from causing OOM
// ============================================================================

/// Maximum number of entities per online features request (default: 1000)
const MAX_ENTITIES_PER_REQUEST: usize = 1000;

/// Maximum number of rows in API response (default: 10000)
#[allow(dead_code)] // Used for future response pagination
const MAX_RESPONSE_ROWS: usize = 10000;

// Registry endpoints (M3)
pub mod registry;

// Sync endpoints (online store sync)
pub mod sync;

// ============================================================================
// Health Check
// ============================================================================

/// Health check response (legacy, kept for backward compatibility)
///
/// Returns basic server status information.
/// Note: New health checks are in health.rs module
#[allow(dead_code)]
#[derive(Serialize)]
pub struct HealthResponse {
    /// Server status (always "healthy" if we can respond)
    status: String,

    /// Server uptime in seconds
    uptime_seconds: u64,

    /// Server version
    version: String,

    /// Storage backend status (optional, only in deep health check)
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_status: Option<String>,

    /// Registry status (optional, only in deep health check)
    #[serde(skip_serializing_if = "Option::is_none")]
    registry_status: Option<String>,
}

/// Health check endpoint
///
/// This is used by load balancers and monitoring systems to check if
/// the server is running and ready to accept requests.
///
/// ## Endpoint
/// `GET /health`
///
/// ## Response
/// ```json
/// {
///   "status": "healthy",
///   "uptime_seconds": 123,
///   "version": "0.1.0"
/// }
/// ```
///
/// ## HTTP Status
/// - 200 OK: Server is healthy
#[allow(dead_code)]
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    // Check storage backend if configured
    let storage_status = if state.storage().is_some() {
        Some("connected".to_string())
    } else {
        None
    };

    // Check registry if configured
    let registry_status = if state.registry().is_some() {
        Some("connected".to_string())
    } else {
        None
    };

    Json(HealthResponse {
        status: "healthy".to_string(),
        uptime_seconds: state.uptime(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        storage_status,
        registry_status,
    })
}

// ============================================================================
// Metrics (Prometheus)
// ============================================================================

/// Prometheus metrics endpoint
///
/// Exports metrics in Prometheus text format for scraping by Prometheus server.
///
/// ## Endpoint
/// `GET /metrics`
///
/// ## Response
/// Prometheus text format with metrics including:
/// - HTTP request latency (P50, P95, P99)
/// - Request counts (success/error)
/// - Feature read/write operations
/// - Active connections
/// - Error rates
///
/// ## HTTP Status
/// - 200 OK: Metrics exported successfully
/// - 500 Internal Server Error: Failed to export metrics
pub async fn metrics() -> Response {
    match metrics::export_metrics() {
        Ok(body) => (StatusCode::OK, body).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to export metrics: {}", e),
        )
            .into_response(),
    }
}

// ============================================================================
// Online Features
// ============================================================================

/// Request to get online features
///
/// This is the primary request type for real-time feature serving.
#[derive(Debug, Deserialize)]
pub struct OnlineFeaturesRequest {
    /// Name of the feature view to query
    feature_view: String,

    /// Entity identifiers to fetch features for
    /// Example: [{"user_id": "123"}, {"user_id": "456"}]
    entities: Vec<serde_json::Value>,
}

/// Response with online features
#[derive(Debug, Serialize)]
pub struct OnlineFeaturesResponse {
    /// Feature rows (one per entity)
    features: Vec<serde_json::Value>,

    /// Metadata about the response
    metadata: ResponseMetadata,
}

/// Response metadata
#[derive(Debug, Serialize)]
pub struct ResponseMetadata {
    /// Number of rows returned
    count: usize,

    /// Latency in milliseconds
    latency_ms: u64,
}

/// Get online features for real-time serving
///
/// This endpoint returns the latest feature values for the specified entities.
///
/// ## Endpoint
/// `POST /v1/features/online`
///
/// ## Request Body
/// ```json
/// {
///   "feature_view": "user_features",
///   "entities": [
///     {"user_id": "123"},
///     {"user_id": "456"}
///   ]
/// }
/// ```
///
/// ## Response
/// ```json
/// {
///   "features": [
///     {"user_id": "123", "clicks_7d": 42, "purchases_30d": 3},
///     {"user_id": "456", "clicks_7d": 15, "purchases_30d": 1}
///   ],
///   "metadata": {
///     "count": 2,
///     "latency_ms": 5
///   }
/// }
/// ```
///
pub async fn get_online_features(
    State(state): State<AppState>,
    Json(request): Json<OnlineFeaturesRequest>,
) -> Result<Json<OnlineFeaturesResponse>> {
    let start = std::time::Instant::now();

    // Resilience: Limit number of entities per request to prevent OOM
    if request.entities.len() > MAX_ENTITIES_PER_REQUEST {
        return Err(crate::error::AppError::BadRequest(format!(
            "Too many entities: {} exceeds limit of {}. Please batch your requests.",
            request.entities.len(),
            MAX_ENTITIES_PER_REQUEST
        )));
    }

    // Pre-allocate with known capacity for performance
    let mut entity_keys: Vec<EntityKey> = Vec::with_capacity(request.entities.len());
    for entity_obj in &request.entities {
        if let Some(obj) = entity_obj.as_object() {
            if obj.len() == 1 {
                if let Some((key, value)) = obj.iter().next() {
                    if let Some(value_str) = value.as_str() {
                        entity_keys.push(EntityKey::new(key.clone(), value_str.to_string()));
                    }
                }
            }
        }
    }

    if entity_keys.is_empty() {
        return Err(crate::error::AppError::BadRequest(
            "No valid entity keys found in request".to_string(),
        ));
    }

    // Try online store first (Redis/PostgreSQL) for low-latency serving
    // Fall back to offline store (Delta Lake/DuckDB) if online store not available
    let feature_rows = if let Some(online_store) = state.online_store() {
        tracing::info!(
            feature_view = %request.feature_view,
            entity_count = entity_keys.len(),
            store_type = online_store.store_type(),
            "Serving features from online store"
        );

        online_store
            .get_online_features(&request.feature_view, entity_keys)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "Online store read failed, falling back to offline store");
                crate::error::AppError::Internal(format!("Online store error: {}", e))
            })?
    } else if let Some(storage) = state.storage() {
        tracing::info!(
            feature_view = %request.feature_view,
            entity_count = entity_keys.len(),
            "Serving features from offline store (online store not configured)"
        );

        storage
            .read_features(&request.feature_view, entity_keys, None)
            .await
            .map_err(|e| crate::error::AppError::Internal(format!("Failed to read features: {}", e)))?
    } else {
        return Err(crate::error::AppError::Internal(
            "No storage connector configured (neither online nor offline store)".to_string(),
        ));
    };

    let features: Vec<serde_json::Value> = feature_rows
        .iter()
        .map(|row| {
            let mut feature_map = serde_json::Map::new();
            for entity in &row.entities {
                feature_map.insert(entity.name.clone(), serde_json::json!(entity.value));
            }
            for (name, value) in &row.features {
                let json_value = match value {
                    featureduck_core::FeatureValue::Int(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Float(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::String(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Bool(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Json(v) => v.clone(),
                    featureduck_core::FeatureValue::ArrayInt(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::ArrayFloat(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::ArrayString(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Date(v) => serde_json::json!(v.to_string()),
                    featureduck_core::FeatureValue::Null => serde_json::Value::Null,
                };
                feature_map.insert(name.clone(), json_value);
            }
            serde_json::Value::Object(feature_map)
        })
        .collect();

    let latency_ms = start.elapsed().as_millis() as u64;
    let count = features.len();

    Ok(Json(OnlineFeaturesResponse {
        features,
        metadata: ResponseMetadata { count, latency_ms },
    }))
}

// ============================================================================
// Historical Features
// ============================================================================

/// Request to get historical features
///
/// This is used for training data, where we need features as they existed
/// at a specific point in time.
#[derive(Debug, Deserialize)]
pub struct HistoricalFeaturesRequest {
    /// Name of the feature view to query
    feature_view: String,

    /// Entity identifiers
    entities: Vec<serde_json::Value>,

    /// Point-in-time timestamp (ISO 8601 format)
    /// Example: "2024-01-15T10:30:00Z"
    #[serde(default)]
    timestamp: Option<String>,
}

/// Get historical features for training data
///
/// This endpoint returns feature values as they existed at a specific timestamp.
/// This is critical for point-in-time correctness in ML training.
///
/// ## Endpoint
/// `POST /v1/features/historical`
///
/// ## Request Body
/// ```json
/// {
///   "feature_view": "user_features",
///   "entities": [{"user_id": "123"}],
///   "timestamp": "2024-01-01T00:00:00Z"
/// }
/// ```
///
/// ## Response
/// Same format as online features, but with values from the specified timestamp.
///
pub async fn get_historical_features(
    State(state): State<AppState>,
    Json(request): Json<HistoricalFeaturesRequest>,
) -> Result<Json<OnlineFeaturesResponse>> {
    let start = std::time::Instant::now();

    // Resilience: Limit number of entities per request to prevent OOM
    if request.entities.len() > MAX_ENTITIES_PER_REQUEST {
        return Err(crate::error::AppError::BadRequest(format!(
            "Too many entities: {} exceeds limit of {}. Please batch your requests.",
            request.entities.len(),
            MAX_ENTITIES_PER_REQUEST
        )));
    }

    tracing::info!(
        feature_view = %request.feature_view,
        entity_count = request.entities.len(),
        timestamp = ?request.timestamp,
        "Handling historical features request"
    );

    let storage = state.storage().ok_or_else(|| {
        crate::error::AppError::Internal("Storage connector not configured".to_string())
    })?;

    // Pre-allocate with known capacity for performance
    let mut entity_keys: Vec<EntityKey> = Vec::with_capacity(request.entities.len());
    for entity_obj in &request.entities {
        if let Some(obj) = entity_obj.as_object() {
            if obj.len() == 1 {
                if let Some((key, value)) = obj.iter().next() {
                    if let Some(value_str) = value.as_str() {
                        entity_keys.push(EntityKey::new(key.clone(), value_str.to_string()));
                    }
                }
            }
        }
    }

    if entity_keys.is_empty() {
        return Err(crate::error::AppError::BadRequest(
            "No valid entity keys found in request".to_string(),
        ));
    }

    let as_of_time = if let Some(ts_str) = request.timestamp {
        Some(
            chrono::DateTime::parse_from_rfc3339(&ts_str)
                .map_err(|e| {
                    crate::error::AppError::BadRequest(format!("Invalid timestamp: {}", e))
                })?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let feature_rows = storage
        .read_features(&request.feature_view, entity_keys, as_of_time)
        .await
        .map_err(|e| crate::error::AppError::Internal(format!("Failed to read features: {}", e)))?;

    let features: Vec<serde_json::Value> = feature_rows
        .iter()
        .map(|row| {
            let mut feature_map = serde_json::Map::new();
            for entity in &row.entities {
                feature_map.insert(entity.name.clone(), serde_json::json!(entity.value));
            }
            for (name, value) in &row.features {
                let json_value = match value {
                    featureduck_core::FeatureValue::Int(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Float(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::String(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Bool(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Json(v) => v.clone(),
                    featureduck_core::FeatureValue::ArrayInt(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::ArrayFloat(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::ArrayString(v) => serde_json::json!(v),
                    featureduck_core::FeatureValue::Date(v) => serde_json::json!(v.to_string()),
                    featureduck_core::FeatureValue::Null => serde_json::Value::Null,
                };
                feature_map.insert(name.clone(), json_value);
            }
            if let Some(ts) = as_of_time {
                feature_map.insert(
                    "_as_of_timestamp".to_string(),
                    serde_json::json!(ts.to_rfc3339()),
                );
            }
            serde_json::Value::Object(feature_map)
        })
        .collect();

    let latency_ms = start.elapsed().as_millis() as u64;
    let count = features.len();

    Ok(Json(OnlineFeaturesResponse {
        features,
        metadata: ResponseMetadata { count, latency_ms },
    }))
}

// ============================================================================
// Feature Views Management
// ============================================================================

/// Response listing available feature views
#[derive(Debug, Serialize)]
pub struct FeatureViewsResponse {
    /// List of feature view names
    feature_views: Vec<String>,

    /// Total count
    count: usize,
}

/// List all available feature views
///
/// This endpoint returns a list of all feature views registered in the system.
///
/// ## Endpoint
/// `GET /v1/features/views`
///
/// ## Response
/// ```json
/// {
///   "feature_views": ["user_features", "product_features"],
///   "count": 2
/// }
/// ```
pub async fn list_feature_views(
    State(state): State<AppState>,
) -> Result<Json<FeatureViewsResponse>> {
    tracing::info!("Listing feature views");

    // P0-5: Use registry to list feature views
    let views: Vec<String> = if let Some(registry) = state.registry() {
        registry
            .list_feature_views(None)
            .await
            .map_err(|e| {
                crate::error::AppError::Internal(format!("Failed to list feature views: {}", e))
            })?
            .into_iter()
            .map(|v| v.name)
            .collect()
    } else {
        vec![]
    };

    Ok(Json(FeatureViewsResponse {
        count: views.len(),
        feature_views: views,
    }))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = AppState::new();
        let response = health(State(state)).await;

        assert_eq!(response.0.status, "healthy");
        assert_eq!(response.0.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_online_features_without_storage() {
        let state = AppState::new();
        let request = OnlineFeaturesRequest {
            feature_view: "test_features".to_string(),
            entities: vec![serde_json::json!({"user_id": "123"})],
        };

        let result = get_online_features(State(state), Json(request)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_feature_views_empty() {
        // Use state with registry (P0-5)
        let state = AppState::new_with_registry().await;
        let response = list_feature_views(State(state)).await.unwrap();

        // Returns empty list when no feature views registered
        assert_eq!(response.0.count, 0);
    }
}
