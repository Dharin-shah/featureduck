//! HTTP API handlers
//!
//! This module contains all the HTTP endpoint handlers.
//! Each handler is an async function that:
//! 1. Receives a request (with validation via Axum extractors)
//! 2. Processes the request (business logic)
//! 3. Returns a response (or error)
//!
//! ## Milestone 0: Placeholder Implementations
//!
//! For now, these handlers return mock data to validate the HTTP layer works.
//! In Milestone 1+, we'll implement real feature serving logic.

use axum::{
    extract::State,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{error::Result, state::AppState};

// ============================================================================
// Health Check
// ============================================================================

/// Health check response
///
/// Returns basic server status information.
#[derive(Serialize)]
pub struct HealthResponse {
    /// Server status (always "healthy" if we can respond)
    status: String,
    
    /// Server uptime in seconds
    uptime_seconds: u64,
    
    /// Server version
    version: String,
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
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        uptime_seconds: state.uptime(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
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
/// ## Milestone 0
/// Returns mock data for testing. Real implementation in Milestone 2.
pub async fn get_online_features(
    State(_state): State<AppState>,
    Json(request): Json<OnlineFeaturesRequest>,
) -> Result<Json<OnlineFeaturesResponse>> {
    // Start timer to measure latency
    let start = std::time::Instant::now();

    // Log the request
    tracing::info!(
        feature_view = %request.feature_view,
        entity_count = request.entities.len(),
        "Handling online features request"
    );

    // TODO Milestone 2: Implement real feature serving
    // For now, return mock data
    let mock_features: Vec<serde_json::Value> = request
        .entities
        .iter()
        .map(|entity| {
            serde_json::json!({
                "entity": entity,
                "clicks_7d": 42,
                "purchases_30d": 3,
                "_note": "This is mock data from Milestone 0"
            })
        })
        .collect();

    // Calculate latency
    let latency_ms = start.elapsed().as_millis() as u64;

    Ok(Json(OnlineFeaturesResponse {
        features: mock_features.clone(),
        metadata: ResponseMetadata {
            count: mock_features.len(),
            latency_ms,
        },
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
/// ## Milestone 0
/// Returns mock data. Real implementation in Milestone 2.
pub async fn get_historical_features(
    State(_state): State<AppState>,
    Json(request): Json<HistoricalFeaturesRequest>,
) -> Result<Json<OnlineFeaturesResponse>> {
    let start = std::time::Instant::now();

    tracing::info!(
        feature_view = %request.feature_view,
        entity_count = request.entities.len(),
        timestamp = ?request.timestamp,
        "Handling historical features request"
    );

    // TODO Milestone 2: Implement point-in-time queries
    // For now, return same mock data as online features
    let mock_features: Vec<serde_json::Value> = request
        .entities
        .iter()
        .map(|entity| {
            serde_json::json!({
                "entity": entity,
                "clicks_7d": 10,  // Different value to show it's "historical"
                "purchases_30d": 1,
                "_note": "This is mock historical data from Milestone 0",
                "_timestamp": request.timestamp
            })
        })
        .collect();

    let latency_ms = start.elapsed().as_millis() as u64;

    Ok(Json(OnlineFeaturesResponse {
        features: mock_features.clone(),
        metadata: ResponseMetadata {
            count: mock_features.len(),
            latency_ms,
        },
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
///
/// ## Milestone 0
/// Returns mock data. Real implementation in Milestone 1.
pub async fn list_feature_views(
    State(_state): State<AppState>,
) -> Result<Json<FeatureViewsResponse>> {
    tracing::info!("Listing feature views");

    // TODO Milestone 1: Query from registry
    // For now, return empty list
    let mock_views = vec![];

    Ok(Json(FeatureViewsResponse {
        count: mock_views.len(),
        feature_views: mock_views,
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
    async fn test_online_features_mock() {
        let state = AppState::new();
        let request = OnlineFeaturesRequest {
            feature_view: "test_features".to_string(),
            entities: vec![serde_json::json!({"user_id": "123"})],
        };

        let response = get_online_features(State(state), Json(request))
            .await
            .unwrap();

        assert_eq!(response.0.metadata.count, 1);
        assert!(response.0.metadata.latency_ms < 100); // Should be very fast (mock)
    }

    #[tokio::test]
    async fn test_list_feature_views_empty() {
        let state = AppState::new();
        let response = list_feature_views(State(state)).await.unwrap();
        
        // Milestone 0 returns empty list
        assert_eq!(response.0.count, 0);
    }
}
