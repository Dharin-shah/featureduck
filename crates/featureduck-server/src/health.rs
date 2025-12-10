//! Health Check Module
//!
//! Provides comprehensive health checking for all system components:
//! - Basic liveness checks (is the server running?)
//! - Deep readiness checks (are dependencies healthy?)
//! - Storage backend connectivity
//! - Registry connectivity
//! - DuckDB connection pool status
//!
//! ## Endpoints
//!
//! - `/health` - Basic liveness (fast, always succeeds if server is up)
//! - `/health/ready` - Readiness check (validates all dependencies)
//! - `/health/storage` - Storage backend health
//! - `/health/registry` - Registry health

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::time::Instant;

use crate::state::AppState;

// ============================================================================
// Health Response Types
// ============================================================================

/// Basic health response (liveness)
#[derive(Serialize)]
pub struct LivenessResponse {
    pub status: String,
    pub uptime_seconds: u64,
    pub version: String,
}

/// Detailed health response (readiness)
#[derive(Serialize)]
pub struct ReadinessResponse {
    pub status: String,
    pub uptime_seconds: u64,
    pub version: String,
    pub checks: HealthChecks,
}

/// Individual health checks
#[derive(Serialize)]
pub struct HealthChecks {
    pub storage: ComponentHealth,
    pub registry: ComponentHealth,
}

/// Health status for a single component
#[derive(Serialize)]
pub struct ComponentHealth {
    pub status: ComponentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Component health status
#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Unhealthy,
    NotConfigured,
}

impl ComponentHealth {
    #[allow(dead_code)]
    pub fn healthy(latency_ms: u64) -> Self {
        Self {
            status: ComponentStatus::Healthy,
            latency_ms: Some(latency_ms),
            message: None,
            details: None,
        }
    }

    pub fn healthy_with_details(latency_ms: u64, details: serde_json::Value) -> Self {
        Self {
            status: ComponentStatus::Healthy,
            latency_ms: Some(latency_ms),
            message: None,
            details: Some(details),
        }
    }

    pub fn degraded(message: &str, latency_ms: u64) -> Self {
        Self {
            status: ComponentStatus::Degraded,
            latency_ms: Some(latency_ms),
            message: Some(message.to_string()),
            details: None,
        }
    }

    pub fn unhealthy(message: &str) -> Self {
        Self {
            status: ComponentStatus::Unhealthy,
            latency_ms: None,
            message: Some(message.to_string()),
            details: None,
        }
    }

    pub fn not_configured() -> Self {
        Self {
            status: ComponentStatus::NotConfigured,
            latency_ms: None,
            message: Some("Component not configured".to_string()),
            details: None,
        }
    }
}

// ============================================================================
// Storage Health Response
// ============================================================================

/// Detailed storage health response
#[derive(Serialize)]
pub struct StorageHealthResponse {
    pub status: ComponentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_status: Option<DuckDBHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// DuckDB connection pool health
#[derive(Serialize)]
pub struct DuckDBHealth {
    pub status: ComponentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_usage: Option<String>,
}

/// Circuit breaker health
#[derive(Serialize)]
pub struct CircuitBreakerHealth {
    pub state: String,
    pub failure_count: u32,
    pub success_count: u32,
    pub total_requests: u64,
}

// ============================================================================
// Registry Health Response
// ============================================================================

/// Detailed registry health response
#[derive(Serialize)]
pub struct RegistryHealthResponse {
    pub status: ComponentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature_view_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ============================================================================
// Health Check Handlers
// ============================================================================

/// Liveness check - is the server running?
///
/// This is a fast check that always succeeds if the server is up.
/// Used by Kubernetes liveness probes.
///
/// ## Endpoint
/// `GET /health`
///
/// ## Response
/// ```json
/// {
///   "status": "ok",
///   "uptime_seconds": 123,
///   "version": "0.1.0"
/// }
/// ```
pub async fn liveness(State(state): State<AppState>) -> Json<LivenessResponse> {
    Json(LivenessResponse {
        status: "ok".to_string(),
        uptime_seconds: state.uptime(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Readiness check - are all dependencies healthy?
///
/// This checks all components and returns overall readiness status.
/// Used by Kubernetes readiness probes and load balancers.
///
/// ## Endpoint
/// `GET /health/ready`
///
/// ## Response
/// ```json
/// {
///   "status": "ready",
///   "uptime_seconds": 123,
///   "version": "0.1.0",
///   "checks": {
///     "storage": { "status": "healthy", "latency_ms": 5 },
///     "registry": { "status": "healthy", "latency_ms": 2 }
///   }
/// }
/// ```
///
/// ## HTTP Status
/// - 200 OK: All components healthy
/// - 503 Service Unavailable: One or more components unhealthy
pub async fn readiness(State(state): State<AppState>) -> Response {
    let storage_health = check_storage_health(&state).await;
    let registry_health = check_registry_health(&state).await;

    // Determine overall status
    let all_healthy = storage_health.status == ComponentStatus::Healthy
        || storage_health.status == ComponentStatus::NotConfigured;
    let registry_ok = registry_health.status == ComponentStatus::Healthy
        || registry_health.status == ComponentStatus::NotConfigured;

    let status = if all_healthy && registry_ok {
        "ready"
    } else {
        "not_ready"
    };

    let response = ReadinessResponse {
        status: status.to_string(),
        uptime_seconds: state.uptime(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        checks: HealthChecks {
            storage: storage_health,
            registry: registry_health,
        },
    };

    if status == "ready" {
        (StatusCode::OK, Json(response)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response()
    }
}

/// Storage health check
///
/// Detailed health check for the storage backend including:
/// - DuckDB connection test
/// - Circuit breaker status
/// - Query latency
///
/// ## Endpoint
/// `GET /health/storage`
pub async fn storage_health(State(state): State<AppState>) -> Response {
    let start = Instant::now();

    let Some(storage) = state.storage() else {
        return (
            StatusCode::OK,
            Json(StorageHealthResponse {
                status: ComponentStatus::NotConfigured,
                latency_ms: None,
                base_path: None,
                duckdb_status: None,
                circuit_breaker: None,
                message: Some("Storage not configured".to_string()),
            }),
        )
            .into_response();
    };

    // Get circuit breaker stats
    let cb_stats = storage.circuit_breaker_stats();
    let circuit_breaker = CircuitBreakerHealth {
        state: format!("{:?}", cb_stats.state),
        failure_count: cb_stats.failure_count,
        success_count: cb_stats.success_count,
        total_requests: cb_stats.total_requests,
    };

    // Test DuckDB connectivity with a simple query
    let duckdb_start = Instant::now();
    let duckdb_health = match storage.health_check().await {
        Ok(()) => DuckDBHealth {
            status: ComponentStatus::Healthy,
            query_latency_ms: Some(duckdb_start.elapsed().as_millis() as u64),
            memory_usage: None,
        },
        Err(e) => DuckDBHealth {
            status: ComponentStatus::Unhealthy,
            query_latency_ms: None,
            memory_usage: Some(format!("Error: {}", e)),
        },
    };

    let overall_status = if duckdb_health.status == ComponentStatus::Healthy {
        ComponentStatus::Healthy
    } else {
        ComponentStatus::Unhealthy
    };

    let response = StorageHealthResponse {
        status: overall_status,
        latency_ms: Some(start.elapsed().as_millis() as u64),
        base_path: Some(storage.base_path().to_string()),
        duckdb_status: Some(duckdb_health),
        circuit_breaker: Some(circuit_breaker),
        message: None,
    };

    let status_code = if overall_status == ComponentStatus::Healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(response)).into_response()
}

/// Registry health check
///
/// Detailed health check for the feature registry including:
/// - Database connectivity
/// - Feature view count
///
/// ## Endpoint
/// `GET /health/registry`
pub async fn registry_health(State(state): State<AppState>) -> Response {
    let start = Instant::now();

    let Some(registry) = state.registry() else {
        return (
            StatusCode::OK,
            Json(RegistryHealthResponse {
                status: ComponentStatus::NotConfigured,
                latency_ms: None,
                backend_type: None,
                feature_view_count: None,
                message: Some("Registry not configured".to_string()),
            }),
        )
            .into_response();
    };

    // Test registry connectivity by listing feature views
    match registry.list_feature_views(None).await {
        Ok(views) => {
            let response = RegistryHealthResponse {
                status: ComponentStatus::Healthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                backend_type: Some("sqlite".to_string()), // TODO: Get from registry
                feature_view_count: Some(views.len()),
                message: None,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let response = RegistryHealthResponse {
                status: ComponentStatus::Unhealthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                backend_type: None,
                feature_view_count: None,
                message: Some(format!("Registry error: {}", e)),
            };
            (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response()
        }
    }
}

// ============================================================================
// Internal Health Check Functions
// ============================================================================

async fn check_storage_health(state: &AppState) -> ComponentHealth {
    let Some(storage) = state.storage() else {
        return ComponentHealth::not_configured();
    };

    let start = Instant::now();
    match storage.health_check().await {
        Ok(()) => {
            let latency = start.elapsed().as_millis() as u64;
            if latency > 1000 {
                ComponentHealth::degraded("High latency", latency)
            } else {
                let cb_stats = storage.circuit_breaker_stats();
                ComponentHealth::healthy_with_details(
                    latency,
                    serde_json::json!({
                        "circuit_breaker_state": format!("{:?}", cb_stats.state),
                        "failure_count": cb_stats.failure_count
                    }),
                )
            }
        }
        Err(e) => ComponentHealth::unhealthy(&format!("Storage error: {}", e)),
    }
}

async fn check_registry_health(state: &AppState) -> ComponentHealth {
    let Some(registry) = state.registry() else {
        return ComponentHealth::not_configured();
    };

    let start = Instant::now();
    match registry.list_feature_views(None).await {
        Ok(views) => {
            let latency = start.elapsed().as_millis() as u64;
            if latency > 1000 {
                ComponentHealth::degraded("High latency", latency)
            } else {
                ComponentHealth::healthy_with_details(
                    latency,
                    serde_json::json!({
                        "feature_view_count": views.len()
                    }),
                )
            }
        }
        Err(e) => ComponentHealth::unhealthy(&format!("Registry error: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_component_health_healthy() {
        let health = ComponentHealth::healthy(5);
        assert_eq!(health.status, ComponentStatus::Healthy);
        assert_eq!(health.latency_ms, Some(5));
    }

    #[test]
    fn test_component_health_unhealthy() {
        let health = ComponentHealth::unhealthy("Connection failed");
        assert_eq!(health.status, ComponentStatus::Unhealthy);
        assert_eq!(health.message, Some("Connection failed".to_string()));
    }

    #[test]
    fn test_component_health_not_configured() {
        let health = ComponentHealth::not_configured();
        assert_eq!(health.status, ComponentStatus::NotConfigured);
    }

    #[test]
    fn test_component_health_degraded() {
        let health = ComponentHealth::degraded("Slow response", 1500);
        assert_eq!(health.status, ComponentStatus::Degraded);
        assert_eq!(health.latency_ms, Some(1500));
    }
}
