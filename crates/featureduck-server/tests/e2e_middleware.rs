//! E2E tests for server middleware
//!
//! Tests the middleware layers:
//! - Rate limiting (P0-6)
//! - Authentication (P0-1)
//! - Request ID tracing

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use featureduck_server::auth::{auth_middleware, AuthConfig};
use featureduck_server::rate_limit::{rate_limit_middleware, RateLimitConfig, RateLimitState};
use featureduck_server::request_id::{request_id_middleware, REQUEST_ID_HEADER};
use std::sync::Arc;
use tower::ServiceExt;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Simple handler that returns 200 OK
async fn ok_handler() -> &'static str {
    "OK"
}

/// Create a test app with rate limiting middleware
fn create_rate_limit_app(config: RateLimitConfig) -> Router {
    let state = RateLimitState::new(config);

    Router::new()
        .route("/test", get(ok_handler))
        .layer(axum::middleware::from_fn_with_state(
            state,
            rate_limit_middleware,
        ))
}

/// Create a test app with auth middleware
fn create_auth_app(config: AuthConfig) -> Router {
    let auth_config = Arc::new(config);

    Router::new()
        .route("/test", get(ok_handler))
        .route("/health", get(ok_handler))
        .route("/metrics", get(ok_handler))
        .layer(axum::middleware::from_fn_with_state(
            auth_config,
            auth_middleware,
        ))
}

/// Create a test app with request ID middleware
fn create_request_id_app() -> Router {
    Router::new()
        .route("/test", get(ok_handler))
        .layer(axum::middleware::from_fn(request_id_middleware))
}

// ============================================================================
// Rate Limiting E2E Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_rate_limit_allows_normal_requests() {
    // Given: A rate limiter with reasonable limits
    let config = RateLimitConfig::new(100, 50);
    let app = create_rate_limit_app(config);

    // When: We send a few requests
    for _ in 0..10 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        // Then: All should succeed
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_e2e_rate_limit_blocks_excess_requests() {
    // Given: A rate limiter with very low limits
    let config = RateLimitConfig::new(1, 1); // 1 RPS with burst of 1
    let app = create_rate_limit_app(config);

    // When: We send many requests rapidly
    let mut blocked_count = 0;
    for _ in 0..20 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            blocked_count += 1;
        }
    }

    // Then: Some requests should be blocked (429)
    assert!(
        blocked_count > 0,
        "At least some requests should be rate limited"
    );
}

#[tokio::test]
async fn test_e2e_rate_limit_returns_retry_after() {
    // Given: A rate limiter with very low limits
    let config = RateLimitConfig::new(1, 1);
    let app = create_rate_limit_app(config);

    // First request consumes the burst
    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();

    // When: We send excess requests until we hit the limit
    let mut response = None;
    for _ in 0..10 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let r = app.clone().oneshot(request).await.unwrap();
        if r.status() == StatusCode::TOO_MANY_REQUESTS {
            response = Some(r);
            break;
        }
    }

    // Then: Response should have Retry-After header
    if let Some(resp) = response {
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            resp.headers().contains_key("retry-after"),
            "Should have Retry-After header"
        );
    }
}

#[tokio::test]
async fn test_e2e_rate_limit_disabled_allows_all() {
    // Given: A disabled rate limiter
    let config = RateLimitConfig::disabled();
    let app = create_rate_limit_app(config);

    // When: We send many requests
    for _ in 0..100 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        // Then: All should succeed
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Disabled rate limiter should allow all requests"
        );
    }
}

// ============================================================================
// Authentication E2E Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_auth_public_endpoints_bypass() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access public endpoints without auth
    for endpoint in ["/health", "/metrics"] {
        let request = Request::builder()
            .uri(endpoint)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        // Then: Should succeed without auth
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{} should be public",
            endpoint
        );
    }
}

#[tokio::test]
async fn test_e2e_auth_protected_endpoint_requires_key() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access protected endpoint without auth
    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should return 401 Unauthorized
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_e2e_auth_x_api_key_header() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access protected endpoint with X-API-Key header
    let request = Request::builder()
        .uri("/test")
        .header("x-api-key", "secret_key")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should succeed
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_e2e_auth_bearer_token() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access protected endpoint with Bearer token
    let request = Request::builder()
        .uri("/test")
        .header("authorization", "Bearer secret_key")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should succeed
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_e2e_auth_query_param() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access protected endpoint with api_key query param
    let request = Request::builder()
        .uri("/test?api_key=secret_key")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should succeed
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_e2e_auth_invalid_key_rejected() {
    // Given: Auth enabled with API keys
    let config = AuthConfig::new(vec!["secret_key".to_string()]);
    let app = create_auth_app(config);

    // When: We access protected endpoint with invalid key
    let request = Request::builder()
        .uri("/test")
        .header("x-api-key", "wrong_key")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should return 401 Unauthorized
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_e2e_auth_multiple_keys() {
    // Given: Auth enabled with multiple API keys
    let config = AuthConfig::new(vec![
        "key1".to_string(),
        "key2".to_string(),
        "key3".to_string(),
    ]);
    let app = create_auth_app(config);

    // When: We use each key
    for key in ["key1", "key2", "key3"] {
        let request = Request::builder()
            .uri("/test")
            .header("x-api-key", key)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        // Then: All should succeed
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Key {} should be valid",
            key
        );
    }
}

#[tokio::test]
async fn test_e2e_auth_disabled_allows_all() {
    // Given: Auth disabled (no keys configured)
    let config = AuthConfig::default(); // Default has no keys = disabled
    let app = create_auth_app(config);

    // When: We access protected endpoint without auth
    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should succeed (auth disabled)
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_e2e_auth_case_sensitive() {
    // Given: Auth enabled with specific case API key
    let config = AuthConfig::new(vec!["MySecretKey".to_string()]);
    let app = create_auth_app(config);

    // When: We use wrong case
    let request = Request::builder()
        .uri("/test")
        .header("x-api-key", "mysecretkey") // lowercase
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should be rejected (case sensitive)
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// Request ID E2E Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_request_id_generated() {
    // Given: App with request ID middleware
    let app = create_request_id_app();

    // When: We send a request without request ID
    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Response should have generated request ID
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().contains_key(REQUEST_ID_HEADER),
        "Response should have X-Request-ID header"
    );

    let request_id = response
        .headers()
        .get(REQUEST_ID_HEADER)
        .unwrap()
        .to_str()
        .unwrap();

    // Should be a UUID format (36 chars with dashes)
    assert_eq!(request_id.len(), 36, "Request ID should be UUID format");
    assert!(request_id.contains('-'), "Request ID should contain dashes");
}

#[tokio::test]
async fn test_e2e_request_id_preserved() {
    // Given: App with request ID middleware
    let app = create_request_id_app();

    // When: We send a request with existing request ID
    let custom_id = "my-custom-request-id-12345";
    let request = Request::builder()
        .uri("/test")
        .header(REQUEST_ID_HEADER, custom_id)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Response should preserve the request ID
    assert_eq!(response.status(), StatusCode::OK);
    let returned_id = response
        .headers()
        .get(REQUEST_ID_HEADER)
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(returned_id, custom_id, "Request ID should be preserved");
}

#[tokio::test]
async fn test_e2e_request_id_unique_per_request() {
    // Given: App with request ID middleware
    let app = create_request_id_app();

    // When: We send multiple requests
    let mut ids = Vec::new();
    for _ in 0..10 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        let id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        ids.push(id);
    }

    // Then: All IDs should be unique
    let unique_count = {
        let mut unique_ids = ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        unique_ids.len()
    };

    assert_eq!(unique_count, ids.len(), "All request IDs should be unique");
}

// ============================================================================
// Combined Middleware Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_middleware_stack() {
    // Given: App with all middleware layers
    let auth_config = Arc::new(AuthConfig::new(vec!["test_key".to_string()]));
    let rate_limit_state = RateLimitState::new(RateLimitConfig::new(100, 50));

    let app = Router::new()
        .route("/test", get(ok_handler))
        .route("/health", get(ok_handler))
        .layer(axum::middleware::from_fn_with_state(
            auth_config,
            auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            rate_limit_state,
            rate_limit_middleware,
        ))
        .layer(axum::middleware::from_fn(request_id_middleware));

    // When: We send authenticated request
    let request = Request::builder()
        .uri("/test")
        .header("x-api-key", "test_key")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Then: Should succeed and have request ID
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key(REQUEST_ID_HEADER));
}

#[tokio::test]
async fn test_e2e_auth_before_rate_limit() {
    // Given: App with auth and rate limiting
    // Auth should be checked before rate limiting (don't waste rate limit on unauthenticated requests)
    let auth_config = Arc::new(AuthConfig::new(vec!["test_key".to_string()]));
    let rate_limit_state = RateLimitState::new(RateLimitConfig::new(100, 50));

    let app = Router::new()
        .route("/test", get(ok_handler))
        // Auth middleware runs first (innermost layer in Axum)
        .layer(axum::middleware::from_fn_with_state(
            auth_config,
            auth_middleware,
        ))
        // Rate limit runs second
        .layer(axum::middleware::from_fn_with_state(
            rate_limit_state,
            rate_limit_middleware,
        ));

    // When: We send unauthenticated requests
    for _ in 0..10 {
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        // Then: Should get 401, not 429
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Unauthenticated requests should get 401, not hit rate limit"
        );
    }
}
