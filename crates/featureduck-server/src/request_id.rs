//! Request ID middleware for tracing and debugging
//!
//! This module provides request ID generation and propagation for distributed tracing.
//!
//! ## How it works
//!
//! 1. Each incoming request is assigned a unique request ID (UUID v4)
//! 2. If the request already has an `X-Request-ID` header, that ID is used
//! 3. The request ID is added to the response headers
//! 4. The request ID is included in all log entries via tracing span
//!
//! ## Usage
//!
//! ```bash
//! # Request without ID (one will be generated)
//! curl http://localhost:8000/v1/features/online -i
//! # Response includes: X-Request-ID: <generated-uuid>
//!
//! # Request with ID (will be preserved)
//! curl -H "X-Request-ID: my-correlation-id" http://localhost:8000/v1/features/online -i
//! # Response includes: X-Request-ID: my-correlation-id
//! ```

use axum::{
    body::Body,
    extract::Request,
    http::{header::HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use std::str::FromStr;

/// Header name for request ID
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Middleware that assigns and propagates request IDs
///
/// This middleware:
/// 1. Checks for existing `X-Request-ID` header on incoming requests
/// 2. Generates a new UUID v4 if no header is present
/// 3. Adds the request ID to a tracing span for structured logging
/// 4. Includes the request ID in the response headers
///
/// ## Example Log Output
///
/// ```text
/// 2024-01-15T10:30:00Z INFO request{id=abc123-def456} Processing feature request
/// 2024-01-15T10:30:00Z DEBUG request{id=abc123-def456} Reading from Delta table
/// ```
pub async fn request_id_middleware(request: Request<Body>, next: Next) -> Response {
    // Extract or generate request ID
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Create tracing span with request ID
    let span = tracing::info_span!("request", id = %request_id);
    let _guard = span.enter();

    // Log request start
    tracing::debug!(
        method = %request.method(),
        path = %request.uri().path(),
        "Incoming request"
    );

    // Process request
    let mut response = next.run(request).await;

    // Add request ID to response headers
    if let Ok(header_name) = HeaderName::from_str(REQUEST_ID_HEADER) {
        if let Ok(header_value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert(header_name, header_value);
        }
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_header_constant() {
        assert_eq!(REQUEST_ID_HEADER, "x-request-id");
    }

    #[test]
    fn test_uuid_generation() {
        let id1 = uuid::Uuid::new_v4().to_string();
        let id2 = uuid::Uuid::new_v4().to_string();

        // UUIDs should be valid format
        assert_eq!(id1.len(), 36); // UUID format: 8-4-4-4-12
        assert!(id1.contains('-'));

        // UUIDs should be unique
        assert_ne!(id1, id2);
    }
}
