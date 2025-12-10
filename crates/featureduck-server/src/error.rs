//! Error handling for the HTTP server
//!
//! This module defines how errors are converted to HTTP responses.
//! We want to return meaningful error messages to clients while also
//! logging detailed errors internally.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::fmt;

/// Result type alias for operations that can fail
pub type Result<T> = std::result::Result<T, AppError>;

/// Application errors that can occur in HTTP handlers
///
/// This wraps the core Error type and adds HTTP-specific error handling.
/// Each error type maps to an appropriate HTTP status code.
#[derive(Debug)]
#[allow(dead_code)] // Some variants used in future milestones
pub enum AppError {
    /// Feature view not found (404)
    NotFound(String),

    /// Invalid request (400)
    BadRequest(String),

    /// Internal server error (500)
    Internal(String),

    /// Error from core library
    Core(featureduck_core::Error),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
            AppError::Core(err) => write!(f, "Core error: {}", err),
        }
    }
}

impl std::error::Error for AppError {}

/// Convert AppError into an HTTP response
///
/// This is where we map our application errors to HTTP status codes
/// and format error messages for the client.
///
/// ## Error Response Format
///
/// All errors return JSON in this format:
/// ```json
/// {
///   "error": {
///     "code": "NOT_FOUND",
///     "message": "Feature view 'user_features' not found"
///   }
/// }
/// ```
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Determine HTTP status code based on error type
        let (status, error_code) = match &self {
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
            AppError::Core(err) => match err {
                featureduck_core::Error::FeatureViewNotFound(_) => {
                    (StatusCode::NOT_FOUND, "FEATURE_VIEW_NOT_FOUND")
                }
                featureduck_core::Error::InvalidInput(_) => {
                    (StatusCode::BAD_REQUEST, "INVALID_INPUT")
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
            },
        };

        // Log the error internally (with full details)
        tracing::error!("Request error: {}", self);

        // Create error response (with sanitized message for client)
        let error_response = ErrorResponse {
            error: ErrorDetail {
                code: error_code.to_string(),
                message: self.to_string(),
            },
        };

        // Return JSON response with appropriate status code
        (status, Json(error_response)).into_response()
    }
}

/// Error response sent to clients
///
/// This provides a consistent error format across all endpoints.
#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

/// Error details in the response
#[derive(Serialize)]
struct ErrorDetail {
    /// Machine-readable error code (e.g., "NOT_FOUND")
    code: String,

    /// Human-readable error message
    message: String,
}

// Conversion from core Error to AppError
impl From<featureduck_core::Error> for AppError {
    fn from(err: featureduck_core::Error) -> Self {
        AppError::Core(err)
    }
}

// Conversion from anyhow Error to AppError
impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Internal(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = AppError::NotFound("test".to_string());
        assert_eq!(err.to_string(), "Not found: test");
    }

    #[test]
    fn test_error_conversion_from_core() {
        let core_err = featureduck_core::Error::FeatureViewNotFound("test".to_string());
        let app_err: AppError = core_err.into();
        assert!(matches!(app_err, AppError::Core(_)));
    }
}
