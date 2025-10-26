//! Error types for FeatureDuck
//!
//! This module defines all error types that can occur in the feature store.
//! We use the `thiserror` crate to make error definitions concise and ergonomic.
//!
//! ## Design Philosophy
//!
//! - Errors should be descriptive and actionable
//! - Use strongly-typed errors (not just strings)
//! - Provide context for debugging
//! - Make it easy to convert between error types

use thiserror::Error;

/// Result type alias for operations that can fail
///
/// This is a convenience type that uses our custom Error type.
/// Instead of writing `Result<T, Error>` everywhere, we can just write `Result<T>`.
pub type Result<T> = std::result::Result<T, Error>;

/// All possible errors that can occur in FeatureDuck
///
/// Each variant represents a different category of error with relevant context.
/// The `#[error(...)]` attribute defines the display message for each error.
#[derive(Error, Debug)]
pub enum Error {
    /// Feature view was not found in the registry
    ///
    /// This happens when trying to query a feature view that hasn't been registered.
    #[error("Feature view '{0}' not found")]
    FeatureViewNotFound(String),
    
    /// Entity column is missing from the query
    ///
    /// Feature views require specific entity columns. This error occurs when
    /// a required entity column is not provided in the query.
    #[error("Entity column '{0}' is required but was not provided")]
    MissingEntityColumn(String),
    
    /// Feature column is missing or invalid
    #[error("Feature column '{0}' not found in feature view '{1}'")]
    FeatureColumnNotFound(String, String),
    
    /// Storage backend error (e.g., Delta Lake, S3 access)
    ///
    /// This is a catch-all for errors from the underlying storage layer.
    /// We wrap the original error to preserve context.
    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),
    
    /// Database query error (DuckDB)
    ///
    /// Errors from DuckDB query execution.
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    /// Serialization/deserialization error
    ///
    /// Occurs when converting between formats (JSON, Arrow, etc.)
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    /// Invalid configuration
    ///
    /// Configuration file or environment variable issues.
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    /// Invalid input from user
    ///
    /// Used for validation errors (e.g., empty entity list, invalid timestamp)
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    /// Point-in-time query not supported
    ///
    /// Some feature views don't have timestamp columns and can't do time-travel.
    #[error("Point-in-time queries not supported for feature view '{0}' (no timestamp column)")]
    PointInTimeNotSupported(String),
    
    /// Internal error - this should rarely happen
    ///
    /// Used for unexpected errors that indicate a bug in our code.
    #[error("Internal error: {0}")]
    InternalError(String),
}

// Helper implementations to make error creation more ergonomic

impl Error {
    /// Creates a DatabaseError from any error type
    ///
    /// This is useful for converting DuckDB errors into our error type.
    pub fn database<E: std::error::Error>(err: E) -> Self {
        Self::DatabaseError(err.to_string())
    }
    
    /// Creates a ConfigError from a string
    pub fn config(msg: impl Into<String>) -> Self {
        Self::ConfigError(msg.into())
    }
    
    /// Creates an InvalidInput error from a string
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }
    
    /// Creates an InternalError from a string
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::InternalError(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::FeatureViewNotFound("user_features".to_string());
        assert_eq!(err.to_string(), "Feature view 'user_features' not found");
    }

    #[test]
    fn test_error_helpers() {
        let err = Error::config("Missing required field");
        assert!(matches!(err, Error::ConfigError(_)));
        
        let err = Error::invalid_input("Empty entity list");
        assert!(matches!(err, Error::InvalidInput(_)));
    }
}
