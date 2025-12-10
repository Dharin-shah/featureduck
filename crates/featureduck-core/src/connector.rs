//! Storage connector trait and related types
//!
//! This module defines the `StorageConnector` trait, which is the core abstraction
//! that allows FeatureDuck to work with any lakehouse format (Delta Lake, Iceberg, etc.).
//!
//! ## Design Philosophy
//!
//! The trait is intentionally minimal - it only defines the operations that are
//! absolutely necessary for feature serving:
//! - Read features for specific entities
//! - Write features to storage
//! - List available feature views
//!
//! This keeps implementations simple and focused.

use crate::{EntityKey, FeatureRow, FeatureView, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// The core trait that all storage backends must implement
///
/// This trait defines the interface between FeatureDuck and the underlying
/// lakehouse storage (Delta Lake, Iceberg, etc.). By programming against this
/// trait, we can easily swap storage backends without changing the rest of the code.
///
/// ## Implementation Notes
///
/// All methods are async because storage operations involve I/O (reading from
/// S3, querying databases, etc.). Rust doesn't have native async traits yet,
/// so we use the `async_trait` macro.
///
/// ## Example Implementation
///
/// ```rust,ignore
/// use featureduck_core::{StorageConnector, FeatureRow, Result};
/// use async_trait::async_trait;
///
/// pub struct MyStorageConnector {
///     // connection details, config, etc.
/// }
///
/// #[async_trait]
/// impl StorageConnector for MyStorageConnector {
///     async fn read_features(&self, ...) -> Result<Vec<FeatureRow>> {
///         // implementation here
///     }
///     // ... other methods
/// }
/// ```
#[async_trait]
pub trait StorageConnector: Send + Sync {
    /// Reads features for the specified entities from a feature view
    ///
    /// This is the primary method for feature retrieval. It takes entity identifiers
    /// and returns the corresponding feature values.
    ///
    /// # Arguments
    ///
    /// * `feature_view` - Name of the feature view to query (e.g., "user_features")
    /// * `entity_keys` - List of entity identifiers to fetch features for
    /// * `as_of` - Optional timestamp for point-in-time queries. If provided, returns
    ///             feature values as they existed at that specific time.
    ///
    /// # Returns
    ///
    /// Vector of FeatureRow, one per entity. The order matches the input entity_keys.
    /// If an entity doesn't exist, the corresponding row will have empty/null features.
    ///
    /// # Errors
    ///
    /// - `FeatureViewNotFound` if the feature view doesn't exist
    /// - `StorageError` if there's an issue reading from storage
    /// - `PointInTimeNotSupported` if as_of is provided but the feature view doesn't
    ///   have a timestamp column
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let entities = vec![
    ///     EntityKey::new("user_id", "123"),
    ///     EntityKey::new("user_id", "456"),
    /// ];
    /// let features = connector.read_features("user_features", entities, None).await?;
    /// ```
    async fn read_features(
        &self,
        feature_view: &str,
        entity_keys: Vec<EntityKey>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Vec<FeatureRow>>;

    /// Writes feature values to storage
    ///
    /// This appends or upserts feature data to the underlying storage.
    /// The exact semantics (append vs upsert) depend on the storage backend.
    ///
    /// # Arguments
    ///
    /// * `feature_view` - Name of the feature view to write to
    /// * `rows` - Feature rows to write
    ///
    /// # Errors
    ///
    /// - `FeatureViewNotFound` if the feature view doesn't exist
    /// - `StorageError` if the write operation fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")])
    ///     .with_feature("clicks_7d", FeatureValue::Int(42));
    /// connector.write_features("user_features", vec![row]).await?;
    /// ```
    async fn write_features(&self, feature_view: &str, rows: Vec<FeatureRow>) -> Result<()>;

    /// Lists all feature views available in this storage backend
    ///
    /// This is used for discovery and validation. It returns the metadata for
    /// all feature views that are available in the storage.
    ///
    /// # Returns
    ///
    /// Vector of FeatureView metadata objects
    ///
    /// # Errors
    ///
    /// - `StorageError` if there's an issue accessing storage
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let views = connector.list_feature_views().await?;
    /// for view in views {
    ///     println!("Found feature view: {}", view.name);
    /// }
    /// ```
    async fn list_feature_views(&self) -> Result<Vec<FeatureView>> {
        Err(crate::Error::StorageError(anyhow::anyhow!(
            "list_feature_views not implemented - use FeatureRegistry instead"
        )))
    }

    /// Gets metadata for a specific feature view
    ///
    /// This is a convenience method that's often more efficient than listing
    /// all views when you only need one.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the feature view
    ///
    /// # Returns
    ///
    /// FeatureView metadata
    ///
    /// # Errors
    ///
    /// - `FeatureViewNotFound` if the feature view doesn't exist
    /// - `StorageError` if there's an issue accessing storage
    async fn get_feature_view(&self, name: &str) -> Result<FeatureView> {
        let _ = name;
        Err(crate::Error::StorageError(anyhow::anyhow!(
            "get_feature_view not implemented - use FeatureRegistry instead"
        )))
    }
}

// Note: We don't provide a default implementation because each storage backend
// has very different requirements. Delta Lake reads parquet files with a transaction
// log, Iceberg uses its metadata layer, etc.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FeatureValue;

    // Mock implementation for testing
    struct MockConnector;

    #[async_trait]
    impl StorageConnector for MockConnector {
        async fn read_features(
            &self,
            _feature_view: &str,
            entity_keys: Vec<EntityKey>,
            _as_of: Option<DateTime<Utc>>,
        ) -> Result<Vec<FeatureRow>> {
            // Return mock data
            let timestamp = Utc::now();
            Ok(entity_keys
                .into_iter()
                .map(|key| {
                    FeatureRow::new(vec![key], timestamp)
                        .with_feature("test_feature", FeatureValue::Int(42))
                })
                .collect())
        }

        async fn write_features(&self, _feature_view: &str, _rows: Vec<FeatureRow>) -> Result<()> {
            Ok(())
        }

        async fn list_feature_views(&self) -> Result<Vec<FeatureView>> {
            Ok(vec![])
        }

        async fn get_feature_view(&self, _name: &str) -> Result<FeatureView> {
            Ok(FeatureView::new(
                "test_view",
                vec!["entity_id".to_string()],
                vec!["test_feature".to_string()],
                "/test/path",
            ))
        }
    }

    #[tokio::test]
    async fn test_mock_connector() {
        let connector = MockConnector;
        let entities = vec![EntityKey::new("user_id", "123")];
        let result = connector.read_features("test", entities, None).await;
        assert!(result.is_ok());

        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].features.get("test_feature"),
            Some(&FeatureValue::Int(42))
        );
    }
}
