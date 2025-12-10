//! Feature Registry for FeatureDuck
//!
//! This module provides persistent storage for feature view metadata and
//! materialization run tracking with support for multiple backends:
//! - **SQLite with WAL** - Embedded, multi-process safe (<10 workers)
//! - **PostgreSQL** - Production scale, connection pooling (100+ workers)
//!
//! # Examples
//!
//! ```rust,ignore
//! use featureduck_registry::{FeatureRegistry, RegistryConfig, FeatureViewDef, RunStatus};
//! use chrono::Utc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // SQLite backend (embedded)
//!     let config = RegistryConfig::sqlite("./registry.db");
//!     let registry = FeatureRegistry::new(config).await?;
//!
//!     // PostgreSQL backend (production)
//!     // let config = RegistryConfig::postgres("postgresql://user:pass@localhost/featureduck");
//!     // let registry = FeatureRegistry::new(config).await?;
//!
//!     // Register feature view
//!     let view = FeatureViewDef {
//!         name: "user_features".to_string(),
//!         // ... other fields
//!     };
//!     registry.register_feature_view(&view).await?;
//!
//!     // Track materialization run
//!     let run_id = registry.create_run("user_features").await?;
//!     registry.update_run_status(run_id, RunStatus::Success, Some(1000), None).await?;
//!
//!     Ok(())
//! }
//! ```

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export backend types
pub mod backend;
pub use backend::{RegistryBackend, RegistryConfig};

// Backend implementations
mod schema;
mod sqlite_backend;

#[cfg(feature = "postgres")]
mod postgres_backend;

pub use schema::RunStatus;
pub use sqlite_backend::SqliteBackend;

#[cfg(feature = "postgres")]
pub use postgres_backend::PostgresBackend;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewDef {
    pub name: String,
    pub version: i32,
    pub source_type: String,
    pub source_path: String,
    pub entities: Vec<String>,
    pub transformations: String,
    pub timestamp_field: Option<String>,
    pub ttl_days: Option<i32>,
    pub batch_schedule: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub owner: Option<String>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewRun {
    pub id: i64,
    pub feature_view_name: String,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub rows_processed: Option<i64>,
    pub error_message: Option<String>,
}

/// Statistics captured during feature materialization
///
/// These stats enable adaptive execution by providing cost estimation data
/// for choosing between DuckDB (fast for small data) and Spark (scales for large data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewStats {
    pub feature_view: String,
    pub version: i32,
    pub row_count: i64,
    pub distinct_entities: i64,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub avg_file_size_bytes: Option<i64>,
    pub total_size_bytes: i64,
    pub histogram_buckets: Option<String>, // JSON histogram data
    #[serde(with = "chrono::serde::ts_seconds")]
    pub created_at: DateTime<Utc>,
}

/// Feature Registry with pluggable backend
///
/// Supports SQLite (embedded) and PostgreSQL (production scale) backends.
pub struct FeatureRegistry {
    backend: Arc<dyn RegistryBackend>,
}

impl FeatureRegistry {
    /// Create new registry with specified backend configuration
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // SQLite (embedded, multi-process safe)
    /// let config = RegistryConfig::sqlite("./registry.db");
    /// let registry = FeatureRegistry::new(config).await?;
    ///
    /// // PostgreSQL (production, connection pooling)
    /// let config = RegistryConfig::postgres("postgresql://user:pass@localhost/featureduck");
    /// let registry = FeatureRegistry::new(config).await?;
    ///
    /// // PostgreSQL with custom pool settings
    /// let config = RegistryConfig::postgres_with_pool(
    ///     "postgresql://user:pass@localhost/featureduck",
    ///     20,  // pool_size
    ///     60,  // timeout_seconds
    /// );
    /// let registry = FeatureRegistry::new(config).await?;
    /// ```
    pub async fn new(config: RegistryConfig) -> Result<Self> {
        let backend: Arc<dyn RegistryBackend> = match config {
            RegistryConfig::SQLite { path } => {
                let backend = SqliteBackend::new(&path)?;
                Arc::new(backend)
            }

            #[cfg(feature = "postgres")]
            RegistryConfig::PostgreSQL {
                connection_string,
                pool_size,
                timeout_seconds,
                statement_timeout_seconds: _,
            } => {
                let backend =
                    PostgresBackend::new(&connection_string, pool_size, timeout_seconds).await?;
                Arc::new(backend)
            }
        };

        // Initialize schema
        backend.init_schema().await?;

        Ok(Self { backend })
    }

    /// Create in-memory registry (for testing)
    pub async fn in_memory() -> Result<Self> {
        let backend = SqliteBackend::in_memory()?;
        let registry = Self {
            backend: Arc::new(backend),
        };

        // Initialize schema
        registry.backend.init_schema().await?;

        Ok(registry)
    }

    /// Register or update a feature view
    pub async fn register_feature_view(&self, view: &FeatureViewDef) -> Result<()> {
        self.backend.register_feature_view(view).await
    }

    /// Get feature view by name
    pub async fn get_feature_view(&self, name: &str) -> Result<FeatureViewDef> {
        self.backend.get_feature_view(name).await
    }

    /// List all feature views (with optional filter)
    pub async fn list_feature_views(&self, filter: Option<&str>) -> Result<Vec<FeatureViewDef>> {
        self.backend.list_feature_views(filter).await
    }

    /// Delete feature view
    pub async fn delete_feature_view(&self, name: &str) -> Result<()> {
        self.backend.delete_feature_view(name).await
    }

    /// Create a new materialization run
    pub async fn create_run(&self, feature_view_name: &str) -> Result<i64> {
        self.backend.create_run(feature_view_name).await
    }

    /// Update run status
    pub async fn update_run_status(
        &self,
        run_id: i64,
        status: RunStatus,
        rows_processed: Option<i64>,
        error_message: Option<&str>,
    ) -> Result<()> {
        self.backend
            .update_run_status(run_id, status, rows_processed, error_message)
            .await
    }

    /// Get recent runs for a feature view
    pub async fn get_recent_runs(
        &self,
        feature_view_name: &str,
        limit: usize,
    ) -> Result<Vec<FeatureViewRun>> {
        self.backend.get_recent_runs(feature_view_name, limit).await
    }

    /// Get a specific run by ID (O(1) lookup)
    ///
    /// More efficient than iterating through all runs when you know the run ID.
    /// Returns None if run doesn't exist.
    pub async fn get_run_by_id(&self, run_id: i64) -> Result<Option<FeatureViewRun>> {
        self.backend.get_run_by_id(run_id).await
    }

    /// Save feature view statistics
    ///
    /// This captures stats during materialization for adaptive execution planning.
    /// Stats include row counts, entity cardinality, data size, and time ranges.
    pub async fn save_stats(&self, stats: &FeatureViewStats) -> Result<()> {
        self.backend.save_stats(stats).await
    }

    /// Get latest statistics for a feature view
    ///
    /// Returns the most recent stats captured during materialization.
    /// Used for adaptive engine selection (DuckDB vs Spark).
    pub async fn get_latest_stats(&self, feature_view: &str) -> Result<Option<FeatureViewStats>> {
        self.backend.get_latest_stats(feature_view).await
    }

    /// Get backend-specific statistics (if available)
    ///
    /// For PostgreSQL: returns connection pool stats
    /// For SQLite: returns None
    #[cfg(feature = "postgres")]
    pub fn pool_stats(&self) -> Option<deadpool_postgres::Status> {
        // Try to downcast to PostgresBackend
        // This is safe because we control the backend creation
        use std::any::Any;
        if let Some(pg_backend) =
            (self.backend.as_ref() as &dyn Any).downcast_ref::<PostgresBackend>()
        {
            Some(pg_backend.pool_stats())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_view(name: &str) -> FeatureViewDef {
        FeatureViewDef {
            name: name.to_string(),
            version: 1,
            source_type: "delta".to_string(),
            source_path: "s3://bucket/data".to_string(),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: Some("timestamp".to_string()),
            ttl_days: Some(30),
            batch_schedule: Some("0 0 * * *".to_string()),
            description: Some("Test view".to_string()),
            tags: vec!["test".to_string()],
            owner: Some("test_user".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    // FeatureViewDef tests
    #[test]
    fn test_feature_view_def_creation() {
        let view = create_test_view("my_features");
        assert_eq!(view.name, "my_features");
        assert_eq!(view.version, 1);
        assert_eq!(view.source_type, "delta");
        assert_eq!(view.source_path, "s3://bucket/data");
        assert_eq!(view.entities, vec!["user_id"]);
        assert_eq!(view.timestamp_field, Some("timestamp".to_string()));
        assert_eq!(view.ttl_days, Some(30));
        assert_eq!(view.owner, Some("test_user".to_string()));
    }

    #[test]
    fn test_feature_view_def_serialization() {
        let view = create_test_view("serialization_test");
        let json = serde_json::to_string(&view).unwrap();
        let deserialized: FeatureViewDef = serde_json::from_str(&json).unwrap();

        assert_eq!(view.name, deserialized.name);
        assert_eq!(view.version, deserialized.version);
        assert_eq!(view.entities, deserialized.entities);
        assert_eq!(view.tags, deserialized.tags);
    }

    #[test]
    fn test_feature_view_def_with_multiple_entities() {
        let view = FeatureViewDef {
            name: "user_item_features".to_string(),
            version: 2,
            source_type: "parquet".to_string(),
            source_path: "gs://bucket/events".to_string(),
            entities: vec!["user_id".to_string(), "item_id".to_string()],
            transformations: r#"{"agg": "sum"}"#.to_string(),
            timestamp_field: None,
            ttl_days: None,
            batch_schedule: None,
            description: None,
            tags: vec![],
            owner: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        assert_eq!(view.entities.len(), 2);
        assert!(view.entities.contains(&"user_id".to_string()));
        assert!(view.entities.contains(&"item_id".to_string()));
    }

    // FeatureViewRun tests
    #[test]
    fn test_feature_view_run_creation() {
        let run = FeatureViewRun {
            id: 42,
            feature_view_name: "test_view".to_string(),
            status: RunStatus::Pending,
            started_at: None,
            completed_at: None,
            rows_processed: None,
            error_message: None,
        };

        assert_eq!(run.id, 42);
        assert_eq!(run.feature_view_name, "test_view");
        assert!(matches!(run.status, RunStatus::Pending));
    }

    #[test]
    fn test_feature_view_run_with_error() {
        let run = FeatureViewRun {
            id: 1,
            feature_view_name: "failing_view".to_string(),
            status: RunStatus::Failed,
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            rows_processed: Some(500),
            error_message: Some("Connection timeout".to_string()),
        };

        assert!(matches!(run.status, RunStatus::Failed));
        assert!(run.error_message.is_some());
        assert_eq!(run.error_message.unwrap(), "Connection timeout");
    }

    #[test]
    fn test_feature_view_run_serialization() {
        let run = FeatureViewRun {
            id: 99,
            feature_view_name: "serialization_test".to_string(),
            status: RunStatus::Success,
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            rows_processed: Some(10000),
            error_message: None,
        };

        let json = serde_json::to_string(&run).unwrap();
        let deserialized: FeatureViewRun = serde_json::from_str(&json).unwrap();

        assert_eq!(run.id, deserialized.id);
        assert_eq!(run.feature_view_name, deserialized.feature_view_name);
        assert_eq!(run.rows_processed, deserialized.rows_processed);
    }

    // FeatureViewStats tests
    #[test]
    fn test_feature_view_stats_creation() {
        let stats = FeatureViewStats {
            feature_view: "user_features".to_string(),
            version: 1,
            row_count: 1_000_000,
            distinct_entities: 100_000,
            min_timestamp: Some(1700000000),
            max_timestamp: Some(1700100000),
            avg_file_size_bytes: Some(10_000_000),
            total_size_bytes: 500_000_000,
            histogram_buckets: Some(r#"{"0-1000": 500, "1000-2000": 300}"#.to_string()),
            created_at: Utc::now(),
        };

        assert_eq!(stats.feature_view, "user_features");
        assert_eq!(stats.row_count, 1_000_000);
        assert_eq!(stats.distinct_entities, 100_000);
        assert_eq!(stats.total_size_bytes, 500_000_000);
    }

    #[test]
    fn test_feature_view_stats_serialization() {
        let stats = FeatureViewStats {
            feature_view: "serialization_test".to_string(),
            version: 2,
            row_count: 50000,
            distinct_entities: 5000,
            min_timestamp: None,
            max_timestamp: None,
            avg_file_size_bytes: None,
            total_size_bytes: 10_000_000,
            histogram_buckets: None,
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: FeatureViewStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.feature_view, deserialized.feature_view);
        assert_eq!(stats.row_count, deserialized.row_count);
        assert_eq!(stats.total_size_bytes, deserialized.total_size_bytes);
    }

    // RunStatus tests
    #[test]
    fn test_run_status_variants() {
        let pending = RunStatus::Pending;
        let running = RunStatus::Running;
        let success = RunStatus::Success;
        let failed = RunStatus::Failed;

        assert!(matches!(pending, RunStatus::Pending));
        assert!(matches!(running, RunStatus::Running));
        assert!(matches!(success, RunStatus::Success));
        assert!(matches!(failed, RunStatus::Failed));
    }

    // RegistryConfig tests
    #[test]
    fn test_registry_config_sqlite() {
        let config = RegistryConfig::sqlite("./test.db");
        match config {
            RegistryConfig::SQLite { path } => assert_eq!(path, "./test.db"),
            #[cfg(feature = "postgres")]
            _ => panic!("Expected SQLite config"),
        }
    }

    #[test]
    fn test_registry_config_sqlite_memory() {
        let config = RegistryConfig::sqlite(":memory:");
        match config {
            RegistryConfig::SQLite { path } => assert_eq!(path, ":memory:"),
            #[cfg(feature = "postgres")]
            _ => panic!("Expected SQLite config"),
        }
    }

    // SQLite backend tests
    #[tokio::test]
    async fn test_sqlite_register_and_get() {
        let config = RegistryConfig::SQLite {
            path: ":memory:".to_string(),
        };
        let registry = FeatureRegistry::new(config).await.unwrap();
        let view = create_test_view("test_view");

        registry.register_feature_view(&view).await.unwrap();

        let retrieved = registry.get_feature_view("test_view").await.unwrap();
        assert_eq!(retrieved.name, "test_view");
        assert_eq!(retrieved.entities, vec!["user_id"]);
    }

    #[tokio::test]
    async fn test_sqlite_create_and_track_run() {
        let config = RegistryConfig::SQLite {
            path: ":memory:".to_string(),
        };
        let registry = FeatureRegistry::new(config).await.unwrap();
        let view = create_test_view("test_view");
        registry.register_feature_view(&view).await.unwrap();

        let run_id = registry.create_run("test_view").await.unwrap();
        assert!(run_id > 0);

        registry
            .update_run_status(run_id, RunStatus::Running, None, None)
            .await
            .unwrap();

        registry
            .update_run_status(run_id, RunStatus::Success, Some(1000), None)
            .await
            .unwrap();

        let runs = registry.get_recent_runs("test_view", 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert!(matches!(runs[0].status, RunStatus::Success));
        assert_eq!(runs[0].rows_processed, Some(1000));
    }

    #[tokio::test]
    async fn test_get_run_by_id() {
        let config = RegistryConfig::SQLite {
            path: ":memory:".to_string(),
        };
        let registry = FeatureRegistry::new(config).await.unwrap();
        let view = create_test_view("test_view");
        registry.register_feature_view(&view).await.unwrap();

        // Create a run
        let run_id = registry.create_run("test_view").await.unwrap();

        // Get by ID (O(1) lookup)
        let run = registry.get_run_by_id(run_id).await.unwrap();
        assert!(run.is_some());
        let run = run.unwrap();
        assert_eq!(run.id, run_id);
        assert_eq!(run.feature_view_name, "test_view");
        assert!(matches!(run.status, RunStatus::Pending));

        // Update status and verify
        registry
            .update_run_status(run_id, RunStatus::Success, Some(500), None)
            .await
            .unwrap();

        let updated_run = registry.get_run_by_id(run_id).await.unwrap().unwrap();
        assert!(matches!(updated_run.status, RunStatus::Success));
        assert_eq!(updated_run.rows_processed, Some(500));

        // Non-existent run should return None
        let missing = registry.get_run_by_id(99999).await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_registry() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let view = create_test_view("memory_test");

        registry.register_feature_view(&view).await.unwrap();
        let retrieved = registry.get_feature_view("memory_test").await.unwrap();
        assert_eq!(retrieved.name, "memory_test");
    }

    #[tokio::test]
    async fn test_list_feature_views() {
        let registry = FeatureRegistry::in_memory().await.unwrap();

        // Register multiple views
        for name in ["user_features", "item_features", "user_item_features"] {
            let view = create_test_view(name);
            registry.register_feature_view(&view).await.unwrap();
        }

        // List all
        let all_views = registry.list_feature_views(None).await.unwrap();
        assert_eq!(all_views.len(), 3);

        // List with filter
        let user_views = registry.list_feature_views(Some("user")).await.unwrap();
        assert_eq!(user_views.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_feature_view() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let view = create_test_view("to_delete");

        registry.register_feature_view(&view).await.unwrap();

        // Verify it exists
        let retrieved = registry.get_feature_view("to_delete").await;
        assert!(retrieved.is_ok());

        // Delete
        registry.delete_feature_view("to_delete").await.unwrap();

        // Verify it's gone
        let deleted = registry.get_feature_view("to_delete").await;
        assert!(deleted.is_err());
    }

    #[tokio::test]
    async fn test_update_feature_view() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let mut view = create_test_view("updatable");

        registry.register_feature_view(&view).await.unwrap();

        // Update
        view.version = 2;
        view.description = Some("Updated description".to_string());
        registry.register_feature_view(&view).await.unwrap();

        // Verify update
        let retrieved = registry.get_feature_view("updatable").await.unwrap();
        assert_eq!(retrieved.version, 2);
        assert_eq!(retrieved.description, Some("Updated description".to_string()));
    }

    #[tokio::test]
    async fn test_failed_run_with_error_message() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let view = create_test_view("failing_view");
        registry.register_feature_view(&view).await.unwrap();

        let run_id = registry.create_run("failing_view").await.unwrap();

        registry
            .update_run_status(
                run_id,
                RunStatus::Failed,
                Some(100),
                Some("Connection timeout after 30 seconds"),
            )
            .await
            .unwrap();

        let run = registry.get_run_by_id(run_id).await.unwrap().unwrap();
        assert!(matches!(run.status, RunStatus::Failed));
        assert_eq!(
            run.error_message,
            Some("Connection timeout after 30 seconds".to_string())
        );
    }

    #[tokio::test]
    async fn test_multiple_runs_ordering() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let view = create_test_view("multi_run");
        registry.register_feature_view(&view).await.unwrap();

        // Create multiple runs
        for i in 0..5 {
            let run_id = registry.create_run("multi_run").await.unwrap();
            registry
                .update_run_status(run_id, RunStatus::Success, Some(i * 100), None)
                .await
                .unwrap();
        }

        // Get recent runs (should be ordered by most recent first)
        let runs = registry.get_recent_runs("multi_run", 3).await.unwrap();
        assert_eq!(runs.len(), 3);

        // Verify we get the most recent runs
        // IDs should be in descending order
        assert!(runs[0].id > runs[1].id);
        assert!(runs[1].id > runs[2].id);
    }

    #[tokio::test]
    async fn test_save_and_get_stats() {
        let registry = FeatureRegistry::in_memory().await.unwrap();
        let view = create_test_view("stats_test");
        registry.register_feature_view(&view).await.unwrap();

        let stats = FeatureViewStats {
            feature_view: "stats_test".to_string(),
            version: 1,
            row_count: 50000,
            distinct_entities: 5000,
            min_timestamp: Some(1700000000),
            max_timestamp: Some(1700100000),
            avg_file_size_bytes: Some(1_000_000),
            total_size_bytes: 50_000_000,
            histogram_buckets: None,
            created_at: Utc::now(),
        };

        registry.save_stats(&stats).await.unwrap();

        let retrieved = registry.get_latest_stats("stats_test").await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.row_count, 50000);
        assert_eq!(retrieved.distinct_entities, 5000);
    }
}
