//! Registry backend trait and implementations
//!
//! This module provides a trait-based abstraction for the registry storage backend.
//! Supports multiple backends:
//! - **SQLite with WAL** - Embedded, multi-process safe (<10 workers)
//! - **PostgreSQL** - Production scale, connection pooling (100+ workers)

use crate::{FeatureViewDef, FeatureViewRun, FeatureViewStats, RunStatus};
use anyhow::Result;
use async_trait::async_trait;

/// Registry backend trait for storage operations
#[async_trait]
pub trait RegistryBackend: Send + Sync {
    /// Initialize schema (create tables, indexes)
    async fn init_schema(&self) -> Result<()>;

    /// Register or update a feature view
    async fn register_feature_view(&self, view: &FeatureViewDef) -> Result<()>;

    /// Get feature view by name
    async fn get_feature_view(&self, name: &str) -> Result<FeatureViewDef>;

    /// List feature views (with optional filter)
    async fn list_feature_views(&self, filter: Option<&str>) -> Result<Vec<FeatureViewDef>>;

    /// Delete feature view
    async fn delete_feature_view(&self, name: &str) -> Result<()>;

    /// Create a new materialization run
    async fn create_run(&self, feature_view_name: &str) -> Result<i64>;

    /// Update run status
    async fn update_run_status(
        &self,
        run_id: i64,
        status: RunStatus,
        rows_processed: Option<i64>,
        error_message: Option<&str>,
    ) -> Result<()>;

    /// Get recent runs for a feature view
    async fn get_recent_runs(
        &self,
        feature_view_name: &str,
        limit: usize,
    ) -> Result<Vec<FeatureViewRun>>;

    /// Get a specific run by ID (O(1) lookup)
    ///
    /// More efficient than iterating through all runs when you know the run ID.
    async fn get_run_by_id(&self, run_id: i64) -> Result<Option<FeatureViewRun>>;

    /// Save feature view statistics
    async fn save_stats(&self, stats: &FeatureViewStats) -> Result<()>;

    /// Get latest statistics for a feature view
    async fn get_latest_stats(&self, feature_view: &str) -> Result<Option<FeatureViewStats>>;
}

/// Configuration for registry backend
#[derive(Debug, Clone)]
pub enum RegistryConfig {
    /// SQLite with WAL mode (embedded, multi-process safe)
    SQLite {
        /// Path to SQLite database file
        path: String,
    },

    /// PostgreSQL with connection pooling (production scale)
    #[cfg(feature = "postgres")]
    PostgreSQL {
        /// PostgreSQL connection string (postgresql://user:pass@host/db)
        connection_string: String,
        /// Connection pool size (default: 10)
        pool_size: usize,
        /// Connection timeout in seconds (default: 30)
        timeout_seconds: u64,
        /// Statement timeout in seconds (default: 60)
        statement_timeout_seconds: u64,
    },
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self::SQLite {
            path: ".featureduck/registry.db".to_string(),
        }
    }
}

impl RegistryConfig {
    /// Create SQLite configuration
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self::SQLite { path: path.into() }
    }

    /// Create PostgreSQL configuration with defaults
    #[cfg(feature = "postgres")]
    pub fn postgres(connection_string: impl Into<String>) -> Self {
        Self::PostgreSQL {
            connection_string: connection_string.into(),
            pool_size: 10,
            timeout_seconds: 30,
            statement_timeout_seconds: 60,
        }
    }

    /// Create PostgreSQL configuration with custom pool settings
    #[cfg(feature = "postgres")]
    pub fn postgres_with_pool(
        connection_string: impl Into<String>,
        pool_size: usize,
        timeout_seconds: u64,
    ) -> Self {
        Self::PostgreSQL {
            connection_string: connection_string.into(),
            pool_size,
            timeout_seconds,
            statement_timeout_seconds: 60,
        }
    }
}
