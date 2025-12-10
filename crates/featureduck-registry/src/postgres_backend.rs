//! PostgreSQL backend implementation with connection pooling
//!
//! This backend uses PostgreSQL with deadpool for connection pooling.
//! Suitable for:
//! - Production deployments (100+ concurrent workers)
//! - High write concurrency (MVCC)
//! - Distributed systems (multiple servers)
//!
//! # Features:
//! - Connection pooling (configurable size)
//! - Automatic reconnection
//! - Health checks
//! - Statement timeouts
//! - Prepared statement caching

#[cfg(feature = "postgres")]
use crate::backend::RegistryBackend;
#[cfg(feature = "postgres")]
use crate::schema::RunStatus;
#[cfg(feature = "postgres")]
use crate::{FeatureViewDef, FeatureViewRun};
#[cfg(feature = "postgres")]
use anyhow::{Context, Result};
#[cfg(feature = "postgres")]
use async_trait::async_trait;
#[cfg(feature = "postgres")]
use chrono::{DateTime, Utc};
#[cfg(feature = "postgres")]
use deadpool_postgres::{Config, Pool, Runtime};
#[cfg(feature = "postgres")]
use tokio_postgres::NoTls;
#[cfg(feature = "postgres")]
use tracing::{debug, info};

#[cfg(feature = "postgres")]
/// PostgreSQL backend with connection pooling
pub struct PostgresBackend {
    pool: Pool,
}

#[cfg(feature = "postgres")]
impl PostgresBackend {
    /// Create new PostgreSQL backend with connection pool
    ///
    /// # Arguments
    /// * `connection_string` - PostgreSQL connection string (postgresql://user:pass@host/db)
    /// * `pool_size` - Maximum number of connections in pool
    /// * `timeout_seconds` - Connection timeout in seconds
    pub async fn new(
        connection_string: &str,
        pool_size: usize,
        timeout_seconds: u64,
    ) -> Result<Self> {
        info!(
            "Initializing PostgreSQL registry with pool_size={}, timeout={}s",
            pool_size, timeout_seconds
        );

        // Parse connection string
        let mut cfg = Config::new();
        cfg.url = Some(connection_string.to_string());
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: pool_size,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(std::time::Duration::from_secs(timeout_seconds)),
                create: Some(std::time::Duration::from_secs(timeout_seconds)),
                recycle: Some(std::time::Duration::from_secs(timeout_seconds)),
            },
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context("Failed to create PostgreSQL connection pool")?;

        // Test connection
        let client = pool
            .get()
            .await
            .context("Failed to get connection from pool")?;

        let version: String = client
            .query_one("SELECT version()", &[])
            .await
            .context("Failed to query PostgreSQL version")?
            .get(0);

        info!("Connected to PostgreSQL: {}", version);

        Ok(Self { pool })
    }

    /// Get pool statistics
    pub fn pool_stats(&self) -> deadpool_postgres::Status {
        self.pool.status()
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl RegistryBackend for PostgresBackend {
    async fn init_schema(&self) -> Result<()> {
        let client = self.pool.get().await?;

        // Create feature_views table
        client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS feature_views (
                    name TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    entities JSONB NOT NULL,
                    transformations TEXT NOT NULL,
                    timestamp_field TEXT,
                    ttl_days INTEGER,
                    batch_schedule TEXT,
                    description TEXT,
                    tags JSONB NOT NULL,
                    owner TEXT,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                )
                "#,
                &[],
            )
            .await
            .context("Failed to create feature_views table")?;

        // Create feature_view_runs table
        client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS feature_view_runs (
                    id BIGSERIAL PRIMARY KEY,
                    feature_view_name TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')),
                    started_at BIGINT,
                    completed_at BIGINT,
                    rows_processed BIGINT,
                    error_message TEXT
                )
                "#,
                &[],
            )
            .await
            .context("Failed to create feature_view_runs table")?;

        // Create indexes
        client
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON feature_view_runs(status)",
                &[],
            )
            .await
            .ok();

        client
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_started ON feature_view_runs(started_at DESC)",
                &[],
            )
            .await
            .ok();

        client
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_feature_view ON feature_view_runs(feature_view_name)",
                &[],
            )
            .await
            .ok();

        info!("PostgreSQL schema initialized successfully");
        Ok(())
    }

    async fn register_feature_view(&self, view: &FeatureViewDef) -> Result<()> {
        let client = self.pool.get().await?;

        let entities_json = serde_json::to_value(&view.entities)?;
        let tags_json = serde_json::to_value(&view.tags)?;

        client
            .execute(
                r#"
                INSERT INTO feature_views (
                    name, version, source_type, source_path, entities,
                    transformations, timestamp_field, ttl_days, batch_schedule,
                    description, tags, owner, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (name) DO UPDATE SET
                    version = EXCLUDED.version,
                    source_type = EXCLUDED.source_type,
                    source_path = EXCLUDED.source_path,
                    entities = EXCLUDED.entities,
                    transformations = EXCLUDED.transformations,
                    timestamp_field = EXCLUDED.timestamp_field,
                    ttl_days = EXCLUDED.ttl_days,
                    batch_schedule = EXCLUDED.batch_schedule,
                    description = EXCLUDED.description,
                    tags = EXCLUDED.tags,
                    owner = EXCLUDED.owner,
                    updated_at = EXCLUDED.updated_at
                "#,
                &[
                    &view.name,
                    &view.version,
                    &view.source_type,
                    &view.source_path,
                    &entities_json,
                    &view.transformations,
                    &view.timestamp_field,
                    &view.ttl_days,
                    &view.batch_schedule,
                    &view.description,
                    &tags_json,
                    &view.owner,
                    &view.created_at.timestamp(),
                    &view.updated_at.timestamp(),
                ],
            )
            .await
            .context("Failed to register feature view")?;

        debug!("Registered feature view: {}", view.name);
        Ok(())
    }

    async fn get_feature_view(&self, name: &str) -> Result<FeatureViewDef> {
        let client = self.pool.get().await?;

        let row = client
            .query_one(
                "SELECT * FROM feature_views WHERE name = $1 ORDER BY version DESC LIMIT 1",
                &[&name],
            )
            .await
            .context(format!("Feature view '{}' not found", name))?;

        let entities_json: serde_json::Value = row.get(4);
        let entities: Vec<String> = serde_json::from_value(entities_json)?;

        let tags_json: serde_json::Value = row.get(10);
        let tags: Vec<String> = serde_json::from_value(tags_json)?;

        let created_ts: i64 = row.get(12);
        let updated_ts: i64 = row.get(13);

        Ok(FeatureViewDef {
            name: row.get(0),
            version: row.get(1),
            source_type: row.get(2),
            source_path: row.get(3),
            entities,
            transformations: row.get(5),
            timestamp_field: row.get(6),
            ttl_days: row.get(7),
            batch_schedule: row.get(8),
            description: row.get(9),
            tags,
            owner: row.get(11),
            created_at: DateTime::from_timestamp(created_ts, 0).unwrap_or_else(Utc::now),
            updated_at: DateTime::from_timestamp(updated_ts, 0).unwrap_or_else(Utc::now),
        })
    }

    async fn list_feature_views(&self, filter: Option<&str>) -> Result<Vec<FeatureViewDef>> {
        let client = self.pool.get().await?;

        let rows = if let Some(f) = filter {
            client
                .query(
                    "SELECT * FROM feature_views WHERE name LIKE $1 ORDER BY name, version DESC",
                    &[&format!("%{}%", f)],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT * FROM feature_views ORDER BY name, version DESC",
                    &[],
                )
                .await?
        };

        let mut views = Vec::with_capacity(rows.len());
        for row in rows {
            let entities_json: serde_json::Value = row.get(4);
            let entities: Vec<String> = serde_json::from_value(entities_json)?;

            let tags_json: serde_json::Value = row.get(10);
            let tags: Vec<String> = serde_json::from_value(tags_json)?;

            let created_ts: i64 = row.get(12);
            let updated_ts: i64 = row.get(13);

            views.push(FeatureViewDef {
                name: row.get(0),
                version: row.get(1),
                source_type: row.get(2),
                source_path: row.get(3),
                entities,
                transformations: row.get(5),
                timestamp_field: row.get(6),
                ttl_days: row.get(7),
                batch_schedule: row.get(8),
                description: row.get(9),
                tags,
                owner: row.get(11),
                created_at: DateTime::from_timestamp(created_ts, 0).unwrap_or_else(Utc::now),
                updated_at: DateTime::from_timestamp(updated_ts, 0).unwrap_or_else(Utc::now),
            });
        }

        Ok(views)
    }

    async fn delete_feature_view(&self, name: &str) -> Result<()> {
        let client = self.pool.get().await?;

        client
            .execute("DELETE FROM feature_views WHERE name = $1", &[&name])
            .await
            .context(format!("Failed to delete feature view '{}'", name))?;

        info!("Deleted feature view: {}", name);
        Ok(())
    }

    async fn create_run(&self, feature_view_name: &str) -> Result<i64> {
        let client = self.pool.get().await?;

        let row = client
            .query_one(
                r#"
                INSERT INTO feature_view_runs (feature_view_name, status, started_at)
                VALUES ($1, 'pending', $2)
                RETURNING id
                "#,
                &[&feature_view_name, &Utc::now().timestamp()],
            )
            .await?;

        let run_id: i64 = row.get(0);

        debug!(
            "Created run {} for feature view: {}",
            run_id, feature_view_name
        );

        Ok(run_id)
    }

    async fn update_run_status(
        &self,
        run_id: i64,
        status: RunStatus,
        rows_processed: Option<i64>,
        error_message: Option<&str>,
    ) -> Result<()> {
        let client = self.pool.get().await?;

        let status_str = status.to_string();
        let completed_at = if matches!(status, RunStatus::Success | RunStatus::Failed) {
            Some(Utc::now().timestamp())
        } else {
            None
        };

        let rows_affected = client
            .execute(
                r#"
                UPDATE feature_view_runs
                SET status = $1,
                    completed_at = $2,
                    rows_processed = $3,
                    error_message = $4
                WHERE id = $5
                "#,
                &[
                    &status_str,
                    &completed_at,
                    &rows_processed,
                    &error_message,
                    &run_id,
                ],
            )
            .await
            .context(format!("Failed to update run {} status", run_id))?;

        if rows_affected == 0 {
            anyhow::bail!("Run {} not found", run_id);
        }

        debug!("Updated run {} status to: {}", run_id, status_str);
        Ok(())
    }

    async fn get_recent_runs(
        &self,
        feature_view_name: &str,
        limit: usize,
    ) -> Result<Vec<FeatureViewRun>> {
        let client = self.pool.get().await?;

        let rows = client
            .query(
                "SELECT * FROM feature_view_runs WHERE feature_view_name = $1 ORDER BY started_at DESC, id DESC LIMIT $2",
                &[&feature_view_name, &(limit as i64)],
            )
            .await?;

        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            let started_ts: Option<i64> = row.get(3);
            let completed_ts: Option<i64> = row.get(4);
            let status_str: String = row.get(2);

            runs.push(FeatureViewRun {
                id: row.get(0),
                feature_view_name: row.get(1),
                status: RunStatus::from_string(&status_str),
                started_at: started_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                completed_at: completed_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                rows_processed: row.get(5),
                error_message: row.get(6),
            });
        }

        Ok(runs)
    }
}
