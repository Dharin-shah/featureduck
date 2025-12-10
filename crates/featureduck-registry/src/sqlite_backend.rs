//! SQLite backend implementation with WAL mode
//!
//! This backend uses SQLite with Write-Ahead Logging (WAL) for multi-process safety.
//! Suitable for:
//! - Small to medium deployments (<10 concurrent workers)
//! - Embedded deployments (no external dependencies)
//! - Development and testing

use crate::backend::RegistryBackend;
use crate::schema::{self, RunStatus};
use crate::{FeatureViewDef, FeatureViewRun, FeatureViewStats};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use featureduck_core::recover_mutex;
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

/// SQLite backend with WAL mode for multi-process safety
pub struct SqliteBackend {
    db: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    /// Create new SQLite backend from file path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy();
        let is_memory = path_str == ":memory:" || path_str.starts_with("file::memory:");

        let db = Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .context("Failed to open SQLite connection for registry")?;

        // Only enable WAL mode for file-backed databases (not in-memory)
        if !is_memory {
            // Enable WAL mode for multi-process safe concurrent writes
            db.pragma_update(None, "journal_mode", "WAL")
                .context("Failed to enable WAL mode")?;

            // Set busy timeout to 5 seconds (wait for locks instead of immediate failure)
            db.pragma_update(None, "busy_timeout", 5000)
                .context("Failed to set busy timeout")?;

            // Optimize for better performance
            db.pragma_update(None, "synchronous", "NORMAL")
                .context("Failed to set synchronous mode")?;

            info!(
                "Initialized SQLite registry at {:?} with WAL mode (multi-process safe)",
                path.as_ref()
            );
        } else {
            info!("Initialized in-memory SQLite registry (testing mode)");
        }

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Create in-memory SQLite backend (for testing)
    pub fn in_memory() -> Result<Self> {
        let db =
            Connection::open_in_memory().context("Failed to create in-memory SQLite connection")?;

        // Note: In-memory databases don't support WAL mode (no persistence)
        // This is fine for testing since all access is single-threaded in tests

        info!("Initialized in-memory SQLite registry");

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
}

#[async_trait]
impl RegistryBackend for SqliteBackend {
    async fn init_schema(&self) -> Result<()> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;
        schema::create_tables(&db)?;
        Ok(())
    }

    async fn register_feature_view(&self, view: &FeatureViewDef) -> Result<()> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let entities_json = serde_json::to_string(&view.entities)?;
        let tags_json = serde_json::to_string(&view.tags)?;

        db.execute(
            r#"
            INSERT OR REPLACE INTO feature_views (
                name, version, source_type, source_path, entities,
                transformations, timestamp_field, ttl_days, batch_schedule,
                description, tags, owner, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                &view.name,
                view.version,
                &view.source_type,
                &view.source_path,
                entities_json,
                &view.transformations,
                &view.timestamp_field,
                view.ttl_days,
                &view.batch_schedule,
                &view.description,
                tags_json,
                &view.owner,
                view.created_at.timestamp(),
                view.updated_at.timestamp(),
            ],
        )
        .context("Failed to register feature view")?;

        debug!("Registered feature view: {}", view.name);
        Ok(())
    }

    async fn get_feature_view(&self, name: &str) -> Result<FeatureViewDef> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let mut stmt =
            db.prepare("SELECT * FROM feature_views WHERE name = ? ORDER BY version DESC LIMIT 1")?;

        let view = stmt
            .query_row(params![name], |row| {
                let entities_json: String = row.get(4)?;
                let entities: Vec<String> = serde_json::from_str(&entities_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let tags_json: String = row.get(10)?;
                let tags: Vec<String> = serde_json::from_str(&tags_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        10,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let created_ts: i64 = row.get(12)?;
                let updated_ts: i64 = row.get(13)?;

                Ok(FeatureViewDef {
                    name: row.get(0)?,
                    version: row.get(1)?,
                    source_type: row.get(2)?,
                    source_path: row.get(3)?,
                    entities,
                    transformations: row.get(5)?,
                    timestamp_field: row.get(6)?,
                    ttl_days: row.get(7)?,
                    batch_schedule: row.get(8)?,
                    description: row.get(9)?,
                    tags,
                    owner: row.get(11)?,
                    created_at: DateTime::from_timestamp(created_ts, 0).unwrap_or_else(Utc::now),
                    updated_at: DateTime::from_timestamp(updated_ts, 0).unwrap_or_else(Utc::now),
                })
            })
            .context(format!("Feature view '{}' not found", name))?;

        Ok(view)
    }

    async fn list_feature_views(&self, filter: Option<&str>) -> Result<Vec<FeatureViewDef>> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let (query, params_vec): (String, Vec<String>) = match filter {
            Some(f) => (
                "SELECT * FROM feature_views WHERE name LIKE ? ORDER BY name, version DESC"
                    .to_string(),
                vec![format!("%{}%", f)],
            ),
            None => (
                "SELECT * FROM feature_views ORDER BY name, version DESC".to_string(),
                vec![],
            ),
        };

        let mut stmt = db.prepare(&query)?;

        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let entities_json: String = row.get(4)?;
            let entities: Vec<String> = serde_json::from_str(&entities_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let tags_json: String = row.get(10)?;
            let tags: Vec<String> = serde_json::from_str(&tags_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let created_ts: i64 = row.get(12)?;
            let updated_ts: i64 = row.get(13)?;

            Ok(FeatureViewDef {
                name: row.get(0)?,
                version: row.get(1)?,
                source_type: row.get(2)?,
                source_path: row.get(3)?,
                entities,
                transformations: row.get(5)?,
                timestamp_field: row.get(6)?,
                ttl_days: row.get(7)?,
                batch_schedule: row.get(8)?,
                description: row.get(9)?,
                tags,
                owner: row.get(11)?,
                created_at: DateTime::from_timestamp(created_ts, 0).unwrap_or_else(Utc::now),
                updated_at: DateTime::from_timestamp(updated_ts, 0).unwrap_or_else(Utc::now),
            })
        })?;

        let mut views = Vec::new();
        for row in rows {
            views.push(row?);
        }

        Ok(views)
    }

    async fn delete_feature_view(&self, name: &str) -> Result<()> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        db.execute("DELETE FROM feature_views WHERE name = ?", params![name])
            .context(format!("Failed to delete feature view '{}'", name))?;

        info!("Deleted feature view: {}", name);
        Ok(())
    }

    async fn create_run(&self, feature_view_name: &str) -> Result<i64> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        // Get next ID
        let next_id: Option<i64> = db
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM feature_view_runs",
                [],
                |row| row.get(0),
            )
            .ok();

        let run_id = next_id.unwrap_or(1);

        db.execute(
            r#"
            INSERT INTO feature_view_runs (id, feature_view_name, status, started_at)
            VALUES (?, ?, 'pending', ?)
            "#,
            params![run_id, feature_view_name, Utc::now().timestamp()],
        )?;

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
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let status_str = status.to_string();
        let completed_at = if matches!(status, RunStatus::Success | RunStatus::Failed) {
            Some(Utc::now().timestamp())
        } else {
            None
        };

        let rows_affected = db
            .execute(
                r#"
            UPDATE feature_view_runs
            SET status = ?,
                completed_at = ?,
                rows_processed = ?,
                error_message = ?
            WHERE id = ?
            "#,
                params![
                    status_str,
                    completed_at,
                    rows_processed,
                    error_message,
                    run_id
                ],
            )
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
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let mut stmt = db.prepare(
            "SELECT * FROM feature_view_runs WHERE feature_view_name = ? ORDER BY started_at DESC, id DESC LIMIT ?"
        )?;

        let rows = stmt.query_map(params![feature_view_name, limit as i64], |row| {
            let started_ts: Option<i64> = row.get(3)?;
            let completed_ts: Option<i64> = row.get(4)?;
            let status_str: String = row.get(2)?;

            Ok(FeatureViewRun {
                id: row.get(0)?,
                feature_view_name: row.get(1)?,
                status: RunStatus::from_string(&status_str),
                started_at: started_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                completed_at: completed_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                rows_processed: row.get(5)?,
                error_message: row.get(6)?,
            })
        })?;

        let mut runs = Vec::new();
        for row in rows {
            runs.push(row?);
        }

        Ok(runs)
    }

    async fn get_run_by_id(&self, run_id: i64) -> Result<Option<FeatureViewRun>> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let mut stmt = db.prepare("SELECT * FROM feature_view_runs WHERE id = ?")?;

        let result = stmt.query_row(params![run_id], |row| {
            let started_ts: Option<i64> = row.get(3)?;
            let completed_ts: Option<i64> = row.get(4)?;
            let status_str: String = row.get(2)?;

            Ok(FeatureViewRun {
                id: row.get(0)?,
                feature_view_name: row.get(1)?,
                status: RunStatus::from_string(&status_str),
                started_at: started_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                completed_at: completed_ts.and_then(|ts| DateTime::from_timestamp(ts, 0)),
                rows_processed: row.get(5)?,
                error_message: row.get(6)?,
            })
        });

        match result {
            Ok(run) => Ok(Some(run)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn save_stats(&self, stats: &FeatureViewStats) -> Result<()> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        let created_at = stats.created_at.timestamp();
        let histogram = stats.histogram_buckets.as_deref();

        db.execute(
            r#"
            INSERT INTO feature_view_stats (
                feature_view, version, row_count, distinct_entities,
                min_timestamp, max_timestamp, avg_file_size_bytes,
                total_size_bytes, histogram_buckets, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(feature_view, version) DO UPDATE SET
                row_count = excluded.row_count,
                distinct_entities = excluded.distinct_entities,
                min_timestamp = excluded.min_timestamp,
                max_timestamp = excluded.max_timestamp,
                avg_file_size_bytes = excluded.avg_file_size_bytes,
                total_size_bytes = excluded.total_size_bytes,
                histogram_buckets = excluded.histogram_buckets,
                created_at = excluded.created_at
            "#,
            params![
                stats.feature_view,
                stats.version,
                stats.row_count,
                stats.distinct_entities,
                stats.min_timestamp,
                stats.max_timestamp,
                stats.avg_file_size_bytes,
                stats.total_size_bytes,
                histogram,
                created_at
            ],
        )?;

        debug!(
            "Saved stats for feature view: {} (version {})",
            stats.feature_view, stats.version
        );
        Ok(())
    }

    async fn get_latest_stats(&self, feature_view: &str) -> Result<Option<FeatureViewStats>> {
        let db = recover_mutex(&self.db, "FeatureRegistry")?;

        // Use ORDER BY created_at DESC to get most recent stats
        // Uses composite index idx_stats_feature_view_created for performance
        let mut stmt = db.prepare(
            r#"
            SELECT * FROM feature_view_stats
            WHERE feature_view = ?
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )?;

        let result = stmt.query_row(params![feature_view], |row| {
            let created_ts: i64 = row.get(9)?;
            let histogram: Option<String> = row.get(8)?;

            Ok(FeatureViewStats {
                feature_view: row.get(0)?,
                version: row.get(1)?,
                row_count: row.get(2)?,
                distinct_entities: row.get(3)?,
                min_timestamp: row.get(4)?,
                max_timestamp: row.get(5)?,
                avg_file_size_bytes: row.get(6)?,
                total_size_bytes: row.get(7)?,
                histogram_buckets: histogram,
                created_at: DateTime::from_timestamp(created_ts, 0).unwrap_or_else(Utc::now),
            })
        });

        match result {
            Ok(stats) => Ok(Some(stats)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
