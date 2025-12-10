//! Database schema for feature registry
//!
//! This module defines the SQLite schema for storing feature view metadata
//! and materialization run tracking.
//!
//! # Tables
//!
//! - **feature_views**: Metadata for feature transformations
//! - **feature_view_runs**: Execution history and status tracking
//!
//! # Design Decisions
//!
//! - JSON columns for entities/tags: Simplifies schema, avoids JOIN complexity
//! - BIGINT timestamps: Unix epoch seconds for portability
//! - Indexes on frequently queried columns: status, started_at, feature_view_name

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Pending,
    Running,
    Success,
    Failed,
}

impl RunStatus {
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "success" => Self::Success,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

pub fn create_tables(db: &Connection) -> Result<()> {
    db.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS feature_views (
            name TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            source_path TEXT NOT NULL,
            entities TEXT NOT NULL,
            transformations TEXT NOT NULL,
            timestamp_field TEXT,
            ttl_days INTEGER,
            batch_schedule TEXT,
            description TEXT,
            tags TEXT NOT NULL,
            owner TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feature_view_runs (
            id INTEGER PRIMARY KEY,
            feature_view_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')),
            started_at BIGINT,
            completed_at BIGINT,
            rows_processed BIGINT,
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_runs_status ON feature_view_runs(status);
        CREATE INDEX IF NOT EXISTS idx_runs_started ON feature_view_runs(started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_runs_feature_view ON feature_view_runs(feature_view_name);

        CREATE TABLE IF NOT EXISTS feature_view_stats (
            feature_view TEXT NOT NULL,
            version INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            distinct_entities INTEGER NOT NULL,
            min_timestamp BIGINT,
            max_timestamp BIGINT,
            avg_file_size_bytes INTEGER,
            total_size_bytes BIGINT NOT NULL,
            histogram_buckets TEXT,
            created_at BIGINT NOT NULL,
            PRIMARY KEY (feature_view, version),
            FOREIGN KEY (feature_view) REFERENCES feature_views(name)
        );

        CREATE INDEX IF NOT EXISTS idx_stats_feature_view ON feature_view_stats(feature_view);
        CREATE INDEX IF NOT EXISTS idx_stats_created ON feature_view_stats(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_stats_feature_view_created ON feature_view_stats(feature_view, created_at DESC);
        "#,
    )
    .context("Failed to create registry schema")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_status_from_string() {
        assert_eq!(RunStatus::from_string("pending"), RunStatus::Pending);
        assert_eq!(RunStatus::from_string("RUNNING"), RunStatus::Running);
        assert_eq!(RunStatus::from_string("Success"), RunStatus::Success);
        assert_eq!(RunStatus::from_string("failed"), RunStatus::Failed);
        assert_eq!(RunStatus::from_string("unknown"), RunStatus::Pending);
    }

    #[test]
    fn test_run_status_display() {
        assert_eq!(RunStatus::Pending.to_string(), "pending");
        assert_eq!(RunStatus::Running.to_string(), "running");
        assert_eq!(RunStatus::Success.to_string(), "success");
        assert_eq!(RunStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn test_create_tables() {
        let db = Connection::open_in_memory().unwrap();
        assert!(create_tables(&db).is_ok());

        // Verify tables exist
        let mut stmt = db
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert!(tables.contains(&"feature_views".to_string()));
        assert!(tables.contains(&"feature_view_runs".to_string()));
    }
}
