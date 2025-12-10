//! Feature State Management for Incremental Aggregation
//!
//! This module provides persistent state storage for incremental feature computation.
//! By maintaining previous aggregate values, we can update features incrementally
//! without re-scanning all historical data.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  SQLite State Database (.db file) with WAL mode         │
//! │                                                          │
//! │  Table: feature_state_{view_name}                       │
//! │  ├─ entity_id: TEXT (PRIMARY KEY)                       │
//! │  ├─ state_blob: BLOB (binary serialized state)          │
//! │  ├─ version: INTEGER                                     │
//! │  └─ last_updated: INTEGER                                │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Why SQLite + MessagePack?
//!
//! **SQLite (OLTP engine):**
//! - Optimized for transactional workloads (point reads/writes)
//! - WAL mode: readers don't block writers
//! - 10-100x faster than DuckDB for state management
//!
//! **MessagePack (binary serialization):**
//! - 3-5x faster than JSON
//! - ~30-40% smaller on disk
//! - Supports all serde features (including untagged enums)
//!
//! ## Storage Footprint
//!
//! - ~8 bytes per feature value (binary)
//! - ~50 bytes overhead per entity (compact binary)
//! - Example: 1K users × 5 features ≈ 90 KB (was 150 KB)
//! - Example: 1M users × 10 features ≈ 90 MB (was 150 MB)
//!
//! ## Example
//!
//! ```rust,no_run
//! use featureduck_core::state::{StateManager, AggregateState};
//! use featureduck_core::FeatureValue;
//! use std::collections::HashMap;
//!
//! let manager = StateManager::new("/tmp/state.db").unwrap();
//! manager.create_state_table("user_features").unwrap();
//!
//! // Save state
//! let mut features = HashMap::new();
//! features.insert("clicks".to_string(), FeatureValue::Int(100));
//!
//! let state = AggregateState {
//!     entity_id: "user_1".to_string(),
//!     features,
//!     metadata: HashMap::new(),
//!     last_updated: 1234567890,
//!     version: 1,
//! };
//!
//! manager.save_state("user_features", vec![state]).unwrap();
//!
//! // Load state
//! let loaded = manager.load_state("user_features", &["user_1".to_string()]).unwrap();
//! ```

use crate::aggregate_tree::AggregateTree;
use crate::hyperloglog::HyperLogLog;
use crate::{recover_mutex, Error, FeatureValue, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Aggregate state for a single entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateState {
    /// Entity identifier (composite key encoded as string)
    pub entity_id: String,

    /// Feature name → aggregate value
    pub features: HashMap<String, FeatureValue>,

    /// Additional metadata for complex aggregations
    /// e.g., {"clicks_count": 100, "clicks_sum": 1500} for AVG
    pub metadata: HashMap<String, FeatureValue>,

    /// Last update timestamp (microseconds since epoch)
    pub last_updated: i64,

    /// Version number (incremented on each update)
    pub version: i64,
}

/// Materialization metadata tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationVersion {
    pub feature_view: String,
    pub version: i64,
    pub timestamp: i64,
    pub source_version: Option<i64>, // Delta Lake version of source table
    pub row_count: usize,
}

/// Incremental state for complex aggregations (HLL, Aggregate Trees)
///
/// Stored as compressed blobs in SQLite for efficient storage and recovery.
/// All state is in one place - no separate files needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalAggregateState {
    /// Entity identifier
    pub entity_id: String,

    /// HyperLogLog states for COUNT DISTINCT features
    /// Key: feature name (e.g., "unique_users_7d")
    /// Value: Serialized HLL bytes
    #[serde(default)]
    pub hll_states: HashMap<String, Vec<u8>>,

    /// Aggregate tree for time-windowed aggregations
    /// Serialized as MessagePack bytes
    #[serde(default)]
    pub aggregate_tree: Option<Vec<u8>>,

    /// Basic aggregate values (COUNT, SUM, MIN, MAX)
    pub features: HashMap<String, FeatureValue>,

    /// Last processed event timestamp
    pub last_event_timestamp: i64,

    /// Version for optimistic locking
    pub version: i64,
}

impl IncrementalAggregateState {
    /// Create new empty incremental state
    pub fn new(entity_id: String) -> Self {
        Self {
            entity_id,
            hll_states: HashMap::new(),
            aggregate_tree: None,
            features: HashMap::new(),
            last_event_timestamp: 0,
            version: 1,
        }
    }

    /// Store HyperLogLog state for a feature
    pub fn set_hll(&mut self, feature_name: &str, hll: &HyperLogLog) {
        self.hll_states
            .insert(feature_name.to_string(), hll.to_bytes());
    }

    /// Load HyperLogLog state for a feature
    pub fn get_hll(&self, feature_name: &str) -> Option<HyperLogLog> {
        self.hll_states
            .get(feature_name)
            .and_then(|bytes| HyperLogLog::from_bytes(bytes).ok())
    }

    /// Store Aggregate Tree
    pub fn set_aggregate_tree(&mut self, tree: &AggregateTree) {
        self.aggregate_tree = Some(tree.to_bytes());
    }

    /// Load Aggregate Tree
    pub fn get_aggregate_tree(&self) -> Option<AggregateTree> {
        self.aggregate_tree
            .as_ref()
            .and_then(|bytes| match AggregateTree::from_bytes(bytes) {
                Ok(tree) => Some(tree),
                Err(e) => {
                    tracing::warn!("Failed to deserialize AggregateTree: {}", e);
                    None
                }
            })
    }

    /// Compress state for storage (ZSTD level 3 - good balance)
    pub fn to_compressed_bytes(&self) -> Result<Vec<u8>> {
        let msgpack_bytes = rmp_serde::to_vec(self).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to serialize state: {}", e))
        })?;

        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to create encoder: {}", e)))?;

        encoder
            .write_all(&msgpack_bytes)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to compress state: {}", e)))?;

        encoder.finish().map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to finish compression: {}", e))
        })
    }

    /// Decompress state from storage
    pub fn from_compressed_bytes(bytes: &[u8]) -> Result<Self> {
        let mut decoder = zstd::stream::Decoder::new(bytes)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to create decoder: {}", e)))?;

        let mut msgpack_bytes = Vec::new();
        decoder.read_to_end(&mut msgpack_bytes).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to decompress state: {}", e))
        })?;

        rmp_serde::from_slice(&msgpack_bytes)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to deserialize state: {}", e)))
    }
}

/// State manager for incremental aggregation
#[derive(Clone)]
pub struct StateManager {
    conn: Arc<Mutex<Connection>>,
    db_path: String,
}

impl StateManager {
    /// Create a new state manager with SQLite backend
    ///
    /// SQLite is optimized for OLTP (transactional) workloads, making it 10-100x
    /// faster than DuckDB for state management. Configured with:
    /// - WAL mode: readers don't block writers
    /// - NORMAL synchronous: faster writes, still safe
    /// - 64MB cache: better performance for hot data
    ///
    /// # Arguments
    /// * `db_path` - Path to SQLite database file (will be created if doesn't exist)
    ///
    /// # Example
    /// ```rust,no_run
    /// use featureduck_core::state::StateManager;
    ///
    /// let manager = StateManager::new("/tmp/feature_state.db").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref().to_string_lossy().to_string();

        let conn = Connection::open(&db_path)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to open state DB: {}", e)))?;

        // Enable WAL mode for better concurrency (readers don't block writers)
        // PRAGMA commands return results, so we use pragma_update or ignore the result
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to enable WAL mode: {}", e))
            })?;

        // Optimize for writes (NORMAL is safe and much faster than FULL)
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to set synchronous mode: {}", e))
            })?;

        // 64MB cache for better performance
        conn.pragma_update(None, "cache_size", "-64000")
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to set cache size: {}", e)))?;

        // Memory-mapped I/O for faster reads (256MB)
        conn.pragma_update(None, "mmap_size", "268435456")
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to set mmap size: {}", e)))?;

        let manager = Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path,
        };

        manager.initialize_tables()?;

        Ok(manager)
    }

    /// Initialize state tables if they don't exist
    fn initialize_tables(&self) -> Result<()> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        // Materialization version tracking (SQLite types)
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS materialization_versions (
                feature_view TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                source_version INTEGER,
                row_count INTEGER NOT NULL
            )
            "#,
            [],
        )
        .map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to create versions table: {}", e))
        })?;

        Ok(())
    }

    /// Create state table for a specific feature view
    pub fn create_state_table(&self, feature_view: &str) -> Result<()> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("feature_state_{}", feature_view);

        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                entity_id TEXT PRIMARY KEY,
                state_blob BLOB NOT NULL,
                version INTEGER NOT NULL,
                last_updated INTEGER NOT NULL
            )
            "#,
            table_name
        );

        conn.execute(&sql, []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to create state table: {}", e))
        })?;

        // Create index on version for efficient queries
        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_version ON {}(version)",
            table_name, table_name
        );

        conn.execute(&index_sql, [])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to create index: {}", e)))?;

        Ok(())
    }

    /// Save aggregate state for entities
    ///
    /// **OPTIMIZED**: SQLite + MessagePack + prepared statements
    /// - SQLite: OLTP-optimized (10-100x faster than DuckDB)
    /// - MessagePack: Binary serialization (3-5x faster than JSON, 30-40% smaller)
    /// - Prepared statements: Single prepare, execute N times
    ///
    /// **Expected Performance**:
    /// - 100 entities: ~2-5ms (was 58ms with DuckDB+JSON)
    /// - 500 entities: ~10-20ms (was 111ms)
    /// - 1000 entities: ~20-40ms (was 975ms)
    pub fn save_state(&self, feature_view: &str, states: Vec<AggregateState>) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Saving state for {} entities in {}",
            states.len(),
            feature_view
        );

        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("feature_state_{}", feature_view);

        // Use transaction for atomicity
        conn.execute("BEGIN TRANSACTION", []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to begin transaction: {}", e))
        })?;

        let states_len = states.len();
        let mut total_state_size = 0;

        // SQLite with prepared statements (already 10-100x faster than DuckDB)
        let sql = format!(
            "INSERT OR REPLACE INTO {} (entity_id, state_blob, version, last_updated) VALUES (?, ?, ?, ?)",
            table_name
        );

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to prepare statement: {}", e))
        })?;

        for state in states {
            // Use MessagePack for binary serialization (3-5x faster than JSON, supports untagged enums)
            let state_bytes = rmp_serde::to_vec(&state).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to serialize state: {}", e))
            })?;

            total_state_size += state_bytes.len();

            stmt.execute(rusqlite::params![
                &state.entity_id,
                &state_bytes,
                &state.version,
                &state.last_updated,
            ])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to insert state: {}", e)))?;
        }

        conn.execute("COMMIT", []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to commit transaction: {}", e))
        })?;

        tracing::info!(
            "State saved: {} entities, {:.2} KB total, {:.0} bytes/entity avg (SQLite+MessagePack)",
            states_len,
            total_state_size as f64 / 1024.0,
            total_state_size as f64 / states_len as f64
        );

        Ok(())
    }

    /// Load aggregate state for specific entities
    pub fn load_state(
        &self,
        feature_view: &str,
        entity_ids: &[String],
    ) -> Result<HashMap<String, AggregateState>> {
        if entity_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("feature_state_{}", feature_view);

        // Check if table exists (SQLite: query sqlite_master)
        let check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        let table_exists = conn
            .query_row(check_sql, [&table_name], |row| row.get::<_, String>(0))
            .is_ok();

        if !table_exists {
            return Ok(HashMap::new());
        }

        // Build IN clause for entity IDs
        let placeholders = entity_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT entity_id, state_blob FROM {} WHERE entity_id IN ({})",
            table_name, placeholders
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to prepare query: {}", e)))?;

        // Convert entity_ids to rusqlite parameters
        let params: Vec<&dyn rusqlite::ToSql> = entity_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();

        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to query state: {}", e)))?;

        let mut result = HashMap::new();

        while let Some(row) = rows
            .next()
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to fetch row: {}", e)))?
        {
            let entity_id: String = row.get(0).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to get entity_id: {}", e))
            })?;

            let state_bytes: Vec<u8> = row.get(1).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to get state_blob: {}", e))
            })?;

            // Use MessagePack to deserialize from binary (3-5x faster than JSON, supports untagged enums)
            let state: AggregateState = rmp_serde::from_slice(&state_bytes).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to deserialize state: {}", e))
            })?;

            result.insert(entity_id, state);
        }

        Ok(result)
    }

    /// Get the last materialization version for a feature view
    pub fn get_last_version(&self, feature_view: &str) -> Result<Option<MaterializationVersion>> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let sql = "SELECT feature_view, version, timestamp, source_version, row_count \
                   FROM materialization_versions WHERE feature_view = ?";

        let result = conn.query_row(sql, [feature_view], |row| {
            Ok(MaterializationVersion {
                feature_view: row.get(0)?,
                version: row.get(1)?,
                timestamp: row.get(2)?,
                source_version: row.get(3)?,
                row_count: row.get::<_, i64>(4)? as usize,
            })
        });

        match result {
            Ok(version) => Ok(Some(version)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::StorageError(anyhow::anyhow!(
                "Failed to get version: {}",
                e
            ))),
        }
    }

    /// Update materialization version with optimistic locking
    ///
    /// Returns error if version conflict detected (another process updated first)
    pub fn update_version(&self, version: MaterializationVersion) -> Result<()> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        // Check current version for optimistic locking
        if version.version > 1 {
            let current_version_result = conn.query_row(
                "SELECT version FROM materialization_versions WHERE feature_view = ?",
                [&version.feature_view],
                |row| row.get::<_, i64>(0),
            );

            match current_version_result {
                Ok(current) => {
                    if current >= version.version {
                        return Err(Error::ConcurrentModification(format!(
                            "Version conflict for {}: expected {}, found {}",
                            version.feature_view,
                            version.version - 1,
                            current
                        )));
                    }
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // No previous version, this is fine
                }
                Err(e) => {
                    return Err(Error::StorageError(anyhow::anyhow!(
                        "Failed to check version: {}",
                        e
                    )));
                }
            }
        }

        let sql = "INSERT OR REPLACE INTO materialization_versions \
                   (feature_view, version, timestamp, source_version, row_count) \
                   VALUES (?, ?, ?, ?, ?)";

        conn.execute(
            sql,
            rusqlite::params![
                &version.feature_view,
                &version.version,
                &version.timestamp,
                &version.source_version,
                &(version.row_count as i64),
            ],
        )
        .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to update version: {}", e)))?;

        Ok(())
    }

    /// Get database path
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Cleanup old state versions (keep only latest N versions)
    ///
    /// This prevents unbounded state growth by pruning old versions.
    ///
    /// # Arguments
    /// * `feature_view` - Feature view to clean
    /// * `keep_versions` - Number of recent versions to keep
    pub fn cleanup_old_versions(&self, feature_view: &str, keep_versions: i64) -> Result<()> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("feature_state_{}", feature_view);

        // Check if table exists (SQLite: query sqlite_master)
        let check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        let table_exists = conn
            .query_row(check_sql, [&table_name], |row| row.get::<_, String>(0))
            .is_ok();

        if !table_exists {
            return Ok(());
        }

        // Get version threshold (delete versions older than this)
        let threshold_sql = format!(
            "SELECT version FROM {} ORDER BY version DESC LIMIT 1 OFFSET ?",
            table_name
        );

        let threshold_result = conn
            .query_row(&threshold_sql, [keep_versions - 1], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to get threshold: {}", e)));

        match threshold_result {
            Ok(threshold) => {
                let delete_sql = format!("DELETE FROM {} WHERE version < ?", table_name);

                let deleted = conn.execute(&delete_sql, [threshold]).map_err(|e| {
                    Error::StorageError(anyhow::anyhow!("Failed to delete old versions: {}", e))
                })?;

                if deleted > 0 {
                    tracing::info!(
                        "Cleaned up {} old state rows from {} (kept versions >= {})",
                        deleted,
                        feature_view,
                        threshold
                    );
                }
            }
            Err(_) => {
                // Not enough versions to clean up or error occurred
            }
        }

        Ok(())
    }

    /// Get state storage statistics
    pub fn get_state_stats(&self, feature_view: &str) -> Result<StateStats> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("feature_state_{}", feature_view);

        // Check if table exists (SQLite: query sqlite_master)
        let check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        let table_exists = conn
            .query_row(check_sql, [&table_name], |row| row.get::<_, String>(0))
            .is_ok();

        if !table_exists {
            return Ok(StateStats::default());
        }

        let stats_sql = format!(
            "SELECT
                COUNT(*) as entity_count,
                COUNT(DISTINCT version) as version_count,
                AVG(LENGTH(state_blob)) as avg_state_size,
                SUM(LENGTH(state_blob)) as total_size
             FROM {}",
            table_name
        );

        let stats = conn
            .query_row(&stats_sql, [], |row| {
                Ok(StateStats {
                    entity_count: row.get::<_, i64>(0)? as usize,
                    version_count: row.get::<_, i64>(1)? as usize,
                    avg_state_size_bytes: row.get::<_, Option<f64>>(2)?.unwrap_or(0.0) as usize,
                    total_size_bytes: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as usize,
                })
            })
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to get stats: {}", e)))?;

        Ok(stats)
    }

    // =========================================================================
    // Incremental State Methods (HLL + Aggregate Trees)
    // =========================================================================

    /// Create incremental state table for a feature view
    ///
    /// Uses a separate table from basic state for efficiency:
    /// - Compressed blobs (ZSTD) for HLL + Aggregate Tree data
    /// - Smaller storage footprint
    /// - Faster recovery (all state in SQLite)
    pub fn create_incremental_table(&self, feature_view: &str) -> Result<()> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("incremental_state_{}", feature_view);

        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                entity_id TEXT PRIMARY KEY,
                state_blob BLOB NOT NULL,
                version INTEGER NOT NULL,
                last_event_timestamp INTEGER NOT NULL
            )
            "#,
            table_name
        );

        conn.execute(&sql, []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to create incremental table: {}", e))
        })?;

        // Create index for timestamp-based queries
        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_ts ON {}(last_event_timestamp)",
            table_name, table_name
        );

        conn.execute(&index_sql, [])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to create index: {}", e)))?;

        Ok(())
    }

    /// Save incremental state for entities (compressed)
    ///
    /// State includes HLL sketches and Aggregate Trees, compressed with ZSTD.
    ///
    /// # Storage Efficiency
    /// - ZSTD compression: 3-10x compression ratio
    /// - HLL (4KB uncompressed) → ~500 bytes compressed
    /// - Aggregate Tree varies by data volume
    pub fn save_incremental_state(
        &self,
        feature_view: &str,
        states: Vec<IncrementalAggregateState>,
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Saving incremental state for {} entities in {}",
            states.len(),
            feature_view
        );

        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("incremental_state_{}", feature_view);

        // Use transaction for atomicity
        conn.execute("BEGIN TRANSACTION", []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to begin transaction: {}", e))
        })?;

        let sql = format!(
            "INSERT OR REPLACE INTO {} (entity_id, state_blob, version, last_event_timestamp) VALUES (?, ?, ?, ?)",
            table_name
        );

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to prepare statement: {}", e))
        })?;

        let mut total_uncompressed = 0usize;
        let mut total_compressed = 0usize;

        for state in &states {
            let compressed_bytes = state.to_compressed_bytes()?;
            total_compressed += compressed_bytes.len();

            // Estimate uncompressed size (rough calculation)
            total_uncompressed += state.hll_states.values().map(|v| v.len()).sum::<usize>();
            total_uncompressed += state.aggregate_tree.as_ref().map(|v| v.len()).unwrap_or(0);

            stmt.execute(rusqlite::params![
                &state.entity_id,
                &compressed_bytes,
                &state.version,
                &state.last_event_timestamp,
            ])
            .map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to insert incremental state: {}", e))
            })?;
        }

        conn.execute("COMMIT", []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to commit transaction: {}", e))
        })?;

        let compression_ratio = if total_compressed > 0 {
            total_uncompressed as f64 / total_compressed as f64
        } else {
            1.0
        };

        tracing::info!(
            "Incremental state saved: {} entities, {:.2} KB compressed (ratio: {:.1}x)",
            states.len(),
            total_compressed as f64 / 1024.0,
            compression_ratio
        );

        Ok(())
    }

    /// Load incremental state for specific entities
    pub fn load_incremental_state(
        &self,
        feature_view: &str,
        entity_ids: &[String],
    ) -> Result<HashMap<String, IncrementalAggregateState>> {
        if entity_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("incremental_state_{}", feature_view);

        // Check if table exists
        let check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        let table_exists = conn
            .query_row(check_sql, [&table_name], |row| row.get::<_, String>(0))
            .is_ok();

        if !table_exists {
            return Ok(HashMap::new());
        }

        // Build IN clause for entity IDs (pre-allocated string)
        let mut placeholders = String::with_capacity(entity_ids.len() * 3); // "?, " = 3 chars
        for i in 0..entity_ids.len() {
            if i > 0 {
                placeholders.push_str(", ");
            }
            placeholders.push('?');
        }

        let sql = format!(
            "SELECT entity_id, state_blob FROM {} WHERE entity_id IN ({})",
            table_name, placeholders
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to prepare query: {}", e)))?;

        let params: Vec<&dyn rusqlite::ToSql> = entity_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();

        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to query state: {}", e)))?;

        // Pre-allocate HashMap for expected number of results
        let mut result = HashMap::with_capacity(entity_ids.len());

        while let Some(row) = rows
            .next()
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to fetch row: {}", e)))?
        {
            let entity_id: String = row.get(0).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to get entity_id: {}", e))
            })?;

            let compressed_bytes: Vec<u8> = row.get(1).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to get state_blob: {}", e))
            })?;

            let state = IncrementalAggregateState::from_compressed_bytes(&compressed_bytes)?;
            result.insert(entity_id, state);
        }

        Ok(result)
    }

    /// Load all incremental states for a feature view
    pub fn load_all_incremental_states(
        &self,
        feature_view: &str,
    ) -> Result<Vec<IncrementalAggregateState>> {
        let conn = recover_mutex(&self.conn, "StateManager")?;

        let table_name = format!("incremental_state_{}", feature_view);

        // Check if table exists
        let check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        let table_exists = conn
            .query_row(check_sql, [&table_name], |row| row.get::<_, String>(0))
            .is_ok();

        if !table_exists {
            return Ok(Vec::new());
        }

        let sql = format!("SELECT state_blob FROM {}", table_name);

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to prepare query: {}", e)))?;

        let mut rows = stmt
            .query([])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to query state: {}", e)))?;

        let mut result = Vec::new();

        while let Some(row) = rows
            .next()
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to fetch row: {}", e)))?
        {
            let compressed_bytes: Vec<u8> = row.get(0).map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to get state_blob: {}", e))
            })?;

            let state = IncrementalAggregateState::from_compressed_bytes(&compressed_bytes)?;
            result.push(state);
        }

        Ok(result)
    }
}

/// State storage statistics
#[derive(Debug, Clone, Default)]
pub struct StateStats {
    pub entity_count: usize,
    pub version_count: usize,
    pub avg_state_size_bytes: usize,
    pub total_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_state_manager_create() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");

        let manager = StateManager::new(&db_path).unwrap();
        assert!(db_path.exists());
        assert_eq!(manager.db_path(), db_path.to_str().unwrap());
    }

    #[test]
    fn test_create_state_table() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_state_table("user_features").unwrap();

        // Verify table exists (SQLite: query sqlite_master)
        let conn = manager.conn.lock().unwrap();
        let table_name = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='feature_state_user_features'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();

        assert_eq!(table_name, "feature_state_user_features");
    }

    #[test]
    fn test_save_and_load_state() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_state_table("user_features").unwrap();

        // Save state
        let mut features = HashMap::new();
        features.insert("clicks".to_string(), FeatureValue::Int(100));
        features.insert("purchases".to_string(), FeatureValue::Int(5));

        let state = AggregateState {
            entity_id: "user_1".to_string(),
            features,
            metadata: HashMap::new(),
            last_updated: 1234567890,
            version: 1,
        };

        manager
            .save_state("user_features", vec![state.clone()])
            .unwrap();

        // Load state
        let loaded = manager
            .load_state("user_features", &["user_1".to_string()])
            .unwrap();

        assert_eq!(loaded.len(), 1);
        let loaded_state = loaded.get("user_1").unwrap();
        assert_eq!(loaded_state.entity_id, "user_1");
        assert_eq!(
            loaded_state.features.get("clicks"),
            Some(&FeatureValue::Int(100))
        );
    }

    #[test]
    fn test_version_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        // No version initially
        let version = manager.get_last_version("user_features").unwrap();
        assert!(version.is_none());

        // Update version
        let new_version = MaterializationVersion {
            feature_view: "user_features".to_string(),
            version: 1,
            timestamp: 1234567890,
            source_version: Some(10),
            row_count: 1000,
        };

        manager.update_version(new_version.clone()).unwrap();

        // Load version
        let loaded = manager.get_last_version("user_features").unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.source_version, Some(10));
        assert_eq!(loaded.row_count, 1000);
    }

    #[test]
    fn test_optimistic_locking() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        // Set version 1
        let v1 = MaterializationVersion {
            feature_view: "user_features".to_string(),
            version: 1,
            timestamp: 1234567890,
            source_version: None,
            row_count: 100,
        };
        manager.update_version(v1).unwrap();

        // Update to version 2 - should succeed
        let v2 = MaterializationVersion {
            feature_view: "user_features".to_string(),
            version: 2,
            timestamp: 1234567900,
            source_version: None,
            row_count: 150,
        };
        manager.update_version(v2).unwrap();

        // Try to update with version 2 again - should fail (version conflict)
        let v2_duplicate = MaterializationVersion {
            feature_view: "user_features".to_string(),
            version: 2,
            timestamp: 1234567910,
            source_version: None,
            row_count: 200,
        };

        let result = manager.update_version(v2_duplicate);
        assert!(result.is_err());
        match result {
            Err(Error::ConcurrentModification(_)) => {}
            _ => panic!("Expected ConcurrentModification error"),
        }
    }

    #[test]
    fn test_cleanup_old_versions() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_state_table("user_features").unwrap();

        // Create 5 versions of state for DIFFERENT entities
        for v in 1..=5 {
            let mut features = HashMap::new();
            features.insert("clicks".to_string(), FeatureValue::Int(v * 100));

            let state = AggregateState {
                entity_id: format!("user_{}", v),
                features,
                metadata: HashMap::new(),
                last_updated: 1234567890 + v,
                version: v,
            };

            manager.save_state("user_features", vec![state]).unwrap();
        }

        // Verify all 5 entities exist (each with different version)
        let stats = manager.get_state_stats("user_features").unwrap();
        assert_eq!(stats.entity_count, 5);
        assert_eq!(stats.version_count, 5);

        // Cleanup, keeping only last 3 versions
        manager.cleanup_old_versions("user_features", 3).unwrap();

        // Verify only 3 entities remain (versions 3, 4, 5 kept; 1, 2 deleted)
        let stats_after = manager.get_state_stats("user_features").unwrap();
        assert_eq!(stats_after.entity_count, 3);
        assert_eq!(stats_after.version_count, 3);
    }

    #[test]
    fn test_state_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_state_table("user_features").unwrap();

        // Initially empty
        let stats = manager.get_state_stats("user_features").unwrap();
        assert_eq!(stats.entity_count, 0);
        assert_eq!(stats.version_count, 0);

        // Add some state
        let mut features = HashMap::new();
        features.insert("clicks".to_string(), FeatureValue::Int(100));
        features.insert("purchases".to_string(), FeatureValue::Int(5));

        let state = AggregateState {
            entity_id: "user_1".to_string(),
            features,
            metadata: HashMap::new(),
            last_updated: 1234567890,
            version: 1,
        };

        manager.save_state("user_features", vec![state]).unwrap();

        // Check stats
        let stats = manager.get_state_stats("user_features").unwrap();
        assert_eq!(stats.entity_count, 1);
        assert_eq!(stats.version_count, 1);
        assert!(stats.total_size_bytes > 0);
        assert!(stats.avg_state_size_bytes > 0);
    }

    // =========================================================================
    // Incremental State Tests (HLL + Aggregate Trees)
    // =========================================================================

    #[test]
    fn test_incremental_state_with_hll() {
        use crate::aggregate_tree::{AggregateTree, TimeGranularity};
        use crate::hyperloglog::HyperLogLog;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_incremental_table("user_features").unwrap();

        // Create HLL with some data
        let mut hll = HyperLogLog::new(12);
        for i in 0..1000 {
            hll.insert(&format!("user_{}", i));
        }

        // Create aggregate tree with some events
        let mut tree = AggregateTree::new(vec![TimeGranularity::Minute, TimeGranularity::Hour]);
        for i in 0..100 {
            tree.insert_event(1699920000 + i * 60, i as f64);
        }

        // Create incremental state
        let mut state = IncrementalAggregateState::new("user_123".to_string());
        state.set_hll("unique_users_7d", &hll);
        state.set_aggregate_tree(&tree);
        state
            .features
            .insert("clicks".to_string(), FeatureValue::Int(100));
        state.last_event_timestamp = 1699920000 + 99 * 60;

        // Save state
        manager
            .save_incremental_state("user_features", vec![state])
            .unwrap();

        // Load state
        let loaded = manager
            .load_incremental_state("user_features", &["user_123".to_string()])
            .unwrap();

        assert_eq!(loaded.len(), 1);
        let loaded_state = loaded.get("user_123").unwrap();

        // Verify HLL
        let loaded_hll = loaded_state
            .get_hll("unique_users_7d")
            .expect("HLL should exist");
        let estimate = loaded_hll.cardinality();
        assert!(
            (900..=1100).contains(&estimate),
            "HLL estimate {} should be ~1000",
            estimate
        );

        // Verify aggregate tree
        let loaded_tree = loaded_state
            .get_aggregate_tree()
            .expect("Tree should exist");
        let count = loaded_tree.query_count(1699920000, 1699920000 + 100 * 60);
        assert_eq!(count, 100, "Tree should have 100 events");

        // Verify features
        assert_eq!(
            loaded_state.features.get("clicks"),
            Some(&FeatureValue::Int(100))
        );
    }

    #[test]
    fn test_incremental_state_compression() {
        use crate::hyperloglog::HyperLogLog;

        // Create a large HLL state
        let mut hll = HyperLogLog::new(14); // 16KB uncompressed
        for i in 0..100_000 {
            hll.insert(&i);
        }

        let mut state = IncrementalAggregateState::new("user_test".to_string());
        state.set_hll("large_hll", &hll);

        // Compress
        let compressed = state.to_compressed_bytes().unwrap();
        let uncompressed_hll_size = state.hll_states.get("large_hll").unwrap().len();

        // ZSTD should achieve significant compression on HLL data
        // (HLL registers are mostly zeros for sparse data)
        println!(
            "HLL size: {} bytes, compressed state: {} bytes",
            uncompressed_hll_size,
            compressed.len()
        );

        // Decompress and verify
        let decompressed = IncrementalAggregateState::from_compressed_bytes(&compressed).unwrap();
        let loaded_hll = decompressed.get_hll("large_hll").expect("HLL should exist");

        assert_eq!(hll.cardinality(), loaded_hll.cardinality());
    }

    #[test]
    fn test_incremental_state_multiple_entities() {
        use crate::hyperloglog::HyperLogLog;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_incremental_table("user_features").unwrap();

        // Create states for multiple entities
        let mut states = Vec::new();
        for i in 0..100 {
            let mut hll = HyperLogLog::new(12);
            for j in 0..100 {
                hll.insert(&format!("item_{}_{}", i, j));
            }

            let mut state = IncrementalAggregateState::new(format!("user_{}", i));
            state.set_hll("unique_items", &hll);
            state
                .features
                .insert("count".to_string(), FeatureValue::Int(i as i64));
            states.push(state);
        }

        // Save all states
        manager
            .save_incremental_state("user_features", states)
            .unwrap();

        // Load specific entities
        let entity_ids: Vec<String> = (0..10).map(|i| format!("user_{}", i)).collect();
        let loaded = manager
            .load_incremental_state("user_features", &entity_ids)
            .unwrap();

        assert_eq!(loaded.len(), 10);

        // Load all entities
        let all_states = manager
            .load_all_incremental_states("user_features")
            .unwrap();

        assert_eq!(all_states.len(), 100);
    }

    #[test]
    fn test_incremental_state_update() {
        use crate::hyperloglog::HyperLogLog;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_state.db");
        let manager = StateManager::new(&db_path).unwrap();

        manager.create_incremental_table("user_features").unwrap();

        // Initial state
        let mut hll = HyperLogLog::new(12);
        for i in 0..500 {
            hll.insert(&format!("user_{}", i));
        }

        let mut state = IncrementalAggregateState::new("entity_1".to_string());
        state.set_hll("unique_users", &hll);
        state.version = 1;

        manager
            .save_incremental_state("user_features", vec![state])
            .unwrap();

        // Update state (simulate incremental update)
        let loaded = manager
            .load_incremental_state("user_features", &["entity_1".to_string()])
            .unwrap();

        let mut updated_state = loaded.get("entity_1").unwrap().clone();
        let mut updated_hll = updated_state.get_hll("unique_users").unwrap();

        // Add more users (some overlap, some new)
        for i in 250..750 {
            updated_hll.insert(&format!("user_{}", i));
        }

        updated_state.set_hll("unique_users", &updated_hll);
        updated_state.version = 2;

        manager
            .save_incremental_state("user_features", vec![updated_state])
            .unwrap();

        // Verify final state
        let final_loaded = manager
            .load_incremental_state("user_features", &["entity_1".to_string()])
            .unwrap();

        let final_state = final_loaded.get("entity_1").unwrap();
        let final_hll = final_state.get_hll("unique_users").unwrap();

        // Should have ~750 unique users (0-749)
        let estimate = final_hll.cardinality();
        assert!(
            (700..=800).contains(&estimate),
            "Final HLL estimate {} should be ~750",
            estimate
        );
        assert_eq!(final_state.version, 2);
    }
}
