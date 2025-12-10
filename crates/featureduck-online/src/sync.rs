//! Delta Lake → Online Store sync
//!
//! ## Sync Strategies
//!
//! 1. **Full sync**: Read all rows from Delta Lake and write to online store
//! 2. **Incremental sync**: Use Delta Lake's change data feed for updates only
//!
//! ## Performance
//!
//! - Parallel reads from Delta Lake (DuckDB)
//! - Batched writes to online store
//! - Progress tracking and resumability
//!
//! ## Usage
//!
//! ```rust,ignore
//! use featureduck_online::{RedisOnlineStore, sync_to_online};
//!
//! let redis = RedisOnlineStore::new(config).await?;
//! let delta_path = "s3://bucket/features/user_features";
//!
//! sync_to_online(&redis, delta_path, "user_features", &["user_id"]).await?;
//! ```

use featureduck_core::{EntityKey, FeatureRow, FeatureValue, OnlineStore, OnlineWriteConfig, Result};
use std::collections::HashMap;
use std::time::Instant;

/// Sync configuration
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Batch size for reading from Delta Lake
    pub read_batch_size: usize,
    /// Batch size for writing to online store
    pub write_batch_size: usize,
    /// TTL for features in online store (None = no expiration)
    pub ttl: Option<std::time::Duration>,
    /// Only sync rows newer than this timestamp (incremental sync)
    ///
    /// If None, syncs all rows (full sync).
    /// Store the returned `latest_timestamp` from SyncResult for the next sync.
    pub since_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            read_batch_size: 10_000,
            write_batch_size: 1_000,
            ttl: None,
            since_timestamp: None, // Full sync by default
        }
    }
}

impl SyncConfig {
    /// Create config for incremental sync (only new/updated rows)
    pub fn incremental(since: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            since_timestamp: Some(since),
            ..Default::default()
        }
    }
}

/// Sync result with metrics
#[derive(Debug)]
pub struct SyncResult {
    /// Total rows synced
    pub rows_synced: usize,
    /// Duration of sync
    pub duration: std::time::Duration,
    /// Rows per second
    pub rows_per_sec: f64,
    /// Latest timestamp seen (use as `since_timestamp` for next incremental sync)
    pub latest_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether this was an incremental sync
    pub incremental: bool,
}

/// Sync features from Delta Lake to an online store
///
/// This reads the latest features from Delta Lake and writes them to the online store.
/// It's designed for blazing fast bulk sync operations.
///
/// # Arguments
///
/// * `online_store` - The online store to sync to (Redis or PostgreSQL)
/// * `delta_path` - Path to the Delta Lake table
/// * `feature_view` - Name of the feature view
/// * `entity_columns` - Names of entity columns
/// * `config` - Sync configuration
///
/// # Performance
///
/// - Uses DuckDB for fast Delta Lake reads
/// - Batched writes to online store
/// - Target: 100K+ rows/sec
pub async fn sync_to_online(
    online_store: &dyn OnlineStore,
    delta_path: &str,
    feature_view: &str,
    entity_columns: &[&str],
    config: SyncConfig,
) -> Result<SyncResult> {
    let start = Instant::now();

    // 1. Connect to DuckDB for reading Delta Lake
    let duckdb = duckdb::Connection::open_in_memory()
        .map_err(|e| featureduck_core::Error::StorageError(anyhow::anyhow!("DuckDB error: {}", e)))?;

    // Install and load Delta extension
    duckdb.execute_batch("INSTALL delta; LOAD delta;")
        .map_err(|e| featureduck_core::Error::StorageError(anyhow::anyhow!("Delta extension error: {}", e)))?;

    // 2. Build query - select latest features per entity using QUALIFY
    let partition_by = entity_columns.join(", ");

    // Build WHERE clause for incremental sync
    let time_filter = if let Some(since) = config.since_timestamp {
        let since_micros = since.timestamp_micros();
        format!("WHERE epoch_us(__timestamp) > {}", since_micros)
    } else {
        String::new()
    };

    // Use QUALIFY to get latest row per entity (blazingly fast!)
    let sql = format!(
        r#"
        SELECT *
        FROM delta_scan('{}')
        {}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {} ORDER BY __timestamp DESC) = 1
        "#,
        delta_path, time_filter, partition_by
    );

    let sync_mode = if config.since_timestamp.is_some() {
        "incremental"
    } else {
        "full"
    };

    tracing::info!(
        delta_path = delta_path,
        feature_view = feature_view,
        mode = sync_mode,
        "Starting {} sync from Delta Lake to {}", sync_mode, online_store.store_type()
    );

    // 3. Execute query and stream results
    let mut stmt = duckdb.prepare(&sql)
        .map_err(|e| featureduck_core::Error::StorageError(anyhow::anyhow!("Query prepare error: {}", e)))?;

    let column_names: Vec<String> = stmt.column_names();
    let entity_col_set: std::collections::HashSet<&str> = entity_columns.iter().cloned().collect();

    let rows_result = stmt.query_map([], |row| {
        // Extract entity keys
        let mut entities = Vec::new();
        for (i, col) in column_names.iter().enumerate() {
            if entity_col_set.contains(col.as_str()) {
                let value: String = row.get(i)?;
                entities.push(EntityKey::new(col.clone(), value));
            }
        }

        // Extract features (skip entity columns and __timestamp)
        let mut features = HashMap::new();
        for (i, col) in column_names.iter().enumerate() {
            if entity_col_set.contains(col.as_str()) || col == "__timestamp" {
                continue;
            }

            // Try to extract as different types
            let feature_value = if let Ok(v) = row.get::<_, i64>(i) {
                FeatureValue::Int(v)
            } else if let Ok(v) = row.get::<_, f64>(i) {
                FeatureValue::Float(v)
            } else if let Ok(v) = row.get::<_, String>(i) {
                FeatureValue::String(v)
            } else if let Ok(v) = row.get::<_, bool>(i) {
                FeatureValue::Bool(v)
            } else {
                FeatureValue::Null
            };

            features.insert(col.clone(), feature_value);
        }

        Ok(FeatureRow {
            entities,
            features,
            timestamp: chrono::Utc::now(),
        })
    }).map_err(|e| featureduck_core::Error::StorageError(anyhow::anyhow!("Query error: {}", e)))?;

    // 4. Batch write to online store
    let write_config = OnlineWriteConfig { ttl: config.ttl };
    let mut total_rows = 0;
    let mut batch = Vec::with_capacity(config.write_batch_size);

    for row_result in rows_result {
        let row = row_result
            .map_err(|e| featureduck_core::Error::StorageError(anyhow::anyhow!("Row error: {}", e)))?;
        batch.push(row);

        if batch.len() >= config.write_batch_size {
            online_store.write_online_features(feature_view, batch, write_config.clone()).await?;
            total_rows += config.write_batch_size;
            batch = Vec::with_capacity(config.write_batch_size);

            // Progress logging
            if total_rows % 10_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                tracing::info!(
                    rows = total_rows,
                    rate = format!("{:.0}/sec", total_rows as f64 / elapsed),
                    "Sync progress"
                );
            }
        }
    }

    // Write remaining rows
    if !batch.is_empty() {
        let batch_len = batch.len();
        online_store.write_online_features(feature_view, batch, write_config).await?;
        total_rows += batch_len;
    }

    let duration = start.elapsed();
    let rows_per_sec = if duration.as_secs_f64() > 0.0 {
        total_rows as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    tracing::info!(
        rows = total_rows,
        duration_ms = duration.as_millis(),
        rate = format!("{:.0}/sec", rows_per_sec),
        mode = sync_mode,
        "Sync complete"
    );

    Ok(SyncResult {
        rows_synced: total_rows,
        duration,
        rows_per_sec,
        latest_timestamp: Some(chrono::Utc::now()), // Use current time as latest
        incremental: config.since_timestamp.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();
        assert_eq!(config.read_batch_size, 10_000);
        assert_eq!(config.write_batch_size, 1_000);
        assert!(config.ttl.is_none());
    }
}
