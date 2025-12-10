//! DuckDB Connection Pool for Concurrent Operations
//!
//! This module provides a connection pool for DuckDB to enable concurrent materializations
//! without blocking. Each materialization gets its own connection from the pool, allowing
//! true parallelism across multiple workers/threads.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │  DuckDB Connection Pool (4-8 connections)            │
//! │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
//! │  │ Conn 1  │  │ Conn 2  │  │ Conn 3  │  │ Conn 4  │ │
//! │  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
//! └──────────────────────────────────────────────────────┘
//!           ↑           ↑           ↑           ↑
//!           │           │           │           │
//!      Worker 1    Worker 2    Worker 3    Worker 4
//!      (mat 1)     (mat 2)     (mat 3)     (mat 4)
//! ```
//!
//! ## Benefits
//!
//! - **3-8x throughput** for concurrent materializations (depends on pool size)
//! - **No blocking** under load (queries run in parallel)
//! - **Better CPU utilization** (all cores busy instead of serialized)
//! - **Automatic health checks** (dead connections replaced automatically)
//!
//! ## Configuration
//!
//! ```rust,no_run
//! use featureduck_delta::pool::{DuckDBPoolConfig, create_duckdb_pool};
//! use std::path::PathBuf;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Default configuration (auto-detect pool size)
//!     let config = DuckDBPoolConfig::default();
//!     let pool = create_duckdb_pool(config)?;
//!
//!     // Custom configuration for high-concurrency workloads
//!     let custom_config = DuckDBPoolConfig {
//!         db_path: Some(PathBuf::from(".featureduck/duckdb.db")),
//!         pool_size: 8,  // 8 concurrent connections
//!         threads_per_connection: 4,  // 4 threads per connection
//!         memory_limit: "8GB".to_string(),
//!         connection_timeout_secs: 10,
//!         aggressive_optimizations: true,
//!     };
//!     let pool = create_duckdb_pool(custom_config)?;
//!
//!     Ok(())
//! # }
//! ```

use crate::DuckDBEngineConfig;
use duckdb::Connection;
use featureduck_core::{Error, Result};
use r2d2::{ManageConnection, Pool, PooledConnection};
use std::path::PathBuf;
use tracing::{debug, info};

/// DuckDB connection pool configuration
///
/// ## Performance Settings
///
/// For optimal performance, configure:
/// - `temp_directory`: Essential for spilling intermediate results (prevents OOM)
/// - `preserve_insertion_order`: false for better parallelism
#[derive(Debug, Clone)]
pub struct DuckDBPoolConfig {
    /// Path to persistent DuckDB database file (None = in-memory)
    pub db_path: Option<PathBuf>,

    /// Number of connections in the pool (default: auto-detect based on CPU cores)
    pub pool_size: u32,

    /// Thread count per connection (0 = auto-detect)
    pub threads_per_connection: usize,

    /// Memory limit per connection (e.g., "4GB", "8GB")
    pub memory_limit: String,

    /// Connection timeout in seconds (default: 10s)
    pub connection_timeout_secs: u64,

    /// Enable aggressive query optimizations
    pub aggressive_optimizations: bool,

    // ========================================================================
    // Performance Optimizations
    // ========================================================================

    /// Directory for spilling intermediate results to disk
    ///
    /// Without this, DuckDB will OOM on large aggregations, joins, or sorts
    /// that exceed memory_limit. Always configure for production workloads.
    pub temp_directory: Option<PathBuf>,

    /// Disable insertion order preservation for better parallelism
    ///
    /// When false, DuckDB can parallelize operations more aggressively.
    /// Default: false (better performance)
    pub preserve_insertion_order: bool,
}

impl Default for DuckDBPoolConfig {
    fn default() -> Self {
        // Auto-detect optimal pool size based on CPU cores
        // Rule: pool_size = max(4, num_cpus / 2)
        // This ensures we don't over-subscribe the CPU while allowing good concurrency
        let num_cpus = num_cpus::get();
        let pool_size = ((num_cpus / 2).max(4)) as u32;

        info!(
            "Auto-detected DuckDB pool configuration: {} connections (system has {} cores)",
            pool_size, num_cpus
        );

        Self {
            db_path: Some(PathBuf::from(".featureduck/duckdb.db")),
            pool_size,
            threads_per_connection: 0, // Auto-detect per connection
            memory_limit: auto_detect_memory_limit_per_connection(pool_size as usize),
            connection_timeout_secs: 10,
            aggressive_optimizations: true,
            // Performance optimizations
            temp_directory: Some(PathBuf::from("/tmp/featureduck-duckdb")),
            preserve_insertion_order: false, // Better parallelism
        }
    }
}

/// Auto-detect memory limit per connection based on total system memory and pool size
///
/// Formula: memory_per_conn = (total_memory * 0.75) / pool_size
/// This ensures the pool doesn't exceed 75% of system memory
fn auto_detect_memory_limit_per_connection(pool_size: usize) -> String {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_memory();

    let total_memory_bytes = sys.total_memory();
    let total_memory_gb = total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    // Allocate 75% of system memory across all connections
    let total_pool_memory_gb = (total_memory_gb * 0.75).floor();
    let memory_per_conn_gb = (total_pool_memory_gb / pool_size as f64).floor() as u64;
    let memory_per_conn = memory_per_conn_gb.max(1); // Minimum 1GB per connection

    info!(
        "Auto-detected memory per connection: {}GB (total: {:.1}GB, pool size: {})",
        memory_per_conn, total_memory_gb, pool_size
    );

    format!("{}GB", memory_per_conn)
}

/// DuckDB connection pool manager implementing r2d2's ManageConnection trait
pub struct DuckDBConnectionManager {
    config: DuckDBPoolConfig,
}

impl DuckDBConnectionManager {
    pub fn new(config: DuckDBPoolConfig) -> Self {
        Self { config }
    }

    /// Configure a DuckDB connection with performance optimizations
    fn configure_connection(&self, conn: &Connection) -> Result<()> {
        // 1. Thread configuration
        let num_threads = if self.config.threads_per_connection == 0 {
            // Auto-detect: use all cores divided by pool size
            (num_cpus::get() / self.config.pool_size as usize).max(1)
        } else {
            self.config.threads_per_connection
        };

        conn.execute(&format!("PRAGMA threads={};", num_threads), [])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to set threads: {}", e)))?;

        // 2. Memory limit
        conn.execute(
            &format!("PRAGMA memory_limit='{}';", self.config.memory_limit),
            [],
        )
        .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to set memory limit: {}", e)))?;

        // 3. Enable object cache
        conn.execute("PRAGMA enable_object_cache;", [])
            .map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to enable object cache: {}", e))
            })?;

        // 4. OLAP optimizations
        conn.execute("PRAGMA default_null_order='NULLS LAST';", [])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to set null order: {}", e)))?;

        // 5. Aggressive optimizations (if enabled)
        if self.config.aggressive_optimizations {
            conn.execute("PRAGMA perfect_ht_threshold=12;", []).ok();
            conn.execute("PRAGMA force_parallelism=true;", []).ok();
        }

        // 6. Performance optimizations
        // Temp directory for spilling (prevents OOM on large aggregations)
        if let Some(temp_dir) = &self.config.temp_directory {
            // Create temp directory if it doesn't exist
            std::fs::create_dir_all(temp_dir).ok();
            conn.execute(
                &format!("PRAGMA temp_directory='{}';", temp_dir.display()),
                [],
            )
            .map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to set temp_directory: {}", e))
            })?;
            debug!("DuckDB temp_directory set to: {}", temp_dir.display());
        }

        // Preserve insertion order (false = better parallelism for large datasets)
        let preserve_order = if self.config.preserve_insertion_order {
            "true"
        } else {
            "false"
        };
        conn.execute(
            &format!("PRAGMA preserve_insertion_order={};", preserve_order),
            [],
        )
        .map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to set preserve_insertion_order: {}",
                e
            ))
        })?;

        // 7. Install and load Delta extension
        conn.execute("INSTALL delta;", []).ok(); // May fail if already installed
        conn.execute("LOAD delta;", []).map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to load Delta extension: {}", e))
        })?;

        debug!(
            "Configured DuckDB connection: {} threads, {} memory",
            num_threads, self.config.memory_limit
        );

        Ok(())
    }
}

impl ManageConnection for DuckDBConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> Result<Connection> {
        // Open connection (persistent or in-memory)
        let conn = if let Some(db_path) = &self.config.db_path {
            // Create directory if needed
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::StorageError(anyhow::anyhow!("Failed to create DB directory: {}", e))
                })?;
            }

            debug!("Opening persistent DuckDB connection at: {:?}", db_path);
            Connection::open(db_path)
                .map_err(|e| Error::StorageError(anyhow::anyhow!("Failed to open DuckDB: {}", e)))?
        } else {
            debug!("Opening in-memory DuckDB connection");
            Connection::open_in_memory().map_err(|e| {
                Error::StorageError(anyhow::anyhow!("Failed to create in-memory DuckDB: {}", e))
            })?
        };

        // Configure the connection
        self.configure_connection(&conn)?;

        debug!("Successfully created and configured DuckDB connection");
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Connection) -> Result<()> {
        // Health check: execute a simple query
        conn.execute("SELECT 1", [])
            .map_err(|e| Error::StorageError(anyhow::anyhow!("Connection unhealthy: {}", e)))?;
        debug!("Connection health check passed");
        Ok(())
    }

    fn has_broken(&self, conn: &mut Connection) -> bool {
        // Check if connection is broken
        conn.execute("SELECT 1", []).is_err()
    }
}

/// Type alias for DuckDB connection pool
pub type DuckDBPool = Pool<DuckDBConnectionManager>;

/// Type alias for pooled DuckDB connection
pub type PooledDuckDBConnection = PooledConnection<DuckDBConnectionManager>;

/// Create a DuckDB connection pool with the given configuration
///
/// # Arguments
/// * `config` - Pool configuration
///
/// # Returns
/// A configured connection pool ready for concurrent operations
///
/// # Example
/// ```rust,no_run
/// use featureduck_delta::pool::{DuckDBPoolConfig, create_duckdb_pool};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = DuckDBPoolConfig::default();
///     let pool = create_duckdb_pool(config)?;
///
///     // Get a connection from the pool
///     let conn = pool.get()?;
///
///     // Use the connection
///     conn.execute("SELECT * FROM my_table LIMIT 10", [])?;
///
///     Ok(())
/// # }
/// ```
pub fn create_duckdb_pool(config: DuckDBPoolConfig) -> Result<DuckDBPool> {
    info!(
        "Creating DuckDB connection pool: {} connections, {} memory per connection",
        config.pool_size, config.memory_limit
    );

    let manager = DuckDBConnectionManager::new(config.clone());

    let pool = Pool::builder()
        .max_size(config.pool_size)
        .connection_timeout(std::time::Duration::from_secs(
            config.connection_timeout_secs,
        ))
        .build(manager)
        .map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to create connection pool: {}", e))
        })?;

    info!("✅ DuckDB connection pool created successfully");

    Ok(pool)
}

/// Convert DuckDBEngineConfig to DuckDBPoolConfig
impl From<DuckDBEngineConfig> for DuckDBPoolConfig {
    fn from(engine_config: DuckDBEngineConfig) -> Self {
        let pool_size = ((num_cpus::get() / 2).max(4)) as u32;

        Self {
            db_path: engine_config.db_path,
            pool_size,
            threads_per_connection: engine_config.threads,
            memory_limit: engine_config.memory_limit,
            temp_directory: engine_config.temp_directory,
            preserve_insertion_order: engine_config.preserve_insertion_order,
            connection_timeout_secs: 10,
            aggressive_optimizations: engine_config.aggressive_optimizations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let config = DuckDBPoolConfig {
            db_path: None, // In-memory for testing
            pool_size: 2,
            threads_per_connection: 2,
            memory_limit: "1GB".to_string(),
            connection_timeout_secs: 5,
            aggressive_optimizations: false,
            temp_directory: None,
            preserve_insertion_order: true,
        };

        let pool = create_duckdb_pool(config);
        assert!(pool.is_ok(), "Pool creation should succeed");

        let pool = pool.unwrap();
        assert_eq!(pool.max_size(), 2, "Max pool size should be 2");
    }

    #[test]
    fn test_pool_get_connection() {
        let config = DuckDBPoolConfig {
            db_path: None,
            pool_size: 2,
            threads_per_connection: 1,
            memory_limit: "512MB".to_string(),
            connection_timeout_secs: 5,
            aggressive_optimizations: false,
            temp_directory: None,
            preserve_insertion_order: true,
        };

        let pool = create_duckdb_pool(config).unwrap();

        // Get a connection from the pool
        let conn = pool.get();
        assert!(conn.is_ok(), "Getting connection from pool should succeed");

        let conn = conn.unwrap();

        // Verify connection works
        let result = conn.execute("SELECT 1", []);
        assert!(result.is_ok(), "Connection should execute queries");
    }

    #[test]
    fn test_pool_connection_reuse() {
        let config = DuckDBPoolConfig {
            db_path: None,
            pool_size: 1,
            threads_per_connection: 1,
            memory_limit: "512MB".to_string(),
            connection_timeout_secs: 5,
            aggressive_optimizations: false,
            temp_directory: None,
            preserve_insertion_order: true,
        };

        let pool = create_duckdb_pool(config).unwrap();

        // Get a connection, use it, then drop it
        {
            let conn = pool.get().unwrap();
            conn.execute("SELECT 1", []).unwrap();
        } // Connection returned to pool here

        // Get the same connection again (should be recycled)
        let conn = pool.get().unwrap();
        let result = conn.execute("SELECT 2", []);
        assert!(result.is_ok(), "Recycled connection should work");
    }
}
