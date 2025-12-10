//! Delta Lake connector implementation using delta-rs + DuckDB
//!
//! ## Architecture (Hybrid Approach):
//! Combines delta-rs and DuckDB for optimal read/write performance.
//!
//! **delta-rs (Write path + Transaction management):**
//! - Write/commit Delta Lake tables (ACID transactions)
//! - Manage transaction log and table metadata
//! - Provides Parquet file paths for reads
//!
//! **DuckDB (Read path optimization):**
//! - Reads Parquet files directly (no Arrow bridge needed!)
//! - QUALIFY clause for efficient point-in-time deduplication
//! - Columnar execution with predicate pushdown
//! - Performance: O(log n) vs O(n) for point-in-time queries
//!
//! **Point-in-time correctness:**
//! - Event-level timestamps stored in `__timestamp` column
//! - DuckDB QUALIFY clause: ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
//! - This gives latest feature row per entity where timestamp <= as_of

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::DeltaOps;
use deltalake::DeltaTableBuilder;
// Query cache removed - LogicalPlan deleted, using SQLFrame instead
use featureduck_core::resource_limits::ResourceLimits;
use featureduck_core::retry::{retry_async, RetryPolicy};
use featureduck_core::state::StateManager;
use featureduck_core::{
    EntityKey, FeatureRow, FeatureValue, FeatureView, Result, StorageConnector,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

// Import observability functions
use crate::observability::{
    increment_read_errors, increment_read_rows, increment_write_errors, increment_write_rows,
    time_materialization, time_read_operation, time_write_operation,
};

/// "Infinity" timestamp for queries without as_of constraint (year 2200)
const INFINITY_TIMESTAMP: i64 = 7258118400; // 2200-01-01 00:00:00 UTC

/// Maximum number of state versions to keep for incremental processing
#[allow(dead_code)] // Will be used in incremental state cleanup
const MAX_STATE_VERSIONS: i64 = 10;

/// DuckDB engine configuration for optimal performance
///
/// ## Key Performance Settings
///
/// - `temp_directory`: Essential for spilling intermediate results to disk (prevents OOM)
/// - `threads`: Set to 0 for auto-detect (uses all available cores)
/// - `memory_limit`: 75% of system RAM (auto-detected by default)
/// - `preserve_insertion_order`: false for better parallelism
///
/// ```rust,ignore
/// let config = DuckDBEngineConfig {
///     temp_directory: Some("/fast-ssd/tmp".into()),
///     threads: 0, // auto-detect
///     memory_limit: "64GB".to_string(),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct DuckDBEngineConfig {
    /// Path to persistent DuckDB database file
    /// If None, uses in-memory database
    pub db_path: Option<PathBuf>,

    /// Thread count (0 = auto-detect all available cores)
    pub threads: usize,

    /// Memory limit (e.g., "4GB", "8GB", "16GB")
    /// Note: DuckDB does not support percentage values
    pub memory_limit: String,

    /// Enable aggressive query optimizations
    pub aggressive_optimizations: bool,

    /// Enable partition pruning for date-partitioned tables
    ///
    /// When enabled, incremental materialization will add a `date_partition >= 'YYYY-MM-DD'`
    /// filter based on the timestamp filter. This allows DuckDB to skip irrelevant partitions
    /// when querying Delta Lake tables partitioned by date.
    ///
    /// **Only enable this if your Delta Lake tables are partitioned by a `date_partition` column!**
    ///
    /// Expected impact: 10-50ms savings for tables with many partitions
    pub enable_partition_pruning: bool,

    // ========================================================================
    // Performance Optimizations
    // ========================================================================

    /// Directory for spilling intermediate results to disk
    ///
    /// Without this, DuckDB will OOM on large aggregations, joins, or sorts
    /// that exceed memory_limit. Always configure for production workloads.
    ///
    /// Best practice: Use fast SSD path (NVMe preferred)
    /// Example: "/tmp/duckdb" or "/mnt/fast-ssd/duckdb-temp"
    pub temp_directory: Option<PathBuf>,

    /// Disable insertion order preservation for better parallelism
    ///
    /// When false, DuckDB can parallelize operations more aggressively.
    /// Only set to true if your application requires deterministic row order.
    /// Default: false (better performance)
    pub preserve_insertion_order: bool,

    /// Maximum number of rows to fetch in a single batch for streaming reads
    ///
    /// Smaller batches = lower memory, more overhead
    /// Larger batches = higher memory, better throughput
    /// Default: 100,000 rows
    pub streaming_batch_size: usize,

    /// Maximum entities per IN() clause before batching
    ///
    /// Large IN() clauses can cause query planning overhead.
    /// When entity count exceeds this, queries are batched.
    /// Default: 10,000 entities
    pub max_entities_per_query: usize,
}

/// Auto-detect system memory and allocate 75% for DuckDB
///
/// This ensures optimal performance across different hardware configurations:
/// - 64GB RAM → 48GB for DuckDB
/// - 32GB RAM → 24GB for DuckDB
/// - 16GB RAM → 12GB for DuckDB
/// - 8GB RAM → 6GB for DuckDB
///
/// Minimum: 1GB (safety floor for low-memory systems)
fn auto_detect_memory_limit() -> String {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_memory();

    let total_memory_bytes = sys.total_memory();
    let total_memory_gb = total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    // Allocate 75% of system memory for DuckDB buffer pool
    let memory_for_duckdb_gb = (total_memory_gb * 0.75).floor() as u64;
    let memory_limit = memory_for_duckdb_gb.max(1); // Minimum 1GB

    info!(
        "Auto-detected system memory: {:.1}GB (using 75% = {}GB for DuckDB buffer pool)",
        total_memory_gb, memory_limit
    );

    format!("{}GB", memory_limit)
}

impl Default for DuckDBEngineConfig {
    fn default() -> Self {
        Self {
            db_path: Some(PathBuf::from(".featureduck/duckdb.db")),
            threads: 0,                               // Auto-detect
            memory_limit: auto_detect_memory_limit(), // Auto-detect 75% of system RAM
            aggressive_optimizations: true,
            enable_partition_pruning: false, // Disabled by default (tables may not be partitioned)
            // Performance optimizations
            temp_directory: Some(PathBuf::from("/tmp/featureduck-duckdb")), // Spill to disk
            preserve_insertion_order: false,   // Better parallelism
            streaming_batch_size: 100_000,     // 100K rows per batch
            max_entities_per_query: 10_000,    // Batch IN() clauses
        }
    }
}

impl DuckDBEngineConfig {
    /// Configuration optimized for high-throughput workloads
    ///
    /// - Large memory allocation (auto-detect 75% of system RAM)
    /// - Temp directory for spilling (required for large aggregations)
    /// - Parallelism enabled (preserve_insertion_order = false)
    /// - Larger streaming batches (500K rows)
    pub fn for_high_throughput() -> Self {
        Self {
            temp_directory: Some(PathBuf::from("/tmp/featureduck-duckdb-prod")),
            preserve_insertion_order: false,
            streaming_batch_size: 500_000,     // 500K rows per batch
            max_entities_per_query: 50_000,    // Larger batches
            ..Default::default()
        }
    }

    /// Configuration for development/testing (minimal resources)
    pub fn for_development() -> Self {
        Self {
            db_path: None,                     // In-memory
            memory_limit: "2GB".to_string(),
            temp_directory: None,              // No spilling needed
            preserve_insertion_order: true,    // Deterministic for tests
            streaming_batch_size: 10_000,
            max_entities_per_query: 1_000,
            ..Default::default()
        }
    }
}

/// Delta Lake storage connector using delta-rs + DuckDB with connection pooling
///
/// This connector uses:
/// - delta-rs for reading/writing (ACID transactions)
/// - DuckDB connection pool for concurrent operations (3-8x throughput)
/// - Circuit breaker for resilience against storage failures
/// - Delta Lake tables can be local or remote (S3/GCS/Azure)
///
/// ## Concurrency
/// The connector uses a DuckDB connection pool to enable true parallelism.
/// Multiple materializations can run concurrently without blocking each other.
///
/// ## Resilience (P0-8)
/// The circuit breaker protects against cascading failures when storage is unavailable.
/// When the circuit is open, requests fail fast without attempting storage operations.
pub struct DeltaStorageConnector {
    delta_path: String,
    /// DuckDB connection pool for concurrent Parquet queries (replaces single connection)
    duckdb_pool: crate::pool::DuckDBPool,
    /// DuckDB engine configuration for optimization settings
    #[allow(dead_code)] // Stored for future reference, config applied at connection creation
    engine_config: DuckDBEngineConfig,
    /// Write configuration for performance tuning
    write_config: crate::write_optimized::WriteConfig,
    /// State manager for incremental processing (optional)
    state_manager: Option<Arc<StateManager>>,
    /// Feature registry for schema validation (optional)
    registry: Option<Arc<featureduck_registry::FeatureRegistry>>,
    /// Retry policy for transient failures (OCC conflicts, network errors)
    retry_policy: RetryPolicy,
    /// Resource limits to prevent OOM and runaway queries
    resource_limits: ResourceLimits,
    /// Circuit breaker for resilience (P0-8)
    circuit_breaker: Arc<crate::circuit_breaker::CircuitBreaker>,
}

impl DeltaStorageConnector {
    /// Creates a new Delta Lake connector
    ///
    /// # Arguments
    /// * `path` - Base path to Delta tables (e.g., "s3://bucket/features" or "/local/features")
    /// * `engine_config` - DuckDB engine configuration for performance tuning
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     // Use default optimized configuration
    ///     let connector = DeltaStorageConnector::new(
    ///         "/data/features",
    ///         DuckDBEngineConfig::default()
    ///     ).await?;
    ///     
    ///     // Or customize for specific needs
    ///     let custom_config = DuckDBEngineConfig {
    ///         db_path: Some(PathBuf::from("/fast-ssd/duckdb.db")),
    ///         threads: 16,
    ///         memory_limit: "32GB".to_string(),
    ///         aggressive_optimizations: true,
    ///         enable_partition_pruning: false,
    ///     };
    ///     let connector = DeltaStorageConnector::new("s3://bucket/features", custom_config).await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn new(path: &str, engine_config: DuckDBEngineConfig) -> Result<Self> {
        info!("Initializing Delta Lake connector at: {}", path);
        info!("DuckDB engine config: {:?}", engine_config);

        // Create base directory if local path
        if !path.starts_with("s3://") && !path.starts_with("gs://") && !path.starts_with("azure://")
        {
            let base_path = PathBuf::from(path);
            std::fs::create_dir_all(&base_path)
                .map_err(|e| featureduck_core::Error::StorageError(e.into()))?;
        }

        // Create DuckDB connection pool for concurrent operations (synchronous)
        let pool_config = crate::pool::DuckDBPoolConfig::from(engine_config.clone());
        let duckdb_pool = crate::pool::create_duckdb_pool(pool_config)?;

        // Initialize circuit breaker for resilience (P0-8)
        let circuit_breaker = Arc::new(crate::circuit_breaker::CircuitBreaker::with_defaults(
            "delta_storage",
        ));

        info!("Delta Lake connector initialized successfully with DuckDB connection pool ({} connections)", duckdb_pool.max_size());

        Ok(Self {
            delta_path: path.to_string(),
            duckdb_pool,
            engine_config,
            write_config: crate::write_optimized::WriteConfig::default(),
            state_manager: None,
            registry: None,
            retry_policy: RetryPolicy::default(),
            resource_limits: ResourceLimits::default(),
            circuit_breaker,
        })
    }

    /// Creates a new Delta Lake connector with feature registry for schema validation
    ///
    /// This constructor enables schema validation by providing a reference to the
    /// feature registry. When writing features, the connector will validate that
    /// feature names and entity keys match the registered schema.
    ///
    /// # Arguments
    /// * `path` - Base path to Delta tables
    /// * `engine_config` - DuckDB engine configuration
    /// * `registry` - Feature registry for schema validation
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
    /// use featureduck_registry::FeatureRegistry;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let registry = Arc::new(FeatureRegistry::in_memory().await?);
    ///     let connector = DeltaStorageConnector::new_with_registry(
    ///         "/data/features",
    ///         DuckDBEngineConfig::default(),
    ///         registry
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn new_with_registry(
        path: &str,
        engine_config: DuckDBEngineConfig,
        registry: Arc<featureduck_registry::FeatureRegistry>,
    ) -> Result<Self> {
        info!(
            "Initializing Delta Lake connector with registry at: {}",
            path
        );
        info!("DuckDB engine config: {:?}", engine_config);

        if !path.starts_with("s3://") && !path.starts_with("gs://") && !path.starts_with("azure://")
        {
            let base_path = PathBuf::from(path);
            std::fs::create_dir_all(&base_path)
                .map_err(|e| featureduck_core::Error::StorageError(e.into()))?;
        }

        // Create DuckDB connection pool for concurrent operations (synchronous)
        let pool_config = crate::pool::DuckDBPoolConfig::from(engine_config.clone());
        let duckdb_pool = crate::pool::create_duckdb_pool(pool_config)?;

        // Initialize circuit breaker for resilience (P0-8)
        let circuit_breaker = Arc::new(crate::circuit_breaker::CircuitBreaker::with_defaults(
            "delta_storage",
        ));

        info!("Delta Lake connector initialized successfully with registry and DuckDB connection pool ({} connections)", duckdb_pool.max_size());

        Ok(Self {
            delta_path: path.to_string(),
            duckdb_pool,
            engine_config,
            write_config: crate::write_optimized::WriteConfig::default(),
            state_manager: None,
            registry: Some(registry),
            retry_policy: RetryPolicy::default(),
            resource_limits: ResourceLimits::default(),
            circuit_breaker,
        })
    }

    /// Helper to get the full path to a feature table
    fn table_path(&self, feature_view: &str) -> String {
        format!("{}/{}", self.delta_path, feature_view)
    }

    /// Configure retry policy for transient failures
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::DeltaStorageConnector;
    /// use featureduck_core::retry::RetryPolicy;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connector = DeltaStorageConnector::new("/tmp/features", Default::default()).await?;
    /// connector.set_retry_policy(RetryPolicy::aggressive());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_retry_policy(&mut self, policy: RetryPolicy) {
        self.retry_policy = policy;
    }

    /// Configure resource limits to prevent OOM and runaway queries
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::DeltaStorageConnector;
    /// use featureduck_core::resource_limits::ResourceLimits;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connector = DeltaStorageConnector::new("/tmp/features", Default::default()).await?;
    /// connector.set_resource_limits(ResourceLimits::strict());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_resource_limits(&mut self, limits: ResourceLimits) {
        self.resource_limits = limits;
    }

    /// Configure circuit breaker for resilience (P0-8)
    ///
    /// The circuit breaker protects against cascading failures when storage is unavailable.
    /// When too many failures occur, the circuit opens and subsequent requests fail fast.
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::{DeltaStorageConnector, CircuitBreakerConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connector = DeltaStorageConnector::new("/tmp/features", Default::default()).await?;
    /// connector.set_circuit_breaker_config(CircuitBreakerConfig::strict());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_circuit_breaker_config(
        &mut self,
        config: crate::circuit_breaker::CircuitBreakerConfig,
    ) {
        self.circuit_breaker = Arc::new(crate::circuit_breaker::CircuitBreaker::new(
            "delta_storage",
            config,
        ));
    }

    /// Get circuit breaker statistics for monitoring
    pub fn circuit_breaker_stats(&self) -> crate::circuit_breaker::CircuitStats {
        self.circuit_breaker.stats()
    }

    /// Reset the circuit breaker to closed state
    ///
    /// Use with caution - typically for testing or manual recovery
    pub fn reset_circuit_breaker(&self) {
        self.circuit_breaker.reset();
    }

    /// Get the base path for Delta tables
    pub fn base_path(&self) -> &str {
        &self.delta_path
    }

    /// Write features in batches for large datasets
    ///
    /// Splits large writes into smaller batches to prevent memory exhaustion
    /// and handle "too many open files" errors gracefully. Each batch is written
    /// independently with retry logic, so partial progress is preserved.
    ///
    /// # Arguments
    /// * `feature_view` - Name of the feature view
    /// * `rows` - Feature rows to write
    /// * `batch_size` - Optional batch size (defaults to write_config.chunk_size)
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::DeltaStorageConnector;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let connector = DeltaStorageConnector::new("/tmp/features", Default::default()).await?;
    ///
    /// // Write 1 million rows in 50K batches
    /// let rows = generate_large_dataset();
    /// connector.write_features_batched("user_features", rows, Some(50_000)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_features_batched(
        &self,
        feature_view: &str,
        rows: Vec<FeatureRow>,
        batch_size: Option<usize>,
    ) -> Result<()> {
        let batch_size = batch_size.unwrap_or(self.write_config.chunk_size);
        let total_rows = rows.len();

        if total_rows <= batch_size {
            // Small enough for single write
            return self.write_features(feature_view, rows).await;
        }

        info!(
            "Writing {} rows in batches of {} to {}",
            total_rows, batch_size, feature_view
        );

        let num_batches = total_rows.div_ceil(batch_size);
        let mut rows_written = 0;

        for (batch_idx, batch) in rows.chunks(batch_size).enumerate() {
            debug!(
                "Writing batch {}/{} ({} rows)",
                batch_idx + 1,
                num_batches,
                batch.len()
            );

            // Clone batch since chunks() gives &[FeatureRow]
            self.write_features(feature_view, batch.to_vec()).await?;

            rows_written += batch.len();

            if (batch_idx + 1) % 10 == 0 {
                info!(
                    "Progress: {}/{} batches ({} rows written)",
                    batch_idx + 1,
                    num_batches,
                    rows_written
                );
            }
        }

        info!(
            "Completed batched write: {} rows in {} batches to {}",
            total_rows, num_batches, feature_view
        );

        Ok(())
    }

    /// Health check - verify DuckDB connection is working
    ///
    /// Performs a lightweight query to verify the DuckDB connection pool
    /// is healthy and responsive.
    ///
    /// # Returns
    /// * `Ok(())` - Connection is healthy
    /// * `Err(_)` - Connection is unhealthy
    pub async fn health_check(&self) -> Result<()> {
        let pool = self.duckdb_pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool
                .get()
                .map_err(|e| featureduck_core::Error::StorageError(e.into()))?;

            // Simple health check query
            conn.execute("SELECT 1", [])
                .map_err(|e| featureduck_core::Error::StorageError(e.into()))?;

            Ok(())
        })
        .await
        .map_err(|e| featureduck_core::Error::StorageError(e.into()))?
    }

    /// Configure write settings including compression
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::{DeltaStorageConnector, WriteConfig, CompressionCodec};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connector = DeltaStorageConnector::new("/tmp/features", Default::default()).await?;
    ///
    /// // Use maximum compression for cold storage
    /// connector.set_write_config(WriteConfig {
    ///     compression: CompressionCodec::smallest(),
    ///     chunk_size: 100_000,
    ///     parallel_enabled: true,
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_write_config(&mut self, config: crate::write_optimized::WriteConfig) {
        info!(
            "Configured write settings: compression={:?}, chunk_size={}, parallel={}",
            config.compression, config.chunk_size, config.parallel_enabled
        );
        self.write_config = config;
    }

    /// Get current write configuration
    pub fn write_config(&self) -> &crate::write_optimized::WriteConfig {
        &self.write_config
    }

    /// Get Parquet writer properties based on configured compression
    ///
    /// Converts our WriteConfig compression setting to arrow-rs WriterProperties
    /// for optimal Parquet file compression.
    fn get_writer_properties(&self) -> parquet::file::properties::WriterProperties {
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let compression = match self.write_config.compression {
            crate::write_optimized::CompressionCodec::Uncompressed => Compression::UNCOMPRESSED,
            crate::write_optimized::CompressionCodec::Snappy => Compression::SNAPPY,
            crate::write_optimized::CompressionCodec::ZstdLevel1 => {
                Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1).unwrap())
            }
            crate::write_optimized::CompressionCodec::ZstdLevel3 => {
                Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap())
            }
            crate::write_optimized::CompressionCodec::ZstdLevel9 => {
                Compression::ZSTD(parquet::basic::ZstdLevel::try_new(9).unwrap())
            }
            crate::write_optimized::CompressionCodec::Lz4 => Compression::LZ4,
        };

        debug!(
            "Using Parquet compression: {:?} (codec: {:?})",
            compression, self.write_config.compression
        );

        WriterProperties::builder()
            .set_compression(compression)
            // Optimize for analytical queries (larger row groups, better compression)
            .set_max_row_group_size(1024 * 1024) // 1M rows per row group
            .set_data_page_size_limit(1024 * 1024) // 1MB page size for better compression
            .set_dictionary_enabled(true) // Enable dictionary encoding for strings
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .build()
    }

    /// Enable incremental processing with state management
    ///
    /// # Arguments
    /// * `state_db_path` - Path to DuckDB state database
    ///
    /// # Example
    /// ```no_run
    /// use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connector = DeltaStorageConnector::new("/tmp/features", DuckDBEngineConfig::default()).await?;
    /// connector.enable_incremental("/tmp/feature_state.db")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn enable_incremental<P: AsRef<std::path::Path>>(
        &mut self,
        state_db_path: P,
    ) -> Result<()> {
        let state_manager = StateManager::new(state_db_path)?;
        self.state_manager = Some(Arc::new(state_manager));
        Ok(())
    }

    /// Set feature registry for stats collection and schema validation
    ///
    /// # Arguments
    /// * `registry` - Feature registry instance
    pub fn set_registry(&mut self, registry: Arc<featureduck_registry::FeatureRegistry>) {
        self.registry = Some(registry);
    }

    /// Report connection pool statistics to observability metrics
    ///
    /// Exports active and idle connection counts to Prometheus
    fn report_pool_metrics(&self) {
        let state = self.duckdb_pool.state();
        let active = state.connections - state.idle_connections;

        crate::observability::set_active_connections(active as usize);
        crate::observability::set_idle_connections(state.idle_connections as usize);

        debug!(
            "Pool metrics: active={}, idle={}, total={}",
            active, state.idle_connections, state.connections
        );
    }

    /// Execute a SQL transformation and materialize results to Delta Lake
    ///
    /// This path bypasses LogicalPlan entirely. It assumes `sql` produces a result set
    /// containing the provided `entity_columns` plus any number of feature columns.
    /// Entity columns will be extracted verbatim; all other columns will be treated as features.
    pub async fn materialize_from_sql(
        &self,
        feature_view: &str,
        sql: &str,
        entity_columns: &[String],
    ) -> Result<()> {
        info!("Materializing from raw SQL for view: {}", feature_view);

        // Start timing
        let _timer = time_materialization(feature_view);

        // Get connection from pool (non-blocking, enables concurrency)
        let duckdb = self.duckdb_pool.get().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Failed to get DuckDB connection from pool: {}",
                e
            ))
        })?;

        // Report pool metrics to observability
        self.report_pool_metrics();

        let mut stmt = duckdb.prepare(sql).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("Failed to prepare SQL: {}", e))
        })?;

        let mut rows_result = stmt.query([]).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("Failed to execute SQL: {}", e))
        })?;

        // Column metadata
        let column_count = rows_result
            .as_ref()
            .ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Query result is None"))
            })?
            .column_count();
        let column_names: Vec<String> = (0..column_count)
            .filter_map(|i| {
                rows_result
                    .as_ref()
                    .and_then(|r| r.column_name(i).ok())
                    .map(|s| s.to_string())
            })
            .collect();

        // Rows → FeatureRows
        // Pre-allocate with estimated capacity to avoid repeated reallocations
        // Typical SQL queries return 100-10K rows, so 1024 is a good middle ground
        let mut feature_rows = Vec::with_capacity(1024);
        while let Some(row) = rows_result.next().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("Failed to fetch row: {}", e))
        })? {
            let feature_row =
                self.duckdb_row_to_feature_row_with_entities(row, entity_columns, &column_names)?;
            feature_rows.push(feature_row);
        }

        // Connection automatically returned to pool when dropped

        let row_count = feature_rows.len();
        info!("SQL produced {} feature rows", row_count);

        if row_count > 0 {
            self.write_features(feature_view, feature_rows.clone())
                .await?;

            // Save stats if registry available
            if let Some(_registry) = &self.registry {
                if let Err(e) = self
                    .compute_and_save_stats(feature_view, &feature_rows)
                    .await
                {
                    tracing::warn!("Failed to save stats for view {}: {}", feature_view, e);
                }
            }
        } else {
            info!("No features to materialize (empty result set)");
        }

        Ok(())
    }

    /// Compute and save feature view statistics to registry
    ///
    /// Stats enable adaptive execution by providing cost estimation data for engine selection.
    async fn compute_and_save_stats(
        &self,
        feature_view: &str,
        feature_rows: &[FeatureRow],
    ) -> Result<()> {
        use featureduck_registry::FeatureViewStats;
        use std::collections::HashSet;

        if feature_rows.is_empty() {
            return Ok(());
        }

        let registry = self.registry.as_ref().ok_or_else(|| {
            featureduck_core::Error::InvalidInput("Registry not available".to_string())
        })?;

        // 1. Row count
        let row_count = feature_rows.len() as i64;

        // 2. Distinct entities
        let distinct_entities: HashSet<String> = feature_rows
            .iter()
            .map(|row| {
                row.entities
                    .iter()
                    .map(|e| format!("{}={}", e.name, e.value))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();
        let distinct_entity_count = distinct_entities.len() as i64;

        // 3. Timestamp range (min/max)
        let timestamps: Vec<i64> = feature_rows
            .iter()
            .map(|row| row.timestamp.timestamp_micros())
            .collect();
        let min_timestamp = timestamps.iter().min().copied();
        let max_timestamp = timestamps.iter().max().copied();

        // 4. Estimate total size (rough estimate based on serialized size)
        let estimated_row_size = 256; // Average bytes per row (conservative estimate)
        let total_size_bytes = row_count * estimated_row_size;

        // 5. Get feature view definition for version
        let view_def = registry.get_feature_view(feature_view).await?;

        let stats = FeatureViewStats {
            feature_view: feature_view.to_string(),
            version: view_def.version,
            row_count,
            distinct_entities: distinct_entity_count,
            min_timestamp,
            max_timestamp,
            avg_file_size_bytes: None, // Would need to read from Delta table metadata
            total_size_bytes,
            histogram_buckets: None, // TODO: Implement histogram computation
            created_at: Utc::now(),
        };

        registry.save_stats(&stats).await?;

        info!(
            "Stats captured for view '{}': {} rows, {} distinct entities, {:.2} MB estimated",
            feature_view,
            row_count,
            distinct_entity_count,
            total_size_bytes as f64 / 1_048_576.0
        );

        Ok(())
    }

    /// Convert a DuckDB row to FeatureRow using an explicit list of entity columns
    fn duckdb_row_to_feature_row_with_entities(
        &self,
        row: &duckdb::Row,
        entity_columns: &[String],
        column_names: &[String],
    ) -> Result<FeatureRow> {
        // Pre-allocate entities based on entity_columns count (small, but avoids reallocation)
        let mut entities = Vec::with_capacity(entity_columns.len().max(1));

        // Pre-allocate HashMap for features (total columns - entity columns)
        // Typical: 5-20 features per row, so this avoids 2-3 reallocations
        let feature_count = column_names.len().saturating_sub(entity_columns.len());
        let mut features = HashMap::with_capacity(feature_count);

        // Extract entities
        for entity_col in entity_columns {
            if column_names.contains(entity_col) {
                let value: String = row.get(entity_col.as_str()).map_err(|e| {
                    featureduck_core::Error::StorageError(anyhow::anyhow!(
                        "Failed to get entity column '{}': {}",
                        entity_col,
                        e
                    ))
                })?;
                entities.push(EntityKey::new(entity_col.clone(), value));
            }
        }

        if entities.is_empty() {
            entities.push(EntityKey::new(
                "__global__".to_string(),
                "global".to_string(),
            ));
        }

        // Extract features from all non-entity columns
        for col_name in column_names {
            if entity_columns.iter().any(|c| c == col_name) {
                continue;
            }
            if let Ok(v) = row.get::<_, Option<i64>>(col_name.as_str()) {
                features.insert(
                    col_name.clone(),
                    v.map(FeatureValue::Int).unwrap_or(FeatureValue::Null),
                );
            } else if let Ok(v) = row.get::<_, Option<f64>>(col_name.as_str()) {
                features.insert(
                    col_name.clone(),
                    v.map(FeatureValue::Float).unwrap_or(FeatureValue::Null),
                );
            } else if let Ok(v) = row.get::<_, Option<String>>(col_name.as_str()) {
                features.insert(
                    col_name.clone(),
                    v.map(FeatureValue::String).unwrap_or(FeatureValue::Null),
                );
            } else if let Ok(v) = row.get::<_, Option<bool>>(col_name.as_str()) {
                features.insert(
                    col_name.clone(),
                    v.map(FeatureValue::Bool).unwrap_or(FeatureValue::Null),
                );
            } else {
                features.insert(col_name.clone(), FeatureValue::Null);
            }
        }

        Ok(FeatureRow {
            entities,
            features,
            timestamp: Utc::now(),
        })
    }

    /// Read features using DuckDB delta_scan() for optimized queries
    ///
    /// Uses DuckDB's native Delta extension for maximum performance.
    ///
    /// **Performance Optimizations (via delta_scan):**
    /// - ✅ Native partition pruning (Delta statistics)
    /// - ✅ File skipping via min/max statistics
    /// - ✅ Deletion vector support
    /// - ✅ QUALIFY clause for O(log n) deduplication
    /// - ✅ Predicate pushdown to Parquet
    /// - ✅ Entity batching (splits large IN() clauses for better query plans)
    ///
    /// **How it works:**
    /// 1. delta_scan() reads Delta transaction log natively
    /// 2. DuckDB applies partition pruning and file skipping automatically
    /// 3. QUALIFY clause deduplicates to latest row per entity
    /// 4. Large entity lists are batched to avoid query planning overhead
    /// 5. Manual conversion: DuckDB rows → FeatureRow
    ///
    /// # Arguments
    /// * `table_path` - Path to Delta table
    /// * `entity_keys` - Entity keys to filter for
    /// * `as_of` - Point-in-time timestamp
    async fn read_features_with_delta_scan(
        &self,
        table_path: &str,
        entity_keys: &[EntityKey],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<FeatureRow>> {
        // ALWAYS use the self-healing batched path for resilience
        // This ensures OOM recovery even for small entity counts
        let max_entities = self.engine_config.max_entities_per_query;
        let initial_batch_size = max_entities.min(entity_keys.len());

        if entity_keys.len() > 1 {
            // Use self-healing batched read for any multi-entity query
            debug!(
                "Using self-healing read for {} entities (initial batch_size={})",
                entity_keys.len(),
                initial_batch_size
            );
            return self
                .read_features_batched(table_path, entity_keys, as_of, initial_batch_size)
                .await;
        }

        // Only single entity queries bypass batching (no point batching 1 entity)
        let duckdb = self.duckdb_pool.get().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Failed to get DuckDB connection from pool: {}",
                e
            ))
        })?;

        // Report pool metrics to observability
        self.report_pool_metrics();

        // Install and load Delta extension
        duckdb.execute("INSTALL delta;", []).ok();
        duckdb.execute("LOAD delta;", []).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Failed to load Delta extension. Ensure DuckDB 1.4+ is installed: {}",
                e
            ))
        })?;

        // Assuming single entity for now (can extend to composite keys later)
        let entity_column = &entity_keys[0].name;
        let entity_values: Vec<String> = entity_keys
            .iter()
            .map(|k| format!("'{}'", k.value.replace("'", "''"))) // SQL escape
            .collect();
        let entity_filter = entity_values.join(", ");

        // Convert timestamp to microseconds (Parquet timestamp format)
        let as_of_micros = as_of.timestamp_micros();

        // Use delta_scan() instead of manual file listing
        // This gives us partition pruning, file skipping, and deletion vectors for free
        let sql = format!(
            r#"
            SELECT *
            FROM delta_scan('{}')
            WHERE {} IN ({})
              AND epoch_us(__timestamp) <= {}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {}
                ORDER BY __timestamp DESC
            ) = 1
            "#,
            table_path.replace("'", "''"), // SQL escape
            entity_column,
            entity_filter,
            as_of_micros,
            entity_column
        );

        debug!("DuckDB delta_scan query: {}", sql);

        let mut stmt = duckdb.prepare(&sql).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("DuckDB prepare failed: {}", e))
        })?;

        let mut rows_result = stmt.query([]).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("DuckDB query failed: {}", e))
        })?;

        // Get column names dynamically from the result set
        let column_count = rows_result
            .as_ref()
            .ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Query result is None"))
            })?
            .column_count();
        let column_names: Vec<String> = (0..column_count)
            .filter_map(|i| {
                rows_result
                    .as_ref()
                    .and_then(|r| r.column_name(i).ok())
                    .map(|s| s.to_string())
            })
            .collect();

        debug!(
            "Query returned {} columns: {:?}",
            column_count, column_names
        );

        // Pre-allocate based on entity_keys length
        // QUALIFY clause ensures we get at most 1 row per entity key
        let mut feature_rows = Vec::with_capacity(entity_keys.len());

        while let Some(row) = rows_result.next().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "DuckDB row iteration failed: {}",
                e
            ))
        })? {
            // Single entity per row (with QUALIFY deduplication)
            let mut entities = Vec::with_capacity(1);

            // Pre-allocate HashMap for features (column_count - 2 for entity and timestamp)
            // Typical delta scan: 5-50 feature columns, avoids 2-4 HashMap reallocations
            let feature_capacity = column_names.len().saturating_sub(2);
            let mut features = HashMap::with_capacity(feature_capacity);

            // Extract timestamp
            let timestamp_micros: i64 = row.get("__timestamp").map_err(|e| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Failed to get timestamp: {}",
                    e
                ))
            })?;

            // Extract entity column
            let entity_value: String = row.get(entity_column.as_str()).map_err(|e| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Failed to get entity {}: {}",
                    entity_column,
                    e
                ))
            })?;
            entities.push(EntityKey::new(entity_column, entity_value));

            // Extract all feature columns dynamically (skip entity and timestamp)
            for (col_idx, col_name) in column_names.iter().enumerate() {
                if col_name == "__timestamp" || col_name == entity_column {
                    continue;
                }

                // Use get_ref to check the actual DuckDB type without coercion
                use duckdb::types::ValueRef;
                match row.get_ref(col_idx) {
                    Ok(ValueRef::Int(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Int(v as i64));
                    }
                    Ok(ValueRef::BigInt(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Int(v));
                    }
                    Ok(ValueRef::Double(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Float(v));
                    }
                    Ok(ValueRef::Text(v)) => {
                        let text = String::from_utf8_lossy(v).to_string();

                        // Try to parse as JSON if it looks like JSON (starts with { or [)
                        if text.starts_with('{') || text.starts_with('[') {
                            if let Ok(json_value) = serde_json::from_str(&text) {
                                features.insert(col_name.clone(), FeatureValue::Json(json_value));
                            } else {
                                features.insert(col_name.clone(), FeatureValue::String(text));
                            }
                        } else {
                            features.insert(col_name.clone(), FeatureValue::String(text));
                        }
                    }
                    Ok(ValueRef::Boolean(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Bool(v));
                    }
                    Ok(ValueRef::Date32(v)) => {
                        // Convert days since Unix epoch to NaiveDate
                        use chrono::NaiveDate;
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        if let Some(date) = epoch.checked_add_days(chrono::Days::new(v as u64)) {
                            features.insert(col_name.clone(), FeatureValue::Date(date));
                        } else {
                            features.insert(col_name.clone(), FeatureValue::Null);
                        }
                    }
                    Ok(ValueRef::List(_, _)) => {
                        // DuckDB list handling - Lists are complex types
                        // For now, store as JSON string representation
                        // TODO: Implement proper list parsing when DuckDB Rust bindings support it
                        debug!(
                            "List type detected for column {} - storing as Null for now",
                            col_name
                        );
                        features.insert(col_name.clone(), FeatureValue::Null);
                    }
                    Ok(ValueRef::Null) => {
                        features.insert(col_name.clone(), FeatureValue::Null);
                    }
                    _ => {
                        // Unknown or unsupported type
                        debug!("Unsupported DuckDB type for column {}", col_name);
                        features.insert(col_name.clone(), FeatureValue::Null);
                    }
                }
            }

            // Convert timestamp from microseconds to DateTime
            let secs = timestamp_micros / 1_000_000;
            let nsecs = ((timestamp_micros % 1_000_000) * 1000) as u32;
            let timestamp = DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Invalid timestamp"))
            })?;

            feature_rows.push(FeatureRow {
                entities,
                features,
                timestamp,
            });
        }

        info!(
            "delta_scan() returned {} rows (with native Delta optimizations)",
            feature_rows.len()
        );
        Ok(feature_rows)
    }

    /// Read features in batches with self-healing OOM recovery
    ///
    /// Splits entity keys into chunks and executes multiple queries.
    /// **Self-Healing**: If OOM occurs, automatically reduces batch size and retries.
    /// This ensures maximum performance while gracefully handling memory constraints.
    async fn read_features_batched(
        &self,
        table_path: &str,
        entity_keys: &[EntityKey],
        as_of: DateTime<Utc>,
        initial_batch_size: usize,
    ) -> Result<Vec<FeatureRow>> {
        let mut all_results = Vec::with_capacity(entity_keys.len());
        let mut current_batch_size = initial_batch_size;
        let min_batch_size = 100; // Minimum batch size before giving up
        let mut entities_remaining: Vec<&EntityKey> = entity_keys.iter().collect();
        let mut retry_count = 0;
        let max_retries_per_batch = 5;

        info!(
            "Reading {} entities with adaptive batching (initial batch_size={})",
            entity_keys.len(),
            current_batch_size
        );

        while !entities_remaining.is_empty() {
            let chunk_size = current_batch_size.min(entities_remaining.len());
            let chunk: Vec<EntityKey> = entities_remaining[..chunk_size]
                .iter()
                .map(|k| (*k).clone())
                .collect();

            debug!(
                "Processing batch: {} entities (batch_size={}, {} remaining)",
                chunk.len(),
                current_batch_size,
                entities_remaining.len()
            );

            let result = Box::pin(self.read_features_with_delta_scan_single(
                table_path,
                &chunk,
                as_of,
            ))
            .await;

            match result {
                Ok(batch_results) => {
                    // Success! Remove processed entities and reset retry count
                    all_results.extend(batch_results);
                    entities_remaining = entities_remaining[chunk_size..].to_vec();
                    retry_count = 0;

                    // Adaptive scale-up: If we've been successful, try increasing batch size
                    // (up to initial_batch_size) to improve throughput
                    if current_batch_size < initial_batch_size {
                        let new_batch_size = (current_batch_size * 3 / 2).min(initial_batch_size);
                        if new_batch_size > current_batch_size {
                            debug!(
                                "Self-healing: scaling up batch_size {} → {} after success",
                                current_batch_size, new_batch_size
                            );
                            current_batch_size = new_batch_size;
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string().to_lowercase();
                    let is_oom = err_str.contains("out of memory")
                        || err_str.contains("oom")
                        || err_str.contains("memory")
                        || err_str.contains("failed to pin block");

                    if is_oom && current_batch_size > min_batch_size {
                        // Self-healing: Reduce batch size and retry
                        let new_batch_size = (current_batch_size / 2).max(min_batch_size);
                        retry_count += 1;

                        warn!(
                            "Self-healing OOM recovery: reducing batch_size {} → {} (retry {}/{})",
                            current_batch_size, new_batch_size, retry_count, max_retries_per_batch
                        );

                        current_batch_size = new_batch_size;

                        if retry_count >= max_retries_per_batch {
                            // Too many retries, fail gracefully with clear message
                            return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                                "Self-healing exhausted: OOM persists even with batch_size={}. \
                                 Consider increasing DuckDB memory_limit or reducing query scope. \
                                 Last error: {}",
                                current_batch_size,
                                e
                            )));
                        }
                        // Retry same entities with smaller batch
                        continue;
                    } else {
                        // Non-OOM error or already at minimum batch size, propagate error
                        return Err(e);
                    }
                }
            }
        }

        info!(
            "Self-healing batched read complete: {} total rows (final batch_size={})",
            all_results.len(),
            current_batch_size
        );
        Ok(all_results)
    }

    /// Internal: Read features for a single batch (no further batching)
    async fn read_features_with_delta_scan_single(
        &self,
        table_path: &str,
        entity_keys: &[EntityKey],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<FeatureRow>> {
        let duckdb = self.duckdb_pool.get().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Failed to get DuckDB connection from pool: {}",
                e
            ))
        })?;

        // Report pool metrics to observability
        self.report_pool_metrics();

        // Install and load Delta extension
        duckdb.execute("INSTALL delta;", []).ok();
        duckdb.execute("LOAD delta;", []).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Failed to load Delta extension. Ensure DuckDB 1.4+ is installed: {}",
                e
            ))
        })?;

        // Assuming single entity for now (can extend to composite keys later)
        let entity_column = &entity_keys[0].name;
        let entity_values: Vec<String> = entity_keys
            .iter()
            .map(|k| format!("'{}'", k.value.replace("'", "''"))) // SQL escape
            .collect();
        let entity_filter = entity_values.join(", ");

        // Convert timestamp to microseconds (Parquet timestamp format)
        let as_of_micros = as_of.timestamp_micros();

        // Use delta_scan() for native optimizations
        let sql = format!(
            r#"
            SELECT *
            FROM delta_scan('{}')
            WHERE {} IN ({})
              AND epoch_us(__timestamp) <= {}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {}
                ORDER BY __timestamp DESC
            ) = 1
            "#,
            table_path.replace("'", "''"),
            entity_column,
            entity_filter,
            as_of_micros,
            entity_column
        );

        debug!("DuckDB delta_scan query (batch): {}", sql);

        let mut stmt = duckdb.prepare(&sql).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("DuckDB prepare failed: {}", e))
        })?;

        let mut rows_result = stmt.query([]).map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("DuckDB query failed: {}", e))
        })?;

        // Get column names dynamically from the result set
        let column_count = rows_result
            .as_ref()
            .ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Query result is None"))
            })?
            .column_count();
        let column_names: Vec<String> = (0..column_count)
            .filter_map(|i| {
                rows_result
                    .as_ref()
                    .and_then(|r| r.column_name(i).ok())
                    .map(|s| s.to_string())
            })
            .collect();

        let mut feature_rows = Vec::with_capacity(entity_keys.len());

        while let Some(row) = rows_result.next().map_err(|e| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "DuckDB row iteration failed: {}",
                e
            ))
        })? {
            let mut entities = Vec::with_capacity(1);
            let feature_capacity = column_names.len().saturating_sub(2);
            let mut features = HashMap::with_capacity(feature_capacity);

            let timestamp_micros: i64 = row.get("__timestamp").map_err(|e| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Failed to get timestamp: {}",
                    e
                ))
            })?;

            let entity_value: String = row.get(entity_column.as_str()).map_err(|e| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Failed to get entity {}: {}",
                    entity_column,
                    e
                ))
            })?;
            entities.push(EntityKey::new(entity_column, entity_value));

            // Extract all feature columns dynamically
            for (col_idx, col_name) in column_names.iter().enumerate() {
                if col_name == "__timestamp" || col_name == entity_column {
                    continue;
                }

                use duckdb::types::ValueRef;
                match row.get_ref(col_idx) {
                    Ok(ValueRef::BigInt(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Int(v));
                    }
                    Ok(ValueRef::Double(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Float(v));
                    }
                    Ok(ValueRef::Text(v)) => {
                        features.insert(
                            col_name.clone(),
                            FeatureValue::String(String::from_utf8_lossy(v).to_string()),
                        );
                    }
                    Ok(ValueRef::Boolean(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Bool(v));
                    }
                    Ok(ValueRef::Int(v)) => {
                        features.insert(col_name.clone(), FeatureValue::Int(v as i64));
                    }
                    Ok(ValueRef::Null) => {
                        features.insert(col_name.clone(), FeatureValue::Null);
                    }
                    _ => {
                        features.insert(col_name.clone(), FeatureValue::Null);
                    }
                }
            }

            let secs = timestamp_micros / 1_000_000;
            let nsecs = ((timestamp_micros % 1_000_000) * 1000) as u32;
            let timestamp = DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Invalid timestamp"))
            })?;

            feature_rows.push(FeatureRow {
                entities,
                features,
                timestamp,
            });
        }

        Ok(feature_rows)
    }

    fn arrow_field_to_delta_field(&self, field: &Field) -> Result<StructField> {
        let delta_type = match field.data_type() {
            DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
            DataType::Float64 => DeltaDataType::Primitive(PrimitiveType::Double),
            DataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
            DataType::Boolean => DeltaDataType::Primitive(PrimitiveType::Boolean),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                DeltaDataType::Primitive(PrimitiveType::Timestamp)
            }
            DataType::Date32 => DeltaDataType::Primitive(PrimitiveType::Date),
            DataType::List(inner_field) => {
                // Map Arrow List to Delta Array
                let inner_type = match inner_field.data_type() {
                    DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
                    DataType::Float64 => DeltaDataType::Primitive(PrimitiveType::Double),
                    DataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
                    _ => {
                        return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                            "Unsupported list item type: {:?}",
                            inner_field.data_type()
                        )))
                    }
                };
                DeltaDataType::Array(Box::new(deltalake::kernel::ArrayType::new(
                    inner_type, true, // contains_null
                )))
            }
            _ => {
                return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Unsupported data type: {:?}",
                    field.data_type()
                )))
            }
        };

        Ok(StructField::new(
            field.name().clone(),
            delta_type,
            field.is_nullable(),
        ))
    }

    #[allow(dead_code)]
    fn rows_to_record_batch(&self, rows: &[FeatureRow]) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Cannot create RecordBatch from empty rows"
            )));
        }

        let first_row = &rows[0];
        let entity_names: Vec<String> = first_row.entities.iter().map(|e| e.name.clone()).collect();
        let feature_names: Vec<String> = first_row.features.keys().cloned().collect();

        // Build schema: entities + features + timestamp
        // Pre-allocate exact capacity (entities + features + 1 for __timestamp)
        let mut fields = Vec::with_capacity(entity_names.len() + feature_names.len() + 1);
        for entity_name in &entity_names {
            fields.push(Field::new(entity_name, DataType::Utf8, false));
        }
        for feature_name in &feature_names {
            let data_type = match first_row.features.get(feature_name).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Feature '{}' not found in first row",
                    feature_name
                ))
            })? {
                FeatureValue::Int(_) => DataType::Int64,
                FeatureValue::Float(_) => DataType::Float64,
                FeatureValue::String(_) => DataType::Utf8,
                FeatureValue::Bool(_) => DataType::Boolean,
                FeatureValue::Null => DataType::Utf8,
                // Advanced types
                FeatureValue::Json(_) => DataType::Utf8, // Store JSON as string
                FeatureValue::ArrayInt(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                }
                FeatureValue::ArrayFloat(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                }
                FeatureValue::ArrayString(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                }
                FeatureValue::Date(_) => DataType::Date32,
            };
            fields.push(Field::new(feature_name, data_type, true));
        }
        // Add timestamp column (REQUIRED for point-in-time correctness)
        fields.push(Field::new(
            "__timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));

        let schema = Arc::new(Schema::new(fields));

        // Pre-allocate columns vector (same capacity as fields: entities + features + timestamp)
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(entity_names.len() + feature_names.len() + 1);

        for entity_name in &entity_names {
            let values: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.entities
                        .iter()
                        .find(|e| &e.name == entity_name)
                        .map(|e| e.value.clone())
                        .unwrap_or_default()
                })
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }

        for feature_name in &feature_names {
            let first_value = first_row.features.get(feature_name).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Feature '{}' not found in first row",
                    feature_name
                ))
            })?;
            let array: ArrayRef = match first_value {
                FeatureValue::Int(_) => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::Int(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                FeatureValue::Float(_) => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::Float(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                FeatureValue::String(_) => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::String(v)) => Some(v.clone()),
                            _ => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                FeatureValue::Bool(_) => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::Bool(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                FeatureValue::Json(_) => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::Json(v)) => {
                                Some(serde_json::to_string(v).unwrap_or_default())
                            }
                            _ => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                FeatureValue::Date(_) => {
                    use arrow::array::Date32Array;
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match row.features.get(feature_name) {
                            Some(FeatureValue::Date(date)) => {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let days = date.signed_duration_since(epoch).num_days();
                                Some(days as i32)
                            }
                            _ => None,
                        })
                        .collect();
                    Arc::new(Date32Array::from(values))
                }
                FeatureValue::ArrayInt(_) => {
                    use arrow::array::{Int64Builder, ListBuilder};
                    let mut builder = ListBuilder::new(Int64Builder::new());
                    for row in rows {
                        match row.features.get(feature_name) {
                            Some(FeatureValue::ArrayInt(arr)) => {
                                for &val in arr {
                                    builder.values().append_value(val);
                                }
                                builder.append(true);
                            }
                            _ => builder.append(false),
                        }
                    }
                    Arc::new(builder.finish())
                }
                FeatureValue::ArrayFloat(_) => {
                    use arrow::array::{Float64Builder, ListBuilder};
                    let mut builder = ListBuilder::new(Float64Builder::new());
                    for row in rows {
                        match row.features.get(feature_name) {
                            Some(FeatureValue::ArrayFloat(arr)) => {
                                for &val in arr {
                                    builder.values().append_value(val);
                                }
                                builder.append(true);
                            }
                            _ => builder.append(false),
                        }
                    }
                    Arc::new(builder.finish())
                }
                FeatureValue::ArrayString(_) => {
                    use arrow::array::{ListBuilder, StringBuilder};
                    let mut builder = ListBuilder::new(StringBuilder::new());
                    for row in rows {
                        match row.features.get(feature_name) {
                            Some(FeatureValue::ArrayString(arr)) => {
                                for val in arr {
                                    builder.values().append_value(val);
                                }
                                builder.append(true);
                            }
                            _ => builder.append(false),
                        }
                    }
                    Arc::new(builder.finish())
                }
                FeatureValue::Null => Arc::new(StringArray::from(vec![None::<String>; rows.len()])),
            };
            columns.push(array);
        }

        // Add timestamp column
        use arrow::array::TimestampMicrosecondArray;
        let timestamps: Vec<i64> = rows
            .iter()
            .map(|row| row.timestamp.timestamp_micros())
            .collect();
        columns.push(Arc::new(
            TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"),
        ));

        RecordBatch::try_new(schema, columns)
            .map_err(|e| featureduck_core::Error::StorageError(e.into()))
    }

    #[allow(dead_code)]
    fn record_batch_to_rows(
        &self,
        batch: &RecordBatch,
        entity_keys: &[EntityKey],
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Vec<FeatureRow>> {
        // Pre-allocate with exact capacity based on RecordBatch size
        let mut rows = Vec::with_capacity(batch.num_rows());

        // Extract timestamp column
        use arrow::array::TimestampMicrosecondArray;
        let timestamp_col = batch.column_by_name("__timestamp").ok_or_else(|| {
            featureduck_core::Error::StorageError(anyhow::anyhow!("Timestamp column not found"))
        })?;
        let timestamp_array = timestamp_col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Invalid timestamp column type"
                ))
            })?;

        for row_idx in 0..batch.num_rows() {
            // Pre-allocate based on entity_keys length (typically 1-3 keys per row)
            let mut entities = Vec::with_capacity(entity_keys.len());
            let mut features = HashMap::new();

            // Extract timestamp for this row
            let timestamp_micros = timestamp_array.value(row_idx);
            // Convert microseconds to seconds and nanoseconds
            let secs = timestamp_micros / 1_000_000;
            let nsecs = ((timestamp_micros % 1_000_000) * 1000) as u32;
            let timestamp = DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!("Invalid timestamp value"))
            })?;

            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let field_name = field.name();

                // Skip timestamp column (already extracted)
                if field_name == "__timestamp" {
                    continue;
                }

                let is_entity = entity_keys.iter().any(|ek| &ek.name == field_name);

                if is_entity {
                    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                        if !array.is_null(row_idx) {
                            entities.push(EntityKey::new(
                                field_name.clone(),
                                array.value(row_idx).to_string(),
                            ));
                        }
                    }
                } else {
                    let value = match field.data_type() {
                        DataType::Int64 => {
                            if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                                if array.is_null(row_idx) {
                                    FeatureValue::Null
                                } else {
                                    FeatureValue::Int(array.value(row_idx))
                                }
                            } else {
                                FeatureValue::Null
                            }
                        }
                        DataType::Float64 => {
                            if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                                if array.is_null(row_idx) {
                                    FeatureValue::Null
                                } else {
                                    FeatureValue::Float(array.value(row_idx))
                                }
                            } else {
                                FeatureValue::Null
                            }
                        }
                        DataType::Utf8 => {
                            if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                                if array.is_null(row_idx) {
                                    FeatureValue::Null
                                } else {
                                    FeatureValue::String(array.value(row_idx).to_string())
                                }
                            } else {
                                FeatureValue::Null
                            }
                        }
                        DataType::Boolean => {
                            if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                                if array.is_null(row_idx) {
                                    FeatureValue::Null
                                } else {
                                    FeatureValue::Bool(array.value(row_idx))
                                }
                            } else {
                                FeatureValue::Null
                            }
                        }
                        _ => FeatureValue::Null,
                    };
                    features.insert(field_name.clone(), value);
                }
            }

            // Check if this row matches requested entity keys
            let entity_matches = entities.iter().any(|e| {
                entity_keys
                    .iter()
                    .any(|ek| ek.name == e.name && ek.value == e.value)
            });

            // Check if timestamp is within as_of constraint (if specified)
            let timestamp_matches = as_of.is_none_or(|as_of_time| timestamp <= as_of_time);

            if entity_matches && timestamp_matches {
                rows.push(FeatureRow {
                    entities,
                    features,
                    timestamp,
                });
            }
        }

        Ok(rows)
    }
}

#[async_trait]
impl StorageConnector for DeltaStorageConnector {
    #[instrument(skip(self, entity_keys), fields(feature_view, entity_count = entity_keys.len()))]
    async fn read_features(
        &self,
        feature_view: &str,
        entity_keys: Vec<EntityKey>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Vec<FeatureRow>> {
        // P0-8: Check circuit breaker before attempting storage operation
        if !self.circuit_breaker.allow_request() {
            increment_read_errors();
            return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Circuit breaker is open - storage operations temporarily disabled. \
                 The system is protecting against cascading failures. Please retry later."
            )));
        }

        // Start timing the read operation
        let _timer = time_read_operation();

        let table_path = self.table_path(feature_view);
        info!("Reading features from Delta table: {}", table_path);

        // Verify table exists by attempting to parse and load metadata
        // (delta_scan() will handle the actual read with all optimizations)
        let table_url = url::Url::parse(&table_path)
            .or_else(|_| url::Url::from_file_path(&table_path).map_err(|_| ()))
            .map_err(|_| {
                self.circuit_breaker.record_failure();
                increment_read_errors();
                featureduck_core::Error::InvalidInput(format!("Invalid table path: {}", table_path))
            })?;

        DeltaTableBuilder::from_uri(table_url)
            .map_err(|e| {
                self.circuit_breaker.record_failure();
                increment_read_errors();
                featureduck_core::Error::StorageError(e.into())
            })?
            .load()
            .await
            .map_err(|_| {
                self.circuit_breaker.record_failure();
                increment_read_errors();
                featureduck_core::Error::FeatureViewNotFound(feature_view.to_string())
            })?;

        if let Some(timestamp) = as_of {
            debug!("Point-in-time query as of: {}", timestamp);
        }

        // Always use DuckDB with QUALIFY to deduplicate to latest row per entity
        // This ensures feature store semantics: one row per entity (latest value)
        // If no as_of is provided, use far future to get absolute latest
        let as_of_time = as_of.unwrap_or_else(|| {
            // Use year 2200 as "infinity" for latest queries
            DateTime::from_timestamp(INFINITY_TIMESTAMP, 0)
                .expect("INFINITY_TIMESTAMP constant is valid")
        });
        debug!(
            "Reading features with delta_scan() + QUALIFY deduplication (as_of: {})",
            as_of_time
        );

        // Use delta_scan() for native Delta Lake optimizations
        // This gives us partition pruning, file skipping, and deletion vectors automatically
        // Wrap with timeout from resource_limits for resilience
        let timeout_duration = self.resource_limits.query_timeout;
        let result = tokio::time::timeout(
            timeout_duration,
            self.read_features_with_delta_scan(&table_path, &entity_keys, as_of_time),
        )
        .await
        .map_err(|_| {
            warn!(
                "Query timeout after {:?} for feature_view={}",
                timeout_duration, feature_view
            );
            self.circuit_breaker.record_failure();
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Query timed out after {:?}. The query may be too complex or storage is slow.",
                timeout_duration
            ))
        })?;

        match &result {
            Ok(rows) => {
                // Info: Large result sets are handled by self-healing batching
                // No hard rejection - system auto-adapts batch size on OOM
                if rows.len() > self.resource_limits.max_rows {
                    info!(
                        "Large result set: {} rows from {} (soft limit: {}). \
                         Self-healing batching handled memory automatically.",
                        rows.len(),
                        feature_view,
                        self.resource_limits.max_rows
                    );
                }

                // P0-8: Record success to circuit breaker
                self.circuit_breaker.record_success();
                increment_read_rows(rows.len());
                info!(
                    "Successfully read {} rows from {}",
                    rows.len(),
                    feature_view
                );
            }
            Err(_) => {
                // P0-8: Record failure to circuit breaker
                self.circuit_breaker.record_failure();
                increment_read_errors();
            }
        }

        result
    }

    #[instrument(skip(self, rows), fields(feature_view, row_count = rows.len()))]
    async fn write_features(&self, feature_view: &str, rows: Vec<FeatureRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // P0-8: Check circuit breaker before attempting storage operation
        if !self.circuit_breaker.allow_request() {
            increment_write_errors();
            return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Circuit breaker is open - storage operations temporarily disabled. \
                 The system is protecting against cascading failures. Please retry later."
            )));
        }

        // Start timing the write operation
        let _timer = time_write_operation();
        let row_count = rows.len();

        let table_path = self.table_path(feature_view);

        info!(
            "Writing {} rows to Delta table: {} (parallel={}, chunk_size={})",
            row_count, table_path, self.write_config.parallel_enabled, self.write_config.chunk_size
        );

        // Use optimized parallel write path (consume rows to avoid clone)
        let record_batches =
            crate::write_optimized::rows_to_record_batches_parallel(rows, &self.write_config)
                .inspect_err(|_| {
                    increment_write_errors();
                })?;

        debug!("Created {} RecordBatches for write", record_batches.len());

        // Create directory for local paths (required by deltalake 0.29)
        if !table_path.starts_with("s3://")
            && !table_path.starts_with("gs://")
            && !table_path.starts_with("azure://")
        {
            std::fs::create_dir_all(&table_path).map_err(|e| {
                increment_write_errors();
                featureduck_core::Error::StorageError(e.into())
            })?;
        }

        // Parse table path as URL for deltalake 0.29 API
        let table_url = url::Url::parse(&table_path)
            .or_else(|_| url::Url::from_file_path(&table_path).map_err(|_| ()))
            .map_err(|_| {
                increment_write_errors();
                featureduck_core::Error::InvalidInput(format!("Invalid table path: {}", table_path))
            })?;

        // Pre-compute Delta schema for table creation (outside retry loop to avoid repeated work)
        let first_batch = &record_batches[0];
        let delta_fields: Vec<StructField> = first_batch
            .schema()
            .fields()
            .iter()
            .map(|f| self.arrow_field_to_delta_field(f))
            .collect::<Result<Vec<_>>>()
            .inspect_err(|_| {
                increment_write_errors();
            })?;

        // Wrap Delta Lake write operation with retry logic for transient failures
        // This handles: network errors, OCC conflicts, temporary resource unavailability
        let table_url_clone = table_url.clone();
        let record_batches_clone = record_batches.clone();
        let delta_fields_clone = delta_fields.clone();

        // Convert our compression codec to delta-rs writer properties
        let writer_properties = self.get_writer_properties();

        let result = retry_async(&self.retry_policy, || {
            let table_url = table_url_clone.clone();
            let record_batches = record_batches_clone.clone();
            let delta_fields = delta_fields_clone.clone();
            let props = writer_properties.clone();

            async move {
                match DeltaTableBuilder::from_uri(table_url.clone())
                    .map_err(|e| featureduck_core::Error::StorageError(e.into()))?
                    .load()
                    .await
                {
                    Ok(table) => {
                        debug!("Table exists, appending data with schema evolution");
                        DeltaOps::from(table)
                            .write(record_batches)
                            .with_writer_properties(props)
                            .await
                            .map_err(|e| featureduck_core::Error::StorageError(e.into()))
                    }
                    Err(_) => {
                        debug!("Table does not exist, creating new table with first write");

                        let table = CreateBuilder::new()
                            .with_location(table_url.as_str())
                            .with_columns(delta_fields)
                            .await
                            .map_err(|e| featureduck_core::Error::StorageError(e.into()))?;

                        DeltaOps::from(table)
                            .write(record_batches)
                            .with_writer_properties(props)
                            .await
                            .map_err(|e| featureduck_core::Error::StorageError(e.into()))
                    }
                }
            }
        })
        .await;

        match result {
            Ok(_) => {
                // P0-8: Record success to circuit breaker
                self.circuit_breaker.record_success();
                increment_write_rows(row_count);
                info!("Successfully wrote {} rows to Delta table", row_count);
                Ok(())
            }
            Err(e) => {
                // P0-8: Record failure to circuit breaker
                self.circuit_breaker.record_failure();
                increment_write_errors();
                Err(e.error)
            }
        }
    }

    async fn list_feature_views(&self) -> Result<Vec<FeatureView>> {
        // Storage connector doesn't track feature view metadata
        // Feature views are registered in FeatureRegistry
        // Return empty list - callers should use FeatureRegistry::list_feature_views() instead
        Ok(vec![])
    }

    async fn get_feature_view(&self, name: &str) -> Result<FeatureView> {
        // Storage connector doesn't track feature view metadata
        // Feature views are registered in FeatureRegistry
        // Return error directing caller to proper API
        Err(featureduck_core::Error::FeatureViewNotFound(format!(
            "Feature view '{}' metadata not found in storage. Use FeatureRegistry::get_feature_view() instead.",
            name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;
    use featureduck_core::FeatureValue;
    use tempfile::TempDir;

    /// Helper to create a temporary Delta Lake path for testing
    async fn create_test_connector() -> (DeltaStorageConnector, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let connector = DeltaStorageConnector::new(
            temp_dir.path().to_str().unwrap(),
            DuckDBEngineConfig::default(),
        )
        .await
        .expect("Failed to create connector");
        (connector, temp_dir)
    }

    #[test]
    fn test_duckdb_delta_extension_availability() {
        // Test if DuckDB Delta extension is available
        let conn = Connection::open_in_memory().expect("Failed to create DuckDB connection");

        // Try to install delta extension (downloads extension if not present)
        let install_result = conn.execute("INSTALL delta;", []);

        // Try to load delta extension
        let load_result = conn.execute("LOAD delta;", []);

        // Check results
        if install_result.is_ok() && load_result.is_ok() {
            info!("✅ DuckDB Delta extension is available");

            // Verify delta_scan function exists
            let test_result = conn.execute("SELECT * FROM delta_scan('nonexistent') LIMIT 0;", []);
            // Should fail with "not found" not "unknown function"
            assert!(test_result.is_err(), "Should fail on nonexistent path");
        } else {
            info!("⚠️ DuckDB Delta extension not available - will use manual file listing");
            info!("Install result: {:?}", install_result);
            info!("Load result: {:?}", load_result);
        }
    }

    #[tokio::test]
    async fn test_delta_connector_new_creates_directory() {
        // Given: A path that doesn't exist
        let temp_dir = TempDir::new().unwrap();
        let delta_path = temp_dir.path().join("new_delta_lake");

        // When: We create a connector
        let result =
            DeltaStorageConnector::new(delta_path.to_str().unwrap(), DuckDBEngineConfig::default())
                .await;

        // Then: Connector is created successfully
        assert!(
            result.is_ok(),
            "Failed to create connector: {:?}",
            result.err()
        );

        // And: Directory exists
        assert!(delta_path.exists());
    }

    #[tokio::test]
    async fn test_delta_write_and_read_features() {
        // Given: A connector
        let (connector, _temp_dir) = create_test_connector().await;

        // And: Some test feature data
        let entity_key = EntityKey::new("user_id".to_string(), "user_123".to_string());
        let timestamp = Utc::now();
        let mut feature_row = FeatureRow::new(vec![entity_key.clone()], timestamp);
        feature_row.add_feature("clicks_7d".to_string(), FeatureValue::Int(42));
        feature_row.add_feature("purchases_7d".to_string(), FeatureValue::Int(3));

        // When: We write features to Delta Lake
        let write_result = connector
            .write_features("user_features", vec![feature_row.clone()])
            .await;

        // Then: Write succeeds (this will fail until implemented - RED)
        assert!(
            write_result.is_ok(),
            "Write should succeed: {:?}",
            write_result.err()
        );

        // When: We read the same features back
        let read_result = connector
            .read_features("user_features", vec![entity_key], None)
            .await;

        // Then: Read succeeds
        assert!(read_result.is_ok(), "Read should succeed");
        let features = read_result.unwrap();

        // And: We get back the same data
        assert_eq!(features.len(), 1);
        assert_eq!(
            features[0].get_feature("clicks_7d"),
            Some(&FeatureValue::Int(42))
        );
        assert_eq!(
            features[0].get_feature("purchases_7d"),
            Some(&FeatureValue::Int(3))
        );
    }

    #[tokio::test]
    async fn test_delta_point_in_time_query() {
        // Given: A connector
        let (connector, _temp_dir) = create_test_connector().await;

        // And: Feature data at two different times
        let entity_key = EntityKey::new("user_id".to_string(), "user_123".to_string());

        // Write at time T1
        let t1 = Utc::now();
        let mut row_t1 = FeatureRow::new(vec![entity_key.clone()], t1);
        row_t1.add_feature("clicks_7d".to_string(), FeatureValue::Int(10));
        connector
            .write_features("user_features", vec![row_t1])
            .await
            .unwrap();

        // Wait a bit to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Write at time T2
        let t2 = Utc::now();
        let mut row_t2 = FeatureRow::new(vec![entity_key.clone()], t2);
        row_t2.add_feature("clicks_7d".to_string(), FeatureValue::Int(20));
        connector
            .write_features("user_features", vec![row_t2])
            .await
            .unwrap();

        // When: We read features as of T1
        let result_t1 = connector
            .read_features("user_features", vec![entity_key.clone()], Some(t1))
            .await
            .unwrap();

        // Then: We get the T1 value
        assert_eq!(
            result_t1[0].get_feature("clicks_7d"),
            Some(&FeatureValue::Int(10))
        );

        // When: We read features as of T2
        let result_t2 = connector
            .read_features("user_features", vec![entity_key.clone()], Some(t2))
            .await
            .unwrap();

        // Then: We get the T2 value
        assert_eq!(
            result_t2[0].get_feature("clicks_7d"),
            Some(&FeatureValue::Int(20))
        );
    }

    #[tokio::test]
    async fn test_point_in_time_between_timestamps() {
        // Given: A connector
        let (connector, _temp_dir) = create_test_connector().await;

        // And: Feature data at T1 and T3
        let entity_key = EntityKey::new("user_id".to_string(), "user_456".to_string());

        let t1 = Utc::now();
        let mut row_t1 = FeatureRow::new(vec![entity_key.clone()], t1);
        row_t1.add_feature("score".to_string(), FeatureValue::Float(100.0));
        connector
            .write_features("user_features", vec![row_t1])
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let t2 = Utc::now(); // Query time between T1 and T3

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let t3 = Utc::now();
        let mut row_t3 = FeatureRow::new(vec![entity_key.clone()], t3);
        row_t3.add_feature("score".to_string(), FeatureValue::Float(200.0));
        connector
            .write_features("user_features", vec![row_t3])
            .await
            .unwrap();

        // When: We query as of T2 (between T1 and T3)
        let result = connector
            .read_features("user_features", vec![entity_key.clone()], Some(t2))
            .await
            .unwrap();

        // Then: We should get T1 value (latest BEFORE T2)
        assert_eq!(result.len(), 1);
        match result[0].get_feature("score") {
            Some(FeatureValue::Float(v)) => assert_eq!(*v, 100.0),
            Some(FeatureValue::Int(v)) => assert_eq!(*v, 100),
            other => panic!("Expected Int(100) or Float(100.0), got {:?}", other),
        }

        // And: Timestamp should be T1
        assert!(result[0].timestamp <= t2);
        assert_eq!(result[0].timestamp, t1);
    }

    #[tokio::test]
    async fn test_no_features_before_query_time() {
        // Given: A connector
        let (connector, _temp_dir) = create_test_connector().await;

        // And: Query time
        let t_query = Utc::now();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // And: Feature data AFTER query time
        let entity_key = EntityKey::new("user_id".to_string(), "user_789".to_string());
        let t_future = Utc::now();
        let mut row = FeatureRow::new(vec![entity_key.clone()], t_future);
        row.add_feature("value".to_string(), FeatureValue::Int(999));
        connector
            .write_features("user_features", vec![row])
            .await
            .unwrap();

        // When: We query as of time BEFORE any features exist
        let result = connector
            .read_features("user_features", vec![entity_key], Some(t_query))
            .await
            .unwrap();

        // Then: We get no results (no features existed at that time)
        assert_eq!(result.len(), 0);
    }

    // ==================== CONNECTION POOL STRESS TESTS (GAP #8) ====================

    #[tokio::test]
    async fn test_connection_pool_concurrent_queries_stress() {
        // GAP #8: Connection pool stress test (100 concurrent queries)
        // This test verifies that the connection pool handles high concurrency without deadlocks

        use std::sync::Arc;
        use tokio::task::JoinSet;

        // Create connector with limited pool (4-8 connections)
        let (connector, _temp_dir) = create_test_connector().await;
        let connector = Arc::new(connector);

        // Prepare test data - write some features first
        let entity_key = EntityKey::new("user_id".to_string(), "user_pool_test".to_string());
        let timestamp = Utc::now();
        let mut feature_row = FeatureRow::new(vec![entity_key.clone()], timestamp);
        feature_row.add_feature("test_value".to_string(), FeatureValue::Int(42));

        connector
            .write_features("pool_test", vec![feature_row])
            .await
            .expect("Setup write should succeed");

        // Spawn 100 concurrent read operations
        let mut tasks = JoinSet::new();

        for task_id in 0..100 {
            let connector_clone = Arc::clone(&connector);
            let entity_key_clone = entity_key.clone();

            tasks.spawn(async move {
                // Each task performs a read operation
                let result = connector_clone
                    .read_features("pool_test", vec![entity_key_clone], None)
                    .await;

                assert!(result.is_ok(), "Task {} failed to read features", task_id);
                let features = result.unwrap();
                assert_eq!(
                    features.len(),
                    1,
                    "Task {} got wrong number of features",
                    task_id
                );

                task_id // Return task ID for tracking
            });
        }

        // Wait for all tasks to complete (with timeout)
        let start = std::time::Instant::now();
        let mut completed = 0;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(_task_id) => {
                    completed += 1;
                }
                Err(e) => {
                    panic!("Task panicked: {}", e);
                }
            }
        }

        let duration = start.elapsed();

        // Verify all tasks completed
        assert_eq!(completed, 100, "All 100 tasks should complete successfully");

        // Check that operations didn't take too long (should be <10s even with queueing)
        assert!(
            duration.as_secs() < 30,
            "Pool stress test took too long: {:?} (possible deadlock)",
            duration
        );

        println!(
            "✅ Connection pool stress test passed: {} concurrent queries in {:?} ({:.1} QPS)",
            completed,
            duration,
            completed as f64 / duration.as_secs_f64()
        );
    }

    #[tokio::test]
    async fn test_connection_pool_mixed_read_write_workload() {
        // GAP #8: Additional stress test - mixed read/write workload
        // This tests realistic scenario with both reads and writes

        use std::sync::Arc;
        use tokio::task::JoinSet;

        let (connector, _temp_dir) = create_test_connector().await;
        let connector = Arc::new(connector);

        // Spawn 50 readers and 50 writers concurrently
        let mut tasks = JoinSet::new();

        // 50 write tasks
        for task_id in 0..50 {
            let connector_clone = Arc::clone(&connector);

            tasks.spawn(async move {
                let entity_key = EntityKey::new("user_id".to_string(), format!("user_{}", task_id));
                let timestamp = Utc::now();
                let mut feature_row = FeatureRow::new(vec![entity_key], timestamp);
                feature_row.add_feature("value".to_string(), FeatureValue::Int(task_id as i64));

                let result = connector_clone
                    .write_features("mixed_test", vec![feature_row])
                    .await;

                assert!(result.is_ok(), "Write task {} failed", task_id);
                ("write", task_id)
            });
        }

        // 50 read tasks (will read data written by write tasks)
        for task_id in 50..100 {
            let connector_clone = Arc::clone(&connector);

            tasks.spawn(async move {
                // Try to read a random user's features
                let user_id = task_id % 50; // Read from one of the written users
                let entity_key = EntityKey::new("user_id".to_string(), format!("user_{}", user_id));

                // Reads might fail early on (before writes complete), so we allow that
                let _result = connector_clone
                    .read_features("mixed_test", vec![entity_key], None)
                    .await;

                // Don't assert on reads - they may legitimately fail if data not written yet
                ("read", task_id)
            });
        }

        // Wait for all tasks to complete
        let start = std::time::Instant::now();
        let mut write_count = 0;
        let mut read_count = 0;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((op_type, _task_id)) => {
                    if op_type == "write" {
                        write_count += 1;
                    } else {
                        read_count += 1;
                    }
                }
                Err(e) => {
                    panic!("Task panicked: {}", e);
                }
            }
        }

        let duration = start.elapsed();

        // Verify all tasks completed
        assert_eq!(write_count, 50, "All 50 write tasks should complete");
        assert_eq!(read_count, 50, "All 50 read tasks should complete");

        println!(
            "✅ Mixed workload test passed: {} writes + {} reads in {:?} ({:.1} ops/sec)",
            write_count,
            read_count,
            duration,
            100.0 / duration.as_secs_f64()
        );
    }

    #[tokio::test]
    async fn test_write_features_batched() {
        // Test that batched write correctly splits large datasets
        let (connector, _temp_dir) = create_test_connector().await;

        // Generate 1000 rows
        let rows: Vec<FeatureRow> = (0..1000)
            .map(|i| {
                let entity_key = EntityKey::new("user_id".to_string(), format!("user_{}", i));
                let timestamp = Utc::now();
                let mut row = FeatureRow::new(vec![entity_key], timestamp);
                row.add_feature("value".to_string(), FeatureValue::Int(i as i64));
                row
            })
            .collect();

        // Write in batches of 100
        let result = connector
            .write_features_batched("batched_test", rows, Some(100))
            .await;

        assert!(result.is_ok(), "Batched write should succeed: {:?}", result);

        // Verify we can read back a sample
        let entity_key = EntityKey::new("user_id".to_string(), "user_500".to_string());
        let read_result = connector
            .read_features("batched_test", vec![entity_key], None)
            .await;

        assert!(read_result.is_ok(), "Read should succeed");
        let features = read_result.unwrap();
        assert_eq!(features.len(), 1, "Should find the written feature");
        assert_eq!(
            features[0].get_feature("value"),
            Some(&FeatureValue::Int(500))
        );

        println!("✅ Batched write test passed");
    }
}
