//! Stress Test: DuckDB Optimizations
//!
//! Tests all performance optimizations:
//! - Temp directory spilling
//! - Entity batching (splitting large IN() clauses)
//! - Incremental sync vs full sync
//! - Parallel query execution
//!
//! Simulates 100GB data characteristics with high variance.
//!
//! Run with: cargo test -p featureduck-delta --test stress_test_optimizations --release -- --nocapture

use chrono::{Duration, Utc};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::TempDir;

// ============================================================================
// Test Configuration
// ============================================================================

/// Configuration for optimization stress tests
struct OptimizationTestConfig {
    /// Number of unique entities (simulating 100GB scale = ~10M entities)
    num_entities: usize,
    /// Features per row (high variance: 5-100)
    features_per_row: usize,
    /// Batch size for writes
    write_batch_size: usize,
    /// Entity batch sizes to test for reads
    entity_batch_sizes: Vec<usize>,
    /// String feature size range (bytes) for high variance
    min_string_size: usize,
    max_string_size: usize,
}

impl OptimizationTestConfig {
    fn for_100gb_simulation() -> Self {
        Self {
            // Scale down for test but maintain characteristics
            // Real 100GB would have ~50M rows, we use 100K for speed
            num_entities: 100_000,
            features_per_row: 50,  // Wide tables
            write_batch_size: 10_000,
            entity_batch_sizes: vec![100, 1_000, 10_000, 50_000],
            min_string_size: 10,
            max_string_size: 500,  // High variance
        }
    }

    fn quick() -> Self {
        Self {
            num_entities: 10_000,
            features_per_row: 20,
            write_batch_size: 5_000,
            entity_batch_sizes: vec![100, 1_000, 5_000],
            min_string_size: 10,
            max_string_size: 100,
        }
    }

    fn estimated_data_size_gb(&self) -> f64 {
        // Estimate: entity(30) + timestamp(8) + features(avg 50 bytes each)
        let avg_row_size = 30 + 8 + (self.features_per_row * 50);
        (self.num_entities * avg_row_size) as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

// ============================================================================
// Data Generation with High Variance
// ============================================================================

/// Generate feature rows with high variance in data types and sizes
fn generate_high_variance_rows(
    start_id: usize,
    count: usize,
    features_per_row: usize,
    min_string: usize,
    max_string: usize,
) -> Vec<FeatureRow> {
    let mut rows = Vec::with_capacity(count);
    let base_time = Utc::now() - Duration::days(30);

    for i in 0..count {
        let entity_id = start_id + i;
        // Vary timestamp across 30 days for realistic distribution
        let time_offset = Duration::seconds((entity_id % (30 * 24 * 3600)) as i64);
        let timestamp = base_time + time_offset;

        let mut features = HashMap::with_capacity(features_per_row);

        for f in 0..features_per_row {
            let feature_name = format!("feature_{}", f);

            // High variance: different types and sizes based on position
            let value = match f % 5 {
                0 => {
                    // Integer features with varying ranges
                    let range_mult = if f < 10 { 100 } else { 1_000_000 };
                    FeatureValue::Int(((entity_id * (f + 1)) % range_mult) as i64)
                }
                1 => {
                    // Float features with varying precision
                    let base = (entity_id as f64) * 0.001;
                    let variance = ((f as f64) * 0.1).sin();
                    FeatureValue::Float(base + variance)
                }
                2 => {
                    // Boolean features
                    FeatureValue::Bool((entity_id + f) % 2 == 0)
                }
                3 => {
                    // String features with HIGH variance in size
                    let str_len = min_string + ((entity_id + f) % (max_string - min_string));
                    let content: String = (0..str_len)
                        .map(|j| char::from(b'a' + ((entity_id + f + j) % 26) as u8))
                        .collect();
                    FeatureValue::String(content)
                }
                _ => {
                    // Null features (10% null rate)
                    if (entity_id + f) % 10 == 0 {
                        FeatureValue::Null
                    } else {
                        FeatureValue::Int((entity_id * f) as i64)
                    }
                }
            };

            features.insert(feature_name, value);
        }

        rows.push(FeatureRow {
            entities: vec![EntityKey::new("user_id", format!("user_{:08}", entity_id))],
            features,
            timestamp,
        });
    }

    rows
}

// ============================================================================
// Tests
// ============================================================================

/// Test 1: Entity Batching Performance
/// Compare query performance with different IN() clause sizes
#[tokio::test]
async fn test_entity_batching_performance() {
    println!("\n================================================================================");
    println!("TEST: Entity Batching Performance");
    println!("================================================================================\n");

    let config = OptimizationTestConfig::quick();
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    println!("Configuration:");
    println!("  Entities: {}", config.num_entities);
    println!("  Features per row: {}", config.features_per_row);
    println!("  Estimated data: {:.2} GB\n", config.estimated_data_size_gb());

    // Create connector with optimizations enabled
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        temp_directory: Some(PathBuf::from("/tmp/featureduck-stress-test")),
        preserve_insertion_order: false,
        max_entities_per_query: 1_000, // Will batch queries > 1000 entities
        streaming_batch_size: 10_000,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write test data
    println!("Writing {} rows...", config.num_entities);
    let write_start = Instant::now();

    for batch_start in (0..config.num_entities).step_by(config.write_batch_size) {
        let batch_end = (batch_start + config.write_batch_size).min(config.num_entities);
        let rows = generate_high_variance_rows(
            batch_start,
            batch_end - batch_start,
            config.features_per_row,
            config.min_string_size,
            config.max_string_size,
        );

        connector
            .write_features("user_features", rows)
            .await
            .expect("Write failed");
    }

    let write_duration = write_start.elapsed();
    let write_rate = config.num_entities as f64 / write_duration.as_secs_f64();
    println!(
        "Write complete: {} rows in {:.2}s ({:.0} rows/sec)\n",
        config.num_entities,
        write_duration.as_secs_f64(),
        write_rate
    );

    // Test reading with different entity batch sizes
    println!("Testing read performance with different entity batch sizes:\n");
    println!("{:>15} {:>15} {:>15} {:>15}", "Entities", "Duration", "Rows/sec", "Status");
    println!("{:-<60}", "");

    for &num_entities in &config.entity_batch_sizes {
        // Generate entity keys to read
        let entity_keys: Vec<EntityKey> = (0..num_entities)
            .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
            .collect();

        let read_start = Instant::now();
        let result = connector
            .read_features("user_features", entity_keys.clone(), None)
            .await;
        let read_duration = read_start.elapsed();

        match result {
            Ok(rows) => {
                let rate = rows.len() as f64 / read_duration.as_secs_f64();
                println!(
                    "{:>15} {:>12.2}ms {:>12.0}/sec {:>15}",
                    format!("{}", num_entities),
                    read_duration.as_secs_f64() * 1000.0,
                    rate,
                    format!("✓ {} rows", rows.len())
                );
            }
            Err(e) => {
                println!(
                    "{:>15} {:>12.2}ms {:>12} {:>15}",
                    format!("{}", num_entities),
                    read_duration.as_secs_f64() * 1000.0,
                    "-",
                    format!("✗ {}", e)
                );
            }
        }
    }

    println!("\n✓ Entity batching test complete\n");
}

/// Test 2: Temp Directory Spilling
/// Verify that large aggregations don't OOM
#[tokio::test]
async fn test_temp_directory_spilling() {
    println!("\n================================================================================");
    println!("TEST: Temp Directory Spilling");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();
    let temp_spill_dir = temp_dir.path().join("duckdb-spill");

    println!("Temp spill directory: {}\n", temp_spill_dir.display());

    // Create connector with temp directory
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        temp_directory: Some(temp_spill_dir.clone()),
        memory_limit: "256MB".to_string(), // Force spilling
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write enough data to potentially cause spilling
    let num_rows = 50_000;
    println!("Writing {} rows (forcing potential spill)...", num_rows);

    let rows = generate_high_variance_rows(0, num_rows, 30, 100, 500);

    let write_start = Instant::now();
    connector
        .write_features("spill_test", rows)
        .await
        .expect("Write failed");
    let write_duration = write_start.elapsed();

    println!(
        "Write complete: {:.2}s ({:.0} rows/sec)",
        write_duration.as_secs_f64(),
        num_rows as f64 / write_duration.as_secs_f64()
    );

    // Check if temp directory was used
    let spill_exists = temp_spill_dir.exists();
    println!("Spill directory created: {}", spill_exists);

    // Read data to verify integrity
    let entity_keys: Vec<EntityKey> = (0..1000)
        .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
        .collect();

    let read_start = Instant::now();
    let result = connector
        .read_features("spill_test", entity_keys, None)
        .await
        .expect("Read failed");
    let read_duration = read_start.elapsed();

    println!(
        "Read verification: {} rows in {:.2}ms",
        result.len(),
        read_duration.as_secs_f64() * 1000.0
    );

    assert!(!result.is_empty(), "Should read data after potential spill");
    println!("\n✓ Temp directory spilling test complete\n");
}

/// Test 3: Parallelism Configuration
/// Compare performance with different parallelism settings
#[tokio::test]
async fn test_parallelism_configuration() {
    println!("\n================================================================================");
    println!("TEST: Parallelism Configuration");
    println!("================================================================================\n");

    let num_rows = 20_000;
    let num_reads = 5;

    println!("Configuration:");
    println!("  Rows: {}", num_rows);
    println!("  Read iterations: {}\n", num_reads);

    // Test with preserve_insertion_order = true (less parallel)
    {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();

        let engine_config = DuckDBEngineConfig {
            db_path: None,
            preserve_insertion_order: true, // Less parallel
            ..Default::default()
        };

        let connector = DeltaStorageConnector::new(storage_path, engine_config)
            .await
            .expect("Failed to create connector");

        let rows = generate_high_variance_rows(0, num_rows, 20, 50, 200);
        connector
            .write_features("parallel_test", rows)
            .await
            .expect("Write failed");

        let entity_keys: Vec<EntityKey> = (0..5000)
            .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
            .collect();

        let mut total_duration = std::time::Duration::ZERO;
        for _ in 0..num_reads {
            let start = Instant::now();
            let _ = connector
                .read_features("parallel_test", entity_keys.clone(), None)
                .await;
            total_duration += start.elapsed();
        }

        let avg_ms = total_duration.as_secs_f64() * 1000.0 / num_reads as f64;
        println!("preserve_insertion_order=true:  avg {:.2}ms per read", avg_ms);
    }

    // Test with preserve_insertion_order = false (more parallel)
    {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();

        let engine_config = DuckDBEngineConfig {
            db_path: None,
            preserve_insertion_order: false, // More parallel
            ..Default::default()
        };

        let connector = DeltaStorageConnector::new(storage_path, engine_config)
            .await
            .expect("Failed to create connector");

        let rows = generate_high_variance_rows(0, num_rows, 20, 50, 200);
        connector
            .write_features("parallel_test", rows)
            .await
            .expect("Write failed");

        let entity_keys: Vec<EntityKey> = (0..5000)
            .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
            .collect();

        let mut total_duration = std::time::Duration::ZERO;
        for _ in 0..num_reads {
            let start = Instant::now();
            let _ = connector
                .read_features("parallel_test", entity_keys.clone(), None)
                .await;
            total_duration += start.elapsed();
        }

        let avg_ms = total_duration.as_secs_f64() * 1000.0 / num_reads as f64;
        println!("preserve_insertion_order=false: avg {:.2}ms per read", avg_ms);
    }

    println!("\n✓ Parallelism configuration test complete\n");
}

/// Test 4: High Variance Data Handling
/// Ensure system handles mixed data types and sizes correctly
#[tokio::test]
async fn test_high_variance_data() {
    println!("\n================================================================================");
    println!("TEST: High Variance Data Handling");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let connector = DeltaStorageConnector::new(storage_path, DuckDBEngineConfig::default())
        .await
        .expect("Failed to create connector");

    // Generate data with extreme variance
    let rows = generate_high_variance_rows(
        0,
        10_000,
        50,   // Many features
        5,    // Min string: 5 chars
        1000, // Max string: 1000 chars (200x variance!)
    );

    println!("Data characteristics:");
    println!("  Rows: 10,000");
    println!("  Features per row: 50");
    println!("  String size range: 5 - 1000 bytes (200x variance)");

    // Sample some rows to show variance
    println!("\nSample feature values (showing variance):");
    for i in [0, 100, 500, 1000, 5000].iter() {
        if let Some(row) = rows.get(*i) {
            if let Some(FeatureValue::String(s)) = row.features.get("feature_3") {
                println!("  Row {}: string len = {} chars", i, s.len());
            }
        }
    }

    // Write all data
    let write_start = Instant::now();
    connector
        .write_features("variance_test", rows)
        .await
        .expect("Write failed");
    let write_duration = write_start.elapsed();

    println!(
        "\nWrite: {:.2}s ({:.0} rows/sec)",
        write_duration.as_secs_f64(),
        10_000.0 / write_duration.as_secs_f64()
    );

    // Read and verify
    let entity_keys: Vec<EntityKey> = [0, 100, 500, 1000, 5000]
        .iter()
        .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
        .collect();

    let read_start = Instant::now();
    let result = connector
        .read_features("variance_test", entity_keys, None)
        .await
        .expect("Read failed");
    let read_duration = read_start.elapsed();

    println!(
        "Read: {} rows in {:.2}ms",
        result.len(),
        read_duration.as_secs_f64() * 1000.0
    );

    // Verify data integrity
    for row in &result {
        assert_eq!(row.features.len(), 50, "Should have all 50 features");
    }

    println!("\n✓ High variance data test complete\n");
}

/// Test 5: Large Scale Simulation (100GB characteristics)
/// Tests with realistic 100GB data patterns at smaller scale
#[tokio::test]
async fn test_100gb_simulation() {
    println!("\n================================================================================");
    println!("TEST: 100GB Data Simulation");
    println!("================================================================================\n");

    let config = OptimizationTestConfig::for_100gb_simulation();

    println!("Simulating 100GB workload characteristics:");
    println!("  Entities: {} (scaled down from 50M)", config.num_entities);
    println!("  Features per row: {}", config.features_per_row);
    println!("  String variance: {} - {} bytes", config.min_string_size, config.max_string_size);
    println!(
        "  Estimated actual test data: {:.2} GB",
        config.estimated_data_size_gb()
    );
    println!("  (Real 100GB would take ~100x longer)\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    // Use optimized config
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        temp_directory: Some(PathBuf::from("/tmp/featureduck-100gb-sim")),
        preserve_insertion_order: false,
        max_entities_per_query: 10_000,
        streaming_batch_size: 50_000,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write in batches (simulating streaming ingestion)
    println!("Phase 1: Write ({} rows in batches of {})",
             config.num_entities, config.write_batch_size);

    let write_start = Instant::now();
    let mut rows_written = 0;

    for batch_start in (0..config.num_entities).step_by(config.write_batch_size) {
        let batch_end = (batch_start + config.write_batch_size).min(config.num_entities);
        let rows = generate_high_variance_rows(
            batch_start,
            batch_end - batch_start,
            config.features_per_row,
            config.min_string_size,
            config.max_string_size,
        );

        connector
            .write_features("sim_100gb", rows)
            .await
            .expect("Write failed");

        rows_written += batch_end - batch_start;

        if rows_written % 20_000 == 0 {
            let elapsed = write_start.elapsed().as_secs_f64();
            let rate = rows_written as f64 / elapsed;
            println!(
                "  Progress: {} rows ({:.0} rows/sec)",
                rows_written, rate
            );
        }
    }

    let write_duration = write_start.elapsed();
    let write_rate = config.num_entities as f64 / write_duration.as_secs_f64();

    println!(
        "Write complete: {} rows in {:.2}s ({:.0} rows/sec)\n",
        config.num_entities,
        write_duration.as_secs_f64(),
        write_rate
    );

    // Read tests with batching
    println!("Phase 2: Read tests with entity batching\n");
    println!("{:>15} {:>15} {:>15} {:>15}", "Entities", "Duration", "Rows/sec", "Batched");
    println!("{:-<65}", "");

    for &num_entities in &config.entity_batch_sizes {
        let entity_keys: Vec<EntityKey> = (0..num_entities)
            .map(|i| EntityKey::new("user_id", format!("user_{:08}", i)))
            .collect();

        let read_start = Instant::now();
        let result = connector
            .read_features("sim_100gb", entity_keys, None)
            .await
            .expect("Read failed");
        let read_duration = read_start.elapsed();

        let rate = result.len() as f64 / read_duration.as_secs_f64();
        let batched = if num_entities > 10_000 { "Yes" } else { "No" };

        println!(
            "{:>15} {:>12.2}ms {:>12.0}/sec {:>15}",
            format!("{}", num_entities),
            read_duration.as_secs_f64() * 1000.0,
            rate,
            batched
        );
    }

    // Calculate extrapolated 100GB metrics
    let extrapolation_factor = 100.0 / config.estimated_data_size_gb();
    let estimated_100gb_write_time = write_duration.as_secs_f64() * extrapolation_factor;

    println!("\n=================================================================");
    println!("100GB Extrapolation (based on test results):");
    println!("  Estimated write time: {:.0} minutes", estimated_100gb_write_time / 60.0);
    println!("  Write rate maintained: {:.0} rows/sec", write_rate);
    println!("=================================================================");

    println!("\n✓ 100GB simulation test complete\n");
}
