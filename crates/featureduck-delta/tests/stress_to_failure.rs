//! Stress Test: Push System to Failure
//!
//! Tests designed to find breaking points:
//! - OOM conditions with tiny memory limits
//! - Disk space exhaustion
//! - Very large entity queries
//! - Extreme data variance
//!
//! Run with: cargo test -p featureduck-delta --test stress_to_failure --release -- --nocapture

use chrono::{Duration, Utc};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::TempDir;

// ============================================================================
// Data Generation
// ============================================================================

/// Generate rows with configurable size
fn generate_rows(
    start_id: usize,
    count: usize,
    features_per_row: usize,
    string_size: usize,
) -> Vec<FeatureRow> {
    let mut rows = Vec::with_capacity(count);
    let base_time = Utc::now() - Duration::days(30);

    for i in 0..count {
        let entity_id = start_id + i;
        let time_offset = Duration::seconds((entity_id % (30 * 24 * 3600)) as i64);
        let timestamp = base_time + time_offset;

        let mut features = HashMap::with_capacity(features_per_row);

        for f in 0..features_per_row {
            let feature_name = format!("f{}", f);

            // Mix of types with configurable string size
            let value = match f % 4 {
                0 => FeatureValue::Int((entity_id * f) as i64),
                1 => FeatureValue::Float((entity_id as f64) * 0.001 + (f as f64) * 0.1),
                2 => FeatureValue::Bool((entity_id + f) % 2 == 0),
                _ => {
                    // Large strings to consume memory
                    let content: String = (0..string_size)
                        .map(|j| char::from(b'a' + ((entity_id + f + j) % 26) as u8))
                        .collect();
                    FeatureValue::String(content)
                }
            };

            features.insert(feature_name, value);
        }

        rows.push(FeatureRow {
            entities: vec![EntityKey::new("user_id", format!("u{:010}", entity_id))],
            features,
            timestamp,
        });
    }

    rows
}

// ============================================================================
// Failure Mode Tests
// ============================================================================

/// Test 1: OOM with tiny memory limit
/// Try to force DuckDB to fail with 32MB memory and large data
#[tokio::test]
async fn test_oom_tiny_memory() {
    println!("\n================================================================================");
    println!("TEST: OOM with Tiny Memory (32MB limit)");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();
    let spill_dir = temp_dir.path().join("spill");

    // Extremely constrained memory
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "32MB".to_string(),
        temp_directory: Some(spill_dir.clone()),
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Generate 100K rows with 100 features each (large data)
    let num_rows = 100_000;
    let features_per_row = 100;
    let string_size = 500;

    println!("Generating {} rows x {} features x {}B strings...", num_rows, features_per_row, string_size);
    println!("Estimated data size: {:.2} GB",
        (num_rows * features_per_row * (8 + string_size / 4)) as f64 / 1_000_000_000.0);

    let write_start = Instant::now();
    let batch_size = 10_000;
    let mut success_count = 0;
    let mut fail_count = 0;

    for batch_start in (0..num_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_rows);
        let rows = generate_rows(batch_start, batch_end - batch_start, features_per_row, string_size);

        match connector.write_features("oom_test", rows).await {
            Ok(_) => {
                success_count += batch_end - batch_start;
                if success_count % 50_000 == 0 {
                    let elapsed = write_start.elapsed().as_secs_f64();
                    println!("  Progress: {} rows ({:.0} rows/sec)", success_count, success_count as f64 / elapsed);
                }
            }
            Err(e) => {
                fail_count += batch_end - batch_start;
                println!("  WRITE FAILED at {} rows: {}", batch_start, e);
                break;
            }
        }
    }

    let write_duration = write_start.elapsed();
    println!("\nWrite result: {} success, {} failed in {:.2}s",
        success_count, fail_count, write_duration.as_secs_f64());

    // Check spill directory usage
    if spill_dir.exists() {
        let spill_size: u64 = walkdir::WalkDir::new(&spill_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum();
        println!("Spill directory size: {:.2} MB", spill_size as f64 / 1_000_000.0);
    }

    // Try to read a large number of entities
    println!("\nTrying large read (10K entities)...");
    let entity_keys: Vec<EntityKey> = (0..10_000)
        .map(|i| EntityKey::new("user_id", format!("u{:010}", i)))
        .collect();

    let read_start = Instant::now();
    let read_result = connector.read_features("oom_test", entity_keys, None).await;
    let read_duration = read_start.elapsed();

    match read_result {
        Ok(rows) => println!("Read SUCCESS: {} rows in {:.2}s", rows.len(), read_duration.as_secs_f64()),
        Err(e) => println!("Read FAILED: {} after {:.2}s", e, read_duration.as_secs_f64()),
    }

    println!("\n✓ OOM test complete\n");
}

/// Test 2: Disk space stress
/// Write until disk is nearly full
#[tokio::test]
async fn test_disk_stress() {
    println!("\n================================================================================");
    println!("TEST: Disk Space Stress");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    // Check initial disk space
    let initial_free = get_free_disk_space("/tmp");
    println!("Initial free space: {:.2} GB", initial_free as f64 / 1_000_000_000.0);

    // Stop if less than 2GB free (safety margin)
    let safety_margin_gb = 2.0;
    let max_write_gb = (initial_free as f64 / 1_000_000_000.0) - safety_margin_gb;

    if max_write_gb <= 0.5 {
        println!("SKIPPED: Not enough disk space for stress test (need >2.5GB free)");
        return;
    }

    println!("Will attempt to write up to {:.2} GB (keeping {:.0}GB safety margin)\n",
        max_write_gb, safety_margin_gb);

    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "2GB".to_string(),
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write large batches until we hit disk limit or 5GB
    let batch_size = 50_000;
    let features_per_row = 50;
    let string_size = 200;
    let estimated_row_size = features_per_row * (8 + string_size / 4);

    let write_start = Instant::now();
    let mut total_rows = 0;
    let mut batch_num = 0;
    let max_rows = ((max_write_gb.min(5.0) * 1_000_000_000.0) / estimated_row_size as f64) as usize;

    println!("Target: {} rows ({:.2} GB estimated)", max_rows,
        (max_rows * estimated_row_size) as f64 / 1_000_000_000.0);

    while total_rows < max_rows {
        let rows = generate_rows(total_rows, batch_size, features_per_row, string_size);

        match connector.write_features("disk_stress", rows).await {
            Ok(_) => {
                total_rows += batch_size;
                batch_num += 1;

                if batch_num % 5 == 0 {
                    let elapsed = write_start.elapsed().as_secs_f64();
                    let free_now = get_free_disk_space("/tmp");
                    let used = initial_free - free_now;
                    println!(
                        "  Batch {}: {} rows, {:.2} GB used, {:.2} GB free, {:.0} rows/sec",
                        batch_num, total_rows,
                        used as f64 / 1_000_000_000.0,
                        free_now as f64 / 1_000_000_000.0,
                        total_rows as f64 / elapsed
                    );

                    // Stop if we're getting close to safety margin
                    if free_now < (safety_margin_gb * 1_000_000_000.0) as u64 {
                        println!("  STOPPING: Approaching safety margin");
                        break;
                    }
                }
            }
            Err(e) => {
                println!("  WRITE FAILED at batch {}: {}", batch_num, e);
                break;
            }
        }
    }

    let write_duration = write_start.elapsed();
    let final_free = get_free_disk_space("/tmp");
    let total_written = initial_free - final_free;

    println!("\nDisk stress result:");
    println!("  Total rows: {}", total_rows);
    println!("  Data written: {:.2} GB", total_written as f64 / 1_000_000_000.0);
    println!("  Duration: {:.2}s ({:.0} rows/sec)",
        write_duration.as_secs_f64(), total_rows as f64 / write_duration.as_secs_f64());
    println!("  Final free space: {:.2} GB", final_free as f64 / 1_000_000_000.0);

    println!("\n✓ Disk stress test complete\n");
}

/// Test 3: Massive entity query
/// Try to query 100K+ entities at once
#[tokio::test]
async fn test_massive_entity_query() {
    println!("\n================================================================================");
    println!("TEST: Massive Entity Query (100K entities)");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "512MB".to_string(),
        max_entities_per_query: 5_000, // Small batch size to stress batching
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write 100K rows
    let num_rows = 100_000;
    let features_per_row = 20;

    println!("Writing {} rows...", num_rows);
    let write_start = Instant::now();

    for batch_start in (0..num_rows).step_by(25_000) {
        let batch_end = (batch_start + 25_000).min(num_rows);
        let rows = generate_rows(batch_start, batch_end - batch_start, features_per_row, 50);
        connector.write_features("massive_query", rows).await.expect("Write failed");
    }

    let write_duration = write_start.elapsed();
    println!("Write complete: {:.2}s ({:.0} rows/sec)\n",
        write_duration.as_secs_f64(), num_rows as f64 / write_duration.as_secs_f64());

    // Test increasingly large queries
    let query_sizes = vec![1_000, 10_000, 50_000, 100_000];

    println!("{:>15} {:>15} {:>15} {:>15}", "Query Size", "Duration", "Rows/sec", "Status");
    println!("{:-<60}", "");

    for &query_size in &query_sizes {
        let entity_keys: Vec<EntityKey> = (0..query_size)
            .map(|i| EntityKey::new("user_id", format!("u{:010}", i)))
            .collect();

        let read_start = Instant::now();
        let result = connector.read_features("massive_query", entity_keys, None).await;
        let read_duration = read_start.elapsed();

        match result {
            Ok(rows) => {
                let rate = rows.len() as f64 / read_duration.as_secs_f64();
                println!(
                    "{:>15} {:>12.2}s {:>12.0}/sec {:>15}",
                    query_size,
                    read_duration.as_secs_f64(),
                    rate,
                    format!("✓ {} rows", rows.len())
                );
            }
            Err(e) => {
                println!(
                    "{:>15} {:>12.2}s {:>15} {:>15}",
                    query_size,
                    read_duration.as_secs_f64(),
                    "-",
                    format!("✗ {}", e)
                );
            }
        }
    }

    println!("\n✓ Massive entity query test complete\n");
}

/// Test 4: Very wide tables
/// Test with 500+ features per row
#[tokio::test]
async fn test_very_wide_tables() {
    println!("\n================================================================================");
    println!("TEST: Very Wide Tables (500 features per row)");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "1GB".to_string(),
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Test with increasing feature counts
    let feature_counts = vec![100, 200, 500];
    let num_rows = 10_000;

    for &features_per_row in &feature_counts {
        println!("\n--- Testing {} features per row ---", features_per_row);
        let view_name = format!("wide_table_{}", features_per_row);

        let rows = generate_rows(0, num_rows, features_per_row, 20);
        let estimated_size = num_rows * features_per_row * 30;
        println!("Estimated data: {:.2} MB", estimated_size as f64 / 1_000_000.0);

        let write_start = Instant::now();
        match connector.write_features(&view_name, rows).await {
            Ok(_) => {
                let write_duration = write_start.elapsed();
                println!("Write SUCCESS: {:.2}s ({:.0} rows/sec)",
                    write_duration.as_secs_f64(), num_rows as f64 / write_duration.as_secs_f64());
            }
            Err(e) => {
                println!("Write FAILED: {}", e);
                continue;
            }
        }

        // Read and verify
        let entity_keys: Vec<EntityKey> = (0..100)
            .map(|i| EntityKey::new("user_id", format!("u{:010}", i)))
            .collect();

        let read_start = Instant::now();
        match connector.read_features(&view_name, entity_keys, None).await {
            Ok(rows) => {
                let read_duration = read_start.elapsed();
                let first_row_features = rows.first().map(|r| r.features.len()).unwrap_or(0);
                println!("Read SUCCESS: {} rows in {:.2}ms, {} features/row",
                    rows.len(), read_duration.as_secs_f64() * 1000.0, first_row_features);
            }
            Err(e) => {
                println!("Read FAILED: {}", e);
            }
        }
    }

    println!("\n✓ Wide tables test complete\n");
}

/// Test 5: Concurrent stress
/// Multiple parallel operations
#[tokio::test]
async fn test_concurrent_stress() {
    println!("\n================================================================================");
    println!("TEST: Concurrent Stress (parallel operations)");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "256MB".to_string(),
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = std::sync::Arc::new(
        DeltaStorageConnector::new(storage_path, engine_config)
            .await
            .expect("Failed to create connector")
    );

    // First write some data
    println!("Preparing data...");
    let rows = generate_rows(0, 20_000, 30, 100);
    connector.write_features("concurrent_test", rows).await.expect("Write failed");
    println!("Data prepared: 20K rows\n");

    // Launch concurrent readers and writers
    let num_concurrent = 20;
    println!("Launching {} concurrent operations...\n", num_concurrent);

    let mut handles = Vec::new();
    let start = Instant::now();

    for i in 0..num_concurrent {
        let conn = connector.clone();
        let handle = tokio::spawn(async move {
            let op_start = Instant::now();

            if i % 2 == 0 {
                // Reader
                let entity_keys: Vec<EntityKey> = (0..1000)
                    .map(|j| EntityKey::new("user_id", format!("u{:010}", (i * 1000 + j) % 20000)))
                    .collect();

                match conn.read_features("concurrent_test", entity_keys, None).await {
                    Ok(rows) => (i, "read", true, rows.len(), op_start.elapsed()),
                    Err(_) => (i, "read", false, 0, op_start.elapsed()),
                }
            } else {
                // Writer
                let rows = generate_rows(i * 1000, 500, 20, 50);
                match conn.write_features("concurrent_test", rows).await {
                    Ok(_) => (i, "write", true, 500, op_start.elapsed()),
                    Err(_) => (i, "write", false, 0, op_start.elapsed()),
                }
            }
        });
        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    let mut fail_count = 0;

    for handle in handles {
        match handle.await {
            Ok((id, op, success, count, duration)) => {
                if success {
                    success_count += 1;
                    println!("  Op {}: {} {} rows in {:.2}ms", id, op, count, duration.as_secs_f64() * 1000.0);
                } else {
                    fail_count += 1;
                    println!("  Op {}: {} FAILED in {:.2}ms", id, op, duration.as_secs_f64() * 1000.0);
                }
            }
            Err(e) => {
                fail_count += 1;
                println!("  Task panicked: {}", e);
            }
        }
    }

    let total_duration = start.elapsed();
    println!("\nConcurrent stress result:");
    println!("  Total operations: {}", num_concurrent);
    println!("  Success: {}, Failed: {}", success_count, fail_count);
    println!("  Total duration: {:.2}s", total_duration.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", num_concurrent as f64 / total_duration.as_secs_f64());

    println!("\n✓ Concurrent stress test complete\n");
}

// ============================================================================
// Helpers
// ============================================================================

fn get_free_disk_space(path: &str) -> u64 {
    use std::process::Command;

    let output = Command::new("df")
        .args(["-k", path])
        .output()
        .expect("Failed to execute df");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();

    if lines.len() >= 2 {
        let parts: Vec<&str> = lines[1].split_whitespace().collect();
        if parts.len() >= 4 {
            // Available space is in KB
            return parts[3].parse::<u64>().unwrap_or(0) * 1024;
        }
    }

    0
}
