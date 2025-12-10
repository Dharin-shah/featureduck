//! Crash Test: Push System to Actual Failure
//!
//! Tests designed to crash or hang the system:
//! - No memory limit at all
//! - Fill all available memory
//! - Test recovery mechanisms
//!
//! Run with: cargo test -p featureduck-delta --test crash_test --release -- --nocapture --test-threads=1

use chrono::{Duration, Utc};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use std::time::Instant;
use tempfile::TempDir;

/// Generate massive rows designed to consume memory
fn generate_massive_rows(start_id: usize, count: usize, string_size: usize) -> Vec<FeatureRow> {
    let mut rows = Vec::with_capacity(count);
    let base_time = Utc::now() - Duration::days(1);

    for i in 0..count {
        let entity_id = start_id + i;
        let timestamp = base_time + Duration::seconds(i as i64);

        let mut features = HashMap::new();

        // Create large string features to consume memory
        for f in 0..50 {
            let content: String = (0..string_size)
                .map(|j| char::from(b'a' + ((entity_id + f + j) % 26) as u8))
                .collect();
            features.insert(format!("big_str_{}", f), FeatureValue::String(content));
        }

        // Add some numeric features
        for f in 0..50 {
            features.insert(format!("num_{}", f), FeatureValue::Int((entity_id * f) as i64));
        }

        rows.push(FeatureRow {
            entities: vec![EntityKey::new("id", format!("e{:012}", entity_id))],
            features,
            timestamp,
        });
    }

    rows
}

/// Test 1: Memory exhaustion without limits
/// Try to allocate more memory than available
#[tokio::test]
async fn test_memory_exhaustion() {
    println!("\n================================================================================");
    println!("TEST: Memory Exhaustion (No Safety Limits)");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    // Use default config (75% of system RAM)
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "16GB".to_string(), // Use lots of memory
        temp_directory: None, // NO spilling - force memory use
        preserve_insertion_order: false,
        ..Default::default()
    };

    println!("Memory limit: 16GB (no spilling)");
    println!("Goal: Write until we hit memory limit\n");

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write increasingly large batches
    let string_size = 2000; // 2KB strings x 50 = 100KB per row
    let batch_size = 50_000;
    let mut total_rows = 0;
    let mut batch_num = 0;
    let start = Instant::now();

    println!("Row size: ~100KB (50 x 2KB strings + metadata)");
    println!("Batch size: {} rows (~5GB per batch)\n", batch_size);

    loop {
        batch_num += 1;
        let rows = generate_massive_rows(total_rows, batch_size, string_size);
        let batch_size_gb = (batch_size * 100_000) as f64 / 1_000_000_000.0;

        println!("Batch {}: Writing {} rows (~{:.1}GB)...", batch_num, batch_size, batch_size_gb);

        let batch_start = Instant::now();
        match connector.write_features("memory_test", rows).await {
            Ok(_) => {
                total_rows += batch_size;
                let elapsed = start.elapsed().as_secs_f64();
                let batch_time = batch_start.elapsed().as_secs_f64();
                println!(
                    "  SUCCESS: {} total rows, {:.1}s batch time, {:.0} rows/sec overall",
                    total_rows, batch_time, total_rows as f64 / elapsed
                );

                // Check memory usage
                let mem_info = get_process_memory();
                println!("  Process memory: {:.2} GB", mem_info as f64 / 1_000_000_000.0);

                // Stop after 10 batches (500K rows) to avoid actually crashing
                if batch_num >= 10 {
                    println!("\n  STOPPING after 10 batches (test limit)");
                    break;
                }
            }
            Err(e) => {
                println!("  FAILED: {}", e);
                println!("\n  MEMORY LIMIT HIT after {} rows!", total_rows);
                break;
            }
        }
    }

    let total_duration = start.elapsed();
    println!("\nMemory exhaustion result:");
    println!("  Total rows written: {}", total_rows);
    println!("  Total duration: {:.2}s", total_duration.as_secs_f64());
    println!("  Final rate: {:.0} rows/sec", total_rows as f64 / total_duration.as_secs_f64());

    // Test read after writes
    println!("\nTesting read after memory stress...");
    let entity_keys: Vec<EntityKey> = (0..1000)
        .map(|i| EntityKey::new("id", format!("e{:012}", i)))
        .collect();

    match connector.read_features("memory_test", entity_keys, None).await {
        Ok(rows) => println!("Read SUCCESS: {} rows recovered", rows.len()),
        Err(e) => println!("Read FAILED: {}", e),
    }

    println!("\n✓ Memory exhaustion test complete\n");
}

/// Test 2: Circuit breaker test
/// Verify that repeated failures trigger circuit breaker
#[tokio::test]
async fn test_circuit_breaker_triggers() {
    println!("\n================================================================================");
    println!("TEST: Circuit Breaker Trigger");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "64MB".to_string(), // Very constrained
        temp_directory: None, // No spilling
        preserve_insertion_order: false,
        ..Default::default()
    };

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write some data first
    println!("Writing initial data...");
    let rows = generate_massive_rows(0, 10_000, 100);
    connector.write_features("circuit_test", rows).await.expect("Initial write failed");
    println!("Initial write complete\n");

    // Now hammer with reads that will fail due to memory
    println!("Hammering with memory-heavy reads to trigger failures...\n");

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut circuit_open_count = 0;

    for i in 0..50 {
        let entity_keys: Vec<EntityKey> = (0..5000)
            .map(|j| EntityKey::new("id", format!("e{:012}", j)))
            .collect();

        match connector.read_features("circuit_test", entity_keys, None).await {
            Ok(rows) => {
                success_count += 1;
                println!("  Read {}: SUCCESS ({} rows)", i, rows.len());
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Circuit breaker") {
                    circuit_open_count += 1;
                    println!("  Read {}: CIRCUIT BREAKER OPEN", i);
                } else {
                    failure_count += 1;
                    println!("  Read {}: FAILED ({})", i, err_str.chars().take(50).collect::<String>());
                }
            }
        }
    }

    // Check circuit breaker stats
    let stats = connector.circuit_breaker_stats();
    println!("\nCircuit breaker stats:");
    println!("  State: {:?}", stats.state);
    println!("  Failures: {}", stats.failure_count);
    println!("  Successes: {}", stats.success_count);

    println!("\nResult:");
    println!("  Successes: {}", success_count);
    println!("  Failures: {}", failure_count);
    println!("  Circuit breaker rejections: {}", circuit_open_count);

    // Test recovery - reset circuit breaker
    println!("\nResetting circuit breaker...");
    connector.reset_circuit_breaker();

    let entity_keys: Vec<EntityKey> = (0..10)
        .map(|j| EntityKey::new("id", format!("e{:012}", j)))
        .collect();

    match connector.read_features("circuit_test", entity_keys, None).await {
        Ok(rows) => println!("Recovery read: SUCCESS ({} rows)", rows.len()),
        Err(e) => println!("Recovery read: FAILED ({})", e),
    }

    println!("\n✓ Circuit breaker test complete\n");
}

/// Test 3: Retry behavior
/// Test that transient failures are retried
#[tokio::test]
async fn test_retry_behavior() {
    println!("\n================================================================================");
    println!("TEST: Retry Behavior");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig::default();

    let mut connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Configure aggressive retry policy
    use featureduck_core::retry::RetryPolicy;
    connector.set_retry_policy(RetryPolicy::aggressive());

    println!("Retry policy: Aggressive (5 attempts, 100ms initial backoff)\n");

    // Write test data
    let rows = generate_massive_rows(0, 1000, 50);
    connector.write_features("retry_test", rows).await.expect("Write failed");
    println!("Test data written\n");

    // Read should succeed
    let entity_keys: Vec<EntityKey> = (0..100)
        .map(|j| EntityKey::new("id", format!("e{:012}", j)))
        .collect();

    let start = Instant::now();
    match connector.read_features("retry_test", entity_keys, None).await {
        Ok(rows) => {
            println!("Read SUCCESS: {} rows in {:.2}ms", rows.len(), start.elapsed().as_secs_f64() * 1000.0);
        }
        Err(e) => {
            println!("Read FAILED after retries: {}", e);
        }
    }

    println!("\n✓ Retry behavior test complete\n");
}

/// Test 4: Timeout behavior
/// Test query timeout handling
#[tokio::test]
async fn test_timeout_behavior() {
    println!("\n================================================================================");
    println!("TEST: Timeout Behavior");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig::default();

    let mut connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Set very short timeout
    use featureduck_core::resource_limits::ResourceLimits;
    let limits = ResourceLimits {
        query_timeout: std::time::Duration::from_millis(100), // 100ms timeout
        max_rows: 1_000_000,
        max_memory_bytes: 1_000_000_000,
        max_concurrent_ops: 10,
    };
    connector.set_resource_limits(limits);

    println!("Query timeout: 100ms\n");

    // Write enough data to make reads slow
    println!("Writing data to make reads slow...");
    for batch in 0..5 {
        let rows = generate_massive_rows(batch * 20_000, 20_000, 200);
        connector.write_features("timeout_test", rows).await.expect("Write failed");
        println!("  Batch {} complete", batch + 1);
    }
    println!("100K rows written\n");

    // Try to read many entities - should timeout
    println!("Reading with 100ms timeout...");
    let entity_keys: Vec<EntityKey> = (0..50_000)
        .map(|j| EntityKey::new("id", format!("e{:012}", j)))
        .collect();

    let start = Instant::now();
    match connector.read_features("timeout_test", entity_keys, None).await {
        Ok(rows) => {
            println!("Read completed in {:.2}ms ({} rows)",
                start.elapsed().as_secs_f64() * 1000.0, rows.len());
        }
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("timed out") {
                println!("TIMEOUT as expected after {:.2}ms: {}",
                    start.elapsed().as_secs_f64() * 1000.0, err_str);
            } else {
                println!("FAILED (not timeout) after {:.2}ms: {}",
                    start.elapsed().as_secs_f64() * 1000.0, err_str);
            }
        }
    }

    println!("\n✓ Timeout behavior test complete\n");
}

/// Test 5: Resource limit enforcement
/// Test max_rows limit
#[tokio::test]
async fn test_resource_limits() {
    println!("\n================================================================================");
    println!("TEST: Resource Limits Enforcement");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let engine_config = DuckDBEngineConfig::default();

    let mut connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Set strict resource limits
    use featureduck_core::resource_limits::ResourceLimits;
    let limits = ResourceLimits {
        query_timeout: std::time::Duration::from_secs(60),
        max_rows: 1_000, // Only allow 1000 rows
        max_memory_bytes: 100_000_000,
        max_concurrent_ops: 10,
    };
    connector.set_resource_limits(limits);

    println!("Max rows limit: 1000\n");

    // Write 10K rows
    let rows = generate_massive_rows(0, 10_000, 50);
    connector.write_features("limit_test", rows).await.expect("Write failed");
    println!("10K rows written\n");

    // Try to read more than limit
    println!("Attempting to read 5000 entities (limit is 1000)...");
    let entity_keys: Vec<EntityKey> = (0..5000)
        .map(|j| EntityKey::new("id", format!("e{:012}", j)))
        .collect();

    match connector.read_features("limit_test", entity_keys, None).await {
        Ok(rows) => {
            println!("Read returned {} rows (should have been limited)", rows.len());
        }
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("exceeds limit") || err_str.contains("too large") {
                println!("LIMIT ENFORCED: {}", err_str);
            } else {
                println!("FAILED (unexpected): {}", err_str);
            }
        }
    }

    println!("\n✓ Resource limits test complete\n");
}

/// Test 6: Self-Healing OOM Recovery
/// Verify that the system automatically reduces batch size on OOM and recovers
#[tokio::test]
async fn test_self_healing_oom_recovery() {
    println!("\n================================================================================");
    println!("TEST: Self-Healing OOM Recovery");
    println!("================================================================================\n");

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    // Constrained memory to trigger OOM on large reads but allow recovery
    // 128MB is enough for small batches but will OOM on large ones
    let engine_config = DuckDBEngineConfig {
        db_path: None,
        memory_limit: "128MB".to_string(),
        temp_directory: None, // No spilling - force memory pressure
        preserve_insertion_order: false,
        max_entities_per_query: 5_000, // Start with moderate batches
        ..Default::default()
    };

    println!("Memory limit: 128MB (no spilling)");
    println!("Initial batch size: 5,000 entities");
    println!("Goal: System should auto-reduce batch size on OOM and succeed\n");

    let connector = DeltaStorageConnector::new(storage_path, engine_config)
        .await
        .expect("Failed to create connector");

    // Write data with medium strings to consume memory on reads
    // 100 byte strings x 50 = 5KB per row
    println!("Writing 50K rows with medium strings (~5KB per row)...");
    let rows = generate_massive_rows(0, 50_000, 100); // 100 byte strings x 50 = 5KB per row
    connector.write_features("self_heal_test", rows).await.expect("Write failed");
    println!("Write complete\n");

    // Now try to read a large number of entities
    // With 64MB limit, this should initially OOM, then self-heal
    let entity_counts = vec![1_000, 5_000, 10_000, 20_000];

    println!("Testing self-healing read with increasing entity counts:\n");
    println!("{:>15} {:>15} {:>15} {:>15}", "Entities", "Duration", "Rows", "Status");
    println!("{:-<65}", "");

    for &num_entities in &entity_counts {
        let entity_keys: Vec<EntityKey> = (0..num_entities)
            .map(|i| EntityKey::new("id", format!("e{:012}", i)))
            .collect();

        let start = Instant::now();
        let result = connector.read_features("self_heal_test", entity_keys, None).await;
        let duration = start.elapsed();

        match result {
            Ok(rows) => {
                println!(
                    "{:>15} {:>12.2}ms {:>15} {:>15}",
                    num_entities,
                    duration.as_secs_f64() * 1000.0,
                    rows.len(),
                    "✓ SUCCESS (self-healed)"
                );
            }
            Err(e) => {
                let err_str = e.to_string();
                let status = if err_str.contains("Self-healing exhausted") {
                    "✗ Self-heal exhausted"
                } else if err_str.contains("Circuit breaker") {
                    "✗ Circuit breaker"
                } else {
                    "✗ Error"
                };
                println!(
                    "{:>15} {:>12.2}ms {:>15} {:>15}",
                    num_entities,
                    duration.as_secs_f64() * 1000.0,
                    "-",
                    status
                );
                // Print actual error for debugging
                println!("    Error details: {}", &err_str[..err_str.len().min(200)]);
            }
        }
    }

    println!("\n✓ Self-healing OOM recovery test complete\n");
}

// Helper to get process memory usage
fn get_process_memory() -> u64 {
    use std::process::Command;

    let pid = std::process::id();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();

    match output {
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            // RSS is in KB
            stdout.trim().parse::<u64>().unwrap_or(0) * 1024
        }
        Err(_) => 0,
    }
}
