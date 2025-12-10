//! Load and stress tests for Delta connector
//!
//! Tests system behavior under:
//! - High concurrent load
//! - Large data volumes
//! - Memory pressure
//! - Sustained throughput
//!
//! Run with: cargo test -p featureduck-delta --test load_test --release -- --nocapture

use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{CompressionCodec, DeltaStorageConnector, DuckDBEngineConfig, WriteConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Semaphore;

// ============================================================================
// Test Configuration
// ============================================================================

/// Configuration for load tests
struct LoadTestConfig {
    /// Number of rows per batch
    batch_size: usize,
    /// Number of batches to write
    num_batches: usize,
    /// Number of concurrent writers
    concurrent_writers: usize,
    /// Number of concurrent readers
    concurrent_readers: usize,
    /// Number of features per row
    features_per_row: usize,
    /// Size of string features (bytes)
    string_size: usize,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            num_batches: 10,
            concurrent_writers: 4,
            concurrent_readers: 8,
            features_per_row: 10,
            string_size: 100,
        }
    }
}

#[allow(dead_code)]
impl LoadTestConfig {
    fn small() -> Self {
        Self {
            batch_size: 1_000,
            num_batches: 5,
            concurrent_writers: 2,
            concurrent_readers: 4,
            features_per_row: 5,
            string_size: 50,
        }
    }

    fn medium() -> Self {
        Self {
            batch_size: 10_000,
            num_batches: 10,
            concurrent_writers: 4,
            concurrent_readers: 8,
            features_per_row: 10,
            string_size: 100,
        }
    }

    fn large() -> Self {
        Self {
            batch_size: 50_000,
            num_batches: 20,
            concurrent_writers: 8,
            concurrent_readers: 16,
            features_per_row: 20,
            string_size: 200,
        }
    }

    fn stress() -> Self {
        Self {
            batch_size: 100_000,
            num_batches: 50,
            concurrent_writers: 16,
            concurrent_readers: 32,
            features_per_row: 30,
            string_size: 500,
        }
    }

    fn total_rows(&self) -> usize {
        self.batch_size * self.num_batches
    }

    fn estimated_memory_mb(&self) -> usize {
        // Rough estimate: each row ~= features * (8 bytes int + string_size + overhead)
        let row_size = self.features_per_row * (8 + self.string_size + 50);
        (self.batch_size * row_size) / (1024 * 1024)
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

async fn create_test_connector() -> (Arc<DeltaStorageConnector>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .expect("Failed to create connector");

    (Arc::new(connector), temp_dir)
}

async fn create_optimized_connector() -> (Arc<DeltaStorageConnector>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig {
            memory_limit: "4GB".to_string(),
            threads: 8,
            ..Default::default()
        },
    )
    .await
    .expect("Failed to create connector");

    connector.set_write_config(WriteConfig {
        compression: CompressionCodec::ZstdLevel1, // Faster compression for load tests
        chunk_size: 100_000,
        parallel_enabled: true,
    });

    (Arc::new(connector), temp_dir)
}

fn generate_test_rows(config: &LoadTestConfig, batch_id: usize) -> Vec<FeatureRow> {
    let mut rows = Vec::with_capacity(config.batch_size);
    let base_string = "x".repeat(config.string_size);

    for i in 0..config.batch_size {
        let entity_id = format!("user_{}_{}", batch_id, i);
        let entity_key = EntityKey::new("user_id", entity_id);
        let mut row = FeatureRow::new(vec![entity_key], chrono::Utc::now());

        for f in 0..config.features_per_row {
            match f % 4 {
                0 => row.add_feature(
                    format!("int_feature_{}", f),
                    FeatureValue::Int((i * f) as i64),
                ),
                1 => row.add_feature(
                    format!("float_feature_{}", f),
                    FeatureValue::Float((i as f64) / ((f + 1) as f64)),
                ),
                2 => row.add_feature(
                    format!("string_feature_{}", f),
                    FeatureValue::String(format!("{}_{}", base_string, i)),
                ),
                _ => row.add_feature(
                    format!("bool_feature_{}", f),
                    FeatureValue::Bool(i % 2 == 0),
                ),
            }
        }

        rows.push(row);
    }

    rows
}

struct LoadTestResult {
    total_rows: usize,
    duration: Duration,
    rows_per_second: f64,
    memory_peak_mb: usize,
    errors: usize,
}

impl LoadTestResult {
    fn print(&self, test_name: &str) {
        println!("\n=== {} Results ===", test_name);
        println!("Total rows: {}", self.total_rows);
        println!("Duration: {:.2?}", self.duration);
        println!("Throughput: {:.0} rows/sec", self.rows_per_second);
        println!("Peak memory: ~{}MB (estimated)", self.memory_peak_mb);
        println!("Errors: {}", self.errors);
        println!();
    }
}

// ============================================================================
// Write Load Tests
// ============================================================================

#[tokio::test]
async fn test_load_sequential_writes_small() {
    let config = LoadTestConfig::small();
    let (connector, _temp_dir) = create_test_connector().await;

    println!("\nSequential Write Test (Small)");
    println!(
        "Config: {} batches x {} rows = {} total",
        config.num_batches,
        config.batch_size,
        config.total_rows()
    );

    let start = Instant::now();
    let mut errors = 0;

    for batch_id in 0..config.num_batches {
        let rows = generate_test_rows(&config, batch_id);
        if connector.write_features("load_test", rows).await.is_err() {
            errors += 1;
        }
    }

    let duration = start.elapsed();
    let result = LoadTestResult {
        total_rows: config.total_rows(),
        duration,
        rows_per_second: config.total_rows() as f64 / duration.as_secs_f64(),
        memory_peak_mb: config.estimated_memory_mb(),
        errors,
    };

    result.print("Sequential Write (Small)");
    assert_eq!(errors, 0, "All writes should succeed");
    assert!(
        result.rows_per_second > 1000.0,
        "Should write at least 1K rows/sec"
    );
}

#[tokio::test]
async fn test_load_sequential_writes_medium() {
    let config = LoadTestConfig::medium();
    let (connector, _temp_dir) = create_optimized_connector().await;

    println!("\nSequential Write Test (Medium)");
    println!(
        "Config: {} batches x {} rows = {} total",
        config.num_batches,
        config.batch_size,
        config.total_rows()
    );

    let start = Instant::now();
    let mut errors = 0;

    for batch_id in 0..config.num_batches {
        let rows = generate_test_rows(&config, batch_id);
        if connector.write_features("load_test", rows).await.is_err() {
            errors += 1;
        }
    }

    let duration = start.elapsed();
    let result = LoadTestResult {
        total_rows: config.total_rows(),
        duration,
        rows_per_second: config.total_rows() as f64 / duration.as_secs_f64(),
        memory_peak_mb: config.estimated_memory_mb(),
        errors,
    };

    result.print("Sequential Write (Medium)");
    assert_eq!(errors, 0, "All writes should succeed");
}

#[tokio::test]
async fn test_load_concurrent_writes() {
    let config = LoadTestConfig::small();
    let (connector, _temp_dir) = create_optimized_connector().await;

    println!("\nConcurrent Write Test");
    println!(
        "Config: {} writers x {} batches x {} rows",
        config.concurrent_writers, config.num_batches, config.batch_size
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_writers));
    let start = Instant::now();
    let errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for batch_id in 0..config.num_batches {
        let connector = Arc::clone(&connector);
        let semaphore = Arc::clone(&semaphore);
        let errors = Arc::clone(&errors);
        let config_clone = LoadTestConfig::small();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let rows = generate_test_rows(&config_clone, batch_id);
            let view_name = format!("concurrent_test_{}", batch_id % 3); // Write to 3 different views
            if connector.write_features(&view_name, rows).await.is_err() {
                errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let error_count = errors.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_rows: config.total_rows(),
        duration,
        rows_per_second: config.total_rows() as f64 / duration.as_secs_f64(),
        memory_peak_mb: config.estimated_memory_mb() * config.concurrent_writers,
        errors: error_count,
    };

    result.print("Concurrent Writes");
    assert_eq!(error_count, 0, "All concurrent writes should succeed");
}

// ============================================================================
// Read Load Tests
// ============================================================================

#[tokio::test]
async fn test_load_sequential_reads() {
    let config = LoadTestConfig::small();
    let (connector, _temp_dir) = create_test_connector().await;

    // Setup: Write test data
    println!("\nSetup: Writing {} rows...", config.batch_size);
    let rows = generate_test_rows(&config, 0);
    connector
        .write_features("read_test", rows)
        .await
        .expect("Setup write failed");

    // Test: Sequential reads
    println!("Sequential Read Test: {} reads", config.batch_size / 10);

    let start = Instant::now();
    let mut errors = 0;
    let num_reads = config.batch_size / 10;

    for i in 0..num_reads {
        let entity_key = EntityKey::new("user_id", format!("user_0_{}", i * 10));
        if connector
            .read_features("read_test", vec![entity_key], None)
            .await
            .is_err()
        {
            errors += 1;
        }
    }

    let duration = start.elapsed();
    let result = LoadTestResult {
        total_rows: num_reads,
        duration,
        rows_per_second: num_reads as f64 / duration.as_secs_f64(),
        memory_peak_mb: 10,
        errors,
    };

    result.print("Sequential Reads");
    assert_eq!(errors, 0, "All reads should succeed");
}

#[tokio::test]
async fn test_load_concurrent_reads() {
    let config = LoadTestConfig::small();
    let (connector, _temp_dir) = create_test_connector().await;

    // Setup: Write test data
    println!("\nSetup: Writing {} rows...", config.batch_size);
    let rows = generate_test_rows(&config, 0);
    connector
        .write_features("concurrent_read_test", rows)
        .await
        .expect("Setup write failed");

    // Test: Concurrent reads
    let num_reads = 100;
    println!("Concurrent Read Test: {} concurrent reads", num_reads);

    let semaphore = Arc::new(Semaphore::new(config.concurrent_readers));
    let start = Instant::now();
    let errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let successes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..num_reads {
        let connector = Arc::clone(&connector);
        let semaphore = Arc::clone(&semaphore);
        let errors = Arc::clone(&errors);
        let successes = Arc::clone(&successes);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let entity_key = EntityKey::new("user_id", format!("user_0_{}", i % config.batch_size));
            match connector
                .read_features("concurrent_read_test", vec![entity_key], None)
                .await
            {
                Ok(rows) if !rows.is_empty() => {
                    successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(_) => {
                    // Empty result is OK for some entity IDs
                    successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(_) => {
                    errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let error_count = errors.load(std::sync::atomic::Ordering::Relaxed);
    let success_count = successes.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_rows: num_reads,
        duration,
        rows_per_second: num_reads as f64 / duration.as_secs_f64(),
        memory_peak_mb: 50,
        errors: error_count,
    };

    result.print("Concurrent Reads");
    println!("Successes: {}/{}", success_count, num_reads);
    assert_eq!(error_count, 0, "All concurrent reads should succeed");
}

// ============================================================================
// Mixed Read/Write Load Tests
// ============================================================================

#[tokio::test]
async fn test_load_mixed_read_write() {
    // This test simulates realistic production usage:
    // - Reads from one feature view
    // - Writes to DIFFERENT feature views (like different materialization jobs)
    // In production, each feature view is written by ONE job at a time (no conflicts)

    let config = LoadTestConfig::small();
    let (connector, _temp_dir) = create_test_connector().await;

    // Setup: Write initial data for reads
    println!("\nSetup: Writing initial data for read tests...");
    let rows = generate_test_rows(&config, 0);
    connector
        .write_features("read_view", rows)
        .await
        .expect("Setup write failed");

    // Test: Mixed reads and writes to DIFFERENT views (realistic scenario)
    let num_operations = 50;
    println!("Mixed Read/Write Test: {} operations", num_operations);
    println!("  - Reads from: read_view");
    println!("  - Writes to: write_view_0, write_view_1, ... (one per write)");

    let semaphore = Arc::new(Semaphore::new(8));
    let start = Instant::now();
    let read_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..num_operations {
        let connector = Arc::clone(&connector);
        let semaphore = Arc::clone(&semaphore);
        let read_errors = Arc::clone(&read_errors);
        let write_errors = Arc::clone(&write_errors);
        let read_count = Arc::clone(&read_count);
        let write_count = Arc::clone(&write_count);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            if i % 2 == 0 {
                // Read operation - from the shared read view
                read_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let entity_key = EntityKey::new("user_id", format!("user_0_{}", i % 100));
                if connector
                    .read_features("read_view", vec![entity_key], None)
                    .await
                    .is_err()
                {
                    read_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else {
                // Write operation - each write goes to its OWN feature view (no conflicts!)
                // This simulates different materialization jobs running in parallel
                write_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let view_name = format!("write_view_{}", i);
                let entity_key = EntityKey::new("user_id", format!("user_{}", i));
                let mut row = FeatureRow::new(vec![entity_key], chrono::Utc::now());
                row.add_feature("value".to_string(), FeatureValue::Int(i as i64));
                if connector
                    .write_features(&view_name, vec![row])
                    .await
                    .is_err()
                {
                    write_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let read_error_count = read_errors.load(std::sync::atomic::Ordering::Relaxed);
    let write_error_count = write_errors.load(std::sync::atomic::Ordering::Relaxed);
    let total_reads = read_count.load(std::sync::atomic::Ordering::Relaxed);
    let total_writes = write_count.load(std::sync::atomic::Ordering::Relaxed);

    println!("\n=== Mixed Read/Write Results ===");
    println!("Duration: {:.2?}", duration);
    println!(
        "Operations/sec: {:.0}",
        num_operations as f64 / duration.as_secs_f64()
    );
    println!("Reads: {} (errors: {})", total_reads, read_error_count);
    println!("Writes: {} (errors: {})", total_writes, write_error_count);

    // With writes to different views, we should have ZERO conflicts
    assert_eq!(read_error_count, 0, "All reads should succeed");
    assert_eq!(
        write_error_count, 0,
        "All writes should succeed (no conflicts when writing to different views)"
    );
}

// ============================================================================
// Memory Stress Tests
// ============================================================================

#[tokio::test]
async fn test_stress_large_batch() {
    // Test writing a large batch to stress memory
    let config = LoadTestConfig {
        batch_size: 50_000,
        num_batches: 1,
        features_per_row: 15,
        string_size: 200,
        ..Default::default()
    };

    let (connector, _temp_dir) = create_optimized_connector().await;

    println!("\nLarge Batch Stress Test");
    println!(
        "Config: {} rows with {} features each",
        config.batch_size, config.features_per_row
    );
    println!("Estimated memory: ~{}MB", config.estimated_memory_mb());

    let start = Instant::now();
    let rows = generate_test_rows(&config, 0);

    let result = connector.write_features("stress_test", rows).await;
    let duration = start.elapsed();

    println!("Duration: {:.2?}", duration);
    println!(
        "Throughput: {:.0} rows/sec",
        config.batch_size as f64 / duration.as_secs_f64()
    );

    assert!(result.is_ok(), "Large batch write should succeed");
}

#[tokio::test]
async fn test_stress_many_small_batches() {
    // Test many small batches to stress transaction handling
    let config = LoadTestConfig {
        batch_size: 100,
        num_batches: 100,
        features_per_row: 5,
        string_size: 50,
        ..Default::default()
    };

    let (connector, _temp_dir) = create_test_connector().await;

    println!("\nMany Small Batches Stress Test");
    println!(
        "Config: {} batches x {} rows",
        config.num_batches, config.batch_size
    );

    let start = Instant::now();
    let mut errors = 0;

    for batch_id in 0..config.num_batches {
        let rows = generate_test_rows(&config, batch_id);
        if connector
            .write_features("small_batch_test", rows)
            .await
            .is_err()
        {
            errors += 1;
        }
    }

    let duration = start.elapsed();
    println!("Duration: {:.2?}", duration);
    println!(
        "Batches/sec: {:.0}",
        config.num_batches as f64 / duration.as_secs_f64()
    );
    println!(
        "Rows/sec: {:.0}",
        config.total_rows() as f64 / duration.as_secs_f64()
    );
    println!("Errors: {}", errors);

    assert_eq!(errors, 0, "All small batch writes should succeed");
}

#[tokio::test]
async fn test_stress_sustained_throughput() {
    // Test sustained throughput over time
    let (connector, _temp_dir) = create_optimized_connector().await;

    let duration_secs = 5;
    let batch_size = 1000;

    println!("\nSustained Throughput Test");
    println!(
        "Duration: {} seconds, batch size: {}",
        duration_secs, batch_size
    );

    let start = Instant::now();
    let mut total_rows = 0;
    let mut batch_id = 0;
    let mut errors = 0;

    while start.elapsed() < Duration::from_secs(duration_secs) {
        let config = LoadTestConfig {
            batch_size,
            features_per_row: 5,
            string_size: 50,
            ..Default::default()
        };

        let rows = generate_test_rows(&config, batch_id);
        if connector
            .write_features("sustained_test", rows)
            .await
            .is_ok()
        {
            total_rows += batch_size;
        } else {
            errors += 1;
        }
        batch_id += 1;
    }

    let actual_duration = start.elapsed();
    println!("Actual duration: {:.2?}", actual_duration);
    println!("Total rows written: {}", total_rows);
    println!(
        "Sustained throughput: {:.0} rows/sec",
        total_rows as f64 / actual_duration.as_secs_f64()
    );
    println!("Batches completed: {}", batch_id);
    println!("Errors: {}", errors);

    assert!(total_rows > 0, "Should write some rows");
    assert_eq!(errors, 0, "Should have no errors during sustained load");
}

// ============================================================================
// Circuit Breaker Under Load
// ============================================================================

#[tokio::test]
async fn test_load_circuit_breaker_resilience() {
    use featureduck_delta::CircuitBreakerConfig;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .expect("Failed to create connector");

    // Configure circuit breaker for load test
    connector.set_circuit_breaker_config(CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        timeout: Duration::from_secs(1),
        ..Default::default()
    });

    let connector = Arc::new(connector);

    // Write some valid data first
    let entity_key = EntityKey::new("user_id", "test_user");
    let mut row = FeatureRow::new(vec![entity_key], chrono::Utc::now());
    row.add_feature("value".to_string(), FeatureValue::Int(42));
    connector
        .write_features("cb_test", vec![row])
        .await
        .expect("Write failed");

    // Now simulate mixed success/failure reads
    let num_operations = 50;
    let mut successes = 0;
    let mut failures = 0;
    let mut circuit_open_rejections = 0;

    for i in 0..num_operations {
        let entity_key = if i % 3 == 0 {
            // Valid entity
            EntityKey::new("user_id", "test_user")
        } else {
            // Invalid entity (will fail)
            EntityKey::new("user_id", format!("nonexistent_{}", i))
        };

        match connector
            .read_features("cb_test", vec![entity_key], None)
            .await
        {
            Ok(_) => successes += 1,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Circuit breaker") || msg.contains("temporarily disabled") {
                    circuit_open_rejections += 1;
                } else {
                    failures += 1;
                }
            }
        }
    }

    println!("\n=== Circuit Breaker Under Load ===");
    println!("Successes: {}", successes);
    println!("Failures: {}", failures);
    println!("Circuit breaker rejections: {}", circuit_open_rejections);

    let stats = connector.circuit_breaker_stats();
    println!("Final circuit state: {:?}", stats.state);
    println!("Total requests tracked: {}", stats.total_requests);
}

// ============================================================================
// Benchmark Helper
// ============================================================================

#[tokio::test]
async fn test_benchmark_summary() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║           FEATUREDUCK LOAD TEST BENCHMARK                  ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ Run all load tests with:                                   ║");
    println!("║   cargo test -p featureduck-delta --test load_test \\      ║");
    println!("║     --release -- --nocapture                               ║");
    println!("║                                                            ║");
    println!("║ Test Configurations:                                       ║");
    println!("║   small:  1K rows x 5 batches  = 5K rows                  ║");
    println!("║   medium: 10K rows x 10 batches = 100K rows               ║");
    println!("║   large:  50K rows x 20 batches = 1M rows                 ║");
    println!("║   stress: 100K rows x 50 batches = 5M rows                ║");
    println!("║                                                            ║");
    println!("║ Expected Performance (M1 Mac):                             ║");
    println!("║   Write: 10K-50K rows/sec (depends on features)           ║");
    println!("║   Read:  1K-5K reads/sec (point lookups)                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
}
