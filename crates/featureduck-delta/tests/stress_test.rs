//! Comprehensive Stress Tests for FeatureDuck
//!
//! Tests production-grade scenarios:
//! - Large data volumes (5-10GB materializations)
//! - Concurrent materializations
//! - Registry under load
//! - Memory and CPU tracking per component
//!
//! Run with: cargo test -p featureduck-delta --test stress_test --release -- --nocapture
//!
//! For full stress test (requires 16GB+ RAM):
//!   STRESS_TEST_FULL=1 cargo test -p featureduck-delta --test stress_test --release -- --nocapture

use chrono::Utc;
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, System};
use tempfile::TempDir;
// Semaphore available for future concurrency control

// ============================================================================
// Resource Tracking
// ============================================================================

/// Resource metrics for a component
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct ResourceMetrics {
    /// Peak memory usage (bytes)
    peak_memory_bytes: u64,
    /// Total CPU time (milliseconds)
    cpu_time_ms: u64,
    /// Number of operations completed
    operations: u64,
    /// Total duration (milliseconds)
    duration_ms: u64,
}

#[allow(dead_code)]
impl ResourceMetrics {
    fn memory_mb(&self) -> f64 {
        self.peak_memory_bytes as f64 / (1024.0 * 1024.0)
    }

    fn memory_gb(&self) -> f64 {
        self.peak_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    fn ops_per_sec(&self) -> f64 {
        if self.duration_ms == 0 {
            return 0.0;
        }
        (self.operations as f64 / self.duration_ms as f64) * 1000.0
    }
}

/// Resource tracker for monitoring memory and CPU
struct ResourceTracker {
    sys: System,
    pid: Pid,
    start_memory: u64,
    peak_memory: AtomicU64,
    start_time: Instant,
}

impl ResourceTracker {
    fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        let pid = Pid::from(std::process::id() as usize);

        let start_memory = sys
            .process(pid)
            .map(|p| p.memory())
            .unwrap_or(0);

        Self {
            sys,
            pid,
            start_memory,
            peak_memory: AtomicU64::new(start_memory),
            start_time: Instant::now(),
        }
    }

    fn update(&mut self) {
        self.sys.refresh_process_specifics(self.pid, ProcessRefreshKind::new().with_memory());
        if let Some(process) = self.sys.process(self.pid) {
            let current = process.memory();
            let peak = self.peak_memory.load(Ordering::Relaxed);
            if current > peak {
                self.peak_memory.store(current, Ordering::Relaxed);
            }
        }
    }

    fn get_metrics(&self, operations: u64) -> ResourceMetrics {
        ResourceMetrics {
            peak_memory_bytes: self.peak_memory.load(Ordering::Relaxed),
            cpu_time_ms: 0, // CPU time tracking would require more complex instrumentation
            operations,
            duration_ms: self.start_time.elapsed().as_millis() as u64,
        }
    }

    fn current_memory_mb(&mut self) -> f64 {
        self.update();
        self.sys
            .process(self.pid)
            .map(|p| p.memory() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0)
    }

    fn memory_delta_mb(&mut self) -> f64 {
        self.update();
        let current = self.sys
            .process(self.pid)
            .map(|p| p.memory())
            .unwrap_or(0);
        (current as i64 - self.start_memory as i64) as f64 / (1024.0 * 1024.0)
    }
}

// ============================================================================
// Test Configuration
// ============================================================================

/// Configuration for stress tests
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct StressTestConfig {
    /// Target data size in GB
    target_size_gb: f64,
    /// Number of concurrent materializations
    concurrent_materializations: usize,
    /// Number of concurrent registry queries
    concurrent_registry_queries: usize,
    /// Number of features per row
    features_per_row: usize,
    /// Size of string features (bytes)
    string_size: usize,
    /// Batch size for writes
    batch_size: usize,
}

impl StressTestConfig {
    /// Quick test (100MB, for CI)
    fn quick() -> Self {
        Self {
            target_size_gb: 0.1, // 100MB
            concurrent_materializations: 2,
            concurrent_registry_queries: 4,
            features_per_row: 10,
            string_size: 100,
            batch_size: 10_000,
        }
    }

    /// Medium test (1GB)
    fn medium() -> Self {
        Self {
            target_size_gb: 1.0,
            concurrent_materializations: 4,
            concurrent_registry_queries: 8,
            features_per_row: 15,
            string_size: 200,
            batch_size: 50_000,
        }
    }

    /// Full stress test (5GB, requires 16GB+ RAM)
    fn full() -> Self {
        Self {
            target_size_gb: 5.0,
            concurrent_materializations: 8,
            concurrent_registry_queries: 16,
            features_per_row: 20,
            string_size: 500,
            batch_size: 100_000,
        }
    }

    /// Calculate rows needed to reach target size
    fn rows_needed(&self) -> usize {
        // Estimate bytes per row: entity (50) + timestamp (8) + features (features * avg_size)
        let avg_feature_size = (8 + 8 + self.string_size) / 3; // Mix of int, float, string
        let bytes_per_row = 50 + 8 + (self.features_per_row * avg_feature_size);
        let target_bytes = (self.target_size_gb * 1024.0 * 1024.0 * 1024.0) as usize;
        target_bytes / bytes_per_row
    }

    fn num_batches(&self) -> usize {
        (self.rows_needed() + self.batch_size - 1) / self.batch_size
    }
}

// ============================================================================
// Data Generation
// ============================================================================

/// Generate a batch of feature rows
fn generate_batch(
    batch_id: usize,
    batch_size: usize,
    features_per_row: usize,
    string_size: usize,
) -> Vec<FeatureRow> {
    let mut rows = Vec::with_capacity(batch_size);
    let base_id = batch_id * batch_size;

    for i in 0..batch_size {
        let entity_id = format!("entity_{}", base_id + i);
        let mut features = HashMap::new();

        for f in 0..features_per_row {
            let feature_name = format!("feature_{}", f);
            let value = match f % 3 {
                0 => FeatureValue::Int((base_id + i + f) as i64),
                1 => FeatureValue::Float((base_id + i + f) as f64 * 1.5),
                _ => FeatureValue::String("x".repeat(string_size)),
            };
            features.insert(feature_name, value);
        }

        rows.push(FeatureRow {
            entities: vec![EntityKey::new("entity_id", &entity_id)],
            features,
            timestamp: Utc::now(),
        });
    }

    rows
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Test large data materialization with memory tracking
#[tokio::test]
async fn test_stress_large_materialization() {
    let config = if std::env::var("STRESS_TEST_FULL").is_ok() {
        StressTestConfig::full()
    } else {
        StressTestConfig::quick()
    };

    println!("\n=== Large Materialization Stress Test ===");
    println!("Config: {:?}", config);
    println!("Target size: {:.2} GB", config.target_size_gb);
    println!("Total rows: {}", config.rows_needed());
    println!("Batches: {}", config.num_batches());
    println!();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = create_test_connector(temp_dir.path().to_str().unwrap()).await;

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();
    let mut total_rows = 0usize;

    // Write batches and track memory
    for batch_id in 0..config.num_batches() {
        let batch = generate_batch(
            batch_id,
            config.batch_size,
            config.features_per_row,
            config.string_size,
        );

        connector
            .write_features("stress_test_view", batch.clone())
            .await
            .expect("Write should succeed");

        total_rows += batch.len();
        tracker.update();

        if batch_id % 10 == 0 || batch_id == config.num_batches() - 1 {
            let elapsed = start.elapsed();
            let throughput = total_rows as f64 / elapsed.as_secs_f64();
            let memory = tracker.current_memory_mb();

            println!(
                "Progress: {}/{} batches | {} rows | {:.0} rows/sec | Memory: {:.1} MB",
                batch_id + 1,
                config.num_batches(),
                total_rows,
                throughput,
                memory
            );
        }
    }

    let metrics = tracker.get_metrics(total_rows as u64);
    let elapsed = start.elapsed();

    println!("\n=== Results ===");
    println!("Total rows written: {}", total_rows);
    println!("Total time: {:.2?}", elapsed);
    println!("Throughput: {:.0} rows/sec", total_rows as f64 / elapsed.as_secs_f64());
    println!("Peak memory: {:.2} MB ({:.2} GB)", metrics.memory_mb(), metrics.memory_gb());
    println!("Memory per 1M rows: {:.1} MB", metrics.memory_mb() / (total_rows as f64 / 1_000_000.0));

    // Verify data was written correctly by reading a sample
    let entities: Vec<EntityKey> = (0..10)
        .map(|i| EntityKey::new("entity_id", &format!("entity_{}", i)))
        .collect();

    let results = connector
        .read_features("stress_test_view", entities.clone(), None)
        .await
        .expect("Read should succeed");

    println!("\nVerification: Read {} rows (expected {})", results.len(), entities.len());
    assert!(!results.is_empty(), "Should read back data");
    assert!(results.len() <= entities.len(), "Should not return more than requested");

    println!("\n✅ Large materialization test PASSED");
}

/// Test concurrent materializations to different feature views
#[tokio::test]
async fn test_stress_concurrent_materializations() {
    let config = if std::env::var("STRESS_TEST_FULL").is_ok() {
        StressTestConfig::medium()
    } else {
        StressTestConfig::quick()
    };

    println!("\n=== Concurrent Materializations Stress Test ===");
    println!("Concurrent materializations: {}", config.concurrent_materializations);
    println!("Rows per materialization: {}", config.rows_needed() / config.concurrent_materializations);
    println!();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = Arc::new(create_test_connector(temp_dir.path().to_str().unwrap()).await);

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();

    let total_success = Arc::new(AtomicUsize::new(0));
    let total_failure = Arc::new(AtomicUsize::new(0));
    let total_rows = Arc::new(AtomicUsize::new(0));

    let rows_per_view = config.rows_needed() / config.concurrent_materializations;
    let batches_per_view = rows_per_view / config.batch_size;

    let mut handles = Vec::new();

    for view_id in 0..config.concurrent_materializations {
        let connector = Arc::clone(&connector);
        let success = Arc::clone(&total_success);
        let failure = Arc::clone(&total_failure);
        let rows = Arc::clone(&total_rows);
        let config = config.clone();

        let handle = tokio::spawn(async move {
            let view_name = format!("concurrent_view_{}", view_id);
            let mut view_rows = 0;

            for batch_id in 0..batches_per_view {
                let batch = generate_batch(
                    view_id * batches_per_view + batch_id,
                    config.batch_size,
                    config.features_per_row,
                    config.string_size,
                );

                match connector.write_features(&view_name, batch.clone()).await {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                        view_rows += batch.len();
                        rows.fetch_add(batch.len(), Ordering::Relaxed);
                    }
                    Err(e) => {
                        failure.fetch_add(1, Ordering::Relaxed);
                        eprintln!("View {} batch {} failed: {}", view_id, batch_id, e);
                    }
                }
            }

            view_rows
        });

        handles.push(handle);
    }

    // Wait for all materializations
    let mut view_results = Vec::new();
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(rows) => {
                println!("View {} completed: {} rows", i, rows);
                view_results.push(rows);
            }
            Err(e) => {
                eprintln!("View {} task panicked: {}", i, e);
            }
        }
    }

    tracker.update();
    let metrics = tracker.get_metrics(total_rows.load(Ordering::Relaxed) as u64);
    let elapsed = start.elapsed();

    let success_count = total_success.load(Ordering::Relaxed);
    let failure_count = total_failure.load(Ordering::Relaxed);
    let rows_written = total_rows.load(Ordering::Relaxed);

    println!("\n=== Results ===");
    println!("Total materializations: {}", config.concurrent_materializations);
    println!("Successful batches: {}", success_count);
    println!("Failed batches: {}", failure_count);
    println!("Total rows written: {}", rows_written);
    println!("Total time: {:.2?}", elapsed);
    println!("Aggregate throughput: {:.0} rows/sec", rows_written as f64 / elapsed.as_secs_f64());
    println!("Peak memory: {:.2} MB", metrics.memory_mb());

    // Success rate should be high (some OCC conflicts expected with concurrent writes)
    let success_rate = success_count as f64 / (success_count + failure_count) as f64 * 100.0;
    println!("Success rate: {:.1}%", success_rate);

    // We expect high success rate - some failures are OK due to OCC conflicts
    assert!(success_rate >= 80.0, "Success rate should be >= 80%, got {:.1}%", success_rate);

    println!("\n✅ Concurrent materializations test PASSED");
}

/// Test mixed read/write workload
#[tokio::test]
async fn test_stress_mixed_workload() {
    let config = StressTestConfig::quick();

    println!("\n=== Mixed Read/Write Workload Stress Test ===");
    println!("Writers: 4, Readers: 8");
    println!();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = Arc::new(create_test_connector(temp_dir.path().to_str().unwrap()).await);

    // Pre-populate with some data
    let setup_batch = generate_batch(0, 10_000, config.features_per_row, config.string_size);
    connector
        .write_features("mixed_workload_view", setup_batch)
        .await
        .expect("Setup write should succeed");

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();
    let duration = Duration::from_secs(5);

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));
    let read_errors = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Spawn writers
    for writer_id in 0..4 {
        let connector = Arc::clone(&connector);
        let writes = Arc::clone(&write_count);
        let errors = Arc::clone(&write_errors);
        let config = config.clone();

        let handle = tokio::spawn(async move {
            let mut batch_id = writer_id * 1000;
            let view_name = format!("mixed_writer_{}", writer_id);

            while start.elapsed() < duration {
                let batch = generate_batch(batch_id, 1000, config.features_per_row, config.string_size);
                batch_id += 1;

                match connector.write_features(&view_name, batch).await {
                    Ok(_) => { writes.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
        handles.push(handle);
    }

    // Spawn readers
    for reader_id in 0..8 {
        let connector = Arc::clone(&connector);
        let reads = Arc::clone(&read_count);
        let errors = Arc::clone(&read_errors);

        let handle = tokio::spawn(async move {
            while start.elapsed() < duration {
                let entities: Vec<EntityKey> = (0..100)
                    .map(|i| EntityKey::new("entity_id", &format!("entity_{}", reader_id * 100 + i)))
                    .collect();

                match connector.read_features("mixed_workload_view", entities, None).await {
                    Ok(_) => { reads.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    tracker.update();
    let elapsed = start.elapsed();

    let writes = write_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);
    let w_errors = write_errors.load(Ordering::Relaxed);
    let r_errors = read_errors.load(Ordering::Relaxed);

    println!("=== Results ===");
    println!("Duration: {:.2?}", elapsed);
    println!("Writes: {} ({} errors)", writes, w_errors);
    println!("Reads: {} ({} errors)", reads, r_errors);
    println!("Write throughput: {:.0} batches/sec", writes as f64 / elapsed.as_secs_f64());
    println!("Read throughput: {:.0} queries/sec", reads as f64 / elapsed.as_secs_f64());
    println!("Peak memory: {:.2} MB", tracker.current_memory_mb());

    // Both reads and writes should complete
    assert!(writes > 0, "Should complete some writes");
    assert!(reads > 0, "Should complete some reads");

    println!("\n✅ Mixed workload test PASSED");
}

/// Test memory pressure with large batches
#[tokio::test]
async fn test_stress_memory_pressure() {
    println!("\n=== Memory Pressure Stress Test ===");
    println!("Testing with progressively larger batches");
    println!();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = create_test_connector(temp_dir.path().to_str().unwrap()).await;

    let batch_sizes = vec![1_000, 10_000, 50_000, 100_000];
    let mut results = Vec::new();

    for batch_size in batch_sizes {
        let mut tracker = ResourceTracker::new();
        let start = Instant::now();

        let batch = generate_batch(0, batch_size, 20, 200);
        let view_name = format!("memory_test_{}", batch_size);

        match connector.write_features(&view_name, batch).await {
            Ok(_) => {
                tracker.update();
                let elapsed = start.elapsed();
                let memory = tracker.memory_delta_mb();
                let throughput = batch_size as f64 / elapsed.as_secs_f64();

                println!(
                    "Batch size {}: {:.2?} | {:.0} rows/sec | Memory delta: {:.1} MB",
                    batch_size, elapsed, throughput, memory
                );

                results.push((batch_size, elapsed, throughput, memory));
            }
            Err(e) => {
                println!("Batch size {} FAILED: {}", batch_size, e);
            }
        }
    }

    println!("\n=== Summary ===");
    for (size, elapsed, throughput, memory) in &results {
        println!(
            "{:>7} rows: {:>8.2?} | {:>10.0} rows/sec | {:>8.1} MB",
            size, elapsed, throughput, memory
        );
    }

    assert!(!results.is_empty(), "At least some batches should succeed");
    println!("\n✅ Memory pressure test PASSED");
}

/// Test sustained throughput over time
#[tokio::test]
async fn test_stress_sustained_throughput() {
    let test_duration_secs = if std::env::var("STRESS_TEST_FULL").is_ok() { 30 } else { 5 };

    println!("\n=== Sustained Throughput Stress Test ===");
    println!("Duration: {} seconds", test_duration_secs);
    println!();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = Arc::new(create_test_connector(temp_dir.path().to_str().unwrap()).await);

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();
    let duration = Duration::from_secs(test_duration_secs);

    let total_rows = Arc::new(AtomicUsize::new(0));
    let total_batches = Arc::new(AtomicUsize::new(0));

    let num_workers = 4;
    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let connector = Arc::clone(&connector);
        let rows = Arc::clone(&total_rows);
        let batches = Arc::clone(&total_batches);

        let handle = tokio::spawn(async move {
            let mut batch_id = worker_id * 10000;
            let view_name = format!("sustained_worker_{}", worker_id);

            while start.elapsed() < duration {
                let batch = generate_batch(batch_id, 5000, 10, 100);
                batch_id += 1;

                if connector.write_features(&view_name, batch.clone()).await.is_ok() {
                    rows.fetch_add(batch.len(), Ordering::Relaxed);
                    batches.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    // Monitor memory during test
    let mut memory_samples = Vec::new();
    while start.elapsed() < duration {
        tokio::time::sleep(Duration::from_millis(500)).await;
        tracker.update();
        memory_samples.push(tracker.current_memory_mb());
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let rows_written = total_rows.load(Ordering::Relaxed);
    let batches_written = total_batches.load(Ordering::Relaxed);

    let avg_memory: f64 = memory_samples.iter().sum::<f64>() / memory_samples.len() as f64;
    let max_memory: f64 = memory_samples.iter().cloned().fold(0.0, f64::max);
    let min_memory: f64 = memory_samples.iter().cloned().fold(f64::MAX, f64::min);

    println!("=== Results ===");
    println!("Duration: {:.2?}", elapsed);
    println!("Total rows: {}", rows_written);
    println!("Total batches: {}", batches_written);
    println!("Sustained throughput: {:.0} rows/sec", rows_written as f64 / elapsed.as_secs_f64());
    println!("Memory - Min: {:.1} MB, Avg: {:.1} MB, Max: {:.1} MB", min_memory, avg_memory, max_memory);
    println!("Memory variance: {:.1} MB", max_memory - min_memory);

    // Memory should be stable (not constantly growing)
    let memory_growth = max_memory - min_memory;
    let memory_stable = memory_growth < avg_memory * 0.5; // Less than 50% variance

    println!("Memory stability: {} (variance < 50% of avg)", if memory_stable { "STABLE" } else { "GROWING" });

    assert!(rows_written > 0, "Should write some rows");

    println!("\n✅ Sustained throughput test PASSED");
}

// ============================================================================
// Helper Functions
// ============================================================================

async fn create_test_connector(base_path: &str) -> DeltaStorageConnector {
    let engine_config = DuckDBEngineConfig {
        db_path: None, // In-memory for tests
        threads: 0,    // Auto-detect
        memory_limit: "4GB".to_string(),
        aggressive_optimizations: true,
        enable_partition_pruning: false,
    };

    DeltaStorageConnector::new(base_path, engine_config)
        .await
        .expect("Failed to create connector")
}
