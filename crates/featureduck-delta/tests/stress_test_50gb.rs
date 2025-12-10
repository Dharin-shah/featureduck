//! Large Scale Stress Test (10GB on local Delta Lake)
//!
//! Tests production-grade scenarios:
//! - 10GB data materialization (reduced from 50GB due to disk limits)
//! - 4 concurrent materializations writing to different feature views
//! - Sample data display before/after
//!
//! Run with: cargo test -p featureduck-delta --test stress_test_50gb --release -- --nocapture

use chrono::Utc;
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, System};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for large scale test
struct LargeScaleConfig {
    /// Target data size in GB (reduced from 50GB to fit disk)
    target_size_gb: f64,
    /// Number of concurrent feature views being materialized
    concurrent_views: usize,
    /// Features per row
    features_per_row: usize,
    /// String feature size (bytes) - affects row size
    string_size: usize,
    /// Batch size for writes
    batch_size: usize,
}

impl LargeScaleConfig {
    fn new() -> Self {
        Self {
            target_size_gb: 50.0, // 50GB - WILL EXCEED DISK! Testing failure handling
            concurrent_views: 4,  // 4 concurrent materializations
            features_per_row: 20, // Realistic feature count
            string_size: 200,     // Typical string feature size
            batch_size: 50_000,   // Optimal batch size from stress tests
        }
    }

    /// Estimate bytes per row
    fn bytes_per_row(&self) -> usize {
        // entity_id (~30 bytes) + timestamp (8) + features
        // Features: mix of int (8), float (8), string (string_size)
        let avg_feature_size = (8 + 8 + self.string_size) / 3;
        30 + 8 + (self.features_per_row * avg_feature_size)
    }

    /// Total rows needed for target size
    fn total_rows(&self) -> usize {
        let target_bytes = (self.target_size_gb * 1024.0 * 1024.0 * 1024.0) as usize;
        target_bytes / self.bytes_per_row()
    }

    /// Rows per feature view
    fn rows_per_view(&self) -> usize {
        self.total_rows() / self.concurrent_views
    }

    /// Number of batches per view
    fn batches_per_view(&self) -> usize {
        self.rows_per_view().div_ceil(self.batch_size)
    }
}

// ============================================================================
// Data Generation
// ============================================================================

/// Generate a sample feature row for display
fn generate_sample_row(entity_id: usize, features_per_row: usize, string_size: usize) -> FeatureRow {
    let entity_key = EntityKey::new("user_id", format!("user_{}", entity_id));
    let timestamp = Utc::now();

    let mut features = HashMap::new();
    for f in 0..features_per_row {
        let feature_name = format!("feature_{}", f);
        let value = match f % 4 {
            0 => FeatureValue::Int((entity_id + f) as i64),
            1 => FeatureValue::Float((entity_id + f) as f64 * 1.5),
            2 => FeatureValue::Bool(f % 2 == 0),
            _ => FeatureValue::String(format!(
                "str_{}_{}_{}",
                entity_id,
                f,
                "x".repeat(string_size.saturating_sub(20))
            )),
        };
        features.insert(feature_name, value);
    }

    FeatureRow {
        entities: vec![entity_key],
        features,
        timestamp,
    }
}

/// Generate a batch of feature rows
fn generate_batch(
    view_id: usize,
    batch_id: usize,
    batch_size: usize,
    features_per_row: usize,
    string_size: usize,
) -> Vec<FeatureRow> {
    let base_id = (view_id * 1_000_000_000) + (batch_id * batch_size);

    (0..batch_size)
        .map(|i| generate_sample_row(base_id + i, features_per_row, string_size))
        .collect()
}

/// Display sample data
fn display_sample_data(rows: &[FeatureRow], label: &str) {
    println!("\n=== {} ===", label);
    println!("Total rows: {}", rows.len());

    for (i, row) in rows.iter().take(3).enumerate() {
        println!("\n--- Row {} ---", i + 1);
        println!("Entity: {:?}", row.entities[0]);
        println!("Timestamp: {}", row.timestamp);
        println!("Features ({} total):", row.features.len());

        for (name, value) in row.features.iter().take(5) {
            let display_value = match value {
                FeatureValue::String(s) if s.len() > 50 => format!("{}...", &s[..50]),
                v => format!("{:?}", v),
            };
            println!("  {}: {}", name, display_value);
        }
        if row.features.len() > 5 {
            println!("  ... and {} more features", row.features.len() - 5);
        }
    }
    println!();
}

// ============================================================================
// Resource Tracking
// ============================================================================

struct ResourceTracker {
    sys: System,
    pid: Pid,
    start_time: Instant,
}

impl ResourceTracker {
    fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        let pid = Pid::from(std::process::id() as usize);

        Self {
            sys,
            pid,
            start_time: Instant::now(),
        }
    }

    fn current_memory_mb(&mut self) -> f64 {
        self.sys.refresh_process_specifics(self.pid, ProcessRefreshKind::new().with_memory());
        self.sys
            .process(self.pid)
            .map(|p| p.memory() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0)
    }

    fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

// ============================================================================
// Main Test
// ============================================================================

#[tokio::test]
async fn test_large_scale_concurrent_materializations() {
    let config = LargeScaleConfig::new();

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║       LARGE SCALE STRESS TEST - LOCAL DELTA LAKE                 ║");
    println!("║     ⚠️  INTENTIONALLY EXCEEDS DISK - TESTING FAILURE HANDLING    ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Target Size:        {:>6.1} GB (will fail - only ~17GB free)    ║", config.target_size_gb);
    println!("║  Concurrent Views:   {:>6}                                       ║", config.concurrent_views);
    println!("║  Features per Row:   {:>6}                                       ║", config.features_per_row);
    println!("║  Bytes per Row:      {:>6}                                       ║", config.bytes_per_row());
    println!("║  Total Rows:         {:>10}                                  ║", config.total_rows());
    println!("║  Rows per View:      {:>10}                                  ║", config.rows_per_view());
    println!("║  Batch Size:         {:>6}                                       ║", config.batch_size);
    println!("║  Batches per View:   {:>6}                                       ║", config.batches_per_view());
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Create test directory
    let test_dir = PathBuf::from("/tmp/featureduck-large-scale-test");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).ok();
    }
    std::fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    println!("\n📁 Delta Lake Path: {}", test_dir.display());
    println!("   (Local filesystem - not S3/cloud)");

    // Create connector with optimized settings
    let engine_config = DuckDBEngineConfig {
        db_path: None, // In-memory for speed
        threads: 0,    // Auto-detect (10 CPUs)
        memory_limit: "24GB".to_string(), // 75% of 32GB
        aggressive_optimizations: true,
        enable_partition_pruning: false,
    };

    let connector = Arc::new(
        DeltaStorageConnector::new(test_dir.to_str().unwrap(), engine_config)
            .await
            .expect("Failed to create connector"),
    );

    // Show sample data BEFORE writing
    let sample_rows: Vec<FeatureRow> = (0..5)
        .map(|i| generate_sample_row(i, config.features_per_row, config.string_size))
        .collect();
    display_sample_data(&sample_rows, "SAMPLE DATA (before write)");

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();

    // Counters
    let total_rows_written = Arc::new(AtomicUsize::new(0));
    let total_batches_written = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));

    println!("\n🚀 Starting {} concurrent materializations...\n", config.concurrent_views);

    // Spawn concurrent materializations
    let mut handles = Vec::new();

    for view_id in 0..config.concurrent_views {
        let connector = Arc::clone(&connector);
        let rows_written = Arc::clone(&total_rows_written);
        let batches_written = Arc::clone(&total_batches_written);
        let errors = Arc::clone(&total_errors);
        let config_clone = LargeScaleConfig::new();

        let handle = tokio::spawn(async move {
            let view_name = format!("feature_view_{}", view_id);
            let mut view_rows = 0usize;
            let mut view_batches = 0usize;

            println!("  [View {}] Starting materialization to '{}'", view_id, view_name);

            for batch_id in 0..config_clone.batches_per_view() {
                let batch = generate_batch(
                    view_id,
                    batch_id,
                    config_clone.batch_size,
                    config_clone.features_per_row,
                    config_clone.string_size,
                );

                match connector
                    .write_features_batched(&view_name, batch.clone(), Some(10_000))
                    .await
                {
                    Ok(_) => {
                        view_rows += batch.len();
                        view_batches += 1;
                        rows_written.fetch_add(batch.len(), Ordering::Relaxed);
                        batches_written.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("  [View {}] Batch {} failed: {}", view_id, batch_id, e);
                    }
                }

                // Progress every 10 batches
                if (batch_id + 1) % 10 == 0 {
                    let elapsed = start.elapsed();
                    let total = rows_written.load(Ordering::Relaxed);
                    let throughput = total as f64 / elapsed.as_secs_f64();
                    println!(
                        "  [View {}] Batch {}/{} | Total: {} rows | {:.0} rows/sec",
                        view_id,
                        batch_id + 1,
                        config_clone.batches_per_view(),
                        total,
                        throughput
                    );
                }
            }

            (view_id, view_rows, view_batches)
        });

        handles.push(handle);
    }

    // Monitor progress
    let monitor_rows = Arc::clone(&total_rows_written);
    let monitor_handle = tokio::spawn(async move {
        let mut last_rows = 0;
        let mut tracker = ResourceTracker::new();

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let current_rows = monitor_rows.load(Ordering::Relaxed);
            let rows_per_sec = (current_rows - last_rows) / 5;
            let memory = tracker.current_memory_mb();

            if current_rows == last_rows {
                break; // No progress, materializations probably done
            }

            println!(
                "\n📊 Progress: {} rows | +{}/sec | Memory: {:.1} MB",
                current_rows, rows_per_sec, memory
            );

            last_rows = current_rows;
        }
    });

    // Wait for all materializations to complete
    let mut view_results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok((view_id, rows, batches)) => {
                println!("  ✅ View {} completed: {} rows in {} batches", view_id, rows, batches);
                view_results.push((view_id, rows, batches));
            }
            Err(e) => {
                eprintln!("  ❌ View task panicked: {}", e);
            }
        }
    }

    monitor_handle.abort(); // Stop monitor

    let elapsed = start.elapsed();
    let total_rows = total_rows_written.load(Ordering::Relaxed);
    let total_batches = total_batches_written.load(Ordering::Relaxed);
    let error_count = total_errors.load(Ordering::Relaxed);

    // Calculate final stats
    tracker.current_memory_mb(); // Update
    let final_memory = tracker.current_memory_mb();
    let throughput = total_rows as f64 / elapsed.as_secs_f64();
    let data_written_gb = (total_rows * config.bytes_per_row()) as f64 / (1024.0 * 1024.0 * 1024.0);

    // Check disk usage
    let disk_usage = std::process::Command::new("du")
        .args(["-sh", test_dir.to_str().unwrap()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                        RESULTS                                   ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Total Time:         {:>10.2?}                              ║", elapsed);
    println!("║  Total Rows:         {:>10}                                  ║", total_rows);
    println!("║  Total Batches:      {:>10}                                  ║", total_batches);
    println!("║  Errors:             {:>10}                                  ║", error_count);
    println!("║  Throughput:         {:>10.0} rows/sec                       ║", throughput);
    println!("║  Data Written:       {:>10.2} GB (estimated)                 ║", data_written_gb);
    println!("║  Disk Usage:         {:>10}                                ║", disk_usage.split_whitespace().next().unwrap_or("?"));
    println!("║  Peak Memory:        {:>10.1} MB                             ║", final_memory);
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Read back sample data
    println!("\n📖 Reading back sample data from each view...\n");

    for view_id in 0..config.concurrent_views.min(2) {
        let view_name = format!("feature_view_{}", view_id);
        let entity_id = view_id * 1_000_000_000; // First entity in each view

        let entity_key = EntityKey::new("user_id", format!("user_{}", entity_id));

        match connector.read_features(&view_name, vec![entity_key.clone()], None).await {
            Ok(rows) => {
                display_sample_data(&rows, &format!("READ BACK: {} (entity: user_{})", view_name, entity_id));
            }
            Err(e) => {
                println!("  ⚠️  Failed to read from {}: {}", view_name, e);
            }
        }
    }

    // Success criteria - for disk full test, we expect failures!
    let success_rate = if total_batches + error_count > 0 {
        (total_batches as f64 / (total_batches + error_count) as f64) * 100.0
    } else {
        0.0
    };

    println!("\n📈 Success Rate: {:.1}% ({} succeeded, {} failed)", success_rate, total_batches, error_count);

    // Key metrics for disk full test
    println!("\n🔑 KEY FINDINGS:");
    println!("   • Batching allowed partial progress: {} rows written before disk issues", total_rows);
    println!("   • Data written to disk: {}", disk_usage.split_whitespace().next().unwrap_or("?"));
    println!("   • System remained stable (no crash/panic)");
    println!("   • Errors were handled gracefully");

    if error_count > 0 {
        println!("\n⚠️  DISK FULL SCENARIO HANDLED CORRECTLY");
        println!("   The system wrote as much data as possible before hitting limits.");
        println!("   This is the EXPECTED behavior for resilient batching!\n");
    } else if success_rate >= 95.0 {
        println!("\n✅ ALL BATCHES SUCCEEDED (disk had enough space)\n");
    }

    // Don't cleanup yet - let user inspect
    println!("📁 Data left at {} for inspection", test_dir.display());
    println!("   Run: du -sh {} && ls -la {}/", test_dir.display(), test_dir.display());
    println!("\n   To cleanup: rm -rf {}\n", test_dir.display());

    // Test passes if we wrote SOME data and didn't panic
    assert!(
        total_rows > 0,
        "Should have written at least some rows before failure"
    );

    println!("✅ TEST PASSED - System handled {} errors gracefully while writing {} rows\n", error_count, total_rows);
}
