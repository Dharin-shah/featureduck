//! Registry Stress Tests
//!
//! Tests registry under concurrent load:
//! - Concurrent feature view registration
//! - Concurrent run tracking
//! - Registry reads during writes
//! - Stats updates under load
//!
//! Run with: cargo test -p featureduck-registry --test stress_test_registry --release -- --nocapture

use chrono::Utc;
use featureduck_registry::{FeatureRegistry, FeatureViewDef, RegistryConfig, RunStatus};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, System};
use tempfile::TempDir;

// ============================================================================
// Resource Tracking
// ============================================================================

struct ResourceTracker {
    sys: System,
    pid: Pid,
    start_memory: u64,
}

impl ResourceTracker {
    fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        let pid = Pid::from(std::process::id() as usize);
        let start_memory = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

        Self { sys, pid, start_memory }
    }

    fn current_memory_mb(&mut self) -> f64 {
        self.sys.refresh_process_specifics(self.pid, ProcessRefreshKind::new().with_memory());
        self.sys
            .process(self.pid)
            .map(|p| p.memory() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0)
    }

    fn memory_delta_mb(&mut self) -> f64 {
        self.sys.refresh_process_specifics(self.pid, ProcessRefreshKind::new().with_memory());
        let current = self.sys.process(self.pid).map(|p| p.memory()).unwrap_or(0);
        (current as i64 - self.start_memory as i64) as f64 / (1024.0 * 1024.0)
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Test concurrent feature view registration
#[tokio::test]
async fn test_registry_concurrent_registration() {
    println!("\n=== Registry Concurrent Registration Test ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("registry.db");

    let registry = Arc::new(
        FeatureRegistry::new(RegistryConfig::SQLite {
            path: db_path.to_str().unwrap().to_string(),
        })
        .await
        .expect("Failed to create registry"),
    );

    let num_workers = 8;
    let views_per_worker = 50;
    let total_views = num_workers * views_per_worker;

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();

    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let registry = Arc::clone(&registry);
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);

        let handle = tokio::spawn(async move {
            for view_id in 0..views_per_worker {
                let view_name = format!("worker_{}_view_{}", worker_id, view_id);
                let now = Utc::now();
                let view = FeatureViewDef {
                    name: view_name,
                    version: 1,
                    source_type: "delta".to_string(),
                    source_path: format!("s3://bucket/worker_{}/view_{}", worker_id, view_id),
                    entities: vec!["user_id".to_string()],
                    transformations: "{}".to_string(),
                    timestamp_field: Some("ts".to_string()),
                    ttl_days: Some(7),
                    batch_schedule: None,
                    description: Some(format!("Test view {} from worker {}", view_id, worker_id)),
                    tags: vec!["stress-test".to_string()],
                    owner: Some(format!("worker_{}", worker_id)),
                    created_at: now,
                    updated_at: now,
                };

                match registry.register_feature_view(&view).await {
                    Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Registration error: {}", e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    let elapsed = start.elapsed();
    let successes = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    println!("Duration: {:.2?}", elapsed);
    println!("Registrations: {} success, {} errors", successes, errors);
    println!("Throughput: {:.0} registrations/sec", successes as f64 / elapsed.as_secs_f64());
    println!("Memory delta: {:.1} MB", tracker.memory_delta_mb());

    // Verify views exist
    let all_views = registry.list_feature_views(None).await.expect("List should work");
    println!("Total views in registry: {}", all_views.len());

    assert_eq!(successes, total_views, "All registrations should succeed");
    assert_eq!(all_views.len(), total_views, "Should have all views");

    println!("\n✅ Concurrent registration test PASSED");
}

/// Test concurrent run tracking
#[tokio::test]
async fn test_registry_concurrent_run_tracking() {
    println!("\n=== Registry Concurrent Run Tracking Test ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("registry.db");

    let registry = Arc::new(
        FeatureRegistry::new(RegistryConfig::SQLite {
            path: db_path.to_str().unwrap().to_string(),
        })
        .await
        .expect("Failed to create registry"),
    );

    // Create some feature views first
    let num_views = 10;
    for i in 0..num_views {
        let now = Utc::now();
        let view = FeatureViewDef {
            name: format!("run_test_view_{}", i),
            version: 1,
            source_type: "delta".to_string(),
            source_path: format!("s3://bucket/view_{}", i),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: Some("ts".to_string()),
            ttl_days: Some(7),
            batch_schedule: None,
            description: None,
            tags: vec![],
            owner: None,
            created_at: now,
            updated_at: now,
        };
        registry.register_feature_view(&view).await.expect("Registration should work");
    }

    let num_workers = 8;
    let runs_per_worker = 100;
    let _total_runs = num_workers * runs_per_worker;

    let mut tracker = ResourceTracker::new();
    let start = Instant::now();

    let run_created = Arc::new(AtomicUsize::new(0));
    let run_completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _worker_id in 0..num_workers {
        let registry = Arc::clone(&registry);
        let created = Arc::clone(&run_created);
        let completed = Arc::clone(&run_completed);

        let handle = tokio::spawn(async move {
            for run_id in 0..runs_per_worker {
                let view_name = format!("run_test_view_{}", run_id % num_views);

                // Create run
                match registry.create_run(&view_name).await {
                    Ok(id) => {
                        created.fetch_add(1, Ordering::Relaxed);

                        // Simulate some work
                        tokio::time::sleep(Duration::from_micros(100)).await;

                        // Complete run
                        let status = if run_id % 10 == 0 {
                            RunStatus::Failed
                        } else {
                            RunStatus::Success
                        };

                        if registry
                            .update_run_status(id, status, Some(1000), None)
                            .await
                            .is_ok()
                        {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("Create run error: {}", e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    let elapsed = start.elapsed();
    let created_count = run_created.load(Ordering::Relaxed);
    let completed_count = run_completed.load(Ordering::Relaxed);

    println!("Duration: {:.2?}", elapsed);
    println!("Runs created: {}", created_count);
    println!("Runs completed: {}", completed_count);
    println!("Throughput: {:.0} run cycles/sec", created_count as f64 / elapsed.as_secs_f64());
    println!("Memory delta: {:.1} MB", tracker.memory_delta_mb());

    // Verify runs exist
    let recent_runs = registry
        .get_recent_runs("run_test_view_0", 100)
        .await
        .expect("Get runs should work");
    println!("Recent runs for view 0: {}", recent_runs.len());

    assert!(created_count > 0, "Should create some runs");
    assert!(completed_count > 0, "Should complete some runs");

    println!("\n✅ Concurrent run tracking test PASSED");
}

/// Test mixed read/write registry operations
#[tokio::test]
async fn test_registry_mixed_operations() {
    println!("\n=== Registry Mixed Operations Test ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("registry.db");

    let registry = Arc::new(
        FeatureRegistry::new(RegistryConfig::SQLite {
            path: db_path.to_str().unwrap().to_string(),
        })
        .await
        .expect("Failed to create registry"),
    );

    // Pre-populate
    for i in 0..20 {
        let now = Utc::now();
        let view = FeatureViewDef {
            name: format!("mixed_view_{}", i),
            version: 1,
            source_type: "delta".to_string(),
            source_path: format!("s3://bucket/mixed_{}", i),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: Some("ts".to_string()),
            ttl_days: Some(7),
            batch_schedule: None,
            description: None,
            tags: vec!["mixed-test".to_string()],
            owner: None,
            created_at: now,
            updated_at: now,
        };
        registry.register_feature_view(&view).await.expect("Setup failed");
    }

    let test_duration = Duration::from_secs(5);
    let start = Instant::now();

    let writes = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let list_ops = Arc::new(AtomicUsize::new(0));
    let run_ops = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Writers (registering new views)
    for worker_id in 0..2 {
        let registry = Arc::clone(&registry);
        let writes = Arc::clone(&writes);

        let handle = tokio::spawn(async move {
            let mut view_id = worker_id * 10000;
            while start.elapsed() < test_duration {
                let now = Utc::now();
                let view = FeatureViewDef {
                    name: format!("dynamic_view_{}_{}", worker_id, view_id),
                    version: 1,
                    source_type: "delta".to_string(),
                    source_path: format!("s3://bucket/dynamic_{}_{}", worker_id, view_id),
                    entities: vec!["user_id".to_string()],
                    transformations: "{}".to_string(),
                    timestamp_field: Some("ts".to_string()),
                    ttl_days: Some(7),
                    batch_schedule: None,
                    description: None,
                    tags: vec![],
                    owner: None,
                    created_at: now,
                    updated_at: now,
                };
                view_id += 1;

                if registry.register_feature_view(&view).await.is_ok() {
                    writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    // Readers (getting specific views)
    for _ in 0..4 {
        let registry = Arc::clone(&registry);
        let reads = Arc::clone(&reads);

        let handle = tokio::spawn(async move {
            let mut i = 0;
            while start.elapsed() < test_duration {
                let view_name = format!("mixed_view_{}", i % 20);
                if registry.get_feature_view(&view_name).await.is_ok() {
                    reads.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        });
        handles.push(handle);
    }

    // Listers (listing all views)
    for _ in 0..2 {
        let registry = Arc::clone(&registry);
        let list_ops = Arc::clone(&list_ops);

        let handle = tokio::spawn(async move {
            while start.elapsed() < test_duration {
                if registry.list_feature_views(None).await.is_ok() {
                    list_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    // Run trackers
    for worker_id in 0..2 {
        let registry = Arc::clone(&registry);
        let run_ops = Arc::clone(&run_ops);

        let handle = tokio::spawn(async move {
            while start.elapsed() < test_duration {
                let view_name = format!("mixed_view_{}", worker_id % 20);
                if let Ok(run_id) = registry.create_run(&view_name).await {
                    let _ = registry
                        .update_run_status(run_id, RunStatus::Success, Some(100), None)
                        .await;
                    run_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_writes = writes.load(Ordering::Relaxed);
    let total_reads = reads.load(Ordering::Relaxed);
    let total_lists = list_ops.load(Ordering::Relaxed);
    let total_runs = run_ops.load(Ordering::Relaxed);
    let total_ops = total_writes + total_reads + total_lists + total_runs;

    println!("Duration: {:.2?}", elapsed);
    println!("Writes: {} ({:.0}/sec)", total_writes, total_writes as f64 / elapsed.as_secs_f64());
    println!("Reads: {} ({:.0}/sec)", total_reads, total_reads as f64 / elapsed.as_secs_f64());
    println!("Lists: {} ({:.0}/sec)", total_lists, total_lists as f64 / elapsed.as_secs_f64());
    println!("Run ops: {} ({:.0}/sec)", total_runs, total_runs as f64 / elapsed.as_secs_f64());
    println!("Total throughput: {:.0} ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    assert!(total_ops > 100, "Should complete significant operations");

    println!("\n✅ Mixed operations test PASSED");
}

/// Test registry with sustained load
#[tokio::test]
async fn test_registry_sustained_load() {
    println!("\n=== Registry Sustained Load Test ===");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("registry.db");

    let registry = Arc::new(
        FeatureRegistry::new(RegistryConfig::SQLite {
            path: db_path.to_str().unwrap().to_string(),
        })
        .await
        .expect("Failed to create registry"),
    );

    // Pre-populate
    for i in 0..100 {
        let now = Utc::now();
        let view = FeatureViewDef {
            name: format!("sustained_view_{}", i),
            version: 1,
            source_type: "delta".to_string(),
            source_path: format!("s3://bucket/sustained_{}", i),
            entities: vec!["user_id".to_string()],
            transformations: "{}".to_string(),
            timestamp_field: Some("ts".to_string()),
            ttl_days: Some(7),
            batch_schedule: None,
            description: None,
            tags: vec![],
            owner: None,
            created_at: now,
            updated_at: now,
        };
        registry.register_feature_view(&view).await.expect("Setup failed");
    }

    let test_duration = Duration::from_secs(10);
    let start = Instant::now();

    let mut tracker = ResourceTracker::new();
    let ops_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Multiple workers doing read/list operations
    for _worker_id in 0..8 {
        let registry = Arc::clone(&registry);
        let ops = Arc::clone(&ops_count);

        let handle = tokio::spawn(async move {
            let mut i = 0;
            while start.elapsed() < test_duration {
                let view_name = format!("sustained_view_{}", i % 100);

                // Mix of operations
                match i % 3 {
                    0 => {
                        let _ = registry.get_feature_view(&view_name).await;
                    }
                    1 => {
                        let _ = registry.list_feature_views(Some("sustained")).await;
                    }
                    _ => {
                        let _ = registry.get_recent_runs(&view_name, 10).await;
                    }
                }

                ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        });
        handles.push(handle);
    }

    // Monitor memory
    let mut memory_samples = Vec::new();
    while start.elapsed() < test_duration {
        tokio::time::sleep(Duration::from_millis(500)).await;
        memory_samples.push(tracker.current_memory_mb());
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_ops = ops_count.load(Ordering::Relaxed);

    let avg_memory: f64 = memory_samples.iter().sum::<f64>() / memory_samples.len() as f64;
    let max_memory: f64 = memory_samples.iter().cloned().fold(0.0, f64::max);
    let min_memory: f64 = memory_samples.iter().cloned().fold(f64::MAX, f64::min);

    println!("Duration: {:.2?}", elapsed);
    println!("Total operations: {}", total_ops);
    println!("Throughput: {:.0} ops/sec", total_ops as f64 / elapsed.as_secs_f64());
    println!("Memory - Min: {:.1} MB, Avg: {:.1} MB, Max: {:.1} MB", min_memory, avg_memory, max_memory);

    let memory_stable = (max_memory - min_memory) < avg_memory * 0.3;
    println!("Memory stability: {}", if memory_stable { "STABLE" } else { "GROWING" });

    assert!(total_ops > 1000, "Should complete many operations");

    println!("\n✅ Sustained load test PASSED");
}
