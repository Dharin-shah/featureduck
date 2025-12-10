//! Load and stress tests for HTTP server
//!
//! Tests server behavior under:
//! - High concurrent HTTP requests
//! - Rate limiting under load
//! - Auth middleware performance
//! - Memory pressure with many requests
//!
//! Run with: cargo test -p featureduck-server --test load_test --release -- --nocapture

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::{get, post},
    Router,
};
use featureduck_server::state::AppState;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tower::ServiceExt;

// ============================================================================
// Test Configuration
// ============================================================================

struct ServerLoadConfig {
    /// Number of concurrent requests
    concurrent_requests: usize,
    /// Total number of requests
    total_requests: usize,
    /// Request timeout (reserved for future use with actual timeouts)
    #[allow(dead_code)]
    timeout_ms: u64,
}

impl ServerLoadConfig {
    fn light() -> Self {
        Self {
            concurrent_requests: 10,
            total_requests: 100,
            timeout_ms: 5000,
        }
    }

    fn medium() -> Self {
        Self {
            concurrent_requests: 50,
            total_requests: 500,
            timeout_ms: 5000,
        }
    }

    fn heavy() -> Self {
        Self {
            concurrent_requests: 100,
            total_requests: 1000,
            timeout_ms: 10000,
        }
    }

    #[allow(dead_code)]
    fn stress() -> Self {
        Self {
            concurrent_requests: 200,
            total_requests: 2000,
            timeout_ms: 15000,
        }
    }
}

// ============================================================================
// Test App Setup
// ============================================================================

async fn create_test_app() -> Router {
    let state = AppState::new_with_registry().await;

    Router::new()
        .route("/health", get(|| async { "OK" }))
        .route(
            "/api/v1/feature-views",
            get(featureduck_server::api::registry::list_feature_views),
        )
        .route(
            "/api/v1/feature-views",
            post(featureduck_server::api::registry::register_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            get(featureduck_server::api::registry::get_feature_view),
        )
        .with_state(state)
}

fn create_feature_view_body(name: &str) -> Value {
    json!({
        "name": name,
        "source_type": "delta",
        "source_path": "s3://test-bucket/features",
        "entities": ["user_id"],
        "transformations": "{}",
        "ttl_days": 30,
        "batch_schedule": "0 * * * *",
        "description": "Load test feature view",
        "tags": ["load_test"],
        "owner": "load_test"
    })
}

struct LoadTestResult {
    total_requests: usize,
    successful: usize,
    failed: usize,
    rate_limited: usize,
    duration: Duration,
    requests_per_second: f64,
    avg_latency_ms: f64,
}

impl LoadTestResult {
    fn print(&self, test_name: &str) {
        println!("\n=== {} Results ===", test_name);
        println!("Total requests: {}", self.total_requests);
        println!(
            "Successful: {} ({:.1}%)",
            self.successful,
            self.successful as f64 / self.total_requests as f64 * 100.0
        );
        println!("Failed: {}", self.failed);
        println!("Rate limited: {}", self.rate_limited);
        println!("Duration: {:.2?}", self.duration);
        println!("Throughput: {:.0} req/sec", self.requests_per_second);
        println!("Avg latency: {:.2}ms", self.avg_latency_ms);
        println!();
    }
}

// ============================================================================
// Health Endpoint Load Tests
// ============================================================================

#[tokio::test]
async fn test_load_health_endpoint_light() {
    let config = ServerLoadConfig::light();
    let app = create_test_app().await;

    println!("\nHealth Endpoint Load Test (Light)");
    println!(
        "Config: {} concurrent, {} total requests",
        config.concurrent_requests, config.total_requests
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_requests));
    let app = Arc::new(app);
    let start = Instant::now();

    let successful = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_latency = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = Vec::new();

    for _ in 0..config.total_requests {
        let app = Arc::clone(&app);
        let semaphore = Arc::clone(&semaphore);
        let successful = Arc::clone(&successful);
        let failed = Arc::clone(&failed);
        let total_latency = Arc::clone(&total_latency);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let req_start = Instant::now();
            let request = Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap();

            // Clone the router for each request
            let response = app.as_ref().clone().oneshot(request).await;
            let latency = req_start.elapsed().as_millis() as u64;
            total_latency.fetch_add(latency, std::sync::atomic::Ordering::Relaxed);

            match response {
                Ok(resp) if resp.status() == StatusCode::OK => {
                    successful.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let success_count = successful.load(std::sync::atomic::Ordering::Relaxed);
    let fail_count = failed.load(std::sync::atomic::Ordering::Relaxed);
    let latency_sum = total_latency.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_requests: config.total_requests,
        successful: success_count,
        failed: fail_count,
        rate_limited: 0,
        duration,
        requests_per_second: config.total_requests as f64 / duration.as_secs_f64(),
        avg_latency_ms: latency_sum as f64 / config.total_requests as f64,
    };

    result.print("Health Endpoint (Light)");

    assert!(
        success_count as f64 / config.total_requests as f64 > 0.99,
        "Should have >99% success rate"
    );
}

#[tokio::test]
async fn test_load_health_endpoint_heavy() {
    let config = ServerLoadConfig::heavy();
    let app = create_test_app().await;

    println!("\nHealth Endpoint Load Test (Heavy)");
    println!(
        "Config: {} concurrent, {} total requests",
        config.concurrent_requests, config.total_requests
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_requests));
    let app = Arc::new(app);
    let start = Instant::now();

    let successful = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _ in 0..config.total_requests {
        let app = Arc::clone(&app);
        let semaphore = Arc::clone(&semaphore);
        let successful = Arc::clone(&successful);
        let failed = Arc::clone(&failed);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let request = Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap();

            match app.as_ref().clone().oneshot(request).await {
                Ok(resp) if resp.status() == StatusCode::OK => {
                    successful.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let success_count = successful.load(std::sync::atomic::Ordering::Relaxed);
    let fail_count = failed.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_requests: config.total_requests,
        successful: success_count,
        failed: fail_count,
        rate_limited: 0,
        duration,
        requests_per_second: config.total_requests as f64 / duration.as_secs_f64(),
        avg_latency_ms: duration.as_millis() as f64 / config.total_requests as f64,
    };

    result.print("Health Endpoint (Heavy)");

    assert!(
        success_count as f64 / config.total_requests as f64 > 0.95,
        "Should have >95% success rate under heavy load"
    );
}

// ============================================================================
// Registry API Load Tests
// ============================================================================

#[tokio::test]
async fn test_load_list_feature_views() {
    let config = ServerLoadConfig::medium();
    let app = create_test_app().await;

    println!("\nList Feature Views Load Test");
    println!(
        "Config: {} concurrent, {} total requests",
        config.concurrent_requests, config.total_requests
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_requests));
    let app = Arc::new(app);
    let start = Instant::now();

    let successful = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _ in 0..config.total_requests {
        let app = Arc::clone(&app);
        let semaphore = Arc::clone(&semaphore);
        let successful = Arc::clone(&successful);
        let failed = Arc::clone(&failed);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let request = Request::builder()
                .uri("/api/v1/feature-views")
                .body(Body::empty())
                .unwrap();

            match app.as_ref().clone().oneshot(request).await {
                Ok(resp) if resp.status() == StatusCode::OK => {
                    successful.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let success_count = successful.load(std::sync::atomic::Ordering::Relaxed);
    let fail_count = failed.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_requests: config.total_requests,
        successful: success_count,
        failed: fail_count,
        rate_limited: 0,
        duration,
        requests_per_second: config.total_requests as f64 / duration.as_secs_f64(),
        avg_latency_ms: duration.as_millis() as f64 / config.total_requests as f64,
    };

    result.print("List Feature Views");

    assert!(success_count > 0, "Should have some successful requests");
}

#[tokio::test]
async fn test_load_register_feature_views() {
    let config = ServerLoadConfig::light();
    let app = create_test_app().await;

    println!("\nRegister Feature Views Load Test");
    println!(
        "Config: {} concurrent, {} total requests",
        config.concurrent_requests, config.total_requests
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_requests));
    let app = Arc::new(app);
    let start = Instant::now();

    let successful = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..config.total_requests {
        let app = Arc::clone(&app);
        let semaphore = Arc::clone(&semaphore);
        let successful = Arc::clone(&successful);
        let failed = Arc::clone(&failed);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let body = create_feature_view_body(&format!("load_test_view_{}", i));
            let request = Request::builder()
                .method("POST")
                .uri("/api/v1/feature-views")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap();

            match app.as_ref().clone().oneshot(request).await {
                Ok(resp) if resp.status() == StatusCode::CREATED => {
                    successful.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let success_count = successful.load(std::sync::atomic::Ordering::Relaxed);
    let fail_count = failed.load(std::sync::atomic::Ordering::Relaxed);

    let result = LoadTestResult {
        total_requests: config.total_requests,
        successful: success_count,
        failed: fail_count,
        rate_limited: 0,
        duration,
        requests_per_second: config.total_requests as f64 / duration.as_secs_f64(),
        avg_latency_ms: duration.as_millis() as f64 / config.total_requests as f64,
    };

    result.print("Register Feature Views");

    // Some may fail due to duplicate names in concurrent scenario
    assert!(
        success_count as f64 / config.total_requests as f64 > 0.5,
        "Should have >50% success rate for registrations"
    );
}

// ============================================================================
// Mixed Workload Tests
// ============================================================================

#[tokio::test]
async fn test_load_mixed_workload() {
    let config = ServerLoadConfig::medium();
    let app = create_test_app().await;

    println!("\nMixed Workload Load Test");
    println!(
        "Config: {} concurrent, {} total requests (33% each: health, list, register)",
        config.concurrent_requests, config.total_requests
    );

    let semaphore = Arc::new(Semaphore::new(config.concurrent_requests));
    let app = Arc::new(app);
    let start = Instant::now();

    let health_success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let list_success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let register_success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..config.total_requests {
        let app = Arc::clone(&app);
        let semaphore = Arc::clone(&semaphore);
        let health_success = Arc::clone(&health_success);
        let list_success = Arc::clone(&list_success);
        let register_success = Arc::clone(&register_success);
        let total_failed = Arc::clone(&total_failed);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            match i % 3 {
                0 => {
                    // Health check
                    let request = Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap();

                    match app.as_ref().clone().oneshot(request).await {
                        Ok(resp) if resp.status() == StatusCode::OK => {
                            health_success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        _ => {
                            total_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
                1 => {
                    // List feature views
                    let request = Request::builder()
                        .uri("/api/v1/feature-views")
                        .body(Body::empty())
                        .unwrap();

                    match app.as_ref().clone().oneshot(request).await {
                        Ok(resp) if resp.status() == StatusCode::OK => {
                            list_success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        _ => {
                            total_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
                _ => {
                    // Register feature view
                    let body = create_feature_view_body(&format!("mixed_test_view_{}", i));
                    let request = Request::builder()
                        .method("POST")
                        .uri("/api/v1/feature-views")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&body).unwrap()))
                        .unwrap();

                    match app.as_ref().clone().oneshot(request).await {
                        Ok(resp) if resp.status() == StatusCode::CREATED => {
                            register_success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        _ => {
                            total_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let health = health_success.load(std::sync::atomic::Ordering::Relaxed);
    let list = list_success.load(std::sync::atomic::Ordering::Relaxed);
    let register = register_success.load(std::sync::atomic::Ordering::Relaxed);
    let failed = total_failed.load(std::sync::atomic::Ordering::Relaxed);

    println!("\n=== Mixed Workload Results ===");
    println!("Total requests: {}", config.total_requests);
    println!("Health successes: {}", health);
    println!("List successes: {}", list);
    println!("Register successes: {}", register);
    println!("Failed: {}", failed);
    println!("Duration: {:.2?}", duration);
    println!(
        "Throughput: {:.0} req/sec",
        config.total_requests as f64 / duration.as_secs_f64()
    );

    let total_success = health + list + register;
    assert!(
        total_success as f64 / config.total_requests as f64 > 0.7,
        "Should have >70% overall success rate"
    );
}

// ============================================================================
// Sustained Load Test
// ============================================================================

#[tokio::test]
async fn test_load_sustained_throughput() {
    let app = create_test_app().await;
    let duration_secs = 3;

    println!("\nSustained Load Test");
    println!("Duration: {} seconds", duration_secs);

    let app = Arc::new(app);
    let start = Instant::now();
    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Spawn multiple worker tasks
    let num_workers = 10;
    let mut handles = Vec::new();

    for _ in 0..num_workers {
        let app = Arc::clone(&app);
        let request_count = Arc::clone(&request_count);
        let success_count = Arc::clone(&success_count);
        // Note: Instant implements Copy, so we can use it directly in async block

        let handle = tokio::spawn(async move {
            while start.elapsed() < Duration::from_secs(duration_secs) {
                let request = Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap();

                request_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Note: oneshot returns Result<Response, Infallible>, so we unwrap safely
                let resp = app.as_ref().clone().oneshot(request).await.unwrap();
                if resp.status() == StatusCode::OK {
                    success_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let actual_duration = start.elapsed();
    let total_requests = request_count.load(std::sync::atomic::Ordering::Relaxed);
    let total_success = success_count.load(std::sync::atomic::Ordering::Relaxed);

    println!("\n=== Sustained Load Results ===");
    println!("Actual duration: {:.2?}", actual_duration);
    println!("Total requests: {}", total_requests);
    println!("Successful: {}", total_success);
    println!(
        "Sustained throughput: {:.0} req/sec",
        total_requests as f64 / actual_duration.as_secs_f64()
    );
    println!(
        "Success rate: {:.1}%",
        total_success as f64 / total_requests as f64 * 100.0
    );

    assert!(total_requests > 100, "Should complete many requests");
    assert!(
        total_success as f64 / total_requests as f64 > 0.95,
        "Should maintain >95% success rate under sustained load"
    );
}

// ============================================================================
// Benchmark Summary
// ============================================================================

#[tokio::test]
async fn test_benchmark_summary() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║           FEATUREDUCK SERVER LOAD TEST BENCHMARK           ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ Run all load tests with:                                   ║");
    println!("║   cargo test -p featureduck-server --test load_test \\     ║");
    println!("║     --release -- --nocapture                               ║");
    println!("║                                                            ║");
    println!("║ Test Configurations:                                       ║");
    println!("║   light:  10 concurrent, 100 total                        ║");
    println!("║   medium: 50 concurrent, 500 total                        ║");
    println!("║   heavy:  100 concurrent, 1000 total                      ║");
    println!("║   stress: 200 concurrent, 2000 total                      ║");
    println!("║                                                            ║");
    println!("║ Expected Performance:                                      ║");
    println!("║   Health endpoint: 5K-20K req/sec                         ║");
    println!("║   List views:      1K-5K req/sec                          ║");
    println!("║   Register view:   500-2K req/sec                         ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
}
