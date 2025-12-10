//! End-to-End Tests for Incremental Feature Materialization
//!
//! Tests the complete incremental pipeline including:
//! - HyperLogLog for COUNT DISTINCT
//! - Aggregate Trees for hierarchical aggregation
//! - Time-windowed features
//! - Incremental merge semantics
//! - State management

use chrono::Utc;
use featureduck_core::aggregate_tree::{AggregateTree, TimeGranularity};
use featureduck_core::hyperloglog::HyperLogLog;
use featureduck_core::incremental::merge_feature_rows;
use featureduck_core::time_window::{TimeWindow, WindowedFeature};
use featureduck_core::{AggExpr, AggFunction, EntityKey, Expr, FeatureRow, FeatureValue};
use std::time::Duration;

// ============================================================================
// HyperLogLog E2E Tests
// ============================================================================

#[test]
fn test_hll_incremental_count_distinct() {
    // Simulate daily materialization batches
    let mut combined_hll = HyperLogLog::new(12);

    // Day 1: 1000 users
    let mut day1_hll = HyperLogLog::new(12);
    for i in 0..1000 {
        day1_hll.insert(&format!("user_{}", i));
    }
    combined_hll.merge(&day1_hll);

    // Day 2: 500 new users + 500 returning
    let mut day2_hll = HyperLogLog::new(12);
    for i in 500..1500 {
        day2_hll.insert(&format!("user_{}", i));
    }
    combined_hll.merge(&day2_hll);

    // Day 3: 300 new users + 700 returning
    let mut day3_hll = HyperLogLog::new(12);
    for i in 1200..2000 {
        day3_hll.insert(&format!("user_{}", i));
    }
    combined_hll.merge(&day3_hll);

    // Should have ~2000 unique users (0-1999)
    let estimate = combined_hll.cardinality();
    assert!(
        (1900..=2100).contains(&estimate),
        "Expected ~2000 unique users, got {}",
        estimate
    );
}

#[test]
fn test_hll_serialization_roundtrip() {
    let mut hll = HyperLogLog::new(14);
    for i in 0..10000 {
        hll.insert(&format!("item_{}", i));
    }

    let original_estimate = hll.cardinality();

    // Serialize to bytes
    let bytes = hll.to_bytes();

    // Deserialize
    let restored = HyperLogLog::from_bytes(&bytes).expect("Deserialization should work");

    assert_eq!(original_estimate, restored.cardinality());
}

#[test]
fn test_hll_high_cardinality() {
    let mut hll = HyperLogLog::new(14); // 0.81% error

    // Insert 1 million unique values
    for i in 0..1_000_000 {
        hll.insert(&i);
    }

    let estimate = hll.cardinality();
    let error_rate = ((estimate as f64 - 1_000_000.0) / 1_000_000.0).abs();

    assert!(
        error_rate < 0.03, // 3% tolerance (3 sigma for 0.81% std error)
        "Expected ~1M, got {} (error: {:.2}%)",
        estimate,
        error_rate * 100.0
    );
}

// ============================================================================
// Aggregate Tree E2E Tests
// ============================================================================

#[test]
fn test_aggregate_tree_daily_materialization() {
    let mut tree = AggregateTree::new(vec![
        TimeGranularity::Minute,
        TimeGranularity::Hour,
        TimeGranularity::Day,
    ]);

    let base_ts = 1699920000i64; // Fixed timestamp

    // Simulate 24 hours of events (1 event per minute)
    for minute in 0..(24 * 60) {
        let ts = base_ts + minute * 60;
        let value = (minute % 100) as f64; // Some value pattern
        tree.insert_event(ts, value);
    }

    // Query full day
    let sum = tree.query_sum(base_ts, base_ts + 24 * 3600);
    let count = tree.query_count(base_ts, base_ts + 24 * 3600);

    assert_eq!(count, 24 * 60, "Should have 1440 events");
    assert!(sum > 0.0, "Sum should be positive");

    // Compact to higher levels
    tree.compact();

    // Query should still work after compaction
    let sum_after = tree.query_sum(base_ts, base_ts + 24 * 3600);
    assert!(
        (sum - sum_after).abs() < 0.001,
        "Sum should be same after compaction"
    );
}

#[test]
fn test_aggregate_tree_with_count_distinct() {
    let mut tree =
        AggregateTree::new(vec![TimeGranularity::Minute, TimeGranularity::Hour]).with_hll(12);

    let base_ts = 1699920000i64;

    // Insert events with user IDs
    for minute in 0..60 {
        for event in 0..10 {
            let ts = base_ts + minute * 60 + event;
            let user_id = format!("user_{}", event % 5); // 5 unique users per minute
            tree.insert_event_with_distinct(ts, 1.0, &user_id);
        }
    }

    // Query count distinct for the hour
    let count_distinct = tree.query_count_distinct(base_ts, base_ts + 3600);
    assert!(count_distinct.is_some());

    // Should be ~5 unique users (same 5 users every minute)
    let cd = count_distinct.unwrap();
    assert!(
        (4..=6).contains(&cd),
        "Expected ~5 unique users, got {}",
        cd
    );
}

#[test]
fn test_aggregate_tree_pruning() {
    let mut tree = AggregateTree::new(vec![TimeGranularity::Minute]);

    let base_ts = 1699920000i64;

    // Insert 7 days of data
    for day in 0..7 {
        for minute in 0..(24 * 60) {
            let ts = base_ts + day * 86400 + minute * 60;
            tree.insert_event(ts, 1.0);
        }
    }

    let total_before = tree.total_buckets();
    assert_eq!(
        total_before,
        7 * 24 * 60,
        "Should have 7 days of minute buckets"
    );

    // Prune data older than 3 days
    let pruned = tree.prune_before(base_ts + 4 * 86400);

    assert!(pruned > 0, "Should have pruned some buckets");
    assert!(
        tree.total_buckets() < total_before,
        "Should have fewer buckets after pruning"
    );
}

// ============================================================================
// Incremental Merge E2E Tests
// ============================================================================

#[test]
fn test_incremental_merge_count_sum() {
    let entity = EntityKey::new("user_id", "user_1");

    // Day 1 aggregates
    let mut day1 = FeatureRow::new(vec![entity.clone()], Utc::now());
    day1.add_feature("click_count".to_string(), FeatureValue::Int(100));
    day1.add_feature("total_amount".to_string(), FeatureValue::Float(1000.0));

    // Day 2 aggregates
    let mut day2 = FeatureRow::new(vec![entity.clone()], Utc::now());
    day2.add_feature("click_count".to_string(), FeatureValue::Int(50));
    day2.add_feature("total_amount".to_string(), FeatureValue::Float(500.0));

    let aggregations = vec![
        AggExpr {
            function: AggFunction::CountAll,
            alias: Some("click_count".to_string()),
        },
        AggExpr {
            function: AggFunction::Sum(Expr::column("amount")),
            alias: Some("total_amount".to_string()),
        },
    ];

    // Merge
    let merged = merge_feature_rows(&day1, day2, &aggregations).expect("Merge should succeed");

    // Verify additive merge
    assert_eq!(
        merged.features.get("click_count"),
        Some(&FeatureValue::Int(150))
    );

    match merged.features.get("total_amount") {
        Some(FeatureValue::Float(v)) => assert!((v - 1500.0).abs() < 0.001),
        _ => panic!("Expected Float value for total_amount"),
    }
}

#[test]
fn test_incremental_merge_min_max() {
    let entity = EntityKey::new("user_id", "user_1");

    // Batch 1: min=5, max=95
    let mut batch1 = FeatureRow::new(vec![entity.clone()], Utc::now());
    batch1.add_feature("min_price".to_string(), FeatureValue::Float(5.0));
    batch1.add_feature("max_price".to_string(), FeatureValue::Float(95.0));

    // Batch 2: min=3, max=80
    let mut batch2 = FeatureRow::new(vec![entity.clone()], Utc::now());
    batch2.add_feature("min_price".to_string(), FeatureValue::Float(3.0));
    batch2.add_feature("max_price".to_string(), FeatureValue::Float(80.0));

    let aggregations = vec![
        AggExpr {
            function: AggFunction::Min(Expr::column("price")),
            alias: Some("min_price".to_string()),
        },
        AggExpr {
            function: AggFunction::Max(Expr::column("price")),
            alias: Some("max_price".to_string()),
        },
    ];

    let merged = merge_feature_rows(&batch1, batch2, &aggregations).expect("Merge should succeed");

    // MIN should take the smaller value
    match merged.features.get("min_price") {
        Some(FeatureValue::Float(v)) => assert!((v - 3.0).abs() < 0.001),
        _ => panic!("Expected Float value for min_price"),
    }

    // MAX should take the larger value
    match merged.features.get("max_price") {
        Some(FeatureValue::Float(v)) => assert!((v - 95.0).abs() < 0.001),
        _ => panic!("Expected Float value for max_price"),
    }
}

#[test]
fn test_incremental_merge_multiple_batches() {
    let entity = EntityKey::new("user_id", "user_1");

    // Start with empty state
    let mut accumulated = FeatureRow::new(vec![entity.clone()], Utc::now());
    accumulated.add_feature("event_count".to_string(), FeatureValue::Int(0));
    accumulated.add_feature("total_value".to_string(), FeatureValue::Float(0.0));

    let aggregations = vec![
        AggExpr {
            function: AggFunction::CountAll,
            alias: Some("event_count".to_string()),
        },
        AggExpr {
            function: AggFunction::Sum(Expr::column("value")),
            alias: Some("total_value".to_string()),
        },
    ];

    // Simulate 10 batches
    for batch_num in 1..=10 {
        let mut batch = FeatureRow::new(vec![entity.clone()], Utc::now());
        batch.add_feature("event_count".to_string(), FeatureValue::Int(100));
        batch.add_feature(
            "total_value".to_string(),
            FeatureValue::Float(batch_num as f64 * 100.0),
        );

        accumulated =
            merge_feature_rows(&accumulated, batch, &aggregations).expect("Merge should succeed");
    }

    // After 10 batches: count = 1000, sum = 100+200+...+1000 = 5500
    assert_eq!(
        accumulated.features.get("event_count"),
        Some(&FeatureValue::Int(1000))
    );

    match accumulated.features.get("total_value") {
        Some(FeatureValue::Float(v)) => assert!((v - 5500.0).abs() < 0.001),
        _ => panic!("Expected Float value for total_value"),
    }
}

// ============================================================================
// Time Window E2E Tests
// ============================================================================

#[test]
fn test_time_window_bucket_alignment() {
    let window = TimeWindow::hourly();

    // Test various timestamps within an hour
    let hour_start = 1699920000i64; // Start of some hour
    let mid_hour = hour_start + 1800; // 30 minutes in
    let end_hour = hour_start + 3599; // 1 second before next hour

    let bucket1 = window.bucket_key(hour_start);
    let bucket2 = window.bucket_key(mid_hour);
    let bucket3 = window.bucket_key(end_hour);

    // All should be in the same bucket
    assert_eq!(bucket1, bucket2);
    assert_eq!(bucket2, bucket3);

    // Next hour should be different bucket
    let next_hour = hour_start + 3600;
    let bucket_next = window.bucket_key(next_hour);
    assert_eq!(bucket_next, bucket1 + 1);
}

#[test]
fn test_sliding_window_overlap() {
    // 24-hour window sliding every hour
    let window = TimeWindow::sliding(Duration::from_secs(24 * 3600), Duration::from_secs(3600));

    // An event belongs to 24 different windows (one per hour of the 24-hour range)
    assert_eq!(window.slides_per_window(), 24);

    // Window boundaries
    let ts = 1699920000i64;
    let bucket = window.bucket_key(ts);
    let start = window.window_start(bucket);
    let end = window.window_end(bucket);

    // Window should span 24 hours from start
    assert_eq!(end - start, 24 * 3600);
}

#[test]
fn test_windowed_feature_definition() {
    // Define a 7-day click count feature
    let clicks_7d = WindowedFeature::count("user_clicks", TimeWindow::last_7d());

    assert_eq!(clicks_7d.name, "user_clicks");
    assert_eq!(clicks_7d.window.duration.as_secs(), 7 * 86400);
    assert!(clicks_7d.window.is_sliding());

    // Define a count distinct feature
    let unique_products = WindowedFeature::count_distinct(
        "unique_products_viewed",
        "product_id",
        TimeWindow::last_30d(),
    );

    assert_eq!(unique_products.name, "unique_products_viewed");
    assert_eq!(unique_products.source_column, "product_id");
}

// ============================================================================
// Combined E2E Test
// ============================================================================

#[test]
fn test_full_incremental_pipeline() {
    // This test simulates a complete incremental materialization pipeline:
    // 1. Raw events arrive in batches
    // 2. Events are aggregated into minute buckets (aggregate tree)
    // 3. Aggregate tree is compacted to hour/day levels
    // 4. HyperLogLog tracks unique users
    // 5. Features are served via time window queries

    let base_ts = 1699920000i64;

    // Step 1: Create aggregate tree with HLL
    let mut tree = AggregateTree::new(vec![
        TimeGranularity::Minute,
        TimeGranularity::Hour,
        TimeGranularity::Day,
    ])
    .with_hll(12);

    // Step 2: Simulate 3 days of events
    let mut total_events = 0;
    let mut unique_users = std::collections::HashSet::new();

    for day in 0..3 {
        for hour in 0..24 {
            for minute in 0..60 {
                // 10 events per minute from 100 different users
                for event in 0..10 {
                    let ts = base_ts + day * 86400 + hour * 3600 + minute * 60;
                    let user_id = format!("user_{}", (day * 1000 + event) % 100);

                    tree.insert_event_with_distinct(ts, 1.0, &user_id);

                    total_events += 1;
                    unique_users.insert(user_id);
                }
            }
        }
    }

    // Step 3: Compact the tree
    tree.compact();

    // Step 4: Query features

    // Total events
    let count = tree.query_count(base_ts, base_ts + 3 * 86400);
    assert_eq!(
        count, total_events,
        "Count should match total events: {} vs {}",
        count, total_events
    );

    // Count distinct users
    let cd = tree.query_count_distinct(base_ts, base_ts + 3 * 86400);
    assert!(cd.is_some(), "Count distinct should be available");

    let cd_value = cd.unwrap();
    let actual_unique = unique_users.len() as u64;

    // HLL estimate should be within 5% of actual
    let error = (cd_value as f64 - actual_unique as f64).abs() / actual_unique as f64;
    assert!(
        error < 0.05,
        "HLL estimate {} should be within 5% of actual {} (error: {:.2}%)",
        cd_value,
        actual_unique,
        error * 100.0
    );

    // Step 5: Query time-windowed features

    // Last 24 hours
    let last_day_count = tree.query_count(base_ts + 2 * 86400, base_ts + 3 * 86400);
    let expected_day_count = 24 * 60 * 10; // 24 hours * 60 minutes * 10 events
    assert_eq!(
        last_day_count, expected_day_count,
        "Last day count should be {}",
        expected_day_count
    );

    println!("Full incremental pipeline test passed!");
    println!("  Total events: {}", total_events);
    println!("  Actual unique users: {}", actual_unique);
    println!(
        "  HLL estimate: {} (error: {:.2}%)",
        cd_value,
        error * 100.0
    );
    println!("  Total buckets: {}", tree.total_buckets());
}
