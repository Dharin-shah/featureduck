//! Tests for feature view statistics collection
//!
//! These tests verify the stats capture workflow:
//! 1. Save stats after materialization
//! 2. Retrieve stats for analysis
//! 3. Versioning and updates

use chrono::Utc;
use featureduck_registry::{FeatureRegistry, FeatureViewDef, FeatureViewStats};

fn create_test_feature_view(name: &str) -> FeatureViewDef {
    FeatureViewDef {
        name: name.to_string(),
        version: 1,
        source_type: "delta".to_string(),
        source_path: "/tmp/data".to_string(),
        entities: vec!["user_id".to_string()],
        transformations: "{}".to_string(),
        timestamp_field: Some("timestamp".to_string()),
        ttl_days: Some(30),
        batch_schedule: None,
        description: Some("Test feature view".to_string()),
        tags: vec![],
        owner: Some("test".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn test_save_and_retrieve_stats() {
    // Given: A registry with a registered feature view
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_feature_view("user_features");
    registry.register_feature_view(&view).await.unwrap();

    // When: We save stats for the feature view
    let stats = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 1,
        row_count: 1_000_000,
        distinct_entities: 50_000,
        min_timestamp: Some(1704067200000000), // 2024-01-01
        max_timestamp: Some(1735689600000000), // 2025-01-01
        avg_file_size_bytes: Some(10_485_760), // 10MB
        total_size_bytes: 1_073_741_824,       // 1GB
        histogram_buckets: Some(r#"{"buckets":[100,200,300]}"#.to_string()),
        created_at: Utc::now(),
    };

    registry.save_stats(&stats).await.unwrap();

    // Then: We can retrieve the stats
    let retrieved = registry
        .get_latest_stats("user_features")
        .await
        .unwrap()
        .expect("Stats should exist");

    assert_eq!(retrieved.feature_view, "user_features");
    assert_eq!(retrieved.version, 1);
    assert_eq!(retrieved.row_count, 1_000_000);
    assert_eq!(retrieved.distinct_entities, 50_000);
    assert_eq!(retrieved.total_size_bytes, 1_073_741_824);
}

#[tokio::test]
async fn test_stats_versioning() {
    // Given: A registry with stats for version 1
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_feature_view("user_features");
    registry.register_feature_view(&view).await.unwrap();

    let stats_v1 = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 1,
        row_count: 1_000,
        distinct_entities: 100,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 1_048_576, // 1MB
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    let created_at_v1 = stats_v1.created_at;
    registry.save_stats(&stats_v1).await.unwrap();
    println!("Saved stats_v1 with created_at: {:?}", created_at_v1);

    // Wait to ensure different created_at timestamps
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // When: We save stats for version 2 (with later timestamp)
    let stats_v2 = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 2,
        row_count: 10_000,
        distinct_entities: 1_000,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 10_485_760, // 10MB
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    let created_at_v2 = stats_v2.created_at;
    registry.save_stats(&stats_v2).await.unwrap();
    println!("Saved stats_v2 with created_at: {:?}", created_at_v2);
    println!(
        "Time difference: {} seconds",
        (created_at_v2 - created_at_v1).num_seconds()
    );

    // Then: get_latest_stats returns version 2 (most recent by created_at)
    let latest = registry
        .get_latest_stats("user_features")
        .await
        .unwrap()
        .expect("Stats should exist");

    println!(
        "Latest stats version: {}, created_at: {:?}",
        latest.version, latest.created_at
    );
    println!("Expected version: 2");
    assert_eq!(
        latest.version, 2,
        "Should return version 2 (most recent by created_at)"
    );
    assert_eq!(latest.row_count, 10_000);
    assert_eq!(latest.total_size_bytes, 10_485_760);
}

#[tokio::test]
async fn test_stats_update_for_same_version() {
    // Given: Stats for version 1
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_feature_view("user_features");
    registry.register_feature_view(&view).await.unwrap();

    let stats_v1_old = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 1,
        row_count: 1_000,
        distinct_entities: 100,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 1_048_576,
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    registry.save_stats(&stats_v1_old).await.unwrap();

    // When: We re-save stats for version 1 with updated values (re-materialization)
    let stats_v1_new = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 1,
        row_count: 2_000,       // Updated
        distinct_entities: 200, // Updated
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 2_097_152, // Updated
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    registry.save_stats(&stats_v1_new).await.unwrap();

    // Then: Latest stats reflect the update (UPSERT behavior)
    let latest = registry
        .get_latest_stats("user_features")
        .await
        .unwrap()
        .expect("Stats should exist");

    assert_eq!(latest.version, 1);
    assert_eq!(latest.row_count, 2_000); // Updated value
    assert_eq!(latest.total_size_bytes, 2_097_152); // Updated value
}

#[tokio::test]
async fn test_get_stats_for_nonexistent_view() {
    // Given: A registry
    let registry = FeatureRegistry::in_memory().await.unwrap();

    // When: We try to get stats for a non-existent feature view
    let result = registry.get_latest_stats("nonexistent").await.unwrap();

    // Then: Returns None (not an error)
    assert!(result.is_none());
}

#[tokio::test]
async fn test_histogram_buckets() {
    // Given: Stats with histogram data
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_feature_view("user_features");
    registry.register_feature_view(&view).await.unwrap();

    let histogram_json = r#"{
        "buckets": [
            {"min": 0, "max": 100, "count": 1000},
            {"min": 100, "max": 1000, "count": 5000},
            {"min": 1000, "max": 10000, "count": 500}
        ]
    }"#;

    let stats = FeatureViewStats {
        feature_view: "user_features".to_string(),
        version: 1,
        row_count: 6_500,
        distinct_entities: 6_500,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 1_048_576,
        histogram_buckets: Some(histogram_json.to_string()),
        created_at: Utc::now(),
    };

    registry.save_stats(&stats).await.unwrap();

    // When: We retrieve the stats
    let retrieved = registry
        .get_latest_stats("user_features")
        .await
        .unwrap()
        .expect("Stats should exist");

    // Then: Histogram data is preserved
    assert!(retrieved.histogram_buckets.is_some());
    let histogram = retrieved.histogram_buckets.unwrap();
    assert!(histogram.contains("buckets"));
    assert!(histogram.contains("\"count\": 5000"));
}

#[tokio::test]
async fn test_timestamp_range() {
    // Given: Stats with timestamp range
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_feature_view("time_series_features");
    registry.register_feature_view(&view).await.unwrap();

    let min_ts = 1704067200000000i64; // 2024-01-01 00:00:00 UTC
    let max_ts = 1735689600000000i64; // 2025-01-01 00:00:00 UTC

    let stats = FeatureViewStats {
        feature_view: "time_series_features".to_string(),
        version: 1,
        row_count: 10_000_000,
        distinct_entities: 100_000,
        min_timestamp: Some(min_ts),
        max_timestamp: Some(max_ts),
        avg_file_size_bytes: None,
        total_size_bytes: 10_737_418_240, // 10GB
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    registry.save_stats(&stats).await.unwrap();

    // When: We retrieve the stats
    let retrieved = registry
        .get_latest_stats("time_series_features")
        .await
        .unwrap()
        .expect("Stats should exist");

    // Then: Timestamp range is preserved
    assert_eq!(retrieved.min_timestamp, Some(min_ts));
    assert_eq!(retrieved.max_timestamp, Some(max_ts));

    // And: We can compute time range for analysis
    let time_range_seconds = (max_ts - min_ts) / 1_000_000;
    let time_range_days = time_range_seconds / 86400;
    assert!(
        time_range_days >= 365,
        "Should span at least 1 year (actual: {} days)",
        time_range_days
    );
}

#[tokio::test]
async fn test_multiple_views_with_different_sizes() {
    // Given: A registry with multiple feature views
    let registry = FeatureRegistry::in_memory().await.unwrap();

    let view1 = create_test_feature_view("user_7d");
    let view2 = create_test_feature_view("user_all_time");
    registry.register_feature_view(&view1).await.unwrap();
    registry.register_feature_view(&view2).await.unwrap();

    // Small view stats
    let small_stats = FeatureViewStats {
        feature_view: "user_7d".to_string(),
        version: 1,
        row_count: 1_000_000,
        distinct_entities: 50_000,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 1_073_741_824, // 1GB
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    // Large view stats
    let large_stats = FeatureViewStats {
        feature_view: "user_all_time".to_string(),
        version: 1,
        row_count: 1_000_000_000,
        distinct_entities: 50_000_000,
        min_timestamp: None,
        max_timestamp: None,
        avg_file_size_bytes: None,
        total_size_bytes: 107_374_182_400, // 100GB
        histogram_buckets: None,
        created_at: Utc::now(),
    };

    registry.save_stats(&small_stats).await.unwrap();
    registry.save_stats(&large_stats).await.unwrap();

    // When: We retrieve stats for both views
    let small = registry
        .get_latest_stats("user_7d")
        .await
        .unwrap()
        .expect("Small stats should exist");
    let large = registry
        .get_latest_stats("user_all_time")
        .await
        .unwrap()
        .expect("Large stats should exist");

    // Then: Each view has correct stats stored
    assert_eq!(small.total_size_bytes, 1_073_741_824);
    assert_eq!(small.distinct_entities, 50_000);

    assert_eq!(large.total_size_bytes, 107_374_182_400);
    assert_eq!(large.distinct_entities, 50_000_000);
}
