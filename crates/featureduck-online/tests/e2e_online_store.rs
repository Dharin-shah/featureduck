//! E2E tests for Online Store
//!
//! These tests require Redis/PostgreSQL to be running.
//! Run with: `cargo test -p featureduck-online --test e2e_online_store`
//!
//! To run ignored tests (requires running services):
//! `cargo test -p featureduck-online --test e2e_online_store -- --ignored`

use featureduck_online::{EntityKey, FeatureRow, FeatureValue, OnlineStore, OnlineWriteConfig};
use std::collections::HashMap;
use std::time::Duration;

// Helper to create test feature rows
fn create_test_feature_rows(count: usize) -> Vec<FeatureRow> {
    (0..count)
        .map(|i| {
            let mut features = HashMap::new();
            features.insert("clicks_7d".to_string(), FeatureValue::Int(i as i64 * 10));
            features.insert("score".to_string(), FeatureValue::Float(0.5 + (i as f64 * 0.01)));
            features.insert(
                "category".to_string(),
                FeatureValue::String(format!("cat_{}", i % 5)),
            );
            features.insert("is_active".to_string(), FeatureValue::Bool(i % 2 == 0));

            FeatureRow {
                entities: vec![EntityKey::new("user_id", format!("user_{}", i))],
                features,
                timestamp: chrono::Utc::now(),
            }
        })
        .collect()
}

// =============================================================================
// Redis Tests
// =============================================================================

#[cfg(feature = "redis")]
mod redis_tests {
    use super::*;
    use featureduck_online::{RedisConfig, RedisOnlineStore};

    async fn get_redis_store() -> Option<RedisOnlineStore> {
        let config = RedisConfig::new("redis://localhost:6379");
        RedisOnlineStore::new(config).await.ok()
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_health_check() {
        let store = get_redis_store().await.expect("Redis connection required");
        let result = store.health_check().await;
        assert!(result.is_ok(), "Health check should pass");
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_write_and_read_single() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_single";

        // Create a single feature row
        let rows = create_test_feature_rows(1);
        let entity_key = rows[0].entities[0].clone();

        // Write
        let write_config = OnlineWriteConfig { ttl: Some(Duration::from_secs(60)) };
        let result = store
            .write_online_features(feature_view, rows.clone(), write_config)
            .await;
        assert!(result.is_ok(), "Write should succeed");

        // Read
        let read_result = store
            .get_online_features(feature_view, vec![entity_key.clone()])
            .await;
        assert!(read_result.is_ok(), "Read should succeed");

        let features = read_result.unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(
            features[0].features.get("clicks_7d"),
            Some(&FeatureValue::Int(0))
        );

        // Cleanup
        let _ = store.delete_online_features(feature_view, vec![entity_key]).await;
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_write_and_read_batch() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_batch";

        // Create batch of feature rows
        let rows = create_test_feature_rows(100);
        let entity_keys: Vec<_> = rows.iter().map(|r| r.entities[0].clone()).collect();

        // Write batch
        let write_config = OnlineWriteConfig { ttl: Some(Duration::from_secs(60)) };
        let result = store
            .write_online_features(feature_view, rows, write_config)
            .await;
        assert!(result.is_ok(), "Batch write should succeed");

        // Read batch
        let read_result = store
            .get_online_features(feature_view, entity_keys.clone())
            .await;
        assert!(read_result.is_ok(), "Batch read should succeed");

        let features = read_result.unwrap();
        assert_eq!(features.len(), 100);

        // Verify some values
        for (i, row) in features.iter().enumerate() {
            let expected_clicks = i as i64 * 10;
            assert_eq!(
                row.features.get("clicks_7d"),
                Some(&FeatureValue::Int(expected_clicks)),
                "Row {} should have correct clicks",
                i
            );
        }

        // Cleanup
        let _ = store.delete_online_features(feature_view, entity_keys).await;
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_delete() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_delete";

        // Create and write
        let rows = create_test_feature_rows(5);
        let entity_keys: Vec<_> = rows.iter().map(|r| r.entities[0].clone()).collect();

        let write_config = OnlineWriteConfig { ttl: None };
        store
            .write_online_features(feature_view, rows, write_config)
            .await
            .unwrap();

        // Verify written
        let read1 = store
            .get_online_features(feature_view, entity_keys.clone())
            .await
            .unwrap();
        assert_eq!(read1.len(), 5);
        assert!(!read1[0].features.is_empty(), "Should have features");

        // Delete
        store
            .delete_online_features(feature_view, entity_keys.clone())
            .await
            .unwrap();

        // Verify deleted (features should be empty)
        let read2 = store
            .get_online_features(feature_view, entity_keys)
            .await
            .unwrap();
        assert_eq!(read2.len(), 5); // Still returns rows for each entity
        assert!(read2[0].features.is_empty(), "Features should be empty after delete");
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_nonexistent_entity() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_nonexistent";

        // Read non-existent entity
        let entity_key = EntityKey::new("user_id", "nonexistent_user_12345");
        let result = store
            .get_online_features(feature_view, vec![entity_key])
            .await;

        assert!(result.is_ok(), "Read should succeed even for non-existent entity");
        let features = result.unwrap();
        assert_eq!(features.len(), 1);
        assert!(features[0].features.is_empty(), "Non-existent entity should have empty features");
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_ttl_expiration() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_ttl";

        // Create row with 1-second TTL
        let rows = create_test_feature_rows(1);
        let entity_key = rows[0].entities[0].clone();

        let write_config = OnlineWriteConfig {
            ttl: Some(Duration::from_secs(1)),
        };
        store
            .write_online_features(feature_view, rows, write_config)
            .await
            .unwrap();

        // Read immediately - should have features
        let read1 = store
            .get_online_features(feature_view, vec![entity_key.clone()])
            .await
            .unwrap();
        assert!(!read1[0].features.is_empty(), "Should have features before TTL");

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Read after TTL - should be empty
        let read2 = store
            .get_online_features(feature_view, vec![entity_key])
            .await
            .unwrap();
        assert!(read2[0].features.is_empty(), "Features should be expired after TTL");
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_store_type() {
        let store = get_redis_store().await.expect("Redis connection required");
        assert_eq!(store.store_type(), "redis");
    }

    #[tokio::test]
    #[ignore = "Requires Redis to be running"]
    async fn test_redis_large_batch_performance() {
        let store = get_redis_store().await.expect("Redis connection required");
        let feature_view = "test_redis_perf";

        // Create 10K rows
        let rows = create_test_feature_rows(10_000);
        let entity_keys: Vec<_> = rows.iter().map(|r| r.entities[0].clone()).collect();

        let start = std::time::Instant::now();

        // Write 10K rows
        let write_config = OnlineWriteConfig { ttl: Some(Duration::from_secs(60)) };
        store
            .write_online_features(feature_view, rows, write_config)
            .await
            .unwrap();

        let write_duration = start.elapsed();
        let write_rate = 10_000.0 / write_duration.as_secs_f64();

        println!("Redis write: 10K rows in {:?} ({:.0}/sec)", write_duration, write_rate);

        // Read 10K rows
        let start = std::time::Instant::now();
        let result = store
            .get_online_features(feature_view, entity_keys.clone())
            .await
            .unwrap();
        let read_duration = start.elapsed();
        let read_rate = 10_000.0 / read_duration.as_secs_f64();

        println!("Redis read: 10K rows in {:?} ({:.0}/sec)", read_duration, read_rate);

        assert_eq!(result.len(), 10_000);
        assert!(write_rate > 10_000.0, "Write should be at least 10K/sec");
        assert!(read_rate > 10_000.0, "Read should be at least 10K/sec");

        // Cleanup
        let _ = store.delete_online_features(feature_view, entity_keys).await;
    }
}

// =============================================================================
// PostgreSQL Tests
// =============================================================================

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;
    use featureduck_online::{PostgresConfig, PostgresOnlineStore};

    async fn get_postgres_store() -> Option<PostgresOnlineStore> {
        let config = PostgresConfig::new("postgresql://postgres:postgres@localhost/featureduck");
        PostgresOnlineStore::new(config).await.ok()
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL to be running"]
    async fn test_postgres_health_check() {
        let store = get_postgres_store()
            .await
            .expect("PostgreSQL connection required");
        let result = store.health_check().await;
        assert!(result.is_ok(), "Health check should pass");
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL to be running"]
    async fn test_postgres_write_and_read_single() {
        let store = get_postgres_store()
            .await
            .expect("PostgreSQL connection required");
        let feature_view = "test_postgres_single";

        // Create a single feature row
        let rows = create_test_feature_rows(1);
        let entity_key = rows[0].entities[0].clone();

        // Write
        let write_config = OnlineWriteConfig { ttl: None };
        let result = store
            .write_online_features(feature_view, rows.clone(), write_config)
            .await;
        assert!(result.is_ok(), "Write should succeed");

        // Read
        let read_result = store
            .get_online_features(feature_view, vec![entity_key.clone()])
            .await;
        assert!(read_result.is_ok(), "Read should succeed");

        let features = read_result.unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(
            features[0].features.get("clicks_7d"),
            Some(&FeatureValue::Int(0))
        );

        // Cleanup
        let _ = store.delete_online_features(feature_view, vec![entity_key]).await;
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL to be running"]
    async fn test_postgres_write_and_read_batch() {
        let store = get_postgres_store()
            .await
            .expect("PostgreSQL connection required");
        let feature_view = "test_postgres_batch";

        // Create batch of feature rows
        let rows = create_test_feature_rows(100);
        let entity_keys: Vec<_> = rows.iter().map(|r| r.entities[0].clone()).collect();

        // Write batch
        let write_config = OnlineWriteConfig { ttl: None };
        let result = store
            .write_online_features(feature_view, rows, write_config)
            .await;
        assert!(result.is_ok(), "Batch write should succeed");

        // Read batch
        let read_result = store
            .get_online_features(feature_view, entity_keys.clone())
            .await;
        assert!(read_result.is_ok(), "Batch read should succeed");

        let features = read_result.unwrap();
        assert_eq!(features.len(), 100);

        // Cleanup
        let _ = store.delete_online_features(feature_view, entity_keys).await;
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL to be running"]
    async fn test_postgres_store_type() {
        let store = get_postgres_store()
            .await
            .expect("PostgreSQL connection required");
        assert_eq!(store.store_type(), "postgres");
    }
}

// =============================================================================
// Sync Tests (Configuration only - actual sync requires Delta data)
// =============================================================================

mod sync_tests {
    use featureduck_online::{SyncConfig, SyncResult};

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();
        assert_eq!(config.read_batch_size, 10_000);
        assert_eq!(config.write_batch_size, 1_000);
        assert!(config.ttl.is_none());
    }

    #[test]
    fn test_sync_config_custom() {
        let config = SyncConfig {
            read_batch_size: 50_000,
            write_batch_size: 5_000,
            ttl: Some(std::time::Duration::from_secs(3600)),
        };
        assert_eq!(config.read_batch_size, 50_000);
        assert_eq!(config.write_batch_size, 5_000);
        assert_eq!(config.ttl.unwrap().as_secs(), 3600);
    }

    #[test]
    fn test_sync_result_metrics() {
        let result = SyncResult {
            rows_synced: 10_000,
            duration: std::time::Duration::from_secs(2),
            rows_per_sec: 5_000.0,
        };
        assert_eq!(result.rows_synced, 10_000);
        assert_eq!(result.duration.as_secs(), 2);
        assert_eq!(result.rows_per_sec, 5_000.0);
    }
}

// =============================================================================
// Common Tests (no external dependencies)
// =============================================================================

mod common_tests {
    use super::*;
    use featureduck_online::{build_online_key, parse_online_key};

    #[test]
    fn test_build_online_key_single_entity() {
        let key = EntityKey::new("user_id", "123");
        let online_key = build_online_key("user_features", &[key]);
        assert!(online_key.contains("user_features"));
        assert!(online_key.contains("user_id=123"));
    }

    #[test]
    fn test_build_online_key_multiple_entities() {
        let key1 = EntityKey::new("user_id", "123");
        let key2 = EntityKey::new("item_id", "456");
        let online_key = build_online_key("user_item_features", &[key1, key2]);
        assert!(online_key.contains("user_item_features"));
        assert!(online_key.contains("user_id=123"));
        assert!(online_key.contains("item_id=456"));
    }

    #[test]
    fn test_parse_online_key() {
        let key = EntityKey::new("user_id", "123");
        let online_key = build_online_key("user_features", &[key]);

        let (feature_view, entities) = parse_online_key(&online_key).unwrap();
        assert_eq!(feature_view, "user_features");
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name, "user_id");
        assert_eq!(entities[0].value, "123");
    }

    #[test]
    fn test_feature_row_creation() {
        let rows = create_test_feature_rows(5);
        assert_eq!(rows.len(), 5);

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.entities.len(), 1);
            assert_eq!(row.entities[0].name, "user_id");
            assert_eq!(row.entities[0].value, format!("user_{}", i));
            assert!(row.features.contains_key("clicks_7d"));
            assert!(row.features.contains_key("score"));
            assert!(row.features.contains_key("category"));
            assert!(row.features.contains_key("is_active"));
        }
    }

    #[test]
    fn test_online_write_config_default() {
        let config = OnlineWriteConfig { ttl: None };
        assert!(config.ttl.is_none());
    }

    #[test]
    fn test_online_write_config_with_ttl() {
        let config = OnlineWriteConfig {
            ttl: Some(std::time::Duration::from_secs(3600)),
        };
        assert_eq!(config.ttl.unwrap().as_secs(), 3600);
    }
}
