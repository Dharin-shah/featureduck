//! Comprehensive tests for dynamic schema discovery
//!
//! This test suite validates that FeatureDuck can handle arbitrary
//! feature schemas without hardcoded column names.

use chrono::{Datelike, Utc};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper to create test connector with isolated directory
async fn create_test_connector() -> (DeltaStorageConnector, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine_config = DuckDBEngineConfig::default();
    let connector = DeltaStorageConnector::new(temp_dir.path().to_str().unwrap(), engine_config)
        .await
        .unwrap();
    (connector, temp_dir)
}

/// Helper to create a feature row with many columns
fn create_wide_feature_row(user_id: &str) -> FeatureRow {
    let mut features = HashMap::new();

    // 5 Int64 features
    features.insert("clicks_1d".to_string(), FeatureValue::Int(100));
    features.insert("clicks_7d".to_string(), FeatureValue::Int(700));
    features.insert("clicks_30d".to_string(), FeatureValue::Int(3000));
    features.insert("purchases_7d".to_string(), FeatureValue::Int(5));
    features.insert("cart_adds_7d".to_string(), FeatureValue::Int(12));

    // 5 Float64 features
    features.insert("conversion_rate".to_string(), FeatureValue::Float(0.05));
    features.insert("avg_order_value".to_string(), FeatureValue::Float(125.50));
    features.insert("bounce_rate".to_string(), FeatureValue::Float(0.35));
    features.insert(
        "session_duration_avg".to_string(),
        FeatureValue::Float(180.5),
    );
    features.insert("revenue_7d".to_string(), FeatureValue::Float(627.50));

    // 5 String features
    features.insert(
        "last_device".to_string(),
        FeatureValue::String("mobile".to_string()),
    );
    features.insert(
        "last_browser".to_string(),
        FeatureValue::String("chrome".to_string()),
    );
    features.insert(
        "last_campaign".to_string(),
        FeatureValue::String("summer_sale".to_string()),
    );
    features.insert(
        "last_country".to_string(),
        FeatureValue::String("US".to_string()),
    );
    features.insert(
        "last_product_category".to_string(),
        FeatureValue::String("electronics".to_string()),
    );

    // 5 Boolean features
    features.insert("is_premium".to_string(), FeatureValue::Bool(true));
    features.insert("is_active_7d".to_string(), FeatureValue::Bool(true));
    features.insert("has_purchased".to_string(), FeatureValue::Bool(true));
    features.insert("email_verified".to_string(), FeatureValue::Bool(true));
    features.insert("phone_verified".to_string(), FeatureValue::Bool(false));

    // 5 derived/computed features
    features.insert("engagement_score".to_string(), FeatureValue::Float(8.5));
    features.insert("churn_risk".to_string(), FeatureValue::Float(0.15));
    features.insert("lifetime_value".to_string(), FeatureValue::Float(1250.00));
    features.insert("days_since_last_visit".to_string(), FeatureValue::Int(2));
    features.insert("total_sessions".to_string(), FeatureValue::Int(45));

    FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), user_id.to_string())],
        features,
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn test_dynamic_schema_25_columns_mixed_types() {
    // Given: Feature view with 25 columns of mixed types
    let (connector, _temp_dir) = create_test_connector().await;

    // Create sample data with 25 features
    let test_data = vec![
        create_wide_feature_row("user_1"),
        create_wide_feature_row("user_2"),
        create_wide_feature_row("user_3"),
    ];

    // When: Write features
    connector
        .write_features("wide_features", test_data.clone())
        .await
        .unwrap();

    // Then: Read back and verify all 25 columns preserved
    let entities = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let features = connector
        .read_features("wide_features", entities, None)
        .await
        .unwrap();

    assert_eq!(features.len(), 1, "Should return exactly 1 feature row");
    assert_eq!(
        features[0].features.len(),
        25,
        "Should preserve all 25 feature columns"
    );

    // Verify each type is preserved correctly
    assert!(
        matches!(
            features[0].features.get("clicks_1d"),
            Some(FeatureValue::Int(100))
        ),
        "Int64 type should be preserved"
    );
    assert!(
        matches!(
            features[0].features.get("conversion_rate"),
            Some(FeatureValue::Float(_))
        ),
        "Float64 type should be preserved"
    );
    assert!(
        matches!(
            features[0].features.get("last_device"),
            Some(FeatureValue::String(_))
        ),
        "String type should be preserved"
    );
    assert!(
        matches!(
            features[0].features.get("is_premium"),
            Some(FeatureValue::Bool(true))
        ),
        "Boolean type should be preserved"
    );
}

#[tokio::test]
async fn test_dynamic_schema_null_handling() {
    // Given: Feature rows with NULL values in various columns
    let (connector, _temp_dir) = create_test_connector().await;

    let mut features1 = HashMap::new();
    features1.insert("clicks".to_string(), FeatureValue::Int(100));
    features1.insert("revenue".to_string(), FeatureValue::Null); // NULL
    features1.insert("is_active".to_string(), FeatureValue::Bool(true));

    let mut features2 = HashMap::new();
    features2.insert("clicks".to_string(), FeatureValue::Null); // NULL
    features2.insert("revenue".to_string(), FeatureValue::Float(250.0));
    features2.insert("is_active".to_string(), FeatureValue::Null); // NULL

    let test_data = vec![
        FeatureRow {
            entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
            features: features1,
            timestamp: Utc::now(),
        },
        FeatureRow {
            entities: vec![EntityKey::new("user_id".to_string(), "user_2".to_string())],
            features: features2,
            timestamp: Utc::now(),
        },
    ];

    // When: Write features with NULLs
    connector
        .write_features("null_features", test_data)
        .await
        .unwrap();

    // Then: Read back and verify NULLs preserved
    let entities1 = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let result1 = connector
        .read_features("null_features", entities1, None)
        .await
        .unwrap();

    assert_eq!(result1.len(), 1);
    assert!(matches!(
        result1[0].features.get("revenue"),
        Some(FeatureValue::Null)
    ));

    let entities2 = vec![EntityKey::new("user_id".to_string(), "user_2".to_string())];
    let result2 = connector
        .read_features("null_features", entities2, None)
        .await
        .unwrap();

    assert_eq!(result2.len(), 1);
    assert!(matches!(
        result2[0].features.get("clicks"),
        Some(FeatureValue::Null)
    ));
    assert!(matches!(
        result2[0].features.get("is_active"),
        Some(FeatureValue::Null)
    ));
}

#[tokio::test]
async fn test_dynamic_schema_type_edge_cases() {
    // Given: Feature rows with edge case values
    let (connector, _temp_dir) = create_test_connector().await;

    let mut features = HashMap::new();

    // Very large integers (but within i64 range)
    features.insert("large_int".to_string(), FeatureValue::Int(i64::MAX - 1));
    features.insert("negative_int".to_string(), FeatureValue::Int(i64::MIN + 1));

    // Float edge cases
    features.insert("zero_float".to_string(), FeatureValue::Float(0.0));
    features.insert("negative_float".to_string(), FeatureValue::Float(-123.456));
    features.insert("small_float".to_string(), FeatureValue::Float(0.0000001));

    // String edge cases
    features.insert(
        "empty_string".to_string(),
        FeatureValue::String("".to_string()),
    );
    features.insert(
        "long_string".to_string(),
        FeatureValue::String("x".repeat(1000)),
    );
    features.insert(
        "unicode_string".to_string(),
        FeatureValue::String("Hello 世界 🌍".to_string()),
    );

    let test_data = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features,
        timestamp: Utc::now(),
    }];

    // When: Write edge case features
    connector
        .write_features("edge_cases", test_data)
        .await
        .unwrap();

    // Then: Read back and verify edge cases handled
    let entities = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let result = connector
        .read_features("edge_cases", entities, None)
        .await
        .unwrap();

    assert_eq!(result.len(), 1);

    // Verify large integers
    assert!(matches!(
        result[0].features.get("large_int"),
        Some(FeatureValue::Int(_))
    ));
    assert!(matches!(
        result[0].features.get("negative_int"),
        Some(FeatureValue::Int(_))
    ));

    // Verify float edge cases
    assert!(matches!(
        result[0].features.get("zero_float"),
        Some(FeatureValue::Float(f)) if *f == 0.0
    ));
    assert!(matches!(
        result[0].features.get("negative_float"),
        Some(FeatureValue::Float(f)) if *f < 0.0
    ));

    // Verify string edge cases
    assert!(matches!(
        result[0].features.get("empty_string"),
        Some(FeatureValue::String(s)) if s.is_empty()
    ));
    assert!(matches!(
        result[0].features.get("long_string"),
        Some(FeatureValue::String(s)) if s.len() == 1000
    ));
    assert!(matches!(
        result[0].features.get("unicode_string"),
        Some(FeatureValue::String(s)) if s.contains("世界")
    ));
}

#[tokio::test]
async fn test_point_in_time_with_arbitrary_schema() {
    // Given: Feature view with arbitrary schema, multiple versions over time
    let (connector, _temp_dir) = create_test_connector().await;

    let t1 = Utc::now();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let t2 = Utc::now();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let t3 = Utc::now();

    // Version 1 at T1
    let mut features_v1 = HashMap::new();
    features_v1.insert("score".to_string(), FeatureValue::Int(10));
    features_v1.insert("rating".to_string(), FeatureValue::Float(4.5));
    features_v1.insert(
        "status".to_string(),
        FeatureValue::String("active".to_string()),
    );

    let data_v1 = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features: features_v1,
        timestamp: t1,
    }];

    connector
        .write_features("versioned_features", data_v1)
        .await
        .unwrap();

    // Version 2 at T2
    let mut features_v2 = HashMap::new();
    features_v2.insert("score".to_string(), FeatureValue::Int(20));
    features_v2.insert("rating".to_string(), FeatureValue::Float(4.8));
    features_v2.insert(
        "status".to_string(),
        FeatureValue::String("premium".to_string()),
    );

    let data_v2 = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features: features_v2,
        timestamp: t2,
    }];

    connector
        .write_features("versioned_features", data_v2)
        .await
        .unwrap();

    // Version 3 at T3
    let mut features_v3 = HashMap::new();
    features_v3.insert("score".to_string(), FeatureValue::Int(30));
    features_v3.insert("rating".to_string(), FeatureValue::Float(5.0));
    features_v3.insert(
        "status".to_string(),
        FeatureValue::String("vip".to_string()),
    );

    let data_v3 = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features: features_v3,
        timestamp: t3,
    }];

    connector
        .write_features("versioned_features", data_v3)
        .await
        .unwrap();

    // When: Query at T2 (should get version 2)
    let entities = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let result = connector
        .read_features(
            "versioned_features",
            entities.clone(),
            Some(t2 + chrono::Duration::milliseconds(50)),
        )
        .await
        .unwrap();

    // Then: Should get T2 version with all columns
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 3, "Should have 3 feature columns");

    assert!(
        matches!(result[0].features.get("score"), Some(FeatureValue::Int(20))),
        "Should get T2 version of score (20)"
    );
    assert!(
        matches!(
            result[0].features.get("rating"),
            Some(FeatureValue::Float(r)) if (*r - 4.8).abs() < 0.01
        ),
        "Should get T2 version of rating (4.8)"
    );
    assert!(
        matches!(
            result[0].features.get("status"),
            Some(FeatureValue::String(s)) if s == "premium"
        ),
        "Should get T2 version of status (premium)"
    );
}

#[tokio::test]
async fn test_dynamic_schema_with_100_columns() {
    // Given: Feature view with 100 columns (stress test)
    let (connector, _temp_dir) = create_test_connector().await;

    let mut features = HashMap::new();
    for i in 0..100 {
        features.insert(format!("feature_{}", i), FeatureValue::Int(i as i64));
    }

    let test_data = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features,
        timestamp: Utc::now(),
    }];

    // When: Write 100 columns
    connector
        .write_features("many_columns", test_data)
        .await
        .unwrap();

    // Then: Read back and verify all 100 columns
    let entities = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let result = connector
        .read_features("many_columns", entities, None)
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].features.len(),
        100,
        "Should preserve all 100 columns"
    );

    // Verify a few columns
    assert!(matches!(
        result[0].features.get("feature_0"),
        Some(FeatureValue::Int(0))
    ));
    assert!(matches!(
        result[0].features.get("feature_50"),
        Some(FeatureValue::Int(50))
    ));
    assert!(matches!(
        result[0].features.get("feature_99"),
        Some(FeatureValue::Int(99))
    ));
}

#[tokio::test]
async fn test_dynamic_schema_multiple_entities() {
    // Given: Multiple entity keys with arbitrary features
    let (connector, _temp_dir) = create_test_connector().await;

    let mut features = HashMap::new();
    features.insert("metric_a".to_string(), FeatureValue::Int(100));
    features.insert("metric_b".to_string(), FeatureValue::Float(3.15)); // Arbitrary test value
    features.insert(
        "label_c".to_string(),
        FeatureValue::String("test".to_string()),
    );

    let test_data = vec![
        FeatureRow {
            entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
            features: features.clone(),
            timestamp: Utc::now(),
        },
        FeatureRow {
            entities: vec![EntityKey::new("user_id".to_string(), "user_2".to_string())],
            features: features.clone(),
            timestamp: Utc::now(),
        },
        FeatureRow {
            entities: vec![EntityKey::new("user_id".to_string(), "user_3".to_string())],
            features,
            timestamp: Utc::now(),
        },
    ];

    // When: Write multiple entities
    connector
        .write_features("multi_entity", test_data)
        .await
        .unwrap();

    // Then: Read each entity and verify schema preserved
    for user_id in &["user_1", "user_2", "user_3"] {
        let entities = vec![EntityKey::new("user_id".to_string(), user_id.to_string())];
        let result = connector
            .read_features("multi_entity", entities, None)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].features.len(), 3);
        assert!(result[0].features.contains_key("metric_a"));
        assert!(result[0].features.contains_key("metric_b"));
        assert!(result[0].features.contains_key("label_c"));
    }
}

#[tokio::test]
async fn test_dynamic_schema_column_name_variations() {
    // Given: Feature columns with various naming conventions
    let (connector, _temp_dir) = create_test_connector().await;

    let mut features = HashMap::new();

    // Various naming conventions
    features.insert("simple".to_string(), FeatureValue::Int(1));
    features.insert("snake_case".to_string(), FeatureValue::Int(2));
    features.insert("camelCase".to_string(), FeatureValue::Int(3));
    features.insert("PascalCase".to_string(), FeatureValue::Int(4));
    features.insert("with_numbers_123".to_string(), FeatureValue::Int(5));
    features.insert("UPPERCASE".to_string(), FeatureValue::Int(6));
    features.insert("lowercase".to_string(), FeatureValue::Int(7));

    let test_data = vec![FeatureRow {
        entities: vec![EntityKey::new("user_id".to_string(), "user_1".to_string())],
        features,
        timestamp: Utc::now(),
    }];

    // When: Write features with various column names
    connector
        .write_features("column_names", test_data)
        .await
        .unwrap();

    // Then: Read back and verify all naming conventions preserved
    let entities = vec![EntityKey::new("user_id".to_string(), "user_1".to_string())];
    let result = connector
        .read_features("column_names", entities, None)
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 7);

    // Verify each naming convention
    assert!(result[0].features.contains_key("simple"));
    assert!(result[0].features.contains_key("snake_case"));
    assert!(result[0].features.contains_key("camelCase"));
    assert!(result[0].features.contains_key("PascalCase"));
    assert!(result[0].features.contains_key("with_numbers_123"));
    assert!(result[0].features.contains_key("UPPERCASE"));
    assert!(result[0].features.contains_key("lowercase"));
}

// ==================== NEW TYPE SUPPORT TESTS ====================

/// Test 8: JSON type support - nested data structures
#[tokio::test]
async fn test_dynamic_schema_json_type() {
    use serde_json::json;

    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Feature with JSON data (user preferences, metadata)
    let entity_key = EntityKey::new("user_id".to_string(), "user_json".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    // Complex nested JSON structure
    row.add_feature(
        "user_preferences".to_string(),
        FeatureValue::Json(json!({
            "theme": "dark",
            "notifications": {
                "email": true,
                "sms": false,
                "push": true
            },
            "language": "en"
        })),
    );

    // Product metadata
    row.add_feature(
        "product_metadata".to_string(),
        FeatureValue::Json(json!({
            "category": "electronics",
            "tags": ["sale", "popular"],
            "rating": 4.5
        })),
    );

    // When: Write and read back
    connector
        .write_features("json_features", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("json_features", vec![entity_key], None)
        .await
        .unwrap();

    // Then: JSON data preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 2);

    match result[0].get_feature("user_preferences") {
        Some(FeatureValue::Json(val)) => {
            assert_eq!(val["theme"], "dark");
            assert_eq!(val["notifications"]["email"], true);
            assert_eq!(val["language"], "en");
        }
        _ => panic!("Expected JSON type for user_preferences"),
    }

    match result[0].get_feature("product_metadata") {
        Some(FeatureValue::Json(val)) => {
            assert_eq!(val["category"], "electronics");
            assert_eq!(val["rating"], 4.5);
        }
        _ => panic!("Expected JSON type for product_metadata"),
    }
}

/// Test 9: ArrayInt type support - integer arrays
#[tokio::test]
async fn test_dynamic_schema_array_int_type() {
    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Features with integer arrays
    let entity_key = EntityKey::new("user_id".to_string(), "user_array_int".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    row.add_feature(
        "purchased_item_ids".to_string(),
        FeatureValue::ArrayInt(vec![1001, 1002, 1003, 1004]),
    );

    row.add_feature(
        "clicked_product_ids".to_string(),
        FeatureValue::ArrayInt(vec![2001, 2002, 2003]),
    );

    row.add_feature(
        "category_ids".to_string(),
        FeatureValue::ArrayInt(vec![10, 20, 30, 40, 50]),
    );

    // When: Write and read back
    connector
        .write_features("array_int_features", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("array_int_features", vec![entity_key], None)
        .await
        .unwrap();

    // Then: Integer arrays preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 3);

    match result[0].get_feature("purchased_item_ids") {
        Some(FeatureValue::ArrayInt(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], 1001);
            assert_eq!(arr[3], 1004);
        }
        _ => panic!("Expected ArrayInt type"),
    }

    match result[0].get_feature("category_ids") {
        Some(FeatureValue::ArrayInt(arr)) => {
            assert_eq!(arr.len(), 5);
            assert_eq!(*arr, vec![10, 20, 30, 40, 50]);
        }
        _ => panic!("Expected ArrayInt type"),
    }
}

/// Test 10: ArrayFloat type support - float arrays (embeddings)
#[tokio::test]
async fn test_dynamic_schema_array_float_type() {
    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Features with float arrays (ML embeddings)
    let entity_key = EntityKey::new("user_id".to_string(), "user_array_float".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    // User embedding (typical ML vector)
    row.add_feature(
        "user_embedding".to_string(),
        FeatureValue::ArrayFloat(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
    );

    // Product scores
    row.add_feature(
        "product_scores".to_string(),
        FeatureValue::ArrayFloat(vec![4.5, 3.2, 4.8, 3.9, 4.1]),
    );

    // When: Write and read back
    connector
        .write_features("array_float_features", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("array_float_features", vec![entity_key], None)
        .await
        .unwrap();

    // Then: Float arrays preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 2);

    match result[0].get_feature("user_embedding") {
        Some(FeatureValue::ArrayFloat(arr)) => {
            assert_eq!(arr.len(), 8);
            assert_eq!(arr[0], 0.1);
            assert_eq!(arr[7], 0.8);
        }
        _ => panic!("Expected ArrayFloat type"),
    }

    match result[0].get_feature("product_scores") {
        Some(FeatureValue::ArrayFloat(arr)) => {
            assert_eq!(arr.len(), 5);
            assert_eq!(arr[0], 4.5);
            assert_eq!(arr[2], 4.8);
        }
        _ => panic!("Expected ArrayFloat type"),
    }
}

/// Test 11: ArrayString type support - string arrays
#[tokio::test]
async fn test_dynamic_schema_array_string_type() {
    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Features with string arrays
    let entity_key = EntityKey::new("user_id".to_string(), "user_array_string".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    row.add_feature(
        "product_tags".to_string(),
        FeatureValue::ArrayString(vec![
            "electronics".to_string(),
            "sale".to_string(),
            "popular".to_string(),
        ]),
    );

    row.add_feature(
        "user_interests".to_string(),
        FeatureValue::ArrayString(vec![
            "technology".to_string(),
            "gaming".to_string(),
            "sports".to_string(),
            "music".to_string(),
        ]),
    );

    row.add_feature(
        "visited_categories".to_string(),
        FeatureValue::ArrayString(vec!["home".to_string(), "garden".to_string()]),
    );

    // When: Write and read back
    connector
        .write_features("array_string_features", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("array_string_features", vec![entity_key], None)
        .await
        .unwrap();

    // Then: String arrays preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 3);

    match result[0].get_feature("product_tags") {
        Some(FeatureValue::ArrayString(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], "electronics");
            assert_eq!(arr[2], "popular");
        }
        _ => panic!("Expected ArrayString type"),
    }

    match result[0].get_feature("user_interests") {
        Some(FeatureValue::ArrayString(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(*arr, vec!["technology", "gaming", "sports", "music"]);
        }
        _ => panic!("Expected ArrayString type"),
    }
}

/// Test 12: Date type support - date-only values
#[tokio::test]
async fn test_dynamic_schema_date_type() {
    use chrono::NaiveDate;

    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Features with date values
    let entity_key = EntityKey::new("user_id".to_string(), "user_dates".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    row.add_feature(
        "birth_date".to_string(),
        FeatureValue::Date(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap()),
    );

    row.add_feature(
        "signup_date".to_string(),
        FeatureValue::Date(NaiveDate::from_ymd_opt(2023, 1, 10).unwrap()),
    );

    row.add_feature(
        "last_purchase_date".to_string(),
        FeatureValue::Date(NaiveDate::from_ymd_opt(2024, 11, 25).unwrap()),
    );

    // When: Write and read back
    connector
        .write_features("date_features", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("date_features", vec![entity_key], None)
        .await
        .unwrap();

    // Then: Dates preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 3);

    match result[0].get_feature("birth_date") {
        Some(FeatureValue::Date(date)) => {
            assert_eq!(date.year(), 1990);
            assert_eq!(date.month(), 5);
            assert_eq!(date.day(), 15);
        }
        _ => panic!("Expected Date type"),
    }

    match result[0].get_feature("signup_date") {
        Some(FeatureValue::Date(date)) => {
            assert_eq!(*date, NaiveDate::from_ymd_opt(2023, 1, 10).unwrap());
        }
        _ => panic!("Expected Date type"),
    }
}

/// Test 13: Mixed types - all types together
#[tokio::test]
async fn test_dynamic_schema_all_types_mixed() {
    use chrono::NaiveDate;
    use serde_json::json;

    // Given: Connector
    let (connector, _temp_dir) = create_test_connector().await;

    // And: Feature row with ALL supported types
    let entity_key = EntityKey::new("user_id".to_string(), "user_all_types".to_string());
    let mut row = FeatureRow::new(vec![entity_key.clone()], Utc::now());

    // Basic types
    row.add_feature("age".to_string(), FeatureValue::Int(30));
    row.add_feature("score".to_string(), FeatureValue::Float(4.5));
    row.add_feature(
        "name".to_string(),
        FeatureValue::String("Alice".to_string()),
    );
    row.add_feature("is_premium".to_string(), FeatureValue::Bool(true));
    row.add_feature("optional_field".to_string(), FeatureValue::Null);

    // Advanced types
    row.add_feature(
        "metadata".to_string(),
        FeatureValue::Json(json!({"key": "value", "count": 42})),
    );
    row.add_feature(
        "item_ids".to_string(),
        FeatureValue::ArrayInt(vec![1, 2, 3]),
    );
    row.add_feature(
        "scores".to_string(),
        FeatureValue::ArrayFloat(vec![1.1, 2.2, 3.3]),
    );
    row.add_feature(
        "tags".to_string(),
        FeatureValue::ArrayString(vec!["tag1".to_string(), "tag2".to_string()]),
    );
    row.add_feature(
        "birth_date".to_string(),
        FeatureValue::Date(NaiveDate::from_ymd_opt(1995, 6, 20).unwrap()),
    );

    // When: Write and read back
    connector
        .write_features("all_types", vec![row])
        .await
        .unwrap();

    let result = connector
        .read_features("all_types", vec![entity_key], None)
        .await
        .unwrap();

    // Then: All types preserved correctly
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].features.len(), 10);

    // Verify each type
    assert!(matches!(
        result[0].get_feature("age"),
        Some(FeatureValue::Int(30))
    ));
    assert!(matches!(
        result[0].get_feature("score"),
        Some(FeatureValue::Float(_))
    ));
    assert!(matches!(
        result[0].get_feature("name"),
        Some(FeatureValue::String(_))
    ));
    assert!(matches!(
        result[0].get_feature("is_premium"),
        Some(FeatureValue::Bool(true))
    ));
    assert!(matches!(
        result[0].get_feature("optional_field"),
        Some(FeatureValue::Null)
    ));
    assert!(matches!(
        result[0].get_feature("metadata"),
        Some(FeatureValue::Json(_))
    ));
    assert!(matches!(
        result[0].get_feature("item_ids"),
        Some(FeatureValue::ArrayInt(_))
    ));
    assert!(matches!(
        result[0].get_feature("scores"),
        Some(FeatureValue::ArrayFloat(_))
    ));
    assert!(matches!(
        result[0].get_feature("tags"),
        Some(FeatureValue::ArrayString(_))
    ));
    assert!(matches!(
        result[0].get_feature("birth_date"),
        Some(FeatureValue::Date(_))
    ));
}
