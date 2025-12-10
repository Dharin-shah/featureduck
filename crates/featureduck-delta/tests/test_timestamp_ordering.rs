use chrono::{Duration, Utc};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use tempfile::TempDir;

#[tokio::test]
async fn test_latest_row_query() {
    let temp_dir = TempDir::new().unwrap();
    let connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .unwrap();

    let base_time = Utc::now();
    let jan_1 = base_time;
    let jan_10 = base_time + Duration::days(10);
    let jan_20 = base_time + Duration::days(20);

    println!("\nTimestamps:");
    println!(
        "  Jan 1:  {:?} (micros: {})",
        jan_1,
        jan_1.timestamp_micros()
    );
    println!(
        "  Jan 10: {:?} (micros: {})",
        jan_10,
        jan_10.timestamp_micros()
    );
    println!(
        "  Jan 20: {:?} (micros: {})",
        jan_20,
        jan_20.timestamp_micros()
    );

    // Write 3 versions of Alice's data
    let alice_v1 = FeatureRow::new(vec![EntityKey::new("user_id", "alice")], jan_1)
        .with_feature("score", FeatureValue::Int(1));

    let alice_v2 = FeatureRow::new(vec![EntityKey::new("user_id", "alice")], jan_10)
        .with_feature("score", FeatureValue::Int(2));

    let alice_v3 = FeatureRow::new(vec![EntityKey::new("user_id", "alice")], jan_20)
        .with_feature("score", FeatureValue::Int(3));

    connector
        .write_features("test", vec![alice_v1])
        .await
        .unwrap();
    connector
        .write_features("test", vec![alice_v2])
        .await
        .unwrap();
    connector
        .write_features("test", vec![alice_v3])
        .await
        .unwrap();

    // Query without as_of (should get latest = jan_20, score=3)
    let result = connector
        .read_features("test", vec![EntityKey::new("user_id", "alice")], None)
        .await
        .unwrap();

    println!("\nResult:");
    println!("  Count: {}", result.len());
    println!("  Score: {:?}", result[0].get_feature("score"));
    println!(
        "  Timestamp: {:?} (micros: {})",
        result[0].timestamp,
        result[0].timestamp.timestamp_micros()
    );
    println!(
        "  Expected: {:?} (micros: {})",
        jan_20,
        jan_20.timestamp_micros()
    );

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get_feature("score"),
        Some(&FeatureValue::Int(3)),
        "Should return score=3 (latest)"
    );
    assert_eq!(
        result[0].timestamp, jan_20,
        "Should return Jan 20 timestamp (latest)"
    );
}
