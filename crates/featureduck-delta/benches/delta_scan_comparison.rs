//! Benchmark comparing delta_scan() vs manual file listing
//!
//! Run with: cargo bench --bench delta_scan_comparison

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use tempfile::TempDir;

async fn setup_test_data() -> (DeltaStorageConnector, TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .unwrap();

    // Create test data with 1000 rows across 100 entities
    let mut rows = Vec::new();
    for entity_id in 0..100 {
        for version in 0..10 {
            let entity_key = EntityKey::new("user_id".to_string(), format!("user_{}", entity_id));
            let timestamp = chrono::Utc::now() + chrono::Duration::seconds(version);
            let mut row = FeatureRow::new(vec![entity_key], timestamp);
            row.add_feature("clicks".to_string(), FeatureValue::Int(version * 10));
            row.add_feature("purchases".to_string(), FeatureValue::Int(version));
            rows.push(row);
        }
    }

    connector
        .write_features("bench_features", rows)
        .await
        .unwrap();

    (connector, temp_dir, "bench_features".to_string())
}

fn bench_delta_scan(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("delta_scan_read_features", |b| {
        b.to_async(&runtime).iter(|| async {
            let (connector, _temp_dir, table_name) = setup_test_data().await;

            // Query 10 entities
            let entity_keys: Vec<EntityKey> = (0..10)
                .map(|i| EntityKey::new("user_id".to_string(), format!("user_{}", i)))
                .collect();

            let result = connector
                .read_features(&table_name, entity_keys, None)
                .await;

            black_box(result.unwrap());
        });
    });
}

criterion_group!(benches, bench_delta_scan);
criterion_main!(benches);
