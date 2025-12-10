use chrono::{DateTime, Duration, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn generate_small_dataset(num_rows: usize, base_time: DateTime<Utc>) -> Vec<FeatureRow> {
    (0..num_rows)
        .map(|i| {
            FeatureRow::new(
                vec![EntityKey::new("user_id", format!("user_{}", i))],
                base_time + Duration::hours(i as i64),
            )
            .with_feature("clicks_7d", FeatureValue::Int((i * 10) as i64))
            .with_feature("score", FeatureValue::Float(i as f64))
        })
        .collect()
}

fn bench_small_dataset_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup: Small dataset (1000 rows)
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = rt.block_on(async {
        DeltaStorageConnector::new(
            temp_dir.path().to_str().unwrap(),
            DuckDBEngineConfig::default(),
        )
        .await
        .expect("Failed to create connector")
    });

    let base_time = Utc::now();
    let rows = generate_small_dataset(1000, base_time);

    rt.block_on(async {
        connector
            .write_features("bench_test", rows)
            .await
            .expect("Failed to write features")
    });

    let query_time = base_time + Duration::hours(500);
    let keys = vec![EntityKey::new("user_id", "user_500")];

    c.bench_function("query_1000_rows_single_user", |b| {
        b.to_async(&rt).iter(|| async {
            connector
                .read_features(
                    black_box("bench_test"),
                    black_box(keys.clone()),
                    black_box(Some(query_time)),
                )
                .await
                .expect("Query failed")
        });
    });
}

criterion_group!(benches, bench_small_dataset_query);
criterion_main!(benches);
