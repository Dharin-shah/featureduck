use chrono::{DateTime, Duration, Utc};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use featureduck_core::{EntityKey, FeatureRow, FeatureValue, StorageConnector};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn generate_feature_data(
    num_users: usize,
    updates_per_user: usize,
    base_time: DateTime<Utc>,
) -> Vec<FeatureRow> {
    let mut rows = Vec::new();

    for user_idx in 0..num_users {
        let user_id = format!("user_{}", user_idx);

        for update_idx in 0..updates_per_user {
            let timestamp = base_time + Duration::hours((update_idx * 24) as i64);

            let row = FeatureRow::new(vec![EntityKey::new("user_id", &user_id)], timestamp)
                .with_feature("clicks_7d", FeatureValue::Int((update_idx * 10) as i64))
                .with_feature("purchases_7d", FeatureValue::Int(update_idx as i64))
                .with_feature("score", FeatureValue::Float((update_idx as f64) * 1.5))
                .with_feature("value", FeatureValue::Int((user_idx * update_idx) as i64));

            rows.push(row);
        }
    }

    rows
}

async fn setup_test_data(
    num_users: usize,
    updates_per_user: usize,
) -> (DeltaStorageConnector, TempDir, DateTime<Utc>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let connector = DeltaStorageConnector::new(
        temp_dir.path().to_str().unwrap(),
        DuckDBEngineConfig::default(),
    )
    .await
    .expect("Failed to create connector");

    let base_time = Utc::now();
    let rows = generate_feature_data(num_users, updates_per_user, base_time);

    connector
        .write_features("benchmark_features", rows)
        .await
        .expect("Failed to write features");

    (connector, temp_dir, base_time)
}

fn bench_point_in_time_query_varying_users(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup: 10K users × 100 updates each = 1M rows total
    let (connector, _temp_dir, base_time) = rt.block_on(setup_test_data(10_000, 100));

    // Query time: Middle of the timeline (should get 50th update)
    let query_time = base_time + Duration::hours(50 * 24);

    let mut group = c.benchmark_group("point_in_time_varying_users");

    for num_query_users in [1, 10, 100, 1000].iter() {
        let keys: Vec<EntityKey> = (0..*num_query_users)
            .map(|i| EntityKey::new("user_id", format!("user_{}", i)))
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_query_users),
            num_query_users,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    connector
                        .read_features(
                            black_box("benchmark_features"),
                            black_box(keys.clone()),
                            black_box(Some(query_time)),
                        )
                        .await
                        .expect("Query failed")
                });
            },
        );
    }

    group.finish();
}

fn bench_point_in_time_query_varying_dataset_size(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("point_in_time_varying_dataset_size");

    // Test different dataset sizes
    for (num_users, updates_per_user) in [
        (100, 100),     // 10K rows
        (1_000, 100),   // 100K rows
        (10_000, 100),  // 1M rows
        (10_000, 1000), // 10M rows (WARNING: This will take time!)
    ]
    .iter()
    {
        let total_rows = num_users * updates_per_user;
        let (connector, _temp_dir, base_time) =
            rt.block_on(setup_test_data(*num_users, *updates_per_user));
        let query_time = base_time + Duration::hours((updates_per_user / 2 * 24) as i64);

        let keys = vec![EntityKey::new("user_id", "user_0")];

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}rows", total_rows)),
            &total_rows,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    connector
                        .read_features(
                            black_box("benchmark_features"),
                            black_box(keys.clone()),
                            black_box(Some(query_time)),
                        )
                        .await
                        .expect("Query failed")
                });
            },
        );
    }

    group.finish();
}

fn bench_duckdb_qualify_optimization(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup: 1K users × 1K updates = 1M rows
    let (connector, _temp_dir, base_time) = rt.block_on(setup_test_data(1_000, 1_000));

    let query_time = base_time + Duration::hours(500 * 24);
    let keys = vec![EntityKey::new("user_id", "user_500")];

    c.bench_function("duckdb_qualify_1M_rows", |b| {
        b.to_async(&rt).iter(|| async {
            connector
                .read_features(
                    black_box("benchmark_features"),
                    black_box(keys.clone()),
                    black_box(Some(query_time)),
                )
                .await
                .expect("Query failed")
        });
    });
}

criterion_group!(
    benches,
    bench_point_in_time_query_varying_users,
    bench_point_in_time_query_varying_dataset_size,
    bench_duckdb_qualify_optimization,
);
criterion_main!(benches);
