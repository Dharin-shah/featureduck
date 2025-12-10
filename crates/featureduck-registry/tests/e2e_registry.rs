//! End-to-end integration tests for the feature registry
//!
//! These tests validate complete workflows from registration to materialization tracking.

use chrono::Utc;
use featureduck_registry::{FeatureRegistry, FeatureViewDef, RegistryConfig, RunStatus};
use tempfile::TempDir;

fn create_test_view(name: &str, version: i32) -> FeatureViewDef {
    FeatureViewDef {
        name: name.to_string(),
        version,
        source_type: "delta".to_string(),
        source_path: "s3://bucket/events".to_string(),
        entities: vec!["user_id".to_string(), "product_id".to_string()],
        transformations: r#"{"filters": [], "aggregations": []}"#.to_string(),
        timestamp_field: Some("event_timestamp".to_string()),
        ttl_days: Some(30),
        batch_schedule: Some("0 * * * *".to_string()),
        description: Some(format!("Test feature view: {}", name)),
        tags: vec!["test".to_string(), "e2e".to_string()],
        owner: Some("data-science-team".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn test_e2e_feature_lifecycle() {
    // Given: A new registry with a feature view
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("registry.db");
    let config = RegistryConfig::SQLite {
        path: db_path.to_string_lossy().to_string(),
    };
    let registry = FeatureRegistry::new(config).await.unwrap();

    let view = create_test_view("user_purchase_features", 1);

    // When: Register feature view
    registry.register_feature_view(&view).await.unwrap();

    // Then: Can retrieve the feature view
    let retrieved = registry
        .get_feature_view("user_purchase_features")
        .await
        .unwrap();
    assert_eq!(retrieved.name, "user_purchase_features");
    assert_eq!(retrieved.version, 1);
    assert_eq!(retrieved.entities, vec!["user_id", "product_id"]);
    assert_eq!(retrieved.source_type, "delta");
    assert_eq!(retrieved.ttl_days, Some(30));

    // When: List all feature views
    let views = registry.list_feature_views(None).await.unwrap();

    // Then: Should contain our view
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].name, "user_purchase_features");

    // When: Update the feature view (new version)
    let updated_view = FeatureViewDef {
        version: 2,
        ttl_days: Some(60),
        ..view.clone()
    };
    registry.register_feature_view(&updated_view).await.unwrap();

    // Then: Should retrieve the latest version
    let latest = registry
        .get_feature_view("user_purchase_features")
        .await
        .unwrap();
    assert_eq!(latest.version, 2);
    assert_eq!(latest.ttl_days, Some(60));

    // When: Delete the feature view
    registry
        .delete_feature_view("user_purchase_features")
        .await
        .unwrap();

    // Then: Should no longer exist
    let result = registry.get_feature_view("user_purchase_features").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_e2e_materialization_workflow() {
    // Given: A registry with a feature view
    let registry = FeatureRegistry::in_memory().await.unwrap();
    let view = create_test_view("daily_user_metrics", 1);
    registry.register_feature_view(&view).await.unwrap();

    // When: Start a materialization run
    let run_id = registry.create_run("daily_user_metrics").await.unwrap();
    assert_eq!(run_id, 1);

    // Then: Run should be in pending state
    let runs = registry
        .get_recent_runs("daily_user_metrics", 10)
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, run_id);
    assert!(matches!(runs[0].status, RunStatus::Pending));

    // When: Update to running
    registry
        .update_run_status(run_id, RunStatus::Running, None, None)
        .await
        .unwrap();

    // Then: Status should be running
    let runs = registry
        .get_recent_runs("daily_user_metrics", 10)
        .await
        .unwrap();
    assert!(matches!(runs[0].status, RunStatus::Running));

    // When: Update progress
    registry
        .update_run_status(run_id, RunStatus::Running, Some(5000), None)
        .await
        .unwrap();

    // Then: Should track progress
    let runs = registry
        .get_recent_runs("daily_user_metrics", 10)
        .await
        .unwrap();
    assert_eq!(runs[0].rows_processed, Some(5000));

    // When: Complete successfully
    registry
        .update_run_status(run_id, RunStatus::Success, Some(10000), None)
        .await
        .unwrap();

    // Then: Should be successful with final count
    let runs = registry
        .get_recent_runs("daily_user_metrics", 10)
        .await
        .unwrap();
    assert!(matches!(runs[0].status, RunStatus::Success));
    assert_eq!(runs[0].rows_processed, Some(10000));
    assert!(runs[0].completed_at.is_some());
}

#[tokio::test]
async fn test_e2e_multiple_feature_views_and_runs() {
    // Given: A registry with multiple feature views
    let registry = FeatureRegistry::in_memory().await.unwrap();

    let views = vec![
        create_test_view("user_7d_features", 1),
        create_test_view("user_30d_features", 1),
        create_test_view("product_features", 1),
    ];

    for view in &views {
        registry.register_feature_view(view).await.unwrap();
    }

    // When: Create multiple runs for each feature view
    let mut run_ids = Vec::new();

    for view in &views {
        for _ in 0..3 {
            let run_id = registry.create_run(&view.name).await.unwrap();
            run_ids.push((view.name.clone(), run_id));
        }
    }

    // Then: Should have 9 total runs (3 views × 3 runs each)
    assert_eq!(run_ids.len(), 9);

    // When: Update runs with different statuses
    for (i, (_view_name, run_id)) in run_ids.iter().enumerate() {
        let status = match i % 3 {
            0 => RunStatus::Success,
            1 => RunStatus::Failed,
            _ => RunStatus::Running,
        };

        let error = if status == RunStatus::Failed {
            Some("Connection timeout")
        } else {
            None
        };

        registry
            .update_run_status(*run_id, status, Some((i as i64 + 1) * 1000), error)
            .await
            .unwrap();
    }

    // Then: Each feature view should have 3 runs
    for view in &views {
        let runs = registry.get_recent_runs(&view.name, 10).await.unwrap();
        assert_eq!(runs.len(), 3);

        // Verify runs are in descending order by started_at
        for i in 1..runs.len() {
            assert!(runs[i - 1].started_at >= runs[i].started_at);
        }
    }

    // When: List all feature views
    let all_views = registry.list_feature_views(None).await.unwrap();

    // Then: Should have all 3 feature views
    assert_eq!(all_views.len(), 3);

    // When: Filter by name pattern
    let user_views = registry.list_feature_views(Some("user")).await.unwrap();

    // Then: Should only return user feature views
    assert_eq!(user_views.len(), 2);
    for view in user_views {
        assert!(view.name.contains("user"));
    }
}

#[tokio::test]
async fn test_e2e_persistence() {
    // Given: A temporary database file
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persistent_registry.db");

    // When: Create registry, add data, and drop it
    {
        let config = RegistryConfig::SQLite {
            path: db_path.to_string_lossy().to_string(),
        };
        let registry = FeatureRegistry::new(config).await.unwrap();
        let view = create_test_view("persistent_features", 1);
        registry.register_feature_view(&view).await.unwrap();

        let run_id = registry.create_run("persistent_features").await.unwrap();
        registry
            .update_run_status(run_id, RunStatus::Success, Some(5000), None)
            .await
            .unwrap();
    }

    // Then: Data should persist after reopening
    {
        let config = RegistryConfig::SQLite {
            path: db_path.to_string_lossy().to_string(),
        };
        let registry = FeatureRegistry::new(config).await.unwrap();

        let view = registry
            .get_feature_view("persistent_features")
            .await
            .unwrap();
        assert_eq!(view.name, "persistent_features");

        let runs = registry
            .get_recent_runs("persistent_features", 10)
            .await
            .unwrap();
        assert_eq!(runs.len(), 1);
        assert!(matches!(runs[0].status, RunStatus::Success));
        assert_eq!(runs[0].rows_processed, Some(5000));
    }
}

#[tokio::test]
async fn test_e2e_error_handling() {
    // Given: A registry
    let registry = FeatureRegistry::in_memory().await.unwrap();

    // When: Try to get non-existent feature view
    let result = registry.get_feature_view("nonexistent").await;

    // Then: Should return error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    // When: Try to update non-existent run
    let result = registry
        .update_run_status(999, RunStatus::Success, None, None)
        .await;

    // Then: Should return error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    // When: Try to get runs for non-existent feature view
    let runs = registry.get_recent_runs("nonexistent", 10).await.unwrap();

    // Then: Should return empty list (not an error)
    assert_eq!(runs.len(), 0);
}
