//! CLI command implementations for FeatureDuck
//!
//! This module provides command-line interface handlers for managing feature views,
//! triggering materialization, and checking run status.

use crate::error::{AppError, Result};
use chrono::Utc;
use featureduck_registry::{FeatureRegistry, FeatureViewDef, RunStatus};
use std::fs;

/// Register a feature view from YAML definition file
///
/// # Arguments
/// * `file_path` - Path to YAML file containing feature view definition
/// * `registry_path` - Path to registry database
pub async fn register(file_path: String, registry_path: String) -> Result<()> {
    println!("📝 Registering feature view from: {}", file_path);

    let yaml_content = fs::read_to_string(&file_path)
        .map_err(|e| AppError::Internal(format!("Failed to read file '{}': {}", file_path, e)))?;

    let view_def: FeatureViewDef = serde_yaml::from_str(&yaml_content)
        .map_err(|e| AppError::Internal(format!("Failed to parse YAML: {}", e)))?;

    let config = featureduck_registry::RegistryConfig::sqlite(&registry_path);
    let registry = FeatureRegistry::new(config)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open registry: {}", e)))?;

    registry
        .register_feature_view(&view_def)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to register feature view: {}", e)))?;

    println!(
        "✅ Feature view '{}' registered successfully",
        view_def.name
    );
    println!("   Version: {}", view_def.version);
    println!("   Entities: {}", view_def.entities.join(", "));
    println!(
        "   Source: {} ({})",
        view_def.source_path, view_def.source_type
    );

    Ok(())
}

/// List all registered feature views
///
/// # Arguments
/// * `registry_path` - Path to registry database
/// * `filter` - Optional name pattern filter
pub async fn list(registry_path: String, filter: Option<String>) -> Result<()> {
    let config = featureduck_registry::RegistryConfig::sqlite(&registry_path);
    let registry = FeatureRegistry::new(config)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open registry: {}", e)))?;

    let views = registry
        .list_feature_views(filter.as_deref())
        .await
        .map_err(|e| AppError::Internal(format!("Failed to list feature views: {}", e)))?;

    if views.is_empty() {
        println!("No feature views found");
        return Ok(());
    }

    println!("📋 Feature Views ({} total):\n", views.len());
    println!(
        "{:<30} {:<8} {:<15} {:<20}",
        "Name", "Version", "Entities", "TTL"
    );
    println!("{}", "-".repeat(80));

    for view in views {
        let entities_str = view.entities.join(", ");
        let entities_display = if entities_str.len() > 14 {
            format!("{}...", &entities_str[..11])
        } else {
            entities_str
        };

        let ttl_display = view
            .ttl_days
            .map(|d| format!("{} days", d))
            .unwrap_or_else(|| "None".to_string());

        println!(
            "{:<30} {:<8} {:<15} {:<20}",
            view.name, view.version, entities_display, ttl_display
        );
    }

    Ok(())
}

/// Trigger materialization for a feature view
///
/// # Arguments
/// * `name` - Feature view name
/// * `registry_path` - Path to registry database
pub async fn materialize(name: String, registry_path: String) -> Result<()> {
    println!("🚀 Triggering materialization for: {}", name);

    let config = featureduck_registry::RegistryConfig::sqlite(&registry_path);
    let registry = FeatureRegistry::new(config)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open registry: {}", e)))?;

    // Get feature view definition (validates it exists)
    let _view = registry
        .get_feature_view(&name)
        .await
        .map_err(|e| AppError::NotFound(format!("Feature view '{}' not found: {}", name, e)))?;

    let run_id = registry
        .create_run(&name)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to create run: {}", e)))?;

    println!("   Run ID: {}", run_id);
    println!("   Status: pending");
    println!("   Started: {}", Utc::now().format("%Y-%m-%d %H:%M:%S"));
    println!("\n📊 Executing materialization...");

    // ⚠️ MIGRATION NOTICE: LogicalPlan-based materialization has been removed
    // The system now uses SQLFrame (PySpark API) for transformations instead
    let error_msg = "LogicalPlan-based materialization is no longer supported. \
                     Please use SQLFrame (PySpark API) for feature transformations. \
                     See documentation for migration guide.";

    let _ = registry
        .update_run_status(run_id, RunStatus::Failed, None, Some(error_msg))
        .await;

    println!("❌ Materialization not supported");
    println!("   Run ID: {} - Status: failed", run_id);
    println!("\n⚠️  MIGRATION REQUIRED:");
    println!("   LogicalPlan-based materialization has been removed in favor of SQLFrame.");
    println!("   Please update your feature views to use SQLFrame (PySpark API).");
    println!("   Contact the development team for migration assistance.");

    Err(AppError::Internal(error_msg.to_string()))
}

/// Check status of a materialization run
///
/// # Arguments
/// * `run_id` - Run ID to check
/// * `registry_path` - Path to registry database
pub async fn status(run_id: i64, registry_path: String) -> Result<()> {
    let config = featureduck_registry::RegistryConfig::sqlite(&registry_path);
    let registry = FeatureRegistry::new(config)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open registry: {}", e)))?;

    let all_views = registry
        .list_feature_views(None)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to list views: {}", e)))?;

    for view in all_views {
        let runs = registry
            .get_recent_runs(&view.name, 100)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to get runs: {}", e)))?;

        if let Some(run) = runs.into_iter().find(|r| r.id == run_id) {
            println!("📊 Run Status");
            println!("   ID: {}", run.id);
            println!("   Feature View: {}", run.feature_view_name);
            println!("   Status: {}", run.status);

            if let Some(started) = run.started_at {
                println!("   Started: {}", started.format("%Y-%m-%d %H:%M:%S"));
            }

            if let Some(completed) = run.completed_at {
                println!("   Completed: {}", completed.format("%Y-%m-%d %H:%M:%S"));

                if let Some(started) = run.started_at {
                    let duration = completed.signed_duration_since(started);
                    println!("   Duration: {}s", duration.num_seconds());
                }
            }

            if let Some(rows) = run.rows_processed {
                println!("   Rows Processed: {}", rows);
            }

            if let Some(error) = run.error_message {
                println!("   Error: {}", error);
            }

            return Ok(());
        }
    }

    Err(AppError::NotFound(format!("Run {} not found", run_id)))
}

/// Show materialization history for a feature view
///
/// # Arguments
/// * `name` - Feature view name
/// * `registry_path` - Path to registry database
/// * `limit` - Number of runs to show
pub async fn history(name: String, registry_path: String, limit: usize) -> Result<()> {
    let config = featureduck_registry::RegistryConfig::sqlite(&registry_path);
    let registry = FeatureRegistry::new(config)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open registry: {}", e)))?;

    let runs = registry
        .get_recent_runs(&name, limit)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to get runs: {}", e)))?;

    if runs.is_empty() {
        println!("No materialization runs found for '{}'", name);
        return Ok(());
    }

    println!(
        "📊 Materialization History for '{}' ({} runs):\n",
        name,
        runs.len()
    );
    println!(
        "{:<8} {:<12} {:<20} {:<20} {:<10}",
        "Run ID", "Status", "Started", "Completed", "Rows"
    );
    println!("{}", "-".repeat(80));

    for run in runs {
        let started = run
            .started_at
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "N/A".to_string());

        let completed = run
            .completed_at
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "In progress".to_string());

        let rows = run
            .rows_processed
            .map(|r| r.to_string())
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{:<8} {:<12} {:<20} {:<20} {:<10}",
            run.id, run.status, started, completed, rows
        );

        if let Some(error) = run.error_message {
            println!("         Error: {}", error);
        }
    }

    Ok(())
}

/// Validate a feature view YAML definition
///
/// # Arguments
/// * `file_path` - Path to YAML file
pub fn validate(file_path: String) -> Result<()> {
    println!("🔍 Validating feature view definition: {}", file_path);

    let yaml_content = fs::read_to_string(&file_path)
        .map_err(|e| AppError::Internal(format!("Failed to read file: {}", e)))?;

    let view_def: FeatureViewDef = serde_yaml::from_str(&yaml_content)
        .map_err(|e| AppError::Internal(format!("Failed to parse YAML: {}", e)))?;

    println!("✅ YAML syntax is valid\n");
    println!("Feature View Details:");
    println!("   Name: {}", view_def.name);
    println!("   Version: {}", view_def.version);
    println!("   Source Type: {}", view_def.source_type);
    println!("   Source Path: {}", view_def.source_path);
    println!("   Entities: {}", view_def.entities.join(", "));

    if let Some(ttl) = view_def.ttl_days {
        println!("   TTL: {} days", ttl);
    }

    if let Some(schedule) = view_def.batch_schedule {
        println!("   Schedule: {}", schedule);
    }

    if let Some(desc) = view_def.description {
        println!("   Description: {}", desc);
    }

    if !view_def.tags.is_empty() {
        println!("   Tags: {}", view_def.tags.join(", "));
    }

    // NOTE: LogicalPlan validation removed (deprecated in favor of SQLFrame)
    // Production SDK uses SQLFrame-generated SQL, not LogicalPlan JSON
    // The transformations field may contain raw SQL or other formats

    println!("\n✅ Validation complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_register_invalid_file() {
        let temp_dir = TempDir::new().unwrap();
        let registry_path = temp_dir.path().join("test.db");

        let result = register(
            "/nonexistent/file.yaml".to_string(),
            registry_path.to_str().unwrap().to_string(),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_empty_registry() {
        let temp_dir = TempDir::new().unwrap();
        let registry_path = temp_dir.path().join("test.db");

        let result = list(registry_path.to_str().unwrap().to_string(), None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_status_nonexistent_run() {
        let temp_dir = TempDir::new().unwrap();
        let registry_path = temp_dir.path().join("test.db");

        let result = status(999, registry_path.to_str().unwrap().to_string()).await;

        assert!(result.is_err());
    }
}
