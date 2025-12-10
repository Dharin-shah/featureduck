//! Validate feature view configuration command

use anyhow::{Context, Result};
use std::path::Path;

use crate::config::CliConfig;
use crate::output::{print_error, print_info, print_success, print_warning};

use super::create_registry;

// Removed unused import: featureduck_delta::DeltaStorageConnector

pub async fn run(
    config: &CliConfig,
    name: &str,
    check_source: bool,
    check_storage: bool,
    check_schema: bool,
) -> Result<()> {
    let registry = create_registry(config).await?;

    // Handle 'all' case
    let views = if name == "all" {
        registry.list_feature_views(None).await?
    } else {
        vec![registry
            .get_feature_view(name)
            .await
            .with_context(|| format!("Feature view '{}' not found", name))?]
    };

    if views.is_empty() {
        print_warning("No feature views found to validate");
        return Ok(());
    }

    print_info(&format!("Validating {} feature view(s)...", views.len()));
    println!();

    let mut total_checks = 0;
    let mut passed_checks = 0;
    let mut failed_checks = 0;
    let mut warnings = 0;

    for view in &views {
        println!("Feature View: {}", view.name);
        println!("{}", "-".repeat(50));

        // Basic validation (always run)
        total_checks += 1;
        if validate_basic(view).is_ok() {
            print_success("Basic configuration valid");
            passed_checks += 1;
        } else {
            print_error("Basic configuration invalid");
            failed_checks += 1;
        }

        // Source validation
        if check_source {
            total_checks += 1;
            match validate_source(view).await {
                Ok(()) => {
                    print_success("Source accessible");
                    passed_checks += 1;
                }
                Err(e) => {
                    print_error(&format!("Source check failed: {}", e));
                    failed_checks += 1;
                }
            }
        }

        // Storage validation
        if check_storage {
            total_checks += 1;
            match validate_storage(config, &view.name).await {
                Ok(exists) => {
                    if exists {
                        print_success("Storage path exists and is accessible");
                    } else {
                        print_warning(
                            "Storage path does not exist yet (will be created on first write)",
                        );
                        warnings += 1;
                    }
                    passed_checks += 1;
                }
                Err(e) => {
                    print_error(&format!("Storage check failed: {}", e));
                    failed_checks += 1;
                }
            }
        }

        // Schema validation
        if check_schema {
            total_checks += 1;
            match validate_schema(view) {
                Ok(()) => {
                    print_success("Schema configuration valid");
                    passed_checks += 1;
                }
                Err(e) => {
                    print_error(&format!("Schema check failed: {}", e));
                    failed_checks += 1;
                }
            }
        }

        // Additional checks
        if let Some(schedule) = &view.batch_schedule {
            if !is_valid_cron(schedule) {
                print_warning(&format!(
                    "Schedule '{}' may not be valid cron syntax",
                    schedule
                ));
                warnings += 1;
            }
        }

        if view.ttl_days.is_none() {
            print_warning("No TTL set - data will be retained indefinitely");
            warnings += 1;
        }

        println!();
    }

    // Summary
    println!("{}", "=".repeat(50));
    println!("Validation Summary:");
    println!("  Total checks: {}", total_checks);
    println!("  Passed: {}", passed_checks);
    println!("  Failed: {}", failed_checks);
    println!("  Warnings: {}", warnings);

    if failed_checks > 0 {
        print_error(&format!("{} validation check(s) failed", failed_checks));
        std::process::exit(1);
    } else if warnings > 0 {
        print_warning(&format!("All checks passed with {} warning(s)", warnings));
    } else {
        print_success("All validation checks passed!");
    }

    Ok(())
}

/// Basic configuration validation
fn validate_basic(view: &featureduck_registry::FeatureViewDef) -> Result<()> {
    // Name validation
    if view.name.is_empty() {
        anyhow::bail!("Feature view name cannot be empty");
    }

    if !view
        .name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        anyhow::bail!(
            "Feature view name must contain only alphanumeric characters, underscores, or hyphens"
        );
    }

    // Entity validation
    if view.entities.is_empty() {
        anyhow::bail!("At least one entity is required");
    }

    // Source validation
    if view.source_path.is_empty() {
        anyhow::bail!("Source path cannot be empty");
    }

    // Version validation
    if view.version < 1 {
        anyhow::bail!("Version must be >= 1");
    }

    Ok(())
}

/// Source accessibility validation
async fn validate_source(view: &featureduck_registry::FeatureViewDef) -> Result<()> {
    match view.source_type.as_str() {
        "delta" => {
            // Check if Delta table exists
            let path = Path::new(&view.source_path);
            if path.exists() {
                // Check for _delta_log directory
                let delta_log = path.join("_delta_log");
                if delta_log.exists() && delta_log.is_dir() {
                    Ok(())
                } else {
                    anyhow::bail!("Path exists but is not a Delta table (missing _delta_log)")
                }
            } else if view.source_path.starts_with("s3://")
                || view.source_path.starts_with("gs://")
                || view.source_path.starts_with("az://")
            {
                // Cloud storage - we can't easily validate without credentials
                // Just check URL format
                Ok(())
            } else {
                anyhow::bail!("Source path does not exist: {}", view.source_path)
            }
        }
        "parquet" => {
            let path = Path::new(&view.source_path);
            if path.exists()
                || view.source_path.starts_with("s3://")
                || view.source_path.starts_with("gs://")
            {
                Ok(())
            } else {
                anyhow::bail!("Parquet source path does not exist: {}", view.source_path)
            }
        }
        _ => {
            // Unknown source type - warn but don't fail
            tracing::warn!("Unknown source type: {}", view.source_type);
            Ok(())
        }
    }
}

/// Storage accessibility validation
async fn validate_storage(config: &CliConfig, view_name: &str) -> Result<bool> {
    let storage_path = format!("{}/{}", config.storage.base_path, view_name);
    let path = Path::new(&storage_path);

    if path.exists() {
        // Check if it's writable
        let test_file = path.join(".write_test");
        match std::fs::write(&test_file, "test") {
            Ok(()) => {
                let _ = std::fs::remove_file(test_file);
                Ok(true)
            }
            Err(e) => anyhow::bail!("Storage path exists but is not writable: {}", e),
        }
    } else {
        // Path doesn't exist - that's OK, it will be created
        Ok(false)
    }
}

/// Schema validation
fn validate_schema(view: &featureduck_registry::FeatureViewDef) -> Result<()> {
    // Try to parse transformations as JSON
    if !view.transformations.is_empty() && view.transformations != "{}" {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&view.transformations);
        if parsed.is_err() {
            anyhow::bail!(
                "Transformations field is not valid JSON: {}",
                view.transformations
            );
        }
    }

    // Validate timestamp field if set
    if let Some(ref ts_field) = view.timestamp_field {
        if ts_field.is_empty() {
            anyhow::bail!("Timestamp field cannot be empty if set");
        }
    }

    Ok(())
}

/// Basic cron syntax validation
fn is_valid_cron(schedule: &str) -> bool {
    let parts: Vec<&str> = schedule.split_whitespace().collect();
    // Standard cron has 5 fields, some systems use 6 (with seconds)
    parts.len() == 5 || parts.len() == 6
}
