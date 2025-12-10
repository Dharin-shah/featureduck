//! Register feature view from YAML file command

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Deserialize;

use crate::config::CliConfig;
use crate::output::{print_error, print_info, print_success};
use featureduck_registry::FeatureViewDef;

use super::create_registry;

/// Feature view definition in YAML format
#[derive(Debug, Deserialize)]
struct FeatureViewYaml {
    name: String,
    #[serde(default = "default_version")]
    version: i32,
    source_type: String,
    source_path: String,
    entities: Vec<String>,
    #[serde(default)]
    transformations: String,
    timestamp_field: Option<String>,
    ttl_days: Option<i32>,
    batch_schedule: Option<String>,
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    owner: Option<String>,
}

fn default_version() -> i32 {
    1
}

pub async fn run(config: &CliConfig, file: &str, force: bool) -> Result<()> {
    let registry = create_registry(config).await?;

    // Read YAML file
    let content =
        std::fs::read_to_string(file).with_context(|| format!("Failed to read file: {}", file))?;

    // Parse YAML
    let yaml_def: FeatureViewYaml = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML from: {}", file))?;

    print_info(&format!("Registering feature view: {}", yaml_def.name));

    // Check if already exists
    let existing = registry.get_feature_view(&yaml_def.name).await;
    if existing.is_ok() && !force {
        print_error(&format!(
            "Feature view '{}' already exists. Use --force to overwrite.",
            yaml_def.name
        ));
        return Ok(());
    }

    let now = Utc::now();

    // Determine version
    let version = if let Ok(ref existing) = existing {
        if force {
            existing.version + 1
        } else {
            yaml_def.version
        }
    } else {
        yaml_def.version
    };

    // Convert to registry type
    let view_def = FeatureViewDef {
        name: yaml_def.name.clone(),
        version,
        source_type: yaml_def.source_type,
        source_path: yaml_def.source_path,
        entities: yaml_def.entities,
        transformations: if yaml_def.transformations.is_empty() {
            "{}".to_string()
        } else {
            yaml_def.transformations
        },
        timestamp_field: yaml_def.timestamp_field,
        ttl_days: yaml_def.ttl_days,
        batch_schedule: yaml_def.batch_schedule,
        description: yaml_def.description,
        tags: yaml_def.tags,
        owner: yaml_def.owner,
        created_at: existing.as_ref().map(|e| e.created_at).unwrap_or(now),
        updated_at: now,
    };

    // Register
    registry
        .register_feature_view(&view_def)
        .await
        .context("Failed to register feature view")?;

    if existing.is_ok() {
        print_success(&format!(
            "Updated feature view '{}' to version {}",
            yaml_def.name, version
        ));
    } else {
        print_success(&format!(
            "Registered feature view '{}' version {}",
            yaml_def.name, version
        ));
    }

    Ok(())
}
