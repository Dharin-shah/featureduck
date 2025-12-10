//! Show feature view details command

use anyhow::Result;
use serde::Serialize;

use crate::config::CliConfig;
use crate::output::{
    create_table, format_bytes, format_count, print_error, print_info, print_output, OutputFormat,
};

use super::create_registry;

#[derive(Debug, Serialize)]
struct FeatureViewDetails {
    name: String,
    version: i32,
    source_type: String,
    source_path: String,
    entities: Vec<String>,
    timestamp_field: Option<String>,
    ttl_days: Option<i32>,
    batch_schedule: Option<String>,
    description: Option<String>,
    tags: Vec<String>,
    owner: Option<String>,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<FeatureViewStatsInfo>,
}

#[derive(Debug, Serialize)]
struct FeatureViewStatsInfo {
    row_count: i64,
    distinct_entities: i64,
    total_size_bytes: i64,
    total_size_human: String,
    stats_updated_at: String,
}

pub async fn run(
    config: &CliConfig,
    name: &str,
    _show_schema: bool,
    show_stats: bool,
    output_format: &str,
) -> Result<()> {
    let registry = create_registry(config).await?;

    let view = match registry.get_feature_view(name).await {
        Ok(v) => v,
        Err(e) => {
            print_error(&format!("Feature view '{}' not found: {}", name, e));
            return Ok(());
        }
    };

    let stats = if show_stats {
        registry
            .get_latest_stats(name)
            .await
            .ok()
            .flatten()
            .map(|s| FeatureViewStatsInfo {
                row_count: s.row_count,
                distinct_entities: s.distinct_entities,
                total_size_bytes: s.total_size_bytes,
                total_size_human: format_bytes(s.total_size_bytes),
                stats_updated_at: s.created_at.to_rfc3339(),
            })
    } else {
        None
    };

    let details = FeatureViewDetails {
        name: view.name.clone(),
        version: view.version,
        source_type: view.source_type.clone(),
        source_path: view.source_path.clone(),
        entities: view.entities.clone(),
        timestamp_field: view.timestamp_field.clone(),
        ttl_days: view.ttl_days,
        batch_schedule: view.batch_schedule.clone(),
        description: view.description.clone(),
        tags: view.tags.clone(),
        owner: view.owner.clone(),
        created_at: view.created_at.to_rfc3339(),
        updated_at: view.updated_at.to_rfc3339(),
        stats,
    };

    match OutputFormat::parse(output_format) {
        OutputFormat::Table => {
            println!("Feature View: {}", view.name);
            println!("{}", "=".repeat(50));

            let mut table = create_table();
            table.set_header(vec!["Property", "Value"]);

            table.add_row(vec!["Name", &details.name]);
            table.add_row(vec!["Version", &details.version.to_string()]);
            table.add_row(vec!["Source Type", &details.source_type]);
            table.add_row(vec!["Source Path", &details.source_path]);
            table.add_row(vec!["Entities", &details.entities.join(", ")]);
            table.add_row(vec![
                "Timestamp Field",
                details.timestamp_field.as_deref().unwrap_or("-"),
            ]);
            table.add_row(vec![
                "TTL",
                &details
                    .ttl_days
                    .map(|d| format!("{} days", d))
                    .unwrap_or_else(|| "-".to_string()),
            ]);
            table.add_row(vec![
                "Schedule",
                details.batch_schedule.as_deref().unwrap_or("-"),
            ]);
            table.add_row(vec![
                "Description",
                details.description.as_deref().unwrap_or("-"),
            ]);
            table.add_row(vec![
                "Tags",
                &if details.tags.is_empty() {
                    "-".to_string()
                } else {
                    details.tags.join(", ")
                },
            ]);
            table.add_row(vec!["Owner", details.owner.as_deref().unwrap_or("-")]);
            table.add_row(vec!["Created", &details.created_at]);
            table.add_row(vec!["Updated", &details.updated_at]);

            println!("{table}");

            if let Some(ref stats) = details.stats {
                println!("\nStatistics:");
                println!("{}", "-".repeat(50));

                let mut stats_table = create_table();
                stats_table.set_header(vec!["Metric", "Value"]);

                stats_table.add_row(vec!["Row Count", &format_count(stats.row_count)]);
                stats_table.add_row(vec![
                    "Distinct Entities",
                    &format_count(stats.distinct_entities),
                ]);
                stats_table.add_row(vec!["Total Size", &stats.total_size_human]);
                stats_table.add_row(vec!["Stats Updated", &stats.stats_updated_at]);

                println!("{stats_table}");
            } else if show_stats {
                print_info("No statistics available for this feature view");
            }
        }
        _ => {
            print_output(&details, output_format)?;
        }
    }

    Ok(())
}
