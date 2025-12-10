//! List feature views command

use anyhow::Result;
use comfy_table::Cell;
use serde::Serialize;

use crate::config::CliConfig;
use crate::output::{create_table, print_info, print_output, OutputFormat};

use super::create_registry;

#[derive(Debug, Serialize)]
struct FeatureViewSummary {
    name: String,
    version: i32,
    source_type: String,
    entities: Vec<String>,
    ttl_days: Option<i32>,
    schedule: Option<String>,
    owner: Option<String>,
}

pub async fn run(
    config: &CliConfig,
    filter: Option<&str>,
    detailed: bool,
    output_format: &str,
) -> Result<()> {
    let registry = create_registry(config).await?;

    let views = registry.list_feature_views(filter).await?;

    if views.is_empty() {
        print_info("No feature views found");
        return Ok(());
    }

    let summaries: Vec<FeatureViewSummary> = views
        .iter()
        .map(|v| FeatureViewSummary {
            name: v.name.clone(),
            version: v.version,
            source_type: v.source_type.clone(),
            entities: v.entities.clone(),
            ttl_days: v.ttl_days,
            schedule: v.batch_schedule.clone(),
            owner: v.owner.clone(),
        })
        .collect();

    match OutputFormat::parse(output_format) {
        OutputFormat::Table => {
            let mut table = create_table();

            if detailed {
                table.set_header(vec![
                    "Name", "Version", "Source", "Entities", "TTL", "Schedule", "Owner",
                ]);

                for view in &summaries {
                    table.add_row(vec![
                        Cell::new(&view.name),
                        Cell::new(view.version),
                        Cell::new(&view.source_type),
                        Cell::new(view.entities.join(", ")),
                        Cell::new(
                            view.ttl_days
                                .map(|d| format!("{}d", d))
                                .unwrap_or_else(|| "-".to_string()),
                        ),
                        Cell::new(view.schedule.as_deref().unwrap_or("-")),
                        Cell::new(view.owner.as_deref().unwrap_or("-")),
                    ]);
                }
            } else {
                table.set_header(vec!["Name", "Version", "Source", "Entities"]);

                for view in &summaries {
                    table.add_row(vec![
                        Cell::new(&view.name),
                        Cell::new(view.version),
                        Cell::new(&view.source_type),
                        Cell::new(view.entities.join(", ")),
                    ]);
                }
            }

            println!("{table}");
            println!("\nTotal: {} feature view(s)", summaries.len());
        }
        _ => {
            print_output(&summaries, output_format)?;
        }
    }

    Ok(())
}
