//! Show materialization run history command

use anyhow::Result;
use comfy_table::Cell;
use serde::Serialize;

use crate::config::CliConfig;
use crate::output::{
    create_table, format_count, format_duration, format_status, print_info, print_output,
    OutputFormat,
};

use super::create_registry;

#[derive(Debug, Serialize)]
struct RunInfo {
    id: i64,
    feature_view: String,
    status: String,
    rows_processed: Option<i64>,
    started_at: Option<String>,
    completed_at: Option<String>,
    duration_seconds: Option<i64>,
    error: Option<String>,
}

pub async fn run(
    config: &CliConfig,
    name: Option<&str>,
    limit: usize,
    status_filter: Option<&str>,
    output_format: &str,
) -> Result<()> {
    let registry = create_registry(config).await?;

    // If name is provided, get runs for that view
    // Otherwise, list all views and get runs for each
    let mut all_runs = Vec::new();

    if let Some(view_name) = name {
        let runs = registry.get_recent_runs(view_name, limit).await?;
        for run in runs {
            let duration = match (run.started_at, run.completed_at) {
                (Some(start), Some(end)) => Some((end - start).num_seconds()),
                _ => None,
            };

            all_runs.push(RunInfo {
                id: run.id,
                feature_view: run.feature_view_name,
                status: run.status.to_string(),
                rows_processed: run.rows_processed,
                started_at: run.started_at.map(|t| t.to_rfc3339()),
                completed_at: run.completed_at.map(|t| t.to_rfc3339()),
                duration_seconds: duration,
                error: run.error_message,
            });
        }
    } else {
        // Get all views and their runs
        let views = registry.list_feature_views(None).await?;
        for view in views {
            let runs = registry.get_recent_runs(&view.name, limit).await?;
            for run in runs {
                let duration = match (run.started_at, run.completed_at) {
                    (Some(start), Some(end)) => Some((end - start).num_seconds()),
                    _ => None,
                };

                all_runs.push(RunInfo {
                    id: run.id,
                    feature_view: run.feature_view_name,
                    status: run.status.to_string(),
                    rows_processed: run.rows_processed,
                    started_at: run.started_at.map(|t| t.to_rfc3339()),
                    completed_at: run.completed_at.map(|t| t.to_rfc3339()),
                    duration_seconds: duration,
                    error: run.error_message,
                });
            }
        }
    }

    // Apply status filter
    if let Some(filter) = status_filter {
        let filter_lower = filter.to_lowercase();
        all_runs.retain(|r| r.status.to_lowercase() == filter_lower);
    }

    // Sort by started_at descending
    all_runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));

    // Limit results
    all_runs.truncate(limit);

    if all_runs.is_empty() {
        print_info("No runs found");
        return Ok(());
    }

    match OutputFormat::parse(output_format) {
        OutputFormat::Table => {
            let mut table = create_table();
            table.set_header(vec![
                "ID",
                "Feature View",
                "Status",
                "Rows",
                "Duration",
                "Started",
            ]);

            for run in &all_runs {
                table.add_row(vec![
                    Cell::new(run.id),
                    Cell::new(&run.feature_view),
                    format_status(&run.status),
                    Cell::new(
                        run.rows_processed
                            .map(format_count)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::new(
                        run.duration_seconds
                            .map(format_duration)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::new(
                        run.started_at
                            .as_ref()
                            .map(|s| {
                                // Show just date/time, not full RFC3339
                                s.split('T')
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .trim_end_matches('Z')
                                    .to_string()
                            })
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                ]);
            }

            println!("{table}");

            // Show error details for failed runs
            let failed: Vec<_> = all_runs
                .iter()
                .filter(|r| r.status == "failed" && r.error.is_some())
                .collect();

            if !failed.is_empty() {
                println!("\nFailed run errors:");
                for run in failed {
                    println!(
                        "  Run {}: {}",
                        run.id,
                        run.error.as_deref().unwrap_or("Unknown error")
                    );
                }
            }

            println!("\nTotal: {} run(s)", all_runs.len());
        }
        _ => {
            print_output(&all_runs, output_format)?;
        }
    }

    Ok(())
}
