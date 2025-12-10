//! Materialize feature view command
//!
//! This command executes feature transformations using DuckDB and writes results to Delta Lake.
//!
//! ## How it works:
//! 1. Reads feature view definition from registry (includes SQL transformation)
//! 2. Applies time range filters to the transformation query
//! 3. Executes query via DuckDB (supports up to 1TB with disk spills)
//! 4. Writes results to Delta Lake with ACID guarantees
//!
//! ## Transformation format:
//! Feature views store transformations as SQL with placeholders:
//! - `{{source}}` - replaced with source path
//! - `{{start_time}}` - replaced with start timestamp
//! - `{{end_time}}` - replaced with end timestamp

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

use crate::config::CliConfig;
use crate::output::{print_error, print_info, print_success, print_warning};
use featureduck_delta::{DeltaStorageConnector, DuckDBEngineConfig};
use featureduck_registry::RunStatus;

use super::create_registry;

pub async fn run(
    config: &CliConfig,
    name: &str,
    start: Option<&str>,
    end: Option<&str>,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let registry = Arc::new(create_registry(config).await?);

    // Get feature view definition
    let view = registry
        .get_feature_view(name)
        .await
        .with_context(|| format!("Feature view '{}' not found", name))?;

    print_info(&format!("Materializing feature view: {}", view.name));

    // Parse time range
    let end_time = parse_time(end.unwrap_or("now"))?;
    let start_time = if let Some(s) = start {
        parse_time(s)?
    } else {
        // Default to TTL if set, otherwise 7 days
        let days = view.ttl_days.unwrap_or(7) as i64;
        end_time - Duration::days(days)
    };

    print_info(&format!(
        "Time range: {} to {}",
        start_time.to_rfc3339(),
        end_time.to_rfc3339()
    ));

    // Build the SQL query from transformation definition
    let sql = build_materialization_sql(&view, start_time, end_time)?;

    if dry_run {
        print_warning("Dry run mode - no changes will be made");
        println!("\nWould execute:");
        println!("  - Source: {} ({})", view.source_path, view.source_type);
        println!("  - Entities: {}", view.entities.join(", "));
        println!("  - Time filter: {} to {}", start_time, end_time);
        println!("  - Output: {}/{}", config.storage.base_path, view.name);
        println!("\n  SQL Query:\n{}", indent_sql(&sql));
        return Ok(());
    }

    // Create run record
    let run_id = registry.create_run(name).await?;
    print_info(&format!("Created run ID: {}", run_id));

    // Update status to running
    registry
        .update_run_status(run_id, RunStatus::Running, None, None)
        .await?;

    // Setup progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("Invalid progress bar template"),
    );
    pb.set_message("Initializing storage connector...");

    // Create storage connector with registry for stats collection
    let mut storage =
        DeltaStorageConnector::new(&config.storage.base_path, DuckDBEngineConfig::default())
            .await
            .context("Failed to create storage connector")?;

    // Attach registry for stats collection
    storage.set_registry(registry.clone());

    pb.set_message("Executing materialization...");

    // Check if feature view already has data (unless force)
    if !force {
        // Could check for existing data here and skip if recent
        // For now, proceed with materialization
    }

    // Execute materialization via DuckDB
    let result = execute_materialization(&storage, &view, &sql).await;

    pb.finish_and_clear();

    match result {
        Ok(rows_processed) => {
            registry
                .update_run_status(run_id, RunStatus::Success, Some(rows_processed), None)
                .await?;

            print_success(&format!(
                "Materialization complete! {} rows processed",
                rows_processed
            ));
        }
        Err(e) => {
            let error_msg = format!("{:?}", e);
            registry
                .update_run_status(run_id, RunStatus::Failed, None, Some(&error_msg))
                .await?;

            print_error(&format!("Materialization failed: {}", e));
            return Err(e);
        }
    }

    Ok(())
}

/// Parse time string to DateTime
/// Supports:
/// - "now" - current time
/// - "-7d" - 7 days ago
/// - "-24h" - 24 hours ago
/// - ISO8601 format
fn parse_time(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();

    if s == "now" {
        return Ok(Utc::now());
    }

    // Relative time format: -Nd or -Nh
    if s.starts_with('-') && s.len() > 1 {
        let (num_str, unit) = s[1..].split_at(s.len() - 2);
        let num: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid number in time string: {}", s))?;

        let duration = match unit {
            "d" => Duration::days(num),
            "h" => Duration::hours(num),
            "m" => Duration::minutes(num),
            _ => anyhow::bail!("Unknown time unit: {}. Use 'd', 'h', or 'm'", unit),
        };

        return Ok(Utc::now() - duration);
    }

    // Try ISO8601 format
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Try date-only format
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        })
        .with_context(|| {
            format!(
                "Invalid time format: {}. Use ISO8601, YYYY-MM-DD, or relative (-7d, -24h)",
                s
            )
        })
}

/// Build SQL query from feature view transformation definition
///
/// Transformation formats supported:
/// 1. Raw SQL with placeholders: `SELECT ... FROM {{source}} WHERE timestamp >= '{{start_time}}'`
/// 2. JSON config with sql field: `{"sql": "SELECT ...", "aggregations": [...]}`
/// 3. Empty/default: Auto-generate SELECT * with time filter
fn build_materialization_sql(
    view: &featureduck_registry::FeatureViewDef,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<String> {
    let transformations = view.transformations.trim();

    // Format timestamps for SQL
    let start_ts = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let end_ts = end_time.format("%Y-%m-%d %H:%M:%S").to_string();

    // Determine source table reference
    let source_ref = match view.source_type.as_str() {
        "delta" => format!("delta_scan('{}')", view.source_path),
        "parquet" => format!("read_parquet('{}/**/*.parquet')", view.source_path),
        "csv" => format!("read_csv('{}')", view.source_path),
        _ => format!("'{}'", view.source_path), // Assume it's a table name
    };

    // Case 1: Empty or default transformation - generate SELECT * with time filter
    if transformations.is_empty() || transformations == "{}" {
        let timestamp_field = view.timestamp_field.as_deref().unwrap_or("timestamp");
        let entity_cols = view.entities.join(", ");

        return Ok(format!(
            r#"SELECT {entities}, *
FROM {source}
WHERE {ts_field} >= '{start}'
  AND {ts_field} < '{end}'
GROUP BY {entities}"#,
            entities = entity_cols,
            source = source_ref,
            ts_field = timestamp_field,
            start = start_ts,
            end = end_ts,
        ));
    }

    // Case 2: Try parsing as JSON config
    if transformations.starts_with('{') {
        if let Ok(config) = serde_json::from_str::<serde_json::Value>(transformations) {
            // Check for SQL field in JSON
            if let Some(sql) = config.get("sql").and_then(|v| v.as_str()) {
                // Replace placeholders in SQL
                let sql = sql
                    .replace("{{source}}", &source_ref)
                    .replace("{{start_time}}", &start_ts)
                    .replace("{{end_time}}", &end_ts);
                return Ok(sql);
            }

            // Check for aggregations config (generate SQL from it)
            if let Some(aggs) = config.get("aggregations").and_then(|v| v.as_array()) {
                return build_sql_from_aggregations(view, &source_ref, aggs, &start_ts, &end_ts);
            }
        }
    }

    // Case 3: Raw SQL with placeholders
    let sql = transformations
        .replace("{{source}}", &source_ref)
        .replace("{{start_time}}", &start_ts)
        .replace("{{end_time}}", &end_ts);

    Ok(sql)
}

/// Build SQL from aggregation configuration
fn build_sql_from_aggregations(
    view: &featureduck_registry::FeatureViewDef,
    source_ref: &str,
    aggregations: &[serde_json::Value],
    start_ts: &str,
    end_ts: &str,
) -> Result<String> {
    let timestamp_field = view.timestamp_field.as_deref().unwrap_or("timestamp");
    let entity_cols = view.entities.join(", ");

    // Build aggregation expressions
    let mut agg_exprs = Vec::new();
    for agg in aggregations {
        if let (Some(func), Some(col), Some(alias)) = (
            agg.get("function").and_then(|v| v.as_str()),
            agg.get("column").and_then(|v| v.as_str()),
            agg.get("alias").and_then(|v| v.as_str()),
        ) {
            let expr = match func.to_uppercase().as_str() {
                "COUNT" => format!("COUNT({}) AS {}", col, alias),
                "SUM" => format!("SUM({}) AS {}", col, alias),
                "AVG" => format!("AVG({}) AS {}", col, alias),
                "MIN" => format!("MIN({}) AS {}", col, alias),
                "MAX" => format!("MAX({}) AS {}", col, alias),
                "COUNT_DISTINCT" => format!("COUNT(DISTINCT {}) AS {}", col, alias),
                _ => format!("{}({}) AS {}", func, col, alias),
            };
            agg_exprs.push(expr);
        }
    }

    if agg_exprs.is_empty() {
        anyhow::bail!("No valid aggregations found in transformation config");
    }

    Ok(format!(
        r#"SELECT {entities}, {aggs}
FROM {source}
WHERE {ts_field} >= '{start}'
  AND {ts_field} < '{end}'
GROUP BY {entities}"#,
        entities = entity_cols,
        aggs = agg_exprs.join(", "),
        source = source_ref,
        ts_field = timestamp_field,
        start = start_ts,
        end = end_ts,
    ))
}

/// Indent SQL for display
fn indent_sql(sql: &str) -> String {
    sql.lines()
        .map(|line| format!("    {}", line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Execute the actual materialization via DuckDB
///
/// This function:
/// 1. Executes the SQL query using DuckDB
/// 2. Writes results to Delta Lake
/// 3. Returns the number of rows processed
async fn execute_materialization(
    storage: &DeltaStorageConnector,
    view: &featureduck_registry::FeatureViewDef,
    sql: &str,
) -> Result<i64> {
    tracing::info!(
        "Executing materialization for {} with entities {:?}",
        view.name,
        view.entities
    );
    tracing::debug!("SQL:\n{}", sql);

    // Execute via DuckDB and write to Delta Lake
    storage
        .materialize_from_sql(&view.name, sql, &view.entities)
        .await
        .context("Failed to execute materialization")?;

    // Get row count from the written data
    // For now, we return 0 and rely on stats saved during write
    // TODO: Return actual row count from materialize_from_sql
    Ok(0)
}
