//! Sync features from Delta Lake to online store
//!
//! This command syncs the latest features from Delta Lake (offline store)
//! to an online store (Redis or PostgreSQL) for low-latency serving.

use anyhow::Result;

use crate::config::CliConfig;
use crate::output::print_error;

#[cfg(feature = "online")]
use anyhow::Context;
#[cfg(feature = "online")]
use serde::Serialize;
#[cfg(feature = "online")]
use crate::output::{print_info, print_success, print_output, OutputFormat};
#[cfg(feature = "online")]
use super::create_registry;

#[cfg(feature = "online")]
#[derive(Debug, Serialize)]
struct SyncReport {
    feature_view: String,
    store_type: String,
    rows_synced: usize,
    duration_ms: u64,
    rows_per_sec: f64,
}

/// Run sync from Delta Lake to online store
///
/// # Arguments
/// * `config` - CLI configuration
/// * `name` - Feature view name
/// * `online_url` - Online store URL (redis://... or postgresql://...)
/// * `ttl_seconds` - Optional TTL for features
/// * `dry_run` - If true, just show what would be synced
#[allow(unused_variables)] // Used when online feature is enabled
pub async fn run(
    config: &CliConfig,
    name: &str,
    online_url: &str,
    ttl_seconds: Option<u64>,
    batch_size: usize,
    dry_run: bool,
    output_format: &str,
) -> Result<()> {
    // Check if online feature is available
    #[cfg(not(feature = "online"))]
    {
        print_error("Online store feature not enabled. Rebuild CLI with --features online");
        Ok(())
    }

    #[cfg(feature = "online")]
    {
        use featureduck_online::{sync_to_online, SyncConfig};

        let registry = create_registry(config).await?;

        // Get feature view from registry
        let view = registry
            .get_feature_view(name)
            .await
            .with_context(|| format!("Feature view '{}' not found in registry", name))?;

        print_info(&format!(
            "Syncing '{}' from {} to {}",
            name, view.source_path, online_url
        ));

        if dry_run {
            print_info("Dry run - would sync:");
            print_info(&format!("  Source: {}", view.source_path));
            print_info(&format!("  Target: {}", online_url));
            print_info(&format!("  Entities: {:?}", view.entities));
            print_info(&format!("  TTL: {:?}", ttl_seconds.map(|s| format!("{}s", s)).unwrap_or_else(|| "none".to_string())));
            return Ok(());
        }

        // Create online store based on URL
        let store: Box<dyn featureduck_core::OnlineStore> = if online_url.starts_with("redis://") {
            #[cfg(feature = "redis")]
            {
                let config = featureduck_online::RedisConfig {
                    url: online_url.to_string(),
                    write_batch_size: batch_size,
                    ..Default::default()
                };
                Box::new(featureduck_online::RedisOnlineStore::new(config).await?)
            }
            #[cfg(not(feature = "redis"))]
            {
                print_error("Redis feature not enabled. Rebuild CLI with --features redis");
                return Ok(());
            }
        } else if online_url.starts_with("postgresql://") || online_url.starts_with("postgres://") {
            #[cfg(feature = "postgres")]
            {
                let config = featureduck_online::PostgresConfig {
                    connection_string: online_url.to_string(),
                    write_batch_size: batch_size,
                    ..Default::default()
                };
                Box::new(featureduck_online::PostgresOnlineStore::new(config).await?)
            }
            #[cfg(not(feature = "postgres"))]
            {
                print_error("PostgreSQL feature not enabled. Rebuild CLI with --features postgres");
                return Ok(());
            }
        } else {
            print_error(&format!("Unknown online store URL scheme: {}", online_url));
            print_info("Supported schemes: redis://, postgresql://, postgres://");
            return Ok(());
        };

        // Entity columns from view
        let entity_columns: Vec<&str> = view.entities.iter().map(|s| s.as_str()).collect();

        let sync_config = SyncConfig {
            write_batch_size: batch_size,
            ttl: ttl_seconds.map(std::time::Duration::from_secs),
            ..Default::default()
        };

        // Run sync
        let result = sync_to_online(
            store.as_ref(),
            &view.source_path,
            name,
            &entity_columns,
            sync_config,
        )
        .await
        .with_context(|| format!("Sync failed for '{}'", name))?;

        let report = SyncReport {
            feature_view: name.to_string(),
            store_type: store.store_type().to_string(),
            rows_synced: result.rows_synced,
            duration_ms: result.duration.as_millis() as u64,
            rows_per_sec: result.rows_per_sec,
        };

        match OutputFormat::parse(output_format) {
            OutputFormat::Table => {
                print_success(&format!(
                    "Synced {} rows to {} in {:.2}s ({:.0} rows/sec)",
                    result.rows_synced,
                    store.store_type(),
                    result.duration.as_secs_f64(),
                    result.rows_per_sec
                ));
            }
            _ => {
                print_output(&report, output_format)?;
            }
        }

        Ok(())
    }
}
