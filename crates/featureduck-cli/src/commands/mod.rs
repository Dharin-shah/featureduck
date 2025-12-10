//! CLI command implementations

pub mod backfill;
pub mod delete;
pub mod list;
pub mod materialize;
pub mod register;
pub mod runs;
pub mod show;
pub mod sync;
pub mod validate;

use anyhow::Result;
use featureduck_registry::{FeatureRegistry, RegistryConfig};

use crate::config::CliConfig;

/// Create registry from config
pub async fn create_registry(config: &CliConfig) -> Result<FeatureRegistry> {
    let registry_config = RegistryConfig::SQLite {
        path: config.registry.path.clone(),
    };
    FeatureRegistry::new(registry_config).await
}
