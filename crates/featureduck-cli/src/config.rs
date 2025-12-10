//! CLI configuration handling

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// CLI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    /// Registry configuration
    pub registry: RegistryConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Materialization defaults
    #[serde(default)]
    pub materialization: MaterializationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Path to SQLite database
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base path for feature storage
    pub base_path: String,

    /// Compression codec (zstd, snappy, lz4, none)
    #[serde(default = "default_compression")]
    pub compression: String,
}

fn default_compression() -> String {
    "zstd".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationConfig {
    /// Default parallelism for backfills
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Default batch size
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Retry attempts on failure
    #[serde(default = "default_retries")]
    pub retries: usize,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

impl Default for MaterializationConfig {
    fn default() -> Self {
        Self {
            parallelism: default_parallelism(),
            batch_size: default_batch_size(),
            retries: default_retries(),
            timeout_seconds: default_timeout(),
        }
    }
}

fn default_parallelism() -> usize {
    4
}

fn default_batch_size() -> usize {
    10000
}

fn default_retries() -> usize {
    3
}

fn default_timeout() -> u64 {
    3600 // 1 hour
}

/// Load configuration from file or defaults
pub fn load_config(
    config_path: Option<&str>,
    registry_path: &str,
    storage_path: &str,
) -> Result<CliConfig> {
    if let Some(path) = config_path {
        load_from_file(path)
    } else {
        // Check for default config locations
        let default_locations = [
            ".featureduck/config.yaml",
            ".featureduck/config.yml",
            "featureduck.yaml",
            "featureduck.yml",
        ];

        for location in &default_locations {
            if Path::new(location).exists() {
                return load_from_file(location);
            }
        }

        // Use CLI arguments as defaults
        Ok(CliConfig {
            registry: RegistryConfig {
                path: registry_path.to_string(),
            },
            storage: StorageConfig {
                base_path: storage_path.to_string(),
                compression: default_compression(),
            },
            materialization: MaterializationConfig::default(),
        })
    }
}

fn load_from_file(path: &str) -> Result<CliConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path))?;

    if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML config: {}", path))
    } else if path.ends_with(".json") {
        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse JSON config: {}", path))
    } else {
        // Try YAML first, then JSON
        serde_yaml::from_str(&content)
            .or_else(|_| serde_json::from_str(&content))
            .with_context(|| format!("Failed to parse config file: {}", path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = load_config(None, "/path/to/registry.db", "/path/to/storage").unwrap();

        assert_eq!(config.registry.path, "/path/to/registry.db");
        assert_eq!(config.storage.base_path, "/path/to/storage");
        assert_eq!(config.storage.compression, "zstd");
        assert_eq!(config.materialization.parallelism, 4);
        assert_eq!(config.materialization.batch_size, 10000);
        assert_eq!(config.materialization.retries, 3);
        assert_eq!(config.materialization.timeout_seconds, 3600);
    }

    #[test]
    fn test_load_yaml_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.yaml");

        let yaml_content = r#"
registry:
  path: /custom/registry.db
storage:
  base_path: /custom/features
  compression: snappy
materialization:
  parallelism: 8
  batch_size: 50000
  retries: 5
  timeout_seconds: 7200
"#;
        std::fs::write(&config_path, yaml_content).unwrap();

        let config = load_from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.registry.path, "/custom/registry.db");
        assert_eq!(config.storage.base_path, "/custom/features");
        assert_eq!(config.storage.compression, "snappy");
        assert_eq!(config.materialization.parallelism, 8);
        assert_eq!(config.materialization.batch_size, 50000);
        assert_eq!(config.materialization.retries, 5);
        assert_eq!(config.materialization.timeout_seconds, 7200);
    }

    #[test]
    fn test_load_json_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");

        let json_content = r#"{
            "registry": { "path": "/json/registry.db" },
            "storage": { "base_path": "/json/features", "compression": "lz4" },
            "materialization": { "parallelism": 16 }
        }"#;
        std::fs::write(&config_path, json_content).unwrap();

        let config = load_from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.registry.path, "/json/registry.db");
        assert_eq!(config.storage.base_path, "/json/features");
        assert_eq!(config.storage.compression, "lz4");
        assert_eq!(config.materialization.parallelism, 16);
        // Defaults should be applied for missing fields
        assert_eq!(config.materialization.batch_size, 10000);
    }

    #[test]
    fn test_yaml_with_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("minimal.yaml");

        // Minimal config - only required fields
        let yaml_content = r#"
registry:
  path: /minimal/registry.db
storage:
  base_path: /minimal/features
"#;
        std::fs::write(&config_path, yaml_content).unwrap();

        let config = load_from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.registry.path, "/minimal/registry.db");
        assert_eq!(config.storage.base_path, "/minimal/features");
        // Defaults should be applied
        assert_eq!(config.storage.compression, "zstd");
        assert_eq!(config.materialization.parallelism, 4);
        assert_eq!(config.materialization.batch_size, 10000);
    }

    #[test]
    fn test_invalid_config_file_not_found() {
        let result = load_from_file("/nonexistent/config.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to read"));
    }

    #[test]
    fn test_invalid_yaml_syntax() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("invalid.yaml");

        let invalid_yaml = "registry:\n  path: [invalid yaml";
        std::fs::write(&config_path, invalid_yaml).unwrap();

        let result = load_from_file(config_path.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_materialization_config_default() {
        let config = MaterializationConfig::default();

        assert_eq!(config.parallelism, 4);
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.retries, 3);
        assert_eq!(config.timeout_seconds, 3600);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = CliConfig {
            registry: RegistryConfig {
                path: "/test/registry.db".to_string(),
            },
            storage: StorageConfig {
                base_path: "/test/features".to_string(),
                compression: "zstd".to_string(),
            },
            materialization: MaterializationConfig {
                parallelism: 8,
                batch_size: 20000,
                retries: 5,
                timeout_seconds: 1800,
            },
        };

        // Serialize to YAML
        let yaml = serde_yaml::to_string(&config).unwrap();

        // Deserialize back
        let parsed: CliConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.registry.path, config.registry.path);
        assert_eq!(parsed.storage.base_path, config.storage.base_path);
        assert_eq!(parsed.storage.compression, config.storage.compression);
        assert_eq!(
            parsed.materialization.parallelism,
            config.materialization.parallelism
        );
    }

    #[test]
    fn test_config_with_explicit_path() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("explicit.yaml");

        let yaml_content = r#"
registry:
  path: /explicit/registry.db
storage:
  base_path: /explicit/features
"#;
        std::fs::write(&config_path, yaml_content).unwrap();

        // When explicit path is given, it should be used over defaults
        let config = load_config(
            Some(config_path.to_str().unwrap()),
            "/ignored/registry.db",
            "/ignored/storage",
        )
        .unwrap();

        assert_eq!(config.registry.path, "/explicit/registry.db");
        assert_eq!(config.storage.base_path, "/explicit/features");
    }
}
