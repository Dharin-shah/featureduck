//! Configuration management
//!
//! This module handles loading and parsing configuration from files.
//! Configuration can come from:
//! - YAML files (featureduck.yaml)
//! - Environment variables (FEATUREDUCK_*)
//! - Command-line arguments (handled in main.rs)

use serde::Deserialize;
use std::path::Path;

/// Main configuration structure
///
/// This represents the complete configuration for the FeatureDuck server.
/// It's loaded from a YAML file and/or environment variables.
///
/// ## Example Configuration File (featureduck.yaml)
///
/// ```yaml
/// server:
///   host: "0.0.0.0"
///   port: 8000
///
/// storage:
///   type: "delta"
///   path: "s3://bucket/features"
///   options:
///     aws_region: "us-west-2"
///
/// duckdb:
///   memory_limit: "8GB"
///   threads: 4
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// Server configuration
    #[serde(default)]
    #[allow(dead_code)] // Will be used in future milestones
    pub server: ServerConfig,
    
    /// Storage configuration (for Milestone 1+)
    #[serde(default)]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub storage: Option<StorageConfig>,
    
    /// DuckDB configuration (for Milestone 1+)
    #[serde(default)]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub duckdb: Option<DuckDBConfig>,
}

/// Server-specific configuration
#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    /// Host to bind to (default: "0.0.0.0")
    #[serde(default = "default_host")]
    #[allow(dead_code)] // Will be used in future milestones
    pub host: String,
    
    /// Port to listen on (default: 8000)
    #[serde(default = "default_port")]
    #[allow(dead_code)] // Will be used in future milestones
    pub port: u16,
}

/// Storage backend configuration
///
/// This will be used in Milestone 1 when we implement storage connectors.
#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    /// Storage type: "delta", "iceberg", "hudi", "duckdb"
    #[serde(rename = "type")]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub storage_type: String,
    
    /// Path to storage (e.g., "s3://bucket/features")
    #[allow(dead_code)] // Will be used in Milestone 1
    pub path: String,
    
    /// Additional options (credentials, etc.)
    #[serde(default)]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub options: std::collections::HashMap<String, String>,
}

/// DuckDB configuration
///
/// Controls DuckDB performance settings.
#[derive(Debug, Deserialize, Clone)]
pub struct DuckDBConfig {
    /// Memory limit (e.g., "8GB")
    #[serde(default = "default_memory_limit")]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub memory_limit: String,
    
    /// Number of threads
    #[serde(default = "default_threads")]
    #[allow(dead_code)] // Will be used in Milestone 1
    pub threads: usize,
}

// Default value functions (used by serde)

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8000
}

fn default_memory_limit() -> String {
    "2GB".to_string()
}

fn default_threads() -> usize {
    4
}

// Default implementations

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl Default for DuckDBConfig {
    fn default() -> Self {
        Self {
            memory_limit: default_memory_limit(),
            threads: default_threads(),
        }
    }
}

/// Load configuration from a file
///
/// This reads a YAML configuration file and parses it into a Config struct.
/// If the file doesn't exist or is invalid, returns an error.
///
/// # Arguments
///
/// * `path` - Path to the configuration file
///
/// # Returns
///
/// Parsed configuration or error
///
/// # Example
///
/// ```rust,ignore
/// let config = load("featureduck.yaml")?;
/// println!("Server will listen on {}:{}", config.server.host, config.server.port);
/// ```
pub fn load(path: &str) -> anyhow::Result<Config> {
    // Check if file exists
    if !Path::new(path).exists() {
        // If config file doesn't exist, use defaults
        tracing::warn!("Configuration file '{}' not found, using defaults", path);
        return Ok(Config {
            server: ServerConfig::default(),
            storage: None,
            duckdb: None,
        });
    }

    // Read file contents
    let contents = std::fs::read_to_string(path)?;

    // Parse YAML
    let config: Config = serde_yaml::from_str(&contents)?;

    tracing::info!("Loaded configuration from {}", path);

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config {
            server: ServerConfig::default(),
            storage: None,
            duckdb: None,
        };
        
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8000);
    }

    #[test]
    fn test_parse_config_from_yaml() {
        let yaml = r#"
server:
  host: "127.0.0.1"
  port: 9000

storage:
  type: "delta"
  path: "s3://test-bucket/features"
  options:
    aws_region: "us-west-2"

duckdb:
  memory_limit: "4GB"
  threads: 8
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 9000);
        
        let storage = config.storage.unwrap();
        assert_eq!(storage.storage_type, "delta");
        assert_eq!(storage.path, "s3://test-bucket/features");
        assert_eq!(storage.options.get("aws_region"), Some(&"us-west-2".to_string()));
        
        let duckdb = config.duckdb.unwrap();
        assert_eq!(duckdb.memory_limit, "4GB");
        assert_eq!(duckdb.threads, 8);
    }

    #[test]
    fn test_load_nonexistent_file() {
        // Should return default config without error
        let result = load("nonexistent_file.yaml");
        assert!(result.is_ok());
        
        let config = result.unwrap();
        assert_eq!(config.server.port, 8000);
    }
}
