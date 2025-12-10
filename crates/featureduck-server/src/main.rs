//! # FeatureDuck Server
//!
//! Main HTTP server for feature serving.
//!
//! ## Architecture
//!
//! This server follows a simple layered architecture:
//! - HTTP layer (Axum routes)
//! - Business logic (in handlers)
//! - Storage layer (via StorageConnector trait)
//!
//! ## Startup Flow
//!
//! 1. Parse command-line arguments
//! 2. Load configuration
//! 3. Initialize tracing/logging
//! 4. Create server state (connections, etc.)
//! 5. Start HTTP server
//!
//! ## Usage
//!
//! ```bash
//! # Start server with defaults
//! featureduck serve
//!
//! # Specify configuration file
//! featureduck serve --config config.yaml
//!
//! # Specify port
//! featureduck serve --port 8080
//! ```

use axum::{
    extract::DefaultBodyLimit,
    routing::{delete, get, post, put},
    Router,
};
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::time::Duration;
use tower_http::timeout::TimeoutLayer;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// Module declarations - each handles a specific concern
mod api; // HTTP route handlers
mod auth; // Authentication middleware (P0-1)
mod cli; // CLI command handlers
mod config; // Configuration loading
mod error; // Error types
mod health; // Health check endpoints (P1-6)
mod metrics; // Prometheus metrics
mod rate_limit; // Rate limiting middleware (P0-6)
mod request_id; // Request ID tracing middleware
mod shutdown; // Graceful shutdown (P1-7)
mod state; // Shared application state

// Re-export for convenience
use crate::error::Result;

/// FeatureDuck command-line interface
///
/// This uses the `clap` crate for argument parsing with derive macros.
/// It provides a clean CLI interface with subcommands.
#[derive(Parser)]
#[command(name = "featureduck")]
#[command(about = "Universal feature store powered by DuckDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Available CLI subcommands
#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server
    Serve {
        /// Configuration file path
        #[arg(short, long, default_value = "featureduck.yaml")]
        config: String,

        /// Port to listen on
        #[arg(short, long, default_value = "8000")]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
    },

    /// Register a feature view from YAML definition
    Register {
        /// Path to feature view YAML file
        file: String,

        /// Registry database path
        #[arg(long, default_value = "./featureduck_registry.db")]
        registry: String,
    },

    /// List all registered feature views
    List {
        /// Registry database path
        #[arg(long, default_value = "./featureduck_registry.db")]
        registry: String,

        /// Filter by name pattern
        #[arg(short, long)]
        filter: Option<String>,
    },

    /// Trigger materialization for a feature view
    Materialize {
        /// Feature view name
        name: String,

        /// Registry database path
        #[arg(long, default_value = "./featureduck_registry.db")]
        registry: String,
    },

    /// Check status of a materialization run
    Status {
        /// Run ID
        run_id: i64,

        /// Registry database path
        #[arg(long, default_value = "./featureduck_registry.db")]
        registry: String,
    },

    /// Show materialization history for a feature view
    History {
        /// Feature view name
        name: String,

        /// Registry database path
        #[arg(long, default_value = "./featureduck_registry.db")]
        registry: String,

        /// Number of runs to show
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },

    /// Validate a feature view YAML definition without registering
    Validate {
        /// Path to feature view YAML file
        file: String,
    },
}

/// Main entry point
///
/// This function:
/// 1. Initializes the tracing subscriber for logging
/// 2. Parses command-line arguments
/// 3. Dispatches to the appropriate command handler
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging with environment-based configuration
    // Supports both pretty-printed (development) and JSON (production) output
    //
    // Environment variables:
    // - RUST_LOG: Set log level (e.g., "info", "debug", "featureduck=trace")
    // - LOG_FORMAT: Set format ("json" for production, "pretty" for development)
    //
    // Examples:
    // - Development: LOG_FORMAT=pretty RUST_LOG=debug featureduck serve
    // - Production:  LOG_FORMAT=json RUST_LOG=info featureduck serve

    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());

    // Create EnvFilter for flexible log level control
    // Defaults to INFO, but can be overridden with RUST_LOG env var
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Initialize tracing subscriber based on format
    if log_format == "json" {
        // JSON output for production (machine-readable, structured)
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true) // Include span context
                    .with_span_list(true) // Include full span hierarchy
                    .with_target(true) // Include module path in JSON
                    .with_thread_ids(true) // Include thread ID
                    .with_thread_names(true), // Include thread name
            )
            .init();
    } else {
        // Pretty output for development (human-readable)
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .pretty()
                    .with_target(false) // Cleaner output
                    .with_thread_ids(false),
            )
            .init();
    }

    info!(
        "FeatureDuck server initializing with LOG_FORMAT={}",
        log_format
    );

    // Parse CLI arguments
    let cli = Cli::parse();

    // Dispatch to command handler
    match cli.command {
        Commands::Serve { config, port, host } => {
            serve(config, host, port).await?;
        }
        Commands::Register { file, registry } => {
            cli::register(file, registry).await?;
        }
        Commands::List { registry, filter } => {
            cli::list(registry, filter).await?;
        }
        Commands::Materialize { name, registry } => {
            cli::materialize(name, registry).await?;
        }
        Commands::Status { run_id, registry } => {
            cli::status(run_id, registry).await?;
        }
        Commands::History {
            name,
            registry,
            limit,
        } => {
            cli::history(name, registry, limit).await?;
        }
        Commands::Validate { file } => {
            cli::validate(file)?;
        }
    }

    Ok(())
}

/// Start the HTTP server
///
/// This is the main server logic:
/// 1. Load configuration from file
/// 2. Initialize authentication and rate limiting (P0)
/// 3. Initialize application state
/// 4. Build router with all routes and middleware
/// 5. Start listening on specified address
///
/// # Arguments
///
/// * `config_path` - Path to configuration file
/// * `host` - Host address to bind to (e.g., "0.0.0.0")
/// * `port` - Port to listen on (e.g., 8000)
///
/// # Security (P0)
///
/// - Authentication: Set `FEATUREDUCK_API_KEYS` env var (comma-separated)
/// - Rate Limiting: Set `FEATUREDUCK_RATE_LIMIT_RPS` env var (default: 1000)
async fn serve(config_path: String, host: String, port: u16) -> Result<()> {
    info!("Starting FeatureDuck server");
    info!("Loading configuration from: {}", config_path);

    // Load configuration
    // For Milestone 0, we'll use minimal configuration
    // Future milestones will load storage connector config, etc.
    let _config = config::load(&config_path)?;

    // Initialize authentication from environment (P0-1)
    let auth_config = std::sync::Arc::new(auth::AuthConfig::from_env());
    if auth_config.is_enabled() {
        info!("Authentication ENABLED (API keys configured)");
    } else {
        info!("Authentication DISABLED (no API keys configured)");
        info!("  Set FEATUREDUCK_API_KEYS env var to enable authentication");
    }

    // Initialize rate limiting from environment (P0-6)
    let rate_limit_state = rate_limit::RateLimitState::from_env();
    if rate_limit_state.config.enabled {
        info!(
            "Rate limiting ENABLED: {} RPS, burst {}",
            rate_limit_state.config.requests_per_second, rate_limit_state.config.burst_size
        );
    } else {
        info!("Rate limiting DISABLED");
    }

    // Initialize application state with registry
    // Check if registry path is specified in config, otherwise use default
    let app_state = match state::AppState::with_registry_path("./featureduck_registry.db").await {
        Ok(state) => state,
        Err(e) => {
            info!(
                "Registry initialization warning: {}. Using in-memory registry for testing.",
                e
            );
            state::AppState::new_with_registry().await
        }
    };

    // =========================================================================
    // RESILIENCE CONFIGURATION - Prevent OOM and ensure graceful degradation
    // =========================================================================

    // Request body size limit (default: 10MB)
    // Prevents memory exhaustion from large request bodies
    let max_body_size: usize = std::env::var("FEATUREDUCK_MAX_BODY_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10 * 1024 * 1024); // 10 MB default

    // Request timeout (default: 60 seconds)
    // Prevents slow clients from holding connections forever
    let request_timeout_secs: u64 = std::env::var("FEATUREDUCK_REQUEST_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    info!(
        "Server resilience config: max_body_size={}MB, request_timeout={}s",
        max_body_size / (1024 * 1024),
        request_timeout_secs
    );

    // Build the router with all HTTP routes
    // Each route is handled by a function in the `api` module
    let app = Router::new()
        // Health check endpoints
        .route("/health", get(health::liveness))
        .route("/health/ready", get(health::readiness))
        .route("/health/storage", get(health::storage_health))
        .route("/health/registry", get(health::registry_health))
        // Prometheus metrics endpoint - exports metrics for monitoring
        .route("/metrics", get(api::metrics))
        // API v1 - Feature serving endpoints (M0-M2)
        .route("/v1/features/online", post(api::get_online_features))
        .route(
            "/v1/features/historical",
            post(api::get_historical_features),
        )
        .route("/v1/features/views", get(api::list_feature_views))
        // API v1 - Feature registry endpoints (M3)
        .route(
            "/api/v1/feature-views",
            get(api::registry::list_feature_views),
        )
        .route(
            "/api/v1/feature-views",
            post(api::registry::register_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            get(api::registry::get_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            put(api::registry::update_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name",
            delete(api::registry::delete_feature_view),
        )
        .route(
            "/api/v1/feature-views/:name/runs",
            get(api::registry::get_runs_history),
        )
        .route(
            "/api/v1/feature-views/:name/stats",
            get(api::registry::get_feature_view_stats),
        )
        .route(
            "/api/v1/feature-views/:name/materialize",
            post(api::registry::trigger_materialization),
        )
        .route("/api/v1/runs/:id", get(api::registry::get_run_status))
        // Admin endpoints - Sync (online store)
        .route("/v1/admin/sync", post(api::sync::sync_to_online_store))
        // Share application state with all handlers
        // This allows handlers to access the state without globals
        .with_state(app_state)
        // P0-1: Authentication middleware
        // Validates API keys on protected endpoints (health/metrics are public)
        .layer(axum::middleware::from_fn_with_state(
            auth_config,
            auth::auth_middleware,
        ))
        // P0-6: Rate limiting middleware
        // Protects against abuse with configurable RPS limits
        .layer(axum::middleware::from_fn_with_state(
            rate_limit_state,
            rate_limit::rate_limit_middleware,
        ))
        // =========================================================================
        // RESILIENCE LAYERS - Applied in reverse order (bottom to top)
        // =========================================================================
        // Request body limit - prevents OOM from large payloads
        .layer(DefaultBodyLimit::max(max_body_size))
        // CORS middleware
        .layer(tower_http::cors::CorsLayer::permissive())
        // Request ID middleware - generates unique ID for each request (for tracing)
        .layer(axum::middleware::from_fn(request_id::request_id_middleware))
        // Tracing middleware (log all requests)
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // Request timeout - prevents slow clients from holding connections
        // Note: Applied outermost so it wraps all other processing
        .layer(TimeoutLayer::new(Duration::from_secs(request_timeout_secs)));

    // Parse the socket address from host and port
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| crate::error::AppError::Internal(format!("Invalid host/port: {}", e)))?;

    info!("Server listening on http://{}", addr);
    info!("Health check: http://{}/health", addr);
    info!("API documentation: http://{}/docs (future)", addr);

    // Start the server with graceful shutdown
    // This will run until interrupted (Ctrl+C or SIGTERM)
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| crate::error::AppError::Internal(format!("Failed to bind address: {}", e)))?;

    // Graceful shutdown signal handler (P1-7)
    // Uses the shutdown module for consistent shutdown handling

    // Run server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
        .map_err(|e| crate::error::AppError::Internal(format!("Server error: {}", e)))?;

    info!("Server shut down gracefully");
    Ok(())
}

// Tests for the main server logic
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing_serve() {
        // Test that CLI arguments parse correctly
        let cli = Cli::parse_from(["featureduck", "serve"]);
        match cli.command {
            Commands::Serve { port, .. } => {
                assert_eq!(port, 8000); // default port
            }
            _ => panic!("Expected Serve command"),
        }
    }

    #[test]
    fn test_cli_with_custom_port() {
        let cli = Cli::parse_from(["featureduck", "serve", "--port", "9000"]);
        match cli.command {
            Commands::Serve { port, .. } => {
                assert_eq!(port, 9000);
            }
            _ => panic!("Expected Serve command"),
        }
    }

    #[test]
    fn test_cli_list_command() {
        let cli = Cli::parse_from(["featureduck", "list"]);
        match cli.command {
            Commands::List { registry, filter } => {
                assert_eq!(registry, "./featureduck_registry.db");
                assert_eq!(filter, None);
            }
            _ => panic!("Expected List command"),
        }
    }

    #[test]
    fn test_cli_materialize_command() {
        let cli = Cli::parse_from(["featureduck", "materialize", "test_view"]);
        match cli.command {
            Commands::Materialize { name, .. } => {
                assert_eq!(name, "test_view");
            }
            _ => panic!("Expected Materialize command"),
        }
    }

    #[test]
    fn test_cli_validate_command() {
        let cli = Cli::parse_from(["featureduck", "validate", "test.yaml"]);
        match cli.command {
            Commands::Validate { file } => {
                assert_eq!(file, "test.yaml");
            }
            _ => panic!("Expected Validate command"),
        }
    }
}
