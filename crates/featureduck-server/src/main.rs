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
    routing::{get, post},
    Router,
};
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

// Module declarations - each handles a specific concern
mod api;      // HTTP route handlers
mod config;   // Configuration loading
mod error;    // Error types
mod state;    // Shared application state

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
///
/// For now we only have `serve` (start the HTTP server).
/// Future commands might include:
/// - `init`: Initialize a new feature store
/// - `migrate`: Run database migrations
/// - `validate`: Validate feature definitions
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
}

/// Main entry point
///
/// This function:
/// 1. Initializes the tracing subscriber for logging
/// 2. Parses command-line arguments
/// 3. Dispatches to the appropriate command handler
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    // This will output structured logs to stdout
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)  // Don't show module paths (cleaner output)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Parse CLI arguments
    let cli = Cli::parse();

    // Dispatch to command handler
    match cli.command {
        Commands::Serve { config, port, host } => {
            serve(config, host, port).await?;
        }
    }

    Ok(())
}

/// Start the HTTP server
///
/// This is the main server logic:
/// 1. Load configuration from file
/// 2. Initialize application state
/// 3. Build router with all routes
/// 4. Start listening on specified address
///
/// # Arguments
///
/// * `config_path` - Path to configuration file
/// * `host` - Host address to bind to (e.g., "0.0.0.0")
/// * `port` - Port to listen on (e.g., 8000)
async fn serve(config_path: String, host: String, port: u16) -> Result<()> {
    info!("Starting FeatureDuck server");
    info!("Loading configuration from: {}", config_path);

    // Load configuration
    // For Milestone 0, we'll use minimal configuration
    // Future milestones will load storage connector config, etc.
    let _config = config::load(&config_path)?;

    // Initialize application state
    // This is where we'd initialize:
    // - DuckDB connection pool
    // - Storage connector
    // - Feature registry
    // For now, it's just a placeholder
    let app_state = state::AppState::new();

    // Build the router with all HTTP routes
    // Each route is handled by a function in the `api` module
    let app = Router::new()
        // Health check endpoint - returns 200 OK if server is running
        .route("/health", get(api::health))
        
        // API v1 endpoints
        // We version the API (/v1/) to allow backwards-compatible changes
        .route("/v1/features/online", post(api::get_online_features))
        .route("/v1/features/historical", post(api::get_historical_features))
        .route("/v1/features/views", get(api::list_feature_views))
        
        // Share application state with all handlers
        // This allows handlers to access the state without globals
        .with_state(app_state)
        
        // Add CORS middleware (allow cross-origin requests)
        // This is useful for web UIs that want to call our API
        .layer(
            tower_http::cors::CorsLayer::permissive()
        )
        
        // Add tracing middleware (log all requests)
        // This gives us automatic request logging with timing
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
        );

    // Parse the socket address from host and port
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .expect("Invalid host/port");

    info!("Server listening on http://{}", addr);
    info!("Health check: http://{}/health", addr);
    info!("API documentation: http://{}/docs (future)", addr);

    // Start the server
    // This will run until interrupted (Ctrl+C)
    // Axum 0.7 uses tokio::net::TcpListener directly
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind address");
    
    axum::serve(listener, app)
        .await
        .expect("Server failed to start");

    Ok(())
}

// Tests for the main server logic
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test that CLI arguments parse correctly
        let cli = Cli::parse_from(&["featureduck", "serve"]);
        match cli.command {
            Commands::Serve { port, .. } => {
                assert_eq!(port, 8000); // default port
            }
        }
    }

    #[test]
    fn test_cli_with_custom_port() {
        let cli = Cli::parse_from(&["featureduck", "serve", "--port", "9000"]);
        match cli.command {
            Commands::Serve { port, .. } => {
                assert_eq!(port, 9000);
            }
        }
    }
}
