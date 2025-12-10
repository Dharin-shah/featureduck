//! FeatureDuck CLI - Feature store orchestration tool
//!
//! Commands:
//! - `materialize` - Run feature materialization for a feature view
//! - `backfill` - Backfill historical features for a date range
//! - `validate` - Validate feature view configuration
//! - `list` - List feature views in registry
//! - `show` - Show details of a feature view
//! - `runs` - Show materialization run history

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod commands;
mod config;
mod output;

use commands::{backfill, list, materialize, runs, show, sync, validate};

/// FeatureDuck CLI - Feature store orchestration tool
#[derive(Parser)]
#[command(name = "featureduck")]
#[command(author = "FeatureDuck Team")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Feature store orchestration CLI", long_about = None)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, env = "FEATUREDUCK_CONFIG")]
    config: Option<String>,

    /// Registry database path (SQLite)
    #[arg(
        long,
        env = "FEATUREDUCK_REGISTRY",
        default_value = ".featureduck/registry.db"
    )]
    registry: String,

    /// Storage base path
    #[arg(
        long,
        env = "FEATUREDUCK_STORAGE",
        default_value = ".featureduck/features"
    )]
    storage: String,

    /// Output format (table, json, yaml)
    #[arg(short, long, default_value = "table")]
    output: String,

    /// Verbose output (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run feature materialization for a feature view
    Materialize {
        /// Feature view name
        #[arg(short, long)]
        name: String,

        /// Start time for materialization (ISO8601 or relative: -7d, -24h)
        #[arg(long)]
        start: Option<String>,

        /// End time for materialization (ISO8601 or relative: now, -1h)
        #[arg(long)]
        end: Option<String>,

        /// Dry run - show what would be done without executing
        #[arg(long)]
        dry_run: bool,

        /// Force re-materialization even if data exists
        #[arg(long)]
        force: bool,
    },

    /// Backfill historical features for a date range
    Backfill {
        /// Feature view name
        #[arg(short, long)]
        name: String,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start_date: String,

        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end_date: String,

        /// Parallelism (number of concurrent days to process)
        #[arg(long, default_value = "4")]
        parallelism: usize,

        /// Dry run - show plan without executing
        #[arg(long)]
        dry_run: bool,
    },

    /// Validate feature view configuration
    Validate {
        /// Feature view name (or 'all' for all views)
        #[arg(short, long)]
        name: String,

        /// Check source data accessibility
        #[arg(long)]
        check_source: bool,

        /// Check storage accessibility
        #[arg(long)]
        check_storage: bool,

        /// Validate schema compatibility
        #[arg(long)]
        check_schema: bool,
    },

    /// List feature views in registry
    List {
        /// Filter by name pattern
        #[arg(short, long)]
        filter: Option<String>,

        /// Show detailed information
        #[arg(long)]
        detailed: bool,
    },

    /// Show details of a feature view
    Show {
        /// Feature view name
        name: String,

        /// Show schema information
        #[arg(long)]
        schema: bool,

        /// Show statistics
        #[arg(long)]
        stats: bool,
    },

    /// Show materialization run history
    Runs {
        /// Feature view name (optional, shows all if not specified)
        #[arg(short, long)]
        name: Option<String>,

        /// Number of runs to show
        #[arg(short, long, default_value = "10")]
        limit: usize,

        /// Filter by status (pending, running, success, failed)
        #[arg(long)]
        status: Option<String>,
    },

    /// Register a new feature view from YAML definition
    Register {
        /// Path to feature view YAML file
        #[arg(short, long)]
        file: String,

        /// Force overwrite if exists
        #[arg(long)]
        force: bool,
    },

    /// Delete a feature view
    Delete {
        /// Feature view name
        name: String,

        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,

        /// Also delete stored features
        #[arg(long)]
        with_data: bool,
    },

    /// Sync features from Delta Lake to online store (Redis/PostgreSQL)
    Sync {
        /// Feature view name
        #[arg(short, long)]
        name: String,

        /// Online store URL (redis://host:port or postgresql://host/db)
        #[arg(short, long)]
        url: String,

        /// TTL for features in seconds (optional, no expiration if not set)
        #[arg(long)]
        ttl: Option<u64>,

        /// Batch size for writes (larger = faster, more memory)
        #[arg(long, default_value = "5000")]
        batch_size: usize,

        /// Dry run - show what would be synced without executing
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup tracing based on verbosity
    let filter = match cli.verbose {
        0 => "warn,featureduck=info",
        1 => "info,featureduck=debug",
        2 => "debug",
        _ => "trace",
    };

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)))
        .init();

    // Load configuration
    let config = config::load_config(cli.config.as_deref(), &cli.registry, &cli.storage)?;

    // Execute command
    match cli.command {
        Commands::Materialize {
            name,
            start,
            end,
            dry_run,
            force,
        } => {
            materialize::run(
                &config,
                &name,
                start.as_deref(),
                end.as_deref(),
                dry_run,
                force,
            )
            .await?;
        }

        Commands::Backfill {
            name,
            start_date,
            end_date,
            parallelism,
            dry_run,
        } => {
            backfill::run(&config, &name, &start_date, &end_date, parallelism, dry_run).await?;
        }

        Commands::Validate {
            name,
            check_source,
            check_storage,
            check_schema,
        } => {
            validate::run(&config, &name, check_source, check_storage, check_schema).await?;
        }

        Commands::List { filter, detailed } => {
            list::run(&config, filter.as_deref(), detailed, &cli.output).await?;
        }

        Commands::Show {
            name,
            schema,
            stats,
        } => {
            show::run(&config, &name, schema, stats, &cli.output).await?;
        }

        Commands::Runs {
            name,
            limit,
            status,
        } => {
            runs::run(
                &config,
                name.as_deref(),
                limit,
                status.as_deref(),
                &cli.output,
            )
            .await?;
        }

        Commands::Register { file, force } => {
            commands::register::run(&config, &file, force).await?;
        }

        Commands::Delete {
            name,
            yes,
            with_data,
        } => {
            commands::delete::run(&config, &name, yes, with_data).await?;
        }

        Commands::Sync {
            name,
            url,
            ttl,
            batch_size,
            dry_run,
        } => {
            sync::run(&config, &name, &url, ttl, batch_size, dry_run, &cli.output).await?;
        }
    }

    Ok(())
}
