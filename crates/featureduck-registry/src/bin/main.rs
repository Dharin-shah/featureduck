use chrono::Utc;
use clap::{Parser, Subcommand};
use featureduck_registry::{FeatureRegistry, FeatureViewDef, RegistryConfig, RunStatus};

#[derive(Parser)]
#[command(
    name = "featureduck-registry",
    version,
    about = "FeatureDuck Registry CLI"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a SQLite registry at the given path
    Init { path: String },
    /// Register a simple feature view (demo)
    Register { name: String, path: String },
    /// List feature views
    List {
        #[arg(default_value_t = String::new())]
        filter: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            let reg = FeatureRegistry::new(RegistryConfig::sqlite(&path)).await?;
            // touching schema happens in new(); just confirm accessible
            let _ = reg.list_feature_views(None).await?;
            println!("Initialized registry at {}", path);
        }
        Commands::Register { name, path } => {
            let reg = FeatureRegistry::new(RegistryConfig::sqlite(&path)).await?;
            let view = FeatureViewDef {
                name: name.clone(),
                version: 1,
                source_type: "delta".to_string(),
                source_path: "s3://bucket/path".to_string(),
                entities: vec!["user_id".to_string()],
                transformations: "{}".to_string(),
                timestamp_field: Some("__timestamp".to_string()),
                ttl_days: Some(30),
                batch_schedule: Some("0 * * * *".to_string()),
                description: Some("demo".to_string()),
                tags: vec![],
                owner: Some("owner".to_string()),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            reg.register_feature_view(&view).await?;
            let run = reg.create_run(&name).await?;
            reg.update_run_status(run, RunStatus::Success, Some(0), None)
                .await?;
            println!("Registered view '{}' in {}", name, path);
        }
        Commands::List { filter } => {
            let reg = FeatureRegistry::new(RegistryConfig::sqlite("./registry.db")).await?;
            let views = if filter.is_empty() {
                reg.list_feature_views(None).await?
            } else {
                reg.list_feature_views(Some(&filter)).await?
            };
            for v in views {
                println!("{} v{}", v.name, v.version);
            }
        }
    }

    Ok(())
}
