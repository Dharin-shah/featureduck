//! Delete feature view command

use anyhow::{Context, Result};
use std::io::{self, Write};
use std::path::Path;

use crate::config::CliConfig;
use crate::output::{print_error, print_info, print_success, print_warning};

use super::create_registry;

pub async fn run(config: &CliConfig, name: &str, yes: bool, with_data: bool) -> Result<()> {
    let registry = create_registry(config).await?;

    // Check if feature view exists
    let view = match registry.get_feature_view(name).await {
        Ok(v) => v,
        Err(_) => {
            print_error(&format!("Feature view '{}' not found", name));
            return Ok(());
        }
    };

    print_info(&format!("Feature view: {}", view.name));
    print_info(&format!("Version: {}", view.version));
    print_info(&format!("Source: {}", view.source_path));

    // Check for existing data
    let storage_path = format!("{}/{}", config.storage.base_path, name);
    let has_data = Path::new(&storage_path).exists();

    if has_data {
        print_warning(&format!(
            "Feature view has stored data at: {}",
            storage_path
        ));
    }

    // Confirm deletion
    if !yes {
        print!(
            "\nAre you sure you want to delete feature view '{}'? [y/N] ",
            name
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            print_info("Deletion cancelled");
            return Ok(());
        }

        if with_data && has_data {
            print_warning("This will also delete all stored feature data!");
            print!("Type the feature view name to confirm data deletion: ");
            io::stdout().flush()?;

            let mut confirm = String::new();
            io::stdin().read_line(&mut confirm)?;

            if confirm.trim() != name {
                print_error("Name did not match. Deletion cancelled.");
                return Ok(());
            }
        }
    }

    // Delete from registry
    registry
        .delete_feature_view(name)
        .await
        .context("Failed to delete feature view from registry")?;

    print_success(&format!("Deleted feature view '{}' from registry", name));

    // Delete data if requested
    if with_data && has_data {
        match std::fs::remove_dir_all(&storage_path) {
            Ok(()) => {
                print_success(&format!("Deleted feature data at: {}", storage_path));
            }
            Err(e) => {
                print_error(&format!(
                    "Failed to delete data directory: {}. You may need to delete it manually.",
                    e
                ));
            }
        }
    } else if has_data {
        print_info(&format!("Feature data still exists at: {}", storage_path));
        print_info("Use --with-data to also delete stored features");
    }

    Ok(())
}
