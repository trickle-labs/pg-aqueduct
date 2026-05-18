use aqueduct_core::{
    config::AqueductConfig,
    destroy::{destroy_project, DestroyOptions},
};
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct DestroyArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Confirm the destructive operation (required unless --dry-run is set).
    #[arg(long)]
    pub confirm: bool,

    /// Show what would be destroyed without executing.
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: DestroyArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    if !args.dry_run && !args.confirm {
        eprintln!(
            "error: `aqueduct destroy` is irreversible and destructive.\n\
             Pass --confirm to execute, or --dry-run to preview what would be destroyed."
        );
        std::process::exit(1);
    }

    let client = connect(&dsn).await?;

    let options = DestroyOptions {
        project: project_name.clone(),
        dry_run: args.dry_run,
    };

    let result = destroy_project(&client, &options).await?;

    if result.is_empty() {
        println!("Nothing to destroy. Project '{}' has no managed resources.", project_name);
        return Ok(());
    }

    if args.dry_run {
        println!("Dry run — would destroy the following resources:");
        for t in &result.stream_tables_dropped {
            println!("  stream table: {}", t);
        }
        for v in &result.consumer_views_dropped {
            println!("  consumer view: {}", v);
        }
        println!("  catalog rows: (would be deleted)");
        return Ok(());
    }

    println!("Destroyed project '{}':", project_name);
    for t in &result.stream_tables_dropped {
        println!("  ✓ dropped stream table: {}", t);
    }
    for v in &result.consumer_views_dropped {
        println!("  ✓ dropped consumer view: {}", v);
    }
    println!(
        "  ✓ deleted {} catalog row(s)",
        result.catalog_rows_deleted
    );

    Ok(())
}
