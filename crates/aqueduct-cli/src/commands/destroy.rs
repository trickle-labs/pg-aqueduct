use aqueduct_core::{
    config::AqueductConfig,
    destroy::{destroy_project, DestroyOptions},
};
use clap::Args;

use super::connect_and_migrate;

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

    /// Also drop dependent database objects (views, etc.) via CASCADE.
    /// Without this flag, `destroy` refuses if any stream table has dependents.
    #[arg(long)]
    pub force_cascade: bool,

    /// Skip ownership verification. By default, `destroy` refuses to drop
    /// stream tables that are not registered as owned by this project.
    #[arg(long)]
    pub force_unowned: bool,
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
        // ERG-2 (v0.14): Use anyhow::bail! so the error flows through main's
        // error handler and exits with code 2 (rather than eprintln + exit(1)).
        anyhow::bail!(
            "`aqueduct destroy` is irreversible and destructive.\n\
             Pass --confirm to execute, or --dry-run to preview what would be destroyed."
        );
    }

    // M-5 (v0.18): use connect_and_migrate so a stale catalog is auto-migrated
    // before destroy queries it, preventing missing-column errors.
    let catalog_schema = config
        .as_ref()
        .and_then(|c| aqueduct_core::catalog::CatalogSchema::new(&c.project.catalog_schema).ok())
        .unwrap_or_default();
    let client = connect_and_migrate(&dsn, &catalog_schema).await?;

    let options = DestroyOptions {
        project: project_name.clone(),
        dry_run: args.dry_run,
        force_cascade: args.force_cascade,
        force_unowned: args.force_unowned,
        catalog_schema: catalog_schema.clone(),
    };

    let result = destroy_project(&client, &options).await?;

    if result.is_empty() {
        println!(
            "Nothing to destroy. Project '{}' has no managed resources.",
            project_name
        );
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
    println!("  ✓ deleted {} catalog row(s)", result.catalog_rows_deleted);

    Ok(())
}
