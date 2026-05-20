use aqueduct_core::executor::import_from_live;
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// PostgreSQL connection string to import from.
    #[arg(long)]
    pub from: Option<String>,

    /// Target name from aqueduct.toml (used as --from source).
    #[arg(long)]
    pub from_target: Option<String>,

    /// Output directory for the generated project.
    #[arg(long, default_value = ".")]
    pub output: std::path::PathBuf,

    /// Project directory (for resolving config).
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Exclude tables matching these patterns (can be specified multiple times).
    #[arg(long)]
    pub exclude_pattern: Vec<String>,

    /// Do not apply default exclusion patterns (_pg_ripple.*, _pg_eddy.*, _riverbank.*).
    #[arg(long)]
    pub no_default_exclusions: bool,
}

pub async fn run(args: ImportArgs) -> anyhow::Result<()> {
    let dsn = if let Some(d) = &args.from {
        aqueduct_core::config::resolve_env_vars(d)?
    } else if let Some(t) = &args.from_target {
        super::resolve_dsn(None, Some(t), &args.project_dir).await?
    } else {
        anyhow::bail!("Either --from <DSN> or --from-target <target> must be specified.");
    };

    let client = connect(&dsn).await?;

    // Build exclude patterns.
    let mut exclude_patterns = args.exclude_pattern.clone();
    if !args.no_default_exclusions {
        exclude_patterns.push(r"^_pg_ripple\.".to_string());
        exclude_patterns.push(r"^_pg_eddy\.".to_string());
        exclude_patterns.push(r"^_riverbank\.".to_string());
    }

    let project_name = args
        .output
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("imported-project")
        .to_string();

    println!("Importing from pg_trickle catalog...");

    let count = import_from_live(
        &client,
        &project_name,
        &args.output,
        &exclude_patterns,
        &aqueduct_core::catalog::CatalogSchema::default(),
    )
    .await?;

    println!(
        "✓ Imported {} stream table{}.",
        count,
        if count == 1 { "" } else { "s" }
    );
    println!("  Output: {}", args.output.display());
    println!();
    println!("Next steps:");
    println!(
        "  1. Edit {} to set your target DSN",
        args.output.join("aqueduct.toml").display()
    );
    println!("  2. Run: aqueduct init --to <target>");
    println!("  3. Run: aqueduct plan --to <target>  (should be a no-op)");

    Ok(())
}
