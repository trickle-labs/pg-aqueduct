use aqueduct_core::ingest::{ingest_from_dbt, IngestChangeKind};
use clap::Args;

#[derive(Debug, Args)]
pub struct IngestArgs {
    /// Source type to ingest from. Currently only `dbt-target` is supported.
    #[arg(long, value_name = "TYPE")]
    pub from: String,

    /// Path to the dbt target directory (contains `manifest.json` and `compiled/`).
    /// Required when `--from dbt-target`.
    #[arg(long, value_name = "PATH")]
    pub target: std::path::PathBuf,

    /// Project directory to write migration files into (default: current directory).
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text (default) or json.
    #[arg(long, default_value = "text")]
    pub format: String,
}

pub async fn run(args: IngestArgs) -> anyhow::Result<()> {
    match args.from.as_str() {
        "dbt-target" => run_dbt_ingest(args).await,
        other => anyhow::bail!(
            "Unknown ingest source: '{}'. Supported sources: dbt-target",
            other
        ),
    }
}

async fn run_dbt_ingest(args: IngestArgs) -> anyhow::Result<()> {
    let dbt_target_dir = args.target.clone();
    let project_dir = args.project_dir.clone();

    if !dbt_target_dir.exists() {
        anyhow::bail!(
            "dbt target directory does not exist: {}",
            dbt_target_dir.display()
        );
    }

    tracing::info!("Ingesting dbt artefacts from {}", dbt_target_dir.display());

    let result = ingest_from_dbt(&dbt_target_dir, &project_dir)?;

    match args.format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        _ => {
            if result.streams_total == 0 {
                println!(
                    "No stream_table models found in {}.",
                    dbt_target_dir.display()
                );
                println!("Ensure models are configured with `materialized: stream_table` in dbt.");
                return Ok(());
            }

            for change in &result.changes {
                let symbol = match change.kind {
                    IngestChangeKind::Created => "+",
                    IngestChangeKind::Updated => "~",
                    IngestChangeKind::Unchanged => " ",
                };
                println!("  {} {} {}", symbol, change.kind, change.path.display());
            }

            println!();
            if result.has_changes() {
                println!(
                    "✓ Ingested {} stream model(s) and {} source(s) from dbt target.",
                    result.streams_changed + result.sources_changed,
                    result.sources_total,
                );
            } else {
                println!(
                    "✓ {} stream model(s) and {} source(s) are already up to date.",
                    result.streams_total, result.sources_total,
                );
            }

            println!();
            println!("Next steps:");
            println!(
                "  1. Review the generated migration files in {}/migrations/",
                project_dir.display()
            );
            println!(
                "  2. Run: aqueduct validate --project-dir {}",
                project_dir.display()
            );
            println!("  3. Run: aqueduct plan --to <target>");
        }
    }

    Ok(())
}
