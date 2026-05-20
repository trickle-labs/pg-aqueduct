use aqueduct_core::{
    config::AqueductConfig,
    dag::build_dag_state,
    executor::PlanExecutor,
    promote::{compute_promotion_plan, validate_source_clean, PromoteOptions},
    renderer::render_plan_text,
};
use clap::Args;

use super::{connect, connect_and_migrate};

#[derive(Debug, Args)]
pub struct PromoteArgs {
    /// Source environment name (e.g. "dev").
    #[arg(long)]
    pub from: String,

    /// Destination environment name (e.g. "staging").
    #[arg(long)]
    pub to: String,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Skip the source-clean validation (use in CI when source is known good).
    #[arg(long)]
    pub skip_source_check: bool,

    /// Show what would be done without executing.
    #[arg(long)]
    pub dry_run: bool,

    /// Auto-approve the promotion (skip interactive prompt).
    #[arg(long)]
    pub yes: bool,
}

pub async fn run(args: PromoteArgs) -> anyhow::Result<()> {
    let config = AqueductConfig::load(&args.project_dir)?;
    let project_name = config.project.name.clone();
    let catalog_schema = aqueduct_core::catalog::CatalogSchema::new(&config.project.catalog_schema)
        .unwrap_or_default();

    // Resolve destination target DSN.
    let dest_target = config.target(&args.to)?;
    let dest_dsn = dest_target.dsn.clone();
    let dest_vars = dest_target.vars.clone();

    // Load migration files with destination variables.
    let files = aqueduct_core::parser::load_migrations(&args.project_dir, &dest_vars)?;
    // Build desired state to pass to the executor for spec_jsonb recording (CORR-5).
    let desired_state = build_dag_state(&files, true).ok();

    let promote_opts = PromoteOptions {
        from_env: args.from.clone(),
        to_env: args.to.clone(),
        project: project_name.clone(),
        dry_run: args.dry_run,
    };

    // Optionally validate that the source environment is in a clean state.
    if !args.skip_source_check {
        let source_target = config.target(&args.from)?;
        let source_client = connect(&source_target.dsn).await?;
        validate_source_clean(&source_client, &files, &project_name, &catalog_schema).await?;
        println!("✓ Source environment '{}' is clean.", args.from);
    }

    // Connect to destination using connect_and_migrate so the catalog is
    // self-migrated before any plan steps run (CORR-5 / v0.14).
    let dest_client = connect_and_migrate(&dest_dsn, &catalog_schema).await?;
    let plan = compute_promotion_plan(&dest_client, &files, &promote_opts, &catalog_schema).await?;

    if plan.summary.is_empty() {
        println!(
            "No changes to promote. Destination '{}' is already up to date.",
            args.to
        );
        return Ok(());
    }

    // Show the plan.
    println!("{}", render_plan_text(&plan, None, None));

    if args.dry_run {
        println!("Dry run: no changes applied.");
        return Ok(());
    }

    // Prompt for confirmation unless --yes was passed.
    if !args.yes {
        eprint!(
            "Promote {} → {} ? This will apply the above plan. [y/N] ",
            args.from, args.to
        );
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Apply the plan.
    // CORR-5: pass desired_state and connection_string so spec_jsonb is
    // populated and the heartbeat can renew the lock.
    let mut executor = PlanExecutor::new(
        &dest_client,
        &project_name,
        env!("CARGO_PKG_VERSION"),
        false,
    )
    .with_connection_string(dest_dsn.clone())
    .with_catalog_schema(catalog_schema);
    if let Some(state) = desired_state {
        executor = executor.with_desired_state(state);
    }
    let result = executor.execute(&plan).await?;

    println!(
        "✓ Promoted {} → {}. New version: v{}",
        args.from, args.to, result.dag_version
    );
    Ok(())
}
