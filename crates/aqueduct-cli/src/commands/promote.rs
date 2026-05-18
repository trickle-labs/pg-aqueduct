use aqueduct_core::{
    config::AqueductConfig,
    executor::PlanExecutor,
    promote::{compute_promotion_plan, validate_source_clean, PromoteOptions},
    renderer::render_plan_text,
};
use clap::Args;

use super::connect;

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

    // Resolve destination target DSN.
    let dest_target = config.target(&args.to)?;
    let dest_dsn = dest_target.dsn.clone();
    let dest_vars = dest_target.vars.clone();

    // Load migration files with destination variables.
    let files =
        aqueduct_core::parser::load_migrations(&args.project_dir, &dest_vars)?;

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
        validate_source_clean(&source_client, &files, &project_name).await?;
        println!("✓ Source environment '{}' is clean.", args.from);
    }

    // Connect to destination and compute the plan.
    let dest_client = connect(&dest_dsn).await?;
    let plan = compute_promotion_plan(&dest_client, &files, &promote_opts).await?;

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
    let executor = PlanExecutor::new(
        &dest_client,
        &project_name,
        env!("CARGO_PKG_VERSION"),
        false,
    );
    let new_version = executor.execute(&plan).await?;

    println!(
        "✓ Promoted {} → {}. New version: v{}",
        args.from, args.to, new_version
    );
    Ok(())
}
