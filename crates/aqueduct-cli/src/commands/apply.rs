use aqueduct_core::{
    config::AqueductConfig,
    dag::{build_dag_state, topological_sort},
    diff::compute_diff,
    executor::PlanExecutor,
    live_state::read_live_state,
    plan::build_plan,
    renderer::render_plan_text,
};
use clap::Args;

#[derive(Debug, Args)]
pub struct ApplyArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Show what would be done without executing.
    #[arg(long)]
    pub dry_run: bool,

    /// Override the maintenance window restriction.
    #[arg(long)]
    pub ignore_maintenance_window: bool,

    /// Allow rebuild-class migrations even when allow_full_refresh = false.
    #[arg(long)]
    pub allow_rebuild: bool,

    /// Resume an interrupted migration.
    #[arg(long)]
    pub resume: bool,

    /// Print the plan before applying.
    #[arg(long, default_value = "true")]
    pub print_plan: bool,

    /// Skip the interactive confirmation prompt (for CI and scripted use).
    #[arg(long, short = 'y')]
    pub yes: bool,

    /// Allow DSN with embedded plaintext password (not recommended outside CI/dev).
    #[arg(long)]
    pub allow_plaintext_password: bool,
}

pub async fn run(args: ApplyArgs) -> anyhow::Result<()> {
    let dsn = super::resolve_dsn_with_opts(
        args.dsn.as_deref(),
        args.to.as_deref(),
        &args.project_dir,
        args.allow_plaintext_password,
    )
    .await?;

    // S-07: For dry-run mode, use a plain connect() to avoid accidentally
    // upgrading the catalog schema when only previewing.
    let client = if args.dry_run {
        super::connect(&dsn).await?
    } else {
        super::connect_and_migrate(&dsn).await?
    };

    // Load config.
    let config = AqueductConfig::load(&args.project_dir).ok();
    let vars = config
        .as_ref()
        .and_then(|c| {
            args.to
                .as_deref()
                .and_then(|t| c.targets.get(t))
                .map(|tc| tc.vars.clone())
        })
        .unwrap_or_default();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Read migration files.
    let files = aqueduct_core::parser::load_migrations(&args.project_dir, &vars)?;
    let desired = build_dag_state(&files, true)?;
    let actual = read_live_state(&client, Some(&project_name)).await?;

    let current_version =
        aqueduct_core::live_state::get_latest_dag_version(&client, &project_name).await?;
    let next_version = current_version.map(|v| v + 1).unwrap_or(1);

    let diff = compute_diff(&desired, &actual);
    let topo_order = topological_sort(&desired)?;
    let plan = build_plan(
        &project_name,
        current_version,
        next_version,
        &diff,
        &topo_order,
    )?;

    if plan.summary.is_empty() {
        println!("No changes to apply. Project is up to date.");
        return Ok(());
    }

    if args.print_plan || !args.yes {
        println!("{}", render_plan_text(&plan, None, None));
    }

    if args.dry_run {
        println!("Dry run: no changes applied.");
        return Ok(());
    }

    // Interactive confirmation prompt when running in a TTY and --yes not passed.
    if !args.yes && is_interactive_tty() {
        use std::io::Write;
        let target_label = args.to.as_deref().unwrap_or("target");
        print!(
            "Apply {} change{} to '{}'? [y/N] ",
            plan.summary.creates + plan.summary.drops + plan.summary.alters,
            if plan.summary.creates + plan.summary.drops + plan.summary.alters == 1 {
                ""
            } else {
                "s"
            },
            target_label
        );
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let answer = input.trim().to_lowercase();
        if answer != "y" && answer != "yes" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Check allow_full_refresh setting.
    if let Some(ref cfg) = config {
        if !cfg.apply.allow_full_refresh && !args.allow_rebuild {
            let has_rebuild = plan.summary.rebuild_count > 0;
            if has_rebuild {
                anyhow::bail!(
                    "Plan contains rebuild-class steps but allow_full_refresh = false in aqueduct.toml.\n\
                     Pass --allow-rebuild to override for this run."
                );
            }
        }
    }

    // Check maintenance window.
    if !args.ignore_maintenance_window {
        if let Some(ref cfg) = config {
            if cfg
                .apply
                .plan_requires_window(plan.summary.rebuild_count, plan.summary.blue_green_count)
            {
                let now = chrono::Utc::now();
                if !cfg.apply.is_in_maintenance_window(now) {
                    anyhow::bail!(
                        "Plan contains rebuild/blue-green steps but current time is outside the \
                         configured maintenance window '{}'.\n\
                         Pass --ignore-maintenance-window to override for this run.",
                        cfg.apply.maintenance_window.as_deref().unwrap_or("")
                    );
                }
            }
        }
    }

    let executor = PlanExecutor::new(&client, &project_name, env!("CARGO_PKG_VERSION"), false)
        .with_resume(args.resume)
        .with_connection_string(dsn.clone())
        .with_desired_state(desired);
    let result = executor.execute(&plan).await?;

    // U-05: Emit a structured JSON event for CI parsers. Separates migration_id
    // (aqueduct.migrations row id) from dag_version (aqueduct.dag_versions bigserial).
    let migration_metadata = serde_json::json!({
        "event": "apply_complete",
        "migration_id": result.migration_id,
        "dag_version": result.dag_version,
        "from_version": plan.from_version,
        "to_version": result.dag_version,
        "project": project_name,
    });
    tracing::info!(
        event = "apply_complete",
        migration_id = result.migration_id,
        dag_version = result.dag_version,
        from_version = plan.from_version,
        to_version = result.dag_version,
        project = %project_name,
        "Migration applied successfully"
    );
    // Also write to stderr for parsers that read the log stream.
    eprintln!("{}", serde_json::to_string(&migration_metadata)?);

    println!(
        "✓ Applied successfully. New version: v{}",
        result.dag_version
    );
    Ok(())
}

/// Returns true if stdout is an interactive terminal (not piped or redirected).
fn is_interactive_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}
