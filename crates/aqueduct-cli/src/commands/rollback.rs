use aqueduct_core::{
    config::AqueductConfig,
    dag::{topological_sort, DagState},
    diff::compute_diff,
    executor::PlanExecutor,
    live_state::read_live_state,
    plan::build_plan,
};
use clap::Args;

use super::connect_and_migrate;

#[derive(Debug, Args)]
pub struct RollbackArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Roll back to a specific version (default: previous version).
    #[arg(long)]
    pub to_version: Option<u64>,

    /// Accept data loss for rebuild-class rollbacks where the lossless window has passed.
    #[arg(long)]
    pub accept_data_loss: bool,

    /// Show what would be done without executing.
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: RollbackArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let client = connect_and_migrate(&dsn).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Get the current version.
    let current_version =
        aqueduct_core::live_state::get_latest_dag_version(&client, &project_name).await?;
    let Some(current_v) = current_version else {
        anyhow::bail!(
            "No version history found for project '{}'. Cannot roll back.",
            project_name
        );
    };

    // Determine target rollback version.
    let target_version =
        args.to_version
            .unwrap_or_else(|| if current_v > 1 { current_v - 1 } else { 1 });

    if target_version >= current_v {
        anyhow::bail!(
            "Target version v{} is not older than the current version v{}.",
            target_version,
            current_v
        );
    }

    // Load the spec from the target version.
    let row = client
        .query_opt(
            "SELECT spec_jsonb FROM aqueduct.dag_versions WHERE project = $1 AND version = $2",
            &[&project_name, &(target_version as i64)],
        )
        .await?;

    let Some(row) = row else {
        anyhow::bail!(
            "Version v{} not found for project '{}'.",
            target_version,
            project_name
        );
    };

    let _spec_jsonb: serde_json::Value = row.get(0);

    // C1 fix: deserialize spec_jsonb from DB into DagState.
    // If spec_jsonb is a populated object (recorded after the M9 fix), use it as desired.
    // Otherwise fall back to reading migration files from disk.
    let spec_jsonb: serde_json::Value = row.get(0);

    let vars = config
        .as_ref()
        .and_then(|c| {
            args.to
                .as_deref()
                .and_then(|t| c.targets.get(t))
                .map(|tc| tc.vars.clone())
        })
        .unwrap_or_default();

    let desired: DagState = if spec_jsonb.is_object()
        && !spec_jsonb.as_object().map(|m| m.is_empty()).unwrap_or(true)
    {
        serde_json::from_value::<DagState>(spec_jsonb)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize prior spec: {}", e))?
    } else {
        // Fallback: re-read migration files (for versions recorded before M9 fix).
        let files = aqueduct_core::parser::load_migrations(&args.project_dir, &vars)?;
        aqueduct_core::dag::build_dag_state(&files, true)?
    };
    let actual = read_live_state(&client, Some(&project_name)).await?;

    let next_version = current_v + 1;
    let diff = compute_diff(&desired, &actual);
    let topo = topological_sort(&desired)?;
    let plan = build_plan(&project_name, Some(current_v), next_version, &diff, &topo)?;

    if plan.summary.is_empty() {
        println!(
            "Nothing to roll back. Current state matches v{}.",
            target_version
        );
        return Ok(());
    }

    // S-11: If the rollback plan includes destructive (rebuild) steps, require
    // --accept-data-loss to proceed.
    if plan.summary.rebuild_count > 0 && !args.accept_data_loss {
        let lossy_changes: Vec<_> = plan
            .summary
            .changes
            .iter()
            .filter(|c| c.class == "rebuild")
            .map(|c| format!("  {} {} ({})", c.symbol, c.name, c.description))
            .collect();
        return Err(anyhow::anyhow!(
            "Rollback would cause data loss for {} rebuild-class change(s):\n{}\n\n\
             Pass --accept-data-loss to proceed.",
            plan.summary.rebuild_count,
            lossy_changes.join("\n")
        ));
    }

    println!(
        "Rolling back from v{} to v{} (applying {} change{})...",
        current_v,
        target_version,
        plan.summary.changes.len(),
        if plan.summary.changes.len() == 1 {
            ""
        } else {
            "s"
        }
    );

    if args.dry_run {
        println!("Dry run: no changes applied.");
        return Ok(());
    }

    let executor = PlanExecutor::new(&client, &project_name, env!("CARGO_PKG_VERSION"), false)
        .with_desired_state(desired)
        .with_connection_string(dsn.clone());
    let result = executor.execute(&plan).await?;

    println!("✓ Rollback applied. New version: v{}", result.dag_version);
    Ok(())
}
