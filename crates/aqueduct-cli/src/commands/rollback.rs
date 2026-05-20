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

/// Rollback risk classification for a set of plan changes (M-10 / v0.13).
#[derive(Debug, Clone, PartialEq, Eq)]
enum RollbackClass {
    /// Free/in-place changes — no data loss, can always roll back.
    Safe,
    /// Rebuild within the lossless point-in-time window.
    PointInTime,
    /// Rebuild outside the lossless window or blue/green schema expired.
    DataLoss,
}

impl RollbackClass {
    fn symbol(&self) -> &'static str {
        match self {
            RollbackClass::Safe => "✓",
            RollbackClass::PointInTime => "⚠",
            RollbackClass::DataLoss => "✗",
        }
    }
    fn label(&self) -> &'static str {
        match self {
            RollbackClass::Safe => "Safe",
            RollbackClass::PointInTime => "PointInTime",
            RollbackClass::DataLoss => "DataLoss",
        }
    }
}

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

    /// Allow PointInTime rollbacks even when close to window expiry.
    #[arg(long)]
    pub within_window: bool,

    /// Show what would be done without executing.
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: RollbackArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let catalog_schema = config
        .as_ref()
        .and_then(|c| aqueduct_core::catalog::CatalogSchema::new(&c.project.catalog_schema).ok())
        .unwrap_or_default();

    let client = connect_and_migrate(&dsn, &catalog_schema).await?;

    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Get the current version.
    let current_version =
        aqueduct_core::live_state::get_latest_dag_version(&client, &project_name, &catalog_schema)
            .await?;
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
    let actual = read_live_state(&client, Some(&project_name), &catalog_schema).await?;

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

    // M-10: Classify rollback risk by querying the applied_at timestamp of the
    // current version.  Rebuilds applied within the last hour are "PointInTime"
    // (pg_trickle WAL retention window); older rebuilds are "DataLoss".
    let applied_at: Option<chrono::DateTime<chrono::Utc>> = client
        .query_opt(
            "SELECT applied_at FROM aqueduct.dag_versions WHERE project = $1 AND version = $2",
            &[&project_name, &(current_v as i64)],
        )
        .await
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<_, chrono::DateTime<chrono::Utc>>(0).ok());

    let lossless_window_secs: i64 = 3600; // 1 hour default
    let in_window = applied_at
        .map(|t| {
            let age = chrono::Utc::now().signed_duration_since(t);
            age.num_seconds() < lossless_window_secs
        })
        .unwrap_or(false);

    // Classify overall rollback risk.
    let overall_class = if plan.summary.rebuild_count == 0 {
        RollbackClass::Safe
    } else if in_window {
        RollbackClass::PointInTime
    } else {
        RollbackClass::DataLoss
    };

    // Render rollback plan with classification.
    println!("Rollback plan: v{} → v{}", current_v, target_version);
    println!(
        "Risk class: {} [{}]",
        overall_class.symbol(),
        overall_class.label()
    );
    println!();
    for change in &plan.summary.changes {
        let class = if change.class == "rebuild" {
            if in_window {
                &RollbackClass::PointInTime
            } else {
                &RollbackClass::DataLoss
            }
        } else {
            &RollbackClass::Safe
        };
        println!(
            "  {} {} {} ({})",
            class.symbol(),
            change.symbol,
            change.name,
            change.description
        );
    }
    println!();

    // S-11: Guard against data loss.
    if overall_class == RollbackClass::DataLoss && !args.accept_data_loss {
        let lossy_changes: Vec<_> = plan
            .summary
            .changes
            .iter()
            .filter(|c| c.class == "rebuild")
            .map(|c| format!("  {} {} ({})", c.symbol, c.name, c.description))
            .collect();
        return Err(anyhow::anyhow!(
            "Rollback would cause data loss for {} rebuild-class change(s):\n{}\n\n\
             The lossless point-in-time window has expired. \
             Pass --accept-data-loss to proceed.",
            plan.summary.rebuild_count,
            lossy_changes.join("\n")
        ));
    }

    // M-10: Warn on PointInTime rollbacks.
    if overall_class == RollbackClass::PointInTime && !args.within_window && !args.accept_data_loss
    {
        return Err(anyhow::anyhow!(
            "Rollback includes rebuild-class changes within the lossless window \
             (applied < {}s ago).\n\
             Pass --within-window to proceed, or --accept-data-loss to skip the window check.",
            lossless_window_secs
        ));
    }

    if args.dry_run {
        println!("Dry run: no changes applied.");
        return Ok(());
    }

    println!(
        "Applying rollback: v{} → v{} ({} change{})...",
        current_v,
        target_version,
        plan.summary.changes.len(),
        if plan.summary.changes.len() == 1 {
            ""
        } else {
            "s"
        }
    );

    let executor = PlanExecutor::new(&client, &project_name, env!("CARGO_PKG_VERSION"), false)
        .with_desired_state(desired)
        .with_connection_string(dsn.clone())
        .with_catalog_schema(catalog_schema);
    let result = executor.execute(&plan).await?;

    println!("✓ Rollback applied. New version: v{}", result.dag_version);
    Ok(())
}
