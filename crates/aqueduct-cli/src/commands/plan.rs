use aqueduct_core::{
    config::AqueductConfig,
    cost::estimate_plan_cost,
    dag::{build_dag_state, topological_sort},
    diff::compute_diff,
    live_state::{check_pgtrickle_version, read_live_state},
    plan::build_plan,
    renderer::{
        render_plan_json, render_plan_markdown, render_plan_text, render_plan_text_with_cost,
    },
};
use clap::Args;

use super::connect_read_only;

#[derive(Debug, Args)]
pub struct PlanArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text (default), json, markdown, or yaml.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Exit non-zero if the plan is non-empty (useful in CI).
    /// Synonymous with the default exit-code behaviour (exit 1 for non-empty plans).
    #[arg(long)]
    pub fail_if_changed: bool,

    /// Exit non-zero if drift is detected between live state and last-applied version.
    #[arg(long)]
    pub fail_on_drift: bool,

    /// Check IVM supportability of all queries.
    #[arg(long, default_value = "true")]
    pub validate_ivm: bool,

    /// Show per-step cost estimates (row count, estimated duration).
    #[arg(long)]
    pub explain_cost: bool,

    /// Write the plan to a JSON file for use with `apply --plan`.
    #[arg(long)]
    pub out: Option<std::path::PathBuf>,

    /// Allow DSN with embedded plaintext password (not recommended outside CI/dev).
    #[arg(long)]
    pub allow_plaintext_password: bool,
}

pub async fn run(args: PlanArgs) -> anyhow::Result<()> {
    let dsn = super::resolve_dsn_with_opts(
        args.dsn.as_deref(),
        args.to.as_deref(),
        &args.project_dir,
        args.allow_plaintext_password,
    )
    .await?;

    let client = connect_read_only(&dsn).await?;

    // Load config and migration files.
    let (config, vars) = load_config_and_vars(&args.project_dir, args.to.as_deref());
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Read migration files.
    let files = aqueduct_core::parser::load_migrations(&args.project_dir, &vars)?;

    // Build desired DAG state.
    let desired = build_dag_state(&files, true)?;

    // Validate queries if requested.
    if args.validate_ivm {
        for table in &desired.stream_tables {
            if !table.query.is_empty() {
                use aqueduct_core::dag::RefreshMode;
                if table.refresh_mode == RefreshMode::Differential {
                    aqueduct_core::validate::validate_ivm_supportability(
                        &table.query,
                        &table.qualified_name.to_string(),
                    )?;
                } else {
                    aqueduct_core::validate::validate_sql_syntax(
                        &table.query,
                        &table.qualified_name.to_string(),
                    )?;
                }
            }
        }
    }

    // Read actual live state.
    let actual = read_live_state(&client, Some(&project_name)).await?;

    // Get current version.
    let current_version =
        aqueduct_core::live_state::get_latest_dag_version(&client, &project_name).await?;
    let next_version = current_version.map(|v| v + 1).unwrap_or(1);

    // Compute diff.
    let diff = compute_diff(&desired, &actual);

    // Check for drift if requested.
    if args.fail_on_drift {
        // M-2 (v0.18): count all three delta collections to detect consumer-layer
        // and source-layer drift, not just stream-table deltas.
        let drift_count = diff
            .deltas
            .iter()
            .filter(|d| d.kind != aqueduct_core::diff::DeltaKind::Unchanged)
            .count()
            + diff
                .source_deltas
                .iter()
                .filter(|s| s.kind != aqueduct_core::diff::SourceDeltaKind::Unchanged)
                .count()
            + diff
                .consumer_deltas
                .iter()
                .filter(|c| c.kind != aqueduct_core::diff::ConsumerDeltaKind::Unchanged)
                .count();
        if drift_count > 0 {
            anyhow::bail!(
                "Drift detected: {} change{} between live state and desired state. \
                 Run `aqueduct plan` to see the full plan.",
                drift_count,
                if drift_count == 1 { "" } else { "s" }
            );
        }
    }

    // Compute topological order.
    let topo_order = topological_sort(&desired)?;

    // Build plan.
    let mut plan = build_plan(
        &project_name,
        current_version,
        next_version,
        &diff,
        &topo_order,
    )?;

    // M-09: Stamp spec_hash for stale-plan detection in `apply --plan`.
    plan.spec_hash = {
        use sha2::{Digest, Sha256};
        let spec_json = serde_json::to_string(&desired).unwrap_or_default();
        format!("{:x}", Sha256::digest(spec_json.as_bytes()))
    };

    // --out: write plan JSON to file for `apply --plan`.
    if let Some(ref out_path) = args.out {
        let plan_json = serde_json::to_string_pretty(&plan)?;
        std::fs::write(out_path, &plan_json)
            .map_err(|e| anyhow::anyhow!("Cannot write plan to '{}': {}", out_path.display(), e))?;
        eprintln!("Plan written to '{}'", out_path.display());
    }

    // Get versions for the renderer.
    let pgtrickle_version = check_pgtrickle_version(&client).await?;
    let pg_version: Option<String> = client
        .query_one("SELECT current_setting('server_version')", &[])
        .await
        .map(|r| r.get(0))
        .ok();

    // Render the plan.
    let output = match args.format.as_str() {
        "json" => render_plan_json(&plan),
        "markdown" | "md" => render_plan_markdown(&plan),
        "yaml" | "yml" => render_plan_yaml(&plan),
        _ => {
            if args.explain_cost {
                let cost = estimate_plan_cost(&client, &plan, None).await?;
                render_plan_text_with_cost(
                    &plan,
                    pg_version.as_deref(),
                    pgtrickle_version.as_deref(),
                    &cost,
                )
            } else {
                render_plan_text(&plan, pg_version.as_deref(), pgtrickle_version.as_deref())
            }
        }
    };

    println!("{}", output);

    // U-02: Exit codes: 0 = empty plan OR non-empty plan with no exit flag,
    // 1 = non-empty plan AND (--fail-if-changed OR --fail-on-drift), 2 = error.
    // Default behaviour is exit 0 for both empty and non-empty plans.
    if !plan.summary.is_empty() && (args.fail_if_changed || args.fail_on_drift) {
        // Non-empty plan with an explicit "fail" flag → exit 1.
        std::process::exit(1);
    }

    Ok(())
}

/// YAML renderer for plan output using serde_yaml (ERG-3).
///
/// Serialises the `Plan` struct directly so that special characters in project
/// names (quotes, colons, newlines) are correctly escaped.
fn render_plan_yaml(plan: &aqueduct_core::plan::Plan) -> String {
    // Build a lightweight summary struct so the YAML output is stable and
    // doesn't include internal executor-only fields.
    #[derive(serde::Serialize)]
    struct PlanYaml<'a> {
        project: &'a str,
        from_version: Option<u64>,
        to_version: u64,
        is_empty: bool,
        creates: usize,
        drops: usize,
        alters: usize,
        changes: &'a Vec<aqueduct_core::plan::PlanChange>,
    }

    let doc = PlanYaml {
        project: &plan.project,
        from_version: plan.from_version,
        to_version: plan.to_version,
        is_empty: plan.summary.is_empty(),
        creates: plan.summary.creates,
        drops: plan.summary.drops,
        alters: plan.summary.alters,
        changes: &plan.summary.changes,
    };

    serde_yaml::to_string(&doc).unwrap_or_else(|e| format!("# YAML serialization error: {}\n", e))
}

fn load_config_and_vars(
    project_dir: &std::path::Path,
    target: Option<&str>,
) -> (
    Option<AqueductConfig>,
    std::collections::HashMap<String, String>,
) {
    let config = AqueductConfig::load(project_dir).ok();
    let vars = config
        .as_ref()
        .and_then(|c| {
            target
                .and_then(|t| c.targets.get(t))
                .map(|tc| tc.vars.clone())
        })
        .unwrap_or_default();
    (config, vars)
}
