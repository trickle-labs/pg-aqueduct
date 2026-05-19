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

use super::connect;

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
}

pub async fn run(args: PlanArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let client = connect(&dsn).await?;

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
        let drift_count = diff
            .deltas
            .iter()
            .filter(|d| d.kind != aqueduct_core::diff::DeltaKind::Unchanged)
            .count();
        if drift_count > 0 {
            anyhow::bail!(
                "Drift detected: {} table{} differ between live state and desired state. \
                 Run `aqueduct plan` to see the full plan.",
                drift_count,
                if drift_count == 1 { "" } else { "s" }
            );
        }
    }

    // Compute topological order.
    let topo_order = topological_sort(&desired)?;

    // Build plan.
    let plan = build_plan(
        &project_name,
        current_version,
        next_version,
        &diff,
        &topo_order,
    )?;

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

    // Exit codes: 0 = empty plan, 1 = non-empty plan, 2 = error (handled by main).
    if !plan.summary.is_empty() {
        // Non-empty plan → exit 1.
        std::process::exit(1);
    }

    Ok(())
}

/// Minimal YAML renderer for plan output (avoids adding a serde_yaml dependency).
fn render_plan_yaml(plan: &aqueduct_core::plan::Plan) -> String {
    let mut out = String::new();
    out.push_str(&format!("project: \"{}\"\n", plan.project));
    out.push_str(&format!(
        "from_version: {}\n",
        plan.from_version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".to_string())
    ));
    out.push_str(&format!("to_version: {}\n", plan.to_version));
    out.push_str(&format!("is_empty: {}\n", plan.summary.is_empty()));
    out.push_str(&format!("creates: {}\n", plan.summary.creates));
    out.push_str(&format!("drops: {}\n", plan.summary.drops));
    out.push_str(&format!("alters: {}\n", plan.summary.alters));
    if plan.summary.changes.is_empty() {
        out.push_str("changes: []\n");
    } else {
        out.push_str("changes:\n");
        for ch in &plan.summary.changes {
            out.push_str(&format!("  - name: \"{}\"\n", ch.name));
            out.push_str(&format!("    class: \"{}\"\n", ch.class));
            out.push_str(&format!("    description: \"{}\"\n", ch.description));
        }
    }
    out
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
