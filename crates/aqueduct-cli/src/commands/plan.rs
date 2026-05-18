use aqueduct_core::{
    config::AqueductConfig,
    dag::{build_dag_state, topological_sort},
    diff::compute_diff,
    live_state::{check_pgtrickle_version, read_live_state},
    plan::build_plan,
    renderer::{render_plan_json, render_plan_markdown, render_plan_text},
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

    /// Output format: text (default), json, or markdown.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Exit non-zero if the plan is non-empty (useful in CI).
    #[arg(long)]
    pub fail_if_changed: bool,

    /// Check IVM supportability of all queries.
    #[arg(long, default_value = "true")]
    pub validate_ivm: bool,
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
                aqueduct_core::validate::validate_sql_syntax(
                    &table.query,
                    &table.qualified_name.to_string(),
                )?;
            }
        }
    }

    // Read actual live state.
    let actual = read_live_state(&client).await?;

    // Get current version.
    let current_version =
        aqueduct_core::live_state::get_latest_dag_version(&client, &project_name).await?;
    let next_version = current_version.map(|v| v + 1).unwrap_or(1);

    // Compute diff.
    let diff = compute_diff(&desired, &actual);

    // Compute topological order.
    let topo_order = topological_sort(&desired)?;

    // Build plan.
    let plan = build_plan(
        &project_name,
        current_version,
        next_version,
        &diff,
        &topo_order,
    );

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
        _ => render_plan_text(&plan, pg_version.as_deref(), pgtrickle_version.as_deref()),
    };

    println!("{}", output);

    if args.fail_if_changed && !plan.summary.is_empty() {
        anyhow::bail!("Plan is non-empty (changes detected).");
    }

    Ok(())
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
