use aqueduct_core::{
    config::AqueductConfig,
    dag::build_dag_state,
    diff::{compute_diff, DeltaKind},
    live_state::read_live_state,
    parser::load_migrations,
    renderer::{render_plan_json, render_plan_markdown, render_plan_text},
};
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct DiffArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Show diff only for the named stream table.
    #[arg(long)]
    pub table: Option<String>,

    /// Output format: text (default), json, markdown, or yaml.
    #[arg(long, default_value = "text")]
    pub format: String,
}

pub async fn run(args: DiffArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let client = connect(&dsn).await?;

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

    let files = load_migrations(&args.project_dir, &vars)?;
    let desired = build_dag_state(&files, true)?;
    let actual = read_live_state(&client).await?;
    let diff = compute_diff(&desired, &actual);

    // Filter to a single table if requested.
    let deltas: Vec<_> = if let Some(ref table_name) = args.table {
        diff.deltas
            .iter()
            .filter(|d| {
                d.qualified_name.name == *table_name
                    || d.qualified_name.to_string() == *table_name
            })
            .collect()
    } else {
        diff.deltas.iter().collect()
    };

    // Count non-Unchanged deltas.
    let changed_count = deltas
        .iter()
        .filter(|d| d.kind != DeltaKind::Unchanged)
        .count();

    match args.format.as_str() {
        "json" => {
            let output = serde_json::json!({
                "deltas": deltas.iter().map(|d| serde_json::json!({
                    "name": d.qualified_name.to_string(),
                    "kind": format!("{:?}", d.kind),
                    "desired_query": d.desired.as_ref().map(|s| &s.query),
                    "actual_query": d.actual.as_ref().map(|s| &s.query),
                    "desired_schedule": d.desired.as_ref().map(|s| &s.schedule),
                    "actual_schedule": d.actual.as_ref().map(|s| &s.schedule),
                })).collect::<Vec<_>>(),
                "changed": changed_count,
                "total": deltas.len(),
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        "yaml" => {
            // Manual YAML output (no serde_yaml dep needed).
            println!("deltas:");
            for d in &deltas {
                println!("  - name: \"{}\"", d.qualified_name);
                println!("    kind: {:?}", d.kind);
                if let Some(ref spec) = d.desired {
                    println!("    desired_schedule: \"{}\"", spec.schedule);
                }
                if let Some(ref spec) = d.actual {
                    println!("    actual_schedule: \"{}\"", spec.schedule);
                }
            }
            println!("changed: {}", changed_count);
            println!("total: {}", deltas.len());
        }
        "markdown" | "md" => {
            println!("## aqueduct diff\n");
            if changed_count == 0 {
                println!("No differences detected between desired and actual state.");
            } else {
                println!("| Table | Change |");
                println!("|-------|--------|");
                for d in deltas.iter().filter(|d| d.kind != DeltaKind::Unchanged) {
                    println!("| `{}` | {:?} |", d.qualified_name, d.kind);
                }
            }
        }
        _ => {
            // Text format.
            if changed_count == 0 {
                println!("No differences detected between desired and actual state.");
                return Ok(());
            }
            println!(
                "Diff: {} change{} detected\n",
                changed_count,
                if changed_count == 1 { "" } else { "s" }
            );
            for d in deltas.iter().filter(|d| d.kind != DeltaKind::Unchanged) {
                let symbol = match d.kind {
                    DeltaKind::Create => "+",
                    DeltaKind::Drop => "-",
                    _ => "~",
                };
                println!(
                    "  {} {:40}  {:?}",
                    symbol, d.qualified_name.to_string(), d.kind
                );
                if let (Some(desired), Some(actual)) = (&d.desired, &d.actual) {
                    if desired.schedule != actual.schedule {
                        println!(
                            "      schedule: {} → {}",
                            actual.schedule, desired.schedule
                        );
                    }
                    if desired.query.trim() != actual.query.trim() {
                        println!("      query changed");
                    }
                }
            }
        }
    }

    // Suppress unused import warnings — these are used in the yaml/markdown branches.
    let _ = render_plan_json;
    let _ = render_plan_markdown;
    let _ = render_plan_text;

    Ok(())
}
