use aqueduct_core::{
    config::AqueductConfig,
    dag::build_dag_state,
    diff::{compute_diff, DeltaKind},
    live_state::read_live_state,
    parser::load_migrations,
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

    /// Exit 1 when any diff delta is non-Unchanged, exit 0 for a clean diff (U-03).
    #[arg(long)]
    pub fail_on_drift: bool,
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

    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let files = load_migrations(&args.project_dir, &vars)?;
    let desired = build_dag_state(&files, true)?;
    let actual = read_live_state(&client, Some(&project_name)).await?;
    let diff = compute_diff(&desired, &actual);

    // Filter to a single table if requested.
    let deltas: Vec<_> = if let Some(ref table_name) = args.table {
        diff.deltas
            .iter()
            .filter(|d| {
                d.qualified_name.name == *table_name || d.qualified_name.to_string() == *table_name
            })
            .collect()
    } else {
        diff.deltas.iter().collect()
    };

    // Count non-Unchanged deltas (U-04: use Result-propagating computation).
    let changed_count = deltas
        .iter()
        .filter(|d| d.kind != DeltaKind::Unchanged)
        .count();

    match args.format.as_str() {
        "json" => {
            let output = serde_json::json!({
                "schema_version": 1,
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
            // ERG-3: use serde_yaml so that special characters are correctly escaped.
            #[derive(serde::Serialize)]
            struct DeltaYaml<'a> {
                name: String,
                kind: String,
                #[serde(skip_serializing_if = "Option::is_none")]
                desired_schedule: Option<&'a str>,
                #[serde(skip_serializing_if = "Option::is_none")]
                actual_schedule: Option<&'a str>,
            }
            #[derive(serde::Serialize)]
            struct DiffYaml<'a> {
                deltas: Vec<DeltaYaml<'a>>,
                changed: usize,
                total: usize,
            }
            let doc = DiffYaml {
                deltas: deltas
                    .iter()
                    .map(|d| DeltaYaml {
                        name: d.qualified_name.to_string(),
                        kind: format!("{:?}", d.kind),
                        desired_schedule: d.desired.as_ref().map(|s| s.schedule.as_str()),
                        actual_schedule: d.actual.as_ref().map(|s| s.schedule.as_str()),
                    })
                    .collect(),
                changed: changed_count,
                total: deltas.len(),
            };
            println!(
                "{}",
                serde_yaml::to_string(&doc)
                    .unwrap_or_else(|e| format!("# YAML error: {}\n", e))
                    .trim_end()
            );
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
            } else {
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
                        symbol,
                        d.qualified_name.to_string(),
                        d.kind
                    );
                    if let (Some(desired), Some(actual)) = (&d.desired, &d.actual) {
                        if desired.schedule != actual.schedule {
                            println!("      schedule: {} → {}", actual.schedule, desired.schedule);
                        }
                        if desired.query.trim() != actual.query.trim() {
                            println!("      query changed");
                        }
                    }
                }
            }
        }
    }

    // U-03: --fail-on-drift: exit 1 when any delta is non-Unchanged, exit 0 for clean.
    // Exit 2 is reserved for errors (handled by main()).
    if args.fail_on_drift && changed_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}
