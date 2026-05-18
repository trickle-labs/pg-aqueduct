use aqueduct_core::{
    config::AqueductConfig,
    live_state::{check_pgtrickle_version, get_latest_dag_version, get_stream_table_count},
    renderer::{render_status_text, StatusReport},
};
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text or json.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Exit non-zero if drift is detected.
    #[arg(long)]
    pub fail_on_drift: bool,
}

pub async fn run(args: StatusArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let client = connect(&dsn).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let current_version = get_latest_dag_version(&client, &project_name).await?;
    let stream_table_count = get_stream_table_count(&client).await?;
    let pgtrickle_version = check_pgtrickle_version(&client).await?;

    let pg_version: Option<String> = client
        .query_one("SELECT current_setting('server_version')", &[])
        .await
        .map(|r| r.get(0))
        .ok();

    // Get applied_at and applied_by from the last version record.
    let (applied_at, applied_by) = if let Some(v) = current_version {
        let row = client
            .query_opt(
                "SELECT applied_at, applied_by FROM aqueduct.dag_versions WHERE version = $1",
                &[&(v as i64)],
            )
            .await?;
        if let Some(r) = row {
            let at: chrono::DateTime<chrono::Utc> = r.get(0);
            let by: String = r.get(1);
            (Some(at), Some(by))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    let status = StatusReport {
        project: project_name,
        current_version,
        applied_at,
        applied_by,
        stream_table_count,
        drift_count: 0, // Drift detection is polling-based; simplified for v0.1.
        pgtrickle_version,
        pg_version,
    };

    match args.format.as_str() {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "project": status.project,
                    "version": status.current_version,
                    "stream_tables": status.stream_table_count,
                    "drift": status.drift_count,
                    "pgtrickle_version": status.pgtrickle_version,
                }))?
            );
        }
        _ => {
            println!("{}", render_status_text(&status));
        }
    }

    if args.fail_on_drift && status.drift_count > 0 {
        anyhow::bail!("Drift detected ({} tables)", status.drift_count);
    }

    Ok(())
}
