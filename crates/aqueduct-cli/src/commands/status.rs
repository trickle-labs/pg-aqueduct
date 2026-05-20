use aqueduct_core::{
    config::AqueductConfig,
    live_state::{check_pgtrickle_version, get_latest_dag_version, get_stream_table_count},
    renderer::{render_status_text, StatusReport},
};
use clap::Args;

use super::connect_read_only;

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

    /// Watch mode: poll continuously instead of running once.
    #[arg(long)]
    pub watch: bool,

    /// Polling interval in watch mode (e.g. "30s", "1m").
    #[arg(long, default_value = "30s")]
    pub interval: String,

    /// Exit after N consecutive drift detections (0 = never exit on drift).
    #[arg(long, default_value = "0")]
    pub max_drift_count: u32,
}

async fn poll_once(
    client: &tokio_postgres::Client,
    project_name: &str,
    project_dir: &std::path::Path,
    format: &str,
    fail_on_drift: bool,
) -> anyhow::Result<u32> {
    let current_version = get_latest_dag_version(client, project_name).await?;
    let stream_table_count = get_stream_table_count(client).await?;
    let pgtrickle_version = check_pgtrickle_version(client).await?;

    let pg_version: Option<String> = client
        .query_one("SELECT current_setting('server_version')", &[])
        .await
        .map(|r| r.get(0))
        .ok();

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

    // CORR-8: count all three diff collections for accurate drift reporting.
    // diff.deltas counts stream table changes; source_deltas and consumer_deltas
    // cover source DDL changes and consumer view divergence respectively.
    let drift_count: usize = {
        let live = aqueduct_core::live_state::read_live_state(client, Some(project_name)).await?;
        let files = aqueduct_core::parser::load_migrations(project_dir, &Default::default())?;
        let desired = aqueduct_core::dag::build_dag_state(&files, true)?;
        let diff = aqueduct_core::diff::compute_diff(&desired, &live);
        let stream_drift = diff
            .deltas
            .iter()
            .filter(|d| d.kind != aqueduct_core::diff::DeltaKind::Unchanged)
            .count();
        let source_drift = diff
            .source_deltas
            .iter()
            .filter(|s| s.kind != aqueduct_core::diff::SourceDeltaKind::Unchanged)
            .count();
        let consumer_drift = diff
            .consumer_deltas
            .iter()
            .filter(|c| c.kind != aqueduct_core::diff::ConsumerDeltaKind::Unchanged)
            .count();
        stream_drift + source_drift + consumer_drift
    };

    let status = StatusReport {
        project: project_name.to_string(),
        current_version,
        applied_at,
        applied_by,
        stream_table_count,
        drift_count,
        pgtrickle_version,
        pg_version,
    };

    match format {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "project": status.project,
                    "version": status.current_version,
                    "stream_tables": status.stream_table_count,
                    "drift": status.drift_count,
                    "pgtrickle_version": status.pgtrickle_version,
                    // M15: include pg_version in JSON output (CORR-8 / v0.14)
                    "pg_version": status.pg_version,
                    "polled_at": chrono::Utc::now().to_rfc3339(),
                }))?
            );
        }
        "yaml" | "yml" => {
            // U-10: YAML format for status output (no serde_yaml dep needed).
            println!("project: \"{}\"", status.project);
            println!(
                "version: {}",
                status
                    .current_version
                    .map_or("null".to_string(), |v| v.to_string())
            );
            println!("stream_tables: {}", status.stream_table_count);
            println!("drift: {}", status.drift_count);
            println!(
                "pgtrickle_version: {}",
                status
                    .pgtrickle_version
                    .as_deref()
                    .map_or("null".to_string(), |v| format!("\"{}\"", v))
            );
            println!("polled_at: \"{}\"", chrono::Utc::now().to_rfc3339());
        }
        _ => {
            println!("{}", render_status_text(&status));
        }
    }

    if fail_on_drift && status.drift_count > 0 {
        anyhow::bail!("Drift detected ({} tables)", status.drift_count);
    }

    Ok(status.drift_count as u32)
}

/// Parse a duration string like "30s", "1m", "2h" into `std::time::Duration`.
pub fn parse_interval(s: &str) -> anyhow::Result<std::time::Duration> {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        let n: u64 = secs.parse().map_err(|_| {
            anyhow::anyhow!("Invalid interval '{}': expected a number before 's'", s)
        })?;
        return Ok(std::time::Duration::from_secs(n));
    }
    if let Some(mins) = s.strip_suffix('m') {
        let n: u64 = mins.parse().map_err(|_| {
            anyhow::anyhow!("Invalid interval '{}': expected a number before 'm'", s)
        })?;
        return Ok(std::time::Duration::from_secs(n * 60));
    }
    if let Some(hrs) = s.strip_suffix('h') {
        let n: u64 = hrs.parse().map_err(|_| {
            anyhow::anyhow!("Invalid interval '{}': expected a number before 'h'", s)
        })?;
        return Ok(std::time::Duration::from_secs(n * 3600));
    }
    // Plain integer treated as seconds.
    let n: u64 = s.parse().map_err(|_| {
        anyhow::anyhow!(
            "Invalid interval '{}': expected format like '30s', '1m', or '2h'",
            s
        )
    })?;
    Ok(std::time::Duration::from_secs(n))
}

pub async fn run(args: StatusArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    // D-03/SEC-08: Use a read-only session for status (no writes).
    let client = connect_read_only(&dsn).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    if !args.watch {
        let drift_count = poll_once(
            &client,
            &project_name,
            &args.project_dir,
            &args.format,
            args.fail_on_drift,
        )
        .await?;
        if args.fail_on_drift && drift_count > 0 {
            anyhow::bail!("Drift detected ({} tables)", drift_count);
        }
        return Ok(());
    }

    // Watch mode: poll repeatedly, reconnecting on each iteration to handle
    // network interruptions and idle_in_transaction_session_timeout.
    let interval = parse_interval(&args.interval)?;
    let mut consecutive_drift: u32 = 0;

    loop {
        // Create a fresh connection each poll to handle reconnections gracefully.
        // D-03/SEC-08: Each poll uses a read-only session.
        let poll_result = async {
            let client = connect_read_only(&dsn).await?;
            poll_once(
                &client,
                &project_name,
                &args.project_dir,
                &args.format,
                false,
            )
            .await
        }
        .await;

        let drift = match poll_result {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("Status poll error (will retry): {}", e);
                // Exponential back-off is handled by the sleep at the bottom of the loop.
                0
            }
        };

        if drift > 0 {
            consecutive_drift += 1;
            tracing::warn!(
                consecutive = consecutive_drift,
                drift = drift,
                "Drift detected"
            );
        } else {
            consecutive_drift = 0;
        }

        if args.max_drift_count > 0 && consecutive_drift >= args.max_drift_count {
            anyhow::bail!(
                "Exiting: {} consecutive drift detections (--max-drift-count {})",
                consecutive_drift,
                args.max_drift_count
            );
        }

        tokio::time::sleep(interval).await;
    }
}
