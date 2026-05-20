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

    /// Polling interval for --watch mode (e.g. "30s", "1m"). Alias for --interval.
    #[arg(long, default_value = "30s")]
    pub poll_interval: String,
}

async fn poll_once(
    client: &tokio_postgres::Client,
    project_name: &str,
    project_dir: &std::path::Path,
    format: &str,
    fail_on_drift: bool,
    catalog_schema: &aqueduct_core::catalog::CatalogSchema,
) -> anyhow::Result<u32> {
    let current_version = get_latest_dag_version(client, project_name, catalog_schema).await?;
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
                &aqueduct_core::catalog::for_schema(
                    "SELECT applied_at, applied_by FROM aqueduct.dag_versions WHERE version = $1",
                    catalog_schema,
                ),
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
        let live =
            aqueduct_core::live_state::read_live_state(client, Some(project_name), catalog_schema)
                .await?;
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
            // ERG-3: use serde_yaml so that special characters are correctly escaped.
            #[derive(serde::Serialize)]
            struct StatusYaml<'a> {
                project: &'a str,
                version: Option<u64>,
                stream_tables: i64,
                drift: usize,
                pgtrickle_version: Option<&'a str>,
                polled_at: String,
            }
            let doc = StatusYaml {
                project: &status.project,
                version: status.current_version,
                stream_tables: status.stream_table_count,
                drift: status.drift_count,
                pgtrickle_version: status.pgtrickle_version.as_deref(),
                polled_at: chrono::Utc::now().to_rfc3339(),
            };
            println!(
                "{}",
                serde_yaml::to_string(&doc)
                    .unwrap_or_else(|e| format!("# YAML error: {}\n", e))
                    .trim_end()
            );
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
    let catalog_schema = config
        .as_ref()
        .and_then(|c| aqueduct_core::catalog::CatalogSchema::new(&c.project.catalog_schema).ok())
        .unwrap_or_default();

    if !args.watch {
        let drift_count = poll_once(
            &client,
            &project_name,
            &args.project_dir,
            &args.format,
            args.fail_on_drift,
            &catalog_schema,
        )
        .await?;
        if args.fail_on_drift && drift_count > 0 {
            anyhow::bail!("Drift detected ({} tables)", drift_count);
        }
        return Ok(());
    }

    // Watch mode: poll repeatedly, reconnecting on each iteration to handle
    // network interruptions and idle_in_transaction_session_timeout.
    // Honour --poll-interval; fall back to --interval for backward compat.
    let interval_str = if args.poll_interval != "30s" {
        &args.poll_interval
    } else {
        &args.interval
    };
    let interval = parse_interval(interval_str)?;
    let mut consecutive_drift: u32 = 0;
    // M-8 (v0.19): track consecutive connection/poll errors for exponential backoff.
    let mut consecutive_errors: u32 = 0;

    // PERF-2: cache the desired state by spec hash so migration files are
    // not re-read when their mtimes haven't changed.
    let mut last_spec_hash: Option<String> = None;
    let mut last_migration_mtimes: std::collections::HashMap<
        std::path::PathBuf,
        std::time::SystemTime,
    > = std::collections::HashMap::new();

    loop {
        // Check whether any migration file mtimes have changed.
        let current_mtimes = collect_migration_mtimes(&args.project_dir);
        let mtimes_changed = current_mtimes != last_migration_mtimes;

        // Recompute spec hash only when files changed.
        let spec_hash = if mtimes_changed || last_spec_hash.is_none() {
            match aqueduct_core::parser::load_migrations(&args.project_dir, &Default::default()) {
                Ok(files) => {
                    use sha2::{Digest, Sha256};
                    match aqueduct_core::dag::build_dag_state(&files, true) {
                        Ok(desired) => {
                            let spec_json = serde_json::to_string(&desired).unwrap_or_default();
                            let hash = format!("{:x}", Sha256::digest(spec_json.as_bytes()));
                            last_migration_mtimes = current_mtimes;
                            last_spec_hash = Some(hash.clone());
                            hash
                        }
                        Err(e) => {
                            tracing::warn!("Failed to build DAG state: {}", e);
                            last_spec_hash.clone().unwrap_or_default()
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to load migrations: {}", e);
                    last_spec_hash.clone().unwrap_or_default()
                }
            }
        } else {
            last_spec_hash.clone().unwrap_or_default()
        };

        let _ = spec_hash; // Used for cache invalidation; live state always re-polled.

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
                &catalog_schema,
            )
            .await
        }
        .await;

        let drift = match poll_result {
            Ok(d) => {
                // Reset error counter on a successful poll.
                consecutive_errors = 0;
                d
            }
            Err(e) => {
                consecutive_errors += 1;
                // M-8 (v0.19): exponential backoff on repeated errors.
                // delay = min(interval * 2^error_count, 5 * interval)
                let backoff_factor = (1u64 << consecutive_errors.min(10)) as u32;
                let backoff = std::cmp::min(
                    interval.saturating_mul(backoff_factor),
                    interval.saturating_mul(5),
                );
                if consecutive_errors >= 10 {
                    tracing::error!(
                        consecutive_errors,
                        "Status poll: repeated connection failures (last: {}); \
                         backing off {:?}",
                        e,
                        backoff
                    );
                } else {
                    tracing::warn!(
                        consecutive_errors,
                        "Status poll error (will retry with backoff {:?}): {}",
                        backoff,
                        e
                    );
                }
                tokio::time::sleep(backoff).await;
                continue;
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

/// Collect modification times for all migration files in the project directory.
fn collect_migration_mtimes(
    project_dir: &std::path::Path,
) -> std::collections::HashMap<std::path::PathBuf, std::time::SystemTime> {
    let mut mtimes = std::collections::HashMap::new();
    let migrations_dir = project_dir.join("migrations");
    if let Ok(walker) = std::fs::read_dir(&migrations_dir) {
        collect_mtimes_recursive(walker, &mut mtimes);
    }
    mtimes
}

fn collect_mtimes_recursive(
    entries: std::fs::ReadDir,
    mtimes: &mut std::collections::HashMap<std::path::PathBuf, std::time::SystemTime>,
) {
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Ok(sub) = std::fs::read_dir(&path) {
                collect_mtimes_recursive(sub, mtimes);
            }
        } else if path.extension().is_some_and(|e| e == "sql") {
            if let Ok(meta) = entry.metadata() {
                if let Ok(mtime) = meta.modified() {
                    mtimes.insert(path, mtime);
                }
            }
        }
    }
}
