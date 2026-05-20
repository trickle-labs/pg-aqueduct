use aqueduct_core::catalog::LIST_MIGRATIONS_FOR_AUDIT_SQL;
use aqueduct_core::config::AqueductConfig;
use clap::Args;

#[derive(Debug, Args)]
pub struct AuditArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: table, json, or yaml.
    #[arg(long, default_value = "table")]
    pub format: String,

    /// Maximum number of migrations to show (most recent first).
    #[arg(long, default_value = "20")]
    pub limit: i64,

    /// Allow DSN with embedded plaintext password (not recommended outside CI/dev).
    #[arg(long)]
    pub allow_plaintext_password: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct MigrationAuditRow {
    migration_id: i64,
    project: String,
    status: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    to_version: Option<i64>,
    cli_version: Option<String>,
    step_count: i64,
    failed_steps: i64,
    last_error: Option<String>,
}

pub async fn run(args: AuditArgs) -> anyhow::Result<()> {
    let dsn = super::resolve_dsn_with_opts(
        args.dsn.as_deref(),
        args.to.as_deref(),
        &args.project_dir,
        args.allow_plaintext_password,
    )
    .await?;

    let client = super::connect_read_only(&dsn).await?;

    let config = AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let rows = client
        .query(LIST_MIGRATIONS_FOR_AUDIT_SQL, &[&project_name, &args.limit])
        .await?;

    let mut records: Vec<MigrationAuditRow> = Vec::new();
    for row in &rows {
        let started_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("started_at").ok();
        let finished_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("finished_at").ok();
        let step_count: i64 = row.try_get::<_, i64>("step_count").unwrap_or(0);
        let failed_steps: i64 = row.try_get::<_, i64>("failed_steps").unwrap_or(0);
        records.push(MigrationAuditRow {
            migration_id: row.try_get::<_, i64>("migration_id").unwrap_or(0),
            project: row.try_get("project").unwrap_or_default(),
            status: row.try_get("status").unwrap_or_default(),
            started_at: started_at.map(|dt| dt.to_rfc3339()),
            finished_at: finished_at.map(|dt| dt.to_rfc3339()),
            to_version: row.try_get::<_, Option<i64>>("to_version").ok().flatten(),
            cli_version: row
                .try_get::<_, Option<String>>("cli_version")
                .ok()
                .flatten(),
            step_count,
            failed_steps,
            last_error: row
                .try_get::<_, Option<String>>("last_error")
                .ok()
                .flatten(),
        });
    }

    match args.format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&records)?);
        }
        "yaml" => {
            println!("{}", serde_yaml::to_string(&records)?);
        }
        _ => {
            // Table format.
            if records.is_empty() {
                println!("No migrations found for project '{}'.", project_name);
                return Ok(());
            }
            println!(
                "{:<6} {:<12} {:<20} {:<25} {:<8} {:<8} ERROR",
                "ID", "STATUS", "STARTED", "VERSION", "STEPS", "FAILED",
            );
            println!("{}", "-".repeat(100));
            for r in &records {
                let started = r
                    .started_at
                    .as_deref()
                    .map(|s| &s[..19.min(s.len())])
                    .unwrap_or("-");
                let version = r
                    .to_version
                    .map(|v| format!("v{}", v))
                    .unwrap_or_else(|| "-".to_string());
                let error = r.last_error.as_deref().unwrap_or("");
                let error_truncated = if error.len() > 40 {
                    format!("{}…", &error[..40])
                } else {
                    error.to_string()
                };
                println!(
                    "{:<6} {:<12} {:<20} {:<25} {:<8} {:<8} {}",
                    r.migration_id,
                    r.status,
                    started,
                    version,
                    r.step_count,
                    r.failed_steps,
                    error_truncated,
                );
            }
        }
    }

    Ok(())
}
