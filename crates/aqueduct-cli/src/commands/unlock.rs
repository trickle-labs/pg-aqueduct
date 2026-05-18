use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct UnlockArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Force unlock regardless of lock holder (requires confirmation).
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: UnlockArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;

    let client = connect(&dsn).await?;

    let config = aqueduct_core::config::AqueductConfig::load(&args.project_dir).ok();
    let project_name = config
        .as_ref()
        .map(|c| c.project.name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Check if a lock exists.
    let lock = client
        .query_opt(
            "SELECT holder, acquired_at, ttl FROM aqueduct.locks WHERE project = $1",
            &[&project_name],
        )
        .await?;

    let Some(lock_row) = lock else {
        println!("No lock found for project '{}'.", project_name);
        return Ok(());
    };

    let holder: String = lock_row.get(0);
    let acquired_at: chrono::DateTime<chrono::Utc> = lock_row.get(1);

    println!("Lock found for project '{}':", project_name);
    println!("  Holder:      {}", holder);
    println!("  Acquired at: {}", acquired_at);

    if !args.force {
        anyhow::bail!(
            "Pass --force to delete this lock. This is an emergency operation — \
             only use it if the lock holder has crashed and the migration is not running."
        );
    }

    client
        .execute(
            "DELETE FROM aqueduct.locks WHERE project = $1",
            &[&project_name],
        )
        .await?;

    println!("✓ Lock released for project '{}'.", project_name);
    Ok(())
}
