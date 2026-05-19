pub mod apply;
pub mod destroy;
pub mod fmt;
pub mod import;
pub mod ingest;
pub mod init;
pub mod lint;
pub mod plan;
pub mod preview;
pub mod promote;
pub mod rollback;
pub mod status;
pub mod unlock;
pub mod validate;

use anyhow::Result;
use tokio_postgres::NoTls;

/// Connect to PostgreSQL using the given DSN.
pub async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

/// Connect and ensure the aqueduct catalog is up-to-date.
pub async fn connect_and_migrate(dsn: &str) -> Result<tokio_postgres::Client> {
    let client = connect(dsn).await?;
    aqueduct_core::catalog::ensure_catalog_current(&client).await?;
    Ok(client)
}

/// Resolve a DSN from either a direct --dsn flag or the config file + --to target.
pub async fn resolve_dsn(
    dsn: Option<&str>,
    target: Option<&str>,
    project_dir: &std::path::Path,
) -> Result<String> {
    if let Some(d) = dsn {
        return Ok(aqueduct_core::config::resolve_env_vars(d)?);
    }

    if let Some(t) = target {
        let config = aqueduct_core::config::AqueductConfig::load(project_dir)?;
        let target_cfg = config.target(t)?;
        return Ok(target_cfg.dsn.clone());
    }

    Err(anyhow::anyhow!(
        "Either --dsn or --to <target> must be specified."
    ))
}
