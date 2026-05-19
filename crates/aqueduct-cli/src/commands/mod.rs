pub mod apply;
pub mod destroy;
pub mod diff;
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

/// Redact the password portion of a PostgreSQL DSN for safe display in logs or
/// error messages (U-06/SEC-01).
///
/// Handles both URL-form (`postgres://user:pass@host/db`) and
/// keyword-value form (`host=... password=...`).  Returns the DSN with any
/// password replaced by `***`.
pub fn redact_dsn(dsn: &str) -> String {
    // URL form: postgres://user:PASSWORD@host/db
    // Match scheme://user:password@... pattern and replace only the password part.
    if dsn.contains("://") {
        if let Some(at_pos) = dsn.rfind('@') {
            // Find the start of the authority section after ://
            if let Some(scheme_end) = dsn.find("://") {
                let authority_start = scheme_end + 3;
                let authority = &dsn[authority_start..at_pos];
                // authority is either "user:password" or just "user"
                if let Some(colon_pos) = authority.find(':') {
                    let before_pass = authority_start + colon_pos + 1;
                    let mut result = String::with_capacity(dsn.len());
                    result.push_str(&dsn[..before_pass]);
                    result.push_str("***");
                    result.push_str(&dsn[at_pos..]);
                    return result;
                }
            }
        }
        return dsn.to_string();
    }

    // Keyword-value form: host=… password=… or sslmode=…
    // Replace `password=<value>` (space- or end-of-string terminated).
    let re = regex::Regex::new(r"(?i)(password\s*=\s*)([^\s]+)").unwrap();
    re.replace_all(dsn, "${1}***").into_owned()
}

/// Connect to PostgreSQL using the given DSN.
pub async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
        anyhow::anyhow!("Failed to connect to database ({}): {}", redact_dsn(dsn), e)
    })?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

/// Connect to PostgreSQL and execute all subsequent queries in a read-only
/// transaction.  Used by `plan` and `status` commands to prevent accidental
/// writes (D-03/SEC-08).
pub async fn connect_read_only(dsn: &str) -> Result<tokio_postgres::Client> {
    let client = connect(dsn).await?;
    client
        .execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", &[])
        .await?;
    Ok(client)
}

/// Connect and ensure the aqueduct catalog is up-to-date.
pub async fn connect_and_migrate(dsn: &str) -> Result<tokio_postgres::Client> {
    let client = connect(dsn).await?;
    aqueduct_core::catalog::ensure_catalog_current(&client).await?;
    Ok(client)
}

/// Resolve a DSN from either a direct --dsn flag or the config file + --to target.
///
/// Handles `${VAR}` env-var expansion, `${secret:BACKEND:KEY}` inline secret
/// resolution, and the `--allow-plaintext-password` security guard.
pub async fn resolve_dsn(
    dsn: Option<&str>,
    target: Option<&str>,
    project_dir: &std::path::Path,
) -> Result<String> {
    resolve_dsn_with_opts(dsn, target, project_dir, false).await
}

/// Like `resolve_dsn` but honours `allow_plaintext_password`.
pub async fn resolve_dsn_with_opts(
    dsn: Option<&str>,
    target: Option<&str>,
    project_dir: &std::path::Path,
    allow_plaintext_password: bool,
) -> Result<String> {
    let raw = if let Some(d) = dsn {
        aqueduct_core::config::resolve_env_vars(d)?
    } else if let Some(t) = target {
        let config = aqueduct_core::config::AqueductConfig::load(project_dir)?;
        let target_cfg = config.target(t)?;
        target_cfg.dsn.clone()
    } else {
        return Err(anyhow::anyhow!(
            "Either --dsn or --to <target> must be specified."
        ));
    };

    // Resolve ${secret:BACKEND:KEY} inline syntax.
    let resolved = aqueduct_core::secrets::resolve_dsn_secrets(
        &raw,
        &aqueduct_core::secrets::SecretBackend::Env,
    )
    .await
    .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Plaintext-password guard (ESSENCE principle 6).
    aqueduct_core::config::check_plaintext_password(&resolved, allow_plaintext_password)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(resolved)
}
