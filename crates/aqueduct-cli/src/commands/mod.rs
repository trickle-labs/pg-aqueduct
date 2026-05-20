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

// ── Global quiet mode (ERG-1) ────────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, Ordering};

/// Global quiet-mode flag set by `main()` before dispatching to a command.
static QUIET_FLAG: AtomicBool = AtomicBool::new(false);

/// Set whether the CLI is running in quiet mode (--quiet or --porcelain).
pub fn set_quiet(quiet: bool) {
    QUIET_FLAG.store(quiet, Ordering::Relaxed);
}

/// Returns `true` when the CLI is running in quiet mode.
///
/// Command handlers use this to suppress decorative (non-error) stdout output.
pub fn is_quiet() -> bool {
    QUIET_FLAG.load(Ordering::Relaxed)
}

/// Redact the password portion of a PostgreSQL DSN for safe display in logs or
/// error messages (U-06/SEC-01).
///
/// Handles both URL-form (`postgres://user:pass@host/db`) and
/// keyword-value form (`host=... password=...`).  Returns the DSN with any
/// password replaced by `***`.
pub fn redact_dsn(dsn: &str) -> String {
    use std::sync::LazyLock;
    // L1 (v0.14): Compile the regex once using LazyLock to avoid recompilation
    // on every call.
    static KV_REDACT_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)(password\s*=\s*)([^\s]+)").expect("valid static regex")
    });

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
    KV_REDACT_RE.replace_all(dsn, "${1}***").into_owned()
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
/// transaction with a statement timeout (P-03 / v0.12).
///
/// Uses `SET LOCAL statement_timeout` inside a `BEGIN READ ONLY` transaction to:
/// 1. Prevent runaway queries from blocking other operations.
/// 2. Ensure all subsequent queries in the session are read-only.
///
/// The `read_timeout` parameter is the statement timeout duration (e.g., `"30s"`).
/// Defaults to `"30s"` when `None` is passed.
pub async fn connect_read_only(dsn: &str) -> Result<tokio_postgres::Client> {
    connect_read_only_with_timeout(dsn, "30s").await
}

/// Like `connect_read_only` but with a configurable statement timeout.
///
/// SEC-3 (v0.14): Executes `BEGIN READ ONLY` first, then `SET LOCAL
/// statement_timeout` so the timeout applies inside the transaction.
/// Previously the order was reversed, which meant the SET LOCAL was issued
/// outside a transaction and had no effect.
pub async fn connect_read_only_with_timeout(
    dsn: &str,
    timeout: &str,
) -> Result<tokio_postgres::Client> {
    // SEC-3: allowlist check — reject duration suffixes that could be used for
    // SQL injection via crafted timeout strings.
    let safe_timeout = sanitize_statement_timeout(timeout).unwrap_or("30s");
    let client = connect(dsn).await?;
    // SEC-3: BEGIN READ ONLY first so SET LOCAL applies inside the transaction.
    client
        .batch_execute(&format!(
            "BEGIN READ ONLY; SET LOCAL statement_timeout = '{}'",
            safe_timeout
        ))
        .await?;
    Ok(client)
}

/// Sanitize a statement timeout string to prevent injection (SEC-3 / v0.14).
///
/// Accepts the forms PostgreSQL recognises: `<N>ms`, `<N>s`, `<N>min`, `<N>h`.
/// Returns `None` if the string does not match, so callers can fall back to a
/// safe default.
fn sanitize_statement_timeout(timeout: &str) -> Option<&str> {
    // Must be non-empty and consist only of digits and an allowed suffix.
    let t = timeout.trim();
    if t.is_empty() {
        return None;
    }
    let suffixes = ["ms", "min", "s", "h"];
    for suffix in suffixes {
        if let Some(digits) = t.strip_suffix(suffix) {
            if digits.chars().all(|c| c.is_ascii_digit()) && !digits.is_empty() {
                return Some(t);
            }
        }
    }
    // Plain integer (seconds) is also valid.
    if t.chars().all(|c| c.is_ascii_digit()) && !t.is_empty() {
        return Some(t);
    }
    None
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
