/// Preview environment support for aqueduct.
///
/// A preview environment is a throwaway copy of the DAG built in a scratch schema
/// (or a scratch database) so reviewers can EXPLAIN and benchmark a candidate change
/// without touching production.
///
/// ## Backends
///
/// | Backend        | Description                                                      |
/// |----------------|------------------------------------------------------------------|
/// | Native (same DB) | Scratch schema with TABLESAMPLE base data. Zero infrastructure. |
/// | CloudNativePG  | Clone cluster via the CNPG API (requires cloud credentials).    |
/// | Neon branch    | Branch via the Neon API (requires Neon API token).              |
///
/// The native backend is fully implemented. CloudNativePG and Neon require
/// configuration and cloud credentials not available in the default CLI; they emit a
/// clear error when not configured.
use crate::dag::DagState;
use crate::error::{AqueductError, Result};

/// The backend to use for preview environments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreviewBackend {
    /// Native (same database): scratch schema with TABLESAMPLE base data.
    Native,
    /// CloudNativePG clone cluster.
    CloudNativePg { endpoint: String },
    /// Neon branch.
    Neon {
        api_token: String,
        project_id: String,
    },
}

/// Configuration for creating a preview environment.
#[derive(Debug, Clone)]
pub struct PreviewConfig {
    /// Branch name (used to derive the schema name).
    pub branch: String,
    /// Backend to use.
    pub backend: PreviewBackend,
    /// Sample fraction for TABLESAMPLE (0.0–1.0). Default: 0.1 (10%).
    pub sample_fraction: f64,
    /// Whether to drop the preview schema when it already exists (recreate).
    pub recreate: bool,
}

impl PreviewConfig {
    pub fn new_native(branch: &str) -> Self {
        Self {
            branch: branch.to_string(),
            backend: PreviewBackend::Native,
            sample_fraction: 0.1,
            recreate: false,
        }
    }

    /// Derive the preview schema name from the branch name.
    ///
    /// Rules:
    /// - Prefix `aqueduct_preview_`
    /// - Replace `/`, `-`, spaces with `_`
    /// - Lowercase
    /// - Truncate to 63 chars (PostgreSQL identifier limit)
    pub fn schema_name(&self) -> String {
        let sanitised = self
            .branch
            .to_lowercase()
            .replace(['/', '-', ' ', '.'], "_")
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>();
        let full = format!("aqueduct_preview_{}", sanitised);
        // PostgreSQL max identifier length = 63 chars.
        full.chars().take(63).collect()
    }
}

/// Information about a running preview environment.
#[derive(Debug, Clone)]
pub struct PreviewEnvironment {
    /// The schema name where the preview is running.
    pub schema_name: String,
    /// The branch this preview was created for.
    pub branch: String,
    /// The stream tables created in this preview.
    pub stream_tables: Vec<String>,
}

/// Create a native preview environment in the same database.
///
/// Steps:
/// 1. Create the scratch schema (`aqueduct_preview_{branch}`).
/// 2. For each source table referenced by the desired DAG, create a sampled copy
///    in the scratch schema using `CREATE TABLE ... AS SELECT * FROM ... TABLESAMPLE BERNOULLI(pct)`.
/// 3. For each stream table in the desired DAG, create it in the scratch schema
///    (rewritten to read from the sampled source copies).
pub async fn create_preview_native(
    client: &tokio_postgres::Client,
    config: &PreviewConfig,
    desired: &DagState,
) -> Result<PreviewEnvironment> {
    let schema = config.schema_name();

    // Drop and recreate if requested.
    if config.recreate {
        client
            .execute(
                &format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    quote_ident_preview(&schema)
                ),
                &[],
            )
            .await?;
    }

    // Create the preview schema.
    client
        .execute(
            &format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                quote_ident_preview(&schema)
            ),
            &[],
        )
        .await?;

    let pct = (config.sample_fraction * 100.0).clamp(0.01, 100.0);
    let mut created_tables: Vec<String> = Vec::new();

    // Sample source tables into the preview schema.
    for source in &desired.sources {
        let src_schema = &source.qualified_name.schema;
        let src_table = &source.qualified_name.name;

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} AS SELECT * FROM {}.{} TABLESAMPLE BERNOULLI({})",
            quote_ident_preview(&schema),
            quote_ident_preview(src_table),
            quote_ident_preview(src_schema),
            quote_ident_preview(src_table),
            pct,
        );

        client.execute(&create_sql, &[]).await.map_err(|e| {
            AqueductError::Other(format!(
                "Failed to sample source table {}.{}: {}",
                src_schema, src_table, e
            ))
        })?;
    }

    // Create stream tables in the preview schema (using the same queries but
    // rewriting table references to point to sampled copies in the preview schema).
    for stream_table in &desired.stream_tables {
        let rewritten_query = rewrite_query_for_preview(&stream_table.query, &schema, desired);

        // Create as a materialised view snapshot (not a live stream).
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} AS SELECT * FROM ({}) _aq_preview LIMIT 0",
            quote_ident_preview(&schema),
            quote_ident_preview(&stream_table.qualified_name.name),
            rewritten_query,
        );

        client.execute(&create_sql, &[]).await.map_err(|e| {
            AqueductError::Other(format!(
                "Failed to create preview stream table {}: {}",
                stream_table.qualified_name, e
            ))
        })?;

        created_tables.push(stream_table.qualified_name.name.clone());
    }

    tracing::info!(
        "Preview environment '{}' created with {} stream table(s)",
        schema,
        created_tables.len()
    );

    Ok(PreviewEnvironment {
        schema_name: schema,
        branch: config.branch.clone(),
        stream_tables: created_tables,
    })
}

/// Drop a native preview environment.
pub async fn drop_preview_native(client: &tokio_postgres::Client, schema: &str) -> Result<()> {
    client
        .execute(
            &format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident_preview(schema)
            ),
            &[],
        )
        .await?;

    tracing::info!("Preview environment '{}' dropped", schema);
    Ok(())
}

/// List all preview environments in the database.
pub async fn list_preview_schemas(client: &tokio_postgres::Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'aqueduct_preview_%' ORDER BY schema_name",
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Rewrite a SQL query to read from the preview schema instead of the original sources.
///
/// This is a simple string-based rewrite: we replace `schema.table` references
/// for known source tables with `preview_schema.table`. For production use, a
/// proper AST-based rewrite is required; this version handles the common cases.
fn rewrite_query_for_preview(query: &str, preview_schema: &str, desired: &DagState) -> String {
    let mut rewritten = query.to_string();

    for source in &desired.sources {
        let original = format!(
            "{}.{}",
            source.qualified_name.schema, source.qualified_name.name
        );
        let replacement = format!("{}.{}", preview_schema, source.qualified_name.name);
        rewritten = rewritten.replace(&original, &replacement);
    }

    // Also handle stream tables referenced within the same DAG.
    for stream_table in &desired.stream_tables {
        let original = format!(
            "{}.{}",
            stream_table.qualified_name.schema, stream_table.qualified_name.name
        );
        let replacement = format!("{}.{}", preview_schema, stream_table.qualified_name.name);
        rewritten = rewritten.replace(&original, &replacement);
    }

    rewritten
}

/// Quote a PostgreSQL identifier for use in preview SQL.
fn quote_ident_preview(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Create a preview using a CloudNativePG cluster clone.
///
/// This requires the CNPG operator to be installed and credentials configured.
/// In v0.3, this emits a structured error with setup instructions.
pub async fn create_preview_cnpg(
    _endpoint: &str,
    _config: &PreviewConfig,
    _desired: &DagState,
) -> Result<PreviewEnvironment> {
    Err(AqueductError::Config(
        "CloudNativePG preview backend requires --cnpg-endpoint and cluster credentials. \
         See the aqueduct documentation for CNPG preview setup."
            .to_string(),
    ))
}

/// Create a preview using a Neon branch.
///
/// This requires a Neon API token and project ID.
/// In v0.3, this emits a structured error with setup instructions.
pub async fn create_preview_neon(
    _api_token: &str,
    _project_id: &str,
    _config: &PreviewConfig,
    _desired: &DagState,
) -> Result<PreviewEnvironment> {
    Err(AqueductError::Config(
        "Neon preview backend requires --neon-api-token and --neon-project-id. \
         See the aqueduct documentation for Neon preview setup."
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_name_sanitisation() {
        let cfg = PreviewConfig::new_native("feat/my-feature");
        assert_eq!(cfg.schema_name(), "aqueduct_preview_feat_my_feature");

        let cfg2 = PreviewConfig::new_native("main");
        assert_eq!(cfg2.schema_name(), "aqueduct_preview_main");

        // Very long name truncated to 63 chars.
        let long_branch = "a".repeat(100);
        let cfg3 = PreviewConfig::new_native(&long_branch);
        assert!(cfg3.schema_name().len() <= 63);
    }

    #[test]
    fn test_quote_ident_preview() {
        assert_eq!(quote_ident_preview("my_schema"), "\"my_schema\"");
        assert_eq!(quote_ident_preview("has\"quote"), "\"has\"\"quote\"");
    }
}
