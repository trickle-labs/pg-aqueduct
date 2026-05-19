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
use crate::dag::{DagState, QualifiedName};
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
    /// When set, restrict the preview to the subgraph anchored at this table (P-07 / v0.12).
    pub anchor_table: Option<QualifiedName>,
}

impl PreviewConfig {
    pub fn new_native(branch: &str) -> Self {
        Self {
            branch: branch.to_string(),
            backend: PreviewBackend::Native,
            sample_fraction: 0.1,
            recreate: false,
            anchor_table: None,
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

/// Collect the subgraph of stream tables that are ancestors or descendants of
/// `anchor` in the DAG (P-07 / v0.12).
///
/// Returns the qualified names of all stream tables in the subgraph (including
/// the anchor itself).  If `anchor` is not found in `desired`, returns an empty vec.
pub fn collect_subgraph(desired: &DagState, anchor: &QualifiedName) -> Vec<QualifiedName> {
    // Build a set of all stream table names for O(1) lookup.
    let all_names: std::collections::HashSet<QualifiedName> = desired
        .stream_tables
        .iter()
        .map(|t| t.qualified_name.clone())
        .collect();

    if !all_names.contains(anchor) {
        return vec![];
    }

    // BFS/DFS to collect ancestors (nodes that anchor depends on, transitively)
    // and descendants (nodes that depend on anchor, transitively).
    let mut subgraph: std::collections::HashSet<QualifiedName> =
        std::collections::HashSet::new();
    subgraph.insert(anchor.clone());

    // Forward BFS: collect all descendants.
    let mut queue: std::collections::VecDeque<QualifiedName> =
        std::collections::VecDeque::new();
    queue.push_back(anchor.clone());
    while let Some(current) = queue.pop_front() {
        for table in &desired.stream_tables {
            if table.depends_on.contains(&current) && !subgraph.contains(&table.qualified_name) {
                subgraph.insert(table.qualified_name.clone());
                queue.push_back(table.qualified_name.clone());
            }
        }
    }

    // Backward BFS: collect all ancestors.
    queue.push_back(anchor.clone());
    while let Some(current) = queue.pop_front() {
        if let Some(table) = desired
            .stream_tables
            .iter()
            .find(|t| t.qualified_name == current)
        {
            for dep in &table.depends_on {
                if all_names.contains(dep) && !subgraph.contains(dep) {
                    subgraph.insert(dep.clone());
                    queue.push_back(dep.clone());
                }
            }
        }
    }

    subgraph.into_iter().collect()
}

/// Create a native preview environment in the same database.
///
/// Steps:
/// 1. Create the scratch schema (`aqueduct_preview_{branch}`).
/// 2. For each source table referenced by the desired DAG, create a sampled copy
///    in the scratch schema using `CREATE TABLE ... AS SELECT * FROM ... TABLESAMPLE BERNOULLI(pct)`.
/// 3. For each stream table in the desired DAG, create it in the scratch schema
///    (rewritten to read from the sampled source copies).
///
/// When `config.anchor_table` is set, only the subgraph containing the anchor is
/// previewed (P-07 / v0.12).
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

    // Determine which tables to include (P-07: subgraph filtering).
    let subgraph_names: Option<std::collections::HashSet<QualifiedName>> =
        config.anchor_table.as_ref().map(|anchor| {
            collect_subgraph(desired, anchor)
                .into_iter()
                .collect()
        });

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

    // Create stream tables in the preview schema.
    for stream_table in &desired.stream_tables {
        // P-07: skip tables not in the subgraph when an anchor is set.
        if let Some(ref names) = subgraph_names {
            if !names.contains(&stream_table.qualified_name) {
                continue;
            }
        }

        // SEC-03: Use AST-based query rewriting instead of string replacement.
        let rewritten_query =
            rewrite_query_for_preview_ast(&stream_table.query, &schema, desired);

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

/// Rewrite a SQL query for a preview environment using AST-based table reference replacement (SEC-03 / v0.12).
///
/// Parses the SQL with sqlparser, walks the AST finding `TableFactor::Table` nodes,
/// and replaces known source/stream table references with their preview-schema equivalents.
/// Re-serialises using sqlparser's Display impl so that the output is syntactically correct
/// regardless of how complex the original query is.
///
/// Falls back to the legacy string-based rewrite if parsing fails.
pub fn rewrite_query_for_preview_ast(
    query: &str,
    preview_schema: &str,
    desired: &DagState,
) -> String {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Build the set of table names to rewrite.
    let mut rewrites: std::collections::HashMap<QualifiedName, QualifiedName> =
        std::collections::HashMap::new();

    for source in &desired.sources {
        rewrites.insert(
            source.qualified_name.clone(),
            QualifiedName::new(preview_schema, &source.qualified_name.name),
        );
        // Also register an unqualified version for queries that omit the schema.
        rewrites.insert(
            QualifiedName::new("public", &source.qualified_name.name),
            QualifiedName::new(preview_schema, &source.qualified_name.name),
        );
    }
    for stream_table in &desired.stream_tables {
        rewrites.insert(
            stream_table.qualified_name.clone(),
            QualifiedName::new(preview_schema, &stream_table.qualified_name.name),
        );
        rewrites.insert(
            QualifiedName::new("public", &stream_table.qualified_name.name),
            QualifiedName::new(preview_schema, &stream_table.qualified_name.name),
        );
    }

    let dialect = PostgreSqlDialect {};
    let Ok(mut stmts) = Parser::parse_sql(&dialect, query) else {
        // Fallback: string-based replacement.
        return rewrite_query_for_preview_string(query, preview_schema, desired);
    };

    // Walk and rewrite each statement.
    for stmt in &mut stmts {
        rewrite_stmt(stmt, &rewrites);
    }

    stmts
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

fn rewrite_stmt(
    stmt: &mut sqlparser::ast::Statement,
    rewrites: &std::collections::HashMap<QualifiedName, QualifiedName>,
) {
    use sqlparser::ast::Statement;
    if let Statement::Query(q) = stmt {
        rewrite_set_expr(&mut q.body, rewrites);
    }
}

fn rewrite_set_expr(
    expr: &mut sqlparser::ast::SetExpr,
    rewrites: &std::collections::HashMap<QualifiedName, QualifiedName>,
) {
    use sqlparser::ast::SetExpr;
    match expr {
        SetExpr::Select(sel) => {
            for twj in &mut sel.from {
                rewrite_table_factor(&mut twj.relation, rewrites);
                for j in &mut twj.joins {
                    rewrite_table_factor(&mut j.relation, rewrites);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, rewrites);
            rewrite_set_expr(right, rewrites);
        }
        _ => {}
    }
}

fn rewrite_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    rewrites: &std::collections::HashMap<QualifiedName, QualifiedName>,
) {
    use sqlparser::ast::{Ident, ObjectName, TableFactor};
    match factor {
        TableFactor::Table { name, .. } => {
            let parts: Vec<&str> = name.0.iter().map(|id| id.value.as_str()).collect();
            let qname = match parts.as_slice() {
                [schema, table] => QualifiedName::new(*schema, *table),
                [table] => QualifiedName::new("public", *table),
                _ => return,
            };
            if let Some(new_name) = rewrites.get(&qname) {
                *name = ObjectName(vec![
                    Ident::new(&new_name.schema),
                    Ident::new(&new_name.name),
                ]);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_set_expr(&mut subquery.body, rewrites);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(&mut table_with_joins.relation, rewrites);
            for j in &mut table_with_joins.joins {
                rewrite_table_factor(&mut j.relation, rewrites);
            }
        }
        _ => {}
    }
}

/// Legacy string-based query rewrite (fallback when AST parsing fails).
fn rewrite_query_for_preview_string(
    query: &str,
    preview_schema: &str,
    desired: &DagState,
) -> String {
    let mut rewritten = query.to_string();

    for source in &desired.sources {
        let original = format!(
            "{}.{}",
            source.qualified_name.schema, source.qualified_name.name
        );
        let replacement = format!("{}.{}", preview_schema, source.qualified_name.name);
        rewritten = rewritten.replace(&original, &replacement);
    }

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
    use crate::dag::{DagState, QualifiedName, RefreshMode, StreamTableSpec};

    fn make_state_with_source(source_schema: &str, source_table: &str, query: &str) -> DagState {
        use crate::dag::SourceSpec;
        DagState {
            stream_tables: vec![StreamTableSpec {
                qualified_name: QualifiedName::new("public", "order_totals"),
                query: query.to_string(),
                refresh_mode: RefreshMode::Differential,
                schedule: "30s".to_string(),
                cdc_mode: None,
                explicit_depends_on: vec![],
                depends_on: vec![],
                cypher_source: None,
            }],
            sources: vec![SourceSpec {
                qualified_name: QualifiedName::new(source_schema, source_table),
                owned: false,
                create_sql: None,
            }],
            consumers: vec![],
        }
    }

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

    /// SEC-03: AST-based query rewriting replaces table references correctly.
    #[test]
    fn test_ast_rewrite_simple_select() {
        let query = "SELECT id, total FROM public.orders WHERE status = 'open'";
        let desired = make_state_with_source("public", "orders", query);
        let rewritten = rewrite_query_for_preview_ast(query, "preview_branch", &desired);
        assert!(
            rewritten.contains("preview_branch"),
            "rewritten query should reference preview schema"
        );
        assert!(
            !rewritten.contains("public.orders") || rewritten.contains("preview_branch.orders"),
            "original reference should be replaced"
        );
    }

    /// SEC-03: AST rewrite handles JOIN queries.
    #[test]
    fn test_ast_rewrite_join() {
        use crate::dag::SourceSpec;
        let query =
            "SELECT o.id, i.qty FROM public.orders o JOIN public.order_items i ON o.id = i.order_id";
        let desired = DagState {
            stream_tables: vec![StreamTableSpec {
                qualified_name: QualifiedName::new("public", "order_totals"),
                query: query.to_string(),
                refresh_mode: RefreshMode::Differential,
                schedule: "30s".to_string(),
                cdc_mode: None,
                explicit_depends_on: vec![],
                depends_on: vec![],
                cypher_source: None,
            }],
            sources: vec![
                SourceSpec {
                    qualified_name: QualifiedName::new("public", "orders"),
                    owned: false,
                    create_sql: None,
                },
                SourceSpec {
                    qualified_name: QualifiedName::new("public", "order_items"),
                    owned: false,
                    create_sql: None,
                },
            ],
            consumers: vec![],
        };
        let rewritten = rewrite_query_for_preview_ast(query, "preview_feat", &desired);
        assert!(rewritten.contains("preview_feat"), "should reference preview schema");
        assert!(!rewritten.contains("public.orders"), "should replace public.orders");
        assert!(!rewritten.contains("public.order_items"), "should replace public.order_items");
    }

    /// P-07: collect_subgraph returns the anchor and its dependencies.
    #[test]
    fn test_collect_subgraph_anchor_with_dep() {
        let desired = DagState {
            stream_tables: vec![
                StreamTableSpec {
                    qualified_name: QualifiedName::new("public", "base"),
                    query: "SELECT 1".to_string(),
                    refresh_mode: RefreshMode::Differential,
                    schedule: "30s".to_string(),
                    cdc_mode: None,
                    explicit_depends_on: vec![],
                    depends_on: vec![],
                    cypher_source: None,
                },
                StreamTableSpec {
                    qualified_name: QualifiedName::new("public", "derived"),
                    query: "SELECT * FROM public.base".to_string(),
                    refresh_mode: RefreshMode::Differential,
                    schedule: "30s".to_string(),
                    cdc_mode: None,
                    explicit_depends_on: vec![],
                    depends_on: vec![QualifiedName::new("public", "base")],
                    cypher_source: None,
                },
                StreamTableSpec {
                    qualified_name: QualifiedName::new("public", "unrelated"),
                    query: "SELECT 99".to_string(),
                    refresh_mode: RefreshMode::Differential,
                    schedule: "30s".to_string(),
                    cdc_mode: None,
                    explicit_depends_on: vec![],
                    depends_on: vec![],
                    cypher_source: None,
                },
            ],
            sources: vec![],
            consumers: vec![],
        };

        let subgraph = collect_subgraph(&desired, &QualifiedName::new("public", "derived"));
        assert!(subgraph.contains(&QualifiedName::new("public", "derived")));
        assert!(subgraph.contains(&QualifiedName::new("public", "base")));
        assert!(!subgraph.contains(&QualifiedName::new("public", "unrelated")));
    }

    /// P-07: collect_subgraph returns empty for unknown anchor.
    #[test]
    fn test_collect_subgraph_unknown_anchor() {
        let desired = DagState::default();
        let result = collect_subgraph(&desired, &QualifiedName::new("public", "nonexistent"));
        assert!(result.is_empty());
    }
}
