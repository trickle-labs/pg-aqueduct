use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::dag::{ConsumerSpec, DagState, QualifiedName, StreamTableSpec};

/// Regex for collapsing whitespace during SQL normalisation.
static WHITESPACE_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\s+").expect("valid static regex"));

/// Source dependency index: maps each `QualifiedName` source table to the set of
/// stream tables whose queries reference it.
///
/// Built once per `compute_diff` call (P-01 / v0.12) so that cascade impact
/// detection is O(N) rather than O(changed_sources × stream_tables × parse_cost).
type SourceIndex = std::collections::HashMap<QualifiedName, Vec<QualifiedName>>;

/// Build the source dependency index for a `DagState`.
///
/// For every stream table, parse its query once and collect the set of source
/// tables it references.  The result is a map from source name to the list of
/// stream table names that depend on it.
fn build_source_index(desired: &DagState) -> SourceIndex {
    let mut index: SourceIndex = std::collections::HashMap::new();

    for stream_table in &desired.stream_tables {
        // Collect every table reference from the stream query.
        let refs = collect_table_refs(&stream_table.query);
        for source_ref in refs {
            index
                .entry(source_ref)
                .or_default()
                .push(stream_table.qualified_name.clone());
        }
    }

    index
}

/// Parse a SQL query and return all qualified table names referenced in FROM / JOIN clauses.
fn collect_table_refs(sql: &str) -> Vec<QualifiedName> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let Ok(stmts) = Parser::parse_sql(&dialect, sql) else {
        return vec![];
    };

    let mut refs = Vec::new();
    for stmt in &stmts {
        collect_refs_stmt(stmt, &mut refs);
    }
    refs
}

fn collect_refs_stmt(stmt: &sqlparser::ast::Statement, out: &mut Vec<QualifiedName>) {
    use sqlparser::ast::Statement;
    if let Statement::Query(q) = stmt {
        collect_refs_set_expr(&q.body, out);
    }
}

fn collect_refs_set_expr(expr: &sqlparser::ast::SetExpr, out: &mut Vec<QualifiedName>) {
    use sqlparser::ast::SetExpr;
    match expr {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                collect_refs_factor(&twj.relation, out);
                for j in &twj.joins {
                    collect_refs_factor(&j.relation, out);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_refs_set_expr(left, out);
            collect_refs_set_expr(right, out);
        }
        _ => {}
    }
}

fn collect_refs_factor(factor: &sqlparser::ast::TableFactor, out: &mut Vec<QualifiedName>) {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Table { name, .. } => {
            let parts: Vec<&str> = name.0.iter().map(|id| id.value.as_str()).collect();
            let qname = match parts.as_slice() {
                [schema, table] => QualifiedName::new(*schema, *table),
                [table] => QualifiedName::new("public", *table),
                _ => return,
            };
            out.push(qname);
        }
        TableFactor::Derived { subquery, .. } => {
            collect_refs_set_expr(&subquery.body, out);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_refs_factor(&table_with_joins.relation, out);
            for j in &table_with_joins.joins {
                collect_refs_factor(&j.relation, out);
            }
        }
        _ => {}
    }
}

/// The kind of change for a single stream table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaKind {
    /// New stream table not present in actual state.
    Create,
    /// Stream table present in actual state but not in desired state.
    Drop,
    /// SQL query changed.
    AlterQuery,
    /// Schedule changed only.
    AlterSchedule,
    /// refresh_mode changed only.
    AlterRefreshMode,
    /// cdc_mode changed only.
    AlterCdcMode,
    /// Multiple metadata changes (schedule + refresh_mode etc.).
    AlterMetadata,
    /// No change.
    Unchanged,
}

/// A change to a single node in the stream-table DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDelta {
    pub qualified_name: QualifiedName,
    pub kind: DeltaKind,
    /// The desired spec (None for drops).
    pub desired: Option<StreamTableSpec>,
    /// The actual spec (None for creates).
    pub actual: Option<StreamTableSpec>,
}

/// The kind of change to a source (base) table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceDeltaKind {
    /// DDL has changed for an owned source table.
    AlterDdl,
    /// No change detected.
    Unchanged,
}

/// Cascade impact of a source-table change on a downstream stream table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeImpact {
    /// The stream table that is affected.
    pub stream_table: QualifiedName,
    /// The migration class required for this stream table due to the source change.
    pub class: String,
}

/// A change to a source (base) table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDelta {
    pub qualified_name: QualifiedName,
    pub kind: SourceDeltaKind,
    /// The desired DDL (from migration file).
    pub desired_ddl: Option<String>,
    /// Stream tables that reference this source and must be cascade-updated.
    pub cascade_impacts: Vec<CascadeImpact>,
}

/// The kind of change to a consumer view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerDeltaKind {
    /// New consumer view not present in live state.
    Create,
    /// Consumer view present in live state but not in desired state.
    Drop,
    /// Consumer view definition changed.
    Alter,
    /// No change.
    Unchanged,
}

/// A change to a consumer view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerDelta {
    /// The view name being managed (expose_as).
    pub expose_as: QualifiedName,
    pub kind: ConsumerDeltaKind,
    /// The desired spec (None for drops).
    pub desired: Option<ConsumerSpec>,
    /// The actual source the view currently points to (None for creates).
    pub actual_source: Option<QualifiedName>,
}

/// The result of comparing desired vs. actual DAG state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DagDiff {
    pub deltas: Vec<NodeDelta>,
    /// Source (base table) changes with their cascade impacts.
    pub source_deltas: Vec<SourceDelta>,
    /// Consumer view changes.
    pub consumer_deltas: Vec<ConsumerDelta>,
}

impl DagDiff {
    pub fn is_empty(&self) -> bool {
        self.deltas.iter().all(|d| d.kind == DeltaKind::Unchanged)
            && self
                .source_deltas
                .iter()
                .all(|s| s.kind == SourceDeltaKind::Unchanged)
            && self
                .consumer_deltas
                .iter()
                .all(|c| c.kind == ConsumerDeltaKind::Unchanged)
    }

    pub fn changes(&self) -> Vec<&NodeDelta> {
        self.deltas
            .iter()
            .filter(|d| d.kind != DeltaKind::Unchanged)
            .collect()
    }

    pub fn source_changes(&self) -> Vec<&SourceDelta> {
        self.source_deltas
            .iter()
            .filter(|s| s.kind != SourceDeltaKind::Unchanged)
            .collect()
    }

    pub fn consumer_changes(&self) -> Vec<&ConsumerDelta> {
        self.consumer_deltas
            .iter()
            .filter(|c| c.kind != ConsumerDeltaKind::Unchanged)
            .collect()
    }
}

/// Compare the desired DAG state (from migrations directory) against the actual state
/// (from the live database catalog).
pub fn compute_diff(desired: &DagState, actual: &DagState) -> DagDiff {
    let mut deltas = Vec::new();

    // Find creates and alters.
    for desired_table in &desired.stream_tables {
        let actual_table = actual.find_stream_table(&desired_table.qualified_name);

        let delta_kind = match actual_table {
            None => DeltaKind::Create,
            Some(actual) => classify_change(desired_table, actual),
        };

        deltas.push(NodeDelta {
            qualified_name: desired_table.qualified_name.clone(),
            kind: delta_kind,
            desired: Some(desired_table.clone()),
            actual: actual_table.cloned(),
        });
    }

    // Find drops: tables in actual but not in desired.
    for actual_table in &actual.stream_tables {
        let in_desired = desired
            .find_stream_table(&actual_table.qualified_name)
            .is_some();
        if !in_desired {
            deltas.push(NodeDelta {
                qualified_name: actual_table.qualified_name.clone(),
                kind: DeltaKind::Drop,
                desired: None,
                actual: Some(actual_table.clone()),
            });
        }
    }

    // Compute source deltas (owned source tables whose DDL changed).
    let source_deltas = compute_source_deltas(desired, actual, &deltas);

    // Compute consumer view deltas.
    let consumer_deltas = compute_consumer_deltas(desired, actual);

    DagDiff {
        deltas,
        source_deltas,
        consumer_deltas,
    }
}

/// Compute source-table deltas and their cascade impacts on stream tables.
///
/// For owned source tables, we compare desired DDL against actual DDL (stored in
/// `aqueduct.dag_versions`).  When the DDL changes, we identify every stream-table
/// node whose query references the source table and mark it for rebuild.
///
/// Uses a pre-built source dependency index (P-01 / v0.12) so each stream-table
/// query is parsed exactly once regardless of how many sources changed.
fn compute_source_deltas(
    desired: &DagState,
    actual: &DagState,
    _stream_deltas: &[NodeDelta],
) -> Vec<SourceDelta> {
    // P-01: Build source index once, then look up per changed source in O(1).
    let source_index = build_source_index(desired);

    let mut source_deltas = Vec::new();

    for desired_source in &desired.sources {
        if !desired_source.owned {
            continue;
        }

        let actual_source = actual.find_source(&desired_source.qualified_name);

        let kind = match actual_source {
            None => SourceDeltaKind::AlterDdl,
            Some(actual_src) => {
                if normalise_sql(desired_source.create_sql.as_deref().unwrap_or(""))
                    != normalise_sql(actual_src.create_sql.as_deref().unwrap_or(""))
                {
                    SourceDeltaKind::AlterDdl
                } else {
                    SourceDeltaKind::Unchanged
                }
            }
        };

        if kind == SourceDeltaKind::Unchanged {
            continue;
        }

        // O(1) lookup using the pre-built index instead of re-parsing all queries.
        let cascade_impacts = source_index
            .get(&desired_source.qualified_name)
            .map(|tables| {
                tables
                    .iter()
                    .map(|t| CascadeImpact {
                        stream_table: t.clone(),
                        class: "rebuild".to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        source_deltas.push(SourceDelta {
            qualified_name: desired_source.qualified_name.clone(),
            kind,
            desired_ddl: desired_source.create_sql.clone(),
            cascade_impacts,
        });
    }

    source_deltas
}

/// Check whether a SQL query references the given qualified table name.
/// Kept for targeted cascade checks and tests.
pub fn query_references_table(sql: &str, target: &QualifiedName) -> bool {
    collect_table_refs(sql).contains(target)
}

fn classify_change(desired: &StreamTableSpec, actual: &StreamTableSpec) -> DeltaKind {
    let query_changed = normalise_sql(&desired.query) != normalise_sql(&actual.query);
    let schedule_changed = desired.schedule != actual.schedule;
    let refresh_mode_changed = desired.refresh_mode != actual.refresh_mode;
    let cdc_mode_changed = desired.cdc_mode != actual.cdc_mode;

    if query_changed {
        return DeltaKind::AlterQuery;
    }

    let metadata_changes = [schedule_changed, refresh_mode_changed, cdc_mode_changed]
        .iter()
        .filter(|&&c| c)
        .count();

    match metadata_changes {
        0 => DeltaKind::Unchanged,
        _ if schedule_changed && !refresh_mode_changed && !cdc_mode_changed => {
            DeltaKind::AlterSchedule
        }
        _ if refresh_mode_changed && !schedule_changed && !cdc_mode_changed => {
            DeltaKind::AlterRefreshMode
        }
        _ if cdc_mode_changed && !schedule_changed && !refresh_mode_changed => {
            DeltaKind::AlterCdcMode
        }
        _ => DeltaKind::AlterMetadata,
    }
}

/// Normalise SQL for comparison: collapse whitespace, uppercase keywords, strip trailing semicolons.
/// This is a simple normalisation — not a full AST comparison.
fn normalise_sql(sql: &str) -> String {
    // Collapse whitespace sequences to a single space.
    let s = WHITESPACE_RE
        .replace_all(sql.trim(), " ")
        .to_string()
        .to_lowercase();
    // Strip trailing semicolons (migration files include them; live catalog doesn't).
    s.trim_end_matches(';').trim().to_string()
}

/// Compute consumer-view deltas between desired and actual state.
///
/// The "actual" consumer state is always read from the live DB (or provided externally).
/// When called from `compute_diff`, the actual `DagState` carries the live consumer list
/// (populated by `read_live_state`). When the actual state has no consumers (e.g., first
/// deployment), all desired consumers are emitted as Create deltas.
fn compute_consumer_deltas(desired: &DagState, actual: &DagState) -> Vec<ConsumerDelta> {
    let mut consumer_deltas = Vec::new();

    // Find creates and alters.
    for desired_consumer in &desired.consumers {
        let actual_consumer = actual
            .consumers
            .iter()
            .find(|c| c.expose_as == desired_consumer.expose_as);

        let kind = match actual_consumer {
            None => ConsumerDeltaKind::Create,
            Some(ac) => {
                // Compare source and SQL body.
                if ac.source != desired_consumer.source || ac.sql_body != desired_consumer.sql_body
                {
                    ConsumerDeltaKind::Alter
                } else {
                    ConsumerDeltaKind::Unchanged
                }
            }
        };

        if kind == ConsumerDeltaKind::Unchanged {
            continue;
        }

        consumer_deltas.push(ConsumerDelta {
            expose_as: desired_consumer.expose_as.clone(),
            kind,
            desired: Some(desired_consumer.clone()),
            actual_source: actual_consumer.map(|ac| ac.source.clone()),
        });
    }

    // Find drops: consumer views in actual but not in desired.
    for actual_consumer in &actual.consumers {
        let in_desired = desired
            .consumers
            .iter()
            .any(|c| c.expose_as == actual_consumer.expose_as);
        if !in_desired {
            consumer_deltas.push(ConsumerDelta {
                expose_as: actual_consumer.expose_as.clone(),
                kind: ConsumerDeltaKind::Drop,
                desired: None,
                actual_source: Some(actual_consumer.source.clone()),
            });
        }
    }

    consumer_deltas
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{DagState, QualifiedName, RefreshMode, StreamTableSpec};

    fn make_spec(name: &str, query: &str, schedule: &str) -> StreamTableSpec {
        StreamTableSpec {
            qualified_name: QualifiedName::new("public", name),
            query: query.to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: schedule.to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
            cypher_source: None,
        }
    }

    #[test]
    fn test_diff_create() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
            consumers: vec![],
        };
        let actual = DagState::default();
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas.len(), 1);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Create);
    }

    #[test]
    fn test_diff_drop() {
        let desired = DagState::default();
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
            consumers: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Drop);
    }

    #[test]
    fn test_diff_unchanged() {
        let tables = vec![make_spec("t1", "SELECT 1", "30s")];
        let desired = DagState {
            stream_tables: tables.clone(),
            sources: vec![],
            consumers: vec![],
        };
        let actual = DagState {
            stream_tables: tables,
            sources: vec![],
            consumers: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Unchanged);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_alter_schedule() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "1m")],
            sources: vec![],
            consumers: vec![],
        };
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
            consumers: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::AlterSchedule);
    }

    #[test]
    fn test_diff_alter_query() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 2", "30s")],
            sources: vec![],
            consumers: vec![],
        };
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
            consumers: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::AlterQuery);
    }

    /// P-01: Source dependency index is built once and used for cascade impact
    /// detection without re-parsing every stream query.
    #[test]
    fn test_source_index_builds_correctly() {
        use crate::dag::{QualifiedName, RefreshMode, StreamTableSpec};

        let q = "SELECT id, total FROM public.orders JOIN public.order_items USING (id)";
        let desired = DagState {
            stream_tables: vec![StreamTableSpec {
                qualified_name: QualifiedName::new("public", "order_totals"),
                query: q.to_string(),
                refresh_mode: RefreshMode::Differential,
                schedule: "30s".to_string(),
                cdc_mode: None,
                explicit_depends_on: vec![],
                depends_on: vec![],
                cypher_source: None,
            }],
            sources: vec![],
            consumers: vec![],
        };

        let index = build_source_index(&desired);
        let orders_key = QualifiedName::new("public", "orders");
        let items_key = QualifiedName::new("public", "order_items");

        assert!(
            index.contains_key(&orders_key),
            "index should contain orders"
        );
        assert!(
            index.contains_key(&items_key),
            "index should contain order_items"
        );

        let orders_deps = &index[&orders_key];
        assert!(orders_deps.contains(&QualifiedName::new("public", "order_totals")));
    }

    /// P-01: query_references_table uses the shared collector internally.
    #[test]
    fn test_query_references_table() {
        let sql = "SELECT * FROM public.orders WHERE id = 1";
        let target = QualifiedName::new("public", "orders");
        let other = QualifiedName::new("public", "products");
        assert!(query_references_table(sql, &target));
        assert!(!query_references_table(sql, &other));
    }
}
