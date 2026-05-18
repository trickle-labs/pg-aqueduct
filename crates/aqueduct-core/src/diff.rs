use serde::{Deserialize, Serialize};

use crate::dag::{ConsumerSpec, DagState, QualifiedName, StreamTableSpec};

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
fn compute_source_deltas(
    desired: &DagState,
    actual: &DagState,
    _stream_deltas: &[NodeDelta],
) -> Vec<SourceDelta> {
    let mut source_deltas = Vec::new();

    for desired_source in &desired.sources {
        if !desired_source.owned {
            // Unowned sources: tracked for reference but no DDL emitted.
            continue;
        }

        let actual_source = actual.find_source(&desired_source.qualified_name);

        let kind = match actual_source {
            None => SourceDeltaKind::AlterDdl, // New owned source — treat as DDL change.
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

        // Find stream tables that reference this source table.
        let cascade_impacts = find_cascade_impacts(&desired_source.qualified_name, desired);

        source_deltas.push(SourceDelta {
            qualified_name: desired_source.qualified_name.clone(),
            kind,
            desired_ddl: desired_source.create_sql.clone(),
            cascade_impacts,
        });
    }

    source_deltas
}

/// Find all stream tables that reference the given source table in their query.
///
/// Returns the list of impacted stream tables with their migration class
/// (always Rebuild for a base-table DDL change, since column structure may have changed).
fn find_cascade_impacts(source: &QualifiedName, desired: &DagState) -> Vec<CascadeImpact> {
    let mut impacts = Vec::new();

    for stream_table in &desired.stream_tables {
        if query_references_table(&stream_table.query, source) {
            impacts.push(CascadeImpact {
                stream_table: stream_table.qualified_name.clone(),
                class: "rebuild".to_string(),
            });
        }
    }

    impacts
}

/// Check whether a SQL query references the given qualified table name.
fn query_references_table(sql: &str, target: &QualifiedName) -> bool {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let Ok(stmts) = Parser::parse_sql(&dialect, sql) else {
        return false;
    };

    for stmt in &stmts {
        if stmt_references_table(stmt, target) {
            return true;
        }
    }
    false
}

fn stmt_references_table(stmt: &sqlparser::ast::Statement, target: &QualifiedName) -> bool {
    use sqlparser::ast::Statement;
    if let Statement::Query(q) = stmt {
        return set_expr_references_table(&q.body, target);
    }
    false
}

fn set_expr_references_table(expr: &sqlparser::ast::SetExpr, target: &QualifiedName) -> bool {
    use sqlparser::ast::SetExpr;
    match expr {
        SetExpr::Select(sel) => {
            for table_with_joins in &sel.from {
                if table_factor_references(&table_with_joins.relation, target) {
                    return true;
                }
                for join in &table_with_joins.joins {
                    if table_factor_references(&join.relation, target) {
                        return true;
                    }
                }
            }
            false
        }
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_references_table(left, target) || set_expr_references_table(right, target)
        }
        _ => false,
    }
}

fn table_factor_references(factor: &sqlparser::ast::TableFactor, target: &QualifiedName) -> bool {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Table { name, .. } => {
            let parts: Vec<&str> = name.0.iter().map(|id| id.value.as_str()).collect();
            let qname = match parts.as_slice() {
                [schema, table] => QualifiedName::new(*schema, *table),
                [table] => QualifiedName::new("public", *table),
                _ => return false,
            };
            &qname == target
        }
        TableFactor::Derived { subquery, .. } => set_expr_references_table(&subquery.body, target),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            if table_factor_references(&table_with_joins.relation, target) {
                return true;
            }
            table_with_joins
                .joins
                .iter()
                .any(|j| table_factor_references(&j.relation, target))
        }
        _ => false,
    }
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
    let re = regex::Regex::new(r"\s+").unwrap();
    let s = re.replace_all(sql.trim(), " ").to_string().to_lowercase();
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
}
