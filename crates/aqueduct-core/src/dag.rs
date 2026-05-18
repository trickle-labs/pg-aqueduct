use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::error::{AqueductError, Result};
use crate::parser::{MigrationFile, MigrationKind};

/// Refresh mode for a stream table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RefreshMode {
    #[default]
    Differential,
    Full,
}

impl std::fmt::Display for RefreshMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshMode::Differential => write!(f, "DIFFERENTIAL"),
            RefreshMode::Full => write!(f, "FULL"),
        }
    }
}

impl std::str::FromStr for RefreshMode {
    type Err = AqueductError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "DIFFERENTIAL" => Ok(RefreshMode::Differential),
            "FULL" => Ok(RefreshMode::Full),
            other => Err(AqueductError::Config(format!(
                "unknown refresh_mode: '{}'",
                other
            ))),
        }
    }
}

/// Fully-qualified table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

impl QualifiedName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }

    pub fn from_str_parts(s: &str) -> Self {
        if let Some((schema, name)) = s.split_once('.') {
            Self::new(schema, name)
        } else {
            Self::new("public", s)
        }
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// A stream table node in the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTableSpec {
    pub qualified_name: QualifiedName,
    pub query: String,
    pub refresh_mode: RefreshMode,
    pub schedule: String,
    pub cdc_mode: Option<String>,
    /// Dependencies declared explicitly in front-matter.
    pub explicit_depends_on: Vec<QualifiedName>,
    /// All dependencies (explicit + inferred from SQL).
    pub depends_on: Vec<QualifiedName>,
}

/// A source (base table) tracked for cascade analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpec {
    pub qualified_name: QualifiedName,
    /// Whether aqueduct can emit DDL for this table (false = tracked only).
    pub owned: bool,
    /// The DDL to create the source (only used when owned = true).
    pub create_sql: Option<String>,
}

/// The complete desired DAG state parsed from migration files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DagState {
    pub stream_tables: Vec<StreamTableSpec>,
    pub sources: Vec<SourceSpec>,
}

impl DagState {
    pub fn find_stream_table(&self, name: &QualifiedName) -> Option<&StreamTableSpec> {
        self.stream_tables
            .iter()
            .find(|t| &t.qualified_name == name)
    }

    pub fn find_source(&self, name: &QualifiedName) -> Option<&SourceSpec> {
        self.sources.iter().find(|s| &s.qualified_name == name)
    }
}

/// Build a DagState from a list of parsed migration files.
pub fn build_dag_state(files: &[MigrationFile], infer_deps: bool) -> Result<DagState> {
    let mut stream_tables = Vec::new();
    let mut sources = Vec::new();

    for file in files {
        match file.front_matter.kind {
            MigrationKind::Stream => {
                let schema = file
                    .front_matter
                    .schema
                    .clone()
                    .unwrap_or_else(|| "public".to_string());

                let qualified_name = QualifiedName::new(&schema, &file.name);

                let refresh_mode = file
                    .front_matter
                    .refresh_mode
                    .as_deref()
                    .map(|s| s.parse())
                    .transpose()?
                    .unwrap_or_default();

                let schedule = file
                    .front_matter
                    .schedule
                    .clone()
                    .unwrap_or_else(|| "30s".to_string());

                let explicit_depends_on: Vec<QualifiedName> = file
                    .front_matter
                    .depends_on
                    .iter()
                    .map(|s| QualifiedName::from_str_parts(s))
                    .collect();

                // Infer additional dependencies from SQL.
                let inferred_deps = if infer_deps && !file.sql_body.is_empty() {
                    infer_sql_dependencies(&file.sql_body)
                } else {
                    Vec::new()
                };

                // Merge explicit + inferred, deduplicated.
                let mut all_deps = explicit_depends_on.clone();
                for dep in inferred_deps {
                    if !all_deps.contains(&dep) {
                        all_deps.push(dep);
                    }
                }

                stream_tables.push(StreamTableSpec {
                    qualified_name,
                    query: file.sql_body.clone(),
                    refresh_mode,
                    schedule,
                    cdc_mode: file.front_matter.cdc_mode.clone(),
                    explicit_depends_on,
                    depends_on: all_deps,
                });
            }
            MigrationKind::Source => {
                let owned = file.front_matter.owned.unwrap_or(false);
                // Extract schema.table from the SQL body if possible, or from file name.
                let schema = file
                    .front_matter
                    .schema
                    .clone()
                    .unwrap_or_else(|| "public".to_string());
                let qualified_name = QualifiedName::new(&schema, &file.name);

                sources.push(SourceSpec {
                    qualified_name,
                    owned,
                    create_sql: if !file.sql_body.is_empty() {
                        Some(file.sql_body.clone())
                    } else {
                        None
                    },
                });
            }
            MigrationKind::Consumer => {
                // Consumer views are tracked but not yet fully managed in v0.1.
            }
        }
    }

    Ok(DagState {
        stream_tables,
        sources,
    })
}

/// Infer table dependencies from a SQL query by extracting FROM / JOIN references.
/// This is a best-effort analysis; use explicit `@aqueduct:depends_on` for edge cases.
fn infer_sql_dependencies(sql: &str) -> Vec<QualifiedName> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let Ok(stmts) = Parser::parse_sql(&dialect, sql) else {
        return Vec::new();
    };

    let mut deps = Vec::new();
    let mut seen = HashSet::new();

    for stmt in &stmts {
        collect_table_refs(stmt, &mut deps, &mut seen);
    }

    deps
}

fn collect_table_refs(
    stmt: &sqlparser::ast::Statement,
    deps: &mut Vec<QualifiedName>,
    seen: &mut HashSet<QualifiedName>,
) {
    use sqlparser::ast::Statement;

    if let Statement::Query(q) = stmt {
        collect_from_set_expr(&q.body, deps, seen);
    }
}

fn collect_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    deps: &mut Vec<QualifiedName>,
    seen: &mut HashSet<QualifiedName>,
) {
    use sqlparser::ast::SetExpr;

    match expr {
        SetExpr::Select(sel) => {
            for table_with_joins in &sel.from {
                collect_table_factor(&table_with_joins.relation, deps, seen);
                for join in &table_with_joins.joins {
                    collect_table_factor(&join.relation, deps, seen);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_from_set_expr(left, deps, seen);
            collect_from_set_expr(right, deps, seen);
        }
        _ => {}
    }
}

fn collect_table_factor(
    factor: &sqlparser::ast::TableFactor,
    deps: &mut Vec<QualifiedName>,
    seen: &mut HashSet<QualifiedName>,
) {
    use sqlparser::ast::TableFactor;

    match factor {
        TableFactor::Table { name, .. } => {
            let parts: Vec<&str> = name.0.iter().map(|id| id.value.as_str()).collect();
            let qname = match parts.as_slice() {
                [schema, table] => QualifiedName::new(*schema, *table),
                [table] => QualifiedName::new("public", *table),
                _ => return,
            };
            if seen.insert(qname.clone()) {
                deps.push(qname);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_from_set_expr(&subquery.body, deps, seen);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_factor(&table_with_joins.relation, deps, seen);
            for join in &table_with_joins.joins {
                collect_table_factor(&join.relation, deps, seen);
            }
        }
        _ => {}
    }
}

/// Compute the topological order of stream tables using Kahn's algorithm.
/// Returns the tables in dependency order (dependencies first).
pub fn topological_sort(state: &DagState) -> Result<Vec<QualifiedName>> {
    // Build a name → index map.
    let names: Vec<&QualifiedName> = state
        .stream_tables
        .iter()
        .map(|t| &t.qualified_name)
        .collect();
    let name_to_idx: HashMap<&QualifiedName, usize> =
        names.iter().enumerate().map(|(i, n)| (*n, i)).collect();

    let n = names.len();
    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];

    // Only consider edges between stream tables (not sources).
    let _stream_names: HashSet<&QualifiedName> = names.iter().copied().collect();

    for (i, table) in state.stream_tables.iter().enumerate() {
        for dep in &table.depends_on {
            if let Some(&j) = name_to_idx.get(dep) {
                // j must come before i.
                adj[j].push(i);
                in_degree[i] += 1;
            }
            // If dep is not in stream_tables, it's a source — no edge needed.
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut sorted = Vec::with_capacity(n);

    while let Some(node) = queue.pop_front() {
        sorted.push(names[node].clone());
        for &neighbor in &adj[node] {
            in_degree[neighbor] -= 1;
            if in_degree[neighbor] == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    if sorted.len() != n {
        // Find the cycle members for a helpful error message.
        let cycle_nodes: Vec<String> = (0..n)
            .filter(|&i| in_degree[i] > 0)
            .map(|i| names[i].to_string())
            .collect();
        return Err(AqueductError::Cycle(cycle_nodes.join(", ")));
    }

    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_migration_file;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn parse_file(name: &str, content: &str) -> MigrationFile {
        parse_migration_file(
            &PathBuf::from(format!("{}.sql", name)),
            content,
            &HashMap::new(),
        )
        .unwrap()
    }

    #[test]
    fn test_build_dag_state_simple() {
        let files = vec![
            parse_file(
                "order_totals",
                "-- @aqueduct:schedule = \"30s\"\nSELECT customer_id, SUM(amount) FROM raw.orders GROUP BY 1;",
            ),
        ];
        let state = build_dag_state(&files, true).unwrap();
        assert_eq!(state.stream_tables.len(), 1);
        assert_eq!(state.stream_tables[0].qualified_name.name, "order_totals");
        assert_eq!(state.stream_tables[0].schedule, "30s");
    }

    #[test]
    fn test_topological_sort_linear() {
        // A -> B -> C (A depends on nothing, B depends on A, C depends on B).
        let files = vec![
            parse_file(
                "c",
                "-- @aqueduct:depends_on = [\"public.b\"]\nSELECT * FROM public.b;",
            ),
            parse_file(
                "b",
                "-- @aqueduct:depends_on = [\"public.a\"]\nSELECT * FROM public.a;",
            ),
            parse_file("a", "-- @aqueduct:schedule = \"30s\"\nSELECT 1;"),
        ];
        let state = build_dag_state(&files, false).unwrap();
        let order = topological_sort(&state).unwrap();
        let names: Vec<&str> = order.iter().map(|q| q.name.as_str()).collect();
        // a must come before b, b before c.
        let a_pos = names.iter().position(|&n| n == "a").unwrap();
        let b_pos = names.iter().position(|&n| n == "b").unwrap();
        let c_pos = names.iter().position(|&n| n == "c").unwrap();
        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);
    }

    #[test]
    fn test_topological_sort_cycle_detection() {
        // A depends on B and B depends on A.
        let files = vec![
            parse_file(
                "a",
                "-- @aqueduct:depends_on = [\"public.b\"]\nSELECT * FROM public.b;",
            ),
            parse_file(
                "b",
                "-- @aqueduct:depends_on = [\"public.a\"]\nSELECT * FROM public.a;",
            ),
        ];
        let state = build_dag_state(&files, false).unwrap();
        let result = topological_sort(&state);
        assert!(result.is_err());
        match result {
            Err(AqueductError::Cycle(_)) => {}
            _ => panic!("expected cycle error"),
        }
    }

    #[test]
    fn test_qualified_name_from_str() {
        let q = QualifiedName::from_str_parts("raw.orders");
        assert_eq!(q.schema, "raw");
        assert_eq!(q.name, "orders");

        let q2 = QualifiedName::from_str_parts("orders");
        assert_eq!(q2.schema, "public");
        assert_eq!(q2.name, "orders");
    }

    #[test]
    fn test_sql_dependency_inference() {
        let sql = "SELECT o.customer_id, SUM(o.amount) FROM raw.orders o JOIN raw.customers c ON o.customer_id = c.id GROUP BY 1;";
        let deps = infer_sql_dependencies(sql);
        let names: Vec<String> = deps.iter().map(|d| d.to_string()).collect();
        assert!(names.contains(&"raw.orders".to_string()));
        assert!(names.contains(&"raw.customers".to_string()));
    }
}
