use serde::{Deserialize, Serialize};

use crate::dag::StreamTableSpec;
use crate::diff::{DeltaKind, NodeDelta};

/// Migration class — how expensive a change is and what technique is used.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MigrationClass {
    /// Metadata-only change: a single `alter_stream_table()` call; no rebuild.
    Free,
    /// `ALTER` + targeted incremental backfill; materialized state preserved.
    InPlace,
    /// Drop + recreate + `FULL` refresh.
    Rebuild,
    /// Build a parallel green DAG, backfill, atomically swap consumer views.
    BlueGreen,
}

impl std::fmt::Display for MigrationClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationClass::Free => write!(f, "free"),
            MigrationClass::InPlace => write!(f, "in-place"),
            MigrationClass::Rebuild => write!(f, "rebuild"),
            MigrationClass::BlueGreen => write!(f, "blue-green"),
        }
    }
}

/// Classify a node delta into a migration class.
///
/// Conservative classification: when in doubt, classify as Rebuild.
pub fn classify_delta(delta: &NodeDelta) -> MigrationClass {
    match delta.kind {
        DeltaKind::Unchanged => MigrationClass::Free,

        DeltaKind::Create | DeltaKind::Drop => MigrationClass::Rebuild,

        // Schedule / refresh-mode / cdc-mode changes are free (metadata only).
        DeltaKind::AlterSchedule
        | DeltaKind::AlterCdcMode
        | DeltaKind::AlterRefreshMode
        | DeltaKind::AlterMetadata => MigrationClass::Free,

        DeltaKind::AlterQuery => classify_query_change(
            delta.desired.as_ref().unwrap(),
            delta.actual.as_ref().unwrap(),
        ),
    }
}

/// Classify a query-change delta.
/// For v0.1 we implement the conservative subset.
fn classify_query_change(desired: &StreamTableSpec, actual: &StreamTableSpec) -> MigrationClass {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};

    let Ok(desired_stmts) = Parser::parse_sql(&dialect, &desired.query) else {
        return MigrationClass::Rebuild;
    };
    let Ok(actual_stmts) = Parser::parse_sql(&dialect, &actual.query) else {
        return MigrationClass::Rebuild;
    };

    if desired_stmts.len() != 1 || actual_stmts.len() != 1 {
        return MigrationClass::Rebuild;
    }

    let (desired_sel, actual_sel) = match (&desired_stmts[0], &actual_stmts[0]) {
        (Statement::Query(d), Statement::Query(a)) => (d, a),
        _ => return MigrationClass::Rebuild,
    };

    // If the structural parts (FROM, WHERE, GROUP BY, HAVING) haven't changed,
    // only the SELECT list changed — check if it's just an addition.
    if structural_parts_match(desired_sel, actual_sel) {
        // Check if desired SELECT list is a superset of actual SELECT list.
        if select_list_is_addition_only(desired_sel, actual_sel) {
            return MigrationClass::InPlace;
        }
    }

    // For refresh_mode FULL → DIFFERENTIAL: that's a Rebuild (needs diff state).
    // For DIFFERENTIAL → FULL: that's Free (just change the mode).
    // But query change + mode change is already handled here as Rebuild.

    MigrationClass::Rebuild
}

fn structural_parts_match(d: &sqlparser::ast::Query, a: &sqlparser::ast::Query) -> bool {
    use sqlparser::ast::SetExpr;

    let (d_sel, a_sel) = match (d.body.as_ref(), a.body.as_ref()) {
        (SetExpr::Select(d), SetExpr::Select(a)) => (d, a),
        _ => return false,
    };

    // Compare FROM, WHERE, GROUP BY, HAVING by their SQL display representation.
    // Using Display (not Debug) avoids span/location differences in the AST.
    let fmt_from = |v: &Vec<sqlparser::ast::TableWithJoins>| {
        v.iter()
            .map(|t| format!("{t}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let d_from = fmt_from(&d_sel.from);
    let a_from = fmt_from(&a_sel.from);
    let d_where = d_sel
        .selection
        .as_ref()
        .map(|e| format!("{e}"))
        .unwrap_or_default();
    let a_where = a_sel
        .selection
        .as_ref()
        .map(|e| format!("{e}"))
        .unwrap_or_default();
    let d_group = format!("{}", d_sel.group_by);
    let a_group = format!("{}", a_sel.group_by);
    let d_having = d_sel
        .having
        .as_ref()
        .map(|e| format!("{e}"))
        .unwrap_or_default();
    let a_having = a_sel
        .having
        .as_ref()
        .map(|e| format!("{e}"))
        .unwrap_or_default();

    d_from == a_from && d_where == a_where && d_group == a_group && d_having == a_having
}

fn select_list_is_addition_only(d: &sqlparser::ast::Query, a: &sqlparser::ast::Query) -> bool {
    use sqlparser::ast::SetExpr;

    let (d_items, a_items) = match (d.body.as_ref(), a.body.as_ref()) {
        (SetExpr::Select(d), SetExpr::Select(a)) => (&d.projection, &a.projection),
        _ => return false,
    };

    // Every item in `a` must still be present in `d` (no removals or renames).
    // `d` may have additional items (additions are ok for in-place).
    if d_items.len() < a_items.len() {
        return false;
    }

    // Check that the first `a_items.len()` items in d match a (by text).
    let d_strs: Vec<String> = d_items
        .iter()
        .take(a_items.len())
        .map(|i| format!("{}", i))
        .collect();
    let a_strs: Vec<String> = a_items.iter().map(|i| format!("{}", i)).collect();

    d_strs == a_strs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use crate::diff::{DeltaKind, NodeDelta};

    fn make_delta(kind: DeltaKind, desired_query: &str, actual_query: &str) -> NodeDelta {
        let make_spec = |q: &str| StreamTableSpec {
            qualified_name: QualifiedName::new("public", "test"),
            query: q.to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
        };
        NodeDelta {
            qualified_name: QualifiedName::new("public", "test"),
            kind,
            desired: if desired_query.is_empty() {
                None
            } else {
                Some(make_spec(desired_query))
            },
            actual: if actual_query.is_empty() {
                None
            } else {
                Some(make_spec(actual_query))
            },
        }
    }

    #[test]
    fn test_classify_create() {
        let delta = make_delta(DeltaKind::Create, "SELECT 1", "");
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    #[test]
    fn test_classify_schedule_change() {
        let delta = make_delta(DeltaKind::AlterSchedule, "SELECT 1", "SELECT 1");
        assert_eq!(classify_delta(&delta), MigrationClass::Free);
    }

    #[test]
    fn test_classify_query_where_change() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT id FROM t WHERE x > 10",
            "SELECT id FROM t WHERE x > 5",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    #[test]
    fn test_classify_query_column_addition() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a, b, c FROM t GROUP BY a",
            "SELECT a, b FROM t GROUP BY a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }
}
