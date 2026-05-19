use serde::{Deserialize, Serialize};

use crate::dag::{RefreshMode, StreamTableSpec};
use crate::diff::{DeltaKind, NodeDelta};

/// Migration class — how expensive a change is and what technique is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
/// This implements the complete v0.2 decision tree:
///
/// | Change                              | Class      |
/// |-------------------------------------|------------|
/// | Schedule only                       | Free       |
/// | cdc_mode only                       | Free       |
/// | refresh_mode DIFF→FULL              | Free       |
/// | refresh_mode FULL→DIFF              | Rebuild    |
/// | Add passthrough or aggregate column | In-place   |
/// | Drop a column from SELECT           | In-place   |
/// | Rename a column                     | Rebuild    |
/// | Change GROUP BY keys                | Rebuild    |
/// | Change a JOIN condition             | Rebuild    |
/// | Add or remove a JOIN                | Rebuild    |
/// | Change a WHERE predicate            | Rebuild    |
/// | Topology change (split/merge)       | Blue/green |
///
/// Conservative classification: when in doubt, classify as Rebuild.
pub fn classify_delta(delta: &NodeDelta) -> MigrationClass {
    match delta.kind {
        DeltaKind::Unchanged => MigrationClass::Free,

        DeltaKind::Create => MigrationClass::Rebuild,
        DeltaKind::Drop => MigrationClass::Rebuild,

        // Schedule / cdc-mode changes are always free (metadata only).
        DeltaKind::AlterSchedule | DeltaKind::AlterCdcMode => MigrationClass::Free,

        // refresh_mode: DIFF→FULL is free; FULL→DIFF requires rebuild to establish
        // delta-tracking state from scratch.
        DeltaKind::AlterRefreshMode => {
            let from = delta.actual.as_ref().map(|s| &s.refresh_mode);
            let to = delta.desired.as_ref().map(|s| &s.refresh_mode);
            match (from, to) {
                (Some(RefreshMode::Full), Some(RefreshMode::Differential)) => {
                    MigrationClass::Rebuild
                }
                _ => MigrationClass::Free,
            }
        }

        // Multiple metadata changes — classify based on worst-case among members.
        DeltaKind::AlterMetadata => {
            // If refresh_mode changes from FULL→DIFF, that dominates.
            let from = delta.actual.as_ref().map(|s| &s.refresh_mode);
            let to = delta.desired.as_ref().map(|s| &s.refresh_mode);
            if matches!(
                (from, to),
                (Some(RefreshMode::Full), Some(RefreshMode::Differential))
            ) {
                MigrationClass::Rebuild
            } else {
                MigrationClass::Free
            }
        }

        DeltaKind::AlterQuery => {
            // C-10: Use safe fallbacks instead of panicking .unwrap() calls.
            // A missing desired or actual spec means we can't prove safety →
            // conservatively classify as Rebuild.
            match (delta.desired.as_ref(), delta.actual.as_ref()) {
                (Some(desired), Some(actual)) => classify_query_change(desired, actual),
                _ => MigrationClass::Rebuild,
            }
        }
    }
}

/// Classify a query-change delta using the full v0.2 decision tree.
///
/// The rule is intentionally conservative: if we cannot *prove* a change is
/// in-place-safe, we fall back to Rebuild.
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

    let (desired_q, actual_q) = match (&desired_stmts[0], &actual_stmts[0]) {
        (Statement::Query(d), Statement::Query(a)) => (d, a),
        _ => return MigrationClass::Rebuild,
    };

    // Structural parts (FROM clause, WHERE predicate, GROUP BY, HAVING) must be
    // identical for any in-place migration to be possible.
    if !structural_parts_match(desired_q, actual_q) {
        // Structural change → Rebuild.  A topology change (e.g., nodes split/merged
        // at the DAG level) would be Blue/green, but that is handled at the DAG diff
        // level, not here.  Any single-node structural change is Rebuild.
        return MigrationClass::Rebuild;
    }

    // Structural parts are the same.  Now inspect the SELECT list delta.
    let select_delta = classify_select_list_change(desired_q, actual_q);
    match select_delta {
        SelectListDelta::Addition => MigrationClass::InPlace,
        SelectListDelta::Removal => MigrationClass::InPlace,
        SelectListDelta::Rename => MigrationClass::Rebuild,
        SelectListDelta::Unchanged => MigrationClass::Free,
        SelectListDelta::Rebuild => MigrationClass::Rebuild,
    }
}

/// The kind of SELECT-list change detected.
#[derive(Debug, PartialEq)]
enum SelectListDelta {
    /// Only new columns were added (superset).
    Addition,
    /// Only existing columns were removed (strict subset by position).
    Removal,
    /// A column was renamed (same count, same structural position, different alias).
    Rename,
    /// No change.
    Unchanged,
    /// Any other change (type expression change, reorder, etc.) → Rebuild.
    Rebuild,
}

fn classify_select_list_change(
    d: &sqlparser::ast::Query,
    a: &sqlparser::ast::Query,
) -> SelectListDelta {
    use sqlparser::ast::SetExpr;

    let (d_items, a_items) = match (d.body.as_ref(), a.body.as_ref()) {
        (SetExpr::Select(d), SetExpr::Select(a)) => (&d.projection, &a.projection),
        _ => return SelectListDelta::Rebuild,
    };

    let d_strs: Vec<String> = d_items.iter().map(|i| format!("{i}")).collect();
    let a_strs: Vec<String> = a_items.iter().map(|i| format!("{i}")).collect();

    if d_strs == a_strs {
        return SelectListDelta::Unchanged;
    }

    // Case 1: Addition only — desired is a superset of actual by prefix.
    // All actual items appear as the first `a_strs.len()` items of desired.
    if d_strs.len() > a_strs.len() {
        let prefix_match = d_strs.iter().take(a_strs.len()).eq(a_strs.iter());
        if prefix_match {
            return SelectListDelta::Addition;
        }
    }

    // Case 2: Removal only — desired is a strict prefix of actual.
    // Removing trailing columns only is in-place; removing from the middle shifts ordinals → Rebuild.
    if d_strs.len() < a_strs.len() {
        let is_prefix = a_strs.starts_with(d_strs.as_slice());
        if is_prefix {
            return SelectListDelta::Removal;
        }
    }

    // Case 3: Same count, same expressions but different aliases → Rename.
    if d_strs.len() == a_strs.len() {
        // Extract the base expression (before AS alias) for each item.
        let d_exprs = extract_base_expressions(d_items);
        let a_exprs = extract_base_expressions(a_items);
        if d_exprs == a_exprs && d_strs != a_strs {
            return SelectListDelta::Rename;
        }
    }

    SelectListDelta::Rebuild
}

/// Extract the base expression from a SELECT item, stripping any AS alias.
fn extract_base_expressions(items: &[sqlparser::ast::SelectItem]) -> Vec<String> {
    use sqlparser::ast::SelectItem;

    items
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { expr, .. } => format!("{expr}"),
            other => format!("{other}"),
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use crate::diff::{DeltaKind, NodeDelta};

    fn make_delta(kind: DeltaKind, desired_query: &str, actual_query: &str) -> NodeDelta {
        make_delta_with_mode(
            kind,
            desired_query,
            actual_query,
            RefreshMode::Differential,
            RefreshMode::Differential,
        )
    }

    fn make_delta_with_mode(
        kind: DeltaKind,
        desired_query: &str,
        actual_query: &str,
        desired_mode: RefreshMode,
        actual_mode: RefreshMode,
    ) -> NodeDelta {
        let make_spec = |q: &str, mode: RefreshMode| StreamTableSpec {
            qualified_name: QualifiedName::new("public", "test"),
            query: q.to_string(),
            refresh_mode: mode,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
            cypher_source: None,
        };
        NodeDelta {
            qualified_name: QualifiedName::new("public", "test"),
            kind,
            desired: if desired_query.is_empty() {
                None
            } else {
                Some(make_spec(desired_query, desired_mode))
            },
            actual: if actual_query.is_empty() {
                None
            } else {
                Some(make_spec(actual_query, actual_mode))
            },
        }
    }

    // ── Create / Drop ─────────────────────────────────────────────────────────

    #[test]
    fn test_classify_create() {
        let delta = make_delta(DeltaKind::Create, "SELECT 1", "");
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    #[test]
    fn test_classify_drop() {
        let delta = make_delta(DeltaKind::Drop, "", "SELECT 1");
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    // ── Metadata-only changes ─────────────────────────────────────────────────

    #[test]
    fn test_classify_schedule_change() {
        let delta = make_delta(DeltaKind::AlterSchedule, "SELECT 1", "SELECT 1");
        assert_eq!(classify_delta(&delta), MigrationClass::Free);
    }

    #[test]
    fn test_classify_cdc_mode_change() {
        let delta = make_delta(DeltaKind::AlterCdcMode, "SELECT 1", "SELECT 1");
        assert_eq!(classify_delta(&delta), MigrationClass::Free);
    }

    /// refresh_mode DIFFERENTIAL → FULL: Free (just change the metadata).
    #[test]
    fn test_classify_refresh_mode_diff_to_full_is_free() {
        let delta = make_delta_with_mode(
            DeltaKind::AlterRefreshMode,
            "SELECT 1",
            "SELECT 1",
            RefreshMode::Full,
            RefreshMode::Differential,
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Free);
    }

    /// refresh_mode FULL → DIFFERENTIAL: Rebuild (must establish delta-tracking state).
    #[test]
    fn test_classify_refresh_mode_full_to_diff_is_rebuild() {
        let delta = make_delta_with_mode(
            DeltaKind::AlterRefreshMode,
            "SELECT 1",
            "SELECT 1",
            RefreshMode::Differential,
            RefreshMode::Full,
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    // ── Query structural changes → Rebuild ────────────────────────────────────

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
    fn test_classify_query_group_by_change() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a, SUM(b) FROM t GROUP BY a, c",
            "SELECT a, SUM(b) FROM t GROUP BY a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    #[test]
    fn test_classify_query_join_added() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a.id FROM t a JOIN u ON a.id = u.id",
            "SELECT a.id FROM t a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    #[test]
    fn test_classify_query_join_removed() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a.id FROM t a",
            "SELECT a.id FROM t a JOIN u ON a.id = u.id",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }

    // ── Column addition → In-place ────────────────────────────────────────────

    #[test]
    fn test_classify_query_column_addition() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a, b, c FROM t GROUP BY a",
            "SELECT a, b FROM t GROUP BY a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }

    #[test]
    fn test_classify_query_aggregate_column_addition() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt FROM orders GROUP BY customer_id",
            "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }

    #[test]
    fn test_classify_query_passthrough_column_addition() {
        // Passthrough column — not in GROUP BY, no aggregate.
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT id, name, email FROM users",
            "SELECT id, name FROM users",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }

    // ── Column removal → In-place ─────────────────────────────────────────────

    #[test]
    fn test_classify_query_column_removal() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a FROM t GROUP BY a",
            "SELECT a, b FROM t GROUP BY a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }

    #[test]
    fn test_classify_query_drop_aggregate_column() {
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
            "SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt FROM orders GROUP BY customer_id",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
    }

    // ── Column rename → Rebuild ───────────────────────────────────────────────

    #[test]
    fn test_classify_query_column_rename() {
        // Same expression, different alias → Rename → Rebuild.
        let delta = make_delta(
            DeltaKind::AlterQuery,
            "SELECT a, b AS new_name FROM t GROUP BY a",
            "SELECT a, b AS old_name FROM t GROUP BY a",
        );
        assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
    }
}
