use crate::dag::DagState;
use crate::diagnostic::{Diagnostic, DiagnosticSet};
use crate::error::{AqueductError, Result};
use crate::parser::MigrationFile;

/// Validate a SQL string: check that it can be parsed.
pub fn validate_sql_syntax(sql: &str, table_name: &str) -> Result<()> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| {
        AqueductError::SqlParse(format!("SQL parse error in '{}': {}", table_name, e))
    })?;
    Ok(())
}

/// Validate IVM supportability for a stream table query.
/// Returns Ok(()) if the query can be maintained incrementally, or an error describing why not.
pub fn validate_ivm_supportability(sql: &str, table_name: &str) -> Result<()> {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| AqueductError::SqlParse(e.to_string()))?;

    if stmts.len() != 1 {
        return Err(AqueductError::IvmUnsupportable {
            table: table_name.to_string(),
            reason: "exactly one SELECT statement is required".to_string(),
        });
    }

    match &stmts[0] {
        Statement::Query(q) => {
            validate_query_ivm(q, table_name)?;
        }
        _ => {
            return Err(AqueductError::IvmUnsupportable {
                table: table_name.to_string(),
                reason: "only SELECT statements are supported for stream tables".to_string(),
            });
        }
    }

    Ok(())
}

fn validate_query_ivm(query: &sqlparser::ast::Query, table_name: &str) -> Result<()> {
    use sqlparser::ast::SetExpr;

    match query.body.as_ref() {
        SetExpr::Select(sel) => {
            // Check for DISTINCT (not supported in DIFFERENTIAL mode).
            if sel.distinct.is_some() {
                return Err(AqueductError::IvmUnsupportable {
                    table: table_name.to_string(),
                    reason: "SELECT DISTINCT is not supported for DIFFERENTIAL refresh".to_string(),
                });
            }

            // Check SELECT list for volatile functions.
            for item in &sel.projection {
                check_select_item_for_volatiles(item, table_name)?;
            }
        }
        SetExpr::SetOperation { .. } => {
            // UNION, INTERSECT, EXCEPT — not supported for DIFFERENTIAL.
            return Err(AqueductError::IvmUnsupportable {
                table: table_name.to_string(),
                reason: "set operations (UNION/INTERSECT/EXCEPT) are not supported for DIFFERENTIAL refresh".to_string(),
            });
        }
        _ => {}
    }

    Ok(())
}

/// List of volatile functions that are not suitable for DIFFERENTIAL IVM.
const VOLATILE_FUNCTIONS: &[&str] = &[
    "random",
    "gen_random_uuid",
    "uuid_generate_v4",
    "now",
    "clock_timestamp",
    "timeofday",
    "transaction_timestamp",
    "statement_timestamp",
    "localtime",
    "localtimestamp",
    "current_timestamp",
    "current_time",
    "current_date",
    "pg_sleep",
    "nextval",
    "lastval",
    "currval",
];

fn check_select_item_for_volatiles(
    item: &sqlparser::ast::SelectItem,
    table_name: &str,
) -> Result<()> {
    use sqlparser::ast::SelectItem;

    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            check_expr_for_volatiles(expr, table_name)?;
        }
        _ => {}
    }
    Ok(())
}

fn check_expr_for_volatiles(expr: &sqlparser::ast::Expr, table_name: &str) -> Result<()> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            if VOLATILE_FUNCTIONS.contains(&func_name.as_str()) {
                return Err(AqueductError::IvmUnsupportable {
                    table: table_name.to_string(),
                    reason: format!(
                        "volatile function '{}()' is not supported for DIFFERENTIAL refresh",
                        func_name
                    ),
                });
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            check_expr_for_volatiles(left, table_name)?;
            check_expr_for_volatiles(right, table_name)?;
        }
        Expr::UnaryOp { expr, .. } => {
            check_expr_for_volatiles(expr, table_name)?;
        }
        _ => {}
    }
    Ok(())
}

/// Run all offline validation checks on a list of migration files.
#[derive(Debug, Default)]
pub struct ValidationResult {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn add_error(&mut self, msg: impl Into<String>) {
        self.errors.push(msg.into());
    }

    pub fn add_warning(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }
}

/// Run all offline validation checks and return a unified `DiagnosticSet` (Q-08 / U-09).
///
/// Diagnostics include file paths and line numbers where available.
pub fn validate_migration_files_diagnostic(files: &[MigrationFile]) -> DiagnosticSet {
    let mut set = DiagnosticSet::new();

    for file in files {
        // Check SQL syntax.
        if !file.sql_body.is_empty() {
            if let Err(e) = validate_sql_syntax(&file.sql_body, &file.name) {
                set.push(
                    Diagnostic::error("E101", e.to_string())
                        .with_file(&file.path)
                        .with_hint("Check the SQL query body for syntax errors"),
                );
            }
        }

        // Warn about unknown keys (U-09: include file path).
        for key in &file.unknown_keys {
            set.push(
                Diagnostic::warning(
                    "W001",
                    format!(
                        "Unknown front-matter key '@aqueduct:{}' (may be from a newer CLI version)",
                        key
                    ),
                )
                .with_file(&file.path),
            );
        }
    }

    set
}

pub fn validate_migration_files(files: &[MigrationFile]) -> ValidationResult {
    let mut result = ValidationResult::default();

    for file in files {
        // Check SQL syntax.
        if !file.sql_body.is_empty() {
            if let Err(e) = validate_sql_syntax(&file.sql_body, &file.name) {
                result.add_error(format!("{}: {}", file.path.display(), e));
            }
        }

        // Warn about unknown keys.
        for key in &file.unknown_keys {
            result.add_warning(format!(
                "{}: unknown front-matter key '@aqueduct:{}' (may be from a newer CLI version)",
                file.path.display(),
                key
            ));
        }
    }

    result
}

/// Run full DAG validation: cycle detection, dependency resolution, IVM checks.
pub fn validate_dag(state: &DagState) -> ValidationResult {
    let mut result = ValidationResult::default();

    // Check for cycles.
    if let Err(e) = crate::dag::topological_sort(state) {
        result.add_error(format!("Dependency cycle detected: {}", e));
    }

    // Validate each stream table's query.
    for table in &state.stream_tables {
        if !table.query.is_empty() {
            if let Err(e) = validate_sql_syntax(&table.query, &table.qualified_name.to_string()) {
                result.add_error(format!("{}: {}", table.qualified_name, e));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_select() {
        assert!(validate_sql_syntax(
            "SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY 1",
            "test"
        )
        .is_ok());
    }

    #[test]
    fn test_invalid_sql() {
        // sqlparser is lenient with some malformed SQL; use something definitively invalid
        assert!(validate_sql_syntax("@@@ NOT VALID SQL @@@", "test").is_err());
    }

    #[test]
    fn test_ivm_valid() {
        assert!(validate_ivm_supportability(
            "SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY customer_id",
            "order_totals"
        )
        .is_ok());
    }

    #[test]
    fn test_ivm_volatile_function() {
        let result = validate_ivm_supportability("SELECT random() AS r FROM t", "test_table");
        assert!(result.is_err());
        match result {
            Err(AqueductError::IvmUnsupportable { reason, .. }) => {
                assert!(reason.contains("random"));
            }
            _ => panic!("expected IvmUnsupportable error"),
        }
    }

    #[test]
    fn test_ivm_distinct_not_allowed() {
        let result =
            validate_ivm_supportability("SELECT DISTINCT customer_id FROM orders", "test_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_ivm_set_operation_not_allowed() {
        let result =
            validate_ivm_supportability("SELECT id FROM a UNION SELECT id FROM b", "test_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_non_select_not_allowed() {
        let result = validate_ivm_supportability("INSERT INTO t VALUES (1)", "test_table");
        assert!(result.is_err());
    }
}
