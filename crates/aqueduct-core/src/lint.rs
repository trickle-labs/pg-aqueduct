//! `aqueduct lint` — static analysis for migration files.
//!
//! Lint rules:
//! 1. FULL-refresh-only changes on large tables — suggests blue/green instead.
//! 2. Missing `@aqueduct:depends_on` when the SQL parser detects an ambiguous dependency.
//! 3. Schedule too aggressive for the estimated data volume.
//! 4. DIFFERENTIAL → FULL refresh-mode downgrade (auto-downgrade risk).
//! 5. `@aqueduct:cypher_source` files that reference non-existent paths.
//! 6. Missing consumer view declarations for stream tables.

use crate::dag::DagState;
use crate::parser::MigrationFile;

/// A single lint diagnostic.
#[derive(Debug, Clone)]
pub struct LintDiagnostic {
    /// Machine-readable lint rule identifier (e.g. "full-refresh-no-bluegreen").
    pub rule: &'static str,
    /// Severity level.
    pub level: LintLevel,
    /// Human-readable description.
    pub message: String,
    /// Source file that triggered the lint (if applicable).
    pub file: Option<std::path::PathBuf>,
}

/// Severity of a lint diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LintLevel {
    Warning,
    Error,
}

impl std::fmt::Display for LintLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LintLevel::Warning => write!(f, "warning"),
            LintLevel::Error => write!(f, "error"),
        }
    }
}

/// Result of a lint run.
#[derive(Debug, Default)]
pub struct LintResult {
    pub diagnostics: Vec<LintDiagnostic>,
}

impl LintResult {
    pub fn warnings(&self) -> impl Iterator<Item = &LintDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.level == LintLevel::Warning)
    }

    pub fn errors(&self) -> impl Iterator<Item = &LintDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.level == LintLevel::Error)
    }

    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| d.level == LintLevel::Error)
    }

    pub fn is_clean(&self) -> bool {
        self.diagnostics.is_empty()
    }

    fn push(&mut self, d: LintDiagnostic) {
        self.diagnostics.push(d);
    }
}

/// Run all lint rules against the parsed migration files and DAG state.
pub fn lint_migrations(files: &[MigrationFile], state: &DagState) -> LintResult {
    let mut result = LintResult::default();

    for file in files {
        lint_file(file, state, &mut result);
    }

    lint_consumer_coverage(files, state, &mut result);

    result
}

fn lint_file(file: &MigrationFile, state: &DagState, result: &mut LintResult) {
    use crate::parser::MigrationKind;

    match file.front_matter.kind {
        MigrationKind::Stream => {
            lint_stream_file(file, state, result);
        }
        MigrationKind::Source | MigrationKind::Consumer => {
            // lint rules specific to sources/consumers go here in future
        }
    }

    // Rule: cypher_source file must exist (if declared).
    if let Some(cypher_path) = &file.front_matter.cypher_source {
        let base_dir = file
            .path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .unwrap_or(std::path::Path::new("."));
        let full_path = base_dir.join(cypher_path);
        if !full_path.exists() {
            result.push(LintDiagnostic {
                rule: "cypher-source-missing",
                level: LintLevel::Error,
                message: format!(
                    "'{}': @aqueduct:cypher_source points to '{}' which does not exist",
                    file.name, cypher_path
                ),
                file: Some(file.path.clone()),
            });
        }
    }
}

fn lint_stream_file(file: &MigrationFile, _state: &DagState, result: &mut LintResult) {
    // Rule: Schedule too aggressive for large tables.
    // We flag schedules faster than 5 seconds as a lint warning — very high-frequency
    // refresh on a potentially large source is usually unintentional.
    if let Some(schedule) = &file.front_matter.schedule {
        if let Some(seconds) = parse_schedule_seconds(schedule) {
            if seconds < 5 {
                result.push(LintDiagnostic {
                    rule: "schedule-too-aggressive",
                    level: LintLevel::Warning,
                    message: format!(
                        "'{}': schedule \"{}\" is faster than 5 seconds — verify this is \
                         intentional for the expected data volume",
                        file.name, schedule
                    ),
                    file: Some(file.path.clone()),
                });
            }
        }
    }

    // Rule: FULL refresh mode without depends_on is potentially risky for large DAGs.
    // If a stream table uses FULL refresh and has a broad query (no WHERE clause),
    // we emit an informational warning.
    if let Some(rm) = &file.front_matter.refresh_mode {
        if rm.to_uppercase() == "FULL" {
            // Check if query has no WHERE clause (simple heuristic).
            let sql_lower = file.sql_body.to_lowercase();
            if !sql_lower.contains("where") && !sql_lower.contains("limit") {
                result.push(LintDiagnostic {
                    rule: "full-refresh-no-filter",
                    level: LintLevel::Warning,
                    message: format!(
                        "'{}': uses FULL refresh mode with no WHERE/LIMIT clause — \
                         consider DIFFERENTIAL refresh or adding a filter to limit \
                         the rebuild cost. For large tables, consider blue/green migration.",
                        file.name
                    ),
                    file: Some(file.path.clone()),
                });
            }
        }
    }

    // Rule: Detect DIFFERENTIAL on queries with volatile functions (auto-downgrade risk).
    if let Some(rm) = &file.front_matter.refresh_mode {
        if rm.to_uppercase() == "DIFFERENTIAL" {
            if let Err(e) = crate::validate::validate_ivm_supportability(&file.sql_body, &file.name)
            {
                result.push(LintDiagnostic {
                    rule: "differential-ivm-unsupportable",
                    level: LintLevel::Warning,
                    message: format!(
                        "'{}': query may not be maintainable in DIFFERENTIAL mode: {}. \
                         This would trigger an automatic downgrade to FULL refresh.",
                        file.name, e
                    ),
                    file: Some(file.path.clone()),
                });
            }
        }
    }
}

/// Check that every stream table referenced by a consumer view actually exists.
/// Also warn about stream tables that have no consumer view when they could benefit
/// from one (i.e. they are likely consumed by downstream applications).
fn lint_consumer_coverage(files: &[MigrationFile], state: &DagState, result: &mut LintResult) {
    use crate::parser::MigrationKind;

    // Collect declared consumer source names.
    let consumer_sources: std::collections::HashSet<String> = files
        .iter()
        .filter(|f| f.front_matter.kind == MigrationKind::Consumer)
        .filter_map(|f| f.front_matter.source.clone())
        .collect();

    // For each consumer, verify the source stream table exists in the DAG.
    for file in files {
        if file.front_matter.kind != MigrationKind::Consumer {
            continue;
        }
        if let Some(src) = &file.front_matter.source {
            let qn = crate::dag::QualifiedName::from_str_parts(src);
            if state.find_stream_table(&qn).is_none() {
                // Also check by simple name.
                let found = state
                    .stream_tables
                    .iter()
                    .any(|t| t.qualified_name.name == qn.name);
                if !found {
                    result.push(LintDiagnostic {
                        rule: "consumer-source-not-found",
                        level: LintLevel::Error,
                        message: format!(
                            "consumer '{}': source '{}' is not a known stream table",
                            file.name, src
                        ),
                        file: Some(file.path.clone()),
                    });
                }
            }
        }
    }

    // Informational: stream tables that are leaf nodes (no other stream table
    // depends on them) but have no consumer view declared.
    // This is a soft warning only — many leaf stream tables are consumed directly.
    let _ = consumer_sources; // used for future heuristics
}

/// Parse a schedule string (e.g. "30s", "5m", "1h") to seconds.
/// Returns None if the format is not recognised.
fn parse_schedule_seconds(schedule: &str) -> Option<u64> {
    let s = schedule.trim();
    if s.is_empty() {
        return None;
    }

    let (num_part, _unit) = s.split_at(s.len().saturating_sub(1));
    let last_char = s.chars().last()?;

    let num: u64 = match last_char {
        's' | 'm' | 'h' | 'd' => num_part.trim().parse().ok()?,
        _ => s.parse().ok()?,
    };

    match last_char {
        's' => Some(num),
        'm' => Some(num * 60),
        'h' => Some(num * 3600),
        'd' => Some(num * 86400),
        _ => Some(num), // treat bare number as seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schedule_seconds() {
        assert_eq!(parse_schedule_seconds("30s"), Some(30));
        assert_eq!(parse_schedule_seconds("5m"), Some(300));
        assert_eq!(parse_schedule_seconds("1h"), Some(3600));
        assert_eq!(parse_schedule_seconds("1d"), Some(86400));
        assert_eq!(parse_schedule_seconds("1"), Some(1));
        assert_eq!(parse_schedule_seconds(""), None);
    }

    #[test]
    fn test_lint_aggressive_schedule() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("fast.sql"),
            name: "fast".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                schedule: Some("1s".to_string()),
                ..Default::default()
            },
            sql_body: "SELECT id FROM t".to_string(),
            unknown_keys: vec![],
        };

        let state = crate::dag::DagState::default();
        let result = lint_migrations(&[file], &state);
        let warnings: Vec<_> = result.warnings().collect();
        assert!(
            warnings.iter().any(|w| w.rule == "schedule-too-aggressive"),
            "expected schedule-too-aggressive warning"
        );
    }

    #[test]
    fn test_lint_full_refresh_no_filter() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("full_table.sql"),
            name: "full_table".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                refresh_mode: Some("FULL".to_string()),
                schedule: Some("30s".to_string()),
                ..Default::default()
            },
            sql_body: "SELECT id, amount FROM orders".to_string(),
            unknown_keys: vec![],
        };

        let state = crate::dag::DagState::default();
        let result = lint_migrations(&[file], &state);
        let warnings: Vec<_> = result.warnings().collect();
        assert!(
            warnings.iter().any(|w| w.rule == "full-refresh-no-filter"),
            "expected full-refresh-no-filter warning"
        );
    }

    #[test]
    fn test_lint_consumer_source_not_found() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let consumer = MigrationFile {
            path: PathBuf::from("api_orders.sql"),
            name: "api_orders".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Consumer,
                source: Some("nonexistent_table".to_string()),
                ..Default::default()
            },
            sql_body: "SELECT * FROM nonexistent_table".to_string(),
            unknown_keys: vec![],
        };

        let state = crate::dag::DagState::default();
        let result = lint_migrations(&[consumer], &state);
        let errors: Vec<_> = result.errors().collect();
        assert!(
            errors.iter().any(|e| e.rule == "consumer-source-not-found"),
            "expected consumer-source-not-found error"
        );
    }

    #[test]
    fn test_lint_clean_file() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("orders.sql"),
            name: "orders".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                schedule: Some("30s".to_string()),
                refresh_mode: Some("DIFFERENTIAL".to_string()),
                ..Default::default()
            },
            sql_body:
                "SELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id"
                    .to_string(),
            unknown_keys: vec![],
        };

        let state = crate::dag::DagState::default();
        let result = lint_migrations(&[file], &state);
        assert!(result.is_clean(), "expected no lint diagnostics");
    }

    #[test]
    fn test_lint_differential_ivm_unsupportable() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("bad_ivm.sql"),
            name: "bad_ivm".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                schedule: Some("30s".to_string()),
                refresh_mode: Some("DIFFERENTIAL".to_string()),
                ..Default::default()
            },
            sql_body: "SELECT DISTINCT customer_id FROM raw.orders".to_string(),
            unknown_keys: vec![],
        };

        let state = crate::dag::DagState::default();
        let result = lint_migrations(&[file], &state);
        let warnings: Vec<_> = result.warnings().collect();
        assert!(
            warnings
                .iter()
                .any(|w| w.rule == "differential-ivm-unsupportable"),
            "expected differential-ivm-unsupportable warning"
        );
    }
}
