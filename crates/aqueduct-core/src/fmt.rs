//! `aqueduct fmt` — canonicalise migration file SQL bodies and front-matter directives.
//!
//! Formatting rules:
//! - Front-matter directives are emitted in canonical key order.
//! - SQL keywords are upper-cased.
//! - Trailing whitespace and redundant semicolons are removed from the SQL body.
//! - A single blank line separates the front-matter block from the SQL body.

use crate::parser::MigrationFile;

/// Keys that the formatter emits explicitly in the front-matter block.
///
/// Used to skip already-emitted keys when iterating `unknown_keys` at the end
/// of rendering — this constant does **not** imply or enforce canonical ordering.
const KNOWN_DIRECTIVE_KEYS: &[&str] = &[
    "kind",
    "owned",
    "schema",
    "depends_on",
    "schedule",
    "refresh_mode",
    "cdc_mode",
    "cypher_source",
    "source",
    "expose_as",
];

/// SQL keywords to be upper-cased during formatting.
const SQL_KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "having",
    "join",
    "inner",
    "left",
    "right",
    "outer",
    "full",
    "cross",
    "on",
    "as",
    "and",
    "or",
    "not",
    "in",
    "is",
    "null",
    "like",
    "between",
    "case",
    "when",
    "then",
    "else",
    "end",
    "distinct",
    "all",
    "union",
    "intersect",
    "except",
    "insert",
    "update",
    "delete",
    "into",
    "values",
    "set",
    "create",
    "alter",
    "drop",
    "table",
    "view",
    "index",
    "unique",
    "primary",
    "key",
    "foreign",
    "references",
    "default",
    "constraint",
    "with",
    "sum",
    "count",
    "min",
    "max",
    "avg",
    "coalesce",
    "nullif",
    "cast",
    "over",
    "partition",
    "window",
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "limit",
    "offset",
    "true",
    "false",
    "asc",
    "desc",
    "nulls",
    "first",
    "last",
];

/// Format a migration file's content according to canonical style.
///
/// Returns the formatted content as a string, or `None` if the content is already
/// canonical (no changes needed).  Returns an `Err` if the file cannot be read.
pub fn format_migration(file: &MigrationFile) -> crate::Result<Option<String>> {
    let formatted = render_migration(file);
    let original = read_original_content(file)?;

    if formatted == original {
        Ok(None)
    } else {
        Ok(Some(formatted))
    }
}

/// Read the original content of a migration file from disk.
///
/// Returns an error (L2) rather than silently using an empty string when the
/// file cannot be read. This ensures `aqueduct fmt` always surfaces IO
/// problems instead of producing misleading "everything changed" output.
fn read_original_content(file: &MigrationFile) -> crate::Result<String> {
    std::fs::read_to_string(&file.path).map_err(|e| {
        crate::AqueductError::Config(format!(
            "cannot read migration file '{}': {}",
            file.path.display(),
            e
        ))
    })
}

/// Render a MigrationFile to its canonical string representation.
pub fn render_migration(file: &MigrationFile) -> String {
    let mut lines: Vec<String> = Vec::new();

    // Emit front-matter in canonical order.
    let fm = &file.front_matter;

    // kind — only emitted when not the default (stream).
    use crate::parser::MigrationKind;
    match fm.kind {
        MigrationKind::Source => {
            lines.push("-- @aqueduct:kind = \"source\"".to_string());
        }
        MigrationKind::Consumer => {
            lines.push("-- @aqueduct:kind = \"consumer\"".to_string());
        }
        MigrationKind::Stream => {
            // stream is the default; only emit if other stream-specific keys are absent
            // (this makes files slightly shorter for the common case).
        }
    }

    // owned — only emit when explicitly set.
    if let Some(owned) = fm.owned {
        lines.push(format!("-- @aqueduct:owned = {}", owned));
    }

    // schema — only emit when not "public".
    if let Some(schema) = &fm.schema {
        if schema != "public" {
            lines.push(format!("-- @aqueduct:schema = \"{}\"", schema));
        }
    }

    // depends_on — emit as an array when non-empty.
    if !fm.depends_on.is_empty() {
        let deps: Vec<String> = fm.depends_on.iter().map(|d| format!("\"{}\"", d)).collect();
        lines.push(format!("-- @aqueduct:depends_on = [{}]", deps.join(", ")));
    }

    // schedule
    if let Some(schedule) = &fm.schedule {
        lines.push(format!("-- @aqueduct:schedule = \"{}\"", schedule));
    }

    // refresh_mode
    if let Some(rm) = &fm.refresh_mode {
        lines.push(format!(
            "-- @aqueduct:refresh_mode = \"{}\"",
            rm.to_uppercase()
        ));
    }

    // cdc_mode
    if let Some(cdc) = &fm.cdc_mode {
        lines.push(format!("-- @aqueduct:cdc_mode = \"{}\"", cdc));
    }

    // cypher_source
    if let Some(cs) = &fm.cypher_source {
        lines.push(format!("-- @aqueduct:cypher_source = \"{}\"", cs));
    }

    // source (consumer views)
    if let Some(src) = &fm.source {
        lines.push(format!("-- @aqueduct:source = \"{}\"", src));
    }

    // expose_as (consumer views)
    if let Some(ea) = &fm.expose_as {
        lines.push(format!("-- @aqueduct:expose_as = \"{}\"", ea));
    }

    // Any unknown/forward-compat keys in stable order.
    let mut extra_keys: Vec<String> = fm.unknown_keys.keys().cloned().collect();
    extra_keys.sort();
    for key in &extra_keys {
        // Skip keys already emitted above.
        if KNOWN_DIRECTIVE_KEYS.contains(&key.as_str()) {
            continue;
        }
        if let Some(val) = fm.unknown_keys.get(key) {
            lines.push(format!("-- @aqueduct:{} = {}", key, val));
        }
    }

    // Blank line between front-matter and SQL body.
    if !lines.is_empty() && !file.sql_body.is_empty() {
        lines.push(String::new());
    }

    // SQL body: uppercase keywords, normalise whitespace.
    if !file.sql_body.is_empty() {
        let formatted_sql = format_sql_keywords(&file.sql_body);
        lines.push(formatted_sql);
    }

    let mut result = lines.join("\n");
    // Ensure the file ends with a single newline.
    if !result.ends_with('\n') {
        result.push('\n');
    }
    result
}

/// Normalise SQL by upper-casing keywords while preserving identifier case.
///
/// This is a best-effort word-boundary replacement. It does not reformat
/// indentation or line breaks — those are preserved as-is.
pub fn format_sql_keywords(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' || ch == '"' {
            // String literal or quoted identifier: pass through verbatim.
            let quote = ch;
            result.push(ch);
            for inner in chars.by_ref() {
                result.push(inner);
                if inner == quote {
                    break;
                }
            }
        } else if ch == '-' && chars.peek() == Some(&'-') {
            // Line comment: pass through verbatim to end of line.
            result.push(ch);
            for c in chars.by_ref() {
                result.push(c);
                if c == '\n' {
                    break;
                }
            }
        } else if ch == '/' && chars.peek() == Some(&'*') {
            // Block comment: pass through verbatim.
            result.push(ch);
            while let Some(c) = chars.next() {
                result.push(c);
                if c == '*' && chars.peek() == Some(&'/') {
                    result.push(chars.next().unwrap());
                    break;
                }
            }
        } else if ch.is_alphabetic() || ch == '_' {
            // Collect a word token.
            let mut word = String::new();
            word.push(ch);
            while let Some(&next) = chars.peek() {
                if next.is_alphanumeric() || next == '_' {
                    word.push(chars.next().unwrap());
                } else {
                    break;
                }
            }
            // Check if this is a SQL keyword (case-insensitive).
            if SQL_KEYWORDS.contains(&word.to_lowercase().as_str()) {
                result.push_str(&word.to_uppercase());
            } else {
                result.push_str(&word);
            }
        } else {
            result.push(ch);
        }
    }

    // Trim trailing whitespace from each line.
    result
        .lines()
        .map(|l| l.trim_end())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Result of formatting one or more migration files.
#[derive(Debug, Default)]
pub struct FmtResult {
    /// Files that were changed.
    pub changed: Vec<std::path::PathBuf>,
    /// Files that were already canonical.
    pub unchanged: Vec<std::path::PathBuf>,
    /// Files that could not be formatted (error message).
    pub errors: Vec<(std::path::PathBuf, String)>,
}

impl FmtResult {
    pub fn had_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Format all migration files in a project directory.
///
/// When `check_only` is true, files are not written — only the diff status
/// is reported.
pub fn format_migrations(files: &[MigrationFile], check_only: bool) -> FmtResult {
    let mut result = FmtResult::default();

    for file in files {
        match format_migration(file) {
            Err(e) => {
                // L2: surface read errors instead of silently skipping.
                result
                    .errors
                    .push((file.path.clone(), format!("read error: {}", e)));
            }
            Ok(None) => {
                result.unchanged.push(file.path.clone());
            }
            Ok(Some(formatted)) => {
                if !check_only {
                    if let Err(e) = std::fs::write(&file.path, &formatted) {
                        result
                            .errors
                            .push((file.path.clone(), format!("write error: {}", e)));
                        continue;
                    }
                }
                result.changed.push(file.path.clone());
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_sql_keywords_uppercases() {
        let sql = "select customer_id, sum(amount) as total from raw.orders group by customer_id";
        let result = format_sql_keywords(sql);
        assert!(result.contains("SELECT"));
        assert!(result.contains("SUM"));
        assert!(result.contains("AS"));
        assert!(result.contains("FROM"));
        assert!(result.contains("GROUP"));
        assert!(result.contains("BY"));
        // Identifiers should be preserved.
        assert!(result.contains("customer_id"));
        assert!(result.contains("raw.orders"));
    }

    #[test]
    fn test_format_sql_keywords_preserves_strings() {
        let sql = "select 'hello world' as greeting from t";
        let result = format_sql_keywords(sql);
        // The string literal should be preserved as-is.
        assert!(result.contains("'hello world'"));
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn test_format_sql_keywords_already_upper() {
        let sql = "SELECT id FROM t WHERE id > 1";
        let result = format_sql_keywords(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn test_format_sql_keywords_preserves_comments() {
        let sql = "-- this is a comment\nselect 1";
        let result = format_sql_keywords(sql);
        assert!(result.contains("-- this is a comment"));
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn test_render_migration_canonical_order() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("test.sql"),
            name: "order_totals".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                schedule: Some("30s".to_string()),
                refresh_mode: Some("DIFFERENTIAL".to_string()),
                ..Default::default()
            },
            sql_body: "select customer_id, sum(amount) as total from orders group by customer_id"
                .to_string(),
            unknown_keys: vec![],
        };

        let rendered = render_migration(&file);
        // schedule should appear before refresh_mode.
        let sched_pos = rendered.find("schedule").unwrap();
        let rm_pos = rendered.find("refresh_mode").unwrap();
        assert!(sched_pos < rm_pos);
        // SQL should be upper-cased.
        assert!(rendered.contains("SELECT"));
        assert!(rendered.contains("FROM"));
        assert!(rendered.contains("GROUP BY"));
    }

    #[test]
    fn test_render_migration_consumer() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};
        use std::path::PathBuf;

        let file = MigrationFile {
            path: PathBuf::from("api_orders.sql"),
            name: "api_orders".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Consumer,
                source: Some("order_totals".to_string()),
                expose_as: Some("public.api_orders".to_string()),
                ..Default::default()
            },
            sql_body: "select customer_id, total from order_totals where total > 0".to_string(),
            unknown_keys: vec![],
        };

        let rendered = render_migration(&file);
        assert!(rendered.contains("-- @aqueduct:kind = \"consumer\""));
        assert!(rendered.contains("-- @aqueduct:source = \"order_totals\""));
        assert!(rendered.contains("-- @aqueduct:expose_as = \"public.api_orders\""));
        assert!(rendered.contains("SELECT"));
    }

    #[test]
    fn test_format_migrations_check_only() {
        use crate::parser::{FrontMatter, MigrationFile, MigrationKind};

        // A file that already has non-canonical SQL (lowercase keywords).
        // Since check_only=true, no files should be written.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let content = "-- @aqueduct:schedule = \"30s\"\n\nselect 1\n";
        std::fs::write(tmp.path(), content).unwrap();

        let file = MigrationFile {
            path: tmp.path().to_path_buf(),
            name: "test".to_string(),
            front_matter: FrontMatter {
                kind: MigrationKind::Stream,
                schedule: Some("30s".to_string()),
                ..Default::default()
            },
            sql_body: "select 1".to_string(),
            unknown_keys: vec![],
        };

        let result = format_migrations(&[file], true);
        // In check-only mode, the file should be reported as changed but not written.
        assert_eq!(result.changed.len(), 1);
        // Original content should be unchanged on disk.
        let on_disk = std::fs::read_to_string(tmp.path()).unwrap();
        assert_eq!(on_disk, content);
    }
}
