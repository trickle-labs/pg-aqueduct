use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::substitute_vars;
use crate::error::{AqueductError, Result};

/// The kind of a migration file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationKind {
    /// A stream table managed by pg_trickle.
    #[default]
    Stream,
    /// A base table tracked for cascade analysis (not managed by aqueduct).
    Source,
    /// A consumer view that reads from stream tables.
    Consumer,
}

impl std::str::FromStr for MigrationKind {
    type Err = AqueductError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "stream" => Ok(MigrationKind::Stream),
            "source" => Ok(MigrationKind::Source),
            "consumer" => Ok(MigrationKind::Consumer),
            other => Err(AqueductError::Parse {
                file: "<front-matter>".to_string(),
                message: format!("unknown kind: '{}'", other),
            }),
        }
    }
}

/// Front-matter directives parsed from a migration SQL file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FrontMatter {
    /// Migration kind (stream | source | consumer). Defaults to "stream".
    #[serde(default)]
    pub kind: MigrationKind,

    /// Whether aqueduct owns the DDL for this object (default: true for streams, false for sources).
    pub owned: Option<bool>,

    /// Explicit dependency list. Usually inferred from SQL via pg_query; use only when the parser
    /// cannot detect the dependency (functions, implicit casts, dynamic SQL).
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Refresh schedule, e.g. "30s", "5m".
    pub schedule: Option<String>,

    /// Refresh mode: DIFFERENTIAL or FULL.
    pub refresh_mode: Option<String>,

    /// CDC mode: trigger or wal.
    pub cdc_mode: Option<String>,

    /// Schema in which the stream table should live (default: "public").
    pub schema: Option<String>,

    /// Unknown / forward-compatible keys (stored for lint warnings).
    #[serde(flatten)]
    pub unknown_keys: HashMap<String, serde_json::Value>,
}

/// A parsed migration file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationFile {
    /// Source file path.
    pub path: std::path::PathBuf,

    /// Object name (derived from filename without extension).
    pub name: String,

    /// Parsed front-matter directives.
    pub front_matter: FrontMatter,

    /// The SQL body (everything after the front-matter directives).
    pub sql_body: String,

    /// Front-matter directives that were not recognised (lint warnings).
    pub unknown_keys: Vec<String>,
}

/// Parse all migration files under `migrations/streams/` and `migrations/sources/` in `project_dir`.
pub fn load_migrations(
    project_dir: &Path,
    vars: &HashMap<String, String>,
) -> Result<Vec<MigrationFile>> {
    let mut files = Vec::new();

    for subdir in &["streams", "sources"] {
        let dir = project_dir.join("migrations").join(subdir);
        if !dir.exists() {
            continue;
        }

        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .map_err(AqueductError::Io)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "sql")
                    .unwrap_or(false)
            })
            .collect();

        // Sort by filename for deterministic ordering.
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let content = std::fs::read_to_string(&path)?;
            let file = parse_migration_file(&path, &content, vars)?;
            files.push(file);
        }
    }

    Ok(files)
}

/// Parse a single migration file's content (front-matter + SQL body).
pub fn parse_migration_file(
    path: &Path,
    content: &str,
    vars: &HashMap<String, String>,
) -> Result<MigrationFile> {
    let filename = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let (front_matter_lines, sql_lines) = split_front_matter(content);

    // Apply template variable substitution to front-matter before parsing.
    let front_matter_substituted: Vec<String> = front_matter_lines
        .iter()
        .map(|line| substitute_vars(line, vars))
        .collect::<Result<Vec<_>>>()?;

    let (front_matter, unknown_keys) = parse_front_matter(&filename, &front_matter_substituted)?;

    let sql_body = sql_lines
        .join("\n")
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();

    Ok(MigrationFile {
        path: path.to_path_buf(),
        name: filename,
        front_matter,
        sql_body,
        unknown_keys,
    })
}

/// Split the content into front-matter lines (the `-- @aqueduct:...` comments)
/// and the SQL body (everything after the front-matter block ends).
fn split_front_matter(content: &str) -> (Vec<String>, Vec<String>) {
    let mut front_matter = Vec::new();
    let mut sql_body = Vec::new();
    let mut in_front_matter = true;

    for line in content.lines() {
        if in_front_matter {
            let trimmed = line.trim();
            if trimmed.starts_with("-- @aqueduct:") {
                front_matter.push(line.to_string());
            } else if trimmed.is_empty() || trimmed.starts_with("--") {
                // Allow blank lines and regular comments in front-matter section.
                // Keep them but don't mark them as front-matter directives.
            } else {
                in_front_matter = false;
                sql_body.push(line.to_string());
            }
        } else {
            sql_body.push(line.to_string());
        }
    }

    (front_matter, sql_body)
}

/// Parse `-- @aqueduct:key = value` directives from a list of lines.
fn parse_front_matter(filename: &str, lines: &[String]) -> Result<(FrontMatter, Vec<String>)> {
    let mut kind: Option<MigrationKind> = None;
    let mut owned: Option<bool> = None;
    let mut depends_on: Vec<String> = Vec::new();
    let mut schedule: Option<String> = None;
    let mut refresh_mode: Option<String> = None;
    let mut cdc_mode: Option<String> = None;
    let mut schema: Option<String> = None;
    let mut unknown_keys: Vec<String> = Vec::new();
    let mut known_key_values: HashMap<String, serde_json::Value> = HashMap::new();

    let directive_re = regex::Regex::new(r"^--\s+@aqueduct:([a-z_]+)\s*=\s*(.+)$").unwrap();

    for line in lines {
        if let Some(cap) = directive_re.captures(line.trim()) {
            let key = cap.get(1).unwrap().as_str();
            let raw_value = cap.get(2).unwrap().as_str().trim();

            match key {
                "kind" => {
                    let stripped = strip_string_quotes(raw_value);
                    kind = Some(stripped.parse()?);
                }
                "owned" => {
                    owned = Some(parse_bool_value(filename, raw_value)?);
                }
                "depends_on" => {
                    depends_on = parse_string_array(filename, raw_value)?;
                }
                "schedule" => {
                    schedule = Some(strip_string_quotes(raw_value).to_string());
                }
                "refresh_mode" => {
                    refresh_mode = Some(strip_string_quotes(raw_value).to_string());
                }
                "cdc_mode" => {
                    cdc_mode = Some(strip_string_quotes(raw_value).to_string());
                }
                "schema" => {
                    schema = Some(strip_string_quotes(raw_value).to_string());
                }
                other => {
                    unknown_keys.push(other.to_string());
                    // Store for forward-compat.
                    known_key_values.insert(
                        other.to_string(),
                        serde_json::Value::String(raw_value.to_string()),
                    );
                }
            }
        }
    }

    let fm = FrontMatter {
        kind: kind.unwrap_or_default(),
        owned,
        depends_on,
        schedule,
        refresh_mode,
        cdc_mode,
        schema,
        unknown_keys: known_key_values,
    };

    Ok((fm, unknown_keys))
}

fn strip_string_quotes(s: &str) -> &str {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

fn parse_bool_value(filename: &str, s: &str) -> Result<bool> {
    match s.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(AqueductError::Parse {
            file: filename.to_string(),
            message: format!("expected boolean, got '{}'", other),
        }),
    }
}

fn parse_string_array(filename: &str, s: &str) -> Result<Vec<String>> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return Err(AqueductError::Parse {
            file: filename.to_string(),
            message: format!("expected array like [\"a\", \"b\"], got '{}'", s),
        });
    }

    // Use serde_json to parse the array.
    let arr: serde_json::Value = serde_json::from_str(s).map_err(|e| AqueductError::Parse {
        file: filename.to_string(),
        message: format!("invalid array syntax: {}", e),
    })?;

    match arr {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(s),
                other => Err(AqueductError::Parse {
                    file: filename.to_string(),
                    message: format!("array item must be a string, got {:?}", other),
                }),
            })
            .collect(),
        _ => Err(AqueductError::Parse {
            file: filename.to_string(),
            message: "expected array".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn parse(content: &str) -> MigrationFile {
        parse_migration_file(&PathBuf::from("test.sql"), content, &HashMap::new()).unwrap()
    }

    #[test]
    fn test_parse_stream_table() {
        let f = parse(
            r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id;
"#,
        );
        assert_eq!(f.front_matter.schedule, Some("30s".to_string()));
        assert_eq!(
            f.front_matter.refresh_mode,
            Some("DIFFERENTIAL".to_string())
        );
        assert!(f.sql_body.contains("SELECT customer_id"));
    }

    #[test]
    fn test_parse_source_file() {
        let f = parse(
            r#"-- @aqueduct:kind = "source"
-- @aqueduct:owned = false
CREATE TABLE raw.orders (id bigint PRIMARY KEY);
"#,
        );
        assert_eq!(f.front_matter.kind, MigrationKind::Source);
        assert_eq!(f.front_matter.owned, Some(false));
    }

    #[test]
    fn test_parse_depends_on() {
        let f = parse(
            r#"-- @aqueduct:depends_on = ["raw.orders", "raw.customers"]
SELECT 1;
"#,
        );
        assert_eq!(
            f.front_matter.depends_on,
            vec!["raw.orders".to_string(), "raw.customers".to_string()]
        );
    }

    #[test]
    fn test_unknown_key_warning() {
        let f = parse(
            r#"-- @aqueduct:future_key = "value"
SELECT 1;
"#,
        );
        assert!(f.unknown_keys.contains(&"future_key".to_string()));
    }

    #[test]
    fn test_template_var_substitution() {
        let mut vars = HashMap::new();
        vars.insert("schedule".to_string(), "1m".to_string());
        let f = parse_migration_file(
            &PathBuf::from("test.sql"),
            "-- @aqueduct:schedule = \"{{ var.schedule }}\"\nSELECT 1;",
            &vars,
        )
        .unwrap();
        assert_eq!(f.front_matter.schedule, Some("1m".to_string()));
    }

    #[test]
    fn test_empty_sql_body() {
        let f = parse("-- @aqueduct:schedule = \"30s\"\n");
        assert_eq!(f.sql_body, "");
    }
}
