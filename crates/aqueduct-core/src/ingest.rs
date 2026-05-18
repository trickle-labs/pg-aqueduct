//! dbt interop — ingest compiled dbt artefacts into an aqueduct migrations directory.
//!
//! This module implements `aqueduct ingest --from dbt-target <dir>`, which reads
//! dbt's compiled `manifest.json` and `compiled/` SQL directory for models
//! materialised as `stream_table` via the `dbt-pgtrickle` package.
//!
//! For each `stream_table` model the function:
//! 1. Reads the compiled SQL from the dbt target directory.
//! 2. Constructs a canonical `migrations/streams/{name}.sql` file with front-matter
//!    directives populated from the dbt model config (`schedule`, `refresh_mode`,
//!    `cdc_mode`, etc.).
//! 3. Generates `migrations/sources/{name}.sql` for every dbt source referenced by
//!    a stream-table model (`owned = false`).
//! 4. Returns an `IngestResult` describing what was created, updated, or unchanged.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{AqueductError, Result};
use crate::fmt::render_migration;
use crate::parser::{FrontMatter, MigrationFile, MigrationKind};

// ── dbt manifest types ────────────────────────────────────────────────────────

/// The top-level structure of a dbt `manifest.json`.
#[derive(Debug, Deserialize)]
pub struct DbtManifest {
    pub nodes: HashMap<String, DbtNode>,
    #[serde(default)]
    pub sources: HashMap<String, DbtSource>,
}

/// A single dbt model node from the manifest.
#[derive(Debug, Deserialize)]
pub struct DbtNode {
    pub resource_type: String,
    pub name: String,
    pub config: DbtNodeConfig,
    pub depends_on: DbtDependsOn,
    /// Path to the compiled SQL file, relative to the dbt target directory (dbt v1.x).
    pub compiled_path: Option<String>,
    /// Original source file path (used as a fallback for locating compiled SQL).
    #[serde(default)]
    pub original_file_path: String,
}

/// dbt model configuration block relevant to `aqueduct ingest`.
#[derive(Debug, Deserialize, Default)]
pub struct DbtNodeConfig {
    /// dbt materialisation type. Must be `"stream_table"` to be processed.
    #[serde(default)]
    pub materialized: Option<String>,
    /// Refresh schedule (maps to `@aqueduct:schedule`).
    #[serde(default)]
    pub schedule: Option<String>,
    /// Refresh mode (maps to `@aqueduct:refresh_mode`).
    #[serde(default)]
    pub refresh_mode: Option<String>,
    /// CDC mode (maps to `@aqueduct:cdc_mode`).
    #[serde(default)]
    pub cdc_mode: Option<String>,
    /// Target schema (maps to `@aqueduct:schema`).
    #[serde(default)]
    pub schema: Option<String>,
    /// Explicit `depends_on` overrides from `+depends_on` in the dbt model config.
    /// When set, these are emitted as `@aqueduct:depends_on` and suppress the
    /// automatic derivation from dbt source references.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// The `depends_on` block inside a dbt node.
#[derive(Debug, Deserialize, Default)]
pub struct DbtDependsOn {
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// A dbt source entry from the manifest.
#[derive(Debug, Deserialize, Clone)]
pub struct DbtSource {
    pub resource_type: String,
    /// The table / object name within the source.
    pub name: String,
    /// The schema the source lives in.
    pub schema: String,
    /// The logical source group name.
    #[serde(default)]
    pub source_name: String,
    /// Physical identifier (overrides `name` when set).
    #[serde(default)]
    pub identifier: Option<String>,
}

// ── ingest result ─────────────────────────────────────────────────────────────

/// Describes what happened to a single migration file during ingest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestChangeKind {
    /// File was written for the first time (new dbt model).
    Created,
    /// File existed and its content changed.
    Updated,
    /// File existed and its content is identical — nothing was written.
    Unchanged,
}

impl std::fmt::Display for IngestChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestChangeKind::Created => write!(f, "created"),
            IngestChangeKind::Updated => write!(f, "updated"),
            IngestChangeKind::Unchanged => write!(f, "unchanged"),
        }
    }
}

/// A per-file change record returned by `ingest_from_dbt`.
#[derive(Debug, Clone, Serialize)]
pub struct IngestChange {
    pub kind: IngestChangeKind,
    pub path: PathBuf,
    pub name: String,
}

/// The aggregate result of an `aqueduct ingest` run.
#[derive(Debug, Serialize)]
pub struct IngestResult {
    /// Number of stream migration files that were created or updated.
    pub streams_changed: usize,
    /// Number of source migration files that were created or updated.
    pub sources_changed: usize,
    /// Total stream models found in the dbt manifest.
    pub streams_total: usize,
    /// Total source files generated (one per referenced dbt source).
    pub sources_total: usize,
    /// Detailed per-file change log.
    pub changes: Vec<IngestChange>,
}

impl IngestResult {
    /// Returns `true` if any file was created or updated.
    pub fn has_changes(&self) -> bool {
        self.streams_changed > 0 || self.sources_changed > 0
    }
}

// ── public API ────────────────────────────────────────────────────────────────

/// Ingest compiled dbt artefacts into an aqueduct migrations directory.
///
/// `dbt_target_dir` — path to the dbt target directory (contains `manifest.json`
/// and the `compiled/` sub-tree produced by `dbt compile`).
///
/// `output_dir` — root of the aqueduct project. Migration files are written to
/// `<output_dir>/migrations/streams/` and `<output_dir>/migrations/sources/`.
pub fn ingest_from_dbt(dbt_target_dir: &Path, output_dir: &Path) -> Result<IngestResult> {
    let manifest = load_manifest(dbt_target_dir)?;

    // Ensure output directories exist.
    let streams_dir = output_dir.join("migrations").join("streams");
    let sources_dir = output_dir.join("migrations").join("sources");
    std::fs::create_dir_all(&streams_dir).map_err(AqueductError::Io)?;
    std::fs::create_dir_all(&sources_dir).map_err(AqueductError::Io)?;

    // Collect stream_table model nodes in deterministic order.
    let mut stream_models: Vec<&DbtNode> = manifest
        .nodes
        .values()
        .filter(|n| is_stream_table_model(n))
        .collect();
    stream_models.sort_by(|a, b| a.name.cmp(&b.name));

    // Collect sources referenced by any stream_table model (deduplicated).
    let referenced_sources = collect_referenced_sources(&stream_models, &manifest.sources);

    let mut changes: Vec<IngestChange> = Vec::new();
    let mut streams_changed = 0usize;
    let mut sources_changed = 0usize;

    // Generate stream migration files.
    for model in &stream_models {
        let compiled_sql = read_compiled_sql(dbt_target_dir, model)?;
        let depends_on = derive_depends_on(model, &manifest.sources);
        let migration_file =
            build_stream_migration_file(model, &compiled_sql, &depends_on, &streams_dir);
        let content = render_migration(&migration_file);
        let output_path = streams_dir.join(format!("{}.sql", model.name));
        let change = write_if_changed(&output_path, &model.name, &content)?;
        if change.kind != IngestChangeKind::Unchanged {
            streams_changed += 1;
        }
        changes.push(change);
    }

    // Generate source migration files.
    let mut sorted_sources: Vec<(&String, &DbtSource)> =
        referenced_sources.iter().map(|(k, v)| (k, *v)).collect();
    sorted_sources.sort_by_key(|(id, _)| id.as_str());

    for (_, source) in &sorted_sources {
        let source_name = source_file_name(source);
        let migration_file = build_source_migration_file(source, &sources_dir, &source_name);
        let content = render_migration(&migration_file);
        let output_path = sources_dir.join(format!("{}.sql", source_name));
        let change = write_if_changed(&output_path, &source_name, &content)?;
        if change.kind != IngestChangeKind::Unchanged {
            sources_changed += 1;
        }
        changes.push(change);
    }

    Ok(IngestResult {
        streams_changed,
        sources_changed,
        streams_total: stream_models.len(),
        sources_total: sorted_sources.len(),
        changes,
    })
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn load_manifest(dbt_target_dir: &Path) -> Result<DbtManifest> {
    let manifest_path = dbt_target_dir.join("manifest.json");
    if !manifest_path.exists() {
        return Err(AqueductError::Config(format!(
            "manifest.json not found at {}. Run `dbt compile` first.",
            manifest_path.display()
        )));
    }
    let content = std::fs::read_to_string(&manifest_path).map_err(AqueductError::Io)?;
    serde_json::from_str(&content).map_err(|e| AqueductError::Parse {
        file: manifest_path.display().to_string(),
        message: format!("failed to parse manifest.json: {}", e),
    })
}

fn is_stream_table_model(node: &DbtNode) -> bool {
    node.resource_type == "model"
        && node
            .config
            .materialized
            .as_deref()
            .map(|m| m == "stream_table")
            .unwrap_or(false)
}

fn collect_referenced_sources<'a>(
    models: &[&DbtNode],
    sources: &'a HashMap<String, DbtSource>,
) -> HashMap<String, &'a DbtSource> {
    let mut result = HashMap::new();
    for model in models {
        for dep_id in &model.depends_on.nodes {
            if dep_id.starts_with("source.") {
                if let Some(src) = sources.get(dep_id) {
                    result.insert(dep_id.clone(), src);
                }
            }
        }
    }
    result
}

/// Derive `@aqueduct:depends_on` for a model.
///
/// If explicit overrides are set in the model's dbt config (`+depends_on`), those
/// take precedence. Otherwise, the list is derived from source references in
/// `depends_on.nodes` (model→model edges are inferred by aqueduct from the SQL).
fn derive_depends_on(model: &DbtNode, sources: &HashMap<String, DbtSource>) -> Vec<String> {
    if !model.config.depends_on.is_empty() {
        return model.config.depends_on.clone();
    }

    let mut deps: Vec<String> = model
        .depends_on
        .nodes
        .iter()
        .filter(|id| id.starts_with("source."))
        .filter_map(|id| {
            sources.get(id).map(|src| {
                let table = src.identifier.as_deref().unwrap_or(&src.name);
                format!("{}.{}", src.schema, table)
            })
        })
        .collect();

    deps.sort();
    deps.dedup();
    deps
}

fn build_stream_migration_file(
    model: &DbtNode,
    compiled_sql: &str,
    depends_on: &[String],
    streams_dir: &Path,
) -> MigrationFile {
    let sql_body = compiled_sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();

    let schema = model
        .config
        .schema
        .clone()
        .filter(|s| s != "public" && !s.is_empty());

    MigrationFile {
        path: streams_dir.join(format!("{}.sql", model.name)),
        name: model.name.clone(),
        front_matter: FrontMatter {
            kind: MigrationKind::Stream,
            owned: None,
            schema,
            depends_on: depends_on.to_vec(),
            schedule: model.config.schedule.clone(),
            refresh_mode: model
                .config
                .refresh_mode
                .as_ref()
                .map(|rm| rm.to_uppercase()),
            cdc_mode: model.config.cdc_mode.clone(),
            cypher_source: None,
            source: None,
            expose_as: None,
            unknown_keys: Default::default(),
        },
        sql_body,
        unknown_keys: vec![],
    }
}

fn build_source_migration_file(
    source: &DbtSource,
    sources_dir: &Path,
    file_stem: &str,
) -> MigrationFile {
    let schema = if source.schema.is_empty() || source.schema == "public" {
        None
    } else {
        Some(source.schema.clone())
    };

    MigrationFile {
        path: sources_dir.join(format!("{}.sql", file_stem)),
        name: file_stem.to_string(),
        front_matter: FrontMatter {
            kind: MigrationKind::Source,
            owned: Some(false),
            schema,
            depends_on: vec![],
            schedule: None,
            refresh_mode: None,
            cdc_mode: None,
            cypher_source: None,
            source: None,
            expose_as: None,
            unknown_keys: Default::default(),
        },
        sql_body: String::new(),
        unknown_keys: vec![],
    }
}

/// Generate the migration file name stem for a dbt source entry.
///
/// When the schema is non-public the file is named `{schema}_{table}` to avoid
/// collisions between same-named tables in different schemas.
fn source_file_name(source: &DbtSource) -> String {
    let table = source.identifier.as_deref().unwrap_or(&source.name);
    if source.schema.is_empty() || source.schema == "public" {
        table.to_string()
    } else {
        format!("{}_{}", source.schema, table)
    }
}

/// Read compiled SQL from the dbt target directory.
///
/// Tries `compiled_path` first (dbt v1.x), then falls back to a recursive search
/// under `<dbt_target_dir>/compiled/` for a file named `{model.name}.sql`.
fn read_compiled_sql(dbt_target_dir: &Path, model: &DbtNode) -> Result<String> {
    // Primary: explicit compiled_path field (dbt v1.x).
    if let Some(ref compiled_path) = model.compiled_path {
        let full_path = dbt_target_dir.join(compiled_path);
        if full_path.exists() {
            return std::fs::read_to_string(&full_path).map_err(AqueductError::Io);
        }
        // Also try the path relative to the dbt project root (dbt sometimes strips
        // the target/ prefix from compiled_path).
        let alt_path = dbt_target_dir.parent().unwrap_or(dbt_target_dir).join(compiled_path);
        if alt_path.exists() {
            return std::fs::read_to_string(&alt_path).map_err(AqueductError::Io);
        }
    }

    // Fallback: recursive search under compiled/.
    let compiled_dir = dbt_target_dir.join("compiled");
    if compiled_dir.exists() {
        if let Some(found) = find_compiled_sql_recursive(&compiled_dir, &model.name) {
            return std::fs::read_to_string(&found).map_err(AqueductError::Io);
        }
    }

    Err(AqueductError::Config(format!(
        "compiled SQL not found for dbt model '{}'. \
         Ensure `dbt compile` has been run and the target directory is correct.",
        model.name
    )))
}

/// Recursively search `dir` for a file named `{name}.sql`.
fn find_compiled_sql_recursive(dir: &Path, name: &str) -> Option<PathBuf> {
    let target_filename = format!("{}.sql", name);
    if let Ok(entries) = std::fs::read_dir(dir) {
        let mut entries: Vec<_> = entries.flatten().collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                if let Some(found) = find_compiled_sql_recursive(&path, name) {
                    return Some(found);
                }
            } else if path.file_name().and_then(|n| n.to_str()) == Some(&target_filename) {
                return Some(path);
            }
        }
    }
    None
}

/// Write `content` to `path`, but only if the file does not already contain the
/// same content. Returns an `IngestChange` describing what happened.
fn write_if_changed(path: &Path, name: &str, content: &str) -> Result<IngestChange> {
    let kind = if path.exists() {
        let existing = std::fs::read_to_string(path).map_err(AqueductError::Io)?;
        if existing == content {
            IngestChangeKind::Unchanged
        } else {
            std::fs::write(path, content).map_err(AqueductError::Io)?;
            IngestChangeKind::Updated
        }
    } else {
        std::fs::write(path, content).map_err(AqueductError::Io)?;
        IngestChangeKind::Created
    };

    Ok(IngestChange {
        kind,
        path: path.to_path_buf(),
        name: name.to_string(),
    })
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_manifest(dir: &Path, manifest_json: &str) {
        fs::write(dir.join("manifest.json"), manifest_json).unwrap();
    }

    fn write_compiled_sql(dir: &Path, project: &str, model: &str, sql: &str) {
        let compiled_dir = dir.join("compiled").join(project).join("models");
        fs::create_dir_all(&compiled_dir).unwrap();
        fs::write(compiled_dir.join(format!("{}.sql", model)), sql).unwrap();
    }

    fn minimal_manifest(model_name: &str, schedule: &str) -> String {
        format!(
            r#"{{
  "nodes": {{
    "model.my_project.{model}": {{
      "resource_type": "model",
      "name": "{model}",
      "config": {{
        "materialized": "stream_table",
        "schedule": "{schedule}",
        "refresh_mode": "DIFFERENTIAL"
      }},
      "depends_on": {{ "nodes": [] }},
      "compiled_path": "compiled/my_project/models/{model}.sql",
      "original_file_path": "models/{model}.sql"
    }}
  }},
  "sources": {{}}
}}"#,
            model = model_name,
            schedule = schedule,
        )
    }

    fn manifest_with_source(model_name: &str, source_id: &str) -> String {
        format!(
            r#"{{
  "nodes": {{
    "model.my_project.{model}": {{
      "resource_type": "model",
      "name": "{model}",
      "config": {{
        "materialized": "stream_table",
        "schedule": "1m",
        "refresh_mode": "FULL"
      }},
      "depends_on": {{ "nodes": ["{source_id}"] }},
      "compiled_path": "compiled/my_project/models/{model}.sql",
      "original_file_path": "models/{model}.sql"
    }}
  }},
  "sources": {{
    "{source_id}": {{
      "resource_type": "source",
      "name": "orders",
      "schema": "raw",
      "source_name": "warehouse",
      "identifier": null
    }}
  }}
}}"#,
            model = model_name,
            source_id = source_id,
        )
    }

    #[test]
    fn test_ingest_simple_model() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        write_manifest(&dbt_dir, &minimal_manifest("order_totals", "30s"));
        write_compiled_sql(
            &dbt_dir,
            "my_project",
            "order_totals",
            "SELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id",
        );

        let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();

        assert_eq!(result.streams_total, 1);
        assert_eq!(result.streams_changed, 1);
        assert_eq!(result.sources_total, 0);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(result.changes[0].kind, IngestChangeKind::Created);
        assert_eq!(result.changes[0].name, "order_totals");

        let written = fs::read_to_string(
            out_dir
                .join("migrations")
                .join("streams")
                .join("order_totals.sql"),
        )
        .unwrap();
        assert!(written.contains("@aqueduct:schedule"));
        assert!(written.contains("30s"));
        assert!(written.contains("DIFFERENTIAL"));
        assert!(written.contains("SELECT"));
    }

    #[test]
    fn test_ingest_generates_source_file() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        let source_id = "source.my_project.warehouse.orders";
        write_manifest(&dbt_dir, &manifest_with_source("totals", source_id));
        write_compiled_sql(
            &dbt_dir,
            "my_project",
            "totals",
            "SELECT customer_id, COUNT(*) FROM raw.orders GROUP BY customer_id",
        );

        let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();

        assert_eq!(result.streams_total, 1);
        assert_eq!(result.sources_total, 1);

        let source_path = out_dir
            .join("migrations")
            .join("sources")
            .join("raw_orders.sql");
        assert!(source_path.exists(), "source file should be generated");

        let source_content = fs::read_to_string(&source_path).unwrap();
        assert!(source_content.contains("@aqueduct:kind"));
        assert!(source_content.contains("source"));
        assert!(source_content.contains("owned = false"));
    }

    #[test]
    fn test_ingest_unchanged_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        write_manifest(&dbt_dir, &minimal_manifest("my_model", "5m"));
        write_compiled_sql(&dbt_dir, "my_project", "my_model", "SELECT 1 AS val");

        // First ingest — creates files.
        let r1 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
        assert_eq!(r1.streams_changed, 1);

        // Second ingest — same manifest/SQL, no changes.
        let r2 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
        assert_eq!(r2.streams_changed, 0);
        assert_eq!(r2.changes[0].kind, IngestChangeKind::Unchanged);
    }

    #[test]
    fn test_ingest_detects_update() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        write_manifest(&dbt_dir, &minimal_manifest("my_model", "30s"));
        write_compiled_sql(&dbt_dir, "my_project", "my_model", "SELECT 1 AS val");

        // First ingest.
        let r1 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
        assert_eq!(r1.streams_changed, 1);

        // Change the compiled SQL.
        write_compiled_sql(
            &dbt_dir,
            "my_project",
            "my_model",
            "SELECT 1 AS val, 2 AS extra",
        );
        let r2 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
        assert_eq!(r2.streams_changed, 1);
        assert_eq!(r2.changes[0].kind, IngestChangeKind::Updated);
    }

    #[test]
    fn test_ingest_skips_non_stream_table_models() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        // Manifest with one table model and one stream_table model.
        let manifest = r#"{
  "nodes": {
    "model.proj.regular_table": {
      "resource_type": "model",
      "name": "regular_table",
      "config": { "materialized": "table" },
      "depends_on": { "nodes": [] },
      "compiled_path": null,
      "original_file_path": "models/regular_table.sql"
    },
    "model.proj.stream_model": {
      "resource_type": "model",
      "name": "stream_model",
      "config": { "materialized": "stream_table", "schedule": "1m" },
      "depends_on": { "nodes": [] },
      "compiled_path": "compiled/proj/models/stream_model.sql",
      "original_file_path": "models/stream_model.sql"
    }
  },
  "sources": {}
}"#;
        write_manifest(&dbt_dir, manifest);
        write_compiled_sql(&dbt_dir, "proj", "stream_model", "SELECT 42 AS answer");

        let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();

        assert_eq!(result.streams_total, 1);
        assert_eq!(result.changes[0].name, "stream_model");
    }

    #[test]
    fn test_ingest_missing_manifest_error() {
        let tmp = TempDir::new().unwrap();
        let result = ingest_from_dbt(tmp.path(), tmp.path());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("manifest.json"));
    }

    #[test]
    fn test_ingest_explicit_depends_on_override() {
        let tmp = TempDir::new().unwrap();
        let dbt_dir = tmp.path().join("target");
        let out_dir = tmp.path().join("project");
        fs::create_dir_all(&dbt_dir).unwrap();

        let manifest = r#"{
  "nodes": {
    "model.p.my_model": {
      "resource_type": "model",
      "name": "my_model",
      "config": {
        "materialized": "stream_table",
        "schedule": "10s",
        "depends_on": ["raw.orders", "raw.customers"]
      },
      "depends_on": { "nodes": [] },
      "compiled_path": "compiled/p/models/my_model.sql",
      "original_file_path": "models/my_model.sql"
    }
  },
  "sources": {}
}"#;
        write_manifest(&dbt_dir, manifest);
        write_compiled_sql(&dbt_dir, "p", "my_model", "SELECT 1");

        let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
        assert_eq!(result.streams_changed, 1);

        let written = fs::read_to_string(
            out_dir
                .join("migrations")
                .join("streams")
                .join("my_model.sql"),
        )
        .unwrap();
        assert!(written.contains("depends_on"));
        assert!(written.contains("raw.orders"));
        assert!(written.contains("raw.customers"));
    }

    #[test]
    fn test_source_file_name_non_public_schema() {
        let src = DbtSource {
            resource_type: "source".to_string(),
            name: "orders".to_string(),
            schema: "analytics".to_string(),
            source_name: "warehouse".to_string(),
            identifier: None,
        };
        assert_eq!(source_file_name(&src), "analytics_orders");
    }

    #[test]
    fn test_source_file_name_public_schema() {
        let src = DbtSource {
            resource_type: "source".to_string(),
            name: "events".to_string(),
            schema: "public".to_string(),
            source_name: "app".to_string(),
            identifier: None,
        };
        assert_eq!(source_file_name(&src), "events");
    }

    #[test]
    fn test_source_file_name_uses_identifier() {
        let src = DbtSource {
            resource_type: "source".to_string(),
            name: "orders".to_string(),
            schema: "raw".to_string(),
            source_name: "warehouse".to_string(),
            identifier: Some("raw_orders_v2".to_string()),
        };
        assert_eq!(source_file_name(&src), "raw_raw_orders_v2");
    }
}
