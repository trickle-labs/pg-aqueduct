//! Multi-environment promotion workflow.
//!
//! `aqueduct promote --from dev --to staging` validates the migrations
//! directory against the source environment, computes a plan against the
//! destination environment, and applies it — recording both environment
//! names in the migration history.

use crate::dag::{build_dag_state, topological_sort};
use crate::diff::compute_diff;
use crate::error::{AqueductError, Result};
use crate::live_state::{get_latest_dag_version, read_live_state};
use crate::parser::MigrationFile;
use crate::plan::{build_plan, Plan};

/// Options for the promote operation.
#[derive(Debug, Clone)]
pub struct PromoteOptions {
    /// Source environment name (e.g. "dev").
    pub from_env: String,
    /// Destination environment name (e.g. "staging").
    pub to_env: String,
    /// Project name.
    pub project: String,
    /// If true, only compute the plan without executing.
    pub dry_run: bool,
}

/// Result of a promotion operation.
#[derive(Debug)]
pub struct PromoteResult {
    /// The computed promotion plan.
    pub plan: Plan,
    /// Whether the promotion was executed (false = dry_run or no-op).
    pub applied: bool,
    /// New DAG version after promotion (None if dry_run or no-op).
    pub new_version: Option<u64>,
    /// Source environment name.
    pub from_env: String,
    /// Destination environment name.
    pub to_env: String,
}

/// Compute the promotion plan from `files` (desired state) against the
/// destination database.  Does not execute — the caller decides whether to
/// apply.
pub async fn compute_promotion_plan(
    client: &tokio_postgres::Client,
    files: &[MigrationFile],
    options: &PromoteOptions,
    catalog_schema: &crate::catalog::CatalogSchema,
) -> Result<Plan> {
    // Build the desired DAG state from the migration files.
    let desired = build_dag_state(files, true)?;

    // Read the live state from the destination environment, filtered by project
    // so tables owned by other projects are never diffed (CORR-4 / v0.14).
    let actual = read_live_state(client, Some(&options.project), catalog_schema).await?;

    // Get the current DAG version from the destination.
    let current_version = get_latest_dag_version(client, &options.project, catalog_schema).await?;
    let next_version = current_version.map(|v| v + 1).unwrap_or(1);

    // Compute the diff.
    let diff = compute_diff(&desired, &actual);
    let topo_order = topological_sort(&desired)?;

    let plan = build_plan(
        &options.project,
        current_version,
        next_version,
        &diff,
        &topo_order,
    )?;

    Ok(plan)
}

/// Validate that the source environment is in a clean state (no drift,
/// applied version matches the current migrations directory).
///
/// Returns `Ok(())` if clean, `Err` with a description if drift is detected.
pub async fn validate_source_clean(
    source_client: &tokio_postgres::Client,
    files: &[MigrationFile],
    project: &str,
    catalog_schema: &crate::catalog::CatalogSchema,
) -> Result<()> {
    let desired = build_dag_state(files, false)?;
    // CORR-4: filter by project so tables from other projects are not included.
    let actual = read_live_state(source_client, Some(project), catalog_schema).await?;
    let diff = compute_diff(&desired, &actual);

    if !diff.is_empty() {
        return Err(AqueductError::Other(format!(
            "Source environment has pending drift ({} change(s)). \
             Run `aqueduct apply` against the source environment before promoting.",
            diff.changes().len()
        )));
    }

    // Ensure the source catalog is initialised.
    let version = get_latest_dag_version(source_client, project, catalog_schema).await?;
    if version.is_none() {
        return Err(AqueductError::Other(
            "Source environment catalog is not initialised. \
             Run `aqueduct init` and `aqueduct apply` against the source environment first."
                .to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{DagState, QualifiedName, RefreshMode, StreamTableSpec};
    use crate::diff::compute_diff;

    fn make_spec(name: &str) -> StreamTableSpec {
        StreamTableSpec {
            qualified_name: QualifiedName::new("public", name),
            query: format!("SELECT 1 AS {}", name),
            refresh_mode: RefreshMode::Full,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
            cypher_source: None,
        }
    }

    #[test]
    fn test_promote_options_clone() {
        let opts = PromoteOptions {
            from_env: "dev".to_string(),
            to_env: "staging".to_string(),
            project: "test-project".to_string(),
            dry_run: true,
        };
        let cloned = opts.clone();
        assert_eq!(cloned.from_env, "dev");
        assert_eq!(cloned.to_env, "staging");
        assert!(cloned.dry_run);
    }

    #[test]
    fn test_promote_plan_is_empty_when_in_sync() {
        let spec = make_spec("orders");
        let state = DagState {
            stream_tables: vec![spec.clone()],
            sources: vec![],
            consumers: vec![],
        };

        // When desired == actual, diff should be empty.
        let diff = compute_diff(&state, &state);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_promote_plan_detects_new_table() {
        let desired = DagState {
            stream_tables: vec![make_spec("orders"), make_spec("customers")],
            sources: vec![],
            consumers: vec![],
        };
        let actual = DagState {
            stream_tables: vec![make_spec("orders")],
            sources: vec![],
            consumers: vec![],
        };

        let diff = compute_diff(&desired, &actual);
        assert!(!diff.is_empty());
        assert_eq!(diff.changes().len(), 1);
    }
}
