//! Project destruction: drop all stream tables, consumer views, and catalog
//! entries owned by a project.
//!
//! `aqueduct destroy --project <name> --to <target> [--confirm]`

use crate::dag::QualifiedName;
use crate::error::{AqueductError, Result};
use crate::live_state::read_live_state;

/// Options for the destroy operation.
#[derive(Debug, Clone)]
pub struct DestroyOptions {
    /// The project name to destroy.
    pub project: String,
    /// If true, only compute what would be done without executing.
    pub dry_run: bool,
    /// If true, allow CASCADE on the DROP TABLE (removes dependent objects).
    /// By default, DROP TABLE refuses if there are dependents (S-10).
    pub force_cascade: bool,
    /// If true, skip ownership verification and drop tables not in this
    /// project's ownership registry (C-06).
    pub force_unowned: bool,
}

/// Summary of what was (or would be) destroyed.
#[derive(Debug)]
pub struct DestroyResult {
    /// Stream tables dropped (or that would be dropped).
    pub stream_tables_dropped: Vec<QualifiedName>,
    /// Consumer views dropped.
    pub consumer_views_dropped: Vec<String>,
    /// Number of catalog rows deleted.
    pub catalog_rows_deleted: usize,
    /// Whether the operation was a dry run.
    pub dry_run: bool,
}

impl DestroyResult {
    pub fn is_empty(&self) -> bool {
        self.stream_tables_dropped.is_empty()
            && self.consumer_views_dropped.is_empty()
            && self.catalog_rows_deleted == 0
    }
}

/// Destroy all resources owned by a project.
///
/// Drops stream tables in reverse topological order, drops consumer views,
/// and removes catalog entries.  Does NOT drop the `aqueduct` schema itself.
///
/// Requires the caller to have confirmed the operation (pass `--confirm` on
/// the CLI or call this function only after user confirmation).
pub async fn destroy_project(
    client: &tokio_postgres::Client,
    options: &DestroyOptions,
) -> Result<DestroyResult> {
    // Check we're connected to a primary.
    let is_primary: bool = client
        .query_one("SELECT NOT pg_is_in_recovery()", &[])
        .await?
        .get(0);
    if !is_primary {
        return Err(AqueductError::NotPrimary);
    }

    // Read the live state to find managed stream tables.
    // C-06: filter by project so we only touch this project's tables.
    let live = read_live_state(client, Some(&options.project)).await?;

    // C-06: Verify ownership for each stream table before dropping.
    // If `force_unowned` is false, reject any table not owned by this project.
    if !options.force_unowned {
        let ownership_exists: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'aqueduct'
                      AND table_name   = 'stream_table_ownership'
                )",
                &[],
            )
            .await?
            .get(0);

        if ownership_exists {
            for table in &live.stream_tables {
                let owner: Option<String> = client
                    .query_opt(
                        "SELECT project FROM aqueduct.stream_table_ownership \
                         WHERE schema_name = $1 AND table_name = $2",
                        &[&table.qualified_name.schema, &table.qualified_name.name],
                    )
                    .await?
                    .map(|r| r.get(0));
                match owner {
                    Some(ref o) if o != &options.project => {
                        return Err(AqueductError::OwnershipRequired {
                            table: table.qualified_name.to_string(),
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    // Compute reverse topological order for safe drops.
    let topo_names = crate::dag::topological_sort(&live)?;
    // Reverse for teardown: drop leaves first (reverse dependency order).
    let tables_to_drop: Vec<QualifiedName> = topo_names.into_iter().rev().collect();

    // Collect consumer views registered for this project.
    let consumer_views: Vec<String> = {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'aqueduct' AND table_name = 'consumer_views'
                )",
                &[],
            )
            .await?
            .get(0);

        if exists {
            let rows = client
                .query(
                    "SELECT expose_as FROM aqueduct.consumer_views WHERE project = $1",
                    &[&options.project],
                )
                .await?;
            rows.iter().map(|r| r.get::<_, String>(0)).collect()
        } else {
            vec![]
        }
    };

    if options.dry_run {
        return Ok(DestroyResult {
            stream_tables_dropped: tables_to_drop,
            consumer_views_dropped: consumer_views,
            catalog_rows_deleted: 0,
            dry_run: true,
        });
    }

    // Check if pgtrickle is available.
    let pgtrickle_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
            &[],
        )
        .await?
        .get(0);

    // Drop stream tables in reverse topological order.
    let mut dropped_tables = Vec::new();
    for name in &tables_to_drop {
        tracing::info!("Dropping stream table '{}'", name);
        if pgtrickle_exists {
            client
                .execute(
                    "SELECT pgtrickle.drop_stream_table($1, $2)",
                    &[&name.schema, &name.name],
                )
                .await
                .ok(); // Best-effort; table may already be gone.
        } else {
            // S-10: No CASCADE by default to avoid silently destroying
            // dependent objects. Use `--force-cascade` to opt in.
            let cascade_clause = if options.force_cascade {
                " CASCADE"
            } else {
                ""
            };
            if let Err(e) = client
                .execute(
                    &format!(
                        "DROP TABLE IF EXISTS {}.{}{}",
                        quote_ident(&name.schema),
                        quote_ident(&name.name),
                        cascade_clause,
                    ),
                    &[],
                )
                .await
            {
                // If the error is about dependent objects and cascade is not enabled,
                // return a clear error message.
                if !options.force_cascade && e.to_string().contains("depends on") {
                    return Err(AqueductError::Other(format!(
                        "Cannot drop '{}': dependent objects exist. \
                         Use --force-cascade to also drop them.",
                        name
                    )));
                }
                tracing::warn!("DROP TABLE '{}' failed (continuing): {}", name, e);
            }
        }
        dropped_tables.push(name.clone());
    }

    // Drop consumer views.
    let mut dropped_views = Vec::new();
    for expose_as in &consumer_views {
        tracing::info!("Dropping consumer view '{}'", expose_as);
        // Parse schema.name
        let (schema, view) = if let Some(dot) = expose_as.find('.') {
            (&expose_as[..dot], &expose_as[dot + 1..])
        } else {
            ("public", expose_as.as_str())
        };
        client
            .execute(
                &format!(
                    "DROP VIEW IF EXISTS {}.{}",
                    quote_ident(schema),
                    quote_ident(view)
                ),
                &[],
            )
            .await
            .ok();
        dropped_views.push(expose_as.clone());
    }

    // Delete catalog rows.
    let mut catalog_deleted: usize = 0;

    // Delete consumer_views rows.
    let consumer_del = client
        .execute(
            "DELETE FROM aqueduct.consumer_views WHERE project = $1",
            &[&options.project],
        )
        .await
        .unwrap_or(0);
    catalog_deleted += consumer_del as usize;

    // Delete blue_green_deployments rows.
    let bg_del = client
        .execute(
            "DELETE FROM aqueduct.blue_green_deployments WHERE project = $1",
            &[&options.project],
        )
        .await
        .unwrap_or(0);
    catalog_deleted += bg_del as usize;

    // Delete locks.
    let lock_del = client
        .execute(
            "DELETE FROM aqueduct.locks WHERE project = $1",
            &[&options.project],
        )
        .await
        .unwrap_or(0);
    catalog_deleted += lock_del as usize;

    // Delete migration records (must come before dag_versions due to FK).
    let mig_del = client
        .execute(
            "DELETE FROM aqueduct.migrations WHERE project = $1",
            &[&options.project],
        )
        .await
        .unwrap_or(0);
    catalog_deleted += mig_del as usize;

    // Delete dag_versions.
    let ver_del = client
        .execute(
            "DELETE FROM aqueduct.dag_versions WHERE project = $1",
            &[&options.project],
        )
        .await
        .unwrap_or(0);
    catalog_deleted += ver_del as usize;

    // Delete stream_table_ownership rows (best-effort; table may not exist in v2 catalogs).
    client
        .execute(
            "DELETE FROM aqueduct.stream_table_ownership WHERE project = $1",
            &[&options.project],
        )
        .await
        .ok();

    Ok(DestroyResult {
        stream_tables_dropped: dropped_tables,
        consumer_views_dropped: dropped_views,
        catalog_rows_deleted: catalog_deleted,
        dry_run: false,
    })
}

/// Minimal identifier quoting.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destroy_options_dry_run() {
        let opts = DestroyOptions {
            project: "test".to_string(),
            dry_run: true,
            force_cascade: false,
            force_unowned: false,
        };
        assert!(opts.dry_run);
    }

    #[test]
    fn test_destroy_result_is_empty() {
        let r = DestroyResult {
            stream_tables_dropped: vec![],
            consumer_views_dropped: vec![],
            catalog_rows_deleted: 0,
            dry_run: true,
        };
        assert!(r.is_empty());
    }

    #[test]
    fn test_destroy_result_not_empty() {
        let r = DestroyResult {
            stream_tables_dropped: vec![QualifiedName::new("public", "orders")],
            consumer_views_dropped: vec![],
            catalog_rows_deleted: 0,
            dry_run: false,
        };
        assert!(!r.is_empty());
    }
}
