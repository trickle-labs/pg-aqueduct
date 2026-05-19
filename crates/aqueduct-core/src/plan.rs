use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::classifier::MigrationClass;
use crate::dag::{ConsumerSpec, QualifiedName, StreamTableSpec};
use crate::diff::{ConsumerDeltaKind, DagDiff, DeltaKind, NodeDelta, SourceDeltaKind};
use crate::error::{AqueductError, Result};

/// A view assignment for a blue/green consumer view swap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewAssignment {
    /// The stable consumer view name (e.g., `public.api_orders`).
    pub view_name: QualifiedName,
    /// The new target table in the green schema.
    pub target_table: QualifiedName,
    /// Optional SQL projection/filter (None = SELECT *).
    pub sql_body: Option<String>,
}

/// A single migration step in a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "step_type")]
pub enum PlanStep {
    LockDag {
        project: String,
        ttl: String,
    },
    ValidateQuery {
        name: String,
        query: String,
    },
    /// Base-table DDL for Tier 1 stream-adjacent columns.
    AlterBaseTable {
        name: QualifiedName,
        statement: String,
    },
    CreateStreamTable {
        spec: StreamTableSpec,
    },
    AlterStreamTable {
        name: QualifiedName,
        schedule: Option<String>,
        refresh_mode: Option<String>,
        cdc_mode: Option<String>,
        new_query: Option<String>,
    },
    DropStreamTable {
        name: QualifiedName,
        cascade: bool,
    },
    Backfill {
        name: QualifiedName,
        mode: String,
    },
    RecordSnapshot {
        version: u64,
    },
    UnlockDag {
        force: bool,
    },

    // ── v0.3: Blue/Green steps ────────────────────────────────────────────────
    /// Create the green versioned schema (`{project}__v{version}`).
    CreateGreenSchema {
        schema: String,
    },
    /// Create a stream table node inside the green schema.
    /// The spec's qualified_name.schema already points to the green schema.
    CreateStreamTableInGreen {
        spec: StreamTableSpec,
        green_schema: String,
    },
    /// Wait for the green DAG nodes to converge (data lag within threshold).
    WaitForConvergence {
        green_schema: String,
        node_names: Vec<String>,
        /// Maximum seconds to wait before failing.
        max_wait_secs: u64,
    },
    /// Atomically swap all consumer views from blue to green schema.
    SwapConsumerViews {
        assignments: Vec<ViewAssignment>,
        blue_schema: String,
        green_schema: String,
    },
    /// Schedule the blue schema for retirement after a TTL.
    RetireBlueSchema {
        schema: String,
        /// Seconds to retain the blue schema before dropping.
        retain_secs: u64,
    },

    // ── v0.3: Consumer view steps ──────────────────────────────────────────────
    /// Create or replace a consumer view.
    ManageConsumerView {
        spec: ConsumerSpec,
        /// "create", "alter", or "drop".
        action: String,
    },

    // ── v0.9: Missing plan step variants ──────────────────────────────────────
    /// Restore RLS / Row Security Policies lost during a Rebuild-class migration.
    RecreatePolicy {
        name: QualifiedName,
        policy_sql: String,
    },

    /// Unhook a pg_tide outbox attachment before a stream table is dropped.
    DetachOutbox {
        stream_table: QualifiedName,
        outbox_name: String,
    },

    /// Restore an outbox attachment after the stream table is recreated.
    ReattachOutbox {
        stream_table: QualifiedName,
        outbox_name: String,
        retention_hours: u32,
    },

    /// Drop or recreate the logical replication slot for a cdc_mode='wal' table.
    ManageWalSlot {
        stream_table: QualifiedName,
        /// "drop" | "create"
        action: String,
    },

    /// Temporarily switch an IMMEDIATE mode stream table to DIFFERENTIAL.
    PauseImmediate {
        name: QualifiedName,
    },

    /// Switch the stream table back to IMMEDIATE mode after a Rebuild.
    ResumeImmediate {
        name: QualifiedName,
    },

    /// Poll pgtrickle.pgt_stream_tables until refresh_status transitions to 'idle'.
    WaitForRefresh {
        name: QualifiedName,
        deadline_secs: u64,
    },

    /// Execute a user-defined SQL statement as a pre or post migration hook.
    RunHook {
        hook_name: String,
        statement: String,
    },
}

impl PlanStep {
    pub fn description(&self) -> String {
        match self {
            PlanStep::LockDag { project, .. } => {
                format!("Acquire lock for project '{}'", project)
            }
            PlanStep::ValidateQuery { name, .. } => {
                format!("Validate query for '{}'", name)
            }
            PlanStep::AlterBaseTable { name, .. } => {
                format!("ALTER base table '{}'", name)
            }
            PlanStep::CreateStreamTable { spec } => {
                format!("CREATE stream table '{}'", spec.qualified_name)
            }
            PlanStep::AlterStreamTable { name, .. } => {
                format!("ALTER stream table '{}'", name)
            }
            PlanStep::DropStreamTable { name, .. } => {
                format!("DROP stream table '{}'", name)
            }
            PlanStep::Backfill { name, mode } => {
                format!("BACKFILL '{}' ({})", name, mode)
            }
            PlanStep::RecordSnapshot { version } => {
                format!("Record DAG version {}", version)
            }
            PlanStep::UnlockDag { .. } => "Release lock".to_string(),
            PlanStep::CreateGreenSchema { schema } => {
                format!("CREATE SCHEMA '{}' (green)", schema)
            }
            PlanStep::CreateStreamTableInGreen { spec, green_schema } => {
                format!(
                    "CREATE stream table '{}' in green schema '{}'",
                    spec.qualified_name, green_schema
                )
            }
            PlanStep::WaitForConvergence { green_schema, .. } => {
                format!("Wait for green schema '{}' to converge", green_schema)
            }
            PlanStep::SwapConsumerViews { green_schema, .. } => {
                format!("Swap consumer views → '{}'", green_schema)
            }
            PlanStep::RetireBlueSchema {
                schema,
                retain_secs,
            } => {
                format!("Retire blue schema '{}' (after {}s)", schema, retain_secs)
            }
            PlanStep::ManageConsumerView { spec, action } => {
                format!(
                    "{} consumer view '{}' → '{}'",
                    action.to_uppercase(),
                    spec.expose_as,
                    spec.source
                )
            }
            PlanStep::RecreatePolicy { name, .. } => {
                format!("RECREATE policy on '{}'", name)
            }
            PlanStep::DetachOutbox {
                stream_table,
                outbox_name,
            } => {
                format!("DETACH outbox '{}' from '{}'", outbox_name, stream_table)
            }
            PlanStep::ReattachOutbox {
                stream_table,
                outbox_name,
                ..
            } => {
                format!("REATTACH outbox '{}' to '{}'", outbox_name, stream_table)
            }
            PlanStep::ManageWalSlot {
                stream_table,
                action,
            } => {
                format!("{} WAL slot for '{}'", action.to_uppercase(), stream_table)
            }
            PlanStep::PauseImmediate { name } => {
                format!("PAUSE IMMEDIATE mode for '{}'", name)
            }
            PlanStep::ResumeImmediate { name } => {
                format!("RESUME IMMEDIATE mode for '{}'", name)
            }
            PlanStep::WaitForRefresh {
                name,
                deadline_secs,
            } => {
                format!("WAIT for '{}' refresh (deadline {}s)", name, deadline_secs)
            }
            PlanStep::RunHook { hook_name, .. } => {
                format!("RUN hook '{}'", hook_name)
            }
        }
    }
}

/// A complete migration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub project: String,
    pub from_version: Option<u64>,
    pub to_version: u64,
    pub steps: Vec<PlanStep>,
    pub summary: PlanSummary,
    pub format_version: u32,
    pub created_at: DateTime<Utc>,
}

/// Summary statistics for a plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanSummary {
    pub creates: usize,
    pub drops: usize,
    pub alters: usize,
    pub free_count: usize,
    pub in_place_count: usize,
    pub rebuild_count: usize,
    pub blue_green_count: usize,
    pub changes: Vec<PlanChange>,
}

impl PlanSummary {
    pub fn is_empty(&self) -> bool {
        self.creates == 0 && self.drops == 0 && self.alters == 0
    }
}

/// A human-readable description of a single change in the plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanChange {
    pub symbol: String,
    pub name: String,
    pub class: String,
    pub description: String,
}

/// Rank a MigrationClass for comparison: higher = more disruptive.
fn class_rank(c: &MigrationClass) -> u8 {
    match c {
        MigrationClass::Free => 0,
        MigrationClass::InPlace => 1,
        MigrationClass::Rebuild => 2,
        MigrationClass::BlueGreen => 3,
    }
}

/// After per-node classification, detect diamond groups (convergence nodes with
/// in-degree ≥ 2 among the classified nodes) and promote all ancestors transitively
/// to the highest migration class in the group.
fn apply_diamond_consistency_promotion(
    classifications: &mut std::collections::HashMap<QualifiedName, MigrationClass>,
    ordered_deltas: &[&NodeDelta],
) {
    // Compute in-degree for each classified node (number of direct upstream classified parents).
    // A "convergence node" has ≥ 2 direct upstream parents (data flow merges into it).
    let mut in_degree: std::collections::HashMap<QualifiedName, usize> =
        std::collections::HashMap::new();
    for n in classifications.keys() {
        in_degree.entry(n.clone()).or_insert(0);
    }
    for delta in ordered_deltas {
        if !classifications.contains_key(&delta.qualified_name) {
            continue;
        }
        if let Some(spec) = &delta.desired {
            // Count how many direct upstream dependencies this node has that are classified.
            let upstream_count = spec
                .depends_on
                .iter()
                .filter(|dep| classifications.contains_key(*dep))
                .count();
            if upstream_count > 0 {
                *in_degree.entry(delta.qualified_name.clone()).or_insert(0) = upstream_count;
            }
        }
    }

    // Convergence nodes have in-degree >= 2.
    let convergence_nodes: Vec<QualifiedName> = in_degree
        .iter()
        .filter(|(_, &deg)| deg >= 2)
        .map(|(n, _)| n.clone())
        .collect();

    for conv_node in convergence_nodes {
        // Collect the diamond group: conv_node + all its transitive ancestors
        // within classified nodes.
        let mut group: std::collections::HashSet<QualifiedName> = std::collections::HashSet::new();
        let mut stack = vec![conv_node.clone()];
        while let Some(n) = stack.pop() {
            if group.insert(n.clone()) {
                // Find which delta has this name and add its depends_on.
                if let Some(delta) = ordered_deltas.iter().find(|d| d.qualified_name == n) {
                    if let Some(spec) = &delta.desired {
                        for dep in &spec.depends_on {
                            if classifications.contains_key(dep) {
                                stack.push(dep.clone());
                            }
                        }
                    }
                }
            }
        }

        // Find the highest class in the group.
        let max_class = group
            .iter()
            .filter_map(|n| classifications.get(n))
            .max_by_key(|c| class_rank(c))
            .copied()
            .unwrap_or(MigrationClass::Free);

        // Promote all members of the group to the maximum class.
        for n in &group {
            if let Some(c) = classifications.get_mut(n) {
                if class_rank(c) < class_rank(&max_class) {
                    *c = max_class;
                }
            }
        }
    }
}

/// Build a Plan from a DagDiff.
pub fn build_plan(
    project: &str,
    from_version: Option<u64>,
    next_version: u64,
    diff: &DagDiff,
    topo_order: &[QualifiedName],
) -> Result<Plan> {
    use crate::classifier::classify_delta;

    let mut steps: Vec<PlanStep> = Vec::new();
    let mut summary = PlanSummary::default();

    // Lock step is always first.
    steps.push(PlanStep::LockDag {
        project: project.to_string(),
        ttl: "30s".to_string(),
    });

    // ── Source (base table) changes come first so the cascade steps see the
    // already-altered schema when they execute. ─────────────────────────────
    for source_delta in diff.source_changes() {
        if source_delta.kind == SourceDeltaKind::Unchanged {
            continue;
        }

        if let Some(ddl) = &source_delta.desired_ddl {
            steps.push(PlanStep::AlterBaseTable {
                name: source_delta.qualified_name.clone(),
                statement: ddl.clone(),
            });

            summary.alters += 1;
            summary.rebuild_count += source_delta.cascade_impacts.len();
            summary.changes.push(PlanChange {
                symbol: "~".to_string(),
                name: source_delta.qualified_name.to_string(),
                class: "rebuild".to_string(),
                description: format!(
                    "base table DDL changed; {} stream table{} affected",
                    source_delta.cascade_impacts.len(),
                    if source_delta.cascade_impacts.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                ),
            });

            // Emit cascade rebuild for each impacted stream table.
            for impact in &source_delta.cascade_impacts {
                // Find the desired spec so we can recreate it correctly.
                if let Some(stream_desired) = topo_order
                    .iter()
                    .find(|n| **n == impact.stream_table)
                    .and_then(|name| {
                        diff.deltas
                            .iter()
                            .find(|d| d.qualified_name == *name)
                            .and_then(|d| d.desired.as_ref())
                    })
                {
                    steps.push(PlanStep::ValidateQuery {
                        name: stream_desired.qualified_name.to_string(),
                        query: stream_desired.query.clone(),
                    });
                    steps.push(PlanStep::DropStreamTable {
                        name: stream_desired.qualified_name.clone(),
                        cascade: false,
                    });
                    steps.push(PlanStep::CreateStreamTable {
                        spec: stream_desired.clone(),
                    });
                    steps.push(PlanStep::Backfill {
                        name: stream_desired.qualified_name.clone(),
                        mode: "FULL".to_string(),
                    });
                }
            }
        }
    }

    // Sort deltas by topological order.
    let ordered_deltas = order_deltas(diff, topo_order);

    // Pre-classify all non-cascaded, non-Unchanged deltas, then apply diamond promotion.
    let mut classifications: std::collections::HashMap<QualifiedName, MigrationClass> =
        std::collections::HashMap::new();
    for delta in &ordered_deltas {
        if delta.kind == DeltaKind::Unchanged {
            continue;
        }
        let already_cascaded = diff.source_changes().iter().any(|s| {
            s.cascade_impacts
                .iter()
                .any(|i| i.stream_table == delta.qualified_name)
        });
        if already_cascaded {
            continue;
        }
        classifications.insert(delta.qualified_name.clone(), classify_delta(delta));
    }
    apply_diamond_consistency_promotion(&mut classifications, &ordered_deltas);

    for delta in &ordered_deltas {
        if delta.kind == DeltaKind::Unchanged {
            continue;
        }

        // Skip stream tables that were already handled as cascade impacts.
        let already_cascaded = diff.source_changes().iter().any(|s| {
            s.cascade_impacts
                .iter()
                .any(|i| i.stream_table == delta.qualified_name)
        });
        if already_cascaded {
            continue;
        }

        let class = classifications
            .get(&delta.qualified_name)
            .copied()
            .unwrap_or_else(|| classify_delta(delta));
        let change_symbol = match &delta.kind {
            DeltaKind::Create => "+",
            DeltaKind::Drop => "-",
            _ => "~",
        };

        // Generate the plan steps for this delta.
        match &delta.kind {
            DeltaKind::Create => {
                let spec =
                    delta
                        .desired
                        .as_ref()
                        .ok_or_else(|| AqueductError::InvariantViolation {
                            context: format!(
                                "Create delta for '{}' has no desired spec",
                                delta.qualified_name
                            ),
                        })?;

                // Validate query first.
                if !spec.query.is_empty() {
                    steps.push(PlanStep::ValidateQuery {
                        name: spec.qualified_name.to_string(),
                        query: spec.query.clone(),
                    });
                }

                steps.push(PlanStep::CreateStreamTable { spec: spec.clone() });
                steps.push(PlanStep::Backfill {
                    name: spec.qualified_name.clone(),
                    mode: spec.refresh_mode.to_string(),
                });

                summary.creates += 1;
                summary.rebuild_count += 1;
                summary.changes.push(PlanChange {
                    symbol: change_symbol.to_string(),
                    name: delta.qualified_name.to_string(),
                    class: class.to_string(),
                    description: "new stream table".to_string(),
                });
            }
            DeltaKind::Drop => {
                steps.push(PlanStep::DropStreamTable {
                    name: delta.qualified_name.clone(),
                    cascade: false,
                });
                summary.drops += 1;
                summary.rebuild_count += 1;
                summary.changes.push(PlanChange {
                    symbol: change_symbol.to_string(),
                    name: delta.qualified_name.to_string(),
                    class: class.to_string(),
                    description: "drop stream table".to_string(),
                });
            }
            DeltaKind::AlterSchedule
            | DeltaKind::AlterRefreshMode
            | DeltaKind::AlterCdcMode
            | DeltaKind::AlterMetadata => {
                let desired =
                    delta
                        .desired
                        .as_ref()
                        .ok_or_else(|| AqueductError::InvariantViolation {
                            context: format!(
                                "Alter delta for '{}' has no desired spec",
                                delta.qualified_name
                            ),
                        })?;
                let actual =
                    delta
                        .actual
                        .as_ref()
                        .ok_or_else(|| AqueductError::InvariantViolation {
                            context: format!(
                                "Alter delta for '{}' has no actual spec",
                                delta.qualified_name
                            ),
                        })?;

                // For FULL→DIFF refresh_mode change, the classifier returns Rebuild.
                if class == MigrationClass::Rebuild {
                    // Must drop + recreate to establish delta-tracking state.
                    if !desired.query.is_empty() {
                        steps.push(PlanStep::ValidateQuery {
                            name: delta.qualified_name.to_string(),
                            query: desired.query.clone(),
                        });
                    }
                    steps.push(PlanStep::DropStreamTable {
                        name: delta.qualified_name.clone(),
                        cascade: false,
                    });
                    steps.push(PlanStep::CreateStreamTable {
                        spec: desired.clone(),
                    });
                    steps.push(PlanStep::Backfill {
                        name: delta.qualified_name.clone(),
                        mode: "FULL".to_string(),
                    });
                    summary.alters += 1;
                    summary.rebuild_count += 1;
                } else {
                    steps.push(PlanStep::AlterStreamTable {
                        name: delta.qualified_name.clone(),
                        schedule: if desired.schedule != actual.schedule {
                            Some(desired.schedule.clone())
                        } else {
                            None
                        },
                        refresh_mode: if desired.refresh_mode != actual.refresh_mode {
                            Some(desired.refresh_mode.to_string())
                        } else {
                            None
                        },
                        cdc_mode: if desired.cdc_mode != actual.cdc_mode {
                            desired.cdc_mode.clone()
                        } else {
                            None
                        },
                        new_query: None,
                    });
                    summary.alters += 1;
                    summary.free_count += 1;
                }

                summary.changes.push(PlanChange {
                    symbol: change_symbol.to_string(),
                    name: delta.qualified_name.to_string(),
                    class: class.to_string(),
                    description: format!("{:?} change", delta.kind),
                });
            }
            DeltaKind::AlterQuery => {
                let desired =
                    delta
                        .desired
                        .as_ref()
                        .ok_or_else(|| AqueductError::InvariantViolation {
                            context: format!(
                                "AlterQuery delta for '{}' has no desired spec",
                                delta.qualified_name
                            ),
                        })?;
                let _actual =
                    delta
                        .actual
                        .as_ref()
                        .ok_or_else(|| AqueductError::InvariantViolation {
                            context: format!(
                                "AlterQuery delta for '{}' has no actual spec",
                                delta.qualified_name
                            ),
                        })?;

                if !desired.query.is_empty() {
                    steps.push(PlanStep::ValidateQuery {
                        name: delta.qualified_name.to_string(),
                        query: desired.query.clone(),
                    });
                }

                match class {
                    MigrationClass::InPlace => {
                        steps.push(PlanStep::AlterStreamTable {
                            name: delta.qualified_name.clone(),
                            schedule: None,
                            refresh_mode: None,
                            cdc_mode: None,
                            new_query: Some(desired.query.clone()),
                        });
                        steps.push(PlanStep::Backfill {
                            name: delta.qualified_name.clone(),
                            mode: "INCREMENTAL".to_string(),
                        });
                        summary.alters += 1;
                        summary.in_place_count += 1;
                    }
                    _ => {
                        // Rebuild: drop + recreate.
                        steps.push(PlanStep::DropStreamTable {
                            name: delta.qualified_name.clone(),
                            cascade: false,
                        });
                        steps.push(PlanStep::CreateStreamTable {
                            spec: desired.clone(),
                        });
                        steps.push(PlanStep::Backfill {
                            name: delta.qualified_name.clone(),
                            mode: "FULL".to_string(),
                        });
                        summary.alters += 1;
                        summary.rebuild_count += 1;
                    }
                }

                summary.changes.push(PlanChange {
                    symbol: change_symbol.to_string(),
                    name: delta.qualified_name.to_string(),
                    class: class.to_string(),
                    description: "query changed".to_string(),
                });
            }
            DeltaKind::Unchanged => {}
        }
    }

    // Always record snapshot and unlock.
    steps.push(PlanStep::RecordSnapshot {
        version: next_version,
    });
    steps.push(PlanStep::UnlockDag { force: false });

    // ── Consumer view steps (after stream table steps, before snapshot) ──────
    // Insert consumer view steps just before RecordSnapshot.
    let unlock_idx = steps.len() - 1;
    let snapshot_idx = steps.len() - 2;

    let mut consumer_steps: Vec<PlanStep> = Vec::new();
    for consumer_delta in diff.consumer_changes() {
        let action = match consumer_delta.kind {
            ConsumerDeltaKind::Create => "create",
            ConsumerDeltaKind::Alter => "alter",
            ConsumerDeltaKind::Drop => "drop",
            ConsumerDeltaKind::Unchanged => continue,
        };
        if let Some(spec) = &consumer_delta.desired {
            consumer_steps.push(PlanStep::ManageConsumerView {
                spec: spec.clone(),
                action: action.to_string(),
            });
            summary.changes.push(PlanChange {
                symbol: match action {
                    "create" => "+",
                    "drop" => "-",
                    _ => "~",
                }
                .to_string(),
                name: consumer_delta.expose_as.to_string(),
                class: "consumer-view".to_string(),
                description: format!("{} consumer view", action),
            });
        } else if action == "drop" {
            // Drop: no desired spec, build a placeholder.
            consumer_steps.push(PlanStep::ManageConsumerView {
                spec: crate::dag::ConsumerSpec {
                    name: consumer_delta.expose_as.name.clone(),
                    source: consumer_delta
                        .actual_source
                        .clone()
                        .unwrap_or_else(|| consumer_delta.expose_as.clone()),
                    expose_as: consumer_delta.expose_as.clone(),
                    sql_body: None,
                },
                action: "drop".to_string(),
            });
            summary.changes.push(PlanChange {
                symbol: "-".to_string(),
                name: consumer_delta.expose_as.to_string(),
                class: "consumer-view".to_string(),
                description: "drop consumer view".to_string(),
            });
        }
    }

    // Insert consumer steps before RecordSnapshot.
    let insert_at = snapshot_idx.min(unlock_idx);
    for (i, cs) in consumer_steps.into_iter().enumerate() {
        steps.insert(insert_at + i, cs);
    }

    Ok(Plan {
        project: project.to_string(),
        from_version,
        to_version: next_version,
        steps,
        summary,
        format_version: 1,
        created_at: Utc::now(),
    })
}

fn order_deltas<'a>(diff: &'a DagDiff, topo_order: &[QualifiedName]) -> Vec<&'a NodeDelta> {
    let mut ordered: Vec<&NodeDelta> = Vec::new();

    // First, add deltas in topological order.
    for name in topo_order {
        if let Some(delta) = diff.deltas.iter().find(|d| &d.qualified_name == name) {
            ordered.push(delta);
        }
    }

    // Then, add any remaining deltas not in the topo order (e.g., drops of removed tables).
    for delta in &diff.deltas {
        if !ordered
            .iter()
            .any(|d| d.qualified_name == delta.qualified_name)
        {
            ordered.push(delta);
        }
    }

    ordered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use crate::diff::{DagDiff, DeltaKind, NodeDelta};

    fn make_create_delta(name: &str) -> NodeDelta {
        NodeDelta {
            qualified_name: QualifiedName::new("public", name),
            kind: DeltaKind::Create,
            desired: Some(StreamTableSpec {
                qualified_name: QualifiedName::new("public", name),
                query: "SELECT 1".to_string(),
                refresh_mode: RefreshMode::Differential,
                schedule: "30s".to_string(),
                cdc_mode: None,
                explicit_depends_on: vec![],
                depends_on: vec![],
                cypher_source: None,
            }),
            actual: None,
        }
    }

    #[test]
    fn test_build_plan_create() {
        let diff = DagDiff {
            deltas: vec![make_create_delta("order_totals")],
            source_deltas: vec![],
            consumer_deltas: vec![],
        };
        let topo = vec![QualifiedName::new("public", "order_totals")];
        let plan = build_plan("test-project", Some(0), 1, &diff, &topo).expect("build plan");

        assert_eq!(plan.project, "test-project");
        assert_eq!(plan.to_version, 1);
        assert_eq!(plan.summary.creates, 1);

        // Should have: Lock, ValidateQuery, Create, Backfill, RecordSnapshot, Unlock
        assert!(plan.steps.len() >= 4);
    }

    #[test]
    fn test_build_plan_empty() {
        let diff = DagDiff::default();
        let plan = build_plan("test-project", Some(5), 6, &diff, &[]).expect("build plan");
        assert!(plan.summary.is_empty());
        // Lock + RecordSnapshot + Unlock
        assert_eq!(plan.steps.len(), 3);
    }

    // ── v0.9 PlanStep variant description tests ───────────────────────────────

    #[test]
    fn test_recreate_policy_description() {
        let step = PlanStep::RecreatePolicy {
            name: QualifiedName::new("public", "orders"),
            policy_sql: "CREATE POLICY p ON public.orders".to_string(),
        };
        assert!(step.description().contains("orders"));
    }

    #[test]
    fn test_detach_outbox_description() {
        let step = PlanStep::DetachOutbox {
            stream_table: QualifiedName::new("public", "orders"),
            outbox_name: "orders_outbox".to_string(),
        };
        assert!(step.description().contains("orders_outbox"));
    }

    #[test]
    fn test_reattach_outbox_description() {
        let step = PlanStep::ReattachOutbox {
            stream_table: QualifiedName::new("public", "orders"),
            outbox_name: "orders_outbox".to_string(),
            retention_hours: 24,
        };
        assert!(step.description().contains("orders_outbox"));
    }

    #[test]
    fn test_manage_wal_slot_description() {
        let step = PlanStep::ManageWalSlot {
            stream_table: QualifiedName::new("public", "events"),
            action: "drop".to_string(),
        };
        let desc = step.description();
        assert!(
            desc.contains("DROP") || desc.contains("drop") || desc.to_uppercase().contains("DROP")
        );
        assert!(desc.contains("events"));
    }

    #[test]
    fn test_pause_immediate_description() {
        let step = PlanStep::PauseImmediate {
            name: QualifiedName::new("public", "orders"),
        };
        assert!(step.description().contains("orders"));
    }

    #[test]
    fn test_resume_immediate_description() {
        let step = PlanStep::ResumeImmediate {
            name: QualifiedName::new("public", "orders"),
        };
        assert!(step.description().contains("orders"));
    }

    #[test]
    fn test_wait_for_refresh_description() {
        let step = PlanStep::WaitForRefresh {
            name: QualifiedName::new("public", "events"),
            deadline_secs: 120,
        };
        let desc = step.description();
        assert!(desc.contains("events"));
        assert!(desc.contains("120"));
    }

    #[test]
    fn test_run_hook_description() {
        let step = PlanStep::RunHook {
            hook_name: "post_migration".to_string(),
            statement: "ANALYZE;".to_string(),
        };
        assert!(step.description().contains("post_migration"));
    }
}
