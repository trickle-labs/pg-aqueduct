use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::classifier::MigrationClass;
use crate::dag::{QualifiedName, StreamTableSpec};
use crate::diff::{DagDiff, DeltaKind, NodeDelta};

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

/// Build a Plan from a DagDiff.
pub fn build_plan(
    project: &str,
    from_version: Option<u64>,
    next_version: u64,
    diff: &DagDiff,
    topo_order: &[QualifiedName],
) -> Plan {
    use crate::classifier::classify_delta;

    let mut steps: Vec<PlanStep> = Vec::new();
    let mut summary = PlanSummary::default();

    // Lock step is always first.
    steps.push(PlanStep::LockDag {
        project: project.to_string(),
        ttl: "30s".to_string(),
    });

    // Sort deltas by topological order.
    let ordered_deltas = order_deltas(diff, topo_order);

    for delta in &ordered_deltas {
        if delta.kind == DeltaKind::Unchanged {
            continue;
        }

        let class = classify_delta(delta);
        let change_symbol = match &delta.kind {
            DeltaKind::Create => "+",
            DeltaKind::Drop => "-",
            _ => "~",
        };

        // Generate the plan steps for this delta.
        match &delta.kind {
            DeltaKind::Create => {
                let spec = delta.desired.as_ref().unwrap();

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
                let desired = delta.desired.as_ref().unwrap();
                let actual = delta.actual.as_ref().unwrap();

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
                summary.changes.push(PlanChange {
                    symbol: change_symbol.to_string(),
                    name: delta.qualified_name.to_string(),
                    class: class.to_string(),
                    description: format!("{:?} change", delta.kind),
                });
            }
            DeltaKind::AlterQuery => {
                let desired = delta.desired.as_ref().unwrap();
                let _actual = delta.actual.as_ref().unwrap();

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

    Plan {
        project: project.to_string(),
        from_version,
        to_version: next_version,
        steps,
        summary,
        format_version: 1,
        created_at: Utc::now(),
    }
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
            }),
            actual: None,
        }
    }

    #[test]
    fn test_build_plan_create() {
        let diff = DagDiff {
            deltas: vec![make_create_delta("order_totals")],
        };
        let topo = vec![QualifiedName::new("public", "order_totals")];
        let plan = build_plan("test-project", Some(0), 1, &diff, &topo);

        assert_eq!(plan.project, "test-project");
        assert_eq!(plan.to_version, 1);
        assert_eq!(plan.summary.creates, 1);

        // Should have: Lock, ValidateQuery, Create, Backfill, RecordSnapshot, Unlock
        assert!(plan.steps.len() >= 4);
    }

    #[test]
    fn test_build_plan_empty() {
        let diff = DagDiff::default();
        let plan = build_plan("test-project", Some(5), 6, &diff, &[]);
        assert!(plan.summary.is_empty());
        // Lock + RecordSnapshot + Unlock
        assert_eq!(plan.steps.len(), 3);
    }
}
