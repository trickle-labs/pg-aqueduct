use serde::{Deserialize, Serialize};

use crate::dag::{DagState, QualifiedName, StreamTableSpec};

/// The kind of change for a single stream table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaKind {
    /// New stream table not present in actual state.
    Create,
    /// Stream table present in actual state but not in desired state.
    Drop,
    /// SQL query changed.
    AlterQuery,
    /// Schedule changed only.
    AlterSchedule,
    /// refresh_mode changed only.
    AlterRefreshMode,
    /// cdc_mode changed only.
    AlterCdcMode,
    /// Multiple metadata changes (schedule + refresh_mode etc.).
    AlterMetadata,
    /// No change.
    Unchanged,
}

/// A change to a single node in the stream-table DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDelta {
    pub qualified_name: QualifiedName,
    pub kind: DeltaKind,
    /// The desired spec (None for drops).
    pub desired: Option<StreamTableSpec>,
    /// The actual spec (None for creates).
    pub actual: Option<StreamTableSpec>,
}

/// The result of comparing desired vs. actual DAG state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DagDiff {
    pub deltas: Vec<NodeDelta>,
}

impl DagDiff {
    pub fn is_empty(&self) -> bool {
        self.deltas.iter().all(|d| d.kind == DeltaKind::Unchanged)
    }

    pub fn changes(&self) -> Vec<&NodeDelta> {
        self.deltas
            .iter()
            .filter(|d| d.kind != DeltaKind::Unchanged)
            .collect()
    }
}

/// Compare the desired DAG state (from migrations directory) against the actual state
/// (from the live database catalog).
pub fn compute_diff(desired: &DagState, actual: &DagState) -> DagDiff {
    let mut deltas = Vec::new();

    // Find creates and alters.
    for desired_table in &desired.stream_tables {
        let actual_table = actual.find_stream_table(&desired_table.qualified_name);

        let delta_kind = match actual_table {
            None => DeltaKind::Create,
            Some(actual) => classify_change(desired_table, actual),
        };

        deltas.push(NodeDelta {
            qualified_name: desired_table.qualified_name.clone(),
            kind: delta_kind,
            desired: Some(desired_table.clone()),
            actual: actual_table.cloned(),
        });
    }

    // Find drops: tables in actual but not in desired.
    for actual_table in &actual.stream_tables {
        let in_desired = desired
            .find_stream_table(&actual_table.qualified_name)
            .is_some();
        if !in_desired {
            deltas.push(NodeDelta {
                qualified_name: actual_table.qualified_name.clone(),
                kind: DeltaKind::Drop,
                desired: None,
                actual: Some(actual_table.clone()),
            });
        }
    }

    DagDiff { deltas }
}

fn classify_change(desired: &StreamTableSpec, actual: &StreamTableSpec) -> DeltaKind {
    let query_changed = normalise_sql(&desired.query) != normalise_sql(&actual.query);
    let schedule_changed = desired.schedule != actual.schedule;
    let refresh_mode_changed = desired.refresh_mode != actual.refresh_mode;
    let cdc_mode_changed = desired.cdc_mode != actual.cdc_mode;

    if query_changed {
        return DeltaKind::AlterQuery;
    }

    let metadata_changes = [schedule_changed, refresh_mode_changed, cdc_mode_changed]
        .iter()
        .filter(|&&c| c)
        .count();

    match metadata_changes {
        0 => DeltaKind::Unchanged,
        _ if schedule_changed && !refresh_mode_changed && !cdc_mode_changed => {
            DeltaKind::AlterSchedule
        }
        _ if refresh_mode_changed && !schedule_changed && !cdc_mode_changed => {
            DeltaKind::AlterRefreshMode
        }
        _ if cdc_mode_changed && !schedule_changed && !refresh_mode_changed => {
            DeltaKind::AlterCdcMode
        }
        _ => DeltaKind::AlterMetadata,
    }
}

/// Normalise SQL for comparison: collapse whitespace, uppercase keywords, strip trailing semicolons.
/// This is a simple normalisation — not a full AST comparison.
fn normalise_sql(sql: &str) -> String {
    // Collapse whitespace sequences to a single space.
    let re = regex::Regex::new(r"\s+").unwrap();
    let s = re.replace_all(sql.trim(), " ").to_string().to_lowercase();
    // Strip trailing semicolons (migration files include them; live catalog doesn't).
    s.trim_end_matches(';').trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{DagState, QualifiedName, RefreshMode, StreamTableSpec};

    fn make_spec(name: &str, query: &str, schedule: &str) -> StreamTableSpec {
        StreamTableSpec {
            qualified_name: QualifiedName::new("public", name),
            query: query.to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: schedule.to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
        }
    }

    #[test]
    fn test_diff_create() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
        };
        let actual = DagState::default();
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas.len(), 1);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Create);
    }

    #[test]
    fn test_diff_drop() {
        let desired = DagState::default();
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Drop);
    }

    #[test]
    fn test_diff_unchanged() {
        let tables = vec![make_spec("t1", "SELECT 1", "30s")];
        let desired = DagState {
            stream_tables: tables.clone(),
            sources: vec![],
        };
        let actual = DagState {
            stream_tables: tables,
            sources: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::Unchanged);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_alter_schedule() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "1m")],
            sources: vec![],
        };
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::AlterSchedule);
    }

    #[test]
    fn test_diff_alter_query() {
        let desired = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 2", "30s")],
            sources: vec![],
        };
        let actual = DagState {
            stream_tables: vec![make_spec("t1", "SELECT 1", "30s")],
            sources: vec![],
        };
        let diff = compute_diff(&desired, &actual);
        assert_eq!(diff.deltas[0].kind, DeltaKind::AlterQuery);
    }
}
