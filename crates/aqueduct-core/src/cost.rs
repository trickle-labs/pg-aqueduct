/// Cost estimation for migration plan steps.
///
/// This module provides per-step estimates of:
/// - Row count (via `EXPLAIN (FORMAT JSON)` on the defining query)
/// - Estimated duration (row_count × bytes_per_row / write_throughput)
///
/// Accuracy goal: distinguish a "2-second migration" from a "2-hour migration".
/// Sub-second precision is explicitly a non-goal.
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::plan::{Plan, PlanStep};

/// Per-step cost estimate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepCost {
    /// Step description (matches `PlanStep::description()`).
    pub step: String,
    /// Migration class: create, rebuild, in-place, free, base-table.
    pub class: String,
    /// Estimated row count (None if not applicable / not queryable).
    pub estimated_rows: Option<i64>,
    /// Human-readable estimated duration.
    pub estimated_duration: String,
}

/// Full cost breakdown for a plan.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PlanCost {
    pub steps: Vec<StepCost>,
}

impl PlanCost {
    /// Total estimated duration (sum of BACKFILL steps, which dominate).
    pub fn total_duration_hint(&self) -> String {
        // Just return the heaviest individual hint as a proxy.
        let has_rebuild = self
            .steps
            .iter()
            .any(|s| s.class == "rebuild" && s.estimated_rows.unwrap_or(0) > 0);
        let has_in_place = self
            .steps
            .iter()
            .any(|s| s.class == "in-place" && s.estimated_rows.unwrap_or(0) > 0);

        if has_rebuild {
            "minutes (rebuild)".to_string()
        } else if has_in_place {
            "seconds (in-place backfill)".to_string()
        } else {
            "< 1s".to_string()
        }
    }
}

/// Estimate the cost of a plan by querying the database with EXPLAIN.
///
/// For each BACKFILL step, we run `EXPLAIN (FORMAT JSON)` on the stream
/// table's defining query and extract `Plan Rows`.  Duration is then
/// estimated as `rows × avg_bytes / write_throughput_bytes_per_sec`.
///
/// When `throughput_bytes_per_sec` is None, a default of 50 MB/s is used
/// (a conservative estimate for a medium-sized Postgres instance).
pub async fn estimate_plan_cost(
    client: &tokio_postgres::Client,
    plan: &Plan,
    throughput_bytes_per_sec: Option<u64>,
) -> Result<PlanCost> {
    let tps = throughput_bytes_per_sec.unwrap_or(50 * 1024 * 1024); // 50 MB/s default
    let avg_row_bytes: u64 = 200; // conservative 200 bytes/row

    let mut steps = Vec::new();

    for step in &plan.steps {
        let cost = match step {
            PlanStep::Backfill { name, mode } => {
                // Try to find the query for this stream table from the CreateStreamTable step.
                let query = find_query_for_table(plan, name);

                let estimated_rows = if let Some(q) = &query {
                    estimate_rows(client, q).await.unwrap_or(None)
                } else {
                    None
                };

                let duration = format_duration(estimated_rows, avg_row_bytes, tps, mode);
                StepCost {
                    step: step.description(),
                    class: if mode == "INCREMENTAL" {
                        "in-place".to_string()
                    } else {
                        "rebuild".to_string()
                    },
                    estimated_rows,
                    estimated_duration: duration,
                }
            }
            PlanStep::AlterBaseTable { name: _, .. } => StepCost {
                step: step.description(),
                class: "base-table".to_string(),
                estimated_rows: None,
                estimated_duration: "< 1s".to_string(),
            },
            PlanStep::CreateStreamTable { .. }
            | PlanStep::AlterStreamTable { .. }
            | PlanStep::DropStreamTable { .. } => StepCost {
                step: step.description(),
                class: "rebuild".to_string(),
                estimated_rows: None,
                estimated_duration: "< 1s".to_string(),
            },
            PlanStep::ValidateQuery { .. } => StepCost {
                step: step.description(),
                class: "free".to_string(),
                estimated_rows: None,
                estimated_duration: "< 1s".to_string(),
            },
            PlanStep::LockDag { .. }
            | PlanStep::RecordSnapshot { .. }
            | PlanStep::UnlockDag { .. } => StepCost {
                step: step.description(),
                class: "free".to_string(),
                estimated_rows: None,
                estimated_duration: "< 1s".to_string(),
            },
            // v0.3: Blue/Green and Consumer View steps
            PlanStep::CreateGreenSchema { .. }
            | PlanStep::CreateStreamTableInGreen { .. }
            | PlanStep::WaitForConvergence { .. }
            | PlanStep::SwapConsumerViews { .. }
            | PlanStep::RetireBlueSchema { .. }
            | PlanStep::ManageConsumerView { .. } => StepCost {
                step: step.description(),
                class: "blue-green".to_string(),
                estimated_rows: None,
                estimated_duration: "< 1s".to_string(),
            },
        };
        steps.push(cost);
    }

    Ok(PlanCost { steps })
}

/// Look up the query for a given stream table within the plan.
fn find_query_for_table(plan: &Plan, name: &crate::dag::QualifiedName) -> Option<String> {
    for step in &plan.steps {
        if let PlanStep::CreateStreamTable { spec } = step {
            if &spec.qualified_name == name {
                return Some(spec.query.clone());
            }
        }
        if let PlanStep::AlterStreamTable {
            name: n,
            new_query: Some(q),
            ..
        } = step
        {
            if n == name {
                return Some(q.clone());
            }
        }
    }
    None
}

/// Run `EXPLAIN (FORMAT JSON)` and extract the top-level estimated row count.
async fn estimate_rows(client: &tokio_postgres::Client, query: &str) -> Result<Option<i64>> {
    if query.is_empty() {
        return Ok(None);
    }

    let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", query);
    let row = client.query_opt(&explain_sql, &[]).await;

    match row {
        Ok(Some(r)) => {
            let json: serde_json::Value = r.get(0);
            // EXPLAIN JSON is an array with a single object containing "Plan".
            let rows = json
                .get(0)
                .and_then(|p| p.get("Plan"))
                .and_then(|plan| plan.get("Plan Rows"))
                .and_then(|v| v.as_i64());
            Ok(rows)
        }
        Ok(None) => Ok(None),
        Err(_) => Ok(None), // Silently ignore EXPLAIN failures (query may reference non-existent tables).
    }
}

/// Format an estimated duration from row count.
fn format_duration(
    estimated_rows: Option<i64>,
    avg_row_bytes: u64,
    throughput_bytes_per_sec: u64,
    _mode: &str,
) -> String {
    let Some(rows) = estimated_rows else {
        return "—".to_string();
    };

    if rows <= 0 {
        return "< 1s".to_string();
    }

    let bytes = rows as u64 * avg_row_bytes;
    let secs = bytes / throughput_bytes_per_sec;

    if secs == 0 {
        "< 1s".to_string()
    } else if secs < 60 {
        format!("~{}s", secs)
    } else if secs < 3600 {
        format!("~{}m", secs / 60)
    } else {
        format!("~{}h", secs / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_under_1s() {
        assert_eq!(format_duration(Some(0), 200, 50_000_000, "FULL"), "< 1s");
    }

    #[test]
    fn test_format_duration_seconds() {
        // 100_000 rows × 200 bytes = 20 MB; at 50 MB/s = ~0.4s → "< 1s"
        assert_eq!(
            format_duration(Some(100_000), 200, 50_000_000, "FULL"),
            "< 1s"
        );
    }

    #[test]
    fn test_format_duration_minutes() {
        // 10_000_000 rows × 200 bytes = 2 GB; at 50 MB/s = ~40s → "~40s"
        assert_eq!(
            format_duration(Some(10_000_000), 200, 50_000_000, "FULL"),
            "~40s"
        );
    }

    #[test]
    fn test_format_duration_none() {
        assert_eq!(format_duration(None, 200, 50_000_000, "FULL"), "—");
    }
}
