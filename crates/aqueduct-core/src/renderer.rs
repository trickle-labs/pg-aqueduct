use crate::plan::Plan;

/// Render a plan as human-readable terminal text.
pub fn render_plan_text(
    plan: &Plan,
    pg_version: Option<&str>,
    pgtrickle_version: Option<&str>,
) -> String {
    let mut out = String::new();

    let from = plan
        .from_version
        .map(|v| format!("v{}", v))
        .unwrap_or_else(|| "(new)".to_string());

    out.push_str(&format!(
        "Project  {}  {} → v{}\n",
        plan.project, from, plan.to_version
    ));

    if let (Some(pgt), Some(pg)) = (pgtrickle_version, pg_version) {
        out.push_str(&format!("Target   (pg_trickle {}, pg {})\n", pgt, pg));
    }

    out.push('\n');

    if plan.summary.is_empty() {
        out.push_str("No changes detected. Plan is a no-op.\n");
        return out;
    }

    let total_changes = plan.summary.creates + plan.summary.drops + plan.summary.alters;
    out.push_str(&format!(
        "Changes  {} node{} affected\n\n",
        total_changes,
        if total_changes == 1 { "" } else { "s" }
    ));

    for change in &plan.summary.changes {
        out.push_str(&format!(
            "  {} {:30}  [{:10}]  {}\n",
            change.symbol, change.name, change.class, change.description
        ));
    }

    out.push('\n');

    // Step table.
    if !plan.summary.changes.is_empty() {
        out.push_str(&format!(
            "  {:<35} {:<15} {:<10}\n",
            "Step", "Rows (est)", "Class"
        ));
        out.push_str(&format!("  {}\n", "─".repeat(65)));

        for change in &plan.summary.changes {
            let rows = "—";
            out.push_str(&format!(
                "  {:<35} {:<15} {:<10}\n",
                change.description, rows, change.class
            ));
        }
    }

    // Cost warnings.
    if plan.summary.rebuild_count > 0 {
        out.push('\n');
        out.push_str(&format!(
            "  ⚠  {} step{} will trigger a FULL refresh.\n",
            plan.summary.rebuild_count,
            if plan.summary.rebuild_count == 1 {
                ""
            } else {
                "s"
            }
        ));
    }

    out
}

/// Render a plan as JSON.
pub fn render_plan_json(plan: &Plan) -> String {
    serde_json::to_string_pretty(plan).unwrap_or_else(|_| "{}".to_string())
}

/// Render a plan as Markdown (for CI PR comments).
pub fn render_plan_markdown(plan: &Plan) -> String {
    let mut out = String::new();

    let from = plan
        .from_version
        .map(|v| format!("v{}", v))
        .unwrap_or_else(|| "(new)".to_string());

    out.push_str("## aqueduct plan\n\n");
    out.push_str(&format!(
        "**Project:** `{}`  **Version:** {} → v{}\n\n",
        plan.project, from, plan.to_version
    ));

    if plan.summary.is_empty() {
        out.push_str("_No changes detected. Plan is a no-op._\n");
        return out;
    }

    let total = plan.summary.creates + plan.summary.drops + plan.summary.alters;
    out.push_str(&format!(
        "**{} change{}**\n\n",
        total,
        if total == 1 { "" } else { "s" }
    ));

    out.push_str("| Symbol | Table | Class | Description |\n");
    out.push_str("|--------|-------|-------|-------------|\n");

    for change in &plan.summary.changes {
        out.push_str(&format!(
            "| `{}` | `{}` | {} | {} |\n",
            change.symbol, change.name, change.class, change.description
        ));
    }

    if plan.summary.rebuild_count > 0 {
        out.push_str(&format!(
            "\n> ⚠️ {} rebuild step{} will trigger a FULL refresh.\n",
            plan.summary.rebuild_count,
            if plan.summary.rebuild_count == 1 {
                ""
            } else {
                "s"
            }
        ));
    }

    out
}

/// Status report for `aqueduct status`.
#[derive(Debug, Default)]
pub struct StatusReport {
    pub project: String,
    pub current_version: Option<u64>,
    pub applied_at: Option<chrono::DateTime<chrono::Utc>>,
    pub applied_by: Option<String>,
    pub stream_table_count: i64,
    pub drift_count: usize,
    pub pgtrickle_version: Option<String>,
    pub pg_version: Option<String>,
}

pub fn render_status_text(status: &StatusReport) -> String {
    let mut out = String::new();

    out.push_str(&format!("Project   {}\n", status.project));

    if let (Some(pgt), Some(pg)) = (&status.pgtrickle_version, &status.pg_version) {
        out.push_str(&format!("Database  (pg_trickle {}, pg {})\n", pgt, pg));
    }

    out.push('\n');

    let version_str = status
        .current_version
        .map(|v| format!("v{}", v))
        .unwrap_or_else(|| "not initialised".to_string());

    let timing_str = status
        .applied_at
        .map(|t| {
            let ago = chrono::Utc::now().signed_duration_since(t);
            if ago.num_seconds() < 60 {
                format!("{} seconds ago", ago.num_seconds())
            } else if ago.num_minutes() < 60 {
                format!("{} minutes ago", ago.num_minutes())
            } else {
                format!("{} hours ago", ago.num_hours())
            }
        })
        .unwrap_or_else(|| "—".to_string());

    out.push_str(&format!(
        "Version   {}   applied {} by {}\n",
        version_str,
        timing_str,
        status.applied_by.as_deref().unwrap_or("—")
    ));

    out.push('\n');
    out.push_str(&format!(
        "Stream tables  {} managed\n",
        status.stream_table_count
    ));

    if status.drift_count > 0 {
        out.push_str(&format!(
            "Drift          {} table{} diverged from recorded state\n",
            status.drift_count,
            if status.drift_count == 1 { "" } else { "s" }
        ));
        out.push_str("               Run `aqueduct plan` to see the full cascade.\n");
    } else {
        out.push_str("Drift          none detected\n");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::diff::DagDiff;
    use crate::plan::build_plan;

    fn make_empty_plan() -> Plan {
        build_plan("test-project", Some(1), 2, &DagDiff::default(), &[])
    }

    #[test]
    fn test_render_empty_plan_text() {
        let plan = make_empty_plan();
        let text = render_plan_text(&plan, None, None);
        assert!(text.contains("no-op"));
    }

    #[test]
    fn test_render_plan_json() {
        let plan = make_empty_plan();
        let json = render_plan_json(&plan);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["project"], "test-project");
    }

    #[test]
    fn test_render_plan_markdown() {
        let plan = make_empty_plan();
        let md = render_plan_markdown(&plan);
        assert!(md.contains("## aqueduct plan"));
        assert!(md.contains("no-op"));
    }

    #[test]
    fn test_render_status() {
        let status = StatusReport {
            project: "checkout-analytics".to_string(),
            current_version: Some(18),
            stream_table_count: 22,
            drift_count: 0,
            pgtrickle_version: Some("0.62".to_string()),
            pg_version: Some("18.3".to_string()),
            ..Default::default()
        };
        let text = render_status_text(&status);
        assert!(text.contains("checkout-analytics"));
        assert!(text.contains("v18"));
        assert!(text.contains("22 managed"));
        assert!(text.contains("none detected"));
    }
}
