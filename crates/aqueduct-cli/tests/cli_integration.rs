/// CLI-level integration tests.
/// These tests spin up a Testcontainers PostgreSQL container, install the mock
/// pg_trickle schema, and exercise the CLI commands end-to-end.
use aqueduct_testkit::TestDb;
use std::fs;
use std::path::PathBuf;

// ── helpers ──────────────────────────────────────────────────────────────────

fn write_project(dir: &std::path::Path, project_name: &str, stream_files: &[(&str, &str)]) {
    fs::create_dir_all(dir.join("migrations").join("streams")).unwrap();

    let toml = format!(
        r#"[project]
name = "{}"

[targets.test]
dsn = "${{AQUEDUCT_TEST_DSN}}"

[apply]
lock_timeout = "30s"
allow_full_refresh = true
"#,
        project_name
    );
    fs::write(dir.join("aqueduct.toml"), toml).unwrap();

    for (name, content) in stream_files {
        fs::write(
            dir.join("migrations")
                .join("streams")
                .join(format!("{}.sql", name)),
            content,
        )
        .unwrap();
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Test: aqueduct validate passes on valid migration files.
#[tokio::test]
async fn test_validate_valid_files() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "validate-test",
        &[(
            "order_totals",
            "-- @aqueduct:schedule = \"30s\"\nSELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id;\n",
        )],
    );

    // Run validate via the library (mimicking CLI behaviour).
    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();

    let result = aqueduct_core::validate::validate_migration_files(&files);
    assert!(result.is_ok());
    assert!(result.warnings.is_empty());
}

/// Test: aqueduct validate fails on invalid SQL.
#[tokio::test]
async fn test_validate_invalid_sql() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "validate-test",
        &[(
            "bad_table",
            "-- @aqueduct:schedule = \"30s\"\n@@@ NOT VALID SQL @@@\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();

    let result = aqueduct_core::validate::validate_migration_files(&files);
    assert!(!result.is_ok());
    assert_eq!(result.errors.len(), 1);
}

/// Test: full end-to-end lifecycle - init, plan, apply, status.
#[tokio::test]
async fn test_end_to_end_lifecycle() {
    let db = TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    let tmp = tempfile::TempDir::new().unwrap();

    // Create source table.
    db.client
        .execute(
            "CREATE TABLE raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source");

    // Write migration files.
    fs::create_dir_all(tmp.path().join("migrations").join("streams")).unwrap();
    fs::write(
        tmp.path().join("migrations").join("streams").join("order_totals.sql"),
        "-- @aqueduct:schedule = \"30s\"\n-- @aqueduct:refresh_mode = \"DIFFERENTIAL\"\nSELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;\n",
    )
    .unwrap();

    // Plan.
    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();
    let desired = aqueduct_core::dag::build_dag_state(&files, true).unwrap();
    let actual = aqueduct_core::live_state::read_live_state(&db.client)
        .await
        .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    assert!(!diff.is_empty());

    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("e2e-test", None, 1, &diff, &topo);
    assert_eq!(plan.summary.creates, 1);

    // Apply.
    let executor =
        aqueduct_core::executor::PlanExecutor::new(&db.client, "e2e-test", "0.1.0", false);
    let version = executor.execute(&plan).await.unwrap();
    assert_eq!(version, 1);

    // Status: stream table count should be 1.
    let count = aqueduct_core::live_state::get_stream_table_count(&db.client)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Plan again: should be a no-op.
    let actual2 = aqueduct_core::live_state::read_live_state(&db.client)
        .await
        .unwrap();
    let diff2 = aqueduct_core::diff::compute_diff(&desired, &actual2);
    assert!(diff2.is_empty());
}

/// Test: plan renders non-empty output for a change.
#[tokio::test]
async fn test_plan_renderer() {
    let db = TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Parse a migration file.
    let file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("test_table.sql"),
        "-- @aqueduct:schedule = \"1m\"\nSELECT 1 AS val;",
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let desired = aqueduct_core::dag::build_dag_state(&[file], false).unwrap();
    let actual = aqueduct_core::live_state::read_live_state(&db.client)
        .await
        .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("renderer-test", None, 1, &diff, &topo);

    let text = aqueduct_core::renderer::render_plan_text(&plan, None, None);
    assert!(text.contains("renderer-test"));
    assert!(text.contains("v1"));
    assert!(text.contains("+") || text.contains("create"));

    let json = aqueduct_core::renderer::render_plan_json(&plan);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["project"], "renderer-test");

    let md = aqueduct_core::renderer::render_plan_markdown(&plan);
    assert!(md.contains("## aqueduct plan"));
}

/// Test: template variable substitution in migration files.
#[tokio::test]
async fn test_template_variable_substitution() {
    let mut vars = std::collections::HashMap::new();
    vars.insert("schedule".to_string(), "45s".to_string());

    let file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("test.sql"),
        "-- @aqueduct:schedule = \"{{ var.schedule }}\"\nSELECT 1;",
        &vars,
    )
    .unwrap();

    assert_eq!(file.front_matter.schedule, Some("45s".to_string()));
}

/// Test: topological sort handles multi-level dependency chains.
#[tokio::test]
async fn test_multi_level_dag() {
    // Level 0: raw_data (source, not a stream table)
    // Level 1: normalized (depends on raw_data)
    // Level 2: aggregated (depends on normalized)

    let files = vec![
        aqueduct_core::parser::parse_migration_file(
            &PathBuf::from("aggregated.sql"),
            "-- @aqueduct:depends_on = [\"public.normalized\"]\nSELECT * FROM public.normalized;",
            &std::collections::HashMap::new(),
        )
        .unwrap(),
        aqueduct_core::parser::parse_migration_file(
            &PathBuf::from("normalized.sql"),
            "-- @aqueduct:schedule = \"30s\"\nSELECT * FROM raw_data;",
            &std::collections::HashMap::new(),
        )
        .unwrap(),
    ];

    let state = aqueduct_core::dag::build_dag_state(&files, false).unwrap();
    let order = aqueduct_core::dag::topological_sort(&state).unwrap();

    let names: Vec<&str> = order.iter().map(|q| q.name.as_str()).collect();
    let normalized_pos = names.iter().position(|&n| n == "normalized").unwrap();
    let aggregated_pos = names.iter().position(|&n| n == "aggregated").unwrap();
    assert!(normalized_pos < aggregated_pos);
}

// ── v0.2 tests ───────────────────────────────────────────────────────────────

/// Test: cypher_source directive is parsed without unknown-key warning.
#[tokio::test]
async fn test_cypher_source_no_warning() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "cypher-test",
        &[(
            "graph_agg",
            r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:cypher_source = "queries/graph_agg.cypher"
SELECT node_id, COUNT(*) AS degree FROM edges GROUP BY node_id;
"#,
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].front_matter.cypher_source.as_deref(),
        Some("queries/graph_agg.cypher")
    );
    assert!(
        files[0].unknown_keys.is_empty(),
        "cypher_source must not generate unknown-key warning"
    );
}

/// Test: in-place plan does not trigger a FULL rebuild.
#[tokio::test]
async fn test_in_place_plan_no_full_rebuild() {
    use aqueduct_core::classifier::{classify_delta, MigrationClass};
    use aqueduct_core::dag::{
        build_dag_state, DagState, QualifiedName, RefreshMode, StreamTableSpec,
    };
    use aqueduct_core::diff::{compute_diff, DeltaKind};

    // Actual state: order_totals with just `total`.
    let actual = DagState {
        stream_tables: vec![StreamTableSpec {
            qualified_name: QualifiedName::new("public", "order_totals"),
            query: "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
                .to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![],
            cypher_source: None,
        }],
        sources: vec![],
    };

    // Desired: add COUNT(*) column.
    let files = vec![aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("order_totals.sql"),
        r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count FROM orders GROUP BY customer_id;
"#,
        &std::collections::HashMap::new(),
    )
    .unwrap()];
    let desired = build_dag_state(&files, false).unwrap();

    let diff = compute_diff(&desired, &actual);
    assert!(!diff.is_empty());

    let changes = diff.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, DeltaKind::AlterQuery);

    let class = classify_delta(changes[0]);
    assert_eq!(
        class,
        MigrationClass::InPlace,
        "Column addition should be in-place"
    );
}

/// Test: FULL→DIFF refresh_mode change produces a rebuild plan.
#[tokio::test]
async fn test_full_to_diff_mode_change_classified_rebuild() {
    use aqueduct_core::classifier::{classify_delta, MigrationClass};
    use aqueduct_core::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use aqueduct_core::diff::{DeltaKind, NodeDelta};

    let make_spec = |mode: RefreshMode| StreamTableSpec {
        qualified_name: QualifiedName::new("public", "test_agg"),
        query: "SELECT a, SUM(b) FROM t GROUP BY a".to_string(),
        refresh_mode: mode,
        schedule: "30s".to_string(),
        cdc_mode: None,
        explicit_depends_on: vec![],
        depends_on: vec![],
        cypher_source: None,
    };

    let delta = NodeDelta {
        qualified_name: QualifiedName::new("public", "test_agg"),
        kind: DeltaKind::AlterRefreshMode,
        desired: Some(make_spec(RefreshMode::Differential)),
        actual: Some(make_spec(RefreshMode::Full)),
    };

    assert_eq!(classify_delta(&delta), MigrationClass::Rebuild);
}

/// Test: explain-cost renders a cost breakdown table.
#[tokio::test]
async fn test_plan_renderer_with_cost() {
    let db = aqueduct_testkit::TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    db.client
        .execute(
            "CREATE TABLE raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source");

    let file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("order_totals.sql"),
        "-- @aqueduct:schedule = \"30s\"\nSELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;",
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let desired = aqueduct_core::dag::build_dag_state(&[file], false).unwrap();
    let actual = aqueduct_core::live_state::read_live_state(&db.client)
        .await
        .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("cost-render-test", None, 1, &diff, &topo);

    let cost = aqueduct_core::cost::estimate_plan_cost(&db.client, &plan, None)
        .await
        .unwrap();

    let text = aqueduct_core::renderer::render_plan_text_with_cost(&plan, None, None, &cost);
    assert!(text.contains("cost-render-test"));
    assert!(text.contains("Duration (est)"));
}
