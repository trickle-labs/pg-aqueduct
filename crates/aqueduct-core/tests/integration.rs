/// Integration tests for aqueduct-core against a live PostgreSQL instance.
/// These tests use aqueduct-testkit to spin up a Testcontainers PostgreSQL container.
use aqueduct_core::{
    catalog::CATALOG_INIT_SQL,
    dag::{build_dag_state, topological_sort},
    diff::compute_diff,
    executor::{import_from_live, PlanExecutor},
    live_state::{check_pgtrickle_version, get_latest_dag_version, read_live_state},
    parser::parse_migration_file,
    plan::build_plan,
    validate::{validate_ivm_supportability, validate_sql_syntax},
};
use aqueduct_testkit::TestDb;
use std::collections::HashMap;
use std::path::PathBuf;

// ── helpers ──────────────────────────────────────────────────────────────────

fn parse_file(name: &str, content: &str) -> aqueduct_core::parser::MigrationFile {
    parse_migration_file(
        &PathBuf::from(format!("{}.sql", name)),
        content,
        &HashMap::new(),
    )
    .unwrap()
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Test: the mock pg_trickle schema is properly installed.
#[tokio::test]
async fn test_mock_pgtrickle_installed() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");

    let version = check_pgtrickle_version(&db.client)
        .await
        .expect("check version");
    assert!(version.is_some());
    assert!(version.unwrap().contains("mock"));
}

/// Test: `aqueduct init` creates the catalog schema.
#[tokio::test]
async fn test_init_creates_catalog() {
    let db = TestDb::new().await.expect("start test db");
    db.client
        .batch_execute(CATALOG_INIT_SQL)
        .await
        .expect("init catalog");

    let version_row = db
        .client
        .query_one(
            "SELECT value_jsonb FROM aqueduct.cluster_profile WHERE key = 'catalog_schema_version'",
            &[],
        )
        .await
        .expect("query cluster_profile");

    let version: serde_json::Value = version_row.get(0);
    assert_eq!(version.as_i64().unwrap(), 1);
}

/// Test: `read_live_state` returns empty when no stream tables exist.
#[tokio::test]
async fn test_read_live_state_empty() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");

    let state = read_live_state(&db.client).await.expect("read state");
    assert!(state.stream_tables.is_empty());
}

/// Test: create stream table via mock, then read it back.
#[tokio::test]
async fn test_create_and_read_stream_table() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");

    // Create a simple source table.
    db.client
        .execute(
            "CREATE TABLE IF NOT EXISTS raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source table");

    // Create a stream table via mock pg_trickle.
    db.client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5)",
            &[
                &"public",
                &"order_totals",
                &"SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id",
                &"DIFFERENTIAL",
                &"30s",
            ],
        )
        .await
        .expect("create stream table");

    let state = read_live_state(&db.client).await.expect("read state");
    assert_eq!(state.stream_tables.len(), 1);
    assert_eq!(state.stream_tables[0].qualified_name.name, "order_totals");
    assert_eq!(state.stream_tables[0].schedule, "30s");
}

/// Test: the full plan → apply → status cycle.
#[tokio::test]
async fn test_plan_apply_cycle() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Create source table.
    db.client
        .execute(
            "CREATE TABLE IF NOT EXISTS raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source");

    // Build desired state from a migration file.
    let migration_content = r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;
"#;

    let files = vec![parse_file("order_totals", migration_content)];
    let desired = build_dag_state(&files, true).expect("build desired state");
    let actual = read_live_state(&db.client)
        .await
        .expect("read actual state");

    let diff = compute_diff(&desired, &actual);
    assert!(!diff.is_empty());
    assert_eq!(diff.changes().len(), 1);

    let topo = topological_sort(&desired).expect("topo sort");
    let plan = build_plan("test-project", None, 1, &diff, &topo);

    assert_eq!(plan.summary.creates, 1);

    // Execute the plan.
    let executor = PlanExecutor::new(&db.client, "test-project", "0.1.0", false);
    let new_version = executor.execute(&plan).await.expect("execute plan");
    assert_eq!(new_version, 1);

    // Verify the stream table was "created" in the mock catalog.
    let state_after = read_live_state(&db.client).await.expect("read state after");
    assert_eq!(state_after.stream_tables.len(), 1);
    assert_eq!(
        state_after.stream_tables[0].qualified_name.name,
        "order_totals"
    );

    // Verify the version was recorded.
    let version = get_latest_dag_version(&db.client, "test-project")
        .await
        .expect("get version");
    assert_eq!(version, Some(1));

    // Now a second plan should be a no-op.
    let actual2 = read_live_state(&db.client).await.expect("read actual2");
    let diff2 = compute_diff(&desired, &actual2);
    assert!(diff2.is_empty());
}

/// Test: plan detects a schedule change as a "free" migration.
#[tokio::test]
async fn test_plan_detects_schedule_change() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Create a stream table with schedule "30s".
    db.client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5)",
            &[
                &"public",
                &"my_table",
                &"SELECT 1 AS val",
                &"DIFFERENTIAL",
                &"30s",
            ],
        )
        .await
        .expect("create");

    // Desired state has schedule "1m".
    let files = vec![parse_file(
        "my_table",
        "-- @aqueduct:schedule = \"1m\"\nSELECT 1 AS val;",
    )];
    let desired = build_dag_state(&files, false).expect("build desired");
    let actual = read_live_state(&db.client).await.expect("read actual");

    let diff = compute_diff(&desired, &actual);
    assert!(!diff.is_empty());

    let changes = diff.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].kind,
        aqueduct_core::diff::DeltaKind::AlterSchedule
    );
}

/// Test: `aqueduct import` generates migration files from live state.
#[tokio::test]
async fn test_import_from_live() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");

    // Create two stream tables.
    db.client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5)",
            &[
                &"public",
                &"table_a",
                &"SELECT 1 AS a",
                &"DIFFERENTIAL",
                &"30s",
            ],
        )
        .await
        .expect("create table_a");

    db.client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5)",
            &[&"public", &"table_b", &"SELECT 2 AS b", &"FULL", &"5m"],
        )
        .await
        .expect("create table_b");

    let tmp_dir = tempfile::TempDir::new().expect("tmp dir");
    let count = import_from_live(&db.client, "imported-project", tmp_dir.path(), &[])
        .await
        .expect("import");

    assert_eq!(count, 2);

    // Check that the files were created.
    let streams_dir = tmp_dir.path().join("migrations").join("streams");
    assert!(streams_dir.join("table_a.sql").exists());
    assert!(streams_dir.join("table_b.sql").exists());

    // Check that aqueduct.toml was created.
    assert!(tmp_dir.path().join("aqueduct.toml").exists());

    // Parse the generated files and verify they round-trip.
    let a_content = std::fs::read_to_string(streams_dir.join("table_a.sql")).unwrap();
    assert!(a_content.contains("30s"));
    assert!(a_content.contains("SELECT 1 AS a"));

    let b_content = std::fs::read_to_string(streams_dir.join("table_b.sql")).unwrap();
    assert!(b_content.contains("5m"));
    assert!(b_content.contains("FULL"));
}

/// Test: rollback resets to a previous version.
#[tokio::test]
async fn test_rollback() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Apply version 1: create table_a.
    let files_v1 = vec![parse_file(
        "table_a",
        "-- @aqueduct:schedule = \"30s\"\nSELECT 1 AS a;",
    )];
    let desired_v1 = build_dag_state(&files_v1, false).expect("desired v1");
    let actual_v1 = read_live_state(&db.client).await.expect("actual v1");
    let diff_v1 = compute_diff(&desired_v1, &actual_v1);
    let topo_v1 = topological_sort(&desired_v1).expect("topo v1");
    let plan_v1 = build_plan("rollback-test", None, 1, &diff_v1, &topo_v1);

    let executor = PlanExecutor::new(&db.client, "rollback-test", "0.1.0", false);
    executor.execute(&plan_v1).await.expect("apply v1");

    // Apply version 2: add table_b.
    let files_v2 = vec![
        parse_file("table_a", "-- @aqueduct:schedule = \"30s\"\nSELECT 1 AS a;"),
        parse_file("table_b", "-- @aqueduct:schedule = \"1m\"\nSELECT 2 AS b;"),
    ];
    let desired_v2 = build_dag_state(&files_v2, false).expect("desired v2");
    let actual_v2 = read_live_state(&db.client).await.expect("actual v2");
    let diff_v2 = compute_diff(&desired_v2, &actual_v2);
    let topo_v2 = topological_sort(&desired_v2).expect("topo v2");
    let plan_v2 = build_plan("rollback-test", Some(1), 2, &diff_v2, &topo_v2);

    executor.execute(&plan_v2).await.expect("apply v2");

    let state_v2 = read_live_state(&db.client).await.expect("state v2");
    assert_eq!(state_v2.stream_tables.len(), 2);

    // Rollback to v1: desired state is files_v1.
    let actual_after_v2 = read_live_state(&db.client).await.expect("actual after v2");
    let diff_rollback = compute_diff(&desired_v1, &actual_after_v2);
    let topo_rb = topological_sort(&desired_v1).expect("topo rb");
    let plan_rollback = build_plan("rollback-test", Some(2), 3, &diff_rollback, &topo_rb);

    executor
        .execute(&plan_rollback)
        .await
        .expect("apply rollback");

    let state_after_rollback = read_live_state(&db.client)
        .await
        .expect("state after rollback");
    assert_eq!(state_after_rollback.stream_tables.len(), 1);
    assert_eq!(
        state_after_rollback.stream_tables[0].qualified_name.name,
        "table_a"
    );
}

/// Test: validate SQL syntax in offline mode (no DB needed but uses aqueduct-core).
#[tokio::test]
async fn test_validate_offline() {
    // Valid SQL.
    assert!(validate_sql_syntax("SELECT id, SUM(amount) FROM orders GROUP BY id", "test").is_ok());

    // Invalid SQL.
    assert!(validate_sql_syntax("@@@ NOT VALID SQL @@@", "test").is_err());

    // IVM valid.
    assert!(validate_ivm_supportability(
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        "test"
    )
    .is_ok());

    // IVM invalid: volatile function.
    assert!(validate_ivm_supportability("SELECT random() FROM t", "test").is_err());
}

/// Test: concurrent lock prevents double apply.
#[tokio::test]
async fn test_lock_prevents_concurrent_apply() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Manually acquire the lock.
    db.client
        .execute(
            "INSERT INTO aqueduct.locks (project, holder, acquired_at, ttl) VALUES ($1, $2, now(), '1 hour'::interval)",
            &[&"locked-project", &"test-holder"],
        )
        .await
        .expect("insert lock");

    // Trying to acquire the same lock should fail.
    let files = vec![parse_file("t", "SELECT 1;")];
    let desired = build_dag_state(&files, false).expect("desired");
    let actual = read_live_state(&db.client).await.expect("actual");
    let diff = compute_diff(&desired, &actual);
    let topo = topological_sort(&desired).expect("topo");
    let plan = build_plan("locked-project", None, 1, &diff, &topo);

    let executor = PlanExecutor::new(&db.client, "locked-project", "0.1.0", false);
    let result = executor.execute(&plan).await;
    assert!(result.is_err());
}

// ── v0.2 tests ───────────────────────────────────────────────────────────────

/// Test: column addition is classified as in-place (v0.2 full classifier).
#[tokio::test]
async fn test_column_addition_is_in_place() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Create source table.
    db.client
        .execute(
            "CREATE TABLE raw_orders (id bigint, customer_id bigint, amount numeric, category text)",
            &[],
        )
        .await
        .expect("create source");

    // Version 1: aggregation with one column.
    let files_v1 = vec![parse_file(
        "order_totals",
        r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;
"#,
    )];
    let desired_v1 = build_dag_state(&files_v1, false).expect("desired v1");
    let actual_v1 = read_live_state(&db.client).await.expect("actual v1");
    let diff_v1 = compute_diff(&desired_v1, &actual_v1);
    let topo_v1 = topological_sort(&desired_v1).expect("topo v1");
    let plan_v1 = build_plan("inplace-test", None, 1, &diff_v1, &topo_v1);

    let executor = PlanExecutor::new(&db.client, "inplace-test", "0.2.0", false);
    executor.execute(&plan_v1).await.expect("apply v1");

    // Version 2: add a new aggregate column (in-place).
    let files_v2 = vec![parse_file(
        "order_totals",
        r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count FROM raw_orders GROUP BY customer_id;
"#,
    )];
    let desired_v2 = build_dag_state(&files_v2, false).expect("desired v2");
    let actual_v2 = read_live_state(&db.client).await.expect("actual v2");
    let diff_v2 = compute_diff(&desired_v2, &actual_v2);

    assert!(!diff_v2.is_empty());
    let changes = diff_v2.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, aqueduct_core::diff::DeltaKind::AlterQuery);

    // Classify: must be in-place.
    let class = aqueduct_core::classifier::classify_delta(changes[0]);
    assert_eq!(class, aqueduct_core::classifier::MigrationClass::InPlace);

    // Build and apply the in-place plan.
    let topo_v2 = topological_sort(&desired_v2).expect("topo v2");
    let plan_v2 = build_plan("inplace-test", Some(1), 2, &diff_v2, &topo_v2);
    assert_eq!(plan_v2.summary.in_place_count, 1);
    assert_eq!(plan_v2.summary.rebuild_count, 0);

    executor.execute(&plan_v2).await.expect("apply v2 in-place");
}

/// Test: column removal is classified as in-place (v0.2 full classifier).
#[tokio::test]
async fn test_column_removal_is_in_place() {
    // No DB needed — this is a pure classifier unit test run at integration level.
    use aqueduct_core::classifier::{classify_delta, MigrationClass};
    use aqueduct_core::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use aqueduct_core::diff::{DeltaKind, NodeDelta};

    let make_spec = |q: &str| StreamTableSpec {
        qualified_name: QualifiedName::new("public", "test"),
        query: q.to_string(),
        refresh_mode: RefreshMode::Differential,
        schedule: "30s".to_string(),
        cdc_mode: None,
        explicit_depends_on: vec![],
        depends_on: vec![],
        cypher_source: None,
    };

    // Drop the `order_count` column — in-place.
    let delta = NodeDelta {
        qualified_name: QualifiedName::new("public", "test"),
        kind: DeltaKind::AlterQuery,
        desired: Some(make_spec(
            "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id",
        )),
        actual: Some(make_spec(
            "SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count FROM raw_orders GROUP BY customer_id",
        )),
    };

    assert_eq!(classify_delta(&delta), MigrationClass::InPlace);
}

/// Test: refresh_mode FULL→DIFFERENTIAL triggers a rebuild plan.
#[tokio::test]
async fn test_refresh_mode_full_to_diff_is_rebuild() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Create the source table first so the mock can execute the query.
    db.client
        .execute(
            "CREATE TABLE raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source table");

    // Create a FULL-mode stream table.
    db.client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5)",
            &[
                &"public",
                &"my_agg",
                &"SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id",
                &"FULL",
                &"1m",
            ],
        )
        .await
        .expect("create FULL stream table");

    // Desired: DIFFERENTIAL (must rebuild to establish delta state).
    let files = vec![parse_file(
        "my_agg",
        r#"-- @aqueduct:schedule = "1m"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;
"#,
    )];
    let desired = build_dag_state(&files, false).expect("desired");
    let actual = read_live_state(&db.client).await.expect("actual");
    let diff = compute_diff(&desired, &actual);

    assert!(!diff.is_empty());
    let changes = diff.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].kind,
        aqueduct_core::diff::DeltaKind::AlterRefreshMode
    );

    let class = aqueduct_core::classifier::classify_delta(changes[0]);
    assert_eq!(class, aqueduct_core::classifier::MigrationClass::Rebuild);

    // The plan should include a drop + recreate (not just an alter).
    let topo = topological_sort(&desired).expect("topo");
    let plan = build_plan("refresh-test", None, 1, &diff, &topo);
    assert_eq!(plan.summary.rebuild_count, 1);
}

/// Test: `cypher_source` front-matter directive is parsed without unknown-key warnings.
#[tokio::test]
async fn test_cypher_source_directive_parsed() {
    let file = parse_file(
        "graph_agg",
        r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:cypher_source = "queries/graph_agg.cypher"
SELECT node_id, COUNT(*) AS degree FROM edges GROUP BY node_id;
"#,
    );

    assert_eq!(
        file.front_matter.cypher_source.as_deref(),
        Some("queries/graph_agg.cypher")
    );
    // Must NOT emit an unknown-key warning.
    assert!(
        file.unknown_keys.is_empty(),
        "cypher_source should not emit unknown-key warning: {:?}",
        file.unknown_keys
    );
}

/// Test: owned source table DDL change triggers cascade rebuild of affected stream tables.
#[tokio::test]
async fn test_source_ddl_cascade_analysis() {
    use aqueduct_core::dag::{DagState, QualifiedName, RefreshMode, SourceSpec, StreamTableSpec};
    use aqueduct_core::diff::{compute_diff, SourceDeltaKind};

    let source_qname = QualifiedName::new("public", "raw_orders");

    let desired = DagState {
        stream_tables: vec![StreamTableSpec {
            qualified_name: QualifiedName::new("public", "order_totals"),
            query: "SELECT customer_id, SUM(amount) AS total FROM public.raw_orders GROUP BY customer_id".to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![source_qname.clone()],
            cypher_source: None,
        }],
        sources: vec![SourceSpec {
            qualified_name: source_qname.clone(),
            owned: true,
            create_sql: Some("CREATE TABLE public.raw_orders (id bigint, customer_id bigint, amount numeric, category text)".to_string()),
        }],
    };

    let actual = DagState {
        stream_tables: desired.stream_tables.clone(),
        sources: vec![SourceSpec {
            qualified_name: source_qname.clone(),
            owned: true,
            // Old DDL without `category`.
            create_sql: Some(
                "CREATE TABLE public.raw_orders (id bigint, customer_id bigint, amount numeric)"
                    .to_string(),
            ),
        }],
    };

    let diff = compute_diff(&desired, &actual);

    // Source delta should detect the DDL change.
    assert_eq!(diff.source_deltas.len(), 1);
    assert_eq!(diff.source_deltas[0].kind, SourceDeltaKind::AlterDdl);

    // The cascade should include the order_totals stream table.
    assert_eq!(diff.source_deltas[0].cascade_impacts.len(), 1);
    assert_eq!(
        diff.source_deltas[0].cascade_impacts[0].stream_table,
        QualifiedName::new("public", "order_totals")
    );

    // The overall diff must not be empty.
    // Note: stream tables themselves are unchanged, but source delta makes it non-empty.
    assert!(!diff.is_empty());
}

/// Test: plan includes AlterBaseTable step for owned source DDL change.
#[tokio::test]
async fn test_plan_includes_alter_base_table_step() {
    use aqueduct_core::dag::{DagState, QualifiedName, RefreshMode, SourceSpec, StreamTableSpec};
    use aqueduct_core::diff::compute_diff;
    use aqueduct_core::plan::{build_plan, PlanStep};

    let source_qname = QualifiedName::new("public", "raw_orders");
    let new_ddl = "CREATE TABLE public.raw_orders (id bigint, customer_id bigint, amount numeric, category text)".to_string();

    let desired = DagState {
        stream_tables: vec![StreamTableSpec {
            qualified_name: QualifiedName::new("public", "order_totals"),
            query: "SELECT customer_id, SUM(amount) AS total FROM public.raw_orders GROUP BY customer_id".to_string(),
            refresh_mode: RefreshMode::Differential,
            schedule: "30s".to_string(),
            cdc_mode: None,
            explicit_depends_on: vec![],
            depends_on: vec![source_qname.clone()],
            cypher_source: None,
        }],
        sources: vec![SourceSpec {
            qualified_name: source_qname.clone(),
            owned: true,
            create_sql: Some(new_ddl.clone()),
        }],
    };

    let actual = DagState {
        stream_tables: desired.stream_tables.clone(),
        sources: vec![SourceSpec {
            qualified_name: source_qname.clone(),
            owned: true,
            create_sql: Some(
                "CREATE TABLE public.raw_orders (id bigint, customer_id bigint, amount numeric)"
                    .to_string(),
            ),
        }],
    };

    let diff = compute_diff(&desired, &actual);
    let topo = vec![QualifiedName::new("public", "order_totals")];
    let plan = build_plan("cascade-test", None, 1, &diff, &topo);

    // Must have an AlterBaseTable step.
    let has_alter_base = plan
        .steps
        .iter()
        .any(|s| matches!(s, PlanStep::AlterBaseTable { name, .. } if name.name == "raw_orders"));
    assert!(has_alter_base, "Plan must contain AlterBaseTable step");

    // The summary must include a change.
    assert!(!plan.summary.is_empty());
}

/// Test: `aqueduct plan --explain-cost` produces a cost breakdown.
#[tokio::test]
async fn test_explain_cost_returns_step_costs() {
    let db = TestDb::new().await.expect("start test db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    db.client
        .execute(
            "CREATE TABLE raw_orders (id bigint, customer_id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source");

    let files = vec![parse_file(
        "order_totals",
        r#"-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id;
"#,
    )];
    let desired = build_dag_state(&files, false).expect("desired");
    let actual = read_live_state(&db.client).await.expect("actual");
    let diff = compute_diff(&desired, &actual);
    let topo = topological_sort(&desired).expect("topo");
    let plan = build_plan("cost-test", None, 1, &diff, &topo);

    let cost = aqueduct_core::cost::estimate_plan_cost(&db.client, &plan, None)
        .await
        .expect("estimate cost");

    assert!(!cost.steps.is_empty());
    // At minimum: lock, validate, create, backfill, snapshot, unlock.
    assert!(cost.steps.len() >= 4);

    // Backfill step should have a duration estimate.
    let backfill_cost = cost.steps.iter().find(|s| s.step.starts_with("BACKFILL"));
    assert!(backfill_cost.is_some(), "Should have a BACKFILL cost step");
}
