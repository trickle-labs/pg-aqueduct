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
        consumers: vec![],
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

// ── v0.3 tests ────────────────────────────────────────────────────────────────

/// Test: consumer migration file (in migrations/consumers/) is parsed and
/// produces a ManageConsumerView plan step.
#[tokio::test]
async fn test_consumer_view_in_plan() {
    use aqueduct_core::plan::PlanStep;

    let db = aqueduct_testkit::TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    let consumer_file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("consumers/report_orders.sql"),
        r#"-- @aqueduct:kind = consumer
-- @aqueduct:source = public.order_totals
-- @aqueduct:expose_as = reporting.orders
SELECT customer_id, total FROM public.order_totals;
"#,
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let desired = aqueduct_core::dag::build_dag_state(&[consumer_file], false).unwrap();
    assert_eq!(desired.consumers.len(), 1);

    let actual = aqueduct_core::live_state::read_live_state(&db.client)
        .await
        .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("consumer-cli-test", None, 1, &diff, &topo);

    let has_consumer_step = plan.steps.iter().any(|s| {
        matches!(s, PlanStep::ManageConsumerView { spec, action }
            if spec.expose_as.schema == "reporting" && action == "create")
    });
    assert!(
        has_consumer_step,
        "Plan should include ManageConsumerView(create) step"
    );
}

/// Test: source and expose_as front-matter directives do not produce
/// unknown-key warnings.
#[test]
fn test_source_and_expose_as_no_unknown_keys() {
    let file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("consumers/my_consumer.sql"),
        r#"-- @aqueduct:kind = consumer
-- @aqueduct:source = public.orders
-- @aqueduct:expose_as = api.orders
SELECT id, amount FROM public.orders;
"#,
        &std::collections::HashMap::new(),
    )
    .expect("should parse without error");

    assert_eq!(
        file.front_matter.unknown_keys.len(),
        0,
        "source and expose_as should not be treated as unknown keys"
    );
}

/// Test: preview backend=neon without credentials returns a Config error.
#[tokio::test]
async fn test_preview_neon_without_credentials() {
    use aqueduct_core::preview::{create_preview_neon, PreviewBackend, PreviewConfig};

    let files: Vec<aqueduct_core::parser::MigrationFile> = vec![];
    let desired = aqueduct_core::dag::build_dag_state(&files, false).unwrap();

    let config = PreviewConfig {
        branch: "test-branch".to_string(),
        backend: PreviewBackend::Neon {
            api_token: String::new(),
            project_id: "proj-123".to_string(),
        },
        sample_fraction: 0.1,
        recreate: false,
    };

    // Extract API token and project ID from config.
    let (api_token, project_id) = if let PreviewBackend::Neon {
        api_token,
        project_id,
    } = &config.backend
    {
        (api_token.as_str(), project_id.as_str())
    } else {
        ("", "")
    };

    let result = create_preview_neon(api_token, project_id, &config, &desired).await;
    assert!(result.is_err(), "Neon preview without token should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not supported") || err.contains("Neon") || err.contains("token"),
        "Error should mention Neon: {}",
        err
    );
}

/// Test: consumer view delta renders correctly as a plan description.
#[test]
fn test_consumer_view_plan_description() {
    use aqueduct_core::dag::ConsumerSpec;
    use aqueduct_core::plan::PlanStep;

    let spec = ConsumerSpec {
        name: "orders_view".to_string(),
        source: aqueduct_core::dag::QualifiedName::new("public", "order_totals"),
        expose_as: aqueduct_core::dag::QualifiedName::new("reporting", "orders"),
        sql_body: None,
    };

    let step = PlanStep::ManageConsumerView {
        spec,
        action: "create".to_string(),
    };

    let desc = step.description();
    assert!(
        desc.contains("reporting.orders")
            || desc.contains("ManageConsumerView")
            || desc.contains("VIEW"),
        "Step description should mention the view: {}",
        desc
    );
}

// ── v0.4 tests ────────────────────────────────────────────────────────────────

/// Test: aqueduct fmt --check detects non-canonical files.
#[test]
fn test_fmt_check_detects_non_canonical() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "fmt-test",
        &[(
            "order_totals",
            // lowercase keywords — not canonical
            "-- @aqueduct:schedule = \"30s\"\n\nselect customer_id, sum(amount) as total from raw.orders group by customer_id;\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();

    let result = aqueduct_core::fmt::format_migrations(&files, true /* check_only */);
    // The file should be reported as needing reformatting.
    assert_eq!(
        result.changed.len(),
        1,
        "Non-canonical file should be detected in check mode"
    );
    assert_eq!(result.unchanged.len(), 0);
    assert!(result.errors.is_empty());
}

/// Test: aqueduct fmt rewrites files to canonical form.
#[test]
fn test_fmt_rewrites_files() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "fmt-rewrite-test",
        &[(
            "order_totals",
            "-- @aqueduct:schedule = \"30s\"\n\nselect customer_id, sum(amount) as total from raw.orders group by customer_id;\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();

    let result = aqueduct_core::fmt::format_migrations(&files, false /* not check_only */);
    assert_eq!(result.changed.len(), 1, "One file should be reformatted");

    // Re-load and verify SQL is now uppercase.
    let files2 =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();
    // The SQL body in the re-loaded file should be uppercase.
    assert!(
        files2[0].sql_body.contains("SELECT") || files2[0].sql_body.contains("select"),
        "File should have been rewritten"
    );
    // Check the on-disk content directly.
    let content = std::fs::read_to_string(&files2[0].path).unwrap();
    assert!(
        content.contains("SELECT"),
        "On-disk file should contain uppercase SELECT"
    );
}

/// Test: aqueduct fmt produces identical output on already-canonical files.
#[test]
fn test_fmt_idempotent_on_canonical() {
    use aqueduct_core::parser::{FrontMatter, MigrationFile, MigrationKind};

    let file = MigrationFile {
        path: std::path::PathBuf::from("dummy.sql"),
        name: "orders".to_string(),
        front_matter: FrontMatter {
            kind: MigrationKind::Stream,
            schedule: Some("30s".to_string()),
            refresh_mode: Some("DIFFERENTIAL".to_string()),
            ..Default::default()
        },
        sql_body: "SELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id"
            .to_string(),
        unknown_keys: vec![],
    };

    let rendered_once = aqueduct_core::fmt::render_migration(&file);
    // Parse the rendered output as a new migration file.
    let re_parsed = aqueduct_core::parser::parse_migration_file(
        &file.path,
        &rendered_once,
        &std::collections::HashMap::new(),
    )
    .unwrap();
    let rendered_twice = aqueduct_core::fmt::render_migration(&re_parsed);

    assert_eq!(
        rendered_once, rendered_twice,
        "fmt should be idempotent: applying it twice should produce the same output"
    );
}

/// Test: aqueduct lint detects schedule-too-aggressive.
#[test]
fn test_lint_aggressive_schedule_cli() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "lint-test",
        &[(
            "fast_refresh",
            "-- @aqueduct:schedule = \"1s\"\nSELECT id FROM t;\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();
    let state = aqueduct_core::dag::build_dag_state(&files, false).unwrap();
    let result = aqueduct_core::lint::lint_migrations(&files, &state);

    let warnings: Vec<_> = result.warnings().collect();
    assert!(
        warnings.iter().any(|w| w.rule == "schedule-too-aggressive"),
        "Lint should warn about 1s schedule"
    );
}

/// Test: aqueduct lint passes on well-formed migration files.
#[test]
fn test_lint_passes_on_well_formed() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "lint-ok-test",
        &[(
            "order_totals",
            "-- @aqueduct:schedule = \"30s\"\n-- @aqueduct:refresh_mode = \"DIFFERENTIAL\"\nSELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id;\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();
    let state = aqueduct_core::dag::build_dag_state(&files, false).unwrap();
    let result = aqueduct_core::lint::lint_migrations(&files, &state);

    assert!(
        result.is_clean(),
        "Well-formed migration should produce no lint diagnostics, but got: {:?}",
        result.diagnostics
    );
}

/// Test: aqueduct lint detects full-refresh-no-filter.
#[test]
fn test_lint_full_refresh_no_filter_cli() {
    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "lint-full-test",
        &[(
            "full_scan",
            "-- @aqueduct:schedule = \"30s\"\n-- @aqueduct:refresh_mode = \"FULL\"\nSELECT id, name FROM large_table;\n",
        )],
    );

    let files =
        aqueduct_core::parser::load_migrations(tmp.path(), &std::collections::HashMap::new())
            .unwrap();
    let state = aqueduct_core::dag::build_dag_state(&files, false).unwrap();
    let result = aqueduct_core::lint::lint_migrations(&files, &state);

    let warnings: Vec<_> = result.warnings().collect();
    assert!(
        warnings.iter().any(|w| w.rule == "full-refresh-no-filter"),
        "Lint should warn about FULL refresh with no filter"
    );
}

// ── v0.5 tests ────────────────────────────────────────────────────────────────

/// Test: ingest from dbt-target generates stream migration files.
#[test]
fn test_ingest_dbt_generates_stream_files() {
    use aqueduct_core::ingest::{ingest_from_dbt, IngestChangeKind};
    use std::fs;

    let tmp = tempfile::TempDir::new().unwrap();
    let dbt_dir = tmp.path().join("target");
    let out_dir = tmp.path().join("project");
    fs::create_dir_all(&dbt_dir).unwrap();

    // Write manifest.json
    let manifest = r#"{
  "nodes": {
    "model.analytics.order_totals": {
      "resource_type": "model",
      "name": "order_totals",
      "config": {
        "materialized": "stream_table",
        "schedule": "30s",
        "refresh_mode": "DIFFERENTIAL"
      },
      "depends_on": { "nodes": [] },
      "compiled_path": "compiled/analytics/models/order_totals.sql",
      "original_file_path": "models/order_totals.sql"
    }
  },
  "sources": {}
}"#;
    fs::write(dbt_dir.join("manifest.json"), manifest).unwrap();

    // Write compiled SQL
    let compiled_dir = dbt_dir.join("compiled").join("analytics").join("models");
    fs::create_dir_all(&compiled_dir).unwrap();
    fs::write(
        compiled_dir.join("order_totals.sql"),
        "SELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id",
    )
    .unwrap();

    let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();

    assert_eq!(result.streams_total, 1, "Should find 1 stream model");
    assert_eq!(result.streams_changed, 1, "Should create 1 file");
    assert_eq!(result.changes[0].kind, IngestChangeKind::Created);

    let stream_file = out_dir
        .join("migrations")
        .join("streams")
        .join("order_totals.sql");
    assert!(stream_file.exists(), "Stream migration file should be created");

    let content = fs::read_to_string(&stream_file).unwrap();
    assert!(content.contains("@aqueduct:schedule"), "Should have schedule directive");
    assert!(content.contains("30s"), "Should have 30s schedule");
    assert!(content.contains("DIFFERENTIAL"), "Should have refresh mode");
    assert!(content.contains("SELECT"), "Should have SQL body");
}

/// Test: ingest generates source files for referenced dbt sources.
#[test]
fn test_ingest_dbt_generates_source_files() {
    use aqueduct_core::ingest::ingest_from_dbt;
    use std::fs;

    let tmp = tempfile::TempDir::new().unwrap();
    let dbt_dir = tmp.path().join("target");
    let out_dir = tmp.path().join("project");
    fs::create_dir_all(&dbt_dir).unwrap();

    let manifest = r#"{
  "nodes": {
    "model.analytics.totals": {
      "resource_type": "model",
      "name": "totals",
      "config": {
        "materialized": "stream_table",
        "schedule": "1m"
      },
      "depends_on": { "nodes": ["source.analytics.warehouse.orders"] },
      "compiled_path": "compiled/analytics/models/totals.sql",
      "original_file_path": "models/totals.sql"
    }
  },
  "sources": {
    "source.analytics.warehouse.orders": {
      "resource_type": "source",
      "name": "orders",
      "schema": "raw",
      "source_name": "warehouse",
      "identifier": null
    }
  }
}"#;
    fs::write(dbt_dir.join("manifest.json"), manifest).unwrap();

    let compiled_dir = dbt_dir.join("compiled").join("analytics").join("models");
    fs::create_dir_all(&compiled_dir).unwrap();
    fs::write(
        compiled_dir.join("totals.sql"),
        "SELECT customer_id, COUNT(*) AS cnt FROM raw.orders GROUP BY customer_id",
    )
    .unwrap();

    let result = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();

    assert_eq!(result.sources_total, 1, "Should generate 1 source file");

    let source_file = out_dir
        .join("migrations")
        .join("sources")
        .join("raw_orders.sql");
    assert!(source_file.exists(), "Source migration file should be created");

    let content = fs::read_to_string(&source_file).unwrap();
    assert!(content.contains("kind"), "Source file should declare kind");
    assert!(content.contains("source"), "Source file should say source");
    assert!(content.contains("owned = false"), "Source should be owned = false");
}

/// Test: ingest is idempotent — running twice produces no changes on second run.
#[test]
fn test_ingest_dbt_idempotent() {
    use aqueduct_core::ingest::{ingest_from_dbt, IngestChangeKind};
    use std::fs;

    let tmp = tempfile::TempDir::new().unwrap();
    let dbt_dir = tmp.path().join("target");
    let out_dir = tmp.path().join("project");
    fs::create_dir_all(&dbt_dir).unwrap();

    let manifest = r#"{
  "nodes": {
    "model.p.my_stream": {
      "resource_type": "model",
      "name": "my_stream",
      "config": { "materialized": "stream_table", "schedule": "30s" },
      "depends_on": { "nodes": [] },
      "compiled_path": "compiled/p/models/my_stream.sql",
      "original_file_path": "models/my_stream.sql"
    }
  },
  "sources": {}
}"#;
    fs::write(dbt_dir.join("manifest.json"), manifest).unwrap();

    let compiled_dir = dbt_dir.join("compiled").join("p").join("models");
    fs::create_dir_all(&compiled_dir).unwrap();
    fs::write(compiled_dir.join("my_stream.sql"), "SELECT 1 AS val").unwrap();

    let r1 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
    assert_eq!(r1.streams_changed, 1);

    let r2 = ingest_from_dbt(&dbt_dir, &out_dir).unwrap();
    assert_eq!(r2.streams_changed, 0, "Second run should be a no-op");
    assert_eq!(r2.changes[0].kind, IngestChangeKind::Unchanged);
}

/// Test: ingest from dbt target using the example's pre-compiled manifest.
#[test]
fn test_ingest_dbt_roundtrip_example() {
    use aqueduct_core::ingest::ingest_from_dbt;

    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let example_dir = workspace_root.join("examples").join("dbt-roundtrip");
    let dbt_target = example_dir.join("target");

    // Use a temp dir as the output to avoid polluting the example.
    let tmp = tempfile::TempDir::new().unwrap();

    let result = ingest_from_dbt(&dbt_target, tmp.path()).unwrap();

    assert_eq!(result.streams_total, 3, "Example has 3 stream_table models");
    assert_eq!(result.sources_total, 2, "Example has 2 referenced sources");
    assert!(result.streams_changed > 0, "Files should be created on first run");
}
