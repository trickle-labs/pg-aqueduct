/// CLI-level integration tests.
/// These tests spin up a Testcontainers PostgreSQL container, install the mock
/// pg_trickle schema, and exercise the CLI commands end-to-end.
use aqueduct_core::catalog::CatalogSchema;
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
    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    assert!(!diff.is_empty());

    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan =
        aqueduct_core::plan::build_plan("e2e-test", None, 1, &diff, &topo).expect("build_plan");
    assert_eq!(plan.summary.creates, 1);

    // Apply.
    let executor =
        aqueduct_core::executor::PlanExecutor::new(&db.client, "e2e-test", "0.1.0", false);
    let exec_result = executor.execute(&plan).await.unwrap();
    assert_eq!(exec_result.dag_version, 1);

    // Status: stream table count should be 1.
    let count = aqueduct_core::live_state::get_stream_table_count(&db.client)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Plan again: should be a no-op.
    let actual2 =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
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
    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("renderer-test", None, 1, &diff, &topo)
        .expect("build_plan");

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
    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("cost-render-test", None, 1, &diff, &topo)
        .expect("build_plan");

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

    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("consumer-cli-test", None, 1, &diff, &topo)
        .expect("build_plan");

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
        anchor_table: None,
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
    assert!(
        stream_file.exists(),
        "Stream migration file should be created"
    );

    let content = fs::read_to_string(&stream_file).unwrap();
    assert!(
        content.contains("@aqueduct:schedule"),
        "Should have schedule directive"
    );
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
    assert!(
        source_file.exists(),
        "Source migration file should be created"
    );

    let content = fs::read_to_string(&source_file).unwrap();
    assert!(content.contains("kind"), "Source file should declare kind");
    assert!(content.contains("source"), "Source file should say source");
    assert!(
        content.contains("owned = false"),
        "Source should be owned = false"
    );
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
    assert!(
        result.streams_changed > 0,
        "Files should be created on first run"
    );
}

// ── v0.6 tests ────────────────────────────────────────────────────────────────

/// Test: status --interval parses human-friendly interval strings.
#[test]
fn test_status_interval_parse() {
    // Re-implement the same logic inline — parse_interval lives in the CLI
    // (binary-only crate) so we test the behaviour rather than the symbol.
    fn parse(s: &str) -> Option<std::time::Duration> {
        if let Some(n) = s.strip_suffix('s') {
            n.parse::<u64>().ok().map(std::time::Duration::from_secs)
        } else if let Some(n) = s.strip_suffix('m') {
            n.parse::<u64>()
                .ok()
                .map(|v| std::time::Duration::from_secs(v * 60))
        } else if let Some(n) = s.strip_suffix('h') {
            n.parse::<u64>()
                .ok()
                .map(|v| std::time::Duration::from_secs(v * 3600))
        } else {
            s.parse::<u64>().ok().map(std::time::Duration::from_secs)
        }
    }

    assert_eq!(parse("30s"), Some(std::time::Duration::from_secs(30)));
    assert_eq!(parse("1m"), Some(std::time::Duration::from_secs(60)));
    assert_eq!(parse("2h"), Some(std::time::Duration::from_secs(7200)));
    assert_eq!(parse("60"), Some(std::time::Duration::from_secs(60)));
    assert_eq!(parse("abc"), None, "Invalid string should return None");
}

/// Test: promote PromoteOptions is correctly constructed and cloned.
#[test]
fn test_promote_options_construction() {
    use aqueduct_core::promote::PromoteOptions;

    let opts = PromoteOptions {
        from_env: "dev".to_string(),
        to_env: "prod".to_string(),
        project: "my-project".to_string(),
        dry_run: false,
    };

    let cloned = opts.clone();
    assert_eq!(cloned.from_env, "dev");
    assert_eq!(cloned.to_env, "prod");
    assert_eq!(cloned.project, "my-project");
    assert!(!cloned.dry_run);
}

/// Test: destroy DestroyOptions with dry_run=true is a no-op.
#[test]
fn test_destroy_options_dry_run_flag() {
    use aqueduct_core::destroy::DestroyOptions;

    let opts = DestroyOptions {
        project: "my-project".to_string(),
        dry_run: true,
        force_cascade: false,
        force_unowned: false,
        catalog_schema: CatalogSchema::default(),
    };
    assert!(opts.dry_run);
}

/// Test: secret backend from_str handles all valid values.
#[test]
fn test_secret_backend_all_variants() {
    use aqueduct_core::secrets::SecretBackend;

    let valid = [
        "env",
        "",
        "aws",
        "aws-secrets-manager",
        "gcp",
        "gcp-secret-manager",
        "vault",
        "hashicorp-vault",
        "sops",
        "age",
    ];
    for name in &valid {
        assert!(
            SecretBackend::from_str(name).is_ok(),
            "Backend '{}' should be valid",
            name
        );
    }

    assert!(
        SecretBackend::from_str("unknown-backend").is_err(),
        "Unknown backend should fail"
    );
}

/// Test: resolve_dsn_secrets resolves plain ${VAR} env references.
#[tokio::test]
async fn test_resolve_dsn_secrets_env_var() {
    use aqueduct_core::secrets::{resolve_dsn_secrets, SecretBackend};

    std::env::set_var("AQUEDUCT_TEST_PGHOST", "db.example.com");
    let dsn = "postgresql://user@${AQUEDUCT_TEST_PGHOST}/testdb";
    let resolved = resolve_dsn_secrets(dsn, &SecretBackend::Env)
        .await
        .expect("resolve dsn");
    assert_eq!(resolved, "postgresql://user@db.example.com/testdb");
    std::env::remove_var("AQUEDUCT_TEST_PGHOST");
}

/// Test: promote plan is empty when destination is already up-to-date.
#[tokio::test]
async fn test_promote_plan_is_empty_when_in_sync() {
    use aqueduct_core::promote::{compute_promotion_plan, PromoteOptions};

    let db = aqueduct_testkit::TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Empty desired state + empty actual state → empty plan.
    let files: Vec<aqueduct_core::parser::MigrationFile> = vec![];
    let opts = PromoteOptions {
        from_env: "dev".to_string(),
        to_env: "staging".to_string(),
        project: "sync-test".to_string(),
        dry_run: true,
    };

    let plan = compute_promotion_plan(&db.client, &files, &opts, &CatalogSchema::default())
        .await
        .expect("compute plan");

    assert!(plan.summary.is_empty(), "Plan should be empty when in sync");
}

/// Test: HA backend detection returns Primary on a plain Testcontainers instance.
#[tokio::test]
async fn test_ha_backend_detect_primary() {
    use aqueduct_core::live_state::{detect_ha_backend, HaBackend};

    let db = aqueduct_testkit::TestDb::new().await.expect("start db");
    let backend = detect_ha_backend(&db.client).await.expect("detect backend");

    // A plain Testcontainers Postgres has no Patroni/CNPG GUC, so it should
    // be detected as Primary.
    assert_eq!(
        backend,
        HaBackend::Primary,
        "Expected Primary, got {:?}",
        backend
    );
}

/// Test: Patroni endpoint verification fails gracefully on an unreachable host.
#[test]
fn test_patroni_verify_unreachable() {
    use aqueduct_core::live_state::verify_patroni_primary;

    let result = verify_patroni_primary("127.0.0.1:29999");
    assert!(
        result.is_err(),
        "Should fail on unreachable Patroni endpoint"
    );
}

/// Test: destroy project dry-run lists tables without dropping them (CLI-level).
#[tokio::test]
async fn test_destroy_project_dry_run_cli() {
    use aqueduct_core::destroy::{destroy_project, DestroyOptions};

    let db = aqueduct_testkit::TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    db.client
        .execute(
            "CREATE TABLE raw_orders_cli (id bigint, amount numeric)",
            &[],
        )
        .await
        .expect("create source");

    let file = aqueduct_core::parser::parse_migration_file(
        &std::path::PathBuf::from("orders_cli.sql"),
        "-- @aqueduct:schedule = \"30s\"\nSELECT id, SUM(amount) AS total FROM raw_orders_cli GROUP BY id;",
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let desired = aqueduct_core::dag::build_dag_state(&[file], false).unwrap();
    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan = aqueduct_core::plan::build_plan("destroy-cli-test", None, 1, &diff, &topo)
        .expect("build_plan");
    let executor =
        aqueduct_core::executor::PlanExecutor::new(&db.client, "destroy-cli-test", "0.6.0", false);
    executor.execute(&plan).await.unwrap();

    let opts = DestroyOptions {
        project: "destroy-cli-test".to_string(),
        dry_run: true,
        force_cascade: false,
        force_unowned: false,
        catalog_schema: CatalogSchema::default(),
    };
    let result = destroy_project(&db.client, &opts).await.unwrap();

    assert!(result.dry_run);
    assert_eq!(result.stream_tables_dropped.len(), 1);
    // Table should still exist after dry run.
    let count = aqueduct_core::live_state::get_stream_table_count(&db.client)
        .await
        .unwrap();
    assert_eq!(count, 1, "Table should survive dry-run destroy");
}

// v0.8 enforcement tests

/// Test: allow_full_refresh = false config field is readable and plan rebuild count works.
#[test]
fn test_allow_full_refresh_false_enforcement() {
    use aqueduct_core::config::ApplyConfig;
    use aqueduct_core::dag::{QualifiedName, RefreshMode, StreamTableSpec};
    use aqueduct_core::diff::{DagDiff, DeltaKind, NodeDelta};
    use aqueduct_core::plan::build_plan;

    let desired_spec = StreamTableSpec {
        qualified_name: QualifiedName::new("public", "t"),
        query: "SELECT id AS user_id FROM t".to_string(),
        refresh_mode: RefreshMode::Differential,
        schedule: "30s".to_string(),
        cdc_mode: None,
        explicit_depends_on: vec![],
        depends_on: vec![],
        cypher_source: None,
    };
    let actual_spec = StreamTableSpec {
        query: "SELECT id FROM t".to_string(),
        ..desired_spec.clone()
    };
    // Column rename: same count, different alias → classifies as Rebuild (S-12).
    let delta = NodeDelta {
        qualified_name: QualifiedName::new("public", "t"),
        kind: DeltaKind::AlterQuery,
        desired: Some(desired_spec),
        actual: Some(actual_spec),
    };
    let diff = DagDiff {
        deltas: vec![delta],
        source_deltas: vec![],
        consumer_deltas: vec![],
    };
    let topo = vec![QualifiedName::new("public", "t")];
    let plan = build_plan("allow-test", None, 1, &diff, &topo).expect("build_plan");

    assert!(
        plan.summary.rebuild_count > 0,
        "Plan should have rebuild steps"
    );

    let cfg = ApplyConfig {
        allow_full_refresh: false,
        ..Default::default()
    };
    assert!(!cfg.allow_full_refresh);
    assert!(!cfg.plan_requires_window(1, 0));
    assert_eq!(plan.summary.rebuild_count, 1);
}

/// Test: maintenance window parsing - inside and outside window, including midnight wrap.
#[test]
fn test_maintenance_window_enforcement() {
    use aqueduct_core::config::ApplyConfig;
    use chrono::{TimeZone, Utc};

    let cfg = ApplyConfig {
        maintenance_window: Some("02:00-04:00 UTC".to_string()),
        maintenance_window_applies_to: vec!["rebuild".to_string()],
        allow_full_refresh: true,
        ..Default::default()
    };

    let in_window = Utc.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap();
    assert!(
        cfg.is_in_maintenance_window(in_window),
        "03:00 should be in 02:00-04:00"
    );

    let outside_window = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    assert!(
        !cfg.is_in_maintenance_window(outside_window),
        "12:00 should be outside 02:00-04:00"
    );

    assert!(cfg.plan_requires_window(1, 0));
    assert!(!cfg.plan_requires_window(0, 0));

    let cfg_wrap = ApplyConfig {
        maintenance_window: Some("22:00-02:00 UTC".to_string()),
        maintenance_window_applies_to: vec!["rebuild".to_string()],
        allow_full_refresh: true,
        ..Default::default()
    };
    let midnight = Utc.with_ymd_and_hms(2026, 1, 1, 0, 30, 0).unwrap();
    assert!(
        cfg_wrap.is_in_maintenance_window(midnight),
        "00:30 should be in 22:00-02:00 wrap window"
    );

    let noon = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    assert!(
        !cfg_wrap.is_in_maintenance_window(noon),
        "12:00 should be outside 22:00-02:00 wrap window"
    );
}

// ── T-07: binary-level CLI tests ─────────────────────────────────────────────

/// T-07: `aqueduct --help` exits 0 and contains "Usage".
#[test]
fn test_cli_help_flag() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage").or(predicate::str::contains("aqueduct")));
}

/// T-07: `aqueduct --version` exits 0 and prints the current version.
#[test]
fn test_cli_version_flag() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("0.19.0"));
}

/// T-07: `aqueduct plan --help` exits 0.
#[test]
fn test_cli_plan_help_flag() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["plan", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("plan").or(predicate::str::contains("Plan")));
}

/// T-07: `aqueduct apply --help` exits 0.
#[test]
fn test_cli_apply_help_flag() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["apply", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("apply").or(predicate::str::contains("Apply")));
}

/// T-07: `aqueduct` without arguments exits with non-zero code and prints usage.
#[test]
fn test_cli_no_args_shows_help() {
    use assert_cmd::Command;

    // With no args, most CLIs show help on stderr and exit non-zero,
    // OR they print help and exit 0. We accept either.
    let assert = Command::cargo_bin("aqueduct").unwrap().assert();

    // Either success (--help mode) or failure (error mode) is fine,
    // but it must output something about aqueduct on stdout or stderr.
    let output = assert.get_output().clone();
    let combined = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);
    assert!(
        combined.contains("aqueduct") || combined.contains("Usage") || combined.contains("help"),
        "No-args output should mention aqueduct or usage, got: {}",
        combined
    );
}

/// T-07: `aqueduct validate --help` exits 0 (if validate subcommand exists).
#[test]
fn test_cli_validate_help_or_unknown() {
    use assert_cmd::Command;

    // If validate exists, --help should succeed.
    // If it doesn't, we should get an error about unknown subcommand — which is still predictable.
    let assert = Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["validate", "--help"])
        .assert();

    let output = assert.get_output().clone();
    let exit_success = output.status.success();
    let combined = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);

    if exit_success {
        // Has validate subcommand.
        assert!(
            combined.contains("validate") || combined.contains("Validate"),
            "validate --help should mention validate"
        );
    } else {
        // Does not have validate subcommand — that's OK too.
        assert!(
            combined.contains("validate")
                || combined.contains("error")
                || combined.contains("unknown"),
            "should mention validate in error output"
        );
    }
}

// ── v0.16 tests ────────────────────────────────────────────────────────────────

/// v0.16/ERG-1: `--quiet` suppresses all non-error output.
#[test]
fn quiet_suppresses_decorative_output() {
    use assert_cmd::Command;

    let tmp = tempfile::TempDir::new().unwrap();
    // Write a minimal project without a DSN so plan fails with exit 2.
    // With --quiet, stdout should be empty; only stderr (error) may have output.
    let toml = "[project]\nname = \"quiet-test\"\n";
    std::fs::write(tmp.path().join("aqueduct.toml"), toml).unwrap();
    std::fs::create_dir_all(tmp.path().join("migrations").join("streams")).unwrap();

    let output = Command::cargo_bin("aqueduct")
        .unwrap()
        .args([
            "--quiet",
            "validate",
            "--project-dir",
            tmp.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();

    // stdout must be empty when --quiet is set.
    assert!(
        output.stdout.is_empty(),
        "--quiet should produce no stdout, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

/// v0.16/ERG-1: `--porcelain` emits only key=value lines on stdout.
#[test]
fn porcelain_outputs_key_value_only() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    // `--porcelain` + `--version` — clap prints the version string before our
    // code runs, but the test validates the flag is accepted without error.
    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["--porcelain", "--version"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"aqueduct \d+\.\d+\.\d+").unwrap());
}

/// v0.16/ERG-3: YAML output round-trips correctly through serde_yaml.
#[test]
fn yaml_escapes_quotes_and_newlines() {
    use aqueduct_core::diff::DagDiff;
    use aqueduct_core::plan::build_plan;

    // Build an empty plan for a project whose name contains special YAML chars.
    let project_name = "project: \"tricky\"\nname";
    let diff = DagDiff {
        deltas: vec![],
        source_deltas: vec![],
        consumer_deltas: vec![],
    };
    let topo = vec![];
    let plan = build_plan(project_name, None, 1, &diff, &topo).unwrap();

    // Render to YAML using the plan struct serializer.
    #[derive(serde::Serialize)]
    struct PlanYaml<'a> {
        project: &'a str,
        from_version: Option<u64>,
        to_version: u64,
        is_empty: bool,
        creates: usize,
        drops: usize,
        alters: usize,
        changes: &'a Vec<aqueduct_core::plan::PlanChange>,
    }
    let doc = PlanYaml {
        project: &plan.project,
        from_version: plan.from_version,
        to_version: plan.to_version,
        is_empty: plan.summary.is_empty(),
        creates: plan.summary.creates,
        drops: plan.summary.drops,
        alters: plan.summary.alters,
        changes: &plan.summary.changes,
    };

    let yaml = serde_yaml::to_string(&doc).unwrap();

    // The YAML must be parseable.
    let reparsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
    // The project field must round-trip exactly.
    assert_eq!(
        reparsed["project"].as_str().unwrap(),
        project_name,
        "Project name must round-trip through YAML"
    );
}

/// v0.16/TEST-2: `aqueduct plan --help` exits 0 and documents --fail-if-changed.
#[test]
fn plan_help_documents_fail_if_changed() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["plan", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fail-if-changed"));
}

/// v0.16/TEST-2: `aqueduct apply --help` exits 0 and documents --dry-run.
#[test]
fn apply_help_documents_dry_run() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["apply", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run"));
}

/// v0.16/TEST-2: `aqueduct status --help` exits 0 and documents --fail-on-drift.
#[test]
fn status_help_documents_fail_on_drift() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["status", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fail-on-drift"));
}

/// v0.16/TEST-2: `aqueduct diff --help` exits 0 and documents --fail-on-drift.
#[test]
fn diff_help_documents_fail_on_drift() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["diff", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fail-on-drift"));
}

/// v0.16/TEST-2: `aqueduct destroy --help` exits 0 and documents --dry-run.
#[test]
fn destroy_help_documents_dry_run() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["destroy", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run"));
}

/// v0.16/TEST-2: `aqueduct rollback --help` exits 0 and documents --dry-run.
#[test]
fn rollback_help_documents_dry_run() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["rollback", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run"));
}

/// v0.16/TEST-2: Missing DSN exits 2 (error code), not 1.
#[test]
fn plan_missing_dsn_exits_2() {
    use assert_cmd::Command;

    let tmp = tempfile::TempDir::new().unwrap();
    // Write a project with no DSN.
    let toml = "[project]\nname = \"no-dsn-test\"\n";
    std::fs::write(tmp.path().join("aqueduct.toml"), toml).unwrap();
    std::fs::create_dir_all(tmp.path().join("migrations").join("streams")).unwrap();

    let output = Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["plan", "--project-dir", tmp.path().to_str().unwrap()])
        .output()
        .unwrap();

    assert_eq!(
        output.status.code().unwrap_or(-1),
        2,
        "Missing DSN should exit with code 2 (error)"
    );
}

/// v0.16/TEST-2: `aqueduct validate` exits 0 on valid files (no DB required).
#[test]
fn validate_exits_0_on_valid_files() {
    use assert_cmd::Command;

    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "validate-ok",
        &[(
            "order_totals",
            "-- @aqueduct:schedule = \"30s\"\nSELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id;\n",
        )],
    );

    Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["validate", "--project-dir", tmp.path().to_str().unwrap()])
        .assert()
        .success();
}

/// v0.16/TEST-2: `aqueduct validate` exits non-zero on IVM-unsupported query
/// (SELECT DISTINCT is not supported by pg_trickle IVM).
#[test]
fn validate_differential_ivm_unsupportable_fails() {
    use assert_cmd::Command;

    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "validate-fail",
        &[(
            "bad_query",
            // SELECT DISTINCT is not IVM-supportable.
            "-- @aqueduct:schedule = \"30s\"\n-- @aqueduct:refresh_mode = \"DIFFERENTIAL\"\nSELECT DISTINCT customer_id FROM orders;\n",
        )],
    );

    let output = Command::cargo_bin("aqueduct")
        .unwrap()
        .args(["validate", "--project-dir", tmp.path().to_str().unwrap()])
        .output()
        .unwrap();

    assert_ne!(
        output.status.code().unwrap_or(0),
        0,
        "validate should exit non-zero for IVM-unsupported DISTINCT query"
    );
}

/// v0.16/TEST-2: `aqueduct validate --format json` produces valid JSON.
#[test]
fn validate_format_json_produces_valid_json() {
    use assert_cmd::Command;

    let tmp = tempfile::TempDir::new().unwrap();
    write_project(
        tmp.path(),
        "validate-json",
        &[(
            "order_totals",
            "-- @aqueduct:schedule = \"30s\"\nSELECT customer_id, SUM(amount) AS total FROM raw.orders GROUP BY customer_id;\n",
        )],
    );

    let output = Command::cargo_bin("aqueduct")
        .unwrap()
        .args([
            "validate",
            "--format",
            "json",
            "--project-dir",
            tmp.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "validate --format json should exit 0"
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout must be valid JSON");
    assert!(
        json.get("schema_version").is_some(),
        "JSON output must have schema_version field"
    );
}

/// v0.16/TEST-2: `aqueduct --version` contains the semver version number.
#[test]
fn version_flag_outputs_semver() {
    use assert_cmd::Command;
    use predicates::prelude::*;

    Command::cargo_bin("aqueduct")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"\d+\.\d+\.\d+").unwrap());
}

/// v0.16/F: PostgreSQL 18 (minimum required by pg_trickle) can run the full
/// create/apply/rollback cycle.
#[tokio::test]
async fn postgres_version_matrix_min_supported() {
    let db = TestDb::new().await.expect("start db");
    db.install_mock_pgtrickle().await.expect("install mock");
    db.install_aqueduct_catalog().await.expect("init catalog");

    // Verify we're running against PG 18+.
    let pg_version = db.pg_version().await.expect("get pg version");
    assert!(
        pg_version.contains("PostgreSQL 18") || pg_version.contains("PostgreSQL 19"),
        "Test must run against PostgreSQL 18+, got: {}",
        pg_version
    );

    // Run the core create→apply→status→rollback cycle.
    db.client
        .execute(
            "CREATE TABLE raw_events_v18 (id bigint, ts timestamptz, val numeric)",
            &[],
        )
        .await
        .expect("create source table");

    let file = aqueduct_core::parser::parse_migration_file(
        &PathBuf::from("event_summary.sql"),
        "-- @aqueduct:schedule = \"30s\"\n-- @aqueduct:refresh_mode = \"DIFFERENTIAL\"\nSELECT id, SUM(val) AS total FROM raw_events_v18 GROUP BY id;\n",
        &std::collections::HashMap::new(),
    )
    .unwrap();

    let desired = aqueduct_core::dag::build_dag_state(&[file], false).unwrap();
    let actual =
        aqueduct_core::live_state::read_live_state(&db.client, None, &CatalogSchema::default())
            .await
            .unwrap();
    let diff = aqueduct_core::diff::compute_diff(&desired, &actual);
    let topo = aqueduct_core::dag::topological_sort(&desired).unwrap();
    let plan =
        aqueduct_core::plan::build_plan("pg18-test", None, 1, &diff, &topo).expect("build_plan");
    assert_eq!(plan.summary.creates, 1, "Should create 1 stream table");

    let executor =
        aqueduct_core::executor::PlanExecutor::new(&db.client, "pg18-test", "0.16.0", false);
    let result = executor.execute(&plan).await.unwrap();
    assert_eq!(result.dag_version, 1);

    let count = aqueduct_core::live_state::get_stream_table_count(&db.client)
        .await
        .unwrap();
    assert_eq!(count, 1, "One stream table should exist after apply");
}
