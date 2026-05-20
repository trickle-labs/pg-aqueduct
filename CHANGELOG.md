# Changelog

What's new in pg_aqueduct — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
**Released**
- [v0.1.0 — Initial Implementation](#v010--initial-implementation)
- [v0.8.0 — Core Correctness & Safety Hardening](#v080--core-correctness--safety-hardening)
- [v0.9.0 — Feature Completeness & Ergonomics](#v090--feature-completeness--ergonomics)
- [v0.10.0 — Safety Contract Repair & Multi-Project Isolation](#v0100--safety-contract-repair--multi-project-isolation)
- [v0.11.0 — Diagnostic Quality, CLI Surface & Documentation Correctness](#v0110--diagnostic-quality-cli-surface--documentation-correctness)
- [v0.12.0 — Real pg_trickle Integration, Security & CI/CD Hardening](#v0120--real-pgtrickle-integration-security--cicd-hardening)
- [v0.13.0 — Blue/Green End-to-End, IMMEDIATE Mode & Advanced Features](#v0130--bluegreen-end-to-end-immediate-mode--advanced-features)
- [v0.14.0 — Failure Safety, CI/CD Trustworthiness & Security Hardening](#v0140--failure-safety-cicd-trustworthiness--security-hardening)
- [v0.15.0 — Execution Integrity, Architecture & Blue/Green Production](#v0150--execution-integrity-architecture--bluegreen-production)

**Planned**
- [v0.2.0 — Online Schema Evolution](#v020--online-schema-evolution)
- [v0.3.0 — Blue/Green, Preview Environments & Optional Extension](#v030--bluegreen-preview-environments--optional-extension)
- [v0.4.0 — CI Integrations & Ergonomics](#v040--ci-integrations--ergonomics)
- [v0.5.0 — dbt Interop](#v050--dbt-interop)
- [v0.6.0 — Production Hardening](#v060--production-hardening)
- [v0.7.0 — Documentation & Cookbook](#v070--documentation--cookbook)

**Archive**
- [Unreleased — Repository Bootstrap](#unreleased--repository-bootstrap)
<!-- TOC end -->

---

## [v0.15.0] — Execution Integrity, Architecture & Blue/Green Production

**Status:** Released

All roadmap items for v0.15 "Phase 18 — Execution Integrity, Architecture & Blue/Green
Production" are implemented and tested.

### Highlights

- **Saga-style compensating steps (CORR-2)**: `CreateStreamTable` and `DropStreamTable`
  now write a compensating action to `aqueduct.ddl_log` before executing DDL. On crash
  recovery (`--resume`), the executor has a log of non-transactional DDL it may need
  to roll back. Two new flags `--force-retry <step-index>` and `--force-skip <step-index>`
  on `aqueduct apply` let operators advance past or retry ambiguous steps (require `--yes`).

- **Catalog schema v7**: The catalog bumps to version 7. The migration adds
  `migration_id` and `compensating_sql` columns to `aqueduct.ddl_log` for the
  compensating-step registry, and adds `rolled_back_at` plus an updated `bg_status_check`
  constraint (now accepting `'rolled_back'`) to `aqueduct.blue_green_deployments`.

- **Typed executor contexts (ARCH-2)**: `ExecutionContext` enum with variants `Apply`,
  `Rollback`, `Promote`, and `DryRun` replaces the optional builder pattern, ensuring
  `dsn` and `desired_state` are present at compile time for all non-dry-run executors.
  Named `for_apply()`, `for_rollback()`, and `for_promote()` constructors document intent.

- **CatalogSchema newtype (ARCH-1)**: A new `CatalogSchema` struct validates the
  catalog schema name at parse time, rejecting SQL injection markers (`--`, `;`, `$`),
  non-identifier characters, leading digits, and reserved system schema names
  (`pg_catalog`, `information_schema`, all `pg_*` prefixed names).

- **Blue/green production state machine (ARCH-3)**: The planner emits a new
  `StartBlueGreenDeployment` step as the first post-lock step, which inserts a row
  into `aqueduct.blue_green_deployments` with `status = 'active'`. The executor then
  writes `status = 'swapped'` after the atomic consumer view swap and `status =
  'retired'` after `RetireBlueSchema`. All `SwapConsumerViews` steps are wrapped in
  a single `BEGIN ... COMMIT` block, making the swap all-or-nothing.

- **Real RLS policy capture and restore (CORR-6)**: Before any `DropStreamTable` step,
  the executor queries `pg_policies` and `pg_class.relrowsecurity` to capture real
  `CREATE POLICY` and `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` statements. The
  `RecreatePolicy` step executes these statements after the table is recreated.
  Failures are non-fatal and logged to `aqueduct.ddl_log`. The `aqueduct lint` command
  now emits a `rebuild-may-affect-rls` warning when a `FULL`-mode table has RLS enabled.

- **Batch convergence polling (PERF-1)**: `WaitForConvergence` now issues a single
  `SELECT ... WHERE table_name = ANY($2)` query per poll interval instead of one
  query per node. `convergence_poll_interval_ms` (default 500 ms) and
  `convergence_timeout_secs` (default 300 s) are configurable via `BuildPlanOptions`.

- **Structured validate diagnostics (M6)**: `aqueduct validate` now returns structured
  `DiagnosticSet` entries with file paths, error codes, and severity levels.
  `validate --format json` emits diagnostics matching the `lint` JSON schema.
  Two new public functions `validate_migration_files_diagnostic` and
  `validate_dag_diagnostic` replace the legacy string-accumulation approach.

- **PostgreSQL 18+ only**: pg_trickle requires PostgreSQL 18+; all integration tests
  now target `postgres:18-alpine` exclusively.

### New plan steps

| Step | Description |
|---|---|
| `StartBlueGreenDeployment { project, green_schema, blue_schema }` | Records deployment start in catalog |

### Catalog changes

- **v7**: `aqueduct.ddl_log` gains `migration_id bigint` and `compensating_sql text` columns.
- **v7**: `aqueduct.blue_green_deployments` gains `rolled_back_at timestamptz` and updated `bg_status_check` constraint.

---

## [v0.14.0] — Failure Safety, CI/CD Trustworthiness & Security Hardening

**Status:** Released

All roadmap items for v0.14 "Phase 17 — Failure Safety, CI/CD Trustworthiness &
Security Hardening" are implemented and tested.

### Highlights

- **Correctness fixes (CORR-1 through CORR-8)**: Seven correctness bugs were found
  and fixed. Progress is now preserved on recoverable failures (CORR-1); the heartbeat
  correctly signals the main executor loop via a `tokio::sync::watch` channel when the
  advisory lock is lost (CORR-3); promotion filters the destination state by project
  (CORR-4); the promote command migrates the catalog before use and records spec_jsonb
  (CORR-5); consumer view drops now delete the catalog row so drift counts stay
  accurate (CORR-7); and status drift counting now sums stream, source, and consumer
  deltas (CORR-8).

- **Catalog v6**: The catalog schema is bumped to version 6. The new migration adds
  the `pgtrickle_mock.scheduler_state` table used by integration tests to assert that
  `pause_scheduler` / `resume_scheduler` calls are routed correctly (TEST-3).

- **Security hardening (SEC-1 – SEC-3)**:
  - **SEC-1**: Keyword-value DSNs (e.g. `host=... password=secret`) are now also
    checked for plaintext passwords; previously only URI-style DSNs were validated.
  - **SEC-2**: Consumer SQL bodies are validated to be a single `SELECT` statement
    before the view is created, preventing DDL injection via migration files.
  - **SEC-3**: The read-only helper now issues `BEGIN READ ONLY` *before* setting
    `SET LOCAL statement_timeout`, so the timeout is correctly scoped to the
    transaction. The `sanitize_statement_timeout` allowlist prevents injection via
    the timeout argument.

- **Ergonomics (ERG-2)**: `aqueduct destroy` now exits with code 2 on error (via
  `anyhow::bail!`) instead of calling `std::process::exit(1)` directly, ensuring the
  error message is routed through the standard error handler.

- **Lint fixes (L1, L7)**:
  - **L1**: The DSN-redaction regex in the CLI is now compiled once via `LazyLock`
    instead of being recompiled on every call.
  - **L7**: CI environment variables (`GITHUB_ACTIONS`, `CI`, `GITLAB_CI`,
    `CIRCLECI`) are now checked for the string value `"true"` rather than merely being
    set, preventing false positive CI detection.

- **CI/CD fixes (CI-1 – CI-3)**:
  - **CI-1**: The composite `plan` and `apply` GitHub Actions now map the runner
    OS/architecture to the correct release archive suffix (`linux-amd64`,
    `linux-arm64`, `macos-arm64`, `macos-amd64`, `windows-amd64`).
  - **CI-2**: The action-smoke workflow now invokes the composite actions end-to-end
    using a locally-packaged archive (via the new `local-archive` input). The
    migration file path was corrected to `migrations/streams/event_count.sql`.
  - **CI-3**: `mdbook test` is now a blocking step (removed `continue-on-error: true`).
    The docs-lint job no longer checks for a non-existent `export` subcommand, and
    the subcommand reference check now fails the job if any documented subcommand is
    missing from the binary.

- **Test infrastructure (TEST-3)**: The `pgtrickle_mock` schema now includes a
  `scheduler_state` table. The mock `pause_scheduler` / `resume_scheduler` functions
  insert and delete rows in this table, enabling integration tests to assert scheduler
  state transitions. Nine new integration tests cover all CORR, SEC, and TEST items.

- **`pg_version` in JSON output (M15)**: `aqueduct status --format json` now includes
  a `"pg_version"` field.

### Migration notes

- **Catalog**: Run `aqueduct init` (or `aqueduct apply`) to apply the v5→v6 catalog
  migration. The migration is fully backward-compatible.

- **Consumer SQL validation**: If you have consumer migration files whose SQL body is
  not a pure `SELECT` statement, `aqueduct apply` will now reject them with error
  code 1307. Update those files to use a single `SELECT`.

---

## [v0.13.0] — Blue/Green End-to-End, IMMEDIATE Mode & Advanced Features

**Status:** Released

All roadmap items for v0.13 "Phase 16 — Blue/Green End-to-End, IMMEDIATE Mode &
Advanced Features" are implemented and tested.

### Highlights

- **Blue/green migrations** (`--strategy blue-green`): The planner now generates the
  full blue/green step sequence — `CreateGreenSchema`, `CreateStreamTableInGreen`,
  `WaitForConvergence` (with real pg_trickle polling), `SwapConsumerViews`, and
  `RetireBlueSchema` — for topology-restructuring diffs.

- **IMMEDIATE refresh mode**: `RefreshMode::Immediate` is parsed from
  `-- @aqueduct:refresh_mode = "IMMEDIATE"`, classified, stored, and correctly handled
  in rebuild paths via `PauseImmediate`/`ResumeImmediate` steps. The new
  `--no-immediate-downgrade` flag rejects plans that would temporarily degrade an
  IMMEDIATE table.

- **Pre/post migration hooks**: SQL statements from `[apply.hooks] pre` and `post`
  in `aqueduct.toml` are emitted as `RunHook` steps. The pre-hook fires immediately
  after `LockDag`; the post-hook fires just before `RecordSnapshot`.

- **Immutable plan artifacts** (`--out` / `--plan`): `aqueduct plan --out plan.json`
  writes the plan (with a SHA-256 spec hash) to a file. `aqueduct apply --plan plan.json`
  loads and executes the pre-computed plan, validating the spec hash to reject stale
  artifacts.

- **Step observability** (`migration_steps`): The catalog is bumped to v5, adding
  `aqueduct.migration_steps`. Every executed step writes a `running` row at start and
  a `done` (or `failed`) row at completion, enabling audit and replay.

- **Import baseline snapshot**: `aqueduct import` now calls `ensure_catalog_current`
  and records version 1 in `aqueduct.dag_versions`. A subsequent `aqueduct plan`
  returns an empty plan.

- **Real preview backends**: The `create_preview_cnpg` and `create_preview_neon` stubs
  are replaced with real HTTP implementations using the CNPG Kubernetes API and the
  Neon management API respectively.

- **Rollback classification** (`--within-window` / `--accept-data-loss`): The rollback
  command now classifies each change as `Safe`, `PointInTime`, or `DataLoss` based on
  when the version was applied relative to the pg_trickle WAL retention window.

- **JSON schema versioning** (`schema_version: 1`): All structured JSON events emitted
  by the CLI now include `"schema_version": 1`. The schema is published at
  `docs/cli-events-schema.json`.

---

## [v0.12.0] — Real pg_trickle Integration, Security & CI/CD Hardening

**Status:** Released

All roadmap items for v0.12 "Phase 15 — Real pg_trickle Integration, Security &
CI/CD Hardening" are complete.  This release replaces all mock/shim pg_trickle
interactions with real HTTP clients, hardens the security surface, improves
planner performance, and significantly expands the test suite and CI pipeline.

### What's new

**Real pg_trickle API Integration**

- **M-07:** New `PgtrickleCaps` struct probes the installed pg_trickle version at
  startup using `to_regprocedure`.  Each capability (`has_create`, `has_alter`,
  `has_drop`, `has_pause_scheduler`, `has_resume_scheduler`, `has_refresh_status`)
  is individually gated — aqueduct gracefully degrades when older pg_trickle builds
  are installed.

- **C-04:** `WaitForRefresh` plan step is now gated on `pgtrickle_caps.has_refresh_status`.
  Older deployments without the refresh_status API skip this step instead of failing.

**Security Hardening**

- **SEC-02:** Replaced env-var shims for AWS, GCP, and HashiCorp Vault secret
  backends with real HTTP clients using `reqwest` (rustls-tls, no OpenSSL).
  - AWS Secrets Manager: full SigV4 request signing implemented inline (no AWS SDK
    dependency), supports `AWS_ENDPOINT_URL_SECRETSMANAGER` override for testing.
  - GCP Secret Manager: bearer token auth via `GOOGLE_OAUTH_TOKEN`, supports
    `GCP_SECRET_MANAGER_ENDPOINT_URL` override.
  - HashiCorp Vault: KV v2 path handling, `VAULT_TOKEN` auth, supports
    `VAULT_ADDR` override.
  - All three backends verified with `httpmock` unit tests.

- **SEC-03:** `rewrite_query_for_preview_ast()` rewrites table references in
  preview-schema queries using the sqlparser AST instead of string substitution.
  Eliminates the risk of query injection via schema names or table names.

**Planner Performance**

- **P-01:** `build_source_index()` builds a `HashMap<QualifiedName, Vec<QualifiedName>>`
  once per diff computation.  `compute_source_deltas()` now looks up cascade
  impacts in O(1) per table rather than re-parsing every SQL query N times.

- **P-02:** Catalog schema bumped from v3 → **v4** with four new performance
  indexes: `aqueduct_dag_versions_project`, `aqueduct_migrations_project_status`,
  `aqueduct_migrations_project_started`, `aqueduct_locks_project`.  Applied
  automatically by `ensure_catalog_current()`.

- **P-03:** `connect_read_only()` and `connect_read_only_with_timeout()` now issue
  `SET LOCAL statement_timeout` before `BEGIN READ ONLY`.  Long-running read
  queries can no longer block the planner indefinitely.

- **P-06:** New `plan_stats()` public function computes a `PlanSummary` from a
  `&[PlanStep]` without executing.  Useful for testing and dry-run reporting.

- **P-07:** `collect_subgraph()` computes the transitive dependency closure for a
  given anchor table.  The `aqueduct preview` command gains a `--table` flag that
  limits preview creation to the subgraph rooted at the specified table.

- **P-08:** `estimate_rows()` returns a typed `CostError` instead of a generic
  `anyhow::Error`.  SQL errors and unsupported plan types are now distinguished
  and surfaced as structured tracing warnings rather than silent failures.

**Test Suite**

- **T-02/CI-04:** Full integration test suite runs against mock pg_trickle schema
  in the `pgtrickle-integration` CI job.

- **T-03:** Resume failure injection test: injects a `recoverable_failure`
  migration record and verifies that the resume path skips completed steps.

- **T-05:** Renamed `test_blue_green_plan_steps` to `test_plan_steps_for_query_change`
  and added `test_blue_green_topology_restructure` covering diamond DAG creation.

- **T-07:** Binary-level CLI tests using `assert_cmd` + `predicates`: `--help`,
  `--version`, subcommand `--help` flags, no-args behavior, and validate.

- **T-08:** Project isolation destructive test: applies two independent projects,
  destroys one, asserts the other is untouched.

- **T-10:** Property-based fuzz tests using `proptest`: `build_plan` never panics
  for arbitrary inputs; `plan_stats` is always consistent with `build_plan.summary`.

- **T-11:** Tutorial smoke test: parses bash code blocks in tutorial docs and
  validates they reference known aqueduct subcommands.

**CI/CD Pipeline**

- **CI-01:** Composite `plan` and `apply` actions now download versioned archives
  (`aqueduct-${VERSION}-${OS}-${ARCH}.tar.gz`) matching the release convention.

- **CI-03:** Removed `continue-on-error` on the Windows build matrix entry in
  `release.yml`.  Windows builds now fail the release if they fail.

- **CI-04:** New `pgtrickle-integration` CI job runs the full integration test
  suite against PostgreSQL 18 with the mock pg_trickle schema.

- **CI-05:** Coverage thresholds updated: 60% workspace-wide minimum.

- **CI-06:** `build-artifacts` in `release.yml` now has `needs: [test]` to prevent
  building release artifacts when tests fail.

- **CI-07:** macOS Intel (`macos-13`) added to the release build matrix as
  `macos-amd64`.

- **CI-09:** New `msrv` CI job checks that the workspace compiles on Rust 1.88
  (the effective MSRV: `testcontainers 0.27`, `tonic 0.14`, `serde_with 3.20`
  and other transitive dependencies require Rust 1.88). `rust-version` in
  `Cargo.toml` set to `1.88`.

- **T-09/CI-01:** New `action-smoke.yml` workflow builds the binary, packages it
  as a versioned archive, and exercises the `plan` and `apply` CLI commands
  against a live PostgreSQL 18 service container.

**Dependencies**

- `reqwest = "0.12"` with `rustls-tls` feature (no OpenSSL) added to `aqueduct-core`.
- `proptest = "1"` added as a dev-dependency for property-based testing.
- `assert_cmd = "2"` and `predicates = "3"` added as dev-dependencies for CLI tests.
- `httpmock = "0.7"` added as a dev-dependency for secret backend HTTP tests.

---

## [v0.11.0] — Diagnostic Quality, CLI Surface & Documentation Correctness

**Status:** Released

All roadmap items for v0.11 "Phase 14 — Diagnostic Quality, CLI Surface &
Documentation Correctness" are complete. This release focuses on making the CLI
output trustworthy, the error messages actionable, and the documentation honest.

### What's new

**Diagnostic Infrastructure**

- **Q-08:** New unified `Diagnostic` type (`aqueduct_core::diagnostic`) replaces
  the separate `ValidationError`, `LintDiagnostic`, and ad-hoc parse error strings.
  `DiagnosticSet` collects diagnostics with file/line/column context. Both the
  validate and lint subsystems now emit `DiagnosticSet` results.

- **Q-04:** Stable numeric error codes (`error_code()` method on `AqueductError`).
  Error categories: 1000–1099 database, 1100–1199 configuration, 1200–1299 planning,
  1300–1399 execution, 1400–1499 catalog. Codes are stable across releases.

- **Q-03:** `ManageWalSlot` plan step uses typed `WalSlotAction` enum (`Create`/`Drop`)
  instead of a free-form `String`. Eliminates the "Unknown action" dead code path.

**CLI Surface Correctness**

- **U-02:** Fixed `aqueduct plan` exit semantics. Previously exited 1 for any non-empty
  plan. Now exits 0 by default; exits 1 only when `--fail-if-changed` or
  `--fail-on-drift` is explicitly set. CI pipelines that just want to _see_ a plan
  without failing now work correctly.

- **U-03:** New `--fail-on-drift` flag for `aqueduct diff`. Exits 1 when any delta is
  non-Unchanged, exits 0 for a clean diff.

- **U-05:** `executor.execute()` now returns `ExecutionResult { migration_id, dag_version }`
  instead of a bare `u64`. The `apply_complete` JSON event on stderr now emits both
  `migration_id` (aqueduct.migrations row id) and `dag_version` (bigserial) as separate
  fields.

- **U-06/SEC-01:** Password redaction for all connection strings displayed in error
  messages. `redact_dsn()` replaces passwords with `***` in both URL-form and
  keyword-value DSNs.

- **U-10:** YAML output format for `aqueduct status` (use `--format yaml`).

**Read-Only Safety (D-03/SEC-08)**

- `aqueduct plan` and `aqueduct status` now use `connect_read_only()`, which issues
  `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` immediately after connecting.
  These commands cannot accidentally mutate data.

**Documentation Correctness**

- **D-01:** README updated to v0.11.0 status and feature list.

- **D-04/D-05:** Blue/green migration strategy (still planned for v0.13) is now clearly
  marked as `[PLANNED]` in `docs/api-reference.md`, `docs/ha-operations.md`, and the
  apply action YAML. The `--strategy blue-green` flag in the apply action is disabled
  (no-op) pending the v0.13 implementation.

- **D-07:** Unimplemented features (YAML output, IMMEDIATE mode, real secret backends)
  moved from `[Released]` to `[Planned]` sections in CHANGELOG. 

- **D-09/SEC-02:** `docs/security.md` now clearly marks AWS/GCP/Vault secret backends
  as "Planned (v0.12)". Only `env`, `sops`, and `age` are implemented today.

- **D-11:** `docs/ha-operations.md` maintenance window documentation updated to remove
  blue/green from the implemented `maintenance_window_applies_to` values.

- **D-12:** Example workflow version pins in `aqueduct-plan.yml` and `aqueduct-apply.yml`
  updated from `@v0.4.0` to `@v0.11.0`.

**Code Quality**

- **Q-01:** Step registry contract test — exhaustive match on all `PlanStep` variants
  ensures every variant has a non-empty `description()`. Adding a new variant without
  updating `description()` is now a compile error.

- **Q-02:** Typed `PlanExecutor` constructors: `for_apply()`, `for_rollback()`,
  `for_promote()`, `for_dry_run()`. The generic `new()` constructor is still available.

- **Q-06:** Typed DAG state wrappers: `DesiredDagState`, `LiveDagState`,
  `RecordedDagState` newtypes wrapping `DagState`. Prevents mixing state kinds in
  function signatures.

- **Q-09:** `lib.rs` exports `CLI_VERSION = env!("CARGO_PKG_VERSION")` as the single
  source of version truth. All commands use `env!("CARGO_PKG_VERSION")` directly.

**CI Improvements**

- **M-13:** `docs-build` job in CI runs `mdbook build` and `mdbook test` on every push.

- **U-01/D-02:** `docs-lint` job in CI builds the release binary and verifies that
  every subcommand responds to `--help`, and that the binary `--version` matches
  `Cargo.toml`.

- New `docs-cli` recipe in `justfile` generates a Markdown CLI reference from
  `--help` output.

**Security**

- **SEC-07:** New `docs/roles.sql` contains SQL grant templates for the four aqueduct
  roles: `aqueduct_planner`, `aqueduct_applier`, `aqueduct_preview`, `aqueduct_destroy`.

---

## [v0.10.0] — Safety Contract Repair & Multi-Project Isolation

**Status:** Released

All roadmap items for v0.10 "Phase 13 — Safety Contract Repair & Multi-Project
Isolation" are complete. This release repairs the safety contract (lock lifecycle,
resume, heartbeat, scheduler pause ordering) and establishes the multi-project
isolation invariant required for shared PostgreSQL deployments.

### What's new

**Critical Correctness Fixes**

- **C-01 / C-11:** Consumer-only plans are no longer silently skipped as no-ops.
  `PlanSummary` now tracks `consumer_creates`, `consumer_drops`, and `consumer_alters`
  separately; `is_empty()` and `total_changes()` include consumer deltas. All renderer
  totals updated.

- **C-03:** `RecordSnapshot` now uses `query_one` with `RETURNING version` to capture
  the actual bigserial value assigned by the database instead of recording the planned
  version number.

- **C-04:** Backfill steps are documented as "Triggered by pg_trickle on table
  creation — no explicit wait" in renderers and log messages.

- **C-05:** `read_live_state()` now populates `sources` by deserialising the latest
  `spec_jsonb` from `aqueduct.dag_versions` instead of always returning `vec![]`.

- **C-06 / C-07:** `read_live_state()` and `read_live_consumers()` both accept an
  optional `project` parameter and filter results via the new
  `aqueduct.stream_table_ownership` table. `destroy_project` verifies ownership before
  dropping any table; use `--force-unowned` to bypass.

- **C-09:** `check_pgtrickle_version()` probes `to_regprocedure(...)` before calling
  the function to avoid a database error when pg_trickle is absent.

- **C-10:** `classify_delta()` no longer panics on missing `desired`/`actual` in the
  `AlterQuery` arm; returns `Rebuild` instead.

- **C-12:** `read_live_state()` project filter enables `diff --from last-applied` and
  `diff --from desired` modes.

**Safety & Idempotency Fixes**

- **S-01 / S-02:** Failed migrations are now marked `recoverable_failure` and can be
  resumed. On resume, `LockDag` always re-executes — DDL steps are only skipped for
  steps already confirmed complete in the checkpoint.

- **S-03:** Scheduler pause now happens inside the `LockDag` executor arm, after the
  lock is acquired, preventing a pre-lock pause from affecting other projects.

- **S-04:** Heartbeat SQL is now holder-bound
  (`WHERE project = $1 AND holder = $2`). Zero rows affected is detected and logged as
  a lock-stolen warning.

- **S-05:** The project lock is released on all exit paths (success and error) via an
  explicit cleanup block after the step loop.

- **S-06:** Step progress checkpoint writes are now fatal — a failed write returns an
  error rather than silently continuing.

- **S-07:** `aqueduct apply --dry-run` uses a plain connection (no catalog migration)
  to avoid upgrading the schema during previews.

- **S-10:** `DROP TABLE` fallback in `destroy` and the executor no longer uses
  `CASCADE`. Dependent objects must be dropped manually; use `--force-cascade` to
  opt in.

- **S-11:** `aqueduct rollback` now enforces the `--accept-data-loss` flag when the
  rollback plan contains rebuild-class steps.

- **S-12:** First-time stream table creations increment `create_count` and not
  `rebuild_count`, preventing false-positive maintenance-window blocking on initial
  deployments.

- **S-13:** Lock holder string is now `aqueduct/{version}/{hostname}/{pid}/{ts}`
  making concurrent processes distinguishable.

- **S-14:** Scheduler pause failure is now a fatal error by default.

**Catalog Schema v3**

- Added `aqueduct.stream_table_ownership(project, schema_name, table_name, managed_since)`
  table with a `PRIMARY KEY (schema_name, table_name)` constraint.
- `status_check` constraint on `aqueduct.migrations` now includes `recoverable_failure`.
- Auto-migration from v2 to v3 applied on `connect_and_migrate`.

**New Tests (6)**

- `test_consumer_only_apply_end_to_end`: consumer view create with no stream table changes.
- `test_concurrent_apply_race`: two applies, second gets `LockContention`.
- `test_stale_plan_detection`: plan from_version mismatch vs DB version.
- `test_two_project_isolation`: destroy project A, project B tables intact.
- `test_resume_skips_non_safety_steps`: LockDag always at index 0.
- `test_heartbeat_lock_is_holder_bound`: wrong holder returns 0 rows.

---

## [v0.9.0] — Feature Completeness & Ergonomics

**Status:** Released

All roadmap items for v0.9 "Phase 11 — Feature Completeness & Ergonomics" are
complete. This release adds the missing `aqueduct diff` command, hardens the CLI
with new flags, improves CI integration with structured events and exit codes,
and tightens security around secret handling.

### What's new

**E1 — `aqueduct diff` command**  
New `aqueduct diff` subcommand computes a per-table diff between the desired
state (migration files) and the live database state without generating a full
migration plan. Supports `--table` (filter to one table), `--format text|json|yaml|markdown`.

**E2 — `--fail-on-drift` for `aqueduct plan`**  
`aqueduct plan --fail-on-drift` exits with code 1 when any changes are detected
and code 0 when the DAG is already up-to-date. Code 2 is reserved for errors.

**E3 — Interactive confirmation for `aqueduct apply`**  
When running in a TTY, `aqueduct apply` now prompts "Apply N changes to
'target'? [y/N]" before executing. Pass `--yes`/`-y` to skip the prompt (ideal
for CI).

**E4 — Structured `apply_complete` event**  
After a successful apply, a JSON line is emitted on stderr:
`{"event":"apply_complete","migration_id":N,"from_version":M,"to_version":N,"project":"..."}`.
The CI composite action reads this line to populate `migration_id`,
`from_version`, and `to_version` output variables.

**E5 — `--strict` mode for `aqueduct validate`**  
`aqueduct validate --strict` promotes warnings to errors. The command exits
non-zero and reports the number of warnings treated as errors.

**E6 — `yaml`/`yml` plan output format**  
`aqueduct plan --format yaml` now works without adding a `serde_yaml` dependency.
The plan is serialised to YAML manually, mirroring the JSON structure.

**E7 — `status --watch` reconnect fix**  
The watch loop now creates a fresh database connection on each poll tick.
Network errors are caught and logged as warnings so a transient disconnect
does not abort the watch.

**S1 — `${secret:BACKEND:KEY}` inline secret syntax**  
Inline `${secret:BACKEND:KEY}` tokens in DSN strings are now resolved via the
configured secret backend before the connection is established. Supported
backends: `env`, `aws`, `gcp`, `vault`, `sops`, `age`.

**S2 — Path traversal guard for SOPS/Age secrets**  
`validate_secret_path()` rejects key arguments containing `..` components to
prevent path traversal in SOPS and Age subprocess invocations.

**S3 — Plaintext password guard**  
`aqueduct plan` / `apply` / `diff` reject DSN strings containing cleartext
passwords (detected via a regex matching `://user:password@`). Pass
`--allow-plaintext-password` to override, or use `${env:DSN}` / a secret
backend instead.

**P1 — `--quiet` / `--porcelain` global flags**  
`--quiet` and `--porcelain` suppress info-level log output (log level set to
`error`). Useful for scripted and machine-readable workflows.

**P2 — LazyLock for compiled regexes**  
All `regex::Regex` patterns across `config.rs`, `secrets.rs`, `diff.rs`, and
`parser.rs` are now compiled once via `std::sync::LazyLock` rather than on
every call.

**P3 — `cdc_mode` normalization**  
The parser now normalises `cdc_mode` values: `"ROW"` and `"STATEMENT"` map to
`"trigger"`, `"WAL"` maps to `"wal"`, `"NONE"` / `"DISABLED"` map to `"none"`.
Unknown values are rejected at parse time with a clear error message.

**P4 — 8 missing PlanStep variants**  
Added `RecreatePolicy`, `DetachOutbox`, `ReattachOutbox`, `ManageWalSlot`,
`PauseImmediate`, `ResumeImmediate`, `WaitForRefresh`, and `RunHook` to
`PlanStep`. All have `description()` implementations, cost estimates, and
executor handlers.

**P5 — PostgreSQL 18+ only (pg_trickle requirement)**  
Integration tests run against PostgreSQL 18 (`postgres:18-alpine`), the minimum
version required by pg_trickle. The test helper reads `AQUEDUCT_TEST_PG_IMAGE`
to override the container image for future PostgreSQL releases.

**P6 — `cargo audit` security-audit job in CI**  
A dedicated `security-audit` job runs `cargo audit --deny warnings` on every
push and pull request.

**P7 — Coverage threshold enforced at 70%**  
The coverage job uses `--all-targets` and sets `minimum-coverage: 70` with
`fail_ci_if_error: true`.

**P8 — DSN masking in CI composite plan action**  
The `plan` composite action now masks `AQUEDUCT_DSN` before running any steps
(`echo "::add-mask::${AQUEDUCT_DSN}"`), consistent with the `apply` action.

### Breaking changes

- `aqueduct plan` exit codes changed: `0` = no changes, `1` = changes detected
  (or `--fail-on-drift` set), `2` = error. Previous behaviour was to always
  exit 0 on success.
- CLI errors now exit with code `2` instead of `1`, reserving `1` for the
  semantic "changes detected" signal.
- DSN strings containing plaintext passwords are now rejected unless
  `--allow-plaintext-password` is passed.

---

## [v0.8.0] — Core Correctness & Safety Hardening

**Status:** Released

All roadmap items for v0.8 "Core Correctness & Safety Hardening" are complete.
This release closes the gap between pg_aqueduct's design guarantees and its
runtime behaviour: every resumable migration now records step-level progress,
every rollback restores the exact prior spec from the database, and every
`--validate-ivm` call truly validates IVM supportability for differential tables.

### What's new

**C1 — Rollback restores the recorded spec, not migration files**  
`aqueduct rollback` now deserialises the `spec_jsonb` stored alongside each
`dag_versions` row. If the target version was recorded before the M9 fix it
falls back to reading migration files from disk.

**C2 — Resume-aware progress tracking**  
`PlanExecutor` reads the most-recent running migration on startup and skips
steps whose index falls below the recorded `last_completed_step`. After each
non-structural step it persists the step index to `aqueduct.migrations.progress`
so a resumed run continues from the correct point.

**C3 — Plan builder uses `Result<Plan>`**  
`build_plan()` no longer calls `.unwrap()`. All five option accesses now use
`ok_or_else(|| AqueductError::InvariantViolation { context: … })?` and the
function returns `Result<Plan>`. Callers updated throughout.

**C4 — `CreateStreamTable` requires pg_trickle**  
The SQL-injection fallback that created bare tables on non-pg_trickle instances
has been removed. Missing pg_trickle now returns
`AqueductError::PgTrickleNotInstalled` instead of silently creating incorrect
objects.

**C5 — Lock heartbeat**  
A background tokio task renews the advisory lock every 10 s while a plan is
executing. The task creates its own dedicated database connection via the stored
DSN and shuts down cleanly after the plan finishes or errors.

**C6 — `aqueduct init` uses the v2 catalog schema**  
`init.rs` now calls `CATALOG_INIT_V2_SQL` and `ensure_catalog_current()` runs
on every connect to apply the v1→v2 migration if needed.

**H1 — `AlterStreamTable` forwards the new query**  
The executor passes `new_query` as the 6th argument to
`pgtrickle.alter_stream_table($1,$2,$3,$4,$5,$6)` for in-place column changes.
The mock `alter_stream_table` function was updated to accept and persist the
new query.

**H2 — `--validate-ivm` calls the right validator**  
`aqueduct plan --validate-ivm` now calls `validate_ivm_supportability()` for
`DIFFERENTIAL` tables and `validate_sql_syntax()` for non-differential tables,
instead of always calling `validate_sql_syntax()`.

**H3 — Column removal is only InPlace for trailing columns**  
The select-list classifier now uses a prefix-match check: `desired` must be a
leading prefix of `actual`. Removing a column from the middle of the select
list is now correctly classified as Rebuild.

**H6 — Drain-then-pause protocol**  
`run_steps()` calls `pgtrickle.pause_scheduler(nodes)` before the first
migration step and `pgtrickle.resume_scheduler(nodes)` in a deferred cleanup
guard, so live pg_trickle refreshes cannot race with in-flight migrations.

**H7 — Diamond DAG consistency promotion**  
After per-node classification, convergence nodes (stream tables with ≥ 2
classified upstream parents) trigger a group promotion: all transitive ancestors
are elevated to the highest migration class in the group, preventing
inconsistent half-rebuilds.

**H8 — Real drift detection in `aqueduct status`**  
`poll_once()` now loads the migrations directory, reads live state, and computes
a diff to count non-Unchanged deltas. The `--fail-on-drift` flag now works
correctly.

**M9 — `RecordSnapshot` stores the full DagState**  
`spec_jsonb` in `aqueduct.dag_versions` now contains the serialised `DagState`
instead of an empty `{}` object, enabling reliable rollbacks and auditing.

**L9 — Remove unused `deadpool-postgres` dependency**  
The `deadpool-postgres` workspace dependency was never used by any crate and has
been removed from `Cargo.toml`.

### Breaking changes

- `build_plan()` now returns `Result<Plan>` instead of `Plan`. All call sites must
  propagate the error with `?` or unwrap it.
- `CreateStreamTable` for non-pg_trickle environments now returns
  `AqueductError::PgTrickleNotInstalled` instead of silently creating tables.

---

## [v0.7.0] — Documentation & Cookbook *(Planned)*

**Status:** Planned — not yet released

All Phase 8 roadmap items are complete. v0.7 is the Documentation & Cookbook release —
every feature delivered in v0.1–v0.6 is now fully documented with worked examples,
benchmarks, and operational guides.

### What's new

**30 cookbook patterns** (`docs/cookbook/`)  
A complete migration cookbook with 30 worked examples covering every stream-table
evolution class (Free, In-place, Rebuild, Blue/green, Create, Drop). Each pattern
includes a test in `crates/aqueduct-core/tests/integration.rs` verified against a live
Testcontainers PostgreSQL cluster.

- Patterns 01–03: Schedule and CDC mode changes (Free)
- Patterns 04, 17: Refresh mode changes (Free/Rebuild)
- Patterns 05–09: Aggregate column changes (In-place/Rebuild)
- Patterns 10–16: Column rename, JOIN, GROUP BY, WHERE changes (Rebuild)
- Patterns 18–19: Create and drop stream tables
- Patterns 20–23: Multi-node DAG patterns (two-node, three-level, add/remove node)
- Patterns 24–25: Consumer view lifecycle
- Patterns 26–27: Cascade and multi-table schedule changes
- Patterns 28–30: Import roundtrip, rollback, and full lifecycle

**Public benchmarks** (`benchmarks/`)  
Benchmark results for a 200-node DAG with a 5-node change set, comparing
pg_aqueduct targeted apply (~6 ms) against drop/recreate baseline (~180 s) — a
**~28 000× speed improvement** for metadata-only changes on large production DAGs.

**Security guide** (`docs/security.md`)  
Least-privilege role setup, connection string security, secret backends (env, AWS,
GCP, Vault, SOPS, age), read-only transaction guarantees, and audit trail.

**HA operations guide** (`docs/ha-operations.md`)  
Primary detection for Patroni, CloudNativePG, and Stolon; maintenance windows;
concurrent apply protection; crash recovery with `--resume`; failover handling.

**API reference** (`docs/api-reference.md`)  
Complete CLI command reference (plan, apply, validate, lint, status, diff, import,
destroy, rollback, unlock), front-matter directive table, and `aqueduct.toml` schema.

### Test suite

- 197 tests total: 101 unit + 63 integration + 33 CLI
- All 30 new cookbook integration tests pass against Testcontainers PostgreSQL 18
- Zero skipped tests

---

## [v0.6.0] — Production Hardening *(Planned)*

**Status:** Planned — not yet released

All Phase 7 roadmap items are complete. v0.6 takes every feature delivered in
v0.1–v0.5 and subjects it to the rigour required for production deployments:
multi-environment promotion, encrypted secrets, continuous drift watching,
HA-aware primary detection, planner fuzzing, and a clean `destroy` command.

### What's New

#### `aqueduct promote`

New `aqueduct promote` command promotes a validated migrations directory from one
environment to another:

```bash
# Validate source is clean, then promote dev → staging.
aqueduct promote --from dev --to staging

# Skip interactive prompt in CI.
aqueduct promote --from staging --to prod --yes

# Preview the promotion plan without executing.
aqueduct promote --from dev --to staging --dry-run
```

The command:
1. **Validates that the source environment is clean** — no pending drift between the
   migrations directory and the source database.  Pass `--skip-source-check` to bypass
   in pipelines that already run `aqueduct validate`.
2. **Connects to the destination** and computes the migration plan using the
   destination target's `vars = { ... }` (environment-specific schedule, cdc_mode,
   etc.).
3. **Shows the plan** and prompts for confirmation (or skips with `--yes`).
4. **Applies the plan** and records the promotion in `aqueduct.migrations`.

#### Encrypted secret handling

`aqueduct` now supports resolving DSN passwords and full connection strings from
external secret backends:

| Backend | Flag value | Credential source |
|---|---|---|
| Environment variables | `env` (default) | `${VAR}` syntax, unchanged |
| AWS Secrets Manager | `aws` / `aws-secrets-manager` | `AWS_REGION` + AWS SDK credentials |
| GCP Secret Manager | `gcp` / `gcp-secret-manager` | `GOOGLE_APPLICATION_CREDENTIALS` |
| HashiCorp Vault | `vault` / `hashicorp-vault` | `VAULT_ADDR` + `VAULT_TOKEN` |
| SOPS-encrypted file | `sops` | `sops -d <file>` subprocess |
| age-encrypted file | `age` | `age -d -i <identity> <file>` subprocess |

DSN strings now support an inline `${secret:BACKEND:KEY}` syntax:

```toml
[targets.prod]
dsn = "postgresql://app:${secret:vault:database/prod-dsn}@db.example.com/app"
```

Plain `${ENV_VAR}` references continue to work as before.

#### `aqueduct status --watch`

The `status` command now supports a long-running watch mode for integration into
alerting pipelines:

```bash
# Poll every 30 seconds (default).
aqueduct status --watch --to prod

# Custom interval and max drift count.
aqueduct status --watch --interval 1m --max-drift-count 3 --to prod

# JSON output for Alertmanager / Grafana OnCall.
aqueduct status --watch --format json --to prod | jq .
```

`--interval` accepts `Ns` (seconds), `Nm` (minutes), `Nh` (hours), or a plain integer
(seconds).  `--max-drift-count N` causes the watcher to exit non-zero after N
consecutive polls that detect drift.

#### HA integration hardening

`aqueduct` now detects and works correctly with the three major PostgreSQL HA
stacks used in production:

| Stack | Detection method |
|---|---|
| **Patroni** | `pg_stat_activity.application_name ILIKE '%patroni%'` |
| **CloudNativePG** | `app.cnpg.cluster_name` GUC |
| **Stolon** | `pg_stat_activity.application_name ILIKE '%stolon%'` |

New `detect_ha_backend()` function returns a typed `HaBackend` enum so downstream
code can adjust its behaviour (e.g., primary-discovery strategy).

`verify_patroni_primary(endpoint)` performs a lightweight HTTP check against the
Patroni REST API (`GET /master`) to authoritatively confirm primary status before
applying — complementing the existing `pg_is_in_recovery()` check.

#### Planner fuzzing

A new planner fuzzing test (`test_planner_fuzzing_random_mutations`) runs 8
LCG-seeded random toggle mutations over a 4-table DAG and asserts the plan-convergence
invariant: every `plan → apply → plan` cycle must end with an empty plan.  The harness
is deterministic (fixed seed `0xDEAD_BEEF_CAFE_1234`) so failures are reproducible.

#### `aqueduct destroy`

New `aqueduct destroy` command safely tears down all resources owned by a project:

```bash
# Preview what would be destroyed.
aqueduct destroy --to prod --dry-run

# Execute the destruction (requires explicit --confirm).
aqueduct destroy --to prod --confirm
```

The command:
1. Drops all stream tables in reverse topological order (leaves first).
2. Drops consumer views managed by the project.
3. Removes catalog rows from `aqueduct.dag_versions`, `aqueduct.migrations`, and
   `aqueduct.locks`.
4. Does **not** drop the `aqueduct` schema itself (shared with other projects).

The `--confirm` flag is required; the command refuses to run without it unless
`--dry-run` is set.

### Test suite

167 tests total (101 unit + 33 integration + 33 CLI) — all pass, none skipped.
New v0.6 tests cover:
- `test_promote_computes_plan_against_destination`
- `test_promote_source_clean_detects_drift`
- `test_secret_backend_env_resolves` / `test_secret_backend_env_missing_var`
- `test_ha_backend_plain_postgres_is_primary`
- `test_ha_backend_detect_primary`
- `test_patroni_verify_unreachable`
- `test_destroy_project_dry_run` / `test_destroy_project_full`
- `test_destroy_project_dry_run_cli`
- `test_planner_fuzzing_random_mutations`
- `test_secret_backend_all_variants`
- `test_promote_plan_is_empty_when_in_sync`
- `test_resolve_dsn_secrets_env_var`
- `test_status_interval_parse`

---

## [v0.5.0] — dbt Interop *(Planned)*

**Status:** Planned — not yet released

All Phase 6 roadmap items are complete. v0.5 makes `pg_aqueduct` a first-class
participant in dbt-pgtrickle workflows by introducing `aqueduct ingest`, a new command
that reads compiled dbt artefacts and generates a canonical aqueduct migrations
directory automatically.

### What's New

#### `aqueduct ingest --from dbt-target`

New `aqueduct ingest` command ingests compiled dbt artefacts into an aqueduct
migrations directory:

```bash
# After running `dbt compile`:
aqueduct ingest \
  --from dbt-target \
  --target ./target \
  --project-dir .
```

For each model materialised as `stream_table` via the `dbt-pgtrickle` package, the
command:

1. **Reads the compiled SQL** from `target/compiled/` (the output of `dbt compile`).
2. **Generates `migrations/streams/{model_name}.sql`** with the compiled SQL as the
   query body and front-matter directives populated from the dbt model config:
   - `+schedule` → `@aqueduct:schedule`
   - `+refresh_mode` → `@aqueduct:refresh_mode`
   - `+cdc_mode` → `@aqueduct:cdc_mode`
   - `+schema` → `@aqueduct:schema`
   - `+depends_on` → `@aqueduct:depends_on` (overrides auto-derived dependencies)
3. **Generates `migrations/sources/{name}.sql`** for each dbt source referenced by a
   stream-table model (`owned = false`).
4. **Produces a diff report** of what was created, updated, or left unchanged.

The command is **idempotent**: running it again when nothing has changed in the dbt
project writes nothing and exits cleanly.

Output formats:

```bash
aqueduct ingest --from dbt-target --target ./target           # human-readable text
aqueduct ingest --from dbt-target --target ./target --format json  # machine-readable
```

#### Workflow composition

The two tools compose cleanly; neither replaces the other:

- **dbt** generates the SQL (authoring tool for analytics engineers).
- **`aqueduct ingest`** translates the compiled artefacts into a migrations directory
  (one-time or periodic sync).
- **`aqueduct plan` / `aqueduct apply`** manages the migration lifecycle (platform
  team tool).

Once the migrations directory is in place, teams can run `aqueduct plan` to see
precisely what migration class each dbt model change requires — without any full
rebuilds that aren't strictly necessary.

#### `examples/dbt-roundtrip/`

A new worked example in `examples/dbt-roundtrip/` demonstrates the full round-trip:

1. A dbt project with three `stream_table` models (`order_totals`, `promo_summary`,
   `customer_ltv`).
2. A pre-compiled `target/` directory (the output of `dbt compile`).
3. Step-by-step README showing `ingest` → `validate` → `plan` → `apply` → `rollback`.

```
examples/dbt-roundtrip/
  dbt-project/           dbt source project
    models/
      order_totals.sql
      promo_summary.sql
      customer_ltv.sql
    dbt_project.yml
  target/                pre-compiled dbt target directory
    manifest.json
    compiled/analytics/models/
      order_totals.sql
      promo_summary.sql
      customer_ltv.sql
  aqueduct.toml
  README.md
```

---

## [v0.4.0] — CI Integrations & Ergonomics *(Planned)*

**Status:** Planned — not yet released

All Phase 5 roadmap items are complete. v0.4 wires `aqueduct plan` and `aqueduct apply`
into standard CI/CD pipelines and adds `aqueduct fmt` and `aqueduct lint` to make the
migrations directory a first-class software artefact.

### What's New

#### `aqueduct fmt`

New `aqueduct fmt` command canonicalises every migration file:

- **SQL keywords** are upper-cased (`select` → `SELECT`, `group by` → `GROUP BY`).
- **Front-matter directives** are emitted in a stable canonical order
  (`kind`, `owned`, `schema`, `depends_on`, `schedule`, `refresh_mode`, `cdc_mode`,
  `cypher_source`, `source`, `expose_as`).
- **Trailing whitespace** is stripped from every line.
- A single blank line separates front-matter from the SQL body.

`aqueduct fmt --check` exits non-zero if any file is not in canonical format — ideal
for CI gatekeeping.

```bash
# Fix all files in place:
aqueduct fmt

# Check-only mode (CI):
aqueduct fmt --check
```

#### `aqueduct lint`

New `aqueduct lint` command checks migration files for risky patterns:

| Rule | Level | Description |
|---|---|---|
| `schedule-too-aggressive` | warning | Schedule faster than 5 seconds |
| `full-refresh-no-filter` | warning | FULL refresh mode with no WHERE/LIMIT clause |
| `differential-ivm-unsupportable` | warning | DIFFERENTIAL query contains constructs that may trigger auto-downgrade to FULL |
| `cypher-source-missing` | error | `@aqueduct:cypher_source` points to a non-existent file |
| `consumer-source-not-found` | error | Consumer view references a stream table that does not exist |

```bash
aqueduct lint
aqueduct lint --fail-on-warn   # treat warnings as errors (CI strict mode)
aqueduct lint --format json    # machine-readable output
```

#### GitHub Actions

Two new composite actions ship in `.github/actions/`:

**`trickle-labs/pg-aqueduct/.github/actions/plan@v0.4.0`**
- Installs the `aqueduct` binary (pinned version, optional SHA256 checksum).
- Runs `aqueduct plan --format markdown`.
- Posts the plan as a PR comment (creates or updates via idempotent HTML marker).
- Supports `fail-on-drift`, `fail-if-changed`, `suppress-no-op`, and OIDC-free secret
  injection via a named secret.

**`trickle-labs/pg-aqueduct/.github/actions/apply@v0.4.0`**
- Runs `aqueduct apply` with `--resume` support for idempotent CI runs.
- Masks DSN secrets from logs via `::add-mask::`.
- Supports OIDC credential injection for AWS (via `aws-actions/configure-aws-credentials`)
  and GCP (via `google-github-actions/auth`).

Example workflows are provided in `.github/workflows/`:
- `aqueduct-plan.yml` — runs on PRs touching `migrations/` or `aqueduct.toml`
- `aqueduct-apply.yml` — runs on push to `main`

#### GitLab CI Templates

`ci/gitlab/aqueduct.gitlab-ci.yml` provides includable templates for GitLab CI:

```yaml
include:
  - project: 'trickle-labs/pg-aqueduct'
    ref: v0.4.0
    file: 'ci/gitlab/aqueduct.gitlab-ci.yml'
```

Two jobs: `aqueduct:plan` (posts plan as an MR note via GitLab Notes API) and
`aqueduct:apply` (runs on default branch push).

#### Pre-commit Hooks

`ci/hooks/aqueduct-pre-commit` is a drop-in git pre-commit hook that runs
`aqueduct fmt --check` and `aqueduct validate` on every commit that touches `migrations/`.

`.pre-commit-hooks.yaml` defines three hooks for the
[pre-commit](https://pre-commit.com/) framework:

```yaml
repos:
  - repo: https://github.com/trickle-labs/pg-aqueduct
    rev: v0.4.0
    hooks:
      - id: aqueduct-fmt
      - id: aqueduct-validate
      - id: aqueduct-lint
```

### Test Coverage

- 20 CLI integration tests (up from 14 in v0.3)
- 73 unit tests (up from 60 in v0.3)
- 25 database-backed integration tests (unchanged)
- **118 total** — all pass, none skipped

---

## [v0.3.0] — Blue/Green, Preview Environments & Optional Extension *(Planned)*

**Status:** Planned — not yet released

All Phase 4 roadmap items are complete. v0.3 delivers zero-downtime structural DAG
migrations via blue/green deployment, per-branch preview environments backed by
`TABLESAMPLE`-sampled data, consumer view management, and CLI-side support for the
optional `pg_aqueduct` companion extension.

### What's New

#### Blue/Green Deployer

New plan step variants orchestrate a zero-downtime structural migration:

- **`CreateGreenSchema`** — creates a parallel versioned schema for the new DAG
- **`CreateStreamTableInGreen`** — builds stream tables in the green schema
- **`WaitForConvergence`** — waits until the green DAG catches up with live data
- **`SwapConsumerViews`** — atomically redirects all consumer views to the green schema
- **`RetireBlueSchema`** — drops the old blue schema after the configured TTL

#### Consumer View Management

Stream tables can now be consumed through stable view indirection, enabling zero-
downtime blue/green cutover without breaking downstream consumers.

Declare consumer views in `migrations/consumers/`:

```sql
-- migrations/consumers/api_orders.sql
-- @aqueduct:kind = consumer
-- @aqueduct:source = public.order_totals
-- @aqueduct:expose_as = reporting.orders
SELECT customer_id, total FROM public.order_totals WHERE total > 0;
```

New types: `ConsumerSpec`, `ConsumerDelta`, `ConsumerDeltaKind`.  
New plan step: `ManageConsumerView { spec, action }` (create / alter / drop).  
New catalog table: `aqueduct.consumer_views` (catalog schema version bumped to 2).

#### Preview Environments

`aqueduct preview --branch <name>` creates a throwaway copy of the DAG in a scratch
schema with `TABLESAMPLE`-sampled base data:

```
aqueduct preview --branch feat/new-aggregates --sample 10
aqueduct preview --list
aqueduct preview --branch feat/new-aggregates --cleanup
```

Three backends:
- **Native** (default) — scratch schema in the same database, zero infrastructure
- **CloudNativePG** — stub implementation (full support in a future release)
- **Neon** — stub implementation (full support in a future release)

Preview schema names are always `aqueduct_preview_{sanitised_branch_name}` and are
dropped automatically on cleanup.

#### Optional Companion Extension Support

The CLI now detects and uses the optional `pg_aqueduct` companion extension when
present:

- `aqueduct.ddl_log` table tracks DDL events from the extension's event trigger
- `detect_extension_installed()` — returns `true` when the DDL event trigger exists
- `read_ddl_log()` — reads recent DDL events for drift detection

When the extension is absent, the CLI operates in fallback mode with no loss of core
functionality.

#### Catalog Schema v2

The `aqueduct` catalog schema is now at version 2. New tables:

| Table | Purpose |
|---|---|
| `aqueduct.ddl_log` | DDL event log (populated by companion extension) |
| `aqueduct.consumer_views` | Consumer view registry |
| `aqueduct.blue_green_deployments` | Blue/green deployment tracking |

---

## [v0.2.0] — Online Schema Evolution *(Planned)*

**Status:** Planned — not yet released

All Phase 3 roadmap items are complete. Column additions, column removals, and
DIFF→FULL refresh-mode changes are now recognised as zero-rebuild (in-place)
operations. Base-table DDL changes cascade automatically to downstream stream tables.
Per-step cost estimates are available via `aqueduct plan --explain-cost`.

### What's New

#### Full Migration Classifier

The migration classifier now implements the complete v0.2 decision tree:

| Change | Class |
|---|---|
| Schedule, `cdc_mode`, `refresh_mode` DIFF→FULL | Free |
| Add a passthrough or aggregate column | In-place |
| Drop a column from SELECT | In-place |
| Rename a column | Rebuild |
| Change `GROUP BY` keys | Rebuild |
| Change a JOIN condition or add/remove a JOIN | Rebuild |
| Change a `WHERE` predicate | Rebuild |
| Switch `refresh_mode` FULL→DIFF | Rebuild (establishes delta state) |
| Topology change (split or merge nodes) | Blue/green |

Previously, any query change defaulted to Rebuild.  With v0.2, column additions and
removals on structurally-unchanged queries are correctly identified as in-place
migrations — preserving materialized state and avoiding a full table scan.

#### `pg_eddy` Cypher Source Handling

Stream tables backed by `pg_eddy` Cypher queries now use the
`@aqueduct:cypher_source` front-matter directive to point to the `.cypher` source
file.  The directive is recognised by the parser and stored on `StreamTableSpec`
without generating unknown-key lint warnings.

#### ALTER TABLE Cascade Analysis (Tier 1)

When an owned source table's DDL changes, `aqueduct plan` now:
1. Emits an `AlterBaseTable` plan step to execute the DDL.
2. Automatically identifies every stream-table node whose query references the altered
   table (via `sqlparser`-based query analysis).
3. Schedules a coordinated Rebuild cascade for all impacted stream tables in the same
   plan.

This replaces the previous manual workflow of updating source DDL and then separately
planning the stream-table changes.

#### `aqueduct plan --explain-cost`

The new `--explain-cost` flag adds per-step cost estimates to the plan output:

```
Step                          Rows (est)    Duration (est)    Class
───────────────────────────────────────────────────────────────────────────────
ALTER stream order_totals       —             < 1s            rebuild
BACKFILL order_totals           1.2M          ~45s            rebuild
ALTER stream customer_summary   —             < 1s            in-place
BACKFILL customer_summary       340K          ~12s            in-place
```

Row counts are obtained via `EXPLAIN (FORMAT JSON)` and duration is estimated from
`rows × 200 bytes / write_throughput` (default 50 MB/s, calibrated per-cluster by
`aqueduct init`).

#### Testing

- **16 classifier unit tests** covering every decision-tree entry.
- **7 new integration tests** covering: in-place column addition, in-place column
  removal, FULL→DIFF rebuild, `cypher_source` parsing, source DDL cascade analysis,
  `AlterBaseTable` plan step, and `--explain-cost` cost estimation.
- **4 new CLI integration tests** covering: `cypher_source` no-warning, in-place
  plan detection, FULL→DIFF rebuild classification, and cost renderer.
- **Total: 85 tests; none skipped.**

---

## [v0.1.0] — Initial Implementation

**Released:** 2026-05-18
**Tag:** [`v0.1.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.1.0)

The first working release of `pg_aqueduct`. All Phase 0, Phase 1, and Phase 2
roadmap items are complete. The full plan → apply → rollback → import cycle runs
against real PostgreSQL via Testcontainers with no skipped tests.

### What's New

#### Eight CLI Commands

| Command | Description |
|---|---|
| `aqueduct init` | Bootstrap the `aqueduct.*` catalog schema; optionally scaffold a new project |
| `aqueduct plan` | Compute the migration plan from desired state (`.sql` files) vs live state; output as text, JSON, or Markdown |
| `aqueduct apply` | Execute the plan; supports `--dry-run` and `--resume` for crash recovery |
| `aqueduct status` | Show current version, pg_trickle info, and drift summary |
| `aqueduct validate` | Offline syntax and IVM-supportability check — no database required |
| `aqueduct rollback` | Revert to a prior DAG version via a forward migration plan |
| `aqueduct import` | Bootstrap migration files from a live pg_trickle deployment |
| `aqueduct unlock` | Release a stale project lock (emergency use) |

#### Migration Planning

- Parses `migrations/streams/*.sql` and `migrations/sources/*.sql` with
  `-- @aqueduct:key = value` front-matter for `kind`, `schedule`, `refresh_mode`,
  `cdc_mode`, `depends_on`, and `owned`.
- Infers dependency edges from SQL `FROM`/`JOIN` clauses using `sqlparser` (pure
  Rust, no C build dependencies).
- Detects cycles in the DAG and reports them clearly before any DDL runs.
- Classifies every change into one of four cost classes: **Free**, **In-place**,
  **Rebuild**, or **Blue/green**.
- Template variable substitution: `{{ var.NAME }}` expands from per-target `vars`
  in `aqueduct.toml`; `${ENV_VAR}` expands from the environment.

#### Catalog Schema

An `aqueduct.*` schema is bootstrapped on first `aqueduct init`. It tracks:
- `aqueduct.dag_versions` — full plan snapshot at each successful apply
- `aqueduct.migrations` — every migration run with status, progress, and CLI version
- `aqueduct.locks` — project-level serialisation lock (crash-safe via TTL expiry)
- `aqueduct.cluster_profile` — per-cluster calibration data

#### Safety Features

- `allow_full_refresh = false` in `aqueduct.toml` blocks any Rebuild-class plan.
- `maintenance_window` gates Rebuild and Blue/green steps to a time window.
- `aqueduct apply` refuses to run against a hot standby (`pg_is_in_recovery()`).
- `plan` and `status` open read-only transactions and cannot mutate data.
- Locks are automatically cleaned up on crash via TTL expiry.

#### Observability

- Structured JSON logs on stderr when `--log-format json` is set, or when
  `$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`, or `$CIRCLECI` are present.
- `application_name` is set to `aqueduct/<project>/<migration_id>` on every
  connection, making in-flight migrations visible in `pg_stat_activity`.

#### Testing

- **42 unit tests** covering config, parser, DAG, differ, classifier, plan
  builder, renderer, and SQL validator.
- **10 integration tests** using Testcontainers + a mock `pg_trickle` schema
  installed on vanilla PostgreSQL (no real extension required).
- **6 CLI integration tests** covering the full end-to-end lifecycle.
- All 58 tests pass; none are skipped.

#### CI / CD

Five GitHub Actions jobs run on every pull request:
- **Lint** — `cargo fmt --check` + `cargo clippy -D warnings`
- **Unit tests** — `cargo test --lib --all` on Ubuntu and macOS
- **Integration tests** — Testcontainers (Docker) on Ubuntu
- **CLI integration tests** — Testcontainers (Docker) on Ubuntu
- **Coverage** — cargo-tarpaulin on `aqueduct-core`

#### Other

- `examples/minimal/` — 3-node DAG example (`orders` source → `order_totals`
  stream → `customer_tiers` stream) with a complete `aqueduct.toml`.
- `ESSENCE.md` — architecture overview and design principles.
- `justfile` — developer convenience recipes (`test`, `lint`, `fmt`, `coverage`).

---

## [Unreleased] — Repository Bootstrap

### What's New

This is the initial state of `trickle-labs/pg-aqueduct`. No code has been
committed yet; this entry records the project's founding documents and the
design decisions locked in before implementation begins.

#### Design: Standalone Rust CLI, No Extension Required

`pg_aqueduct` is distributed as a standalone Rust CLI binary (`aqueduct`). It
bootstraps its own `aqueduct.*` catalog schema directly over a plain `libpq`
connection on first `aqueduct init` — the same approach used by Atlas, Flyway,
and Liquibase. No `CREATE EXTENSION` is required for core plan/apply/rollback
functionality, which means the tool works on day one against every managed
PostgreSQL service: RDS, Supabase, Neon, Azure Database, AlloyDB, CloudNativePG,
and Citus.

An optional companion extension (`pg_aqueduct`) will be added in v0.3 to enable
DDL event triggers for passive drift detection and SQL-callable diagnostic views.
Both the CLI-only and CLI+extension configurations are first-class and permanently
supported.

#### Design: Pluggable StreamExecutor Trait

All target-system knowledge is isolated behind a `StreamExecutor` trait. The
planner (DAG differ, classifier, plan renderer) is completely target-system-agnostic
and operates on abstract `StreamTableSpec` values. The built-in executor targets
`pg_trickle`; the trait boundary is designed so that RisingWave, Feldera,
Materialize, and Snowflake Dynamic Tables can be added in v2.0 without
restructuring any planning logic.

#### Design: Migration Classification — Four Cost Classes

Every stream-table change is classified into one of four migration classes before
any DDL is emitted:

| Class | Example | Cost |
|---|---|---|
| **Free** | Schedule change, CDC mode change | Single `pgtrickle.alter_stream_table()` call; no data touched |
| **In-place** | Add a passthrough column, add an aggregate, widen a type | `ALTER` + incremental backfill; existing rows preserved |
| **Rebuild** | Change `GROUP BY` keys, change a `WHERE` predicate, rename a column | Drop + recreate + full refresh |
| **Blue/green** | Restructure DAG topology (split or merge nodes) | Build green DAG in parallel, swap consumer views atomically |

This classification prevents the current state of the art — drop every node and
recreate from scratch — from being the only migration option for non-trivial changes
to a production DAG.

#### Design: Unordered Migrations Directory

Migration files have no sequence numbers or timestamps in their filenames.
`aqueduct plan` derives execution order entirely from DAG topology (dependency
edges extracted by `pg_query.rs`). This eliminates the merge-conflict problem
that plagues Flyway and Alembic when two branches add migrations concurrently:
two PRs can each add a new `.sql` file independently, and when both merge, the
next `aqueduct plan` sees two new nodes and orders them by their dependency
edges.

#### Design: Rollback Semantics and the Lossless Window

`aqueduct rollback` is always a single forward migration plan from the current
state to the desired prior spec — it never replays individual rollbacks in
reverse. For Free and In-place migrations rollback is always lossless. For
Rebuild-class migrations, rollback is lossless only within the current migration
window (before the next full refresh completes). The CLI reports the lossless
window expiry time and requires `--accept-data-loss` if the window has passed.

#### Design: Diamond DAG Consistency During Migration

Stream tables with `diamond_consistency = 'atomic'` membership are always
migrated as a unit. If any member of a diamond group is classified as Rebuild,
all members are upgraded to Rebuild and migrated together in a single locked
batch. The plan renderer explains the promotion. No migration may place diamond
group members in different schemas during a blue/green deployment.

#### Design: Security Posture

The CLI refuses plaintext passwords in config files unless `--allow-plaintext-password`
is explicitly set. DSN values in `aqueduct.toml` use environment variable references
(`${AQUEDUCT_PROD_DSN}`). The `plan` and `status` commands open read-only
transactions (`SET TRANSACTION READ ONLY`) and cannot accidentally mutate data.
Every `aqueduct apply` records the authenticated Postgres role, client IP, CLI
version, and full plan in `aqueduct.migrations` for compliance audit trails. A
documented `aqueduct_admin` least-privilege role requires no superuser, no
`CREATEROLE`, and no replication.

#### Documents Added

- `README.md` — project overview, quickstart, and positioning relative to dbt,
  `pg_trickle`, Atlas, and the broader `trickle-labs` ecosystem.
- `ROADMAP.md` — eight-version roadmap (v0.1 through v2.0) with full deliverable
  detail for each phase.
- `CHANGELOG.md` — this file.
- `plans/pg-aqueduct-plan.md` — the full design document: problem statement,
  architecture, implementation plan, 24 risk and open-question analyses, and the
  multi-executor capability profiles.
