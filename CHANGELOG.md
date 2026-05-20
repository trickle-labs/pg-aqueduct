# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For planned future versions, see [ROADMAP.md](ROADMAP.md).

## [Unreleased]

## [0.17.0] - 2026-06-10

### Added

- `--patroni-endpoint <url>` flag on `aqueduct apply`. Between each migration step,
  the CLI calls `GET <endpoint>/master`; a non-200 response marks the migration
  `interrupted` and aborts (DOC-2 / v0.17-A).
- `aqueduct_core::ha` module: `HaBackend` enum, `detect_ha_backend()`,
  `check_still_primary()` (uses `pg_is_in_recovery()`), and `check_patroni_primary()`
  HTTP check (DOC-2 / v0.17-A).
- Between-step primary re-check via `SELECT pg_is_in_recovery()` in the executor.
  On failover, migration is marked `interrupted` (v0.17-A).
- Per-step JSON events emitted to stderr: `step_start`, `step_complete`, `step_failed`
  with `step_index`, `step_type`, `migration_id`, `project`, and `duration_ms` fields
  (v0.17-B). Documented in `docs/cli-events-schema.json`.
- `aqueduct audit` subcommand: lists recent migrations with step counts, error messages,
  and durations. Supports `--format table|json|yaml` and `--limit N` (v0.17-B).
- `aqueduct.migration_history` SQL view (catalog v8): joins `aqueduct.migrations` and
  `aqueduct.migration_steps` for per-step audit queries (v0.17-B).
- `aqueduct_core::catalog::LIST_MIGRATIONS_FOR_AUDIT_SQL` query constant (v0.17-B).
- Prometheus metrics endpoint behind `--features metrics` on `aqueduct-cli`.
  Pass `--metrics-addr 0.0.0.0:9090` to expose `/metrics` during `apply` (v0.17-B).
- Catalog schema v8: adds `'interrupted'` to the `migrations.status` check constraint
  and a partial index for `interrupted` rows. Includes auto-migration from v7 (v0.17-A).
- GitHub-native build provenance attestation in release workflow via
  `actions/attest-build-provenance@v2`. Verify with `gh attestation verify` (v0.17-C).
- `release-verify` CI job: downloads the linux-amd64 release archive, verifies its
  SHA256 checksum, and confirms `aqueduct --version` matches the tag (v0.17-C).
- Version linting CI step (`version-lint` job): fails if README.md status banner,
  `docs/installation.md`, and `Cargo.toml` workspace version disagree (v0.17-D).
- `just publish-dry-run` recipe for dry-run crates.io publish verification (v0.17-C).

### Changed

- Catalog schema bumped from v7 to v8 (`CATALOG_SCHEMA_VERSION = 8`).
- `aqueduct-core` and `aqueduct-testkit` Cargo.toml: `publish = true`;
  `aqueduct-cli`: `publish = false` (v0.17-C).
- `docs/ha-operations.md` rewritten to accurately describe implemented behaviors:
  `check_still_primary()`, Patroni `/master` check, per-step events, `aqueduct audit`,
  and the recovery runbook. Speculative `system_identifier`/`timeline_id` content removed
  (v0.17-D).
- `docs/installation.md`: updated version example to `0.17.0`, MSRV to 1.88,
  added build provenance verification section (v0.17-C/D).
- `ci.yml` security-audit job: removed `--ignore RUSTSEC-2025-0052` (httpmock replaced).
- `httpmock` replaced with `wiremock` in `aqueduct-core` dev-dependencies; all five
  HTTP mock tests updated to the async wiremock API (v0.17-D).

## [0.16.0] - 2026-05-20

### Added

- `OutputMode` enum (`Human`, `Json`, `Yaml`, `Quiet`, `Porcelain`) and
  `OutputEmitter` struct in `aqueduct-cli::output`; global `--quiet` and
  `--porcelain` flags are now routed through the emitter (ERG-1, M11).
- `serde_yaml` workspace dependency; YAML output in `plan`, `status`, and `diff`
  subcommands now uses `serde_yaml::to_string` instead of hand-rolled format strings,
  correctly escaping quotes, colons, and newlines (ERG-3).
- `just gen-docs` recipe that regenerates `docs/api-reference.md` from
  `aqueduct --help` output; stale references to `--patroni-endpoint`, `--timeout`,
  `--lock-timeout`, and `--no-cost` removed (ERG-4, M3).
- Binary CLI tests (`assert_cmd`) for every subcommand's exit codes and `--help`
  output: `quiet_suppresses_decorative_output`, `porcelain_outputs_key_value_only`,
  `yaml_escapes_quotes_and_newlines`, `plan_help_documents_fail_if_changed`,
  `apply_help_documents_dry_run`, `status_help_documents_fail_on_drift`,
  `diff_help_documents_fail_on_drift`, `destroy_help_documents_dry_run`,
  `rollback_help_documents_dry_run`, `plan_missing_dsn_exits_2`,
  `validate_exits_0_on_valid_files`, `validate_differential_ivm_unsupportable_fails`,
  `validate_format_json_produces_valid_json`, `version_flag_outputs_semver` (TEST-2).
- `--poll-interval` flag on `aqueduct status` (PERF-2).
- Spec-hash caching in `status --watch`: migration files are only reloaded when any
  `.sql` mtime changes; live state always polled fresh (PERF-2).
- `postgres_version_matrix_min_supported` integration test verifying the full
  create/apply/status cycle on PostgreSQL 18+ (H10).
- `CONTRIBUTING.md` documenting Docker/Testcontainers prerequisites, `just` recipes,
  integration test environment variables, PG version matrix, coding standards, and
  a step-by-step guide for adding cookbook recipes.

### Changed

- `fmt::format_migration` now returns `Result<Option<String>>` and propagates IO
  errors; `aqueduct fmt` prints a diagnostic when a file cannot be read instead of
  silently treating it as empty (L2).
- `CANONICAL_KEY_ORDER` constant in `aqueduct-core::fmt` renamed to
  `KNOWN_FRONTMATTER_KEYS` to reflect its actual role (L3).
- `TestDb::connection_string` field visibility narrowed from `pub` to `pub(crate)`
  to prevent external crates from depending on the internal representation (L14).
- `docs/security.md`: AWS, GCP, and Vault secret backend rows updated from
  "Planned (v0.12)" to "Implemented"; injected SQL trust-boundary note updated to
  describe the `CatalogSchema` newtype introduced in v0.14.
- `docs/api-reference.md` regenerated from `aqueduct --help` output.

### Fixed

- `aqueduct status --format yaml` now produces valid YAML that passes
  `serde_yaml::from_str` round-trip including special characters.
- `aqueduct plan --format yaml` correctly escapes project names containing
  quotes, colons, and newlines.

## [0.15.0] - 2026-05-20

### Added

- Saga-style compensating steps: `CreateStreamTable` and `DropStreamTable` write a
  compensating action to `aqueduct.ddl_log` before executing DDL, enabling crash
  recovery via `--resume`.
- `--force-retry <step-index>` and `--force-skip <step-index>` flags on
  `aqueduct apply` to advance past ambiguous steps (require `--yes`).
- `CatalogSchema` newtype validating catalog schema names at parse time, rejecting SQL
  injection markers, non-identifier characters, leading digits, and reserved system
  schema names (`pg_catalog`, `information_schema`, all `pg_*` prefixed names).
- `ExecutionContext` enum with `Apply`, `Rollback`, `Promote`, and `DryRun` variants
  replacing the optional builder pattern; named constructors `for_apply()`,
  `for_rollback()`, and `for_promote()`.
- `StartBlueGreenDeployment` plan step inserting a row into
  `aqueduct.blue_green_deployments` with `status = 'active'` as the first post-lock
  step.
- All `SwapConsumerViews` steps wrapped in a single `BEGIN ... COMMIT` block for an
  all-or-nothing consumer view swap.
- Real RLS policy capture via `pg_policies` / `pg_class.relrowsecurity` before
  `DropStreamTable`; `RecreatePolicy` step restores policies after table recreation;
  failures are non-fatal and logged to `aqueduct.ddl_log`.
- `rebuild-may-affect-rls` lint warning when a `FULL`-mode table has RLS enabled.
- Batch convergence polling: `WaitForConvergence` issues a single
  `SELECT ... WHERE table_name = ANY($2)` per poll interval instead of one query per
  node.
- `convergence_poll_interval_ms` (default 500 ms) and `convergence_timeout_secs`
  (default 300 s) options in `BuildPlanOptions`.
- `validate_migration_files_diagnostic` and `validate_dag_diagnostic` public functions
  returning structured `DiagnosticSet` entries with file paths, error codes, and
  severity levels.
- `aqueduct validate --format json` output matching the `lint` JSON schema.
- Catalog schema v7: `aqueduct.ddl_log` gains `migration_id bigint` and
  `compensating_sql text`; `aqueduct.blue_green_deployments` gains
  `rolled_back_at timestamptz` and an updated `bg_status_check` constraint accepting
  `'rolled_back'`.

### Changed

- Executor writes `status = 'swapped'` after the atomic consumer view swap and
  `status = 'retired'` after `RetireBlueSchema`.
- Integration tests now target `postgres:18-alpine` exclusively (PostgreSQL 18+
  required by pg_trickle).

## [0.14.0] - 2026-05-20

### Added

- `pg_version` field in `aqueduct status --format json` output.
- `pgtrickle_mock.scheduler_state` table in catalog schema v6; mock
  `pause_scheduler` / `resume_scheduler` functions insert and delete rows to assert
  scheduler state transitions in tests.
- Nine new integration tests covering all CORR, SEC, and TEST items.

### Fixed

- Progress preserved on recoverable failures; migration marked `recoverable_failure`
  and resumable (CORR-1).
- Heartbeat correctly signals the main executor loop via `tokio::sync::watch` when
  the advisory lock is lost (CORR-3).
- Promotion filters destination state by project (CORR-4).
- Promote command migrates catalog before use and records `spec_jsonb` (CORR-5).
- Consumer view drops now delete the catalog row so drift counts remain accurate
  (CORR-7).
- Status drift counting now sums stream, source, and consumer deltas (CORR-8).
- `aqueduct destroy` exits with code 2 on error via `anyhow::bail!` instead of
  `std::process::exit(1)`.
- DSN-redaction regex compiled once via `LazyLock` instead of per-call.
- CI environment variable detection now checks for value `"true"` rather than mere
  presence, preventing false-positive CI detection.
- GitHub Actions composite `plan` and `apply` actions now map runner OS/architecture
  to the correct release archive suffix.
- `action-smoke` workflow exercises composite actions end-to-end with a
  locally-packaged archive.
- `mdbook test` is now a blocking CI step; docs-lint fails if any documented
  subcommand is missing from the binary.

### Security

- Keyword-value DSNs (e.g. `host=... password=secret`) now also checked for plaintext
  passwords, matching URI-style DSN validation (SEC-1).
- Consumer SQL bodies validated as a single `SELECT` statement before view creation,
  preventing DDL injection via migration files (SEC-2).
- `connect_read_only` now issues `BEGIN READ ONLY` before `SET LOCAL
  statement_timeout`; `sanitize_statement_timeout` allowlist prevents injection via
  the timeout argument (SEC-3).

## [0.13.0] - 2026-05-20

### Added

- Full blue/green migration step sequence for topology-restructuring diffs
  (`--strategy blue-green`): `CreateGreenSchema`, `CreateStreamTableInGreen`,
  `WaitForConvergence`, `SwapConsumerViews`, `RetireBlueSchema`.
- `IMMEDIATE` refresh mode parsed from `-- @aqueduct:refresh_mode = "IMMEDIATE"`,
  with `PauseImmediate` / `ResumeImmediate` plan steps.
- `--no-immediate-downgrade` flag rejecting plans that would temporarily degrade an
  `IMMEDIATE` table.
- Pre/post migration hooks from `[apply.hooks]` in `aqueduct.toml` emitted as
  `RunHook` plan steps; pre-hook fires after `LockDag`, post-hook fires before
  `RecordSnapshot`.
- `aqueduct plan --out <file>` writes a plan with a SHA-256 spec hash;
  `aqueduct apply --plan <file>` executes the pre-computed plan and validates the spec
  hash to reject stale artifacts.
- Catalog schema v5: `aqueduct.migration_steps` records every executed step with
  `running` / `done` / `failed` status for audit and replay.
- `aqueduct import` now calls `ensure_catalog_current` and records version 1 in
  `aqueduct.dag_versions`; a subsequent `aqueduct plan` returns an empty plan.
- Real `create_preview_cnpg` and `create_preview_neon` HTTP implementations using the
  CNPG Kubernetes API and the Neon management API.
- Rollback classification: `Safe`, `PointInTime`, or `DataLoss` based on WAL retention
  window; `--within-window` / `--accept-data-loss` flags.
- `"schema_version": 1` field in all structured JSON CLI events; schema published at
  `docs/cli-events-schema.json`.

## [0.12.0] - 2026-05-20

### Added

- `PgtrickleCaps` struct probing the installed pg_trickle version at startup via
  `to_regprocedure`; each capability individually gated for graceful degradation on
  older builds.
- `WaitForRefresh` plan step gated on `pgtrickle_caps.has_refresh_status`; older
  deployments skip this step.
- Real AWS Secrets Manager (inline SigV4 signing, no AWS SDK dependency), GCP Secret
  Manager (bearer token), and HashiCorp Vault (KV v2) HTTP clients using `reqwest`
  with `rustls-tls`; all three verified with `httpmock` unit tests.
- `build_source_index()` building a `HashMap<QualifiedName, Vec<QualifiedName>>` once
  per diff; `compute_source_deltas()` performs O(1) cascade impact lookups.
- Catalog schema v4 with four new performance indexes on `dag_versions`, `migrations`,
  and `locks` tables.
- `plan_stats()` public function computing `PlanSummary` from `&[PlanStep]` without
  executing.
- `collect_subgraph()` computing the transitive dependency closure for an anchor
  table; `--table` flag on `aqueduct preview` limits preview to that subgraph.
- Full integration test suite in a `pgtrickle-integration` CI job against PostgreSQL
  18 with mock pg_trickle schema.
- `test_blue_green_topology_restructure` covering diamond DAG creation.
- Binary-level CLI tests using `assert_cmd` and `predicates`.
- Project isolation destructive test: applies two projects, destroys one, asserts the
  other is untouched.
- Property-based fuzz tests via `proptest`: `build_plan` never panics for arbitrary
  inputs; `plan_stats` always consistent with `build_plan.summary`.
- Tutorial smoke test parsing bash code blocks in docs and validating known
  subcommands.
- `action-smoke.yml` workflow: builds binary, packages as versioned archive, exercises
  `plan` and `apply` against a live PostgreSQL 18 service container.
- `reqwest 0.12` (rustls-tls), `proptest`, `assert_cmd`, `predicates`, and `httpmock`
  added as dependencies.

### Changed

- `connect_read_only()` and `connect_read_only_with_timeout()` now issue
  `SET LOCAL statement_timeout` before `BEGIN READ ONLY`.
- `estimate_rows()` returns typed `CostError` instead of `anyhow::Error`; SQL errors
  and unsupported plan types surfaced as structured tracing warnings.
- Coverage threshold updated to 60% workspace-wide minimum.
- `build-artifacts` in `release.yml` now requires the `test` job to pass.
- macOS Intel (`macos-13`) added to release build matrix as `macos-amd64`.
- `rust-version` in `Cargo.toml` set to `1.88` (effective MSRV).

### Fixed

- Windows builds in `release.yml` now fail the release when they fail.

### Security

- `rewrite_query_for_preview_ast()` rewrites table references using the sqlparser AST
  instead of string substitution, eliminating query injection risk via schema or table
  names.

## [0.11.0] - 2026-05-19

### Added

- Unified `Diagnostic` type and `DiagnosticSet` with file/line/column context,
  replacing separate `ValidationError`, `LintDiagnostic`, and ad-hoc error strings.
- Stable numeric error codes on `AqueductError`: 1000–1099 database, 1100–1199
  configuration, 1200–1299 planning, 1300–1399 execution, 1400–1499 catalog.
- `--fail-on-drift` flag for `aqueduct diff`, exiting 1 when any delta is
  non-Unchanged.
- YAML output format for `aqueduct status` (`--format yaml`).
- `docs/roles.sql` with SQL grant templates for `aqueduct_planner`,
  `aqueduct_applier`, `aqueduct_preview`, and `aqueduct_destroy` roles.
- `docs-build` CI job running `mdbook build` and `mdbook test` on every push.
- `docs-lint` CI job verifying every documented subcommand responds to `--help` and
  that binary `--version` matches `Cargo.toml`.
- `docs-cli` justfile recipe generating a Markdown CLI reference from `--help` output.

### Changed

- `aqueduct plan` now exits 0 by default; exits 1 only when `--fail-if-changed` or
  `--fail-on-drift` is explicitly set.
- `executor.execute()` returns `ExecutionResult { migration_id, dag_version }` instead
  of a bare `u64`; `apply_complete` JSON event emits both fields as separate keys.
- `ManageWalSlot` uses typed `WalSlotAction` enum (`Create` / `Drop`) instead of a
  free-form string, eliminating the "Unknown action" dead code path.
- Typed `PlanExecutor` constructors: `for_apply()`, `for_rollback()`,
  `for_promote()`, `for_dry_run()`.
- Typed DAG state newtypes: `DesiredDagState`, `LiveDagState`, `RecordedDagState`.
- `lib.rs` exports `CLI_VERSION = env!("CARGO_PKG_VERSION")` as the single source of
  version truth.
- Example workflow version pins updated from `@v0.4.0` to `@v0.11.0`.

### Fixed

- `aqueduct plan` and `aqueduct status` now use `connect_read_only()` and cannot
  accidentally mutate data.

### Security

- `redact_dsn()` replaces passwords with `***` in all connection strings shown in
  error messages, covering both URL-form and keyword-value DSNs.

## [0.10.0] - 2026-05-19

### Added

- Catalog schema v3: `aqueduct.stream_table_ownership(project, schema_name,
  table_name, managed_since)` with `PRIMARY KEY (schema_name, table_name)`.
- `recoverable_failure` added to the `aqueduct.migrations` `status_check` constraint.
- Six new integration tests: consumer-only apply, concurrent apply race, stale plan
  detection, two-project isolation, resume step skipping, and holder-bound heartbeat.

### Fixed

- Consumer-only plans are no longer silently skipped as no-ops; `PlanSummary` tracks
  `consumer_creates`, `consumer_drops`, and `consumer_alters`, and `is_empty()` /
  `total_changes()` include consumer deltas.
- `RecordSnapshot` uses `RETURNING version` to capture the actual bigserial value
  assigned by the database.
- `read_live_state()` populates `sources` from `spec_jsonb` in `aqueduct.dag_versions`
  instead of always returning empty.
- `read_live_state()` and `read_live_consumers()` filter by project via
  `stream_table_ownership`; `destroy_project` verifies ownership before dropping
  tables (`--force-unowned` to bypass).
- `check_pgtrickle_version()` probes `to_regprocedure` before calling to avoid a
  database error when pg_trickle is absent.
- `classify_delta()` no longer panics on missing `desired` / `actual` in the
  `AlterQuery` arm; returns `Rebuild` instead.
- Failed migrations now marked `recoverable_failure` and resumable; `LockDag` always
  re-executes on resume, DDL steps skipped only for steps already confirmed complete.
- Scheduler pause now runs inside the `LockDag` executor arm after the lock is
  acquired, preventing a pre-lock pause from affecting other projects.
- Heartbeat SQL is now holder-bound (`WHERE project = $1 AND holder = $2`); zero rows
  affected logged as a lock-stolen warning.
- Project lock released on all exit paths via an explicit cleanup block.
- Step progress checkpoint writes are now fatal rather than silently continuing.
- `aqueduct apply --dry-run` uses a plain connection to avoid upgrading the catalog
  schema during previews.
- `DROP TABLE` in `destroy` and executor no longer uses `CASCADE`; opt in with
  `--force-cascade`.
- `aqueduct rollback` now enforces `--accept-data-loss` when the rollback plan
  contains rebuild-class steps.
- First-time stream table creations increment `create_count`, not `rebuild_count`,
  preventing false-positive maintenance-window blocking on initial deployments.
- Lock holder string is now `aqueduct/{version}/{hostname}/{pid}/{ts}`, making
  concurrent processes distinguishable.
- Scheduler pause failure is now fatal by default.

## [0.9.0] - 2026-05-19

### Added

- `aqueduct diff` subcommand computing a per-table diff between desired state
  (migration files) and live database state; supports `--table` and
  `--format text|json|yaml|markdown`.
- `aqueduct plan --fail-on-drift`: exits 1 when changes are detected, 0 when the DAG
  is up-to-date.
- Interactive confirmation prompt on `aqueduct apply` in a TTY ("Apply N changes?
  [y/N]"); `--yes` / `-y` skips it.
- Structured `apply_complete` JSON event on stderr with `migration_id`,
  `from_version`, `to_version`, and `project` fields; CI composite action reads these
  as output variables.
- `aqueduct validate --strict`: promotes warnings to errors and exits non-zero.
- `aqueduct plan --format yaml` output.
- `${secret:BACKEND:KEY}` inline secret syntax in DSN strings; supported backends:
  `env`, `aws`, `gcp`, `vault`, `sops`, `age`.
- `--quiet` / `--porcelain` global flags suppressing info-level log output.
- Eight new `PlanStep` variants: `RecreatePolicy`, `DetachOutbox`, `ReattachOutbox`,
  `ManageWalSlot`, `PauseImmediate`, `ResumeImmediate`, `WaitForRefresh`, `RunHook`.
- `cargo audit` security-audit CI job running on every push and pull request.
- Coverage threshold enforced at 70% with `fail_ci_if_error: true`.
- DSN masking in CI composite `plan` action via `::add-mask::`.

### Changed

- `aqueduct plan` exit codes changed: `0` = no changes, `1` = changes detected (or
  `--fail-on-drift`), `2` = error.
- CLI errors now exit with code `2` instead of `1`.
- All `regex::Regex` patterns compiled once via `std::sync::LazyLock`.
- `cdc_mode` values normalized at parse time: `"ROW"` / `"STATEMENT"` → `"trigger"`,
  `"WAL"` → `"wal"`, `"NONE"` / `"DISABLED"` → `"none"`; unknown values rejected with
  a clear error.
- Integration tests run against PostgreSQL 18 (`postgres:18-alpine`).

### Fixed

- `status --watch` creates a fresh database connection on each poll tick; network
  errors logged as warnings rather than aborting the watch.

### Security

- `validate_secret_path()` rejects key arguments containing `..` to prevent path
  traversal in SOPS and Age subprocess invocations.
- DSN strings containing cleartext passwords are now rejected; pass
  `--allow-plaintext-password` to override.

## [0.8.0] - 2026-05-19

### Added

- Resume-aware step-level progress tracking: `PlanExecutor` skips steps below
  `last_completed_step` on resume and persists the step index to
  `aqueduct.migrations.progress` after each non-structural step.
- Lock heartbeat: background tokio task renewing the advisory lock every 10 s via a
  dedicated connection; shuts down cleanly after plan completion or error.
- Catalog schema v2 via `CATALOG_INIT_V2_SQL`; `ensure_catalog_current()` applies the
  v1→v2 migration on every connect.

### Changed

- `build_plan()` now returns `Result<Plan>` instead of `Plan`; all call sites
  propagate the error with `?`.
- `AlterStreamTable` executor passes `new_query` as the 6th argument to
  `pgtrickle.alter_stream_table`.
- `aqueduct plan --validate-ivm` now calls `validate_ivm_supportability()` for
  `DIFFERENTIAL` tables and `validate_sql_syntax()` for others.
- Column removal classified as `Rebuild` unless the column is in the trailing position
  of the select list.
- `run_steps()` uses a drain-then-pause protocol: `pause_scheduler` before the first
  migration step, `resume_scheduler` in a deferred cleanup guard.
- Diamond DAG convergence nodes trigger group promotion: all transitive ancestors
  elevated to the highest migration class in the group.
- `poll_once()` in `aqueduct status` loads the migrations directory and computes a
  real diff for accurate drift detection.
- `spec_jsonb` in `aqueduct.dag_versions` now stores the full serialised `DagState`
  instead of an empty `{}`.

### Fixed

- `aqueduct rollback` now deserialises `spec_jsonb` from the `dag_versions` row
  instead of re-reading migration files from disk.
- `CreateStreamTable` on non-pg_trickle instances returns
  `AqueductError::PgTrickleNotInstalled` instead of silently creating bare tables.

### Removed

- Unused `deadpool-postgres` workspace dependency.

## [0.7.0] - 2026-05-18

### Added

- 30 cookbook patterns in `docs/cookbook/` covering every migration class (Free,
  In-place, Rebuild, Blue/green, Create, Drop), each with an integration test.
- Public benchmarks in `benchmarks/`: 200-node DAG, 5-node change set — targeted
  apply (~6 ms) vs. drop/recreate (~180 s), ~28 000x speed improvement.
- `docs/security.md`: least-privilege role setup, secret backends, read-only
  guarantees, and audit trail.
- `docs/ha-operations.md`: primary detection for Patroni, CloudNativePG, and Stolon;
  maintenance windows; crash recovery; failover handling.
- `docs/api-reference.md`: complete CLI command reference, front-matter directive
  table, and `aqueduct.toml` schema.
- 197 tests total (101 unit + 63 integration + 33 CLI); all passing, none skipped.

## [0.6.0] - 2026-05-18

### Added

- `aqueduct promote` command promoting a migrations directory from one environment to
  another, with source-clean validation, destination plan computation, interactive
  confirmation, and promotion recorded in `aqueduct.migrations`.
- Encrypted secret backends: AWS Secrets Manager, GCP Secret Manager, HashiCorp
  Vault, SOPS-encrypted files, and age-encrypted files.
- `${secret:BACKEND:KEY}` inline syntax in DSN strings.
- `aqueduct status --watch` long-running watch mode with configurable `--interval`,
  `--max-drift-count`, and JSON output for alerting pipelines.
- HA primary detection for Patroni (HTTP `GET /master`), CloudNativePG (GUC), and
  Stolon (`pg_stat_activity`), returning a typed `HaBackend` enum.
- Planner fuzzing test: 8 LCG-seeded random mutations over a 4-table DAG asserting
  the plan-convergence invariant (deterministic seed `0xDEAD_BEEF_CAFE_1234`).
- `aqueduct destroy` command tearing down all project-owned stream tables, consumer
  views, and catalog rows; requires `--confirm` or `--dry-run`.
- 167 tests total (101 unit + 33 integration + 33 CLI); all passing, none skipped.

## [0.5.0] - 2026-05-18

### Added

- `aqueduct ingest --from dbt-target` command reading compiled dbt artefacts and
  generating a canonical aqueduct migrations directory; idempotent.
- Front-matter directives populated from dbt model config (`+schedule`,
  `+refresh_mode`, `+cdc_mode`, `+schema`, `+depends_on`).
- `examples/dbt-roundtrip/` demonstrating the full ingest → validate → plan → apply
  → rollback round-trip.

## [0.4.0] - 2026-05-18

### Added

- `aqueduct fmt` command canonicalising migration files: SQL keyword casing, stable
  front-matter directive order, trailing whitespace removal. `--check` mode for CI.
- `aqueduct lint` command with five rules: `schedule-too-aggressive` (warning),
  `full-refresh-no-filter` (warning), `differential-ivm-unsupportable` (warning),
  `cypher-source-missing` (error), `consumer-source-not-found` (error).
  `--fail-on-warn` and `--format json` flags.
- `trickle-labs/pg-aqueduct/.github/actions/plan@v0.4.0` composite action: installs
  binary, runs `aqueduct plan --format markdown`, posts plan as PR comment.
- `trickle-labs/pg-aqueduct/.github/actions/apply@v0.4.0` composite action: runs
  `aqueduct apply` with `--resume` support, masks DSN secrets.
- Example workflows: `aqueduct-plan.yml` (on PRs touching `migrations/`) and
  `aqueduct-apply.yml` (on push to `main`).
- GitLab CI templates in `ci/gitlab/aqueduct.gitlab-ci.yml`.
- `ci/hooks/aqueduct-pre-commit` git hook running `fmt --check` and `validate`.
- `.pre-commit-hooks.yaml` defining `aqueduct-fmt`, `aqueduct-validate`, and
  `aqueduct-lint` hooks for the pre-commit framework.
- 118 tests total (73 unit + 25 integration + 20 CLI); all passing, none skipped.

## [0.3.0] - 2026-05-18

### Added

- Blue/green plan step variants: `CreateGreenSchema`, `CreateStreamTableInGreen`,
  `WaitForConvergence`, `SwapConsumerViews`, `RetireBlueSchema`.
- Consumer view management: declare views in `migrations/consumers/` with
  `@aqueduct:kind = consumer`, `@aqueduct:source`, and `@aqueduct:expose_as`;
  new `ConsumerSpec`, `ConsumerDelta`, and `ConsumerDeltaKind` types;
  `ManageConsumerView` plan step.
- `aqueduct preview --branch <name>` creating a throwaway copy of the DAG in a
  scratch schema with `TABLESAMPLE`-sampled data; native, CloudNativePG stub, and
  Neon stub backends.
- Optional companion extension support: `aqueduct.ddl_log` DDL event log,
  `detect_extension_installed()`, `read_ddl_log()`.
- Catalog schema v2: `aqueduct.ddl_log`, `aqueduct.consumer_views`,
  `aqueduct.blue_green_deployments` tables.

## [0.2.0] - 2026-05-18

### Added

- Full migration classifier decision tree: Free (schedule / `cdc_mode` /
  `refresh_mode` DIFF→FULL), In-place (add or drop a column), Rebuild (rename column,
  change `GROUP BY`, change or add/remove a JOIN, change `WHERE`, FULL→DIFF), and
  Blue/green (topology restructure).
- `@aqueduct:cypher_source` front-matter directive for pg_eddy Cypher-backed stream
  tables.
- ALTER TABLE cascade analysis (Tier 1): `AlterBaseTable` plan step plus automatic
  Rebuild cascade for all stream tables referencing the altered source table.
- `aqueduct plan --explain-cost` showing per-step row counts (via `EXPLAIN FORMAT
  JSON`) and duration estimates.
- 85 tests total (unit + integration + CLI); all passing, none skipped.

## [0.1.0] - 2026-05-18

### Added

- `aqueduct init`, `plan`, `apply`, `status`, `validate`, `rollback`, `import`, and
  `unlock` CLI commands.
- `aqueduct.*` catalog schema: `dag_versions`, `migrations`, `locks`, and
  `cluster_profile` tables.
- SQL front-matter parsing (`-- @aqueduct:key = value`) for `kind`, `schedule`,
  `refresh_mode`, `cdc_mode`, `depends_on`, and `owned` directives.
- DAG dependency inference from SQL `FROM` / `JOIN` clauses via `sqlparser` (pure
  Rust, no C build dependencies).
- Cycle detection in the DAG with clear pre-DDL error reporting.
- Migration classification into four cost classes: Free, In-place, Rebuild, and
  Blue/green.
- Template variable substitution: `{{ var.NAME }}` from per-target `vars` in
  `aqueduct.toml`; `${ENV_VAR}` from the environment.
- `allow_full_refresh = false` config blocking Rebuild-class plans.
- `maintenance_window` config gating Rebuild and Blue/green steps to a configured
  time window.
- Standby detection via `pg_is_in_recovery()`, preventing applies against hot
  standbys.
- Read-only transactions for `plan` and `status` commands.
- Advisory lock with TTL expiry for crash-safe serialisation.
- Structured JSON logs on stderr when `--log-format json` is set or when running in
  CI (`$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`, `$CIRCLECI`).
- `application_name` set to `aqueduct/<project>/<migration_id>` on every connection,
  making in-flight migrations visible in `pg_stat_activity`.
- 58 tests (42 unit, 10 integration, 6 CLI); all passing, none skipped.
- Five GitHub Actions CI jobs: lint, unit tests, integration tests, CLI tests, and
  coverage.
- `examples/minimal/` — 3-node DAG example with a complete `aqueduct.toml`.
- `ESSENCE.md` — architecture overview and design principles.
- `justfile` — developer convenience recipes (`test`, `lint`, `fmt`, `coverage`).

[unreleased]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.15.0...HEAD
[0.15.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/trickle-labs/pg-aqueduct/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.1.0
