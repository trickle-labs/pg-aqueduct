# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For planned future versions, see [ROADMAP.md](ROADMAP.md).

## [Unreleased]

## [0.20.0] - 2025-07-14

This release delivers the three high-priority architectural improvements identified in the Phase 4 engineering assessment: typed executor configuration, full multi-tenant catalog parameterisation, and automated crash recovery via the compensating-step registry.

The `ExecutionContext` struct replaces the optional builder pattern for the two safety-critical executor fields (`desired_state` and `connection_string`), making it a compile error to omit them. All command handlers (`apply`, `rollback`, `promote`) have been updated accordingly.

Every catalog SQL constant now accepts a `CatalogSchema` parameter and substitutes the schema name via `for_schema()`. The `validate_catalog_schema_not_overridden()` stop-gap from v0.19 is removed. `aqueduct init --schema my_catalog` creates all catalog tables in `my_catalog`; the multi-tenant isolation integration test verifies that two projects on the same database with different catalog schemas have fully independent version histories.

The `--resume` flow now queries `aqueduct.ddl_log` for rows in `status = 'running'` before advancing to the checkpoint. For each such row the compensating SQL is executed and the row is updated to `status = 'compensated'`. A new integration test covers the crash-after-DDL, before-RecordSnapshot scenario.

The `OutputEmitter` is now stored as a process-wide singleton via `OnceLock`, initialised in `main` before command dispatch and accessible via `output::emitter()`. The `init` command is the first to use the emitter for its success messages.

The `httpmock` dependency was replaced with `wiremock` in v0.17, eliminating the last `cargo audit --deny warnings` finding (RUSTSEC-2025-0052).

The catalog schema version advances from 8 to 9, adding a `status` column to `aqueduct.ddl_log` used by compensating-step recovery.

### Added

- **Typed `ExecutionContext` struct (ARCH-2):** `ExecutionContext` struct with required
  `desired_state` and `connection_string` fields replaces the optional builder pattern.
  Omitting either field is now a compile error. `for_apply()`, `for_rollback()`, and
  `for_promote()` constructors updated.
- **Full catalog schema parameterisation (ARCH-1):** `CatalogSchema` threaded through
  every catalog SQL function. `ensure_catalog_current()`, `connect_and_migrate()`,
  `read_live_state()`, `get_latest_dag_version()`, `detect_extension_installed()`,
  `read_ddl_log()`, `import_from_live()`, `compute_promotion_plan()`,
  `validate_source_clean()`, and `destroy_project()` all accept `&CatalogSchema`.
  `validate_catalog_schema_not_overridden()` stop-gap removed.
- **Multi-tenant catalog isolation test:** `test_multi_tenant_catalog_isolation` verifies
  that two catalogs (`tenant_a`, `tenant_b`) on the same database have independent
  `dag_versions` tables with no cross-contamination.
- **Automated compensating-step recovery (CORR-2):** `find_resume_step()` queries
  `aqueduct.ddl_log` for `status = 'running'` rows and executes their `compensating_sql`
  via `batch_execute` before advancing to the resume checkpoint. Rows are updated to
  `status = 'compensated'` on success.
- **`COMPLETE_COMPENSATING_STEP_SQL` and `MARK_COMPENSATING_APPLIED_SQL` constants** in
  `catalog.rs` for the two-phase compensating-step lifecycle.
- **CORR-2 crash recovery integration test:** `test_corr2_compensating_step_recovery`
  inserts a synthetic `status = 'running'` ddl_log row and verifies that `--resume` moves
  it out of the `running` state.
- **Catalog schema version 9:** `CATALOG_SCHEMA_VERSION` advances from 8 to 9. The v8→v9
  migration adds a `status TEXT NOT NULL DEFAULT 'complete'` column to `aqueduct.ddl_log`
  and backfills existing rows.
- **`OutputEmitter` process-wide singleton (M-1):** `output::init_emitter(mode)` stores
  the emitter in a `static OnceLock<OutputEmitter>`; `output::emitter()` returns the
  global reference. `main.rs` calls `init_emitter` before dispatch. The `init` command
  uses the emitter for its success messages, replacing direct `println!` calls.

### Changed

- `for_schema(sql, schema)` now substitutes `= 'aqueduct'` string-literal comparisons
  and `CREATE/DROP SCHEMA IF NOT EXISTS aqueduct` statements in addition to the
  `aqueduct.` dot-prefix references.
- `DestroyOptions` gains a `catalog_schema: CatalogSchema` field.
- All `cargo check --tests` call sites for `read_live_state`, `get_latest_dag_version`,
  `detect_extension_installed`, `read_ddl_log`, `import_from_live`,
  `compute_promotion_plan`, `validate_source_clean`, and `DestroyOptions::new` updated
  throughout CLI commands and integration tests.
- Workspace version bumped `0.19.0` → `0.20.0`.

### Fixed

- `executor.rs:import_from_live` was calling `read_live_state(client, None)` (2-arg form)
  instead of the 3-arg `read_live_state(client, None, catalog_schema)`. Fixed.
- Extra trailing semicolons (`await?;;`) in `commands/promote.rs` and `commands/status.rs`
  introduced by the multi-replace pass. Removed.

## [0.19.0] - 2026-05-20

This release eliminates every structural correctness bug identified in the Phase 4 engineering assessment, makes the blue/green view swap fully atomic, and ensures every known correctness regression path is covered by a test that would catch it.

The headline change is that the deployment status row update in `SwapConsumerViews` is now executed *inside* the surrounding `BEGIN`/`COMMIT` block, eliminating the window where consumer views point at the green schema while the deployment row still shows `status = 'active'`. If the transaction is rolled back for any reason, both the view assignments and the deployment row revert together.

The heartbeat task is now supervised by a `JoinHandle`-based wrapper. If the heartbeat task panics — something that could previously produce a silent hang — the executor immediately receives a `LockLost` signal, stops execution, and surfaces the failure. This closes the last known path to a silent runaway executor.

The mock `pg_trickle` DDL gains a `pgtrickle.paused_nodes` table so integration tests can assert precisely which nodes were paused during DDL and that the scheduler was fully resumed after apply or after a failure. Two new test-kit helpers (`assert_scheduler_idle` and `assert_scheduler_paused_for`) make these assertions one-liners.

Four executor and CLI quality fixes round out the release: the `WaitForConvergence` poll query is now guarded by a `statement_timeout` to prevent a hung `pg_trickle` query from holding the loop past the configured deadline; `status --watch` applies exponential backoff on repeated connection failures; the `EXPLAIN` trade-off in `cost.rs` is documented with a code comment; and `aqueduct init` and `aqueduct apply` now return an explicit `NotYetImplemented` error when `catalog_schema` is set to anything other than `"aqueduct"`, preventing silent data mixing in multi-tenant deployments until full schema parameterisation ships in v0.20.

All ten failure-mode test cases identified in the Phase 4 assessment are now present and passing.

### Added

- **Blue/green atomicity (ARCH-3):** `SWAP_BLUE_GREEN_SQL` (the `blue_green_deployments`
  status row update) is now executed inside the `SwapConsumerViews` transaction. A failed
  mid-swap now rolls back both view assignments and the deployment status atomically.
  Integration test `swap_consumer_views_atomicity` verifies this.
- **Heartbeat panic supervisor (CORR-3):** `tokio::spawn(run_heartbeat(...))` is wrapped in
  a `JoinHandle`-based supervisor that calls `lock_lost_tx.send(true)` if the heartbeat
  task panics. Integration test `heartbeat_panic_signals_lock_loss` verifies the signal.
- **Observable scheduler mock (TEST-3):** `mock_pgtrickle.rs` gains a
  `pgtrickle.paused_nodes(node_name text PRIMARY KEY, paused_at timestamptz)` table.
  `pause_scheduler` inserts into it; `resume_scheduler` deletes from it.
- **Test-kit helpers:** `assert_scheduler_idle(client)` and
  `assert_scheduler_paused_for(client, nodes)` added to `aqueduct-testkit`. Integration
  tests `mock_scheduler_records_pause` and `mock_scheduler_idle_after_apply` demonstrate
  usage.
- **`statement_timeout` guard for `WaitForConvergence` (M-9):** the poll `SELECT` is
  wrapped in `BEGIN READ ONLY; SET LOCAL statement_timeout = '{timeout}ms'` so a hung
  `pg_trickle` query cannot hold the loop open past the configured deadline. Integration
  test `waitforconvergence_deadline_respected` verifies the deadline is enforced.
- **Exponential backoff in `status --watch` (M-8):** consecutive connection or poll
  failures back off up to `5 × interval` before retrying; the counter resets on the next
  successful poll.
- **EXPLAIN trade-off comment (M-4):** `cost.rs::estimate_rows` now documents why
  `format!("EXPLAIN...")` is intentional and safe. The EXPLAIN call is also wrapped in a
  `BEGIN READ ONLY; SET LOCAL statement_timeout = '5s'` block.
- **Catalog schema stop-gap (ARCH-1 partial):** `validate_catalog_schema_not_overridden`
  in `catalog.rs` returns `AqueductError::NotYetImplemented { feature: "catalog_schema override" }`
  for any value other than `"aqueduct"`. Called from `aqueduct init` and `aqueduct apply`
  to prevent silent data mixing until full multi-tenant support lands in v0.20.
  Integration test `catalog_schema_override_rejected_until_implemented` verifies this.
- **Failure-mode tests (Phase 4):** all ten test cases identified in the assessment are
  now present and passing:
  `consumer_sql_injection_rejected_at_apply`,
  `heartbeat_panic_signals_lock_loss`,
  `swap_consumer_views_atomicity`,
  `plan_fail_on_drift_counts_consumer_deltas`,
  `destroy_auto_migrates_catalog`,
  `catalog_schema_override_rejected_until_implemented`,
  `age_identity_file_traversal_rejected`,
  `cnpg_preview_requires_valid_cert`,
  `waitforconvergence_deadline_respected`,
  `mock_scheduler_records_pause`.
- `AqueductError::NotYetImplemented { feature }` variant added (error code 1308).
- `cnpg_preview_requires_valid_cert` unit test in `preview.rs` verifies that a
  `create_preview_cnpg` call to a plain-HTTP endpoint fails with a TLS/connection error
  when `CNPG_INSECURE_SKIP_VERIFY` is absent (SEC-2).

### Changed

- Workspace version bumped to `0.19.0`.
- Six user-facing integration files updated to reference version `0.19.0`:
  `.github/workflows/aqueduct-plan.yml`, `.github/workflows/aqueduct-apply.yml`,
  `ci/gitlab/aqueduct.gitlab-ci.yml`, `.pre-commit-hooks.yaml`,
  `docs/api-reference.md`, `docs/introduction.md`.
- `ROADMAP.md` status line updated to `v0.19.0 released`.



This release closes every open security finding from the Phase 4 engineering assessment, bringing `pg_aqueduct` to zero open CVEs and zero open critical or high security issues. The headline change is that consumer view bodies are now validated as a single `SELECT` statement before any database call is made — meaning a misconfigured or maliciously crafted migration file containing DDL like `CREATE TABLE` or `DROP TABLE` can never reach the database, even if it somehow made it into your migrations directory. This is an important defence-in-depth layer for teams running Aqueduct in automated CI/CD pipelines.

Two additional critical security fixes ship in this version: the CloudNativePG preview client now validates TLS certificates by default (previously it silently accepted any certificate), and the `age` secret backend now validates the identity file path for both path traversal and flag injection before passing it to the `age` subprocess. Together with the consumer SQL validation, these three fixes mean `pg_aqueduct` meets the full OWASP A03 (Injection) and A02 (Cryptographic Failures) controls relevant to its operation.

Beyond security, this version improves day-to-day operational quality: `aqueduct plan --fail-on-drift` now correctly detects drift in consumer views and source tables (not just stream tables), `aqueduct destroy` auto-migrates a stale catalog before querying it, and several documentation files and CI template files that had stale version pins are updated to match the current release. A new `.github/SECURITY.md` establishes the project's vulnerability reporting policy.

### Added

- `validate_consumer_sql_is_single_select()` is now called immediately before every
  `CREATE OR REPLACE VIEW` in the executor (`ManageConsumerView` and `SwapConsumerViews`
  arms). A migration file containing injected DDL is rejected with
  `AqueductError::UntrustedSqlBody` before any database call is made (SEC-1).
- Integration test `consumer_sql_injection_rejected_at_apply` confirms that a consumer
  migration with a `CREATE TABLE` body is rejected at the executor level (SEC-1).
- CNPG preview HTTP client now validates TLS certificates by default. Set
  `CNPG_INSECURE_SKIP_VERIFY` to skip verification in development environments only.
  Emits a `warn!` when the env var is set (SEC-2).
- Unit test `cnpg_tls_default_does_not_skip_verification` confirms the env-var gate
  logic (SEC-2).
- `age` secret backend now validates the identity file path (from `SOPS_AGE_KEY_FILE` /
  `AGE_KEY_FILE`) via `validate_secret_path()` and rejects paths starting with `-`
  (flag-injection guard). Unit tests `age_identity_file_traversal_rejected` and
  `age_identity_file_flag_injection_rejected` cover both rejection cases (SEC-3).
- `.github/SECURITY.md`: security policy with contact address, response SLA, supported
  version matrix, and a summary of implemented controls (L-5).
- `docs/security.md`: new sections covering consumer SQL injection protection,
  CNPG TLS configuration, and `age` identity file security (SEC-1, SEC-2, SEC-3).

### Changed

- `aqueduct plan --fail-on-drift`: drift count now includes `source_deltas` and
  `consumer_deltas` in addition to `deltas`, matching the already-correct `status`
  implementation. Previously, consumer-layer and source-layer drift was silently
  ignored (M-2).
- `aqueduct destroy`: changed from `connect()` to `connect_and_migrate()` so a stale
  catalog schema is auto-migrated before destroy queries it (M-5).
- `release.yml`: removed global `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` env var — all
  actions in the workflow use modern Node.js runtimes that do not require this
  workaround (M-6). Added `just gen-docs` step to regenerate `docs/api-reference.md`
  on every release tag.
- `ingest.rs` test helpers: `fs::write(...).unwrap()` replaced with descriptive
  `.expect("...")` calls so a failing CI write produces a clear error message (M-7).
- `fmt.rs`: renamed `KNOWN_FRONTMATTER_KEYS` to `KNOWN_DIRECTIVE_KEYS` with updated
  doc comment explaining it is used to skip already-emitted keys, not enforce ordering
  (L-1).
- `main.rs`: replaced stale `// L7 (v0.14)` tracking comment with descriptive
  per-platform CI env var documentation (L-3).
- `Cargo.toml`: replaced stale `# DEP-1 (v0.14): pre-declare serde_yaml` comment with
  `# YAML serialization for plan/status/diff --format yaml output.` (L-4).
- Six user-facing integration files updated to reference version `0.18.0`:
  `.github/workflows/aqueduct-plan.yml`, `.github/workflows/aqueduct-apply.yml`,
  `ci/gitlab/aqueduct.gitlab-ci.yml` (also fixes archive name pattern to match
  the release pipeline: `aqueduct-{version}-{os}-{arch}.tar.gz`),
  `.pre-commit-hooks.yaml`, `docs/api-reference.md`, `docs/introduction.md` (CI-1).
- `version-lint` CI job extended to check nine files: `README.md`,
  `docs/installation.md`, `docs/api-reference.md`, `docs/introduction.md`,
  `ci/gitlab/aqueduct.gitlab-ci.yml`, `.pre-commit-hooks.yaml`,
  `.github/workflows/aqueduct-plan.yml`, `.github/workflows/aqueduct-apply.yml`,
  and `ROADMAP.md`. Fails if any disagrees with the workspace `Cargo.toml` version
  (CI-1, L-6).
- `ROADMAP.md` status line updated to `v0.18.0 released` (L-6).
- Workspace version bumped to `0.18.0`.

## [0.17.0] - 2026-06-10

This release is all about confidence and safety for teams running critical database infrastructure. When you're managing a high-availability PostgreSQL cluster using tools like Patroni, you can now tell Aqueduct exactly where to check that the database is still the primary leader between every migration step. If the database fails over to a replica mid-migration, Aqueduct detects this immediately, stops what it's doing, and marks the migration as interrupted — rather than blindly continuing and potentially corrupting data on a read-only standby. This dramatically reduces the risk of one of the most dangerous scenarios in database operations: a migration that half-completes during a failover event.

Beyond failover protection, this version gives teams unprecedented visibility into what Aqueduct is doing at runtime. Every single migration step — when it starts, how long it takes, whether it succeeded or failed — is now emitted as a structured event, making it trivial to connect Aqueduct to monitoring dashboards, alerting pipelines, or log aggregation systems. The new `aqueduct audit` command lets you look back at recent migrations and see exactly what happened and when. For teams that need to demonstrate compliance or investigate incidents, this audit trail is invaluable. And with a built-in Prometheus metrics endpoint, you can wire Aqueduct directly into Grafana and get real-time migration dashboards with zero extra tooling.

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

Version 0.16 is a polish release focused on making Aqueduct a pleasure to work with, both for humans sitting at a terminal and for automated systems parsing its output. Teams integrating Aqueduct into CI/CD pipelines can now choose between structured JSON, clean YAML, a minimal porcelain format designed for shell scripting, or a completely silent quiet mode that suppresses all decorative output and shows only what matters. These improvements mean that whatever your downstream tooling looks like — Slack alerts, dashboards, custom scripts — Aqueduct can speak its language directly without any fragile text scraping.

Under the hood, this version also significantly raised the quality bar for testing, adding a comprehensive suite of binary-level tests that verify every subcommand's exit codes, help text, and output formats behave exactly as documented. This means fewer surprises when you upgrade — if a command's behavior changed accidentally, a test will catch it before it ships. The `aqueduct status` command gained a new `--poll-interval` flag with smarter file-change detection, so in watch mode it only reloads migration files when they actually change, making long-running monitoring sessions faster and lighter on the database.

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

This release is the one that gives operations teams confidence to run Aqueduct in the most demanding production environments. Database schema changes are inherently risky — if something goes wrong halfway through, you need a clear path back. Version 0.15 introduces "saga-style" compensating actions: before Aqueduct executes any potentially destructive DDL, it first records what it would take to undo that action. If the process crashes or is interrupted, you can resume from exactly where you left off, or roll back cleanly without any manual investigation. For teams who have ever been woken up at 3 AM to manually clean up a half-finished migration, this is a transformative improvement.

Security and resilience round out this release. Aqueduct now validates catalog schema names at the earliest possible point, firmly rejecting any value that could be used to inject SQL — a class of vulnerability that has burned many database automation tools. Row-level security policies, which PostgreSQL uses to control which users can see which data, are now properly preserved across table recreations, meaning a migration that rebuilds a stream table won't silently drop your access controls. And for situations where a migration step is genuinely ambiguous — perhaps a step that appears to have started but you can't confirm it completed — operators now have `--force-retry` and `--force-skip` flags to advance past stuck states safely.

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

Version 0.14 is a focused correctness and security release that addressed a number of real-world edge cases discovered through extensive integration testing. The most important of these is that failed migrations are now properly marked as recoverable and can be resumed — previously, a failure in certain code paths could leave the migration in a state where Aqueduct wouldn't offer a clean resume path. A heartbeat mechanism that keeps the database advisory lock alive during long-running migrations now correctly signals the main process when the lock is lost, preventing processes from continuing work on a database they no longer exclusively own.

Three security improvements shipped alongside these correctness fixes. Connection strings that embed plaintext passwords are now caught and rejected for keyword-value style DSNs, matching the protection already in place for URL-style connections. Consumer view SQL bodies are validated to be single SELECT statements before any database objects are created, closing off a potential SQL injection path through migration files. Read-only connections now explicitly begin a READ ONLY transaction and use a safe allowlist for statement timeout values, preventing injection through configuration parameters. Together these fixes ensure that even if a migration file or configuration value is maliciously crafted, Aqueduct won't execute arbitrary SQL against your database.

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

Version 0.13 delivers the complete blue/green deployment workflow that enterprise teams need for zero-downtime schema migrations on production databases. When your changes are complex enough to require restructuring the underlying tables — for example, changing which columns are used as grouping keys in a large aggregation — Aqueduct now orchestrates the full sequence automatically: creating a parallel "green" schema, building the new stream tables there, waiting for them to fully catch up with the "blue" production tables, atomically swapping all consumer views in a single transaction, and then retiring the old schema. The entire process is hands-off once initiated, dramatically reducing the skill and attention required to execute what was previously one of the most nerve-wracking database operations.

Two other significant capabilities arrived in this release. Pre- and post-migration hooks let teams wire Aqueduct into their broader operational playbook — for example, firing a Slack notification when a migration starts, or triggering a cache flush after it completes — all configured declaratively in `aqueduct.toml`. The new `--out` and `--plan` flags let teams generate a migration plan during a code review, have a human or automated system approve it, and then execute exactly that pre-approved plan later — with a cryptographic hash check ensuring the plan hasn't been tampered with between approval and execution. This separation of planning from execution is a powerful governance tool for organizations with strict change-management requirements.

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

Version 0.12 is a major leap forward for production readiness, tackling three areas that enterprise teams consistently ask about: secret management, resilience against infrastructure variation, and confidence in the planner's correctness. Rather than requiring database passwords to live in environment variables or configuration files, Aqueduct can now fetch connection credentials directly from AWS Secrets Manager, GCP Secret Manager, or HashiCorp Vault at runtime. This means your production database password never needs to touch a CI/CD configuration file, a developer's laptop, or a Kubernetes secret — it stays in your secrets vault and is retrieved only at the moment it's needed.

A subtler but equally important improvement is how Aqueduct now deals with different versions of pg_trickle, the underlying PostgreSQL extension it builds on. Rather than assuming all features are available and failing at runtime, Aqueduct now probes the installed version's capabilities at startup and gracefully skips or adapts any step the current version doesn't support. This makes deployments far more robust across environments where the extension version may differ from what was tested in CI. The addition of property-based fuzz testing — automatically generating thousands of random migration scenarios and verifying that the planner never panics and always produces consistent results — gives the entire planning engine a level of correctness assurance that hand-written tests simply can't match.

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

Version 0.11 focuses on the experience of getting help when something goes wrong. All errors, lint warnings, and validation failures that Aqueduct produces now share a single, unified format that includes the exact file and line number where the problem occurred, a stable numeric error code, and a severity level. This means that editor plugins, CI parsers, and custom tooling can all reliably interpret Aqueduct's output without having to guess at error message formats. The stable numeric codes are particularly valuable for teams writing runbooks — instead of matching on potentially-changing error text, you can reference code 1200 for a planning error or 1300 for an execution error and know that code will remain stable across versions.

The documentation story got a major upgrade too. A new CI job now automatically builds the documentation and verifies that every subcommand mentioned in the docs actually exists and responds to `--help`, so documentation can never silently drift out of sync with the binary. Security also improved: all database connection strings are now scrubbed of passwords before they appear in error messages, log output, or CI logs — a simple but critical safeguard that prevents credentials from leaking into build artifacts or error reports. The `aqueduct plan` and `aqueduct status` commands are now strictly read-only at the connection level, making it physically impossible for them to accidentally modify database state even in edge-case error paths.

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

Version 0.10 closes out a category of subtle bugs that could cause incorrect behavior when working with consumer views — the downstream SQL views that read from stream tables. Previously, a plan that only added, dropped, or modified consumer views without touching any stream tables was silently treated as a no-op and never executed. This was a correctness bug with real consequences: teams could deploy consumer view changes, see no errors, and then discover their views weren't updated because Aqueduct thought there was nothing to do. All consumer operations now correctly flow through the full plan-and-execute path with proper tracking and reporting.

Project isolation — the ability to manage multiple independent DAGs in the same PostgreSQL database — received a significant reliability upgrade. All live state reads now correctly filter by project, meaning one project's tables can never accidentally appear in another project's plan or status. The `aqueduct destroy` command now verifies that it actually owns the tables it's about to drop, preventing accidental cross-project destruction. Several concurrency scenarios were also hardened: the lock heartbeat is now bound to the specific holder that acquired the lock, so a fresh Aqueduct process starting up won't be fooled by a stale heartbeat from a crashed predecessor into thinking the database is still locked.

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

Version 0.9 is a turning point in how operators interact with Aqueduct day-to-day. The new `aqueduct diff` command provides a clear, human-readable comparison between what your migration files describe and what currently exists in the live database — across every table, every column, and every configuration detail. This makes drift visible at a glance before you commit to running any migration, transforming what was previously a moment of uncertainty before a production apply into a clear-eyed, informed decision. Whether you're checking before a deployment, investigating after an incident, or auditing compliance, `aqueduct diff` gives you the full picture.

This release also introduced the secret backend system that underpins enterprise-grade credential management: database connection strings can now reference secrets stored in AWS, GCP, Vault, SOPS-encrypted files, or age-encrypted files using a simple `${secret:BACKEND:KEY}` syntax, keeping credentials entirely out of configuration files and CI logs. Every successful `aqueduct apply` now emits a structured JSON event with the migration ID, version numbers, and project name — making it trivial for CI systems to capture and act on migration results. For teams using interactive terminals, a confirmation prompt before any apply ensures no one accidentally runs a migration they didn't intend to.

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

Version 0.8 tackles one of the hardest problems in database automation: what happens when things go wrong in the middle of a migration. The executor now tracks progress at the granularity of individual steps, persisting a checkpoint to the database after each one. If a migration is interrupted — whether by a network blip, a process crash, or an operator hitting Ctrl-C — re-running `aqueduct apply --resume` will pick up from exactly where it left off, skipping every step that already completed successfully. For long-running migrations that touch large tables, this means an interruption no longer requires starting over from scratch.

The lock mechanism that prevents two Aqueduct processes from stepping on each other's work was made significantly more robust. A background task now continuously renews the advisory lock while a migration is running, sending a heartbeat every ten seconds on a dedicated database connection. This ensures that a migration taking longer than PostgreSQL's default lock timeout can complete without its lock expiring mid-flight. If the heartbeat connection is lost — for example, because the database restarted — the main migration process is immediately notified and can shut down cleanly rather than continuing to issue commands to a database that may no longer be in the expected state.

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

Version 0.7 is the documentation and confidence-building release. Thirty detailed cookbook recipes were added covering every migration pattern that Aqueduct supports — from simply adding a column, to changing GROUP BY keys, to performing a full blue/green topology restructure — each with a complete before-and-after example and a matching integration test. Whether you're a new team trying to understand what Aqueduct can do, or an experienced operator looking for the exact right pattern for a non-obvious migration, the cookbook gives you a concrete, tested answer rather than requiring you to reason from first principles.

Public benchmarks were published for the first time, quantifying the core value proposition in plain numbers: on a 200-node DAG with a 5-node change set, Aqueduct applies changes in roughly 6 milliseconds — compared to approximately 180 seconds that a naive drop-and-recreate approach would take on the same workload. That's a 28,000× speed improvement. Security and high-availability documentation were written in depth, covering everything from setting up least-privilege database roles to handling failover events and crash recovery. With 197 tests now passing across unit, integration, and CLI test suites, this version represents the highest level of tested confidence the project had achieved to that point.

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

Version 0.6 completes the core operational toolset that teams need to manage Aqueduct migrations across the full software delivery lifecycle. The `aqueduct promote` command solves the environment promotion problem elegantly: rather than manually copying migration files between staging and production and hoping you didn't miss anything, you point Aqueduct at your source environment, it validates that everything is clean, computes exactly what needs to happen in the destination environment, asks for confirmation, and records the promotion event in the audit trail. This makes the act of "promoting to production" repeatable, auditable, and safe — the same migration files validated in staging are what get applied to production.

The `aqueduct destroy` command gives teams a clean, safe way to tear down all objects associated with a project — useful for cleaning up ephemeral preview environments or decommissioning retired workloads. High-availability primary detection was formalized with support for Patroni, CloudNativePG, and Stolon, so Aqueduct can reliably determine whether it's talking to a writable primary before taking any action. The `aqueduct status --watch` mode, combined with configurable alerting thresholds, means teams can leave a monitor running in production that immediately flags any drift between what's deployed and what the migration files describe — turning Aqueduct into a lightweight continuous compliance tool as well as a migration executor.

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

Version 0.5 bridges two worlds that data teams live in simultaneously: dbt, the SQL transformation tool that millions of data engineers use to define their data models, and Aqueduct, which manages the real-time stream tables that power live analytics. The new `aqueduct ingest --from dbt-target` command reads the compiled output of a dbt project and automatically generates a canonical Aqueduct migrations directory. Schedule, refresh mode, CDC mode, schema, and dependency relationships are all inferred from the dbt model configuration, so no manual translation work is required — what you express in dbt flows directly into Aqueduct.

The practical impact is that teams can maintain a single source of truth in their dbt project and have Aqueduct automatically stay in sync. The `examples/dbt-roundtrip/` example included with this release demonstrates the full workflow: run dbt, ingest the results into Aqueduct, validate, plan, apply, and roll back — end to end, without any manual steps. Because the ingest command is idempotent, it can safely be re-run after every dbt compile without risk of creating duplicates or inconsistencies, making it trivial to slot into CI/CD pipelines that already build dbt artifacts.

### Added

- `aqueduct ingest --from dbt-target` command reading compiled dbt artefacts and
  generating a canonical aqueduct migrations directory; idempotent.
- Front-matter directives populated from dbt model config (`+schedule`,
  `+refresh_mode`, `+cdc_mode`, `+schema`, `+depends_on`).
- `examples/dbt-roundtrip/` demonstrating the full ingest → validate → plan → apply
  → rollback round-trip.

## [0.4.0] - 2026-05-18

Version 0.4 is the release that makes Aqueduct a first-class member of your development workflow rather than just a tool you run manually before deployments. The `aqueduct fmt` command automatically reformats migration files to a canonical style — consistent SQL keyword casing, stable front-matter ordering, no trailing whitespace — and a `--check` mode makes it easy to enforce this standard in CI without modifying files. The `aqueduct lint` command catches five categories of common mistakes before they ever reach a database: schedules that are too aggressive for the workload, queries that IVM can't optimize, consumer views that reference sources that don't exist, and more.

The GitHub Actions composite actions make it genuinely effortless to add Aqueduct to any GitHub-hosted project: a single line in your PR workflow will install the binary, run `aqueduct plan`, and post the plan as a PR comment so reviewers can see exactly what database changes a PR will produce before merging. The same experience is available for GitLab CI via provided templates. Pre-commit hooks for `fmt`, `validate`, and `lint` mean that quality checks happen before code is even committed, catching problems at the earliest possible moment. Together, these integrations turn database migration management from an afterthought into a first-class, automated part of the code review process.

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

Version 0.3 introduces three powerful capabilities that together define Aqueduct's vision for sophisticated, production-safe database change management. Blue/green plan steps — creating a parallel schema, building new stream tables, waiting for convergence, atomically swapping consumer views in a single transaction, retiring the old schema — provide the foundation for zero-downtime migrations of even the most complex DAG topologies. Consumer views give teams a clean way to expose stable, versioned SQL interfaces to downstream applications, completely decoupling what the application queries from the underlying stream table implementation.

The `aqueduct preview` command is perhaps the most innovative addition in this release: it creates a throwaway copy of your entire DAG in a scratch schema, populated with a sampled subset of real production data, so you can run and test your new migration against realistic data without any risk of affecting production. This works natively against any PostgreSQL instance and has stub implementations for CloudNativePG and Neon-managed databases. For data teams building and iterating on complex analytics queries, the ability to spin up a safe, realistic preview environment in seconds — and tear it down just as quickly — fundamentally changes how development and testing workflows operate.

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

Version 0.2 introduces the intelligence at the core of Aqueduct's value proposition: the migration classifier. When you change a migration file, Aqueduct doesn't just blindly re-execute everything — it analyzes what changed and classifies the migration into one of four cost categories: Free (can be applied with zero downtime and no data movement), In-place (a structural change made with a simple ALTER TABLE), Rebuild (requires recreating the table, typically within a maintenance window), or Blue/green (a topology-level restructure requiring the full zero-downtime workflow). This classification drives every decision Aqueduct makes about how to safely apply your changes.

A particularly important extension of this classifier is the automatic cascade analysis for ALTER TABLE changes. When a source table that stream tables depend on changes — for example, a column is renamed or its type changes — Aqueduct automatically identifies every stream table in the DAG that references that source and escalates them to a Rebuild classification. This prevents the silent failures that plague naive migration tools, where a source table change breaks dependent views in ways that aren't discovered until queries start returning errors. The new `--explain-cost` flag rounds out this release by showing per-step row count estimates and duration projections, giving operators a clear picture of what a migration will actually cost before committing to run it.

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

Version 0.1 is the foundation on which everything else is built. At its core, Aqueduct solves a problem that every team managing real-time analytics in PostgreSQL eventually hits: how do you safely change a complex web of interdependent materialized views and stream tables — built with extensions like pg_trickle — without causing outages, data loss, or hours of manual database administration? The answer is to treat database schema changes the same way software engineers treat code changes: with version control, automated planning, safe execution, and the ability to roll back. This first release establishes all of those primitives.

The key capabilities in this release — automatic DAG inference from SQL FROM/JOIN clauses, cycle detection, migration classification, advisory locking for crash-safe serialization, read-only plan and status commands, structured JSON logging for CI, and standby detection via `pg_is_in_recovery()` — together form a complete and production-usable system even at this initial version. The minimal example included in the repository shows a 3-node DAG going from nothing to a fully-managed real-time analytics pipeline in a handful of configuration lines. With 58 tests passing and five CI jobs running on every commit, the project launched with a commitment to quality and correctness that every subsequent release has built upon.

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
