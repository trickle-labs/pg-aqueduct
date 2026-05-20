# pg-aqueduct — Overall Engineering Assessment (Phase 3)

**Date:** 2026-05-20
**Codebase version:** 0.13.0
**Scope:** Full codebase, CI, docs, examples, roadmap

---

## Executive Summary

- **Overall project health score:** 6.5 / 10. The project has moved substantially since `plans/overall-assessment-1.md`: the core plan/apply/rollback/resume loop exists, the full workspace test suite passes, the catalog has migrations/indexes, cookbook coverage is broad, and several prior critical bugs are fixed. It is still not production-ready for high-risk environments because failure recovery is not reliable on executor errors, multi-step DDL is not atomic, promotion bypasses project ownership and executor safety context, blue/green is mostly a planned step sequence rather than an atomic deployment system, the GitHub composite actions cannot download the release artifacts they expect, and docs still overclaim HA/security/CLI behavior.
- **Top 5 critical issues:**
  1. `--resume` progress is erased when an executor error is converted to `recoverable_failure`, so normal failed applies resume from step 0 instead of the last checkpoint.
  2. Plan execution is not transactionally bounded; DDL, catalog writes, scheduler pause/resume, and lock release can leave partially-applied state after crashes or errors.
  3. The published composite GitHub actions construct archive names (`linux-x86_64`) that do not match release artifacts (`linux-amd64`), so CI installs fail for released binaries.
  4. `aqueduct promote` reads all stream tables instead of project-owned tables and applies without desired-state snapshots, heartbeat DSN, or catalog migration.
  5. Blue/green swaps are not atomic and do not use the blue/green catalog tracking SQL, despite the changelog claiming end-to-end support.
- **Top 5 quick wins:**
  1. Preserve `progress` in `FINISH_MIGRATION_SQL` on failures or pass the latest checkpoint instead of `{}` (XS, < 1h).
  2. Fix composite action archive naming to `linux-amd64`, `linux-arm64`, `macos-arm64`, `macos-amd64`, `windows-amd64` and update action smoke to invoke the composite actions (S, half day).
  3. Change `promote` to use `connect_and_migrate`, pass the project to `read_live_state`, and call `.with_desired_state(...).with_connection_string(...)` (S, half day).
  4. Extend the plaintext password guard to keyword-value DSNs (`password=...`) and add tests (XS, < 1h).
  5. Replace hand-written YAML emitters with `serde_yaml` or a shared escaping helper (S, half day).

| Severity | Correctness | Architecture | Ergonomics | TestCoverage | Performance | Security | Documentation | CI-CD | Dependencies | Roadmap | Total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Critical | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 3 |
| High | 4 | 2 | 0 | 1 | 0 | 2 | 1 | 1 | 0 | 1 | 12 |
| Medium | 2 | 1 | 4 | 2 | 2 | 1 | 1 | 1 | 0 | 0 | 14 |
| Low | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| **Total** | **8** | **3** | **4** | **3** | **2** | **3** | **2** | **3** | **1** | **1** | **30** |

**Verification performed:** `cargo audit` completed with one ignored/allowed warning (`RUSTSEC-2025-0052`, `async-std` unmaintained via `httpmock`). `cargo test --workspace` passed: 41 CLI integration tests, 144 core unit tests, 90 core integration tests, and doc tests.

**Severity definitions:**
- **Critical:** Can corrupt data, break recovery, bypass project isolation, or make a released automation path unusable.
- **High:** Can cause incorrect migrations, security regressions, major production surprises, or false safety claims.
- **Medium:** Degrades reliability, observability, UX, or maintainability but usually has an operator workaround.
- **Low:** Cleanup, polish, stale comments, or bounded dependency/process risk.

---

## Status of Prior Findings

| Prior ID | Prior finding | Current status | Evidence |
|---|---|---|---|
| C1 | Rollback ignored recorded prior spec | Fixed | `rollback.rs:128-144` deserializes `spec_jsonb`; tests cover prior spec. |
| C2 | `--resume` no-op | Partially Fixed | Resume reads/writes progress (`executor.rs:337-352`, `executor.rs:1044`) but failure finalization overwrites progress (`executor.rs:316-321`). |
| C3 | Panickable `.unwrap()` in planner delta fields | Fixed | Planner now returns `InvariantViolation` around missing desired/actual specs (`plan.rs:689-742`, `plan.rs:920-946`). |
| C4 | Executor fallback SQL injection when pg_trickle absent | Fixed | `CreateStreamTable` now errors if pg_trickle is absent (`executor.rs:463-474`). Preview still embeds rewritten queries; tracked as new SEC-2. |
| C5 | Lock TTL never renewed | Partially Fixed | Heartbeat exists (`executor.rs:1145-1188`) but lock-loss is only logged, not propagated. |
| C6 | `init` installed v1 catalog | Fixed | `init.rs:1,54` uses `CATALOG_INIT_V4_SQL`, currently aliased to v5 (`catalog.rs:200-204`). |
| H1 | `AlterStreamTable` ignored `new_query` | Fixed | Executor passes six arguments including `new_query` (`executor.rs:490-509`). |
| H2 | `--validate-ivm` syntax-only | Fixed | `plan.rs:78-91` calls `validate_ivm_supportability` for DIFFERENTIAL tables. |
| H3 | Mid-column removal classified InPlace | Fixed | Classifier now allows only tail removal via prefix check (`classifier.rs:173-181`). |
| H4 | `aqueduct diff` missing | Fixed | CLI registers `Diff` (`main.rs:68-70`, `main.rs:122`). |
| H5 | Apply lacked confirmation / `--yes` | Fixed | `ApplyArgs.yes` and prompt exist (`apply.rs:55-59`, `apply.rs:170-197`). |
| H6 | Drain-then-pause missing | Fixed | Scheduler pause after lock and resume guard exist (`executor.rs:427-445`, `executor.rs:1074-1086`). |
| H7 | Diamond consistency missing | Fixed | Planner applies diamond promotion (`plan.rs:671-677`); integration test exists. |
| H8 | Status drift hardcoded zero | Fixed for stream deltas | `status.rs:67-82` computes stream-table drift; source/consumer drift still missed (CORR-8). |
| H9 | Status watch held one connection | Fixed | Watch reconnects per poll (`status.rs:214-226`). |
| H10 | CI single PostgreSQL version | Open | Matrix is still only PG 18 (`ci.yml:81-84`). |
| M1 | Secret inline syntax dead code | Fixed | `resolve_dsn_with_opts` calls `resolve_dsn_secrets` (`commands/mod.rs:136-142`). |
| M2 | Testkit catalog diverged from init | Fixed | Testkit uses core catalog constant (`aqueduct-testkit/src/lib.rs:82-84`). |
| M3 | Plan exit docs wrong | Open | API still says `1` for non-empty by default (`docs/api-reference.md:23`); code exits 1 only with flags (`plan.rs:172-177`). |
| M4 | CDC docs/implementation mismatch | Fixed | API documents trigger/wal/none plus aliases (`docs/api-reference.md:217`). |
| M5 | `--output` vs `--format` docs mismatch | Mostly Fixed | Plan docs use `--format`; other stale flags remain (ERG-4). |
| M6 | Validation lacks line numbers | Partially Fixed | Diagnostic type exists, but CLI validate uses legacy string result (`validate.rs:205-218`, `validate.rs:150-186`). |
| M7 | Destroy missing confirmation | Fixed | `--confirm` exists and is required unless dry-run (`destroy.rs:22-38`, `destroy.rs:48-56`). |
| M8 | Plaintext password guard missing | Partially Fixed | URL DSNs rejected (`config.rs:20`, `config.rs:252-260`); keyword-value DSNs still pass (SEC-1). |
| M9 | `RecordSnapshot` wrote `{}` | Partially Fixed | Apply/rollback pass desired state (`apply.rs:234-237`, `rollback.rs:269-271`); promote does not (CORR-5). |
| M10 | IVM checks never run in plan | Fixed | See H2. |
| M11 | No quiet/porcelain | Partially Fixed | Global flags exist (`main.rs:29-33`) but command `println!` output ignores them (ERG-1). |
| M12 | Progress always `{}` | Partially Fixed | Per-step progress exists, but finish resets it (`executor.rs:316-321`). |
| M13 | CI action project-dir issue | No longer applicable | `--project-dir` exists in plan args (`plan.rs:22-24`) and action. |
| M14 | Action used missing `--fail-on-drift` | Fixed | Plan has `fail_on_drift` (`plan.rs:35-37`). |
| M15 | Status JSON omits pg_version | Open | JSON object lacks `pg_version` despite collecting it (`status.rs:48-53`, `status.rs:98-110`). |
| M16 | Status watch no reconnect | Fixed | See H9. |
| L1 | Regex hot-path unwrap/recompile | Partially Fixed | Core uses `LazyLock`; CLI `redact_dsn` still recompiles (`commands/mod.rs:51`). |
| L2 | `fmt` swallows read errors | Open | `read_original_content` uses `unwrap_or_default` (`fmt.rs:126-127`). |
| L3 | `CANONICAL_KEY_ORDER` misleading | Open | Constant only used for unknown-key skip (`fmt.rs:12-23`, `fmt.rs:208`). |
| L4 | Testkit duplicated catalog SQL | Fixed | Testkit imports core constant (`aqueduct-testkit/src/lib.rs:82-84`). |
| L5 | No property-based tests | Fixed | Proptest tests exist (`integration.rs:3989-4118`). |
| L6 | No maintenance-window tests | Fixed | CLI integration includes maintenance-window enforcement (`cli_integration.rs:1168-1212`). |
| L7 | CI env var truthiness | Open | Any `GITHUB_ACTIONS` value enables JSON logs (`main.rs:95-100`). |
| L8 | Release archive naming inconsistency | Partially Fixed | Release names are consistent, but composite actions still use incompatible names (CI-1). |
| L9 | No cargo audit in CI | Fixed | Security audit job exists (`ci.yml:37-53`). |
| L10 | Tracing init concern | No longer applicable | No evidence this affects current binary tests. |
| L11 | Testcontainers image unpinned | Fixed | Default image is `postgres:18-alpine` (`aqueduct-testkit/src/lib.rs:13`). |
| L12 | Node24 workaround | Open | `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` remains (`release.yml:10`). |
| L13 | Top-level help lacks subcommand flags | No longer applicable | This is normal clap behavior; subcommand help covers flags. |
| L14 | TestDb public connection string | Open | Field remains public (`aqueduct-testkit/src/lib.rs:20`). |

---

## Findings

### [CORR-1] Failed migrations erase resume checkpoints
**Severity:** Critical
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 316-321, 337-352, 1044)
**Description:** `run_steps` writes progress after each completed step, and `find_resume_step` reads `completed_steps`. However, `execute` calls `FINISH_MIGRATION_SQL` with `serde_json::json!({})` for every result, including the `recoverable_failure` case. Since `FINISH_MIGRATION_SQL` sets `progress = $4`, a normal executor error destroys the checkpoint needed by `--resume`.
**Impact:** A migration that fails through a returned error, rather than process crash, is marked `recoverable_failure` but resumes from step 0. Already-completed destructive steps can be repeated, and the operator is told recovery exists when the recorded progress has been wiped.
**Recommendation:** Preserve progress on failure. Either remove `progress = $4` from `FINISH_MIGRATION_SQL`, pass through the latest progress JSON, or only clear progress on `committed`. Add an integration test that causes `run_steps` to fail after a non-idempotent step and then asserts `--resume` skips that step.
**Effort:** XS (< 1h)

### [CORR-2] Plan execution lacks an atomic unit of work
**Severity:** Critical
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 427-1089), `crates/aqueduct-core/src/catalog.rs` (lines 392-402)
**Description:** The executor performs scheduler pause, DDL/function calls, catalog inserts, progress updates, snapshot writes, and lock release as independent auto-commit statements. It also swallows the final migration-status update error with `.ok()` (`executor.rs:315-329`).
**Impact:** A crash or network error can leave the stream table changed but the DAG version missing, the scheduler paused but the migration status stale, or the migration committed in the database but not in the catalog. This is especially dangerous around `DropStreamTable`, consumer view swaps, and blue/green transitions.
**Recommendation:** Introduce explicit transactional boundaries for catalog bookkeeping and DDL groups that PostgreSQL can safely transact. Where pg_trickle operations cannot be held in one transaction, implement a saga-style step registry with compensating/resume guards and make final status updates fatal or explicitly surfaced.
**Effort:** L (3-5 days)

### [CORR-3] Heartbeat lock loss is logged but not acted on
**Severity:** High
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 366-375, 1145-1188), `crates/aqueduct-core/src/error.rs` (lines 68-70)
**Description:** The comments say the heartbeat signals a shared atomic and the main loop aborts with `LockLost`, but the implementation only logs renewal failures or `Ok(0)`. No state is shared back to `run_steps`, and `AqueductError::LockLost` is never returned.
**Impact:** If the heartbeat connection cannot connect, loses the row, or is starved long enough for another process to steal the lock, the migration continues under a false exclusivity assumption.
**Recommendation:** Replace the fire-and-forget heartbeat with a watched cancellation/error channel. Check that channel before and after every step, and return `LockLost` if renewal fails or affects zero rows. Add a test that deletes/steals the lock mid-apply.
**Effort:** M (1-2 days)

### [CORR-4] Promotion ignores project ownership and can diff against another project
**Severity:** High
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/promote.rs` (lines 46-64, 80-98), `crates/aqueduct-cli/src/commands/promote.rs` (lines 55-67)
**Description:** `compute_promotion_plan` and `validate_source_clean` call `read_live_state(client, None)`. Passing `None` disables the ownership filter that other commands use, so promotion reads every row in `pgtrickle.pgt_stream_tables`.
**Impact:** In a shared database, promoting project A can produce drops/alters for project B, or fail the source-clean check because unrelated project tables exist. This violates the multi-project isolation work added in v0.10.
**Recommendation:** Pass `Some(&options.project)` through all promotion live-state reads. Add a two-project promotion test where the destination contains another project's table and ensure it is ignored.
**Effort:** S (half day)

### [CORR-5] Promotion bypasses executor safety context
**Severity:** High
**Area:** Correctness
**File(s):** `crates/aqueduct-cli/src/commands/promote.rs` (lines 66-67, 100-106), `crates/aqueduct-core/src/executor.rs` (lines 224-232, 564-570)
**Description:** `promote` uses `connect`, not `connect_and_migrate`, and constructs `PlanExecutor::new(...)` without `.with_desired_state(...)` or `.with_connection_string(...)`. That means promoted versions record `{}` in `spec_jsonb`, and promoted applies have no lock heartbeat DSN.
**Impact:** Rollback to a promoted version cannot restore the DAG spec, and long-running promotions are vulnerable to lock expiry. Catalog migrations are also not guaranteed before promotion touches v5 tables.
**Recommendation:** Load the destination desired `DagState` once, call `connect_and_migrate`, and build the executor with desired state plus destination DSN. Add tests asserting promotion writes non-empty `spec_jsonb` and renews locks.
**Effort:** S (half day)

### [CORR-6] RLS policy restoration is a placeholder that silently does nothing
**Severity:** High
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/plan.rs` (lines 863-866, 998-1001), `crates/aqueduct-core/src/executor.rs` (line 841)
**Description:** Rebuild paths emit `RecreatePolicy` steps with SQL comments such as `-- RLS policies for ... will be restored by executor`. The executor simply executes `policy_sql.as_str()`, so it executes a comment and does not restore any real policies.
**Impact:** Drop/recreate rebuilds can remove row-level security policies and leave recreated stream tables without the protections users had before the migration.
**Recommendation:** Before dropping a table, query `pg_policies` and `pg_class.relrowsecurity`, serialize real `CREATE POLICY` / `ALTER TABLE ENABLE ROW LEVEL SECURITY` statements, and execute them after recreation. Until implemented, remove the step or mark rebuild plans as policy-lossy.
**Effort:** M (1-2 days)

### [CORR-7] Consumer view drops leave stale catalog rows
**Severity:** Medium
**Area:** Correctness
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 761-776, 812-823), `crates/aqueduct-core/src/catalog.rs` (lines 557-568)
**Description:** `ManageConsumerView { action: "drop" }` drops the database view but does not call `DELETE_CONSUMER_VIEW_SQL`. Create/alter paths upsert the catalog row, so drops are asymmetric.
**Impact:** The next `read_live_state` still sees the consumer view in `aqueduct.consumer_views`, causing repeated drop plans or drift that cannot be cleared by apply.
**Recommendation:** In the drop arm, call `DELETE_CONSUMER_VIEW_SQL` with project/name after successful `DROP VIEW`, and make stale-row cleanup idempotent.
**Effort:** XS (< 1h)

### [CORR-8] Status drift ignores source and consumer deltas
**Severity:** Medium
**Area:** Correctness
**File(s):** `crates/aqueduct-cli/src/commands/status.rs` (lines 67-82), `crates/aqueduct-core/src/diff.rs` (lines 171-177)
**Description:** `status` computes drift by counting only `diff.deltas`, while `DagDiff::is_empty` also considers `source_deltas` and `consumer_deltas`.
**Impact:** Base-table DDL drift and consumer-view drift can be present while `aqueduct status --fail-on-drift` exits successfully.
**Recommendation:** Use `diff.is_empty()` for boolean drift and count all three collections for a total drift count. Include per-area counts in JSON output.
**Effort:** XS (< 1h)

### [ARCH-1] Catalog schema override is parsed but ignored
**Severity:** High
**Area:** Architecture
**File(s):** `crates/aqueduct-core/src/config.rs` (lines 34-42), `crates/aqueduct-cli/src/commands/init.rs` (lines 18-21, 54), `crates/aqueduct-core/src/catalog.rs` (lines 7-11)
**Description:** Config exposes `project.catalog_schema`, and `init` exposes `--schema`, but all catalog SQL constants and runtime queries hardcode `aqueduct`. `init` never uses `args.schema`.
**Impact:** Users cannot isolate multiple catalog schemas or avoid collisions as ROADMAP promises. The flag creates a false sense of configurability.
**Recommendation:** Either remove the schema override until implemented, or introduce a `Catalog` abstraction that quotes/injects the chosen schema into all catalog SQL in one place.
**Effort:** L (3-5 days)

### [ARCH-2] Executor correctness depends on optional builder calls
**Severity:** Medium
**Area:** Architecture
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 148-160, 224-232), `crates/aqueduct-cli/src/commands/apply.rs` (lines 234-237), `crates/aqueduct-cli/src/commands/promote.rs` (lines 100-106)
**Description:** `PlanExecutor::new` creates a partially-safe executor; safety features such as heartbeat and snapshot recording require optional builder calls. Apply and rollback remember them; promote does not.
**Impact:** New command implementations can easily bypass core safety properties without compile-time feedback.
**Recommendation:** Replace the builder with typed constructors requiring `ExecutionContext { dsn, desired_state, mode }`, and make dry-run the only context allowed to omit DB-write dependencies.
**Effort:** M (1-2 days)

### [ARCH-3] Blue/green is not an atomic deployment system yet
**Severity:** High
**Area:** Architecture
**File(s):** `crates/aqueduct-core/src/plan.rs` (lines 1139-1275), `crates/aqueduct-core/src/executor.rs` (lines 702-737, 742-757), `crates/aqueduct-core/src/catalog.rs` (lines 572-588)
**Description:** The planner emits a plausible blue/green step sequence, but the executor swaps views one by one without a transaction, does not write `blue_green_deployments`, and `RetireBlueSchema` with `retain_secs > 0` only logs. The catalog SQL for start/swap/retire is defined but unused.
**Impact:** A failure midway through `SwapConsumerViews` can leave consumers split across blue and green schemas. There is no durable record of active/retired deployments and no later cleanup.
**Recommendation:** Make blue/green a transactionally grouped operation: insert deployment row, create all green objects, validate convergence, atomically replace all consumer views in one transaction where possible, update deployment status, and schedule/perform retirement with durable metadata.
**Effort:** XL (> 1 week)

### [ERG-1] Global `--quiet` and `--porcelain` do not suppress command output
**Severity:** Medium
**Area:** Ergonomics
**File(s):** `crates/aqueduct-cli/src/main.rs` (lines 29-33, 91-103), representative commands `crates/aqueduct-cli/src/commands/apply.rs` (lines 154-205)
**Description:** The global flags only change logging level. Command handlers still call `println!`/`eprintln!` directly and do not receive a global output mode.
**Impact:** Scripted pipelines using `--quiet` or `--porcelain` still get decorative text and prompts, making the flags unreliable.
**Recommendation:** Pass an `OutputMode` into command handlers or centralize output behind an emitter that respects quiet/porcelain/json modes.
**Effort:** M (1-2 days)

### [ERG-2] `destroy` bypasses the main error/exit-code path
**Severity:** Medium
**Area:** Ergonomics
**File(s):** `crates/aqueduct-cli/src/commands/destroy.rs` (lines 48-56), `crates/aqueduct-cli/src/main.rs` (lines 127-137)
**Description:** Missing `--confirm` uses `eprintln!` and `std::process::exit(1)`, while the rest of the CLI returns errors and main maps them to exit 2.
**Impact:** CI cannot consistently interpret errors, and tracing/structured error handling is bypassed for a common destructive-command mistake.
**Recommendation:** Replace manual exit with `anyhow::bail!` and add an exit-code test.
**Effort:** XS (< 1h)

### [ERG-3] YAML output is hand-written and not safely escaped
**Severity:** Medium
**Area:** Ergonomics
**File(s):** `crates/aqueduct-cli/src/commands/plan.rs` (lines 181-216), `crates/aqueduct-cli/src/commands/status.rs` (lines 112-131), `crates/aqueduct-cli/src/commands/diff.rs` (lines 86-99)
**Description:** YAML strings are built with `format!("... \"{}\"")` and no escaping for quotes, newlines, or colons.
**Impact:** Project names, table names, descriptions, or versions containing YAML-special characters produce invalid YAML.
**Recommendation:** Use `serde_yaml` for all YAML outputs or introduce a tested YAML string-escape helper.
**Effort:** S (half day)

### [ERG-4] API reference still documents non-existent or misleading flags
**Severity:** Medium
**Area:** Ergonomics
**File(s):** `docs/api-reference.md` (lines 15-23, 43-46, 164-169), `crates/aqueduct-cli/src/commands/plan.rs` (lines 19-51), `crates/aqueduct-cli/src/commands/apply.rs` (lines 11-60)
**Description:** The API reference documents `aqueduct plan --project`, `--no-cost`, default exit code 1 for a non-empty plan, and apply flags `--patroni-endpoint`, `--timeout`, `--lock-timeout` that are not present in `ApplyArgs`.
**Impact:** Users following the reference hit clap errors or build incorrect CI logic.
**Recommendation:** Generate the CLI reference from `aqueduct <cmd> --help` in CI and fail on drift. Keep narrative docs separate from generated option tables.
**Effort:** S (half day)

### [TEST-1] Resume tests do not cover the real executor-failure path
**Severity:** High
**Area:** TestCoverage
**File(s):** `crates/aqueduct-core/tests/integration.rs` (lines 3731-3829), `crates/aqueduct-core/src/executor.rs` (lines 304-321)
**Description:** `test_resume_failure_injection` manually inserts a `recoverable_failure` row with progress. It does not make `run_steps` fail and then inspect the progress that `execute` leaves behind.
**Impact:** The test suite passes while CORR-1 remains present.
**Recommendation:** Add a fault-injection plan step or mock pgtrickle function that fails after a completed DDL step, then assert the migration row still has `completed_steps` and resume skips the completed step.
**Effort:** M (1-2 days)

### [TEST-2] Binary-level CLI coverage is still shallow
**Severity:** Medium
**Area:** TestCoverage
**File(s):** `crates/aqueduct-cli/tests/cli_integration.rs` (lines 1216-1365)
**Description:** The binary-level `assert_cmd` coverage is concentrated on top-level `--help`, `--version`, selected subcommand help, no-args output, and validate help. The suite has good library/integration coverage, but many CLI command behaviors are tested by calling core functions rather than invoking the binary.
**Impact:** Exit codes, global flags, prompts, JSON/YAML shape, and command-specific clap regressions can slip through.
**Recommendation:** Add binary tests for each subcommand's success/error paths, including `--quiet`, `--porcelain`, `--log-format json`, stale plan, destroy confirmation, and apply prompt behavior.
**Effort:** L (3-5 days)

### [TEST-3] Mock pg_trickle pause/resume has no observable state
**Severity:** Medium
**Area:** TestCoverage
**File(s):** `crates/aqueduct-testkit/src/mock_pgtrickle.rs` (lines 92-101)
**Description:** `pause_scheduler` and `resume_scheduler` return `SELECT 1 WHERE false`; they do not record paused nodes or affect refresh status.
**Impact:** Scheduler pause/resume tests cannot verify that the executor actually drains or resumes affected nodes.
**Recommendation:** Add a mock scheduler-state table and have pause/resume mutate it. Assert it is set while a migration is running and cleared on success/failure.
**Effort:** S (half day)

### [PERF-1] Blue/green convergence polling is O(nodes) every 500ms
**Severity:** Medium
**Area:** Performance
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 637-675)
**Description:** `WaitForConvergence` loops over every node and issues one query per node every 500ms.
**Impact:** A 200-node blue/green rollout can issue hundreds of queries per second against `pgtrickle.pgt_stream_tables`, and the loop scales poorly as DAGs grow.
**Recommendation:** Query all node statuses in a single `WHERE schema_name = $1 AND table_name = ANY($2)` call and compare in memory. Add a max-query budget test.
**Effort:** S (half day)

### [PERF-2] Status watch repeatedly reparses the full project and rebuilds the diff
**Severity:** Medium
**Area:** Performance
**File(s):** `crates/aqueduct-cli/src/commands/status.rs` (lines 67-82, 214-244)
**Description:** Every watch poll reconnects, reloads all migration files, builds the DAG state, reads live state, and computes a full diff.
**Impact:** This is simple and robust, but on large projects it makes `status --watch` expensive and can become a noisy background load.
**Recommendation:** Cache desired state and its mtime/spec hash between polls, then only reload when files change. Keep reconnecting for live state.
**Effort:** M (1-2 days)

### [SEC-1] Plaintext password guard misses keyword-value DSNs
**Severity:** High
**Area:** Security
**File(s):** `crates/aqueduct-core/src/config.rs` (lines 20, 252-260), `crates/aqueduct-cli/src/commands/mod.rs` (lines 43-52)
**Description:** The guard regex detects `://user:pass@host`, but libpq keyword DSNs such as `host=db user=app password=secret dbname=prod` are accepted unless redacted for display.
**Impact:** The documented promise that plaintext passwords are refused unless `--allow-plaintext-password` is set is false for a common DSN format.
**Recommendation:** Extend `check_plaintext_password` with a case-insensitive keyword-value password detector that handles quoted values. Reuse or share logic with `redact_dsn` and add tests.
**Effort:** XS (< 1h)

### [SEC-2] Raw SQL fragments are executed without a trust/validation boundary
**Severity:** High
**Area:** Security
**File(s):** `crates/aqueduct-core/src/executor.rs` (lines 453-455, 716-726, 790-806, 1026-1028), `crates/aqueduct-core/src/preview.rs` (lines 232-240), `docs/security.md` (lines 128-131)
**Description:** Source DDL, hooks, consumer view bodies, and preview queries are interpolated/executed as SQL strings. Some of this is by design for migration tools, but the security guide says all database queries use parameterized statements and that there is no Rust-layer interpolation.
**Impact:** In shared CI or dbt-ingest workflows, a malicious migration artifact can execute arbitrary DDL under the aqueduct role. Even when trusted, malformed consumer SQL can break view creation after previous steps already ran.
**Recommendation:** Document migration files as trusted code, validate consumer SQL as a single SELECT query, reject multi-statement bodies, and separate intentionally arbitrary hooks/source DDL behind explicit trust controls.
**Effort:** M (1-2 days)

### [SEC-3] Read-only statement timeout is set before the transaction starts
**Severity:** Medium
**Area:** Security
**File(s):** `crates/aqueduct-cli/src/commands/mod.rs` (lines 89-97)
**Description:** `connect_read_only_with_timeout` executes `SET LOCAL statement_timeout = '...'; BEGIN READ ONLY`. `SET LOCAL` outside a transaction has no durable effect for the following transaction.
**Impact:** Plan/status queries may not actually be bounded by the intended statement timeout, despite docs and comments saying they are.
**Recommendation:** Execute `BEGIN READ ONLY; SET LOCAL statement_timeout = ...` in that order. Prefer a parameterized/validated duration whitelist rather than interpolating a string.
**Effort:** XS (< 1h)

### [DOC-1] README and installation docs advertise stale versions
**Severity:** Medium
**Area:** Documentation
**File(s):** `README.md` (lines 7, 147), `docs/installation.md` (lines 23-38, 52-54), `Cargo.toml` (lines 8-13)
**Description:** Cargo version is 0.13.0, while README status and tree say 0.11.0 and installation examples use 0.7.0.
**Impact:** New users cannot tell which features are current, which release to install, or whether the docs match the binary.
**Recommendation:** Centralize version strings or add a docs lint that checks README/installation examples against workspace package version.
**Effort:** XS (< 1h)

### [DOC-2] HA guide claims unimplemented Patroni/failover/resume controls
**Severity:** High
**Area:** Documentation
**File(s):** `docs/ha-operations.md` (lines 23-36, 59-90), `crates/aqueduct-cli/src/commands/apply.rs` (lines 11-60)
**Description:** The HA guide documents `--patroni-endpoint`, system identifier/timeline checks between steps, `status = 'interrupted'`, `--force-retry`, and `--force-skip`. These flags/statuses do not exist in the apply CLI or catalog status constraint.
**Impact:** Operators may believe failover is handled safely when only `pg_is_in_recovery()` is checked.
**Recommendation:** Downgrade the HA guide to current behavior, or implement the documented flags/statuses before calling them supported.
**Effort:** S (half day for docs; L for implementation)

### [CI-1] Composite actions download artifact names that releases do not publish
**Severity:** Critical
**Area:** CI-CD
**File(s):** `.github/actions/plan/action.yml` (lines 92-107), `.github/actions/apply/action.yml` (lines 100-114), `.github/workflows/release.yml` (lines 75-91, 118-133)
**Description:** Composite actions compute names like `aqueduct-${VERSION}-linux-x86_64.tar.gz`, while release artifacts are created as `aqueduct-${VERSION}-linux-amd64.tar.gz`, `linux-arm64`, etc.
**Impact:** Users of the published plan/apply actions cannot install released binaries through the actions.
**Recommendation:** Share one platform mapping between release and composite actions. Add an action-smoke job that downloads the artifact name produced by release packaging and then invokes the composite action.
**Effort:** S (half day)

### [CI-2] Action smoke test does not test the composite actions and uses the wrong migration directory
**Severity:** High
**Area:** CI-CD
**File(s):** `.github/workflows/action-smoke.yml` (lines 53-70, 91-95, 112-130), `crates/aqueduct-core/src/parser.rs` (lines 117-118)
**Description:** The smoke workflow comments say it exercises composite plan/apply actions, but it directly invokes the local binary. It also writes `/tmp/smoke-project/migrations/event_count.sql`, while the parser only reads `migrations/streams`, `migrations/sources`, and `migrations/consumers`.
**Impact:** The workflow can pass without testing the composite action installer and without planning the intended migration file.
**Recommendation:** Move the SQL file under `migrations/streams/`, install mock pgtrickle if apply should touch pgtrickle, and add steps that use `./.github/actions/plan` and `./.github/actions/apply`.
**Effort:** S (half day)

### [CI-3] Documentation CI is allowed to pass with broken docs tests and CLI reference drift
**Severity:** Medium
**Area:** CI-CD
**File(s):** `.github/workflows/ci.yml` (lines 172-173, 194-200)
**Description:** `mdbook test` has `continue-on-error: true`. The CLI reference check loops over an `export` command that does not exist, but only prints a failure and never exits non-zero.
**Impact:** Documentation examples and generated CLI coverage can rot while CI remains green.
**Recommendation:** Remove `continue-on-error`, or split known-broken examples behind explicit ignores. Make the CLI reference loop fail if any documented command lacks `--help`, and remove or implement `export`.
**Effort:** XS (< 1h)

### [DEP-1] Supply-chain audit relies on ignoring an unmaintained async runtime
**Severity:** Low
**Area:** Dependencies
**File(s):** `Cargo.toml` (line 69), `.github/workflows/ci.yml` (lines 51-53)
**Description:** `cargo audit` reports `async-std` as unmaintained via `httpmock`; CI ignores `RUSTSEC-2025-0052` because it is a dev-dependency path.
**Impact:** This is not a production binary risk today, but it normalizes permanent audit ignores and may grow risk if `httpmock` moves into non-dev code.
**Recommendation:** Track an issue to replace `httpmock` or isolate it under dev-only feature gates. Keep the ignore comment time-boxed.
**Effort:** S (half day)

### [ROAD-1] Roadmap/changelog status is internally inconsistent
**Severity:** High
**Area:** Roadmap
**File(s):** `ROADMAP.md` (lines 1-4, 44-56), `CHANGELOG.md` (lines 7-24, 33-41), `README.md` (line 7)
**Description:** ROADMAP says `v0.6 implementation complete`, CHANGELOG marks v0.13 released while v0.2-v0.7 are planned, and README claims v0.11 status.
**Impact:** Planning, release communication, and user trust suffer. It is impossible to know from docs which features are stable, planned, or scaffolded.
**Recommendation:** Pick a single release truth model. Mark roadmap milestones as `Implemented`, `Partially implemented`, or `Planned`; move stale historical phase text to an archive; update README for 0.13.0.
**Effort:** S (half day)

---

## Gap Analysis by Area

### 1. Correctness & Bug Audit
Current state: The core create/alter/drop/rollback flow is significantly healthier than in Phase 1, and the test suite covers many cookbook scenarios. The remaining correctness risk is concentrated in failure paths, promotion, blue/green, and catalog symmetry.

Key gaps identified:
- Failed executor runs wipe progress, undermining `--resume`.
- Multi-step execution is not atomic and final catalog status failures are swallowed.
- Promotion bypasses project filtering, desired-state snapshots, catalog migration, and heartbeat.
- RLS policy preservation and consumer-view drop cleanup are incomplete.
- Status drift does not count source/consumer drift.

Recommended remediation steps:
1. Fix progress preservation and add fault-injection resume tests.
2. Make final migration updates fatal or surfaced.
3. Refactor promote onto the same apply execution path/context.
4. Implement real RLS policy capture/restore.
5. Close consumer-view catalog symmetry and drift counting.

### 2. Architecture & Design Quality
Current state: The modules are well separated (`catalog`, `dag`, `diff`, `classifier`, `plan`, `executor`, `config`, diagnostics), and the plan pipeline is understandable. The architecture still leaks critical invariants through optional executor builder methods and hardcoded catalog schema SQL.

Key gaps identified:
- Catalog schema configurability is declared but not architected.
- Executor safety features are optional and easy to forget.
- Blue/green requires a first-class deployment state machine, not only plan variants.
- Catalog schema SQL is constant-based and hard to parameterize.

Recommended remediation steps:
1. Introduce typed execution contexts for apply/rollback/promote.
2. Centralize catalog schema and SQL rendering.
3. Promote blue/green into a transaction/saga abstraction with durable deployment rows.
4. Move stale-plan validation and spec hashing deeper into core so external callers cannot bypass it.

### 3. API & Ergonomics (CLI UX)
Current state: The CLI surface is broad and mostly coherent: `plan`, `apply`, `diff`, `status`, `validate`, `lint`, `fmt`, `ingest`, `import`, `rollback`, `promote`, `destroy`, `preview`, and `unlock` exist. UX consistency still lags because global output modes are not wired through, some commands manually exit, and docs are not generated from clap.

Key gaps identified:
- `--quiet`/`--porcelain` do not control direct command output.
- Exit code semantics are inconsistent around `destroy` and condition exits.
- YAML output is not robust.
- API docs list stale flags and wrong default exit semantics.

Recommended remediation steps:
1. Add a shared output/event emitter and pass output mode into every command.
2. Replace manual exits with typed outcomes in `main`.
3. Use generated docs for flags.
4. Serialize all machine formats through serde.

### 4. Test Coverage & Test Quality
Current state: Coverage is much better than the prior report suggested: 275 tests pass, all 30 cookbook scenarios appear represented, and proptest exists. The tests still miss several failure modes where safety claims matter most.

Key gaps identified:
- No executor-error resume test catches progress erasure.
- Composite GitHub actions are not actually smoke-tested.
- Mock scheduler pause/resume has no state.
- Binary CLI tests do not cover all output modes and exit codes.
- Blue/green atomicity and failure recovery tests are missing.

Recommended remediation steps:
1. Add fault-injection primitives to the testkit.
2. Add binary CLI snapshot/exit tests for every command.
3. Make action-smoke run the composite actions locally.
4. Add multi-project promotion tests.
5. Add blue/green partial-failure tests.

### 5. Performance & Scalability
Current state: The code uses reasonable data structures for small/medium DAGs, and source dependency indexing avoids a prior N x parse-cost pattern. Large DAGs will expose polling and repeated parsing costs.

Key gaps identified:
- Blue/green convergence polls one row at a time per node.
- `status --watch` reloads and reparses the entire project each poll.
- Plan execution is serial even for independent DAG branches.
- Cost estimates are display-only, not used to order or gate work.

Recommended remediation steps:
1. Batch status polling and convergence queries.
2. Cache desired state by spec hash/mtime in watch mode.
3. Identify independent step groups for future parallelism.
4. Make the cost model feed maintenance-window and plan ordering decisions.

### 6. Security Audit
Current state: There is no unsafe Rust, most database calls are parameterized, DSN redaction exists, and secret backends are implemented. Security documentation overstates protections, and several trust boundaries need explicit design.

Key gaps identified:
- Keyword-value DSN plaintext passwords are not rejected.
- Trusted migration SQL vs. untrusted dbt/CI artifacts is not clearly separated.
- Read-only timeout setup is wrong.
- RLS policy preservation is not implemented.
- Security docs are stale for secret backend status and injection claims.

Recommended remediation steps:
1. Fix password detection and add regression tests.
2. Document migration files as trusted code and validate single-statement consumer SQL.
3. Correct read-only transaction setup.
4. Implement real policy capture/restore before claiming RLS-safe rebuilds.
5. Align security docs with actual behavior.

### 7. Documentation & Developer Experience
Current state: README, tutorials, cookbook, and API docs are substantial and approachable. The problem is truthfulness drift: several docs describe future-state behavior as shipped.

Key gaps identified:
- README/install versions are stale.
- HA guide claims unimplemented flags and failover semantics.
- API reference has stale flags.
- Security guide says cloud secret backends are planned while code/changelog say implemented.
- No `CONTRIBUTING.md` or clear dev setup beyond `justfile`.

Recommended remediation steps:
1. Generate CLI reference from clap help.
2. Add docs tests that fail CI.
3. Add `CONTRIBUTING.md` with Docker/Testcontainers expectations and common commands.
4. Update roadmap/changelog/README to one release truth.

### 8. CI/CD & Release Pipeline
Current state: CI covers fmt, clippy, cargo audit, unit/integration/CLI tests, coverage, docs build, MSRV, and mock pgtrickle integration. Release builds five platform artifacts with checksums.

Key gaps identified:
- Composite actions cannot find release artifacts.
- Action smoke does not exercise composite actions.
- PostgreSQL test matrix is only 18.
- `mdbook test` is non-blocking.
- Release does not publish crates.io packages or verify install scripts.

Recommended remediation steps:
1. Fix artifact naming and action-smoke.
2. Add PG 14-18 matrix, at least for a focused integration subset.
3. Make docs tests blocking.
4. Add release dry-run install verification.
5. Decide whether crates.io publishing is in scope and automate it if yes.

### 9. Dependency & Ecosystem Audit
Current state: Dependencies are idiomatic and not excessive for a Rust CLI. The workspace is appropriately split into core, CLI, and testkit.

Key gaps identified:
- `httpmock` pulls unmaintained `async-std`, currently ignored in cargo audit.
- Manual YAML avoids a small dependency but creates correctness issues.
- `reqwest` is correctly configured with rustls and no OpenSSL.
- Feature flags are minimal; dev-only dependencies are separated by crate dev-deps.

Recommended remediation steps:
1. Track and replace/upgrade `httpmock` when possible.
2. Add `serde_yaml` or a dedicated escaping helper.
3. Keep dependency audit ignores time-boxed and documented.

### 10. Roadmap Feasibility & Feature Completeness
Current state: Many roadmap claims from v0.8-v0.13 are implemented in some form, and earlier Phase 1 criticals are mostly fixed. The truth gap is now about depth: several features are scaffolded or happy-path-only but documented as production-grade.

Key gaps identified:
- Blue/green lacks atomic tracking and cleanup.
- HA failover support is documented but not implemented.
- Consumer layer exists but catalog/drop behavior is incomplete.
- Multi-schema catalog support is not real.
- GitOps composite actions are broken by artifact naming.

Recommended remediation steps:
1. Reclassify roadmap items into `Implemented`, `Partial`, `Scaffolded`, `Planned`.
2. Prioritize recovery, CI actions, and docs truth before new features.
3. Treat blue/green and HA as design projects, not checklist items.

---

## Missing Test Cases

1. **resume_preserves_progress_after_executor_error**
   - Verifies: failed `run_steps` leaves `completed_steps` intact.
   - Why it matters: catches CORR-1.
   - Skeleton:
     ```rust
     // create plan with step 0 lock, step 1 create, step 2 failing hook
     let err = executor.execute(&plan).await.unwrap_err();
     let progress = query_progress(migration_id).await;
     assert_eq!(progress["completed_steps"], 1);
     ```

2. **resume_skips_completed_destructive_step_after_failure**
   - Verifies: resumed apply does not re-drop/recreate a completed table.
   - Why it matters: validates actual crash-safety semantics.
   - Skeleton:
     ```rust
     fail_after_drop_once();
     run_apply_expect_failure();
     run_apply_resume();
     assert_eq!(drop_call_count("orders"), 1);
     ```

3. **heartbeat_lock_stolen_aborts_main_executor**
   - Verifies: deleting/updating the lock during execution returns `LockLost`.
   - Why it matters: lock exclusivity must be real for long migrations.
   - Skeleton:
     ```rust
     start_blocking_step();
     steal_lock(project);
     assert_matches!(join_apply().await, Err(AqueductError::LockLost));
     ```

4. **promote_filters_destination_by_project**
   - Verifies: project B tables are ignored when promoting project A.
   - Why it matters: prevents cross-project destructive plans.
   - Skeleton:
     ```rust
     seed_project("a", ["a_orders"]); seed_project("b", ["b_orders"]);
     let plan = compute_promotion_plan(... project="a").await?;
     assert!(!plan_mentions("b_orders"));
     ```

5. **promote_records_non_empty_spec_jsonb**
   - Verifies: promoted DAG versions can be rolled back.
   - Why it matters: closes CORR-5.
   - Skeleton:
     ```rust
     run_promote();
     let spec = latest_spec_jsonb("project");
     assert!(spec["stream_tables"].as_array().unwrap().len() > 0);
     ```

6. **consumer_drop_deletes_catalog_row**
   - Verifies: dropping a consumer view removes `aqueduct.consumer_views` row.
   - Why it matters: prevents permanent drift.
   - Skeleton:
     ```rust
     apply_consumer(); remove_consumer_file(); apply();
     assert_eq!(count_consumer_rows(project), 0);
     ```

7. **status_counts_consumer_and_source_drift**
   - Verifies: `status --fail-on-drift` fails on source/consumer deltas.
   - Why it matters: status is a CI safety net.
   - Skeleton:
     ```rust
     mutate_consumer_catalog();
     assert_cmd!("aqueduct status --fail-on-drift").failure().code(2);
     ```

8. **keyword_dsn_plaintext_password_rejected**
   - Verifies: `host=... password=secret` fails without override.
   - Why it matters: common DSN format.
   - Skeleton:
     ```rust
     assert!(check_plaintext_password("host=x password=s", false).is_err());
     ```

9. **read_only_timeout_is_active**
   - Verifies: `SHOW statement_timeout` inside plan/status session is `30s`.
   - Why it matters: protects catalog reads.
   - Skeleton:
     ```rust
     let client = connect_read_only_with_timeout(dsn, "30s").await?;
     assert_eq!(show_timeout(&client).await?, "30s");
     ```

10. **blue_green_swap_is_all_or_nothing**
    - Verifies: injected failure during second view swap rolls back first swap.
    - Why it matters: atomic consumer cutover.
    - Skeleton:
      ```rust
      inject_failure_on_view("v2");
      let before = view_targets(); apply_blue_green().unwrap_err();
      assert_eq!(view_targets(), before);
      ```

11. **blue_green_deployment_row_lifecycle**
    - Verifies: active -> swapped -> retired rows are written.
    - Why it matters: cleanup/audit.
    - Skeleton:
      ```rust
      apply_blue_green();
      assert_eq!(deployment_status(project), "swapped");
      ```

12. **rls_policies_restored_after_rebuild**
    - Verifies: RLS policies survive drop/recreate plans.
    - Why it matters: data access safety.
    - Skeleton:
      ```rust
      create_policy(); apply_rebuild();
      assert!(policy_exists("orders_policy"));
      ```

13. **quiet_suppresses_decorative_output**
    - Verifies: `--quiet plan` emits no normal stdout.
    - Why it matters: scripting contract.
    - Skeleton:
      ```rust
      cmd.args(["--quiet", "plan", ...]).assert().stdout("");
      ```

14. **porcelain_outputs_key_value_only**
    - Verifies: no rendered plan text appears in porcelain mode.
    - Why it matters: stable CI parsing.
    - Skeleton:
      ```rust
      assert_stdout_matches("is_empty=(true|false)\n");
      ```

15. **yaml_escapes_quotes_and_newlines**
    - Verifies: YAML output parses when names contain quotes/colon/newline.
    - Why it matters: machine-readable output correctness.
    - Skeleton:
      ```rust
      let y = render_plan_yaml(project_with_quote());
      serde_yaml::from_str::<serde_yaml::Value>(&y).unwrap();
      ```

16. **composite_plan_action_downloads_release_artifact**
    - Verifies: local action can install an archive named by release convention.
    - Why it matters: published CI action usability.
    - Skeleton:
      ```yaml
      - uses: ./.github/actions/plan
        with: { version: ${{ steps.version.outputs.version }}, ... }
      ```

17. **action_smoke_reads_streams_directory**
    - Verifies: smoke project places SQL under `migrations/streams` and plan is non-empty.
    - Why it matters: current smoke test can pass with zero migrations.
    - Skeleton:
      ```bash
      test -f /tmp/smoke-project/migrations/streams/event_count.sql
      aqueduct plan ... | grep event_count
      ```

18. **postgres_version_matrix_min_supported**
    - Verifies: focused integration tests pass on PG 14, 15, 16, 17, 18.
    - Why it matters: supported managed PostgreSQL range.
    - Skeleton:
      ```yaml
      strategy: { matrix: { pg-version: ["14", "15", "16", "17", "18"] } }
      ```

19. **mock_scheduler_state_pause_resume**
    - Verifies: mock records paused nodes and clears them on resume.
    - Why it matters: scheduler safety tests need observability.
    - Skeleton:
      ```rust
      apply_plan();
      assert_eq!(paused_nodes_after(), Vec::<String>::new());
      ```

20. **validate_differential_ivm_unsupportable_fails**
    - Verifies: `aqueduct validate` fails a DIFFERENTIAL `SELECT DISTINCT` query.
    - Why it matters: offline validation should match plan validation.
    - Skeleton:
      ```rust
      write_stream("SELECT DISTINCT id FROM raw.orders");
      cmd.args(["validate", "--project-dir", dir]).assert().failure();
      ```

---

## Prioritised Feature Backlog

| # | Feature | Value | Effort | Rationale |
|---:|---|---|---|---|
| 1 | Preserve resume progress on failure | Very High | XS | Restores a core safety promise quickly. |
| 2 | Fix release/action artifact naming | Very High | S | Unblocks GitOps users immediately. |
| 3 | Promote safety/context refactor | Very High | S | Prevents cross-project and rollback failures. |
| 4 | Transaction/saga execution hardening | Very High | L | Reduces partial-state risk across all commands. |
| 5 | Lock-loss propagation | High | M | Makes long-running apply concurrency control real. |
| 6 | Blue/green deployment state machine | High | XL | Turns scaffolded steps into production behavior. |
| 7 | RLS policy capture/restore | High | M | Prevents security regressions on rebuilds. |
| 8 | Catalog schema abstraction | High | L | Required for documented schema override/multi-tenant use. |
| 9 | Generated CLI reference docs | High | S | Stops recurring docs drift. |
| 10 | Output emitter for quiet/porcelain/json | Medium | M | Makes CLI scriptable and consistent. |
| 11 | PG 14-18 CI matrix | Medium | S | Increases confidence on supported platforms. |
| 12 | YAML serialization via serde | Medium | S | Fixes machine-readable output correctness. |
| 13 | Mock scheduler state | Medium | S | Enables realistic drain/pause tests. |
| 14 | Status drift area counts | Medium | XS | Makes drift monitoring complete. |
| 15 | HA failover design implementation | Medium | XL | Needed before marketing HA-safe migrations. |

---

## Competitive Analysis

### Where pg-aqueduct leads
- It targets a niche the incumbents do not: state-aware migration of pg_trickle stream-table DAGs.
- The migration-class model (Free, In-place, Rebuild, Blue/green) is more domain-specific than Atlas/Flyway/Liquibase/sqitch, and the cookbook maps real stream-DAG changes to classes.
- It understands DAG topology and pg_trickle metadata, which dbt does not own.
- Rust implementation, typed plan steps, and Testcontainers coverage are good foundations for reliability.

### Where it is behind
- Atlas has mature schema diffing, declarative state, drift detection, and transaction behavior for relational schemas.
- Flyway/Liquibase have mature audit trails, checksum validation, repair commands, and enterprise CI/CD patterns.
- sqitch has a clearer deploy/revert/verify mental model and avoids overclaiming automatic rollback.
- dbt has a much stronger model compilation ecosystem, docs generation, lineage, and adoption.
- pg-aqueduct's release/action/docs pipeline is not yet trustworthy enough for production users to treat as stable.

### What would establish a clear moat
- A genuinely safe pg_trickle-aware executor with reliable resume, lock-loss detection, and policy preservation.
- Atomic blue/green consumer swaps with durable deployment tracking and cleanup.
- Deep dbt ingestion that preserves lineage and validates compiled SQL before apply.
- GitOps-native plan/apply actions that produce immutable plan artifacts, check spec hashes, and post accurate PR comments.
- Observability integrations: Prometheus metrics, structured events, and audit queries for every step.

---

## Recommendations Summary

### 1. Do now — critical correctness fixes (< 1 week total)
- Preserve resume progress after failed executor runs.
- Fix composite action artifact naming and action-smoke coverage.
- Refactor `promote` to use project-filtered live state, catalog migration, heartbeat, and desired-state snapshots.
- Fix plaintext keyword DSN password detection.
- Correct docs that currently claim HA/failover flags that do not exist.

### 2. Do next — high-value improvements (next 1-2 milestones)
- Implement lock-loss propagation from heartbeat to executor.
- Add fault-injection tests for resume, blue/green, scheduler pause/resume, and consumer catalog cleanup.
- Replace YAML hand rendering and wire global output modes.
- Implement real RLS policy capture/restore.
- Batch blue/green convergence polling and status drift counting.

### 3. Do later — important but lower urgency (v1.0 horizon)
- Build a catalog schema abstraction for non-`aqueduct` schemas.
- Turn blue/green into a durable deployment state machine.
- Add PG 14-18 matrix coverage and real pg_trickle compatibility tests.
- Publish crates/binaries with a single release manifest consumed by install docs and actions.
- Add observability integrations and a documented repair/diagnose workflow.
