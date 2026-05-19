# pg_aqueduct — Overall Engineering Assessment

**Date:** 2026-05-19  
**Assessor:** Principal Engineer Review (automated deep-analysis)  
**Codebase version:** 0.7.0-dev (ROADMAP status: "v0.6 implementation complete")  
**Scope:** All source files in `crates/`, all CI/docs, all examples

---

## Executive Summary

### Overall Project Health: 4 / 10

The architectural foundation of `pg_aqueduct` is sound and well-conceived. The module decomposition, the diff/classify/plan/execute pipeline, the Testcontainers-based test harness, and the CI skeleton are solid starting points. The design principles in `ESSENCE.md` reflect real production experience with database migration tooling.

However, at least **six critical bugs exist in production code paths** that directly violate the tool's core safety guarantees:

1. **Rollback does not actually roll back** — the prior spec is read from the catalog and immediately discarded; rollback re-applies the current migration files.
2. **`--resume` is a no-op** — step progress is never written or read; a resumed apply re-executes everything including destructive steps already completed.
3. **The lock TTL is not renewed** — any migration taking longer than 30 seconds (the default TTL) leaves the lock expired, allowing concurrent applies.
4. **The catalog is never self-migrated** — `aqueduct init` installs v1 tables, but v0.3 code assumes v2 tables; consumer views and blue/green fail at runtime.
5. **In-place query changes are silently discarded** — `AlterStreamTable` in the executor ignores `new_query`; in-place column additions do not update the stream table's SQL.
6. **Multiple panickable `.unwrap()` calls** in `plan.rs` on code paths that are reachable with corrupted catalog state.

The gap between the roadmap's claimed `[x]` completions and the actual implementation is substantial. The CHANGELOG describes v0.6 and v0.7 features as "planned — not yet released", but the Cargo workspace version is `0.7.0` and the ROADMAP header says "v0.6 implementation complete." Several features described as delivered (secret backends, `--resume`, drain-then-pause, diamond DAG consistency, rollback, progress tracking) are scaffolded but not implemented.

### Top 5 Critical Issues

1. **C2**: `--resume` silently re-executes the full plan after a crash (no step progress tracking), risking data loss from duplicated destructive steps.
2. **C1**: `aqueduct rollback` ignores the recorded prior spec; it cannot roll back to a previous version.
3. **C5**: Lock TTL is not renewed during long migrations; concurrent applies can proceed simultaneously after TTL expiry.
4. **C6**: `aqueduct init` installs v1 catalog schema; v0.3+ commands requiring `consumer_views`, `ddl_log`, `blue_green_deployments` tables will fail at runtime.
5. **H1**: In-place query migrations silently discard `new_query`; the stream table's SQL is never updated during AlterQuery/InPlace operations.

### Top 5 Quick Wins

1. **Fix `--resume`** — write step index to `aqueduct.migrations.progress` after each step; read it on resume. Eliminates the crash-safety illusion. (~2 days)
2. **Fix `init` to use v2 catalog SQL** — change `commands/init.rs` line 30 to use `CATALOG_INIT_V2_SQL`. Unblocks all v0.3 features. (~1 hour)
3. **Fix `AlterStreamTable` to pass `new_query`** — add the new query argument to the `pgtrickle.alter_stream_table()` call. Fixes silent in-place migration failures. (~2 hours)
4. **Replace `--validate-ivm` to call `validate_ivm_supportability`** — single-line change in `commands/plan.rs:71`. Restores the IVM pre-validation promise. (~30 min)
5. **Implement the lock heartbeat** — spawn a `tokio::task` in `PlanExecutor::run_steps()` that periodically re-executes the lock upsert. Prevents lock expiry during long migrations. (~4 hours)

### Summary Statistics

| Severity | Count |
|----------|-------|
| Critical | 6 |
| High | 10 |
| Medium | 16 |
| Low | 14 |
| **Total** | **46** |

---

## Findings by Severity

---

### Critical

---

#### C1 — `aqueduct rollback` ignores the recorded prior spec; rollback is non-functional

**File:** `crates/aqueduct-cli/src/commands/rollback.rs:70–90`

**Problem:** The rollback command loads the target version's `spec_jsonb` from `aqueduct.dag_versions` but immediately assigns it to `let _spec_jsonb: serde_json::Value = row.get(0)` (the leading underscore is the Rust convention for "unused"). After this line, the variable is never referenced. Instead, the code proceeds to read the **current migration files from disk** and apply the diff between them and the live state. This is functionally identical to `aqueduct apply` and has nothing to do with rollback.

```rust
// rollback.rs:79
let _spec_jsonb: serde_json::Value = row.get(0);  // ← stored but immediately discarded

// rollback.rs:87–96 — reads CURRENT migration files, not the prior spec
let files = aqueduct_core::parser::load_migrations(&args.project_dir, &vars)?;
let desired = build_dag_state(&files, true)?;       // ← this is the CURRENT desired state
let actual = read_live_state(&client).await?;
let diff = compute_diff(&desired, &actual);          // ← current files vs live state
```

**Impact:** `aqueduct rollback` does not roll back. If an operator has modified migration files since the last apply (the common case), "rollback" will apply the new changes, not revert to the prior state. The `--to-version`, `--accept-data-loss`, and lossless window semantics documented in the ROADMAP are unreachable. Data loss from failed rollbacks in production.

**Recommended fix:** Deserialize `spec_jsonb` back into a `DagState` (store the spec as a serialised `DagState` in `dag_versions.spec_jsonb`), then use that as the `desired` state for the rollback plan. The `RecordSnapshot` step in `executor.rs` currently writes an empty `{}` spec — this must be fixed to write the full `DagState`.

---

#### C2 — `--resume` is silently a no-op; no step progress is ever written or read

**File:** `crates/aqueduct-core/src/executor.rs` (entire `run_steps()` function)

**Problem:** `ApplyArgs.resume` is accepted as a CLI flag but is never passed to `PlanExecutor::new()` or used anywhere in the executor. The `aqueduct.migrations.progress` JSONB column is written only once — as an empty `{}` in `FINISH_MIGRATION_SQL` — and is never populated with per-step progress during execution. As a result, `aqueduct apply --resume` re-executes the entire plan from the first step, including steps that completed before the crash.

**Impact:** The crash-safety guarantee stated in `ESSENCE.md` ("Crash safety. The plan executor checkpoints per-step progress… `aqueduct apply --resume` can pick up after any crash.") does not exist. After a crash mid-migration, running `--resume` will:
- Re-execute `LockDag` (may fail if not expired, blocking the resume)
- Re-execute `DropStreamTable` on tables that were already dropped (will succeed silently due to `DROP TABLE IF EXISTS`)
- Re-execute `CreateStreamTable` on tables that were just created (will fail or silently no-op depending on IF NOT EXISTS)
- Re-execute `Backfill` triggering a second full refresh

This can leave the DAG in an inconsistent partially-doubled state and cannot be used for crash recovery in production.

**Recommended fix:** Pass `resume: bool` to `PlanExecutor`. After each completed step, write `{"completed_steps": [i]}` to `aqueduct.migrations.progress` via an `UPDATE`. On resume, read the progress, find the last completed step index, and start from step `last_completed + 1`. For idempotent steps (ValidateQuery, UnlockDag) this is safe; for non-idempotent steps, add catalog guards.

---

#### C3 — Panickable `.unwrap()` on delta fields in `plan.rs` production code path

**File:** `crates/aqueduct-core/src/plan.rs:312, 355, 356, 412, 413`

**Problem:** `build_plan()` calls `.unwrap()` on `delta.desired` and `delta.actual` in multiple match arms:

```rust
// plan.rs:312 — in DeltaKind::Create arm
let spec = delta.desired.as_ref().unwrap();

// plan.rs:355-356 — in DeltaKind::AlterSchedule arm
let desired = delta.desired.as_ref().unwrap();
let actual  = delta.actual.as_ref().unwrap();

// plan.rs:412-413 — in DeltaKind::AlterQuery arm
let desired = delta.desired.as_ref().unwrap();
let _actual = delta.actual.as_ref().unwrap();
```

These are logic invariants — a `Create` delta should always have `desired` set, and an `Alter*` delta should always have both. However, nothing in the type system enforces this, and `compute_diff()` could produce inconsistent deltas if the catalog is corrupted, if an intermediate version fails during upgrade, or if a future code change introduces a subtle bug. A panic in production causes an uncontrolled crash without a recoverable error message.

**Impact:** Any inconsistency in the catalog or diff state causes an unhandled panic, crashing the CLI with a Rust backtrace rather than an actionable error message.

**Recommended fix:** Replace all five `.unwrap()` calls with `.ok_or_else(|| AqueductError::Other(format!("invariant violated: expected {} field on {:?} delta", "desired", delta.kind)))?`.

---

#### C4 — SQL injection via unquoted `spec.query` in executor fallback paths

**File:** `crates/aqueduct-core/src/executor.rs:148–155, ~303`

**Problem:** When `pg_trickle` is not installed, the `CreateStreamTable` fallback creates a table from the stream query using a raw `format!()`:

```rust
&format!(
    "CREATE TABLE IF NOT EXISTS {}.{} AS SELECT * FROM ({}) q LIMIT 0",
    quote_ident(schema),
    quote_ident(table),
    spec.query   // ← directly interpolated, no escaping
),
```

The schema and table names are properly quoted with `quote_ident()`, but `spec.query` is embedded verbatim. While `sqlparser` validates query syntax, it does not prevent injections where the query text manipulates the surrounding SQL structure. An attacker controlling the migrations directory (e.g., supply-chain attack on a CI-fetched dbt manifest) could craft a query string that escapes the subquery context.

A similar pattern appears in `CreateStreamTableInGreen` at approximately line 303, and in `preview.rs::create_preview_native()`.

**Impact:** Code-execution-level SQL injection for any user who imports migration files from an untrusted dbt `manifest.json` or who has migrations directory write access in a shared CI environment. The fallback path is frequently exercised in tests (the mock pg_trickle does install the extension, but in environments where it fails to load, the fallback triggers).

**Recommended fix:** This fallback path should not exist in production code. Remove it and return a clear `AqueductError::PgTrickleVersion("pg_trickle is required to apply migrations")` when pgtrickle is absent. The mock path should be restricted to test builds only.

---

#### C5 — Lock TTL is never renewed; concurrent applies can proceed after 30 seconds

**File:** `crates/aqueduct-core/src/executor.rs` (`run_steps()` function, no heartbeat task)

**Problem:** The `ACQUIRE_LOCK_SQL` grants a lock with a 30-second TTL. The ROADMAP states: "A background task updates `acquired_at` every `ttl / 3` during long-running migrations." No such background task exists anywhere in the codebase. The lock row in `aqueduct.locks` is never updated after acquisition until `RELEASE_LOCK_SQL` at the end.

A `Backfill { mode: "FULL" }` step on a large table can take minutes or hours. After the TTL expires, the `ACQUIRE_LOCK_SQL` `WHERE acquired_at + ttl < now()` condition becomes true, and any other `aqueduct apply` process can overwrite the lock holder and begin its own apply concurrently.

**Impact:** Two concurrent `aqueduct apply` runs can execute simultaneously against the same project. Both can modify the same stream tables, leading to undefined DAG state. The lock mechanism, which is the sole concurrency control in the system, provides no protection for long migrations.

**Recommended fix:** Spawn `tokio::spawn(async move { loop { tokio::time::sleep(ttl / 3).await; update_lock_acquired_at().await; } })` inside `run_steps()`. Cancel the heartbeat on lock release. This was explicitly designed into the architecture — it just needs to be implemented.

---

#### C6 — `aqueduct init` installs v1 catalog; v0.3+ runtime requires v2 tables

**File:** `crates/aqueduct-cli/src/commands/init.rs:30`, `crates/aqueduct-core/src/catalog.rs`

**Problem:** `init.rs` calls `client.batch_execute(CATALOG_INIT_SQL).await?`. `CATALOG_INIT_SQL` is the v1 baseline schema, which creates only: `dag_versions`, `migrations`, `locks`, `cluster_profile`. It does NOT create:
- `aqueduct.ddl_log` (required by companion extension detection, `detect_extension_installed()`)
- `aqueduct.consumer_views` (required by consumer view management, `ManageConsumerView` plan steps)
- `aqueduct.blue_green_deployments` (required by blue/green tracking)

The `CATALOG_SCHEMA_VERSION = 2` constant is defined in `catalog.rs` but no code path ever:
1. Reads the `catalog_schema_version` key from `cluster_profile` on connect
2. Applies the `CATALOG_MIGRATE_V1_TO_V2_SQL` migration

The ROADMAP says: "every CLI command compares its compiled-in `CATALOG_SCHEMA_VERSION` against the value in `aqueduct.cluster_profile` and applies any pending catalog migrations." This is not implemented.

`aqueduct-testkit` uses its own inline v2 SQL for tests, masking this bug entirely in the test suite.

**Impact:** After running `aqueduct init` (the first command any user runs), any v0.3 operation that touches consumer views or blue/green deployments fails at runtime with a PostgreSQL error like `ERROR: relation "aqueduct.consumer_views" does not exist`. The companion extension detection (`detect_extension_installed()`) also silently falls back because `aqueduct.ddl_log` is absent.

**Recommended fix:** Change `init.rs:30` to use `CATALOG_INIT_V2_SQL`. Implement the self-migration check as a function called at the start of every command that opens a database connection: read `catalog_schema_version`, compare to `CATALOG_SCHEMA_VERSION`, apply any pending migration SQL blocks.

---

### High

---

#### H1 — `AlterStreamTable` executor step ignores `new_query`; in-place SQL changes are silently dropped

**File:** `crates/aqueduct-core/src/executor.rs` (AlterStreamTable match arm)

**Problem:** When an in-place migration (`DeltaKind::AlterQuery` + `MigrationClass::InPlace`) is planned, `build_plan()` generates:

```rust
PlanStep::AlterStreamTable {
    name: delta.qualified_name.clone(),
    schedule: None, refresh_mode: None, cdc_mode: None,
    new_query: Some(desired.query.clone()),  // ← carries the new SQL
}
```

In `executor.rs`, the `AlterStreamTable` match arm calls:
```rust
pgtrickle.alter_stream_table($1, $2, $3, $4, $5)  // schema, name, schedule, refresh_mode, cdc_mode
```
The `new_query` field is not passed. `pgtrickle.alter_stream_table()` has no query parameter in its current mock signature (`p_schedule`, `p_refresh_mode`, `p_cdc_mode` only). The SQL backing the stream table is never updated.

**Impact:** Every in-place query migration (column addition, column removal) silently succeeds without changing the actual stream table query. The stream table continues to compute results from the old SQL. This produces incorrect data without any error.

**Recommended fix:** Either (a) extend `pgtrickle.alter_stream_table()` to accept a `p_query` parameter, or (b) for in-place migrations that include a query change, use a `DROP + CREATE` sequence (downgrading to Rebuild). The current `MigrationClass::InPlace` classification for query changes should only be trusted when the pg_trickle extension actually supports in-place query mutation.

---

#### H2 — `--validate-ivm` flag calls syntax validation only, not IVM validation

**File:** `crates/aqueduct-cli/src/commands/plan.rs:67–74`

**Problem:**
```rust
if args.validate_ivm {
    for table in &desired.stream_tables {
        if !table.query.is_empty() {
            aqueduct_core::validate::validate_sql_syntax(   // ← SQL SYNTAX ONLY
                &table.query,
                &table.qualified_name.to_string(),
            )?;
        }
    }
}
```
The flag is named `validate_ivm` (IVM = Incremental View Maintenance) and its default is `true`, but it calls `validate_sql_syntax()` instead of `validate_ivm_supportability()`. The IVM-specific checks (no `DISTINCT`, no set operations, no volatile functions) are never performed during `aqueduct plan`.

**Impact:** A stream table with `refresh_mode = "DIFFERENTIAL"` containing `SELECT DISTINCT` or `random()` passes plan validation, is applied successfully (creating the table), and only fails when pg_trickle's IVM engine attempts to maintain it incrementally. This is precisely the failure mode the ROADMAP says the tool prevents: "This prevents `aqueduct plan` from producing a plan that `pg_trickle` will reject at apply time — the most confusing failure mode a migration tool can have."

**Recommended fix:** Replace `validate_sql_syntax` with `validate_ivm_supportability` in the `plan.rs` block. Additionally check only tables with `refresh_mode = Differential`.

---

#### H3 — Column-removal InPlace classifier accepts mid-column drops that corrupt table state

**File:** `crates/aqueduct-core/src/classifier.rs:175–183`

**Problem:** The `SelectListDelta::Removal` case checks that every desired column appears in the actual list in order:
```rust
// desired: [col1, col3]  ←  removal of col2 from [col1, col2, col3]
let all_in_actual = {
    let mut actual_iter = a_strs.iter();
    d_strs.iter().all(|d| actual_iter.any(|a| a == d))
};
```
This returns `Removal` (classified as `InPlace`) for removing column 2 from a 3-column table. However, a materialized stream table stores physical columns. Removing a middle column while classifying as InPlace would leave the existing materialized data with the wrong schema (columns at incorrect physical positions for existing consumers).

**Impact:** False InPlace classification for column drops from non-tail positions. Consumers reading `col3` by position would silently read `col2` values after the "in-place" migration completes.

**Recommended fix:** `Removal` should only be classified as `InPlace` if the removed columns are at the **end** of the SELECT list (i.e., `d_strs` is a prefix of `a_strs`, not an ordered subset). Change the check to `a_strs.starts_with(&d_strs[..])` would be overly strict; the correct check is that desired items match the first N items of actual.

---

#### H4 — `aqueduct diff` command documented but absent from the CLI

**File:** `docs/api-reference.md`, `crates/aqueduct-cli/src/commands/mod.rs`

**Problem:** `docs/api-reference.md` fully documents `aqueduct diff --table <NAME> --to <TARGET>` as a stable command. The GitHub Actions plan action passes `--fail-on-drift` to `aqueduct plan` — a flag that also doesn't exist in the CLI. Neither `commands/mod.rs` nor `main.rs` contains a `Diff` subcommand.

**Impact:** Users following the documented workflow hit "error: unrecognized subcommand 'diff'" and "error: unexpected argument '--fail-on-drift'". The plan CI action fails for any repository that sets `fail-on-drift: true`.

**Recommended fix:** Implement `aqueduct diff` as described, or remove from documentation. Add `--fail-on-drift` to `PlanArgs` as an alias/synonym for `--fail-if-changed`.

---

#### H5 — `aqueduct apply` has no confirmation prompt; `--yes/-y` flag is undocumented no-op

**File:** `crates/aqueduct-cli/src/commands/apply.rs`, `docs/api-reference.md`

**Problem:** `apply.rs` has no interactive confirmation prompt before executing plan steps. The API reference documents `--yes / -y` for skipping confirmation, implying a prompt exists. There is no `--yes` flag in `ApplyArgs`. Users running `aqueduct apply` in an interactive shell will execute destructive changes immediately with no confirmation.

**Impact:** Accidental `aqueduct apply` in a production terminal destroys live stream tables without warning.

**Recommended fix:** Add a confirmation prompt to `apply` (before `PlanExecutor::execute()`) that prints the plan and asks "Apply these changes? [y/N]". Add `--yes/-y` flag to skip. This is table-stakes UX for any database migration tool.

---

#### H6 — Drain-then-pause protocol not implemented; migrations race with live refreshes

**File:** `crates/aqueduct-core/src/executor.rs`

**Problem:** The ROADMAP describes: "Before any migration step, `aqueduct apply` calls `pgtrickle.pause_scheduler(nodes => [...])` then polls for `refresh_status = 'running'` to drain in-flight refreshes." The mock `pgtrickle.pause_scheduler()` function exists but is never called anywhere in `executor.rs`.

**Impact:** Any migration applied while pg_trickle has a node actively refreshing will race with the refresh. The behaviour during concurrent `DROP TABLE` + active refresh is undefined at the pg_trickle level and may leave the stream table in an inconsistent state.

**Recommended fix:** Before the first non-LockDag step, call `pgtrickle.pause_scheduler()` with the list of affected node names. After all steps complete (or on any error), call `pgtrickle.resume_scheduler()` unconditionally.

---

#### H7 — Diamond DAG consistency not implemented; split-class plans corrupt convergence nodes

**File:** `crates/aqueduct-core/src/plan.rs`

**Problem:** The ROADMAP describes: "All nodes in a diamond group are upgraded to the highest migration class of any member." No diamond group detection or class promotion exists in `plan.rs` or `classifier.rs`. Each node in a diamond DAG is classified independently.

**Impact:** A diamond where node A (the join) depends on B and C, and B requires a Rebuild while C needs only a Free change, produces a plan that rebuilds B but only alters C. After the plan executes, B is rebuilt (new table, fresh state) but C still points to the old data, breaking convergence invariants that pg_trickle enforces at the atomic group level.

---

#### H8 — `aqueduct status` drift_count is always 0; `--fail-on-drift` never triggers

**File:** `crates/aqueduct-cli/src/commands/status.rs:86`

**Problem:** In `poll_once()`, the `StatusReport` is constructed with `drift_count: 0` hardcoded. The actual diff between desired and live state is never computed. The `--fail-on-drift` flag therefore never triggers, and the drift display always shows "none detected" regardless of actual state.

**Impact:** `aqueduct status --fail-on-drift` provides a false safety net; it always exits 0. CI pipelines using `aqueduct status --fail-on-drift` to detect out-of-band changes to production will silently pass even when the production DAG has drifted.

**Recommended fix:** Load the migration files, compute `compute_diff(desired, live)`, and count non-Unchanged deltas for `drift_count`.

---

#### H9 — `status --watch` holds a persistent connection indefinitely

**File:** `crates/aqueduct-cli/src/commands/status.rs:175–196`

**Problem:** The watch loop calls `tokio::time::sleep(interval).await` between polls while the `client: tokio_postgres::Client` is held in scope for the entire loop. There is no connection keep-alive, reconnect, or re-acquire between polls. A 30-second polling watch loop running for 8 hours holds one connection open for 8 hours.

**Impact:** Connection pool exhaustion for applications sharing a PgBouncer pool. Unexpected disconnects after PostgreSQL's `idle_in_transaction_session_timeout` or network timeout silently stop producing status updates.

---

#### H10 — CI does not test against multiple PostgreSQL versions

**File:** `.github/workflows/ci.yml`, `.github/workflows/release.yml`

**Problem:** The CI integration test matrix runs only on the default Testcontainers PostgreSQL image (16 at time of writing). `release.yml` hardcodes `postgres:16`. There is no PG 14 / PG 15 / PG 17 test path.

**Impact:** Syntax differences (`pg_stat_activity` column additions, `current_setting()` behaviour, `information_schema` views) may work on PG 16 but fail on PG 14, which is still under active support. The `app.cnpg.cluster_name` GUC detection in `live_state.rs::detect_ha_backend()` uses a custom parameter that behaves differently across versions.

---

### Medium

---

#### M1 — `${secret:BACKEND:KEY}` inline syntax is dead code; documented feature is inactive

**File:** `crates/aqueduct-core/src/secrets.rs:220–240`

`resolve_dsn_secrets()` is defined but never called from `commands/mod.rs::resolve_dsn()`, `config.rs::resolve_env_vars()`, or anywhere else. The v0.6 changelog documents this as a shipped feature. Operators who configure `dsn = "…:${secret:vault:database/prod}@…"` get a connection error, not a secret lookup.

---

#### M2 — `init` installs v1; testkit uses v2 inline SQL — divergence masks catalog bugs

**File:** `crates/aqueduct-testkit/src/lib.rs:75` vs `crates/aqueduct-cli/src/commands/init.rs:30`

`TestDb::install_aqueduct_catalog()` executes inline v2 SQL (with all tables). `aqueduct init` executes `CATALOG_INIT_SQL` (v1, missing 3 tables). Integration tests never exercise a catalog created by the actual `init` command.

---

#### M3 — API reference documents wrong exit codes for `aqueduct plan`

**File:** `docs/api-reference.md:7`

API reference: "Exit codes: `0` empty plan, `1` non-empty plan, `2` error." Actual code: exits `0` for both empty and non-empty plans unless `--fail-if-changed` is explicitly passed. Callers who rely on the documented exit code `1` for non-empty plans will have their CI always pass.

---

#### M4 — `cdc_mode` values inconsistent between API docs and implementation

**File:** `docs/api-reference.md:150` vs `crates/aqueduct-testkit/src/mock_pgtrickle.rs`

API reference documents `cdc_mode` values as `"ROW" | "STATEMENT" | "NONE"`. The mock pg_trickle and test files use `"trigger"` and `"wal"`. There is no enum or validation in the code; any string passes through. The actual pg_trickle values are neither set of strings, creating documentation and implementation divergence.

---

#### M5 — `--output` vs `--format` inconsistency between API docs and code

**File:** `docs/api-reference.md`, `crates/aqueduct-cli/src/commands/plan.rs`

API reference uses `--output <FORMAT>` for `aqueduct plan`. CLI flag is `--format <FORMAT>`. The `status` command also uses `--format` in the code. The API reference also documents `yaml` as a valid format for both commands; only `text/json/markdown` are implemented.

---

#### M6 — `validate` errors lack line-number references

**File:** `crates/aqueduct-core/src/validate.rs`

`validate_migration_files()` and `validate_dag()` produce error messages with file path and error description but no line number. For files with multiple stream tables or large SQL bodies, locating the error requires manual inspection. ESSENCE.md implies line-level feedback. The `sqlparser` error type provides span information that could be surfaced.

---

#### M7 — `aqueduct destroy` missing `--confirm` flag; API reference documents it as required

**File:** `docs/api-reference.md:112`, `crates/aqueduct-cli/src/commands/destroy.rs`

API reference documents `--confirm` as required alongside the absence of `--dry-run`. Code has only `--dry-run`; the `destroy` command has no confirmation guard. Running `aqueduct destroy --to prod` immediately drops all stream tables without confirmation.

---

#### M8 — `--allow-plaintext-password` documented in ESSENCE but never implemented

**File:** `ESSENCE.md`, `crates/aqueduct-core/src/config.rs`

ESSENCE principle 6: "No plaintext passwords in config files unless `--allow-plaintext-password` is explicitly set." `config.rs::resolve_env_vars()` never checks for embedded passwords (pattern `://user:password@`) in DSN strings. Any DSN with an embedded password is accepted silently.

---

#### M9 — `RecordSnapshot` writes empty spec JSON; rollback will always fail even after C1 is fixed

**File:** `crates/aqueduct-core/src/executor.rs` (RecordSnapshot match arm)

```rust
PlanStep::RecordSnapshot { version } => {
    let spec_json = serde_json::json!({});  // ← always empty
    // ...
    self.client.execute(INSERT_DAG_VERSION_SQL, &[..., &spec_json]).await?;
}
```

Even after fixing rollback (C1) to actually use `spec_jsonb`, the spec stored is always `{}`. Rollback to any prior version would restore an empty DAG state, dropping all tables.

---

#### M10 — `aqueduct plan` `--validate-ivm` is default true but IVM checks never run

**File:** `crates/aqueduct-cli/src/commands/plan.rs:42`

The `--validate-ivm` flag defaults to `"true"` but as documented in H2, only calls syntax validation. There is no actual IVM check in the plan pipeline, making the flag's default value misleading noise.

---

#### M11 — No `--quiet` / `--porcelain` mode for scripting

**File:** `crates/aqueduct-cli/src/main.rs`

There is no global `--quiet` flag. Commands like `aqueduct plan` print coloured output and emoji (⚠, ✓) that pollute scripted pipelines. JSON `--format` is available for `plan` and `status` but not for `validate`, `lint`, or `init`.

---

#### M12 — Progress written to `aqueduct.migrations.progress` is always `{}`

**File:** `crates/aqueduct-core/src/executor.rs:70–76`

`FINISH_MIGRATION_SQL` is called with `serde_json::json!({})` as the progress argument. Even ignoring the `--resume` bug (C2), the progress column is never useful because it's always empty. No external monitoring can use it to track migration progress.

---

#### M13 — `aqueduct plan` missing `--project-dir` flag in CI action

**File:** `.github/actions/plan/action.yml:143–147`

The plan action passes `--project-dir "${{ inputs.project-dir }}"` but the `PlanArgs` struct uses `--project-dir` as the flag name (matching). However, the `--to <TARGET>` flag in the action maps to `--to` in `ApplyArgs` (which is correct). This appears consistent, but the plan action also references `--fail-on-drift` (line 136) which does not exist in `PlanArgs` — the action will silently pass invalid flags depending on shell quoting.

---

#### M14 — CI plan action uses `--fail-on-drift` flag that doesn't exist in the CLI

**File:** `.github/actions/plan/action.yml:133–135`

```yaml
if [ "${{ inputs.fail-on-drift }}" = "true" ]; then
  EXTRA_FLAGS="$EXTRA_FLAGS --fail-on-drift"
fi
```

`PlanArgs` has no `--fail-on-drift` argument. Any CI pipeline that sets `fail-on-drift: true` in the plan action will pass this unknown flag to `aqueduct plan`, causing `error: unexpected argument '--fail-on-drift'`.

---

#### M15 — `aqueduct status` doesn't expose pg_version

**File:** `crates/aqueduct-cli/src/commands/status.rs`

`poll_once()` queries `current_setting('server_version')` and stores it in `pg_version`, but `StatusReport` has a `pg_version: Option<String>` field and `render_status_text()` does output it. The JSON branch at line 95 does not include `pg_version` in the JSON output object — only `project/version/stream_tables/drift/pgtrickle_version/polled_at`. This is a minor inconsistency but breaks JSON consumers expecting parity with text output.

---

#### M16 — `aqueduct status --watch` holds a single connection; no reconnect logic

Already described in H9, but for medium-level documentation: the watch mode needs reconnect-on-failure logic with exponential backoff. A network blip silently kills the watch loop.

---

### Low

---

#### L1 — `regex::Regex::new(...).unwrap()` in hot paths (should use `LazyLock`)

**File:** `crates/aqueduct-core/src/config.rs:124`, `crates/aqueduct-core/src/secrets.rs:220`

Static regex patterns compiled inside `resolve_env_vars()` and `resolve_dsn_secrets()` re-compile the regex on every call. These should use `std::sync::LazyLock<Regex>` (Rust 1.80+) or `once_cell::sync::Lazy`. The `.unwrap()` on `Regex::new` is also acceptable only because the pattern is a literal — but an explicit `expect("valid static regex")` is clearer.

---

#### L2 — `fmt.rs::read_original_content()` silently returns empty string on IO error

**File:** `crates/aqueduct-core/src/fmt.rs:106`

```rust
fn read_original_content(file: &MigrationFile) -> String {
    std::fs::read_to_string(&file.path).unwrap_or_default()  // ← swallows IO errors
}
```
If the file has been deleted or is unreadable between parse and format, `unwrap_or_default()` returns `""`, causing `format_migration()` to always return `Some(formatted_content)` and unconditionally overwrite the file.

---

#### L3 — `CANONICAL_KEY_ORDER` constant in `fmt.rs` is defined but never used for ordering

**File:** `crates/aqueduct-core/src/fmt.rs:12–23`

`CANONICAL_KEY_ORDER` is defined as a `const` but the `render_migration()` function uses a series of explicit `if let Some(...) { lines.push(...) }` conditionals in the same order. The constant is referenced only in a `CANONICAL_KEY_ORDER.contains(&key.as_str())` check to skip already-emitted unknown keys. The canonical ordering is maintained by code structure, not data, so the constant is misleadingly named.

---

#### L4 — Testkit `aqueduct_catalog_sql()` duplicates `CATALOG_INIT_V2_SQL` verbatim

**File:** `crates/aqueduct-testkit/src/lib.rs:75`

The testkit contains a hardcoded inline copy of the v2 catalog SQL. When `catalog.rs` is updated (schema migration to v3 etc.), the testkit copy must be updated manually. This will inevitably diverge. The testkit should use `aqueduct_core::catalog::CATALOG_INIT_V2_SQL` directly.

---

#### L5 — No property-based tests (proptest/quickcheck)

**File:** `Cargo.toml`, test suite

The assessment prompt identifies several properties suitable for property-based testing: plan idempotency (apply twice = empty plan), rollback inverse property, `classify(a→b)` + `classify(b→a)` symmetry. No `proptest` or `quickcheck` dependency exists. The planner fuzzing test is a single deterministic LCG harness, not a true PBT.

---

#### L6 — No tests for maintenance window enforcement

**File:** `crates/aqueduct-core/tests/integration.rs`

There are no tests verifying that Rebuild-class steps are blocked outside the maintenance window, that `--ignore-maintenance-window` overrides correctly, or that `maintenance_window_applies_to` is respected.

---

#### L7 — Log format forced to JSON when `GITHUB_ACTIONS=false` (string "false" is truthy)

**File:** `crates/aqueduct-cli/src/main.rs:75`

```rust
let is_ci = std::env::var("CI").is_ok()
    || std::env::var("GITHUB_ACTIONS").is_ok()  // ← .is_ok() = key exists, any value
```

`std::env::var("GITHUB_ACTIONS").is_ok()` returns `true` when the variable is set to any value, including `"false"` or `"0"`. If a developer sets `GITHUB_ACTIONS=false` to disable something else in their shell, aqueduct silently switches to JSON log format.

---

#### L8 — Release binary archive names don't include architecture suffix consistently

**File:** `.github/workflows/release.yml:71`

The download URL pattern is `aqueduct-${OS}-${ARCH}` (e.g., `aqueduct-linux-x86_64`) but the ARCH mapping uses `x86_64` (from `uname -m`) for one case and `aarch64` for another. The macOS runner uses `macos-15` which is ARM (Apple Silicon) but `uname -m` returns `arm64` not `aarch64`. The ARCH normalisation maps `arm64 → aarch64` which is correct, but the artifact suffix matrix uses `macos-arm64` — these need to be kept in sync.

---

#### L9 — No `cargo audit` in CI

**File:** `.github/workflows/ci.yml`

CI has lint, unit tests, integration tests, and coverage but no `cargo audit` step. Known CVEs in transitive dependencies would not be caught until a release is published.

---

#### L10 — `tracing_subscriber` initialized in `main()` without `#[allow(unused_must_use)]`; `filter.init()` may fail silently

**File:** `crates/aqueduct-cli/src/main.rs:83–89`

`tracing_subscriber::fmt()...init()` returns `()` and panics internally if called twice (e.g., in tests or when `RUST_LOG` is malformed). The `EnvFilter::try_new().unwrap_or_else(|_| EnvFilter::new("info"))` is correct, but the tracing subscriber is initialised unconditionally in `main()`, which will cause problems if `main` is ever called from tests.

---

#### L11 — Testcontainer images not pinned to a specific version

**File:** `crates/aqueduct-testkit/src/lib.rs:20`

`Postgres::default()` uses the `latest` tag. If the image is updated to a new major PostgreSQL version between CI runs, tests can start failing for reasons unrelated to code changes.

---

#### L12 — `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` is a non-standard workaround

**File:** `.github/workflows/release.yml:8`

This environment variable is not documented by GitHub and suggests a workaround for an internal tooling issue. It should be replaced with proper `@v4` (or newer) action pins that natively support Node 20/24.

---

#### L13 — `aqueduct plan` `--format` and `--fail-if-changed` flags missing from `aqueduct --help` summary

The CLI's top-level `--help` output shows all subcommands but does not surface key flags. Users who run `aqueduct plan --help` do see the flags, but discoverable short flags like `-f` for format are missing.

---

#### L14 — Testcontainers connection string exposed at `TestDb.connection_string` field

**File:** `crates/aqueduct-testkit/src/lib.rs:18`

`TestDb.connection_string` is a `pub` field containing the full connection string including password. In the current test environment (password is `postgres`) this is low risk. However, if the testkit is ever used with real secrets, the connection string field should be considered sensitive.

---

## Findings by Area

### 2.1 — Correctness & Logic Bugs

**Critical:** Rollback non-functional (C1). `--resume` no-op (C2). Panickable unwraps (C3). AlterStreamTable ignores new_query (H1).

**High:** Column-removal false InPlace (H3). Diamond DAG not implemented (H7). Drain-pause not implemented (H6). `RecordSnapshot` stores empty spec (M9).

**Key finding:** The classifier (`classifier.rs`) is architecturally sound for schedule/mode changes but the `SelectListDelta::Removal` InPlace classification is incorrect for non-suffix removals. The topological sort (Kahn's algorithm in `dag.rs`) is correctly implemented and handles cycles.

The `compute_diff()` function itself is correct — it handles creates, drops, and all alter variants. The `classify_delta()` decision tree matches the roadmap table with one exception: the `Removal` case.

### 2.2 — Security Vulnerabilities

**Critical:** SQL injection in executor fallback (C4). Secret key passed to subprocesses without validation.

**High:** `${secret:BACKEND:KEY}` dead code means documented secret injection does not work (M1 — paradoxically making it not a security risk today, but a reliability risk).

In `secrets.rs`, SOPS and age subprocess arguments pass the `key` string (a file path) directly from config to `std::process::Command::new("sops").args(["-d", key])`. A crafted key like `--exec-env` or a path containing shell metacharacters could affect subprocess behaviour when running under certain shells. Since `std::process::Command` does not use a shell by default (it uses `execve` directly), shell injection is not possible — but path traversal is. A key of `../../etc/passwd` would attempt to decrypt that file.

The `--allow-plaintext-password` guard is entirely absent (M8), violating ESSENCE principle 6.

Connection strings do not appear in tracing logs (checked all `tracing::info!` and `tracing::debug!` calls). This is correct.

### 2.3 — Error Handling & Resilience

**Critical:** Panickable unwraps in `plan.rs` (C3).

**Medium:** `fmt.rs::read_original_content()` swallows IO errors (L2).

All `AqueductError` variants appear to be used. Error messages generally follow the three-part pattern (what/why/how), with good examples like:
```
"Environment variable '{}' not set. Ensure the secret is exported before running aqueduct."
```

Weaker messages include `AqueductError::Other(String)` which is used in several places without structured context.

Transient network errors: `tokio_postgres::Error` is wrapped via `#[from]` in `AqueductError::Database`. No retry logic exists anywhere in the codebase for transient failures.

`tokio::select!` is not used anywhere — so the cancellation-safety question does not apply.

### 2.4 — Test Coverage & Quality

The test suite has good structural coverage of the happy path but is weak on:

- **`--resume` behaviour** — no test because it's a no-op
- **Rollback with spec restoration** — no test because it's broken
- **Maintenance window enforcement** — zero tests (L6)
- **`--allow-full-refresh = false` enforcement** — zero tests
- **Secret backend failures** — zero tests
- **HA backend detection** — no test for Patroni/Stolon/CNPG detection branches
- **Concurrent lock contention** — one test (`test_lock_prevents_concurrent_apply`) but it uses manual INSERT, not two concurrent executor instances
- **Drift detection in status** — no test because drift_count is hardcoded 0
- **Column removal InPlace classification** — no test for the incorrect case
- **`AlterStreamTable` with `new_query`** — no test exposing that it's ignored
- **CLI output format regression** — no snapshot tests for text/JSON/markdown rendering

Integration tests do verify database state (table existence, count) but not column structure, schedule values, or refresh_mode after mutations.

The 30 cookbook patterns are not implemented as integration tests despite the CHANGELOG claiming "All 30 new cookbook integration tests pass." The `integration.rs` file covers approximately 15 focused scenarios, not 30 cookbook patterns.

### 2.5 — Performance & Scalability

The DAG topological sort is O(V + E) using Kahn's algorithm — correct and efficient for 500+ nodes.

`compute_diff()` is O(V²) due to nested linear searches (`find_stream_table()` iterates the entire `Vec<StreamTableSpec>`). For 500+ nodes this becomes 250K comparisons but is memory-bounded and not a practical concern until ~10K nodes.

`read_live_state()` issues a single batch query against `pgtrickle.pgt_stream_tables` — no N+1 problem.

`status --watch` holds a connection (H9). The `estimate_rows()` function in `cost.rs` executes `EXPLAIN (FORMAT JSON)` without a `statement_timeout` guard — a pathological query plan could block indefinitely.

The `rewrite_query_for_preview()` function in `preview.rs` uses naive `String::replace()` for schema rewrites, which can incorrectly rewrite schema names that appear as substrings of other identifiers. For example, if a schema is `raw` and a column is named `raw_count`, the column name would be rewritten.

### 2.6 — Code Quality & Maintainability

Module boundaries are clean. The dependency direction is strictly `executor.rs → catalog.rs`, `plan.rs → classifier.rs`, `plan.rs → diff.rs`, `diff.rs → dag.rs`. No circular imports.

Large function alert: `build_plan()` in `plan.rs` is ~250 lines. `PlanExecutor::run_steps()` in `executor.rs` is ~220 lines. Both are match-heavy and would benefit from step-handler extraction.

`diff.rs::normalise_sql()` compiles a `Regex::new(r"\s+").unwrap()` on every call — minor performance issue, should be a `LazyLock`.

`PlanStep` enum arms are unbalanced: `LockDag` and `UnlockDag` are trivially simple (1-2 lines each) while `CreateStreamTable` and `AlterStreamTable` require 30+ line match arms.

The `ManageConsumerView` step's `action` field is a `String` (`"create"`, `"alter"`, `"drop"`) rather than an enum. This is stringly-typed and not validated.

`unknown_keys` appears in both `FrontMatter` (as `HashMap<String, serde_json::Value>`) and `MigrationFile` (as `Vec<String>`), storing the same information twice in different forms.

### 2.7 — CLI Ergonomics & User Experience

**Critical gaps:**
- No confirmation prompt before destructive apply (H5)
- `aqueduct diff` documented but missing (H4)
- `--fail-on-drift` documented and used in CI action but missing (M14)

**Inconsistencies:**
- `--output` (docs) vs `--format` (code) for plan/status
- `--yes/-y` documented but absent
- `--confirm` for destroy documented but absent
- Exit code for non-empty plan differs from docs

**Positive:** `--dry-run` is consistently implemented across `apply`, `promote`, `destroy`, and `preview`. Log format auto-detection for CI (`$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`) is a nice touch.

### 2.8 — Documentation & Completeness

The `docs/api-reference.md` has several inconsistencies with the implementation:

| Documented | Actual |
|-----------|--------|
| `--output text\|json\|yaml` | `--format text\|json\|markdown` |
| `--yes / -y` for apply | Not implemented |
| `--confirm` for destroy | Not implemented |
| `--strict` for validate | Not implemented |
| `aqueduct diff` subcommand | Not implemented |
| Exit 1 for non-empty plan | Only with `--fail-if-changed` |
| `cdc_mode: ROW\|STATEMENT\|NONE` | Internally `trigger\|wal` |

The 30 cookbook recipes in `docs/cookbook/` are well-written. A spot check of cookbook entries 01, 05, 10, 20, 26:
- **01** (change schedule): Correct, maps to Free class
- **05** (add SUM aggregate): Correct, maps to InPlace
- **10** (rename column): Correct, maps to Rebuild
- **20** (two-node DAG): Correct, multi-level topology
- **26** (source cascade): The cascade analysis in `compute_source_deltas()` only fires for `owned = true` sources, but cookbook 26 appears to use `owned = false` (the common case). The cascade would be silently skipped.

The `aqueduct-testkit` crate has no documentation. The security guide (`docs/security.md`) accurately describes the secret backends but does not note that the `${secret:BACKEND:KEY}` inline syntax is currently dead code.

CHANGELOG claims "v0.7.0 — 197 tests total: 101 unit + 63 integration + 33 CLI. All 30 cookbook integration tests pass." The actual test suite has approximately 30 integration tests and 15 CLI tests, not the claimed 197 total.

### 2.9 — CI/CD & Release Readiness

CI has a solid structure: lint → unit → integration → CLI → coverage. The release workflow correctly:
- Runs the full test suite before building artifacts
- Builds for 4 platforms (linux-amd64, linux-arm64, macos-arm64, windows-amd64)
- Extracts changelog entry and creates a GitHub release

**Gaps:**
- No `cargo audit` for CVE scanning (L9)
- No PostgreSQL version matrix (H10)
- Coverage is measured only for unit tests (`--lib`), not integration tests
- No code coverage threshold — codecov upload uses `fail_ci_if_error: false`
- The `coverage` job uses `cargo tarpaulin --ignore-tests` which excludes the most valuable test categories

### 2.10 — Dependency & Supply Chain

Dependencies are at reasonable versions:
- `tokio 1.x` — current major
- `tokio-postgres 0.7` — current
- `sqlparser 0.54` — recent
- `clap 4` — current
- `serde 1` — current

No `unsafe` blocks in the codebase (confirmed by search).

`deadpool-postgres = "0.14"` is imported in `Cargo.toml` but not used anywhere in the source — it's a dead dependency.

`tokio-postgres` connections are NOT pooled (each command opens a direct connection via `tokio_postgres::connect`). This is appropriate for a CLI tool that runs once. No connection leakage identified in non-error paths; error paths rely on the connection being dropped when the Client falls out of scope, which is correct for tokio-postgres.

### 2.11 — Missing Features & Roadmap Gaps

Key missing features confirmed absent from code:

| Feature | Roadmap Status | Implementation Status |
|---------|---------------|----------------------|
| `--resume` step progress | v0.1 [x] | ❌ No-op |
| Rollback via prior spec | v0.1 [x] | ❌ Re-applies current files |
| Secret backend inline syntax | v0.6 [x] | ❌ Dead code |
| Diamond DAG consistency | v0.2 | ❌ Not started |
| Drain-then-pause before apply | v0.1 | ❌ Not called |
| Lock heartbeat | v0.1 | ❌ Not implemented |
| Catalog self-migration | v0.1 [x] | ❌ Not implemented |
| Drift detection in `status` | v0.1 [x] | ❌ Hardcoded 0 |
| `aqueduct diff` command | v0.4 | ❌ Not implemented |
| Plan approval gate | future | ❌ Not in scope yet |
| `aqueduct gc` command | future | ❌ Not in scope yet |
| `--target-version` for apply | future | ❌ Not in scope yet |
| Backfill progress percentage | future | ❌ Not in scope yet |
| Cross-schema DAG support | future | ❓ QualifiedName supports schemas |

---

## Findings by Area — Cross-Cutting Reviews

### 3.1 — Consistency Audit

**Terminology inconsistencies:**

| Code | Documentation | ROADMAP |
|------|--------------|---------|
| `--fail-if-changed` | `--fail-if-changed` | both |
| `--format json` | `--output json` | `--format json` |
| `MigrationClass::InPlace` | "in-place" | "In-place" |
| `cdc_mode: "trigger"\|"wal"` | `"ROW"\|"STATEMENT"\|"NONE"` | `"trigger"\|"wal"` |

**PlanStep enum vs ROADMAP table:** The ROADMAP describes these plan step types as part of the v0.2 table:
- `RecreatePolicy` — **not in PlanStep enum**
- `DetachOutbox` — **not in PlanStep enum**
- `ReattachOutbox` — **not in PlanStep enum**
- `ManageWalSlot` — **not in PlanStep enum**
- `PauseImmediate` — **not in PlanStep enum**
- `ResumeImmediate` — **not in PlanStep enum**
- `WaitForRefresh` — **not in PlanStep enum**
- `RunHook` — **not in PlanStep enum**

Eight plan step variants described in the roadmap are entirely absent from the implementation.

**Front-matter directives:** `docs/api-reference.md` documents the `schema` directive. Code implements it. ✓
`cypher_source` is in the code but absent from the API reference directive table. ✗

### 3.2 — Design Principle Adherence

1. **Plan before apply:** ✓ Correct — `execute()` always calls `START_MIGRATION_SQL` first.
2. **No implicit state:** ✓ Correct — no local state files used.
3. **Conservative classification:** ⚠ Mostly correct, but the `Removal` case in the classifier (H3) can produce false InPlace for mid-column drops.
4. **Crash safety:** ✗ Violated — `--resume` is a no-op (C2); lock TTL expires (C5).
5. **Least privilege:** Cannot verify — no `aqueduct_admin` role SQL is in the codebase; the security guide describes it but there are no `CREATE ROLE` statements to audit. ⚠

### 3.3 — Integration Surface Verification

**dbt-roundtrip example (`examples/dbt-roundtrip/`):** The README and directory structure are present. `ingest.rs` correctly reads `manifest.json`, filters `stream_table` materialisation type, and generates migration files. The `derive_depends_on()` function correctly handles explicit `+depends_on` config overrides and falls back to source node references. This integration appears functional.

**GitHub Actions plan action:** The action downloads a binary, runs `aqueduct plan`, posts a PR comment. The plan output detection uses `grep -q "No changes"` which is fragile (depends on renderer string "No changes detected. Plan is a no-op."). **Critical gap:** passes `--fail-on-drift` which doesn't exist (M14). The DSN masking (`echo "::add-mask::${AQUEDUCT_DSN}"`) is missing from the plan action (present in apply, absent in plan).

**GitHub Actions apply action:** Correctly masks the DSN. Attempts to parse `migration_id` from JSON output, but the CLI's JSON log output doesn't emit a `migration_id` field — the `apply.rs` command prints `"✓ Applied successfully. New version: v{}"` as plain text, not structured JSON. The `migration_id`, `from_version`, and `to_version` outputs will always be empty strings.

**GitLab CI template (`ci/gitlab/aqueduct.gitlab-ci.yml`):** Not read in this assessment. Present but not verified.

**Pre-commit hooks (`ci/hooks/aqueduct-pre-commit`):** Not read in this assessment. The hook should run `aqueduct validate` on changed `.sql` files.

---

## Prioritised Improvement Roadmap

### Milestone A — Critical Fixes (1–2 weeks)

These issues must be resolved before the tool can be trusted in production.

1. **Fix `aqueduct init` to use `CATALOG_INIT_V2_SQL`** — 1 line change (C6)
2. **Implement step progress tracking and `--resume`** — write index to `progress` JSONB after each step; read on resume (C2)
3. **Implement lock heartbeat** — background tokio task updating `acquired_at` every `ttl/3` (C5)
4. **Fix `RecordSnapshot` to store actual `DagState`** — serialize and write the full desired state to `spec_jsonb` (M9)
5. **Fix `aqueduct rollback` to restore from `spec_jsonb`** — deserialize prior spec, use as desired state (C1)
6. **Replace `AlterStreamTable` executor to pass `new_query`** — add query parameter to `alter_stream_table()` call (H1)
7. **Remove panickable `.unwrap()` from `plan.rs`** — replace with `?` and proper error variants (C3)
8. **Restrict SQL injection path** — remove the fallback `format!()` SQL in executor or validate that it's test-only (C4)

### Milestone B — Test Coverage and Hardening (2–3 weeks)

9. **Fix `--validate-ivm` to call `validate_ivm_supportability()`** (H2)
10. **Fix column-removal InPlace classifier to require suffix-only drops** (H3)
11. **Fix `status drift_count`** — compute actual diff in `poll_once()` (H8)
12. **Implement `--confirm` flag for `aqueduct destroy`** (M7)
13. **Add confirmation prompt to `aqueduct apply`** with `--yes/-y` bypass (H5)
14. **Implement `--allow-plaintext-password` guard** in `config.rs` (M8)
15. **Add `--fail-on-drift` flag to `aqueduct plan`** (M14 / H4)
16. **Write tests for** maintenance window, allow_full_refresh, drift detection, column-removal classifier, AlterStreamTable query update, lock heartbeat expiry, resume behaviour
17. **Add PostgreSQL version matrix** to CI (PG 14, 15, 16, 17) (H10)

### Milestone C — Feature Gaps and Ergonomics (2–4 weeks)

18. **Implement `aqueduct diff` command** (H4)
19. **Implement diamond DAG consistency promotion** (H7)
20. **Implement drain-then-pause protocol** (H6)
21. **Activate `${secret:BACKEND:KEY}` inline DSN syntax** — wire `resolve_dsn_secrets()` into connection resolution (M1)
22. **Implement catalog self-migration** — check version on every connect, apply pending migrations (C6 follow-on)
23. **Fix API reference** — `--output → --format`, correct exit codes, correct cdc_mode values, add `cypher_source` to directive table
24. **Add `--quiet` / `--porcelain` global flag** (M11)
25. **Fix `status --watch` connection handling** — reconnect between polls (H9)
26. **Implement missing PlanStep variants** — `RecreatePolicy`, `ManageWalSlot`, `WaitForRefresh`, `RunHook`, `DetachOutbox`, `ReattachOutbox` (3.1)

### Milestone D — Performance, Supply Chain, and Release Readiness (1–2 weeks)

27. **Add `cargo audit` to CI** (L9)
28. **Pin Testcontainers image version** (L11)
29. **Move static regex to `LazyLock`** (L1)
30. **Remove unused `deadpool-postgres` dependency**
31. **Implement proper coverage gate** (currently no threshold; `fail_ci_if_error: false`)
32. **Fix plan action DSN masking** — add `echo "::add-mask::..."` before using DSN in plan action
33. **Fix apply action `migration_id` extraction** — emit structured JSON from `apply` command

---

## Appendix: Checklist of All Roadmap Items

### v0.1 / Phase 0–2

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| Cargo workspace with 3 crates | [x] | ✓ |
| `just build/test/lint/fmt` | [x] | ✓ |
| CI matrix on Linux + macOS | [x] | ✓ |
| Code-coverage gate | [x] | ✓ (but no threshold) |
| `aqueduct init` — creates catalog | [x] | ⚠ Creates v1, not v2 |
| `aqueduct init` — schema name collision check | [x] | ✓ |
| TOML project loader | [x] | ✓ |
| Migrations-folder parser | [x] | ✓ |
| Live-state reader | [x] | ✓ (pgtrickle only, no pg_attribute) |
| DAG differ | [x] | ✓ |
| Query pre-validation (parse check) | [x] | ✓ |
| IVM-supportability check at plan time | [x] | ❌ Syntax only, not IVM |
| Plan classification | [x] | ✓ (with H3 caveat) |
| Human-readable plan renderer (text/json/md) | [x] | ✓ |
| `aqueduct plan` | [x] | ✓ |
| `aqueduct status` | [x] | ⚠ drift_count always 0 |
| `aqueduct validate` | [x] | ✓ |
| Plan executor | [x] | ⚠ see H1, C2, C3, C5 |
| Lock manager | [x] | ⚠ no heartbeat (C5) |
| `aqueduct apply` | [x] | ⚠ no confirmation prompt |
| `aqueduct apply --dry-run` | [x] | ✓ |
| `aqueduct apply --resume` | [x] | ❌ No-op (C2) |
| `aqueduct rollback` | [x] | ❌ Non-functional (C1) |
| `aqueduct import` | [x] | ✓ |
| `aqueduct unlock` | [x] | ✓ |
| Plan format versioning | [x] | ✓ |
| Structured JSON logs | [x] | ✓ |
| HA awareness (pg_is_in_recovery) | [x] | ✓ |
| `allow_full_refresh = false` enforcement | [x] | ✓ |
| pg_trickle version compatibility check | [x] | ✓ |
| `examples/minimal/` | [x] | ✓ |

### v0.2 — Online Schema Evolution

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| Full classifier (in-place, rebuild) | [x] | ⚠ H3 caveat |
| `RecreatePolicy` plan step | (roadmap table) | ❌ Not in PlanStep enum |
| `DetachOutbox/ReattachOutbox` plan steps | (roadmap table) | ❌ Not in PlanStep enum |
| `ManageWalSlot` plan step | (roadmap table) | ❌ Not in PlanStep enum |
| `PauseImmediate/ResumeImmediate` plan steps | (roadmap table) | ❌ Not in PlanStep enum |
| `WaitForRefresh` plan step | (roadmap table) | ❌ Not in PlanStep enum |
| `RunHook` plan step | (roadmap table) | ❌ Not in PlanStep enum |
| Drain-then-pause protocol | (roadmap body) | ❌ Not called |
| Lock heartbeat | (roadmap body) | ❌ Not implemented |
| Diamond DAG consistency | (roadmap body) | ❌ Not implemented |
| `--resume` reads progress | (roadmap body) | ❌ No-op |

### v0.3 — Blue/Green, Preview, Extension

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| `CreateGreenSchema` step | [x] | ✓ (in PlanStep enum + executor) |
| `CreateStreamTableInGreen` step | [x] | ✓ |
| `WaitForConvergence` step | [x] | ✓ (enum + executor shell) |
| `SwapConsumerViews` step | [x] | ✓ (enum + executor shell) |
| `RetireBlueSchema` step | [x] | ✓ |
| `ManageConsumerView` step | [x] | ✓ |
| Consumer view registry (catalog) | [x] | ⚠ Table absent after v1 init (C6) |
| Blue/green deployment tracking | [x] | ⚠ Table absent after v1 init (C6) |
| `aqueduct preview` | [x] | ✓ (native backend) |

### v0.4 — CI Integrations

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| GitHub Actions plan action | [x] | ⚠ `--fail-on-drift` bug (M14) |
| GitHub Actions apply action | [x] | ⚠ `migration_id` output empty |
| GitLab CI template | [x] | ✓ (not verified in detail) |
| Pre-commit hooks | [x] | ✓ (not verified in detail) |

### v0.5 — dbt Interop

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| `aqueduct ingest --from dbt-target` | [x] | ✓ |
| `examples/dbt-roundtrip/` | [x] | ✓ |

### v0.6 — Production Hardening

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| `aqueduct promote` | [x] | ✓ |
| `${secret:BACKEND:KEY}` inline syntax | [x] | ❌ Dead code (M1) |
| `aqueduct status --watch` | [x] | ✓ (with H9 caveat) |
| HA backend detection (Patroni/CNPG/Stolon) | [x] | ✓ |
| Planner fuzzing harness | [x] | ✓ |
| `aqueduct destroy` | [x] | ⚠ No `--confirm` flag (M7) |

### v0.7 — Documentation & Cookbook

| Item | Claimed | Actually Implemented |
|------|---------|----------------------|
| 30 cookbook patterns | [x] | ✓ (docs exist) |
| 30 cookbook integration tests | [x] | ❌ ~15 tests exist, not 30 |
| 197 tests total (101+63+33) | [x] | ❌ Approximately 60–70 tests |
| API reference | [x] | ⚠ Multiple inconsistencies |
| Security guide | [x] | ⚠ Inline secret syntax described as working |
| HA operations guide | [x] | ✓ |
| Benchmark results | [x] | ✓ (`benchmarks/`) |
