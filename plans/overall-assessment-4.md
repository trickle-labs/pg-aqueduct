# pg-aqueduct — Overall Engineering Assessment (Phase 4)

**Date:** 2026-05-20
**Codebase version:** 0.17.0
**Assessor:** Principal Engineer Review (automated deep-analysis)
**Scope:** All source files in `crates/`, all CI/docs/examples
**Methodology:** Direct source reading + static analysis + prior-assessment reconciliation
**Verification:** `cargo audit` ran clean (0 vulnerabilities, 378 deps); all subagent audits
cross-checked against primary source reads

---

## Executive Summary

### Overall Project Health: 7.5 / 10

pg-aqueduct 0.17.0 represents a significant maturation from the state described in Phase 1
(4/10) and Phase 3 (6.5/10). The core plan→apply→rollback→resume pipeline is now implemented
correctly for the common case. The executor heartbeat, RLS policy capture/restore, consumer
view catalog cleanup, status drift counting, producer IVM validation, mid-column drop
classification, and diamond DAG consistency have all been addressed. The test suite is
comprehensive for happy-path cookbook patterns (30 patterns, 275+ tests across three test
categories). `cargo audit` passes with no CVEs.

However, **three new critical gaps were found**, **four Phase 3 items are not yet fully
closed**, and several documentation and CI items remain as risk multipliers.

### Top 5 Critical Issues

1. **SEC-1 — Consumer view SQL is never validated at apply time.** `validate_consumer_sql_is_single_select()` exists and is unit-tested but is never called in the executor's `ManageConsumerView` arm. An attacker or supply-chain compromise of a migration file can inject arbitrary SQL (including DDL) into the database via a consumer view body.
2. **ARCH-1 — Catalog schema override is parsed but silently ignored.** `project.catalog_schema` and `--schema` are documented and parseable but all catalog SQL hardcodes `aqueduct`. Operators relying on multi-tenant schema isolation have no actual isolation.
3. **SEC-2 — CNPG preview client disables all TLS certificate validation.** `preview.rs:518` uses `danger_accept_invalid_certs(true)` unconditionally, allowing MITM against the Kubernetes API server.
4. **SEC-3 — Age identity file is not path-validated.** `validate_secret_path()` is applied to the `key` argument but not to `identity_file` from `$AGE_KEY_FILE` / `$SOPS_AGE_KEY_FILE`, enabling path traversal and flag-injection attacks.
5. **DOC-1 — Version drift in user-facing integration templates.** Six files consumed by users — the GitLab CI template, the pre-commit hook example, and the two workflow example files — reference `v0.4.0` or `v0.16.0`, which will cause 404 download failures or misleading docs for any user who follows the shipped examples.

### Top 5 Quick Wins

1. **Invoke `validate_consumer_sql_is_single_select()` before executing consumer view SQL in the executor** (`crates/aqueduct-core/src/executor.rs`, ManageConsumerView "create"/"alter" arm). ~30 min; closes SEC-1.
2. **Add `validate_secret_path(identity_file)` call in the Age backend** (`secrets.rs:134`). ~15 min; closes SEC-3.
3. **Update all stale version references** (`docs/api-reference.md`, `docs/introduction.md`, `ci/gitlab/aqueduct.gitlab-ci.yml`, `.pre-commit-hooks.yaml`, `.github/workflows/aqueduct-plan.yml`, `.github/workflows/aqueduct-apply.yml`, `README.md`). ~30 min; closes DOC-1 entirely.
4. **Guard CNPG TLS behind `CNPG_INSECURE_SKIP_VERIFY`** (`preview.rs:518`). ~30 min; closes SEC-2.
5. **Extend version-lint CI to cover `docs/api-reference.md`, `docs/introduction.md`, and example workflow files**. ~30 min; prevents DOC-1 from recurring.

### Summary Statistics

| Severity | Count |
|---|---|
| Critical | 3 |
| High | 7 |
| Medium | 9 |
| Low | 6 |
| **Total** | **25** |

### Comparison with Prior Assessments

| Phase | Health Score | Total Findings | Critical |
|---|---|---|---|
| Phase 1 (v0.7) | 4/10 | 46 | 6 |
| Phase 3 (v0.13) | 6.5/10 | 30 | 3 |
| **Phase 4 (v0.17)** | **7.5/10** | **25** | **3** |

---

## Status of All Prior Phase 3 Findings

| Phase 3 ID | Finding | Phase 4 Status | Evidence |
|---|---|---|---|
| CORR-1 | Failed migrations erase resume checkpoints | **Fixed** | `FINISH_MIGRATION_RECOVERABLE_SQL` preserves progress; progress write is fatal (`executor.rs:1421 .await?`) — intentional per comment "S-06" |
| CORR-2 | Plan execution not transactionally bounded | **Partially Fixed** | Compensating steps recorded in `ddl_log`; saga-style recovery exists; but DDL and catalog writes are still independent auto-commit statements (no full transaction boundary) |
| CORR-3 | Heartbeat loss not propagated | **Partially Fixed** | `lock_lost_tx.send(true)` on `Ok(0)` and on `Err(e)` logs; checked before each step; but heartbeat task has no panic handler — a tokio runtime panic would silently stop the heartbeat without signalling the main loop |
| CORR-4 | Promotion ignores project ownership | **Fixed** | `read_live_state(client, Some(&options.project))` in `promote.rs:55` |
| CORR-5 | Promotion bypasses executor safety context | **Fixed** | `connect_and_migrate`, `.with_desired_state()`, `.with_connection_string()` all called in `promote.rs:63-106` |
| CORR-6 | RLS policy restoration placeholder | **Fixed** | `capture_rls_policies()` queries `pg_policies`/`pg_class.relrowsecurity` before drop; `RecreatePolicy` executor arm uses cache |
| CORR-7 | Consumer view drops leave stale catalog rows | **Fixed** | `DELETE_CONSUMER_VIEW_SQL` called in drop arm (`executor.rs:1017`) |
| CORR-8 | Status drift ignores source/consumer deltas | **Fixed** | All three collections counted in `status.rs:87-99` |
| ARCH-1 | Catalog schema override ignored | **Open** | `catalog_schema` parsed in config, `--schema` flag in InitArgs, but `init.rs` calls `CATALOG_INIT_V4_SQL` without substitution; all catalog SQL hardcodes `aqueduct` |
| ARCH-2 | Executor correctness depends on optional builder | **Partially Fixed** | `for_apply`, `for_rollback`, `for_promote` typed constructors exist; desired_state is still optional and falls back to `{}` spec when omitted |
| ARCH-3 | Blue/green swap not atomic | **Partially Fixed** | View swaps wrapped in `BEGIN`/`COMMIT`; `SWAP_BLUE_GREEN_SQL` status update still outside transaction (`executor.rs:985-996`) |
| ERG-1 | Global --quiet/--porcelain does not suppress output | **Partially Fixed** | `OutputEmitter` constructed, `commands::set_quiet()` wired; but `let _emitter = ...` is discarded and most commands still use direct `println!` |
| ERG-2 | destroy bypasses main error/exit-code path | **Fixed** | `anyhow::bail!` used (`destroy.rs:53`) |
| ERG-3 | YAML output hand-written and unsafe | **Fixed** | `serde_yaml` used in `plan.rs:228`, `status.rs:152`, `diff.rs:134` |
| ERG-4 | API reference documents non-existent flags | **Partially Fixed** | Most stale flags removed; `docs/api-reference.md` still shows v0.16.0 header |
| TEST-1 | Resume tests don't cover real executor failure path | **Fixed** | `test_v014_resume_preserves_progress_after_executor_error` covers executor failure; progress is now fatal, so any test that causes the DB update to fail validates the semantics |
| TEST-2 | Binary CLI coverage shallow | **Partially Fixed** | 45+ binary CLI tests; some output modes and exit codes still untested |
| TEST-3 | Mock scheduler has no observable state | **Open** | `pause_scheduler`/`resume_scheduler` in mock pgtrickle still return `SELECT 1 WHERE false` (`mock_pgtrickle.rs:92-101`) |
| PERF-1 | Blue/green convergence per-node polling | **Fixed** | `WaitForConvergence` uses `ANY($2)` batch query (`executor.rs:907-930`) |
| PERF-2 | Status watch reparses project every poll | **Partially Fixed** | mtime-based cache implemented in `status.rs:248-290`; live state still reconnects each poll (correct) |
| SEC-1 | Keyword-value DSN passwords not detected | **Fixed** | `KV_PASSWORD_RE` in `config.rs:25-28`; tests at `config.rs:461` |
| SEC-2 | Consumer view SQL executed without validation | **Open** (new from Phase 3 claim) | `validate_consumer_sql_is_single_select()` defined (`validate.rs:173`) but never called in executor; see finding SEC-1 below |
| SEC-3 | SET LOCAL timeout outside transaction | **Fixed** | `BEGIN READ ONLY; SET LOCAL statement_timeout` order corrected (`mod.rs:126`) |
| DOC-1 | README/install docs stale versions | **Partially Fixed** | README banner and `docs/installation.md` now correct; `docs/api-reference.md:6` still says v0.16.0; `docs/introduction.md:13` still says v0.7.0 |
| DOC-2 | HA guide claims unimplemented flags | **Fixed** | `--patroni-endpoint` implemented in `ApplyArgs` (`apply.rs:92-96`); `check_patroni_primary()` called between steps; status `'interrupted'` added; `MARK_MIGRATION_INTERRUPTED_SQL` added |
| CI-1 | Composite actions download wrong artifact names | **Fixed** | Platform-to-suffix mapping in both composite actions now matches release.yml (`linux-amd64`, `linux-arm64`, `macos-arm64`, `windows-amd64`) |
| CI-2 | Action smoke test uses wrong migration dir | **Fixed** | Smoke test writes to `migrations/streams/` (`action-smoke.yml:90`); composite actions exercised end-to-end |
| CI-3 | Docs CI passes with broken docs tests | **Fixed** | `continue-on-error` removed from `mdbook test` (`ci.yml:180`) |
| DEP-1 | async-std unmaintained via httpmock | **Open** | `cargo audit` now passes with `--deny warnings` (RUSTSEC-2025-0052 ignored) |
| ROAD-1 | Roadmap/changelog internally inconsistent | **Partially Fixed** | ROADMAP says "v0.16.0 released"; CHANGELOG goes to 0.16.0; workspace is 0.17.0; ROADMAP not updated to 0.17 |

---

## New Findings

---

### Critical

---

#### SEC-1 — Consumer view SQL injected directly without calling the existing validator

**File:** `crates/aqueduct-core/src/executor.rs:966-975` (ManageConsumerView "create"/"alter" arm)
**File:** `crates/aqueduct-core/src/validate.rs:173–220` (validator defined but uncalled)

**Problem:**

The function `validate_consumer_sql_is_single_select()` exists, is documented, and is
unit-tested (4 tests at `validate.rs:407-440`). The CHANGELOG entry for v0.14 says:
"Consumer SQL bodies validated as a single `SELECT` statement before view creation."
The implementation was never connected to the executor.

The executor's `ManageConsumerView "create" | "alter"` arm at `executor.rs:966-975` reads:

```rust
let body = spec.sql_body.as_deref().unwrap_or(default_body.as_str());
self.client
    .execute(
        &format!(
            "CREATE OR REPLACE VIEW {}.{} AS {}",
            quote_ident(view_schema),
            quote_ident(view_name),
            body          // ← injected verbatim, no validation
        ),
        &[],
    )
    .await?;
```

A consumer migration file containing:
```sql
-- @aqueduct:source = "orders"
-- @aqueduct:expose_as = "public.orders_view"
SELECT 1); DROP TABLE aqueduct.migrations; SELECT (1
```
would be planned and applied with the view body injected directly into the `CREATE OR REPLACE VIEW ... AS` statement.

**Impact:** SQL injection leading to arbitrary DDL execution under the `aqueduct_admin` role.
Any CI system that processes dbt manifests from an untrusted artifact store or allows
contributors to modify consumer migration files is vulnerable.

**Recommended fix:** Add the following before the `CREATE OR REPLACE VIEW` call:

```rust
if let Some(ref sql_body) = spec.sql_body {
    aqueduct_core::validate::validate_consumer_sql_is_single_select(
        sql_body, &spec.name
    )?;
}
```

The same guard should be added to the `SwapConsumerViews` assignment bodies.

**Effort:** XS (< 30 min)

---

#### SEC-2 — CNPG preview client unconditionally disables TLS certificate validation

**File:** `crates/aqueduct-core/src/preview.rs:518`

**Problem:**

```rust
let client = reqwest::Client::builder()
    .danger_accept_invalid_certs(true) // Allow self-signed K8s certs
    .build()
    ...
```

This is unconditional and disables hostname verification as well as certificate chain
validation. Any MITM attacker between the aqueduct runner and the Kubernetes API server
can intercept the clone manifest, inject cluster configuration, or capture the bearer
token used in the request.

**Impact:** Full MITM of the CNPG clone API call. The bearer token from
`CNPG_API_TOKEN` is sent in the `Authorization` header to an unverified endpoint.

**Recommended fix:**

```rust
let insecure = std::env::var("CNPG_INSECURE_SKIP_VERIFY").is_ok();
let client = if insecure {
    eprintln!("warning: CNPG_INSECURE_SKIP_VERIFY is set — TLS certificate validation disabled");
    reqwest::Client::builder().danger_accept_invalid_certs(true).build()?
} else {
    reqwest::Client::builder().build()?
};
```

Document `CNPG_INSECURE_SKIP_VERIFY` as a development-only escape hatch in `docs/security.md`.

**Effort:** XS (< 30 min)

---

#### SEC-3 — Age secret backend `identity_file` not path-validated; path traversal possible

**File:** `crates/aqueduct-core/src/secrets.rs:67-70, 134`

**Problem:**

`validate_secret_path(key)` is called for the encrypted file argument (line 108) but
`identity_file` — read from `$SOPS_AGE_KEY_FILE` or `$AGE_KEY_FILE` environment
variables — is passed directly to the `age` subprocess without validation:

```rust
let output = std::process::Command::new("age")
    .args(["-d", "-i", identity_file, key])   // identity_file unvalidated
    .output()?;
```

An attacker or misconfigured environment that sets `AGE_KEY_FILE=../../etc/passwd` will
cause `age` to attempt decryption using that path. Setting `AGE_KEY_FILE=-h` passes a
flag to `age` (flag-confusion), which may produce unexpected behavior depending on the
`age` version.

**Note:** `std::process::Command` does not use a shell, so shell injection is not
possible. The risk is path traversal and flag injection to the `age` subprocess.

**Recommended fix:**

```rust
validate_secret_path(&identity_file)?;
// Additionally reject strings starting with '-' (flag injection)
if identity_file.starts_with('-') {
    return Err(AqueductError::InvalidSecretPath { path: identity_file });
}
```

**Effort:** XS (< 15 min)

---

### High

---

#### ARCH-1 — Catalog schema override parsed but silently ignored

**File:** `crates/aqueduct-core/src/config.rs:44-46` (field declared)
**File:** `crates/aqueduct-cli/src/commands/init.rs:53` (flag declared, not used)
**File:** `crates/aqueduct-core/src/catalog.rs` (all SQL hardcodes `aqueduct`)

**Problem:**

`project.catalog_schema` is accepted in `aqueduct.toml` and `--schema` is accepted by
`aqueduct init`, but:
1. `init.rs:53` executes `CATALOG_INIT_V4_SQL` regardless of `args.schema`.
2. Every catalog SQL constant (`ACQUIRE_LOCK_SQL`, `START_MIGRATION_SQL`, etc.) hardcodes
   the schema name `aqueduct`.
3. `ensure_catalog_current()` references `aqueduct.cluster_profile` directly.

The ROADMAP describes this as a supported multi-tenant feature. Operators who configure
`catalog_schema = "team_a_catalog"` will silently have their catalog created in the
default `aqueduct` schema while no error is raised.

**Impact:** Multi-tenant isolation through catalog schema separation does not work.
Two projects sharing a database with different `catalog_schema` values will write
to the same `aqueduct` schema, allowing one project to read or corrupt another's
migration history.

**Recommended fix:**

1. Introduce a `CatalogSchema` parameter threading through `ensure_catalog_current()`,
   `connect_and_migrate()`, and all SQL-emitting functions, substituting the schema
   name using the validated `CatalogSchema::quoted()` method (the type already exists
   at `catalog.rs:1-80`).
2. Until this is done, remove the `catalog_schema` field from public config and the
   `--schema` flag from `init`, or display an explicit "not yet implemented" error.

**Effort:** L (3-5 days for full implementation; XS to add the explicit error)

---

#### ARCH-2 — Executor desired_state and heartbeat are opt-in, not enforced

**File:** `crates/aqueduct-core/src/executor.rs:786-793` (RecordSnapshot arm)
**File:** `crates/aqueduct-core/src/executor.rs:148-240` (builder methods)

**Problem:**

When `desired_state` is not set via `.with_desired_state()`, `RecordSnapshot` writes
`{}` as `spec_jsonb`. When `connection_string` is not set via `.with_connection_string()`,
no heartbeat task is spawned. Both are required for correct rollback and long-migration
safety respectively, but they are opt-in builder methods rather than required constructor
arguments.

Currently all production command paths (`apply.rs`, `promote.rs`, `rollback.rs`) set
both. But the typed constructors `for_apply()`, `for_rollback()`, `for_promote()` do not
chain these calls — they return a base executor. A future command author can call
`PlanExecutor::for_apply(...)` and `.execute(plan)` without the safety calls and the
compiler will not warn them.

**Impact:** Future regressions silently record empty specs (unrollbackable versions) or
omit the heartbeat (lock expiry on long migrations).

**Recommended fix:**

Replace the typed constructors with an `ExecutionContext` struct that requires both
`desired_state: DagState` and `connection_string: String`, and makes dry-run the only
escape hatch. Example:

```rust
pub struct ExecutionContext {
    pub desired_state: DagState,
    pub connection_string: String,
    pub patroni_endpoint: Option<String>,
    // ...
}
pub fn for_apply(client: &Client, project: &str, ctx: ExecutionContext) -> Self { ... }
```

**Effort:** M (1-2 days)

---

#### CORR-2 — Plan execution not transactionally bounded; partial-apply state possible on crash

**File:** `crates/aqueduct-core/src/executor.rs:427-1470`

**Status:** Open since Phase 1; partially mitigated by compensating-step registry (v0.15).

**Problem:**

The executor records compensating steps in `aqueduct.ddl_log` before each DDL operation.
However, compensating steps are advisory — the executor does not automatically execute
them on crash recovery. There is no code path that reads `ddl_log` entries on resume and
decides whether to roll forward or backward.

The consequence of a crash between `CreateStreamTable` and `RecordSnapshot` is that a
stream table exists in pg_trickle but has no catalog version. `aqueduct plan` will see
the table as pre-existing, compute a no-op diff, and silently succeed — leaving the DAG
at a version the catalog does not know about.

The consequence of a crash during `SwapConsumerViews` (after `BEGIN` but before `COMMIT`)
is that PostgreSQL rolls back the view swap automatically — this is now correct. The
problem is the `SWAP_BLUE_GREEN_SQL` call outside the transaction that follows.

**Impact:** Catalog drift after an interrupted migration. The compensating-step registry
records what was done but there is no automated recovery path that uses it.

**Recommended fix:**

1. On `--resume`, read `ddl_log` for the migration ID and check for `'running'` compensating
   steps. If any exist, apply them before resuming from the progress checkpoint.
2. Move `SWAP_BLUE_GREEN_SQL` inside the `SwapConsumerViews` transaction block.

**Effort:** L (3-5 days for recovery path; XS for the SWAP_BLUE_GREEN_SQL fix)

---

#### CORR-3 — Heartbeat task has no panic handler; silent lock loss on tokio panic

**File:** `crates/aqueduct-core/src/executor.rs:1540`

**Status:** Partially fixed (Phase 3). The heartbeat signals lock loss on `Ok(0)` and
logs on error. But panics in the heartbeat task are not caught.

**Problem:**

```rust
tokio::spawn(run_heartbeat(dsn, project, holder, rx, lock_lost_tx));
```

If `run_heartbeat` panics (e.g., a bug in future code, OOM), the task terminates silently.
The `lock_lost_tx` watch sender is dropped, but the main loop holds a `lock_lost_rx` clone
and checks `*lock_lost_rx.borrow()` — the value remains `false` (not signalled). The
executor continues under the false assumption the lock is held.

**Recommended fix:**

Wrap the heartbeat in a panic-catching closure:

```rust
tokio::spawn(async move {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || run_heartbeat(dsn, project, holder, rx, lock_lost_tx.clone())
    )).is_err() {
        let _ = lock_lost_tx.send(true);
    }
});
```

Or use a supervisor channel that triggers `lock_lost_tx` on task exit.

**Effort:** S (2-4 hours)

---

#### ARCH-3-REMAINING — Blue/green deployment status update outside swap transaction

**File:** `crates/aqueduct-core/src/executor.rs:985-996`

**Status:** Partially fixed (Phase 3). View swaps are in a transaction. The deployment
row update is still outside.

**Problem:**

```rust
// After COMMIT:
if let Some(deploy_id) = active_bg_deployment_id {
    let retain = 3600i64;
    let _ = self.client.execute(
        SWAP_BLUE_GREEN_SQL,    // ← outside the view swap transaction
        &[&deploy_id, &Option::<i64>::None, &retain.to_string()],
    ).await;
}
```

A network error after `COMMIT` but before `SWAP_BLUE_GREEN_SQL` leaves views pointing
to the green schema but the deployment row still showing `status='active'`. Downstream
monitoring or cleanup processes will misread the deployment state.

**Recommended fix:** Move `SWAP_BLUE_GREEN_SQL` inside the transaction by converting the
`swapped_at` and `retire_at` update into a savepoint within the `BEGIN` block.

**Effort:** S (2-4 hours)

---

#### TEST-3 — Mock scheduler has no observable state; drain/pause tests non-functional

**File:** `crates/aqueduct-testkit/src/mock_pgtrickle.rs:92-101`

**Status:** Open since Phase 3.

**Problem:**

```sql
-- pause_scheduler: returns nothing (mock, no state)
CREATE OR REPLACE FUNCTION pgtrickle.pause_scheduler(p_nodes text[])
RETURNS void AS $$ SELECT 1 WHERE false $$ LANGUAGE sql;
```

The mock accepts pause and resume calls but records no state. Integration tests that
exercise the drain-then-pause protocol cannot verify:
- The scheduler was paused before DDL executed.
- The scheduler was resumed after apply completed.
- A failed mid-migration apply resumes the scheduler correctly.

**Impact:** The drain-before-DDL safety guarantee is untestable; a regression would be
undetectable in the test suite.

**Recommended fix:**

Add a `pgtrickle.paused_nodes` table to the mock DDL and make `pause_scheduler` /
`resume_scheduler` insert/delete from it. Add an assertion helper `assert_scheduler_idle(client)`.

**Effort:** S (half day)

---

#### CI-1 — Example workflow files use stale action ref and version; users will get 404s

**File:** `.github/workflows/aqueduct-plan.yml:25-27`
**File:** `.github/workflows/aqueduct-apply.yml:27-29`
**File:** `ci/gitlab/aqueduct.gitlab-ci.yml:21`
**File:** `.pre-commit-hooks.yaml:7`

**Problem:**

These files are shipped as user-facing examples and templates. They contain:

| File | Problem |
|---|---|
| `aqueduct-plan.yml:25` | `uses: trickle-labs/pg-aqueduct/.github/actions/plan@v0.11.0` |
| `aqueduct-plan.yml:27` | `version: '0.4.0'` — attempts to download a release binary for v0.4.0 |
| `aqueduct-apply.yml:27` | Same action ref `@v0.11.0` |
| `aqueduct-apply.yml:29` | `version: '0.4.0'` — same 404 risk |
| `aqueduct.gitlab-ci.yml:21` | `AQUEDUCT_VERSION: "0.4.0"` — constructs a URL that 404s |
| `.pre-commit-hooks.yaml:7` | Comment shows `rev: v0.4.0` |

The action ref `@v0.11.0` means the installed composite action YAML is 6 versions behind;
any input fields added after v0.11 (e.g., `allow-plaintext-password`, `local-archive`)
would be unavailable.

The version-lint CI job (`ci.yml:220-248`) checks `README.md` and `docs/installation.md`
but does not check any of these six files.

**Impact:** Any user who copies the shipped example files and pushes them will fail CI
immediately with "HTTP 404: release binary not found."

**Effort:** XS (all six files updated in < 30 min)

---

### Medium

---

#### M-1 — `--quiet` and `--porcelain` flags are parsed but do not suppress command output

**File:** `crates/aqueduct-cli/src/main.rs:120-123`

**Problem:**

`OutputEmitter` is constructed and `commands::set_quiet(quiet)` is called, but the
constructed emitter is assigned to `let _emitter` and discarded. Approximately 15 command
handlers call `println!` directly and never call `is_quiet()` from the `commands` module.

```rust
let _emitter = OutputEmitter::new(output_mode);  // ← discarded
commands::set_quiet(quiet);  // ← only affects tracing log level
```

**Impact:** Users who pass `--quiet` to `aqueduct apply` or `aqueduct plan` still see
decorative plan output. CI pipelines that set `--porcelain` for machine parsing still
receive human-readable text mixed with machine output.

**Recommended fix:**

1. Thread `OutputEmitter` into command handlers by storing it as a process-wide singleton
   (e.g., via `OnceLock`) or by passing it as an argument to each `run()` function.
2. Replace direct `println!` calls in command handlers with `emitter.info()` / `emitter.raw()`.

**Effort:** M (1-2 days)

---

#### M-2 — `plan.rs --fail-on-drift` counts only stream deltas; source/consumer drift ignored

**File:** `crates/aqueduct-cli/src/commands/plan.rs:148-159`

**Problem:**

```rust
if args.fail_on_drift {
    let drift_count = diff
        .deltas
        .iter()
        .filter(|d| d.kind != DeltaKind::Unchanged)
        .count();
    ...
}
```

Only `diff.deltas` (stream table deltas) are counted. `diff.source_deltas` and
`diff.consumer_deltas` are not included. By contrast, `status.rs` was fixed in Phase 3
to count all three collections. The plan command has the same gap.

**Impact:** `aqueduct plan --fail-on-drift` exits 0 even when consumer views or source
tables have drifted. CI pipelines relying on `--fail-on-drift` in `aqueduct plan` will
miss consumer-layer and base-table drift.

**Recommended fix:** Replace the inline count with `diff.is_empty()` for the boolean
check and `diff.deltas.len() + diff.source_deltas.len() + diff.consumer_deltas.len()`
(excluding Unchanged) for the count.

**Effort:** XS (< 30 min)

---

#### M-3 — No panic handler on heartbeat task; lock loss may not be detected on tokio error

Already described as CORR-3 above. Repeated here for medium-priority tracking because
the practical frequency is low (tokio runtime panics are rare), but the safety impact
is high when it does occur.

---

#### M-4 — `EXPLAIN (FORMAT JSON)` in cost.rs uses `format!()` to embed user query

**File:** `crates/aqueduct-core/src/cost.rs:232`

**Problem:**

```rust
let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", query);
```

`query` is the stream table query from the migration file. PostgreSQL's `EXPLAIN`
does not support parameterized execution, so this is not fixable with `$1`
placeholders. The risk is limited because:
1. `query` is validated by `validate_sql_syntax()` or `validate_ivm_supportability()`
   before reaching this point.
2. Migration files are considered trusted input.

However, the existing `sqlparser` parse validation does not prevent all forms of
SQL that could behave unexpectedly when embedded in `EXPLAIN (FORMAT JSON) ...`,
such as a query containing a literal semicolon in a string, or a comment that
closes the EXPLAIN context.

**Impact:** Bounded risk. Migration files are operator-controlled, not user-controlled.
Not exploitable in the default threat model. But inconsistent with the principle that
SQL is never interpolated in production code.

**Recommended fix:** Document this as an intentional trade-off in a code comment.
For defense-in-depth, wrap the call in a `statement_timeout`-bounded read-only
transaction to limit any unexpected side effects.

**Effort:** S (1 hour for the comment + timeout guard)

---

#### M-5 — `destroy` command connects without calling `connect_and_migrate`

**File:** `crates/aqueduct-cli/src/commands/destroy.rs:61`

**Problem:**

```rust
let client = connect(&dsn).await?;
```

`destroy` is a write path that touches the catalog. If an operator runs `destroy` on
a database with an older catalog schema (e.g., upgraded from v0.14 to v0.17 but not
yet ran `aqueduct init` or `aqueduct apply`), the catalog tables may not have the
v8 columns that `destroy` queries.

`diff` (`diff.rs:43`) and `import` (`import.rs:42`) have the same pattern, but they
are read-only operations where schema drift is less harmful.

**Recommended fix:** Change `destroy.rs:61` to `connect_and_migrate(&dsn).await?`.

**Effort:** XS (< 5 min)

---

#### M-6 — `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` workaround still present in release workflow

**File:** `.github/workflows/release.yml:10`

```yaml
env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true
```

This is an undocumented GitHub Actions environment variable used as a workaround. It
should be replaced with proper action version pins (currently using `actions/checkout@v5`,
`dtolnay/rust-toolchain@stable`, etc. — already correct). Verify that removing the
workaround does not break the release workflow.

**Effort:** XS (verify and remove if safe)

---

#### M-7 — `ingest.rs` uses `.unwrap()` when writing compiled SQL in test helper

**File:** `crates/aqueduct-core/src/ingest.rs:476`

```rust
fs::write(compiled_dir.join(format!("{}.sql", model)), sql).unwrap();
```

This is in a test helper function (`build_test_dbt_target_dir`), not production code.
However, it is in the `src/` tree (not `tests/`) and will panic on CI if the test
harness fails to write the temp file. Should use `?` and propagate.

**Effort:** XS (< 5 min)

---

#### M-8 — Status watch mode never exits on repeated errors; no maximum backoff

**File:** `crates/aqueduct-cli/src/commands/status.rs:302-316`

**Problem:**

```rust
let poll_result = async {
    let client = connect_read_only(&dsn).await?;
    poll_once(...).await
}.await;

match poll_result {
    Ok(d) => d,
    Err(e) => {
        tracing::warn!("Status poll error (will retry): {}", e);
        0
    }
};
// ...
tokio::time::sleep(interval).await;
```

On repeated errors (e.g., database down), the loop retries at the fixed `interval`
indefinitely. There is no exponential backoff and no maximum error count. A long-running
`aqueduct status --watch` in a `systemd` unit against an unavailable database will log
repeated warnings at the configured interval forever.

**Recommended fix:** Add error counting with exponential backoff capped at some maximum
interval (e.g., 5× polling interval).

**Effort:** S (1 hour)

---

#### M-9 — `WaitForConvergence` deadline check happens after the sleep, not before

**File:** `crates/aqueduct-core/src/executor.rs:921-929`

**Problem:**

```rust
loop {
    let rows = self.client.query(...).await?;
    let still_running = rows.iter().any(...);
    if !still_running { break; }
    if std::time::Instant::now() >= deadline {
        return Err(...);
    }
    tokio::time::sleep(Duration::from_millis(poll_ms)).await;
}
```

The deadline check happens _before_ the sleep. If the deadline is exactly equal to the
query round-trip time (e.g., `max_wait_secs = 0`), the loop sleeps even after the
deadline has passed. More importantly, the poll query itself is not bounded by a
statement timeout — a very slow pg_trickle query could hold the loop open past the
deadline without triggering the error.

**Recommended fix:** Add `SET LOCAL statement_timeout` for the convergence poll query,
and move the deadline check _after_ the sleep to avoid a race on tight deadlines.

**Effort:** S (1 hour)

---

### Low

---

#### L-1 — `CANONICAL_KEY_ORDER` in `fmt.rs` is misleadingly named

**File:** `crates/aqueduct-core/src/fmt.rs:12-23`

The constant is used only to skip keys that have already been emitted by explicit
`if let Some(...)` blocks. The canonical ordering is maintained by code structure, not
data. The constant should be named `KNOWN_DIRECTIVE_KEYS` or the code should be
refactored to drive emission from the constant.

**Effort:** XS (rename only)

---

#### L-2 — `fmt.rs::read_original_content()` swallows IO errors

**File:** `crates/aqueduct-core/src/fmt.rs:126`

```rust
fn read_original_content(file: &MigrationFile) -> String {
    std::fs::read_to_string(&file.path).unwrap_or_default()
}
```

If the file is deleted or unreadable between parse and format, `unwrap_or_default()`
returns `""`, causing the formatter to always report a change and overwrite the file
with the reformatted content (empty SQL body becomes the body).

**Effort:** XS (propagate error via `?`)

---

#### L-3 — `GITHUB_ACTIONS` env var truthiness now correctly checked, but comment is outdated

**File:** `crates/aqueduct-cli/src/main.rs:103`

The Phase 1 finding (L7) was fixed — the code now checks `.as_deref() == Ok("true")`.
However, the comment above still reads "L7 (v0.14)" which suggests the fix was applied
at v0.14. The CIRCLECI and JENKINS_URL branches were added later but the comment was not
updated.

**Effort:** XS (comment update)

---

#### L-4 — `serde_yaml` pinned at v0.9 with an explanatory comment referencing v0.16

**File:** `Cargo.toml:41-42`

```toml
# DEP-1 (v0.14): pre-declare serde_yaml for v0.16 YAML work.
serde_yaml = "0.9"
```

v0.17 is now released and YAML work was completed (ERG-3 fixed). The comment is stale.
`serde_yaml` 0.9 is the current major series; there is no 1.0 release of serde_yaml —
the `DEP-1` tracking note in the comment is no longer relevant.

**Effort:** XS (update comment)

---

#### L-5 — No `.github/SECURITY.md` or security reporting policy

**File:** Repository root

The repository has no `SECURITY.md`. Users who discover security issues have no
documented disclosure path. GitHub recommends `.github/SECURITY.md` for vulnerability
reporting guidance.

**Effort:** XS (create file with contact and process)

---

#### L-6 — `ROADMAP.md` status says "v0.16.0 released" while workspace is at 0.17.0

**File:** `ROADMAP.md:3`

```
> **Status:** v0.16.0 released. Versions v0.16–v0.17 address all findings from the
```

The version-lint CI job does not check `ROADMAP.md`. This is a low-priority cosmetic
issue but creates reader confusion about project status.

**Effort:** XS (one-line update)

---

## Gap Analysis by Area

### 1. Correctness & Bug Audit

**Status: Good with known gaps.** The Phase 1 critical pipeline bugs (rollback, resume,
init catalog, AlterStreamTable, lock heartbeat, panics) are all fixed. The remaining
correctness risk is concentrated in:
- Blue/green atomicity (SWAP_BLUE_GREEN_SQL outside transaction)
- Crash recovery using the compensating-step registry (not yet automated)
- Heartbeat panic not signalling lock loss

The `--resume` mechanism is now functionally correct: `FINISH_MIGRATION_RECOVERABLE_SQL`
preserves progress, `find_resume_step` reads it, and the step loop skips completed steps.
The design choice to make progress writes **fatal** (`S-06: Checkpoint writes are fatal`)
is correct — a failed checkpoint means recovery is unsafe.

### 2. Security Audit

**Status: Good overall with three specific gaps (SEC-1, SEC-2, SEC-3).**

- `cargo audit` is clean (0 vulnerabilities).
- All database queries use parameterized statements on the core path.
- DSN redaction covers both URL-form and keyword-value formats.
- Read-only transactions are correctly ordered (`BEGIN READ ONLY; SET LOCAL`).
- Plaintext password detection covers both URL and keyword-value DSNs.
- Secret backends (env, sops, age, aws, gcp, vault) are all implemented.

The three new critical findings (consumer SQL injection, CNPG TLS bypass, Age path
traversal) are each fixable in under 30 minutes.

The security documentation (`docs/security.md`) now accurately reflects the implemented
features. The `--allow-plaintext-password` guard, parameterized queries, and audit trail
claims are all accurate.

### 3. CLI Ergonomics

**Status: Broadly functional, `--quiet`/`--porcelain` flags not fully wired.**

All 15 documented subcommands are present and exercised in CI. The `--yes`, `--confirm`,
`--fail-on-drift`, `--fail-if-changed`, `--dry-run`, `--resume`, `--force-retry`,
`--force-skip` flags are all implemented consistently. YAML output uses `serde_yaml`.
Exit code semantics are consistent (0=success, 1=non-empty plan with exit flag, 2=error).

`OutputEmitter` infrastructure exists but is not wired to command handlers (M-1). The
`--quiet` flag suppresses tracing log output but not `println!` in command bodies.

### 4. Test Coverage

**Status: Good for happy paths; missing failure-mode tests.**

275+ tests across three test categories. All 30 cookbook patterns are covered. Proptest
exists for planner fuzzing. Maintenance window, `allow_full_refresh`, and diamond DAG
tests exist.

Remaining gaps:
- Heartbeat panic → lock loss integration test
- Consumer SQL validation rejection (blocked by SEC-1 not being called)
- Scheduler state observability in mock (TEST-3)
- Blue/green partial failure with compensating step recovery

### 5. Performance & Scalability

**Status: Good for typical workloads. Known ceiling at ~1,000 nodes.**

- `WaitForConvergence` uses a batched `ANY($2)` query (PERF-1 fixed).
- Source cascade index built O(N) per diff (PERF-1 fixed in Phase 3).
- `status --watch` caches migration file mtimes (PERF-2 partially fixed).
- `WaitForRefresh` still polls one table at a time (by design — per-table steps).
- No scale tests beyond the 200-node benchmark document.

### 6. Documentation

**Status: Mostly accurate with stale version references and six user-facing files with
broken version pins.**

- `docs/security.md`: accurate for all implemented features.
- `docs/ha-operations.md`: accurate — `--patroni-endpoint` is implemented.
- `docs/api-reference.md:6`: says `Binary version: aqueduct 0.16.0` (should be 0.17.0).
- `docs/introduction.md:13`: says `v0.7.0 — Documentation & Cookbook release`.
- Example workflow files: reference `version: '0.4.0'` and action `@v0.11.0`.
- GitLab CI template: `AQUEDUCT_VERSION: "0.4.0"`.
- Pre-commit hooks: example comment shows `rev: v0.4.0`.

### 7. CI/CD & Release Pipeline

**Status: Release pipeline is solid; example automation files are stale.**

The CI matrix covers lint, security audit, unit tests, integration tests (PG 18), CLI
tests, coverage (≥60% gate), docs build, CLI reference check, version lint, and MSRV
check. The release pipeline builds five platform artifacts, attaches checksums, generates
SLSA provenance, and verifies the released binary.

The composite action smoke test exercises the composite plan/apply actions end-to-end
with a local archive — a meaningful improvement over Phase 3.

The six stale version references in example files (CI-1) are the only remaining user-
facing CI gap.

### 8. Dependencies

**Status: Clean. No CVEs. One informational warning acknowledged.**

`cargo audit --deny warnings` passes in CI (RUSTSEC-2025-0052 via `httpmock`/`async-std`
is ignored and tracked as DEP-1). All production dependencies are current major releases.
`serde_yaml = "0.9"` is the current stable series.

---

## Prioritised Improvement Roadmap

### Milestone A — Security Fixes (< 1 day total)

These are trivially fixable and should ship in the next patch.

1. **SEC-1**: Add `validate_consumer_sql_is_single_select()` call in executor `ManageConsumerView` arm and `SwapConsumerViews` assignments. (XS)
2. **SEC-2**: Gate `danger_accept_invalid_certs(true)` behind `CNPG_INSECURE_SKIP_VERIFY`. (XS)
3. **SEC-3**: Add `validate_secret_path(identity_file)` and flag-prefix check in Age backend. (XS)
4. **DOC-1**: Update all six files with stale version references to `0.17.0` / `@v0.17.0`. (XS)
5. **CI-extend**: Add `docs/api-reference.md`, `docs/introduction.md`, example workflow version refs to version-lint CI job. (XS)

### Milestone B — Correctness Hardening (v0.18)

6. **CORR-2**: Move `SWAP_BLUE_GREEN_SQL` inside the `SwapConsumerViews` transaction. (XS)
7. **CORR-3**: Add panic handler to heartbeat task; signal `lock_lost_tx` on task exit. (S)
8. **M-2**: Count all three delta collections in `plan.rs --fail-on-drift`. (XS)
9. **M-5**: Use `connect_and_migrate` in `destroy.rs`. (XS)
10. **TEST-3**: Add observable state to mock scheduler; assert paused/resumed in integration tests. (S)
11. **ARCH-1**: Either implement catalog schema substitution or add an explicit error when `catalog_schema != "aqueduct"`. (XS to add error; L for full implementation)

### Milestone C — Architecture & Ergonomics (v0.19 / v1.0 prep)

12. **ARCH-2**: Typed `ExecutionContext` that requires `desired_state` and `connection_string`. (M)
13. **M-1**: Thread `OutputEmitter` to command handlers; replace direct `println!` calls. (M)
14. **CORR-2 full**: Automated compensating-step recovery on `--resume` (read `ddl_log`, apply compensating steps before resuming). (L)
15. **TEST-new**: Heartbeat panic integration test; blue/green partial-failure test; consumer SQL rejection test. (M)
16. **M-8**: Exponential backoff in `status --watch` error loop. (S)
17. **L-5**: Add `.github/SECURITY.md`. (XS)
18. **M-6**: Remove `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` workaround or document. (XS)

---

## Missing Test Cases (Prioritised)

1. **consumer_sql_injection_rejected_at_apply** — create a consumer with injected DDL SQL body; assert executor returns `UntrustedSqlBody` error without executing any DDL. (Closes SEC-1 regression guard)

2. **heartbeat_panic_signals_lock_loss** — spawn an executor, trigger a heartbeat task panic via a channel; assert executor returns `LockLost` at the next step boundary. (Closes CORR-3)

3. **swap_consumer_views_atomicity** — inject a failure on the second consumer view swap inside the transaction; assert all views still point to the original schema (rollback), and `aqueduct.blue_green_deployments.status` still shows `active`. (Closes ARCH-3-REMAINING)

4. **plan_fail_on_drift_counts_consumer_deltas** — apply with a consumer view, then manually alter the catalog consumer_views row; run `aqueduct plan --fail-on-drift`; assert exit 1. (Closes M-2)

5. **destroy_auto_migrates_catalog** — create a pre-v8 catalog and run `aqueduct destroy`; assert it does not fail with a missing-column error. (Closes M-5)

6. **catalog_schema_override_rejected_until_implemented** — set `catalog_schema = "custom_schema"` in `aqueduct.toml`; run `aqueduct init`; assert explicit error is returned. (Closes ARCH-1 stop-gap)

7. **age_identity_file_traversal_rejected** — set `AGE_KEY_FILE=../../etc/passwd`; attempt secret resolution; assert `InvalidSecretPath` error. (Closes SEC-3)

8. **cnpg_preview_requires_valid_cert** — clear `CNPG_INSECURE_SKIP_VERIFY`; mock a CNPG endpoint with a self-signed cert; assert connection fails with a TLS error. (Validates SEC-2 fix)

9. **waitforconvergence_deadline_respected** — mock pgtrickle to always return `running` status; set `max_wait_secs = 1`; assert step fails within 2 seconds with `WaitForConvergence timeout` error. (Closes M-9)

10. **mock_scheduler_records_pause** — run any migration that modifies stream tables; assert `SELECT * FROM pgtrickle.paused_nodes` is populated during DDL steps and empty after apply completes. (Closes TEST-3)

---

## Appendix: All Phase 1 Findings — Final Resolution Status

| Phase 1 ID | Finding | Final Status |
|---|---|---|
| C1 | Rollback non-functional | ✅ Fixed |
| C2 | `--resume` no-op | ✅ Fixed |
| C3 | Panickable `.unwrap()` in plan.rs | ✅ Fixed |
| C4 | SQL injection in executor fallback | ✅ Fixed |
| C5 | Lock TTL never renewed | ✅ Fixed (heartbeat implemented) |
| C6 | `aqueduct init` installs v1 catalog | ✅ Fixed (v4 SQL used) |
| H1 | `AlterStreamTable` ignores `new_query` | ✅ Fixed |
| H2 | `--validate-ivm` calls syntax only | ✅ Fixed |
| H3 | Mid-column removal classified InPlace | ✅ Fixed |
| H4 | `aqueduct diff` missing | ✅ Fixed |
| H5 | Apply lacks confirmation prompt | ✅ Fixed |
| H6 | Drain-then-pause not implemented | ✅ Fixed |
| H7 | Diamond DAG consistency missing | ✅ Fixed |
| H8 | Status drift_count hardcoded 0 | ✅ Fixed |
| H9 | Status watch holds one connection | ✅ Fixed |
| H10 | CI single PG version | ⚠️ Open (only PG 18 in matrix; justified since pg_trickle requires PG 18+) |
| M1 | Secret inline syntax dead code | ✅ Fixed |
| M2 | Testkit catalog diverged | ✅ Fixed |
| M3 | Plan exit docs wrong | ✅ Fixed |
| M4 | CDC docs/implementation mismatch | ✅ Fixed |
| M5 | `--output` vs `--format` docs mismatch | ✅ Fixed |
| M6 | Validation lacks line numbers | ⚠️ Partially Fixed (DiagnosticSet exists; legacy path still string-based) |
| M7 | Destroy missing `--confirm` | ✅ Fixed |
| M8 | Plaintext password guard missing | ✅ Fixed |
| M9 | `RecordSnapshot` writes empty spec | ✅ Fixed |
| M10 | IVM checks never run | ✅ Fixed |
| M11 | No `--quiet`/`--porcelain` mode | ⚠️ Partially Fixed (infrastructure exists; not wired) |
| M12 | Progress always `{}` | ✅ Fixed |
| M13 | CI action project-dir issue | ✅ Fixed |
| M14 | Action uses missing `--fail-on-drift` | ✅ Fixed |
| M15 | Status JSON omits pg_version | ✅ Fixed |
| M16 | Status watch no reconnect | ✅ Fixed |
| L1 | Regex hot-path recompile | ✅ Fixed (`LazyLock`) |
| L2 | `fmt.rs` swallows read errors | ⚠️ Open |
| L3 | `CANONICAL_KEY_ORDER` misleading | ⚠️ Open |
| L4 | Testkit duplicates catalog SQL | ✅ Fixed |
| L5 | No property-based tests | ✅ Fixed (proptest) |
| L6 | No maintenance window tests | ✅ Fixed |
| L7 | CI env var truthiness | ✅ Fixed |
| L8 | Release archive naming | ✅ Fixed |
| L9 | No `cargo audit` in CI | ✅ Fixed |
| L10 | Tracing init concern | ✅ No longer applicable |
| L11 | Testcontainers image unpinned | ✅ Fixed (`postgres:18-alpine`) |
| L12 | Node24 workaround | ⚠️ Open |
| L13 | Top-level help flags | ✅ No longer applicable |
| L14 | TestDb public connection string | ⚠️ Open (low risk) |

**Resolution summary:** 38 of 46 original Phase 1 findings fully resolved. 6 partially
fixed or pending further work. 2 no longer applicable.
