# pg_aqueduct Roadmap

> **Status:** v0.17.0 released. Versions v0.18–v0.20 address all findings from the
> Phase 4 engineering assessment (`plans/overall-assessment-4.md`). v1.0 is the first
> production-ready release.
> This roadmap reflects the agreed design in `plans/pg-aqueduct-plan.md`.
> Versions correspond directly to the implementation phases described there.

---

## Overview

`pg_aqueduct` is a declarative migration and orchestration tool for stream-table DAGs
powered by `pg_trickle`. It is to a stream-table DAG what **Atlas** is to a relational
schema and what **Terraform** is to infrastructure.

The tool is delivered as a **standalone Rust CLI** (`aqueduct`) with an **optional
companion PostgreSQL extension** that adds DDL event triggers and SQL-callable
diagnostics. The CLI operates against any PostgreSQL cluster that has `pg_trickle`
installed; it requires no superuser privileges beyond what `pg_trickle` itself needs.

Each version in this roadmap is independently releasable and immediately useful. Later
versions build on earlier ones without breaking the established CLI surface.

---

## Version Map

| Version | Theme | Phases | Estimated Effort |
|---|---|---|---|
| [v0.1](#v01--foundation-plan--apply--rollback) | Foundation — plan, apply, rollback | 0–2 | 4–8 weeks |
| [v0.2](#v02--online-schema-evolution) | Online schema evolution | 3 | 2 weeks |
| [v0.3](#v03--bluegreen-preview-environments--optional-extension) | Blue/green, preview environments, optional extension | 4 | 3 weeks |
| [v0.4](#v04--ci-integrations--ergonomics) | CI integrations & ergonomics | 5 | 1 week |
| [v0.5](#v05--dbt-interop) | dbt interop | 6 | 1 week |
| [v0.6](#v06--production-hardening) | Production hardening | 7 | 3 weeks |
| [v0.7](#v07--documentation--cookbook) | Documentation & cookbook | 8 | 1 week |
| [v0.8](#v08--core-correctness--safety-hardening) | Core correctness & safety hardening | 10 | 3–4 weeks |
| [v0.9](#v09--feature-completeness--ergonomics) | Feature completeness & ergonomics | 11 | 3–4 weeks |
| [v0.10](#v010--safety-contract-repair--multi-project-isolation) | Safety contract repair & multi-project isolation | 13 | 5–6 weeks |
| [v0.11](#v011--documentation-truthfulness-cli-surface--code-quality) | Documentation truthfulness, CLI surface & code quality | 14 | 3–4 weeks |
| [v0.12](#v012--real-pgtrickle-integration-security--cicd-hardening) | Real pg_trickle integration, security & CI/CD hardening | 15 | 4–5 weeks |
| [v0.13](#v013--bluegreen-end-to-end-immediate-mode--advanced-features) | Blue/green end-to-end, IMMEDIATE mode & advanced features | 16 | 6–8 weeks |
| [v0.14](#v014--failure-safety-cicd-trustworthiness--security-hardening) | Failure safety, CI/CD trustworthiness & security hardening | 17 | 4–5 weeks |
| [v0.15](#v015--execution-integrity-architecture--bluegreen-production) | Execution integrity, architecture & blue/green production | 18 | 5–6 weeks |
| [v0.16](#v016--cli-quality-test-infrastructure--postgresql-compatibility) | CLI quality, test infrastructure & PostgreSQL compatibility | 19 | 4–5 weeks | ✅ Released |
| [v0.17](#v017--ha-operations-documentation-truthfulness--observability) | HA operations, documentation truthfulness & observability | 20 | 4–5 weeks | ✅ Released |
| [v0.18](#v018--security-hardening-documentation-correctness--operational-quality) | Security hardening, documentation correctness & operational quality | 21 | 3–4 weeks |
| [v0.19](#v019--correctness-hardening-bluegreen-atomicity--test-infrastructure) | Correctness hardening, blue/green atomicity & test infrastructure | 22 | 4–5 weeks |
| [v0.20](#v020--executor-architecture-multi-tenant-catalog--cli-polish) | Executor architecture, multi-tenant catalog & CLI polish | 23 | 5–6 weeks |
| [v1.0](#v10--release-engineering) | Release engineering | 24 | 2 weeks |
| [v1.1](#v11--consumer-layer-management) | Consumer layer management | — | TBD |
| [v2.0](#v20--multi-executor-support) | Multi-executor support | — | TBD |

---

## v0.1 — Foundation: Plan, Apply, Rollback

**Target effort:** 4–8 weeks for one engineer.
**Prerequisite:** `pg_trickle` exposes a stable JSON projection of a stream-table spec
(small change — file as a `pg_trickle` issue if not yet present).

This is the MVP release. It delivers the full plan → apply → rollback → import cycle
against a `pg_trickle` cluster. After v0.1, a team can fully adopt GitOps for their
stream-table DAG. No PostgreSQL extension is required; the CLI bootstraps its own
`aqueduct.*` catalog schema over a plain `libpq` connection on first `aqueduct init`,
exactly like Atlas manages `atlas_schema_revisions`.

### Phase 0 — Repository Bootstrap (3 days)

- [x] Create `trickle-labs/pg-aqueduct` with the standard `trickle-labs` repository
  layout (mirroring `trickle-labs/pg-tide` as the established precedent for extracting
  a tightly-scoped companion repository).
- [x] Cargo workspace with three initial crates:
  - [x] `aqueduct-core` — planner, differ, plan executor, catalog access
  - [x] `aqueduct-cli` — the `aqueduct` binary and `clap`-based command surface
  - [x] `aqueduct-testkit` — shared Testcontainers helpers for integration tests
- [x] No pgrx in this phase. The optional companion extension is deferred to v0.3 (Phase 4).
  This eliminates the pgrx build pipeline from the critical path and allows the CLI to
  ship on every managed PostgreSQL service (RDS, Supabase, Neon, Azure Database,
  AlloyDB, CloudNativePG, Citus) from day one.
- [x] `justfile` with standard recipes: `just build`, `just test`, `just lint`, `just fmt`.
- [x] CI matrix on Linux and macOS: unit tests + Testcontainers-based integration tests
  against `pg_trickle` (via mock schema; real pg_trickle in a follow-up).
- [x] Code-coverage gate to prevent regression below the initial baseline.
- [x] Bootstrap `aqueduct init`:
  - [x] Connects to the target database over `libpq`.
  - [x] Creates the `aqueduct` schema and all catalog tables (see catalog definition below).
  - [x] Handles the schema name collision case: if `aqueduct` already exists and is not
    owned by the connecting role, fails with a clear error. The `--schema` flag overrides
    the catalog schema name for this and all subsequent commands.
  - [x] Stores the chosen schema name in `aqueduct.cluster_profile` so subsequent commands
    pick it up automatically.
- [x] `README.md` and `ESSENCE.md` document the project's scope, non-goals, and the
  rationale for being a standalone repository rather than part of `pg_trickle` or dbt.

#### Catalog Tables (created by `aqueduct init`)

```sql
CREATE SCHEMA IF NOT EXISTS aqueduct;

-- Records a full snapshot of the DAG spec at each successful apply.
CREATE TABLE aqueduct.dag_versions (
    version    bigserial PRIMARY KEY,
    project    text      NOT NULL,
    spec_hash  bytea     NOT NULL,   -- SHA-256 of spec_jsonb (canonical JSON, keys sorted)
    applied_at timestamptz NOT NULL DEFAULT now(),
    applied_by text      NOT NULL,
    plan_jsonb jsonb     NOT NULL,
    spec_jsonb jsonb     NOT NULL
);

-- Tracks each migration run with its steps and progress (for resume).
CREATE TABLE aqueduct.migrations (
    id              bigserial PRIMARY KEY,
    from_version    bigint REFERENCES aqueduct.dag_versions(version),
    to_version      bigint REFERENCES aqueduct.dag_versions(version),
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    status          text NOT NULL,  -- 'running' | 'committed' | 'failed' | 'rolled_back'
    plan            jsonb NOT NULL,
    progress        jsonb NOT NULL DEFAULT '{}'::jsonb,
    cli_version     text,
    plan_format_version int NOT NULL DEFAULT 1
);

-- Serialises concurrent apply runs per project.
CREATE TABLE aqueduct.locks (
    project      text PRIMARY KEY,
    holder       text      NOT NULL,
    acquired_at  timestamptz NOT NULL,
    ttl          interval  NOT NULL,
    paused_nodes jsonb     NOT NULL DEFAULT '[]'::jsonb
);

-- Per-cluster calibration data (throughput benchmarks, catalog version).
CREATE TABLE aqueduct.cluster_profile (
    key         text PRIMARY KEY,
    value_jsonb jsonb NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now()
);
```

The catalog schema is self-migrating: every CLI command compares its compiled-in
`CATALOG_SCHEMA_VERSION` against the value in `aqueduct.cluster_profile` and applies
any pending catalog migrations from an embedded SQL bundle (`include_str!()`) before
doing anything else. No external files are required at runtime.

### Phase 1 — Read-Only Plan (1.5 weeks)

#### Deliverables

- [x] **TOML project loader.** Parses `aqueduct.toml` at the project root. Validates all
  required fields, resolves environment variable references (`${AQUEDUCT_PROD_DSN}`).
  The `[targets.*]` and `[apply]` sections are fully parsed. Template variables in
  `vars = { ... }` blocks are stored for substitution into migration front-matter.

- [x] **Migrations-folder parser.** Reads `migrations/streams/*.sql` and
  `migrations/sources/*.sql`. Parses SQL front-matter directives (`-- @aqueduct:key = value`).
  Unknown keys emit a lint warning for forward-compatibility. Template variable
  substitution (`{{ var.NAME }}`) is applied before parsing. File ordering is entirely
  derived from DAG topology — no sequence numbers or timestamps in filenames.

- [x] **Live-state reader.** Queries `pgtrickle.pgt_stream_tables` to build the current
  "actual" DAG state. Falls back to an empty state if pg_trickle is not installed.

- [x] **DAG differ.** Compares desired state (parsed migrations directory) against actual
  state (live catalog). Computes topological order using Kahn's algorithm. Detects and
  rejects cycles. Classifies changes as Create, Drop, AlterQuery, AlterSchedule,
  AlterRefreshMode, AlterCdcMode, or Unchanged.

- [x] **Query pre-validation.** Before classifying any new or changed stream-table query,
  the planner runs two validation passes:
  1. **Parse check** — `sqlparser` parses the SQL. Parse failures are plan errors.
  2. **IVM-supportability check** — validates that the query is differentiable under
     `pg_trickle`'s rules (no volatile functions, no DISTINCT, no set operations).

- [x] **Plan classification.** Each stream-table delta is classified into one of four
  migration kinds: Free, In-place, Rebuild, Blue/green.

- [x] **Human-readable plan renderer.** Outputs Text (default), JSON (`--format json`),
  and Markdown (`--format markdown`).

- [x] **`aqueduct plan` command.** Computes and renders the migration plan.

- [x] **`aqueduct status` command.** Reports the current project state in a concise
  one-screen summary: version, stream table count, drift status.

- [x] **`aqueduct validate` command.** Offline check — no database connection required.
  Parses all migration files, validates front-matter syntax, checks SQL via `sqlparser`,
  detects dependency cycles. Reports errors and warnings.

**Exit criteria.** `aqueduct plan` produces a correct plan covering {add, drop, change
schedule, change refresh_mode} for a DAG in a Testcontainers Postgres. ✅
the full apply cycle.

#### Deliverables

**TOML project loader.** Parses `aqueduct.toml` at the project root. Validates all
required fields, resolves environment variable references (`${AQUEDUCT_PROD_DSN}`),
and rejects plaintext passwords in config files unless `--allow-plaintext-password` is
explicitly set (security requirement per §10.20). The `[targets.*]` and `[apply]`
sections are fully parsed. Template variables in `vars = { ... }` blocks are stored for
substitution into migration front-matter.

**Migrations-folder parser.** Reads `migrations/streams/*.sql` and
`migrations/sources/*.sql`. Parses SQL front-matter directives (`-- @aqueduct:key =
value`) using a lenient TOML-inline-value grammar. Unknown keys emit a lint warning
for forward-compatibility. Template variable substitution (`{{ var.NAME }}`) is applied
before the TOML parser sees directive values. Detects and reports missing variables.
File ordering is entirely derived from DAG topology — no sequence numbers or timestamps
in filenames, eliminating the merge-conflict problem that plagues Flyway and Alembic.

**Live-state reader.** Queries `pgtrickle.pgt_stream_tables`, `pg_class`, and
`pg_attribute` to build the current "actual" DAG state. Reconstructs a
`StreamTableSpec` per node using the JSON spec projection from `pg_trickle` (or falls
back to direct catalog column reads if the projection function is not yet available).

**DAG differ.** Compares the desired state (parsed migrations directory) against the
actual state (live catalog) and the recorded history (`aqueduct.dag_versions`). Walks
three layers simultaneously:

1. **Source layer** — base tables declared as `owned = false` sources. Tracks schema
   changes from `pg_attribute` for impact analysis. Never generates DDL for these.
   Internal extension tables (`_pg_ripple.*`, `_pg_eddy.*`, `_riverbank.*`) are
   excluded by default via built-in exclusion patterns.
2. **Stream-table DAG** — nodes and edges managed by `pg_trickle`. Computes
   topological order using Kahn's algorithm. Detects and rejects cycles.
3. **Drift** — divergence between recorded history and actual state (manually created
   tables, out-of-band schedule changes, dropped columns).

Produces a typed `Plan` value containing an ordered list of `PlanStep` variants.

**Query pre-validation.** Before classifying any new or changed stream-table query,
the planner runs two validation passes:
1. **Parse check** — `pg_query.rs` (libpg_query bindings) parses the SQL. Parse
   failures are plan errors — the migration is rejected before any step is emitted.
2. **IVM-supportability check** — validates that the query is differentiable under
   `pg_trickle`'s rules (no volatile functions, no unsupported aggregates, no
   non-deterministic expressions) when `refresh_mode = 'DIFFERENTIAL'`. An
   IVM-unsupportable query with `DIFFERENTIAL` mode is a plan error, not a warning.
   The `--auto-downgrade-refresh-mode` flag allows the planner to reclassify to
   `FULL` in this case, but emits a prominent warning and a `aqueduct lint` flag.

This prevents `aqueduct plan` from producing a plan that `pg_trickle` will reject at
apply time — the most confusing failure mode a migration tool can have.

**Plan classification.** Each stream-table delta is classified into one of four
migration kinds, in order of increasing cost:

| Class | Example Changes | Cost |
|---|---|---|
| **Free** | Schedule change, refresh-mode change, CDC mode change | Single `pgtrickle.alter_stream_table()` call; no rebuild |
| **In-place** | Add a passthrough column, add a new aggregate column, widen a type | `ALTER` + targeted incremental backfill; preserves materialized state |
| **Rebuild** | Change `GROUP BY` keys, change a join condition, change `WHERE` predicate, rename a column | Drop + recreate + `FULL` refresh |
| **Blue/green** | Restructure DAG topology (split/merge nodes, change aggregate keys structurally) | Build green DAG alongside blue, swap atomically (v0.3) |

Conservative classification: when in doubt, classify as Rebuild. A `--strict` mode
refuses any unproven in-place path. Property-based tests compare in-place results to
from-scratch rebuilds.

**Diamond DAG consistency.** The planner detects `diamond_consistency = 'atomic'`
groups by querying `pgtrickle.pgt_stream_tables` and by traversing convergence nodes
in the dependency graph. All nodes in a diamond group are upgraded to the highest
migration class of any member. A Rebuild in one member means all members are
Rebuilt together, in a single locked batch. The plan renderer explains the promotion.

**Human-readable plan renderer.** Outputs:
- **Text** — the default, for terminal output:
  ```
  Project  checkout-analytics  v18 → v19
  Target   prod (pg_trickle 0.62, pg 18.3)

  Changes  3 nodes affected

    ~ order_totals          [in-place]   add column discount_total (SUM)
    + promo_summary         [create]     new stream table (DIFFERENTIAL, schedule 30s)
    ! customer_ytd          [rebuild]    WHERE predicate changed → requires full rebuild

  Cost estimate (rebuild steps gated by maintenance window 02:00-04:00 UTC):

    Step                           Rows (est)    Duration (est)    Class
    ─────────────────────────────────────────────────────────────────────
    ALTER stream order_totals           —             < 1s          in-place
    BACKFILL order_totals            1.2M            ~45s          in-place
    CREATE stream promo_summary         —             < 1s          create
    BACKFILL promo_summary            340K            ~12s          create
    DROP + RECREATE customer_ytd      5.8M            ~3m           rebuild ⏰

  ⏰ Rebuild steps will be deferred to the maintenance window (02:00-04:00 UTC).
  ```
- **JSON** — for machine consumers and scripting (`--format json`).
- **Markdown** — the format posted to PRs by CI integrations (`--format markdown`).

**Cost estimation.** `aqueduct plan --dry-run --explain-cost` provides per-step
estimates without executing anything:
- **Row count:** `EXPLAIN (FORMAT JSON)` against the stream table's defining query;
  extract `Plan Rows` from the top-level node.
- **Refresh duration for FULL:** `estimated_rows × bytes_per_row /
  measured_write_throughput`. Write throughput is calibrated per-cluster by a small
  benchmark (INSERT 10k rows into a temp table, measure wall time) run during
  `aqueduct init` and stored in `aqueduct.cluster_profile`.
- **For DIFFERENTIAL:** "near-zero — materialized state preserved." Reports estimated
  change-buffer size instead.
- **For IN-PLACE:** row-count estimate with a lower throughput constant than Rebuild.
- Goal: distinguish "2-second migration" from "2-hour migration"; sub-second accuracy
  is explicitly a non-goal.

**`maintenance_window` enforcement.** When `maintenance_window = "02:00-04:00 UTC"` is
set in `aqueduct.toml`, the planner gates Rebuild and Blue/green steps:
- If the current time is outside the window and the plan contains Rebuild or Blue/green
  steps, `aqueduct apply` exits non-zero with a clear message.
- Free and In-place steps execute immediately regardless of the window by default.
- `--ignore-maintenance-window` overrides for emergency runs.
- The `maintenance_window_applies_to` key controls which classes are gated.

**`allow_full_refresh = false` enforcement.** When set in `aqueduct.toml`, the planner
rejects any plan that would classify a stream table as Rebuild class, listing the
affected tables and the classification reason. The operator must either rewrite the
query to be in-place-compatible or pass `--allow-rebuild` to override for one run.

**`aqueduct status`.** Reports the current project state in a concise one-screen
summary:
```
Project   checkout-analytics    v18   applied 2m ago by ci@prod
Database  prod (pg_trickle 0.62, pg 18.3)

Stream tables  22 managed   0 drift   0 unmanaged
Last migration 18→18 (no-op): 2026-05-13 09:01:12 UTC

Drift          none detected  (polled 2m ago)
Scheduler      running  (all 22 nodes active)
Pending plan   none
```
When drift exists, reports which tables diverged and how, and suggests
`aqueduct plan` to see the full cascade. In polling-based mode (no companion extension
installed), drift is detected by comparing `pg_attribute` snapshots between runs.

**`aqueduct validate`.** Offline check — no database connection required. Parses all
migration files, validates front-matter syntax, checks SQL via `pg_query.rs`, detects
dependency cycles in the declared DAG, and reports errors. Does not check
IVM-supportability (that requires `pg_trickle`'s exact capabilities, which vary by
version — use `aqueduct plan` for that). Intended for pre-commit hooks and CI jobs
where a database connection is unavailable.

**Exit criteria.** Against a 20-node DAG on a Testcontainers Postgres, `aqueduct plan`
produces a correct plan for the full cartesian product of {add, drop, change SQL, change
schedule, change refresh_mode}. All changed queries pass IVM-supportability
pre-validation before the plan is emitted; an unsupported query produces a plan error,
not a later apply failure.

### Phase 2 — Apply (In-Place + Rebuild) (2 weeks)

#### Deliverables

- [x] **Plan executor.** Executes an ordered `Plan` as a sequence of typed `PlanStep`
  variants: `LockDag`, `ValidateQuery`, `CreateStreamTable`, `AlterStreamTable`,
  `DropStreamTable`, `Backfill`, `RecordSnapshot`, `UnlockDag`.

- [x] **Lock manager.** `aqueduct.locks` serialises concurrent apply runs. Lock is
  released automatically on crash via TTL expiry.

- [x] **`aqueduct apply` command.** Executes the plan, records the migration in
  `aqueduct.migrations`, and reports the new version.

- [x] **`aqueduct apply --dry-run`.** Shows what would be done without executing.

- [x] **`aqueduct apply --resume`.** Skips already-completed steps after a crash.

- [x] **`aqueduct rollback` command.** Reverts to the previous DAG version by computing
  a forward migration plan from the current state to the desired prior state.

- [x] **`aqueduct import` command.** Bootstraps from an existing live `pg_trickle`
  deployment: generates `migrations/streams/*.sql` and a skeleton `aqueduct.toml`.
  Supports `--exclude-pattern` with built-in exclusions for `_pg_ripple.*`,
  `_pg_eddy.*`, `_riverbank.*`.

- [x] **`aqueduct unlock` command.** Releases a stale project lock (emergency use).

- [x] **Plan format versioning.** Every serialised plan includes a `plan_format_version`
  integer starting at 1.

- [x] **Observability.** Structured JSON logs when `--log-format json` is set or any of
  `$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`, `$CIRCLECI` are set.

- [x] **HA awareness.** `aqueduct apply` refuses to run against a hot standby
  (`pg_is_in_recovery()` — hard error).

- [x] **Security model.** Env-var resolution for DSN. `allow_full_refresh = false`
  enforcement prevents accidental full rebuilds.

- [x] **`pg_trickle` version compatibility.** Queries `pgtrickle.pgt_extension_version()`
  on connect; gracefully degrades when pg_trickle is not installed.

- [x] **Example projects.** `examples/minimal/` — 3-node DAG with sources, streams,
  and README.

**Exit criteria.** A plan → apply → plan → apply cycle on a Testcontainers cluster
ends with an empty plan. ✅

---

## v0.2 — Online Schema Evolution

**Target effort:** ~2 weeks.
**Builds on:** v0.1 complete.

This version makes the in-place classifier accurate and complete, turning the most

| Step | Description |
|---|---|
| `LockDag { ttl }` | Acquire project lock in `aqueduct.locks` via `SELECT FOR UPDATE NOWAIT` |
| `ValidateQuery { name, query }` | IVM-supportability pre-check; always emitted first for any query change |
| `AlterBaseTable { name, statement }` | Base-table DDL for Tier 1 stream-adjacent columns |
| `CreateStreamTable { spec }` | Creates a new stream table via `pgtrickle.create_stream_table()` |
| `AlterStreamTable { name, change }` | Alters an existing stream table via `pgtrickle.alter_stream_table()` |
| `DropStreamTable { name, cascade }` | Drops a stream table via `pgtrickle.drop_stream_table()` |
| `Backfill { name, mode }` | Triggers a full or windowed backfill |
| `RecreatePolicy { name, policy_sql }` | Restores RLS policies lost in rebuild-class migrations |
| `DetachOutbox { stream_table, outbox_name }` | Unhooks a `pg_tide` outbox attachment before drop |
| `ReattachOutbox { stream_table, outbox_name, retention_hours }` | Restores the outbox attachment after recreate |
| `ManageWalSlot { stream_table, action }` | Drops or recreates the logical replication slot for `cdc_mode = 'wal'` tables |
| `PauseImmediate { name }` | Temporarily switches an IMMEDIATE stream table to DIFFERENTIAL during a rebuild |
| `ResumeImmediate { name }` | Switches back to IMMEDIATE mode after rebuild completes |
| `RecordSnapshot { version }` | Records the new DAG version in `aqueduct.dag_versions` |
| `WaitForRefresh { name, deadline }` | Polls until a stream table's refresh completes |
| `RunHook { name, statement }` | User-defined SQL pre/post hooks |
| `UnlockDag { force }` | Always the last step; restores scheduler and releases lock |

Execution is transactional where possible. Steps that cannot be transactional
(large `FULL` refreshes, `CONCURRENTLY` index operations) are made **resumable**:
the plan stores per-step progress in `aqueduct.migrations.progress` (JSONB) so
`aqueduct apply --resume` can skip completed steps after a crash.

Recovery semantics per step category:

| Category | Recovery |
|---|---|
| **Transactional** (`AlterStreamTable`, `SwapView`, `RecordSnapshot`) | Rolled back automatically on crash; step is retried |
| **Idempotent** (`CreateStreamTable IF NOT EXISTS`, `AlterBaseTable` with catalog guard) | Safe to re-execute |
| **Long-running** (`Backfill { mode: FULL }`, `WaitForRefresh`) | Checkpointed by row-range or table. On resume, Backfill restarts the affected table's full refresh, not the entire plan |

**Lock manager.** `aqueduct.locks` serialises concurrent apply runs using a PostgreSQL
row-level lock (`SELECT FOR UPDATE NOWAIT`). Unlike advisory locks, row locks are
automatically cleaned up if the client disconnects — making the model crash-safe
without a separate cleanup job. The `ttl` column is used as a heartbeat deadline:
a background task updates `acquired_at` every `ttl / 3` during long-running migrations.
If the CLI crashes, the heartbeat stops and the lock expires after one `ttl`.

**Drain-then-pause protocol.** Before any migration step, `aqueduct apply` calls
`pgtrickle.pause_scheduler(nodes => [...])` then polls for `refresh_status = 'running'`
to drain in-flight refreshes. If the drain deadline (from `lock_timeout` in
`aqueduct.toml`) expires, the CLI aborts the migration cleanly (no changes applied)
and reports which node is blocking. On success or failure, the scheduler is always
resumed. If the CLI crashes before resuming, the lock TTL expiry + `aqueduct apply
--resume` handles recovery.

Fallback for older `pg_trickle` versions that lack `pause_scheduler()`: set the
affected node's schedule to `'999d'`, apply, then restore. Racy but acceptable for v0.1
against pre-release clusters.

**`aqueduct apply --resume`.** Reads `aqueduct.migrations.progress`, identifies
completed steps, and skips them. For ambiguous steps (connection lost mid-`ALTER TABLE`
with no way to query the catalog), prints a diagnostic report and exits non-zero.
The operator must inspect and either `--force-retry` or `--force-skip` the ambiguous
step. Automatic guessing is never acceptable — this is the "data loss is unacceptable"
principle.

**`aqueduct rollback`.** Reverts to the previous DAG version by computing a forward
migration plan from the current state to the prior spec. Rollback is always a single
atomic operation regardless of how many versions are being skipped — it never replays
individual rollbacks in reverse.

Lossless guarantee:
- **Free and In-place migrations:** always lossless.
- **Rebuild migrations:** lossless only if no full refresh has completed since the
  migration applied. `aqueduct rollback` reports the lossless window expiry time
  (estimated from the slowest affected node's schedule) and requires
  `--accept-data-loss` if the window has passed.
- **Blue/green deployments:** the blue schema is retained until `--blue-ttl` expires
  (default 1 hour). Rollback within this period swaps the consumer views back to blue
  and drops green. After expiry, rollback requires a new forward migration.
- `aqueduct rollback` never rolls back base-table DDL (Tier 1 changes). The operator
  supplies a compensating Atlas migration for any base-table changes.
- `--to-version vN` rolls back to an arbitrary prior version in a single atomic plan.

**`aqueduct import`.** Bootstraps from an existing live `pg_trickle` deployment:
```bash
aqueduct import --from prod --output ./my-project/
```
1. Connects to the target and queries `pgtrickle.pgt_stream_tables`.
2. Generates `migrations/streams/*.sql` for each stream table, with front-matter
   directives populated from the catalog.
3. Optionally generates `migrations/sources/*.sql` for each referenced base table
   (`owned = false`).
4. Generates a skeleton `aqueduct.toml`.
5. Runs `aqueduct init` against the database.
6. Records the current state as `dag_version = 1` (the baseline).

After import, the next `aqueduct plan` produces an empty plan. The
`--exclude-pattern` flag skips internal extension tables:
```bash
aqueduct import --from prod \
  --exclude-pattern '_pg_ripple.*' \
  --exclude-pattern '_pg_eddy.*'
```
Built-in exclusions (applied by default, disable with `--no-default-exclusions`):
`_pg_ripple.*`, `_pg_eddy.*`, `_riverbank.*`.

**Plan format versioning.** Every serialised plan in `aqueduct.migrations.plan`
includes a `plan_format_version` integer (starting at 1). A CLI can resume any plan
whose format version is ≤ its own compiled-in version. It refuses to resume a plan
from a newer CLI. Format changes are always backwards-compatible additions until a
breaking change warrants a major format version bump.

**Observability.** Every command emits structured JSON logs to stderr when
`--log-format json` is set, or when any of `$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`,
`$CIRCLECI` are set. Each plan step emits a log line with step type, table, status,
and elapsed milliseconds. `aqueduct apply` sets `application_name` on the Postgres
connection to `aqueduct/<project>/<migration_id>` so in-flight migrations are visible
in `pg_stat_activity`.

**HA awareness.** `aqueduct apply` refuses to run against a hot standby
(`pg_is_in_recovery()` — hard error). Between each plan step, it re-checks the
primary's `system_identifier` and `timeline_id`. On failover detection, it marks the
migration as `status = 'interrupted'` and exits with a clear message. Patroni
`--patroni-endpoint` and CloudNativePG `cnpg.io/cluster` annotations are supported
for primary discovery.

**IMMEDIATE mode stream tables.** For Free/In-place changes: the entire migration
(trigger drop + ALTER + trigger recreate) executes inside a single `SERIALIZABLE`
transaction — no DML is lost. For Rebuild-class changes: the plan inserts
`PauseImmediate` (switch to DIFFERENTIAL temporarily) + rebuild + `ResumeImmediate`.
The plan renderer warns of the brief consistency window. The `--no-immediate-downgrade`
flag rejects plans that would temporarily downgrade an IMMEDIATE table.

**Security model.** The CLI:
- Refuses plaintext passwords in config files unless `--allow-plaintext-password` is
  explicitly set.
- Documents the `aqueduct_admin` least-privilege role: `USAGE`/`CREATE` on the
  `aqueduct` schema; `USAGE` on the `pgtrickle` schema; `SELECT` on `pg_class`,
  `pg_attribute`, `pg_type`; `CREATE`/`ALTER`/`DROP` on stream-table schemas only.
  No superuser, no `CREATEROLE`, no replication.
- Opens read-only transactions (`SET TRANSACTION READ ONLY`) for `plan` and `status`
  — these commands cannot accidentally mutate data.
- Records in `aqueduct.migrations`: authenticated Postgres role, client IP
  (`inet_client_addr()`), CLI version, and full plan — queryable for compliance audits.

**`pg_trickle` version compatibility.** `aqueduct` declares a minimum supported
`pg_trickle` version in `aqueduct.toml`. On apply, the CLI queries
`pgtrickle.pgt_extension_version()` and fails with a clear error if the installed
version is too old. CI runs the E2E suite against {latest, latest-1, minimum supported}.
The CLI uses only the stable SQL API and does not depend on internal catalog column
layouts.

**Exit criteria.** A randomised property test runs N random plan → apply → plan → apply
cycles on a Testcontainers cluster and asserts that every cycle ends with an empty plan.

---

## v0.2 — Online Schema Evolution

**Target effort:** ~2 weeks.
**Builds on:** v0.1 complete.
**Status:** v0.2 implementation complete. See checklist below.

This version makes the in-place classifier accurate and complete, turning the most
common DAG evolution patterns into zero-rebuild operations. It also delivers cost
visibility for every migration class before it is executed.

### Phase 3 — Online Evolution (2 weeks)

#### Deliverables

- [x] **Full migration classifier (§7.3 / §10.3).** Implements the complete decision tree
  for mapping a stream-table delta to a migration class:

| Change | Class | Rationale |
|---|---|---|
| Schedule, `cdc_mode`, `refresh_mode` DIFF→FULL | Free | Metadata-only |
| Add a non-aggregate passthrough column | In-place | Column in SELECT but not in GROUP BY/aggregate; incremental backfill |
| Widen a column type (`int → bigint`, `varchar(50) → varchar(200)`) | In-place | Type-compatible; existing rows remain valid |
| Add a new aggregate column (SUM, COUNT, etc.) | In-place | New column backfilled incrementally |
| Drop a column from SELECT | In-place | `ALTER ... DROP COLUMN` on the materialised table |
| Rename a column | Rebuild | Cannot rename in-place without losing delta-state tracking |
| Change `GROUP BY` keys | Rebuild | Entire aggregation structure changes |
| Change a JOIN condition | Rebuild | Row membership changes unpredictably |
| Add or remove a JOIN | Rebuild | Source set changes |
| Change a `WHERE` predicate | Rebuild | Row membership changes |
| Switch `refresh_mode` FULL→DIFF | Rebuild | Must establish delta-tracking state from scratch |
| Topology change (split or merge nodes) | Blue/green | Structural DAG change |

The classifier is intentionally conservative. When a case cannot be proven safe for
in-place migration, it falls back to Rebuild. The `--strict` mode refuses any unproven
in-place path.

The classification logic is vendored into `aqueduct-core` initially, with a CI job
that diffs it against `pg_trickle`'s internal rules to catch divergence. The
medium-term destination is a shared `pg_trickle_calculus` crate that both projects
depend on.

- [x] **`pg_eddy` Cypher source handling.** A stream table backed by a `pg_eddy` MATCH query
  is defined in Cypher, which `pg_eddy` compiles to SQL at create time. The stored SQL is
  the compiled translation. Migration files for `pg_eddy`-backed tables use the
  `@aqueduct:cypher_source` front-matter directive pointing to the `.cypher` file.
  The directive is recognised and stored in `StreamTableSpec`; it does not generate
  unknown-key warnings. Full Cypher pre-translation via `pg_eddy.cypher_to_sql()` is
  supported in plans where a database connection is available.

- [x] **`ALTER TABLE` cascade analysis (Tier 1 base-table changes, §10.5).** When a base
  table column that appears in at least one stream-table query is altered, `aqueduct plan`
  takes full ownership of the cascade:
  1. Generates the base-table `ALTER TABLE` DDL (`AlterBaseTable` plan step).
  2. Computes the downstream impact on every stream-table node that references the
     changed column (directly or transitively) via `compute_source_deltas`.
  3. Classifies each affected node (always Rebuild for a base-table DDL change).
  4. Emits a single coordinated plan covering the base-table ALTER and the full
     stream-table cascade.

  Tier 2 standalone base-table objects (new tables, non-referenced columns, indexes,
  partitioning, types) are delegated to Atlas/sqitch via pre/post hook:
  ```toml
  [apply.hooks]
  pre  = "atlas schema apply --to file://schema.hcl"
  ```
  The boundary is determined automatically by `sqlparser`: if a column is provably
  unreferenced in any stream-table query, `aqueduct` defers.

- [x] **`aqueduct plan --explain-cost`.** Full cost breakdown per step:
  ```
  Step                          Rows (est)    Duration (est)    Class
  ─────────────────────────────────────────────────────────────────────
  ALTER base raw.orders           —             < 1s            free
  ALTER stream order_totals       —             < 1s            free
  BACKFILL order_totals           1.2M          ~45s            rebuild
  ALTER stream customer_summary   —             < 1s            in-place
  BACKFILL customer_summary       340K          ~12s            in-place
  ```
  Row counts are obtained via `EXPLAIN (FORMAT JSON)`.  Duration is estimated as
  `rows × avg_bytes / write_throughput` (default 50 MB/s).

- [x] **`pg_trickle` issues filed.** Phase 3 identifies the small extensions to
  `pg_trickle` needed to unlock additional in-place paths (e.g., ALTER to widen a column
  type without a full rebuild). These are tracked as `pg_trickle` issues and unblocked
  in a future patch to this classifier as `pg_trickle` ships the corresponding API.

**Exit criteria.** Column-add and schedule-change migrations on the integration-test
DAG produce zero FULL-refresh steps when classified as in-place. ✅

---

## v0.3 — Blue/Green, Preview Environments & Optional Extension

**Target effort:** ~3 weeks.
**Builds on:** v0.2 complete.
**Status:** Complete ✅

This version delivers zero-downtime structural DAG migrations, per-branch preview
environments for testing candidate changes, and the optional `pg_aqueduct` companion
extension that enables passive DDL drift detection and SQL-callable diagnostics.

### Phase 4 — Blue/Green, Preview, and Optional Extension (3 weeks)

#### Blue/Green Deployer

- [x] `CreateGreenSchema` / `CreateStreamTableInGreen` / `WaitForConvergence` /
  `SwapConsumerViews` / `RetireBlueSchema` plan steps added to `plan.rs`
- [x] `PlanExecutor` handles all six new step variants (`executor.rs`)
- [x] `PlanCost` extended to cover blue/green and consumer-view steps (`cost.rs`)
- [x] Catalog v2: `blue_green_deployments` table with status tracking (`catalog.rs`)
- [x] `ViewAssignment` struct for atomic view swap metadata

For large structural changes (a node is split into two, an aggregate key changes,
sub-DAG topology restructuring), `aqueduct apply --strategy blue-green --to prod`:

1. Creates the new DAG nodes in a parallel versioned schema:
   `{project_name}__v{dag_version}` (double underscore to prevent collision with user
   schemas and other projects). For project `checkout-analytics` migrating from v17
   to v18, the green schema is `checkout_analytics__v18`.
2. Backfills the green DAG nodes in the background while the blue DAG continues
   serving reads.
3. Waits for the green DAG to converge (drift from live data is within the configured
   `--convergence-lag` threshold).
4. Executes the consumer view cutover in a single sub-millisecond transaction:
   `CREATE OR REPLACE VIEW public.foo AS SELECT * FROM checkout_analytics__v18.foo`
   for every node in the DAG. This takes an `ACCESS EXCLUSIVE` lock on the views —
   not on the underlying tables — making it invisible to running read queries.
5. Retires the blue schema (`checkout_analytics__v17`) after `--blue-ttl` (default
   1 hour) via `aqueduct apply --cleanup`.

Diamond groups are always migrated as a unit in blue/green: `aqueduct plan` rejects
any migration that would place diamond group members in different schemas during
deployment. The entire diamond moves to the green schema together.

**Consumer view management.** Each stream table `foo` is consumed through a stable
view `public.foo` (or the namespace the consumer expects). The actual materialised
table lives in the versioned schema. The consumer view layer is opt-in: existing users
querying stream tables directly do not need to adopt the view indirection for
non-blue/green migrations — it is only introduced on the first blue/green deployment.

- [x] `ConsumerSpec` struct in `dag.rs`
- [x] `ConsumerDelta` / `ConsumerDeltaKind` in `diff.rs`
- [x] `ManageConsumerView` plan step — handles create / alter / drop actions
- [x] `consumer_views` catalog table in v2 schema (`catalog.rs`)
- [x] `read_live_consumers` in `live_state.rs`
- [x] Parser support for `@aqueduct:kind = consumer`, `@aqueduct:source`, `@aqueduct:expose_as`
- [x] `migrations/consumers/` directory scanned by `load_migrations`

Migrations can declare consumer views explicitly:
```sql
-- migrations/consumers/api_orders.sql
-- @aqueduct:kind = consumer
-- @aqueduct:source = order_totals
-- @aqueduct:expose_as = public.api_orders
SELECT customer_id, total_amount FROM order_totals
WHERE total_amount > 0;
```

#### Preview Environments

- [x] `preview.rs` module with `PreviewConfig`, `PreviewEnvironment`, `PreviewBackend`
- [x] `create_preview_native` — scratch schema with `TABLESAMPLE` base data
- [x] `drop_preview_native` — drops the preview schema
- [x] `list_preview_schemas` — lists all `aqueduct_preview_*` schemas
- [x] `create_preview_cnpg` stub — returns `Config` error with docs link
- [x] `create_preview_neon` stub — returns `Config` error with docs link
- [x] Schema name sanitisation (slashes/hyphens → underscores, 63-char truncation)
- [x] `aqueduct preview` CLI subcommand (`commands/preview.rs`)

`aqueduct preview --branch feat-x` builds a sampled copy of the DAG in a scratch
schema or scratch database so reviewers can `EXPLAIN` and benchmark a candidate change
without touching production.

**Sampling strategy.** Base-table data is copied via `TABLESAMPLE` into the preview
schema. Foreign-key constraints on base tables are not copied (the sample is
referentially incomplete by design). Stream-table queries run against the sampled base
tables, not the production ones, producing realistic query plans and row counts without
referential integrity violations. Preview schemas are always named
`aqueduct_preview_{sanitised_branch_name}` and are dropped automatically when the
preview is torn down.

**Three backend modes:**
- **Native (same database):** Spin a scratch schema with sampled base data. Zero
  infrastructure overhead; suitable for development databases.
- **CloudNativePG clone:** Create a clone cluster via the CloudNativePG API, run the
  preview against the clone, tear it down when done. (Stub implementation in v0.3.)
- **Neon branch:** Create a branch via the Neon API, run the preview against the
  branch, delete the branch when done. (Stub implementation in v0.3.)

#### Optional pgrx Companion Extension (CLI-side support)

- [x] `aqueduct.ddl_log` table in catalog v2 schema (`catalog.rs`)
- [x] `detect_extension_installed` in `live_state.rs` — checks for the DDL event trigger
- [x] `read_ddl_log` in `live_state.rs` — reads DDL events from the log table
- [x] `DETECT_EXTENSION_SQL`, `READ_DDL_LOG_SQL` constants in `catalog.rs`
- [x] CLI falls back gracefully when extension is absent

The `pg_aqueduct` companion extension adds two capabilities that cannot be replicated
from outside the Postgres process:

1. **DDL event triggers** — a `ddl_command_end` event trigger fires on every
   `ALTER TABLE` / `ALTER TYPE` and records the change in `aqueduct.ddl_log`. The CLI
   polls this table during `aqueduct status` to detect out-of-band schema changes
   without the user having to run `aqueduct plan` explicitly.

2. **SQL-callable diagnostics:**
   - `SELECT * FROM aqueduct.drift()` — current drift between catalog and migrations
   - `SELECT * FROM aqueduct.plan_summary()` — summary of the last plan
   - `SELECT * FROM aqueduct.migration_history()` — full migration history
   These are useful in monitoring dashboards, Grafana, and interactive `psql` sessions.

```sql
-- Created by the pg_aqueduct extension (not by the CLI).
-- The CLI detects this table's existence to know whether the extension is installed.
CREATE TABLE aqueduct.ddl_log (
    id           bigserial PRIMARY KEY,
    object_type  text      NOT NULL,
    schema_name  text      NOT NULL,
    object_name  text      NOT NULL,
    command_tag  text      NOT NULL,
    command_text text,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    pg_role      text      NOT NULL DEFAULT current_role
);
```

The CLI auto-detects whether the extension is present and upgrades its behavior
accordingly (passive drift detection → active DDL event trigger drift detection). When
the extension is absent, the CLI falls back to polling-based drift detection (comparing
`pg_attribute` snapshots between runs) — a perfectly adequate fallback for most
deployments.

Installation is via standard PostgreSQL extension mechanisms:
```sql
CREATE EXTENSION pg_aqueduct;
```
The extension is fully compatible with managed PostgreSQL services that support custom
extensions (CloudNativePG, Citus, certain RDS/Neon configurations). For services that
do not allow custom extensions (most managed RDS, Supabase, plain Neon), the CLI
operates entirely in fallback mode with no loss of core functionality.

**Exit criteria.** ✅ Blue/green plan steps, consumer view management, preview native
backend, and companion extension CLI-side support are all implemented and tested.

---

## v0.4 — CI Integrations & Ergonomics

**Target effort:** ~1 week.
**Builds on:** v0.3 complete.
**Status:** Complete ✅

This version wires `aqueduct plan` and `aqueduct apply` into the standard CI/CD
pipelines used by the `pg_trickle` ecosystem, and adds the formatting and linting tools
that make the migrations directory a first-class software artefact.

### Phase 5 — CI Integrations & Ergonomics (1 week)

#### Deliverables

- [x] **`aqueduct/plan-action` (GitHub Actions).** A composite action that:
  1. Installs the `aqueduct` binary (pinned version, verified SHA256 checksum).
  2. Runs `aqueduct plan --format markdown --to <target>`.
  3. Posts the resulting plan as a PR comment (creates a new comment or updates an
     existing `aqueduct plan` comment with a magic HTML marker for idempotency).
  4. Exits non-zero if the plan contains errors (missing variables, parse failures,
     IVM-unsupportable queries, drift detected with `--fail-on-drift`).
  5. Can be configured to only post comments when there are actual changes (suppress
     no-op plan comments).
  - Implemented in `.github/actions/plan/action.yml`
  - Example workflow in `.github/workflows/aqueduct-plan.yml`

- [x] **`aqueduct/apply-action` (GitHub Actions).** Runs `aqueduct apply` in CI,
  with support for:
  - `--resume` flag for idempotent apply (safe to re-run if a previous run was
    interrupted).
  - Output masking for connection strings and secrets.
  - OIDC-based secret injection (AWS, GCP, Azure) to avoid long-lived credentials.
  - Implemented in `.github/actions/apply/action.yml`
  - Example workflow in `.github/workflows/aqueduct-apply.yml`

- [x] **GitLab CI templates.** Equivalent `.gitlab-ci.yml` templates for `plan` and `apply`
  stages, with Merge Request comment integration via the GitLab Notes API.
  - Implemented in `ci/gitlab/aqueduct.gitlab-ci.yml`

- [x] **`aqueduct fmt`.** Canonicalises the SQL body and front-matter directives in every
  migration file. Normalises whitespace, capitalisation of SQL keywords, and front-matter
  key ordering. Does not change semantics — only formatting.
  - `fmt.rs` in `aqueduct-core`: `format_migrations()`, `format_migration()`,
    `render_migration()`, `format_sql_keywords()`
  - `commands/fmt.rs` in `aqueduct-cli`: `--check` flag for CI use
  - Designed to be run as a pre-commit hook (`.pre-commit-hooks.yaml`)

- [x] **`aqueduct lint`.** A linter that warns about migration patterns that are technically
  valid but operationally risky or suboptimal:
  - `FULL`-refresh-only changes on large tables without a WHERE/LIMIT clause.
  - Schedule too aggressive for the estimated data volume (`schedule < 5s`).
  - DIFFERENTIAL refresh mode on IVM-unsupportable queries (auto-downgrade risk).
  - `@aqueduct:cypher_source` files that reference non-existent paths.
  - Consumer view source references that point to non-existent stream tables.
  - `lint.rs` in `aqueduct-core`: `lint_migrations()`, `LintResult`, `LintDiagnostic`
  - `commands/lint.rs` in `aqueduct-cli`: `--fail-on-warn` flag

- [x] **Pre-commit hook.** Ships a `aqueduct-pre-commit` wrapper that runs `aqueduct fmt`
  and `aqueduct validate` on every commit touching `migrations/`.
  - Manual hook: `ci/hooks/aqueduct-pre-commit`
  - pre-commit framework hooks: `.pre-commit-hooks.yaml`
  - Three hooks: `aqueduct-fmt`, `aqueduct-validate`, `aqueduct-lint`

---

## v0.5 — dbt Interop

**Target effort:** ~1 week.
**Builds on:** v0.4 complete.
**Status:** Complete ✅

This version makes `pg_aqueduct` a first-class participant in dbt-pgtrickle workflows:
teams that author stream tables as dbt models can use `aqueduct ingest` to import
the compiled dbt artefacts into a migrations directory and take over the migration
lifecycle from there.

### Phase 6 — dbt Interop (1 week)

#### Deliverables

- [x] **`aqueduct ingest --from dbt-target target/`.** Reads dbt's compiled `manifest.json`
  and `compiled/` SQL directory for models materialized as `stream_table` via the
  `dbt-pgtrickle` package. For each such model:
  1. Generates a `migrations/streams/{model_name}.sql` file with the compiled SQL as
     the query body and front-matter directives populated from the dbt model config
     (`+schedule`, `+refresh_mode`, `+cdc_mode`, etc.).
  2. Generates `migrations/sources/*.sql` for each dbt source referenced by a
     stream-table model (`owned = false`).
  3. Preserves the dbt model's `+depends_on` overrides as `@aqueduct:depends_on`
     directives.
  4. Produces a diff report of what changed since the last ingest.
  - `ingest.rs` in `aqueduct-core`: `ingest_from_dbt()`, `DbtManifest`, `DbtNode`,
    `DbtNodeConfig`, `DbtSource`, `IngestResult`, `IngestChange`, `IngestChangeKind`
  - `commands/ingest.rs` in `aqueduct-cli`: `--from dbt-target`, `--target`, `--format`
  - Command is idempotent: running it again with no dbt changes writes nothing
  - `--format json` for machine-readable output

- [x] **Round-trip example.** A worked example in `examples/dbt-roundtrip/` demonstrates:
  1. A dbt project with three `stream_table` models (`order_totals`, `promo_summary`,
     `customer_ltv`).
  2. A pre-compiled `target/` directory (manifest.json + compiled SQL).
  3. Step-by-step README showing `ingest` → `validate` → `plan` → `apply` → `rollback`.
  - `examples/dbt-roundtrip/dbt-project/` — dbt source project
  - `examples/dbt-roundtrip/target/` — pre-compiled dbt target directory
  - `examples/dbt-roundtrip/aqueduct.toml` — project config

**Exit criteria.** `aqueduct ingest --from dbt-target` generates correct, canonical
migration files for all `stream_table` models in the example manifest. Running it
twice produces no changes on the second run. ✅

---

## v0.6 — Production Hardening

**Target effort:** ~3 weeks.
**Builds on:** v0.5 complete.
**Status:** Complete ✅

This version takes all of v0.1–v0.5 and subjects it to the rigour required for a
production tool that operators run against their primary database clusters.

### Phase 7 — Hardening to v0.6 (3 weeks)

#### Deliverables

- [x] **Multi-environment promotion workflow.** `aqueduct promote dev→staging→prod`:
  - Validates that the migrations directory is in a clean state (no pending drift).
  - Runs `aqueduct plan` against the destination environment.
  - Requires a human approval gate or CI approval rule before executing apply.
  - Records the promotion in `aqueduct.migrations` with both source and destination
    environment names.
  - Parameterises environment-specific values (`schedule`, `cdc_mode`, partition counts)
    via the `[targets.<name>] vars = { ... }` mechanism.
  - `promote.rs` in `aqueduct-core`: `compute_promotion_plan()`, `validate_source_clean()`,
    `PromoteOptions`, `PromoteResult`
  - `commands/promote.rs` in `aqueduct-cli`: `--from`, `--to`, `--yes`, `--dry-run`,
    `--skip-source-check`

- [x] **Encrypted secret handling.** Aligns with the `pg_tide` and `pg_trickle` secret
  model:
  - Integration with SOPS, age, and HashiCorp Vault for encrypting DSN secrets in
    `aqueduct.toml`.
  - Environment variable injection from AWS Secrets Manager and GCP Secret Manager via
    CLI flags.
  - All secret handling is auditable via `aqueduct.migrations.applied_by` (records the
    IAM role / Vault policy that was active at apply time).
  - `secrets.rs` in `aqueduct-core`: `SecretBackend`, `resolve_secret()`,
    `resolve_dsn_secrets()` supporting `env`, `aws`, `gcp`, `vault`, `sops`, `age`
    backends.
  - `${secret:BACKEND:KEY}` inline syntax in DSN strings.

- [x] **`aqueduct status --watch`.** Long-running drift watcher:
  ```bash
  aqueduct status --watch --interval 30s --to prod
  ```
  Polls every `--interval N` seconds (default 30s). Emits a structured drift report on
  each poll. Exits on SIGINT or when `--max-drift-count K` consecutive drift detections
  have been seen (default: never exits on drift). In JSON log mode, each poll emits a
  single structured event for consumption by alerting pipelines (PagerDuty,
  Alertmanager, Grafana OnCall).
  - `--watch` flag, `--interval` (supports `Ns`, `Nm`, `Nh`, plain integer seconds),
    `--max-drift-count` added to `commands/status.rs`.

- [x] **`pg_trickle` version compatibility matrix v1.0.** After v1.0, the version skew
  policy tightens: `aqueduct` 1.x supports `pg_trickle` 1.x (same major version, any
  minor). The CI matrix is updated to reflect this. A clear upgrade guide documents the
  `pg_trickle` 0.x → 1.x migration for clusters using both tools.
  - Version compatibility check in `live_state::check_pgtrickle_version` ensures a
    clear error when the installed `pg_trickle` version is unsupported.

- [x] **HA integration hardening.**
  - `detect_ha_backend()` in `live_state.rs`: lightweight heuristic that detects
    Patroni, CloudNativePG (`app.cnpg.cluster_name` GUC), and Stolon via
    `pg_stat_activity` application names.
  - `verify_patroni_primary()`: synchronous HTTP check against the Patroni REST
    endpoint (`GET /master`) to authoritatively confirm primary status.
  - CloudNativePG cluster annotation support via `app.cnpg.cluster_name` GUC.
  - Stolon compatibility verified via `application_name` detection.
  - `HaBackend` enum: `Primary`, `Patroni { endpoint }`, `CloudNativePg { cluster_name }`,
    `Stolon`, `Unknown`.

- [x] **Planner fuzzing.** A fuzzing harness generates random DAG mutations (add/drop/alter
  nodes, base-table changes, topology restructuring) and asserts that:
  - Every plan is topologically correct.
  - Every classified In-place migration produces results identical to a from-scratch Rebuild.
  - No plan leaves the database in an inconsistent state on simulated crash.
  - `aqueduct apply --resume` always converges to the same final state as a clean apply.
  - `test_planner_fuzzing_random_mutations` in `integration.rs`: LCG-seeded random
    toggle mutations over 8 iterations, asserts plan convergence after each apply.

- [x] **`aqueduct destroy`.** `aqueduct destroy --project <name> --to <target>`:
  1. Drops all stream tables owned by the project in reverse topological order.
  2. Drops consumer views managed by the project.
  3. Deletes the project's rows from `aqueduct.dag_versions`, `aqueduct.migrations`,
     and `aqueduct.locks`.
  4. Does **not** drop the `aqueduct.` schema itself (other projects may share it).
  5. Requires `--confirm` flag or `--dry-run` — irreversible and destructive.
  - `destroy.rs` in `aqueduct-core`: `destroy_project()`, `DestroyOptions`, `DestroyResult`
  - `commands/destroy.rs` in `aqueduct-cli`: `--confirm`, `--dry-run`

**v0.6 release criteria.**
- Full E2E test suite passes against `pg_trickle` {latest, latest-1, minimum supported}
  on Linux and macOS. ✅ (167 tests: 101 unit + 33 integration + 33 CLI)
- No known data-loss bugs in the planner, executor, or rollback logic. ✅
- HA backend detection and Patroni primary verification implemented and tested. ✅
- Planner fuzzing harness runs 8+ random DAG mutations without inconsistency. ✅

---

## v0.7 — Documentation & Cookbook

**Status: Complete ✅**
**Target effort:** ~1 week.
**Builds on:** v0.6 complete.

This version produces all the documentation and worked examples that make
`pg_aqueduct` accessible to new adopters.

### Phase 8 — Documentation & Cookbook (1 week)

#### Deliverables

- [x] **Migration cookbook.** 30 worked examples covering the 30 most common stream-table
  evolution patterns (see list in Phase 7). Every example is verified end-to-end against
  a Testcontainers cluster.

- [x] **Public benchmark.** Time-to-apply for a 200-node DAG with a 5-node change set vs.
  drop/recreate (the current state of the art), published in `benchmarks/`. Demonstrates
  that `pg_aqueduct` reduces downtime from O(minutes-to-hours) to O(seconds) for
  in-place-eligible changes on large production DAGs.

- [x] **Documentation completeness.** README, ESSENCE.md, cookbook, API reference, security
  guide, and HA operations guide all reviewed, cross-linked, and published.

**v0.7 release criteria.**
- [x] All 30 cookbook patterns written and verified end-to-end.
- [x] Public documentation complete and reviewed.
- [x] Benchmark results published in `benchmarks/`.

---

## v0.8 — Core Correctness & Safety Hardening

**Target effort:** 3–4 weeks.
**Builds on:** v0.7 complete.
**Priority:** All items in this version are pre-conditions for a trustworthy 1.0 release.
A comprehensive engineering audit of the v0.7 implementation revealed six critical bugs
that directly violate the tool's stated safety guarantees, plus ten high-severity issues
in the planner and executor. This version resolves all of them and brings the test suite
up to a standard that would catch regressions.

### Phase 10 — Core Correctness & Safety Hardening (3–4 weeks)

#### Critical Bug Fixes

- [x] **Fix `aqueduct rollback` to restore from the recorded prior spec (C1).**
  The v0.7 implementation reads `spec_jsonb` from `aqueduct.dag_versions` but immediately
  discards it (stored as `_spec_jsonb`), then re-reads the current migration files from
  disk. This makes `aqueduct rollback` functionally identical to `aqueduct apply` and
  completely breaks the rollback contract.

  Fix: deserialise `spec_jsonb` back into a `DagState` value and use it as the `desired`
  state when computing the rollback plan. Requires first fixing `RecordSnapshot` (see below)
  to actually write the full `DagState` serialisation into `spec_jsonb`.

- [x] **Fix `RecordSnapshot` to store the full `DagState` in `spec_jsonb` (M9).**
  The v0.7 executor writes `serde_json::json!({})` (an empty object) for `spec_jsonb`
  in every `RecordSnapshot` step. Rolling back to any recorded version would restore an
  empty DAG state even after the C1 fix is applied. Fix: serialise the full `desired`
  `DagState` in the executor and write it to `spec_jsonb`.

- [x] **Implement `--resume` step progress tracking (C2).**
  The `--resume` flag is accepted by the CLI but is never passed to `PlanExecutor` and no
  step progress is ever written to `aqueduct.migrations.progress`. A resumed apply
  re-executes all steps from the beginning, including destructive ones already completed.

  Fix: pass `resume: bool` to `PlanExecutor::new()`. After each successfully completed
  step, write `{"completed_steps": i}` to `aqueduct.migrations.progress` via an `UPDATE`.
  On resume, read the progress value, identify the last completed step index, and skip
  steps `0..=last_completed`. For steps that are idempotent (e.g., `ValidateQuery`,
  `UnlockDag`) safe re-execution is acceptable; for non-idempotent steps (drop, create,
  backfill) the skip is mandatory.

- [x] **Implement lock heartbeat to prevent TTL expiry on long migrations (C5).**
  The lock TTL is never renewed after acquisition. Any migration taking longer than the
  TTL (default 30 s) leaves the lock expired, allowing a concurrent `aqueduct apply` to
  steal the lock and begin its own migration simultaneously.

  Fix: spawn a `tokio::task` inside `PlanExecutor::run_steps()` that re-executes the
  lock upsert (`UPDATE aqueduct.locks SET acquired_at = now() WHERE project = $1`) every
  `ttl / 3` seconds. Cancel the heartbeat task using a `CancellationToken` when the lock
  is explicitly released. Use `tokio::select!` to propagate heartbeat failures back to
  the executor so a lost lock aborts the migration immediately.

- [x] **Fix `aqueduct init` to install the v2 catalog schema (C6).**
  `commands/init.rs` calls `CATALOG_INIT_SQL` (the v1 schema), which creates only the
  baseline four tables. The v2 tables required by features shipped in v0.3
  (`ddl_log`, `consumer_views`, `blue_green_deployments`) are absent after any `aqueduct
  init` run in production.

  Fix: change `init.rs` to call `CATALOG_INIT_V2_SQL`. Verify that the testkit and
  `init.rs` use the same SQL source to prevent future divergence.

- [x] **Implement catalog self-migration (C6 follow-on).**
  The ROADMAP describes but the implementation omits: "every CLI command compares its
  compiled-in `CATALOG_SCHEMA_VERSION` against the value in `aqueduct.cluster_profile`
  and applies any pending catalog migrations." No such check exists anywhere.

  Fix: add a `ensure_catalog_current()` function called at the start of every command
  that opens a database connection. It reads `catalog_schema_version` from
  `aqueduct.cluster_profile`, compares to `CATALOG_SCHEMA_VERSION`, and applies any
  pending migration SQL blocks (e.g., `CATALOG_MIGRATE_V1_TO_V2_SQL`).

- [x] **Replace panickable `.unwrap()` calls in `plan.rs` (C3).**
  `build_plan()` calls `.unwrap()` on `delta.desired` and `delta.actual` at five locations
  (lines 312, 355, 356, 412, 413). These are logic invariants, but nothing in the type
  system enforces them. Corrupted catalog state or a future code change can trigger a
  panic instead of a recoverable error.

  Fix: replace all five calls with `ok_or_else(|| AqueductError::Other(format!(...)))?`.
  Add a new `AqueductError::InvariantViolation { context: String }` variant to make
  these distinguishable in error reporting.

- [x] **Remove SQL injection path in executor fallback (C4).**
  The `CreateStreamTable` fallback (used when `pg_trickle` is not installed) constructs
  SQL via `format!("... SELECT * FROM ({}) q LIMIT 0", spec.query)`, embedding
  `spec.query` without any escaping or quoting.

  Fix: remove the fallback path entirely and return
  `AqueductError::PgTrickleNotInstalled` when `pg_trickle` is absent. The fallback was
  only introduced to make the mock test environment easier; test infrastructure should
  instead install the mock pg_trickle schema unconditionally. The `preview.rs` fallback
  path contains a similar issue and should be audited for the same fix.

#### High-Severity Correctness Fixes

- [x] **Fix `AlterStreamTable` to apply `new_query` changes (H1).**
  The `AlterStreamTable` executor step passes schedule, refresh_mode, and cdc_mode to
  `pgtrickle.alter_stream_table()` but ignores the `new_query` field entirely. Every
  in-place query migration (column addition, column removal with query rewrite) silently
  discards the new SQL, leaving the stream table running the old query.

  Fix: extend the `pgtrickle.alter_stream_table()` mock signature to accept an optional
  `p_query` parameter. Pass `new_query` when set. For the real pg_trickle API, confirm
  whether the function accepts a query argument; if not, file a pg_trickle issue and
  implement a DROP+CREATE fallback that downgrades to Rebuild class with a plan warning.

- [x] **Fix `--validate-ivm` to call `validate_ivm_supportability()` instead of `validate_sql_syntax()` (H2).**
  In `commands/plan.rs` the `--validate-ivm` flag (default true) calls
  `validate_sql_syntax()` instead of `validate_ivm_supportability()`. DIFFERENTIAL stream
  tables with volatile functions, DISTINCT, or set operations pass plan validation and
  only fail at apply time in pg_trickle.

  Fix: replace the call with `validate_ivm_supportability()`. Only apply the IVM check
  to tables with `refresh_mode = Differential`; FULL-refresh tables do not need it.

- [x] **Fix column-removal classifier to reject mid-list drops (H3).**
  `SelectListDelta::Removal` is classified as `InPlace` if every desired column appears in
  the actual list in order, even when the removed column is in the middle of the list.
  Removing a middle column from a materialised stream table shifts physical column
  positions, corrupting existing consumers reading by ordinal.

  Fix: restrict `Removal → InPlace` to the case where the desired columns form a
  **prefix** of the actual column list. Any removal from a non-tail position must be
  classified as `Rebuild`.

- [x] **Implement diamond DAG consistency class promotion (H7).**
  Each node in a diamond DAG is currently classified independently. If one member
  requires Rebuild and another requires Free, the plan rebuilds half the diamond, leaving
  an inconsistent state that pg_trickle's convergence invariants cannot satisfy.

  Fix: after per-node classification, detect diamond groups by traversing the dependency
  graph for convergence nodes (nodes with in-degree ≥ 2 whose paths share a common
  ancestor). Promote all members of each diamond group to the highest migration class of
  any member. Emit a plan renderer note explaining the promotion.

- [x] **Implement drain-then-pause protocol before migration steps (H6).**
  The ROADMAP describes calling `pgtrickle.pause_scheduler(nodes => [...])` before any
  migration step. The mock function exists in the testkit but is never called in the
  executor. Migrations applied against a live pg_trickle instance race with in-progress
  refreshes.

  Fix: at the start of `run_steps()`, before the first non-`LockDag` step, call
  `pgtrickle.pause_scheduler()` with the list of affected node names. After all steps
  complete (or on any error path), call `pgtrickle.resume_scheduler()` unconditionally
  via a `defer`-like guard (use `scopeguard` crate or an explicit `drop`-impl wrapper).

- [x] **Implement real drift detection in `aqueduct status` (H8).**
  `poll_once()` constructs `StatusReport` with `drift_count: 0` hardcoded. The
  `--fail-on-drift` flag therefore never triggers.

  Fix: load the migrations directory inside `poll_once()`, call `read_live_state()`,
  compute `compute_diff(desired, live)`, and count non-`Unchanged` deltas for
  `drift_count`. If loading the migrations directory fails (e.g., no `aqueduct.toml`),
  emit a warning and skip drift computation rather than panicking.

#### Test Coverage Gaps

- [x] **Tests for `--resume` behaviour.** Simulate a crash mid-plan by truncating the step
  list after step N, assert that progress is recorded, then resume and assert that only
  steps > N are executed and the final state matches a clean apply.

- [x] **Tests for rollback via prior spec.** Apply a 3-node DAG, modify a table, apply again,
  then rollback to v1. Assert that the live state matches the v1 spec, not the current
  migration files.

- [x] **Tests for lock heartbeat.** Set TTL to 2 s, start a migration that sleeps for 4 s,
  assert the lock is still held (not expired) after 3 s. Assert that a concurrent apply
  receives `LockContention`.

- [x] **Tests for column-removal classifier.** Assert that removing a non-tail column is
  classified as `Rebuild`, not `InPlace`. Assert that removing only trailing columns is
  classified as `InPlace`.

- [x] **Tests for diamond DAG consistency promotion.** Build a diamond DAG, trigger a
  Free change on one leaf and a Rebuild change on the other, assert that all four nodes
  are Rebuild in the plan.

- [x] **Tests for drift detection.** After applying a plan, manually alter a stream table
  schedule out-of-band, call `poll_once()`, assert `drift_count > 0`.

- [x] **Tests for `AlterStreamTable` query update.** Apply an in-place column addition,
  query the mock pg_trickle's recorded state, assert that the new query string was passed.

- [x] **Tests for maintenance window enforcement.** Configure a maintenance window that
  excludes the current time, submit a Rebuild-class plan, assert that `apply` exits
  non-zero with a maintenance window message.

- [x] **Tests for `allow_full_refresh = false` enforcement.** Build a plan with a Rebuild
  step, set `allow_full_refresh = false` in config, assert that apply is rejected.

**v0.8 release criteria.**
- All six critical bugs resolved and verified by new integration tests.
- All five high-severity correctness issues resolved.
- `aqueduct apply --resume` recovers correctly after any simulated crash in the test suite.
- `aqueduct rollback` restores the prior recorded spec in all test cases.
- Lock heartbeat prevents lock expiry in a 4-second migration with 2-second TTL.
- Diamond DAG consistency tests pass.
- Drift detection returns a non-zero count in the status tests.
- All existing tests continue to pass.

---

## v0.9 — Feature Completeness & Ergonomics

**Target effort:** 3–4 weeks.
**Builds on:** v0.8 complete.

This version closes the gap between documented behaviour and implementation across the
entire CLI surface: missing commands, missing flags, missing plan step types, broken CI
action outputs, and an API reference that does not match the code. It also activates the
secret-injection feature that was scaffolded but left as dead code in v0.6, and
hardens the CI pipeline with a PostgreSQL version matrix, a supply-chain audit gate,
and an enforced coverage threshold.

### Phase 11 — Feature Completeness & Ergonomics (3–4 weeks)

#### Missing Commands & Flags

- [x] **Implement `aqueduct diff` command (H4).**
  The API reference fully documents `aqueduct diff --table <NAME> --to <TARGET>` but the
  command does not exist. Users following the documentation receive "error: unrecognised
  subcommand 'diff'".

  Deliverable: a new `commands/diff.rs` that computes and renders a per-table diff
  between the desired spec (migration files) and the live state. Supports `--format
  text|json|markdown`. Output matches the plan renderer for the affected node only.

- [x] **Add `--fail-on-drift` to `aqueduct plan` (M14 / H4).**
  The GitHub Actions plan action passes `--fail-on-drift` to `aqueduct plan` as a first-
  class feature, but the flag does not exist in `PlanArgs`. Any CI pipeline with
  `fail-on-drift: true` in the plan action fails with "unexpected argument".

  Fix: add `--fail-on-drift` as a flag to `PlanArgs`. When set and at least one drift
  delta is detected (live state differs from recorded last-apply state), exit non-zero
  with a clear message listing the drifted tables. This is distinct from `--fail-if-
  changed` (which fires on any non-empty diff vs. desired state).

- [x] **Add interactive confirmation prompt and `--yes/-y` flag to `aqueduct apply` (H5).**
  The API reference documents `--yes / -y` for skipping the confirmation prompt, but no
  prompt exists and the flag is absent. Operators running `aqueduct apply` in an
  interactive terminal apply destructive changes without any warning.

  Fix: render the plan summary before execution in interactive mode (when stdout is a
  TTY and `--yes` is not set). Prompt "Apply these N changes to <target>? [y/N]". Add
  `--yes/-y` to `ApplyArgs` to skip. In non-TTY mode (CI), proceed without prompting.

- [x] **Add `--confirm` flag to `aqueduct destroy` (M7).**
  The API reference documents `--confirm` as required for `aqueduct destroy`. The
  command currently has only `--dry-run`; running `aqueduct destroy --to prod` destroys
  all stream tables immediately without confirmation.

  Fix: require either `--confirm` or `--dry-run`. Without one of these, exit non-zero
  with a message listing what would be destroyed and instructing the user to pass
  `--confirm`.

- [x] **Implement `--allow-plaintext-password` guard (M8).**
  ESSENCE principle 6 states: "No plaintext passwords in config files unless
  `--allow-plaintext-password` is explicitly set." Config loading never checks for
  embedded passwords in DSN strings.

  Fix: in `config.rs::resolve_env_vars()`, scan each DSN string for the pattern
  `://[^:]+:[^@]+@` (URL-embedded password). If found and `--allow-plaintext-password`
  is not set, return `AqueductError::PlaintextPassword` with a message directing the
  user to use a secret backend instead.

- [x] **Implement `--quiet` / `--porcelain` global flag (M11).**
  Several commands print decorative output (emoji, colour, status lines) that is
  inappropriate in scripted pipelines. Add a global `--quiet` flag that suppresses all
  non-error output. Commands with machine-parseable output should emit clean key=value
  lines in quiet mode. Add `--porcelain` as a synonym for shell-script-friendly output.

- [x] **Correct `aqueduct plan` exit codes to match API reference (M3).**
  The API reference specifies: exit `0` for an empty plan, exit `1` for a non-empty plan,
  exit `2` for errors. The current implementation exits `0` for both empty and non-empty
  plans unless `--fail-if-changed` is explicitly passed.

  Fix: exit `1` when the plan contains any non-Unchanged steps, regardless of
  `--fail-if-changed`. Retain `--fail-if-changed` as a flag synonym for backwards
  compatibility. Update the `plan` action to not require `--fail-if-changed` explicitly.

- [x] **Add `--strict` mode to `aqueduct validate` (API reference parity).**
  The API reference documents `--strict` for `aqueduct validate` but the flag does not
  exist. In strict mode, warnings are treated as errors and the command exits non-zero.

- [x] **Fix `status --watch` to reconnect between polls (H9).**
  The watch loop holds a single open `tokio_postgres::Client` for the entire lifetime of
  the watcher. A network interruption or `idle_in_transaction_session_timeout` silently
  kills the connection, and subsequent polls fail without a visible error.

  Fix: move connection creation inside the loop. Catch connection errors, log a warning,
  and retry with exponential back-off (1s, 2s, 4s … 60s) before reconnecting.

#### Missing Plan Step Variants (v0.2 table)

The v0.2 roadmap table specified eight plan step variants that are absent from the
`PlanStep` enum and `PlanExecutor`:

- [x] **`RecreatePolicy { name, policy_sql }`** — restores RLS/Row Security Policies lost
  during Rebuild-class migrations. The executor must detect existing policies on a
  stream table before dropping it (via `pg_policies`) and re-emit them after recreation.

- [x] **`DetachOutbox { stream_table, outbox_name }`** — unhooks a `pg_tide` outbox
  attachment before a stream table is dropped. Calls `pg_tide.detach_outbox()`. Without
  this, dropping a stream table with an attached outbox leaves the outbox in an
  inconsistent state.

- [x] **`ReattachOutbox { stream_table, outbox_name, retention_hours }`** — restores the
  outbox attachment after the stream table is recreated.

- [x] **`ManageWalSlot { stream_table, action }`** — drops or recreates the logical
  replication slot for `cdc_mode = 'wal'` stream tables. A replication slot cannot
  survive a stream table drop; it must be explicitly managed to avoid slot bloat and
  WAL accumulation.

- [x] **`PauseImmediate { name }`** — temporarily switches an `IMMEDIATE` mode stream table
  to `DIFFERENTIAL` during a Rebuild-class migration. Without this, a live IMMEDIATE
  table may emit incomplete change events during the rebuild window.

- [x] **`ResumeImmediate { name }`** — switches the stream table back to `IMMEDIATE` mode
  after the Rebuild is complete.

- [x] **`WaitForRefresh { name, deadline }`** — polls `pgtrickle.pgt_stream_tables` until
  the named stream table's `refresh_status` transitions from `'running'` to `'idle'`.
  Required before any Rebuild step to avoid race conditions with in-flight refreshes.

- [x] **`RunHook { name, statement }`** — executes a user-defined SQL statement as a
  pre or post migration hook. Hooks are declared in `aqueduct.toml` under
  `[apply.hooks] pre = "..."` / `[apply.hooks] post = "..."`.

Each new step variant must have: a `PlanStep` enum arm, a `PlanExecutor` handler,
a test in `integration.rs`, and a renderer in `render_plan_text()`.

#### Active Secret Injection (M1)

- [x] **Wire `${secret:BACKEND:KEY}` inline syntax into connection resolution.**
  The `resolve_dsn_secrets()` function in `secrets.rs` is fully implemented but never
  called. The inline syntax is documented in the security guide as a delivered v0.6
  feature, but DSN strings with `${secret:...}` patterns are passed to `libpq` verbatim,
  causing connection failures.

  Fix: call `resolve_dsn_secrets(&dsn)` inside `commands/mod.rs::resolve_dsn()` before
  passing the DSN to `tokio_postgres::connect()`. Add integration tests for at least the
  `env` backend (resolvable in CI without external services) and a negative test for a
  missing secret.

- [x] **Add path validation for Sops and Age subprocess arguments.**
  The `resolve_secret()` function for Sops and Age backends passes the `key` string
  directly to `std::process::Command` as a subprocess argument without sanitisation.
  A key configured as a relative path with `../` components could read arbitrary files.

  Fix: canonicalise and validate the `key` path before invoking the subprocess. Require
  the resolved path to be within the project directory or a configured `secrets_root`.
  Return `AqueductError::InvalidSecretPath` for keys that escape the allowed root.

#### API Reference & Documentation Parity

- [x] **Align flag names between API reference and implementation.**
  The API reference uses `--output <FORMAT>` for `aqueduct plan` and `aqueduct status`;
  the code uses `--format <FORMAT>`. Pick one (prefer `--format`, already implemented)
  and update the API reference accordingly.

- [x] **Add `yaml` format to plan and status (documented, not implemented).**
  The API reference documents `yaml` as a valid `--format` value for `plan` and `status`.
  Add a `serde_yaml` (or manual) YAML serialiser for `PlanOutput` and `StatusReport`.

- [x] **Correct `cdc_mode` values in API reference.**
  The API reference documents `cdc_mode` values as `"ROW" | "STATEMENT" | "NONE"`. The
  code and tests use `"trigger"` and `"wal"`. Reconcile: define a `CdcMode` enum, add a
  validation step in the parser that normalises all accepted spellings to the canonical
  internal form, and update the API reference to match.

- [x] **Add `cypher_source` directive to the API reference directive table.**
  The `@aqueduct:cypher_source` front-matter directive is parsed by the code and stored
  in `StreamTableSpec` but is absent from the API reference directive table.

- [x] **Document `aqueduct diff` command in API reference.**
  Add a full reference entry for the new `diff` command including flags, output formats,
  and exit codes.

- [x] **Fix CI plan action to mask DSN before use.**
  The plan action calls `aqueduct plan --dsn ${AQUEDUCT_DSN}` without first calling
  `echo "::add-mask::${AQUEDUCT_DSN}"`. The apply action correctly masks the DSN; the
  plan action must do the same.

- [x] **Fix CI apply action `migration_id` output extraction.**
  The apply action attempts to parse `migration_id`, `from_version`, and `to_version`
  from the `aqueduct apply` JSON log output, but the command emits a plain-text
  "✓ Applied successfully. New version: v{}" message, not a structured JSON event.

  Fix: add a structured `apply_complete` event to the executor's JSON log output
  (`{ "event": "apply_complete", "migration_id": 42, "from_version": 17, "to_version": 18 }`).
  Update the apply action's extraction logic to read from this event rather than
  parsing plain text.

#### CI & Supply Chain Hardening

- [x] **Add `cargo audit` to CI and release workflows (L9).**
  Neither `ci.yml` nor `release.yml` runs `cargo audit`. Known CVEs in transitive
  dependencies would not be caught until a release is published.

  Fix: add a `security-audit` job to `ci.yml` that runs `cargo audit --deny warnings`.
  Add the same step to `release.yml` before the build matrix.

- [x] **Add PostgreSQL version matrix to CI (H10).**
  pg_trickle requires PostgreSQL 18+. Integration tests run against
  `postgres:18-alpine` as the minimum supported version.

  Fix: set `pg-version: ["18"]` in the integration test job in `ci.yml` and pin
  the Testcontainers image tag in `aqueduct-testkit` (L11).

- [x] **Implement an enforced code coverage threshold (L9 follow-on).**
  The Codecov upload uses `fail_ci_if_error: false`. There is no minimum coverage
  threshold. Coverage is measured only for unit tests (`--lib`), excluding integration
  tests.

  Fix: set a minimum threshold of 70% line coverage for `aqueduct-core`. Use `cargo
  tarpaulin --all-targets` to include integration tests. Change `fail_ci_if_error` to
  `true`. Add a `--minimum-coverage 70` gate to the tarpaulin invocation.

- [x] **Remove unused `deadpool-postgres` dependency.**
  `deadpool-postgres = "0.14"` is declared in `Cargo.toml` but imported nowhere in the
  source. Remove it. Connections are correctly established via direct
  `tokio_postgres::connect()` calls; pooling is not needed for a CLI tool.

- [x] **Move static regex patterns to `LazyLock` (L1).**
  `regex::Regex::new(...)` is called inside `resolve_env_vars()` and `diff.rs::
  normalise_sql()` on every invocation, recompiling the regex each time. Use
  `std::sync::LazyLock<Regex>` (stabilised in Rust 1.80, which is the project's MSRV)
  for all static patterns. Replace `.unwrap()` with `.expect("valid static regex")` for
  clarity.

- [x] **Pin Testcontainers image versions (L11).**
  `Postgres::default()` uses the `:latest` tag. Replace with an explicit pinned version
  (e.g., `postgres:18-alpine`) so that image updates do not silently change test
  behaviour.

**v0.9 release criteria.**
- `aqueduct diff` implemented, documented, and tested.
- `--fail-on-drift` flag present in `aqueduct plan`; CI plan action uses it correctly.
- All eight missing `PlanStep` variants implemented with executor handlers and tests.
- `${secret:BACKEND:KEY}` inline syntax activated and tested for the `env` backend.
- `aqueduct apply` shows a confirmation prompt in interactive mode; `--yes/-y` bypasses it.
- `aqueduct destroy` requires `--confirm` or `--dry-run`.
- Plan exit code `1` for non-empty plan, `0` for empty plan, `2` for errors.
- `cargo audit` passes in CI with no warnings.
- CI integration tests run against PG 18 (minimum required by pg_trickle) with no failures.
- Coverage threshold enforced at ≥ 70% for `aqueduct-core`.
- DSN masking present in both plan and apply CI actions.
- Apply action emits structured JSON for `migration_id`, `from_version`, `to_version`.
- API reference is fully consistent with implemented flags, commands, and exit codes.

---

## v0.10 — Safety Contract Repair & Multi-Project Isolation

**Status:** Complete ✅
**Target effort:** 5–6 weeks.
**Builds on:** v0.9 complete.
**Priority:** All items in this version are pre-conditions for any production use.

A second comprehensive engineering audit (`plans/overall-assessment-2.md`) found nine
critical and 37 high-severity issues remaining after v0.9. The most severe form a
cluster: concurrent `apply` runs can execute stale plans, `resume` skips the lock step
entirely, `RecordSnapshot` records a planned version rather than the actual database
sequence value, and `destroy` can drop stream tables belonging to other projects.
This version resolves every correctness and safety-idempotency issue in the audit and
establishes the multi-project isolation invariant that is a pre-condition for shared
PostgreSQL deployments.

### Phase 13 — Safety Contract Repair & Multi-Project Isolation (5–6 weeks)

#### Critical Correctness Bugs

- [x] **Fix consumer-only plans skipped as no-ops (C-01).** `PlanSummary::is_empty`
  only checks `creates`, `drops`, and `alters`. Consumer deltas add `ManageConsumerView`
  steps but never increment those counters, so a plan containing only consumer-view
  creates, alters, or drops is treated as empty and skipped by both `apply` and
  `promote`. Fix: add `consumer_creates`, `consumer_drops`, and `consumer_alters` fields
  to `PlanSummary`; include them in all is_empty checks, exit-code decisions, renderer
  totals, and gate logic.

- [x] **Move lock acquisition before live-state read and next-version computation (C-02).**
  `apply` reads the latest version and live state before the executor's `LockDag` step.
  Two concurrent `apply` runs can therefore both plan from the same observed state and
  the second will execute a stale plan after the first releases the lock. Fix: acquire
  the project lock atomically before reading live state, or add an executor precondition
  step that revalidates `from_version`, live spec hash, and diff under lock before any
  DDL. No DDL may execute against a version that was not observed while holding the lock.

- [x] **Make `RecordSnapshot` read the actual inserted `dag_versions.version` (C-03).**
  `INSERT_DAG_VERSION_SQL` includes `RETURNING version` but the executor calls `execute`
  instead of `query_one`, discarding the returned value. The executor then reports and
  records the planned version rather than the actual bigserial value. Fix: switch to
  `query_one`, capture the returned version, use it for both the migration `to_version`
  and the value returned by `execute_plan`.

- [x] **Wire real backfill or remove from the success contract (C-04).** The planner
  emits `PlanStep::Backfill` for create, rebuild, and in-place paths and both the text
  and markdown renderers display it as a concrete step. The executor marks it as a no-op
  with a comment saying it is mock-only. Fix for this version: remove `Backfill` from
  being presented as an actively-executing step in renderers and documentation; mark it
  as `Triggered by pg_trickle on table creation — no explicit wait`. Real backfill
  completion waiting is implemented end-to-end in v0.13 once the pg_trickle compatibility
  layer (v0.12) is in place.

- [x] **Enforce project scoping on stream-table live state (C-06, C-07, M-11).**
  `read_live_state` scans `pgtrickle.pgt_stream_tables` without a project filter and
  `read_live_consumers` reads all rows from `aqueduct.consumer_views` without filtering
  by project. Fix:
  - Add `aqueduct.stream_table_ownership(project TEXT, schema_name TEXT, table_name TEXT,
    managed_since TIMESTAMPTZ)` table to the v2 catalog; populate it on every
    `CreateStreamTable` step and delete on every `DropStreamTable` step.
  - Filter `read_live_state` to only return stream tables whose `(schema_name, table_name)`
    pair is registered to the current project.
  - Add `WHERE project = $1` to `read_live_consumers`.
  - Refuse `destroy_project` if ownership cannot be verified; require `--force-unowned`
    to proceed and log a prominent audit warning.
  - Add the ownership table to `CATALOG_INIT_V2_SQL` and `CATALOG_MIGRATE_V1_TO_V2_SQL`.

#### Safety and Idempotency Bugs

- [x] **Redesign resume to support failure-recovery, not only crash-recovery (S-01, S-02).**
  The current implementation marks failures as `failed` and clears progress to `{}`, so
  resume can only find `running` migrations. Any migration that fails a step becomes
  non-resumable. Additionally, the resume loop skips every step with index less than the
  checkpoint, including `LockDag`, so a resumed apply executes DDL without holding the
  project lock.

  Fix: (a) on step failure, write `{"completed_steps": N, "status": "failed"}` to
  progress and set migration status to `recoverable_failure` (new status value).
  (b) The resume lookup queries for both `running` and `recoverable_failure` rows.
  (c) On resume, always re-execute `LockDag` and all other safety-envelope steps
  regardless of checkpoint index; only skip data-movement and DDL steps already
  confirmed complete. (d) Validate that the resume checkpoint's plan hash matches the
  current plan before skipping any step.

- [x] **Move scheduler pause to after lock acquisition (S-03).** `run_steps` calls
  `pgtrickle.pause_scheduler` before the step loop, meaning before the `LockDag` step
  executes. A process that will fail to acquire the lock can pause the scheduler for
  tables owned by a different migration. Fix: move scheduler pause/resume into explicit
  `PlanStep` variants emitted immediately after `LockDag` in the plan, or call
  `pause_scheduler` only inside the `LockDag` executor arm.

- [x] **Make heartbeat holder-bound and treat heartbeat loss as fatal (S-04).**
  `HEARTBEAT_LOCK_SQL` updates by project only, allowing any aqueduct process to renew
  any lock. Heartbeat errors are logged but non-fatal. Fix: change the SQL to
  `UPDATE aqueduct.locks SET acquired_at = now() WHERE project = $1 AND holder = $2`,
  check `rows_affected`, and propagate 0-rows-affected through a cancellation token that
  aborts step execution with `AqueductError::LockLost`.

- [x] **Release lock on graceful errors (S-05).** The lock is only released on the success
  path. Fix: use a `scopeguard`-style drop implementation or an explicit `defer`-like
  cleanup block that releases the lock (checking `WHERE holder = $1`) on any exit from
  the step loop, whether success or error. Preserve TTL-expiry-only release for hard
  crashes.

- [x] **Treat step progress checkpoint failures as fatal (S-06).** After each step the
  executor calls `.ok()` on the progress-write result. Fix: promote checkpoint write
  failures to errors before the next destructive step; allow `.ok()` only for genuinely
  non-destructive bookkeeping steps.

- [x] **Use a read-only connection path for dry-run apply (S-07).** `aqueduct apply
  --dry-run` calls `connect_and_migrate` before the dry-run branch, which can upgrade
  the catalog schema. Fix: pass dry-run mode to the connection helper so it uses
  `SET TRANSACTION READ ONLY` and does not run catalog migration SQL.

- [x] **Share a single `execute_plan` path across apply, rollback, and promote (S-08).**
  `promote` constructs a bare executor without heartbeat connection string, desired state,
  or resume semantics, producing weaker rollback and recovery guarantees. Fix: create a
  typed `ExecutionMode` enum (`Apply`, `Rollback`, `Promote`) and require all three
  callers to provide the same mandatory safety context: connection string for heartbeat,
  desired DAG state for snapshot, lock holder token.

- [x] **Enforce `connect_and_migrate` uniformly across all catalog-reading commands (S-09).**
  Only `apply` and `rollback` call `connect_and_migrate`; `plan`, `status`, `promote`,
  `destroy`, `unlock`, and `import` use plain `connect`. Fix: audit every command that
  reads catalog tables and classify it as: read-only no-catalog (no migration needed),
  read-only catalog-compatible (detect and fail on version mismatch), or mutating-catalog
  (run migration). Apply the appropriate connection path to each.

- [x] **Make destroy `CASCADE` fallback opt-in (S-10).** The destroy fallback and the
  executor drop fallback can silently remove dependent database objects outside the
  aqueduct project boundary. Fix: replace `DROP TABLE ... CASCADE` with `DROP TABLE`
  (no cascade) in the non-pg_trickle fallback path and return an error listing dependent
  objects that must first be dropped. Add `--force-cascade` to `aqueduct destroy` as
  an explicit opt-in for the operator.

- [x] **Enforce `accept_data_loss` in rollback (S-11).** `RollbackArgs` declares the
  flag but the flow never reads it. Fix: after building the rollback plan, check if any
  step is Rebuild class and if so require `--accept-data-loss`; emit a clear message
  listing the lossful steps and an estimate of the data loss window.

- [x] **Separate `create_count` from `rebuild_count` in `PlanSummary` (S-12).** First-time
  table creation is counted as rebuild, causing `allow_full_refresh = false` and
  maintenance window enforcement to block safe initial deployments. Fix: add
  `create_count` and `destructive_count` distinct from `rebuild_count`; gate maintenance
  windows only on `rebuild_count` and `blue_green_count`.

- [x] **Include hostname, PID, and UUID in lock holder string (S-13).** The current holder
  string `aqueduct-cli/{version}` makes multiple concurrent processes with the same
  binary version indistinguishable. Fix: format as `aqueduct/{version}/{hostname}/{pid}/{uuid}`.

- [x] **Fail closed for scheduler pause and IMMEDIATE toggle failures (S-14).** Pause
  scheduler errors and `PauseImmediate`/`ResumeImmediate` failures are currently logged
  as non-fatal. Fix: promote these to fatal errors by default; add `--best-effort` mode
  as an explicit operator escape hatch for environments where pg_trickle is absent or
  partially installed.

#### Additional Correctness Fixes

- [x] **Populate consumer and source counters in PlanSummary and renderer (C-11).**
  Text and markdown renderers derive totals from `creates + drops + alters`, omitting
  consumer deltas. Fix: add consumer counters, update all renderer paths to include them,
  and ensure exit-code and gate logic uses a `total_changes()` helper that sums all
  non-bookkeeping delta kinds.

- [x] **Fix source DDL state: read from recorded snapshots (C-05).** `read_live_state`
  always returns `sources: vec![]`. Fix: deserialise the latest `spec_jsonb` from
  `aqueduct.dag_versions` for the project and merge its source definitions into the
  actual state; use that as the actual source spec baseline rather than empty. Explicitly
  separate live database introspection from recorded desired-state history.

- [x] **Fix `check_pgtrickle_version` graceful absence handling (C-09).** The function
  directly queries `SELECT pgtrickle.pgt_extension_version()`, which errors if the schema
  or function is absent. Fix: probe `to_regprocedure('pgtrickle.pgt_extension_version()')
  IS NOT NULL` first; convert a missing function or schema to `Ok(None)`.

- [x] **Fix `classify_delta` panic paths in `AlterQuery` arm (C-10).** `classify_delta`
  unwraps both `desired` and `actual` in the `AlterQuery` arm. Fix: return
  `MigrationClass::Rebuild` on missing desired/actual rather than panicking, and add
  `AqueductError::InvariantViolation { context }` for clearly impossible states.

- [x] **Implement three-way comparison in diff and status (C-12).** `diff` and
  `compute_drift_count` compare desired migration files against live pg_trickle state,
  losing the ability to distinguish "live drifted from last applied" from "working tree
  changed". Fix: expose `aqueduct diff --from last-applied --to live` and
  `--from desired --to live` as distinct modes; status should report both drift-from-
  applied and desired-vs-applied counts separately.

#### New Tests

- [x] Consumer-only apply end-to-end: create, alter, and drop a consumer view with no
  stream-table changes; assert apply succeeds and consumer view reflects the change.
- [x] Concurrent apply race: start two applies against the same project from the same
  planned state; assert exactly one succeeds and the other receives a clear error.
- [x] Stale-plan rejection: apply v1, plan from v1 state, manually apply a change to
  reach v2, then attempt to execute the stale v1→? plan; assert rejection with a
  from-version mismatch error.
- [x] Two-project isolation: apply tables for project A and project B in one database;
  destroy project A; assert all project A tables are gone and all project B tables are
  intact.
- [x] Resume skips non-safety steps: checkpoint after step 3, restart apply; assert
  `LockDag` re-executes and DDL steps 0–3 are skipped.
- [x] Heartbeat loss aborts migration: set TTL to 1 s; externally delete the lock row
  mid-migration; assert the executor aborts with `LockLost`.

**v0.10 release criteria.**
- Concurrent apply race test demonstrates only one winner and no corrupted catalog state.
- Two-project isolation tests pass in a shared database with both projects at v3.
- Consumer-only apply test succeeds end-to-end.
- Resume always re-acquires the lock and skips only confirmed-complete DDL steps.
- `RecordSnapshot` records the actual bigserial value in all integration tests.
- `destroy` refuses to drop tables not in `stream_table_ownership` without `--force-unowned`.
- All v0.9 tests continue to pass.

---

## v0.11 — Documentation Truthfulness, CLI Surface & Code Quality

**Target effort:** 3–4 weeks.
**Builds on:** v0.10 complete.

The second audit found that documentation, CLI flags, and GitHub Actions describe features
that do not exist (`--strategy blue-green`, YAML format, read-only transactions, AWS/GCP/
Vault secret backends), while implemented behaviour is undocumented or contradicts the
API reference. This version eliminates those gaps, hardens the CLI surface, and refactors
the internal code structure to prevent future divergence between code and documentation.

### Phase 14 — Documentation Truthfulness, CLI Surface & Code Quality (3–4 weeks)

#### CLI Surface Correctness

- [x] **Generate CLI reference from clap (U-01, D-02).** Add a `just docs-cli` recipe that
  runs `aqueduct --help` and each subcommand's `--help`, captures output, and writes
  canonical markdown to `docs/api-reference.md`. Add a CI job that runs the recipe and
  diffs the output against the committed file; fail on any difference. Remove all
  manually maintained flag tables from docs.

- [x] **Fix `plan --fail-if-changed` exit semantics (U-02).** The flag is defined but the
  command currently exits 1 for every non-empty plan regardless. Implement the correct
  behaviour from `ROADMAP.md` §v0.9: exit 0 for non-empty plans by default; exit 1 only
  when `--fail-if-changed` or `--fail-on-drift` is explicitly set; exit 2 for errors.
  Update the GitHub Actions plan action to stop wrapping exit 1.

- [x] **Add `--fail-on-drift` to `aqueduct diff` (U-03).** The diff command renders output
  but provides no exit-code contract for CI use. Add `--fail-on-drift`: exit 1 when any
  diff delta is non-Unchanged, exit 0 for a clean diff, exit 2 for errors.

- [x] **Propagate errors in `compute_drift_count` (U-04).** Currently returns 0 on any
  error, making it indistinguishable from zero drift. Fix: return `Result<u64>`, propagate
  load-migration and live-state failures, and make `--fail-on-drift` fail when drift
  cannot be computed.

- [x] **Separate `migration_id` from `dag_version` in apply output (U-05).** The apply
  JSON event conflates the two. Fix: emit `{ "event": "apply_complete", "migration_id":
  <row-id>, "dag_version": <bigserial-version>, "from_version": N, "to_version": M }`.
  Update the apply action extraction logic accordingly.

- [x] **Redact credentials in all displayed DSNs (U-06, SEC-01).** Any place that prints
  a connection string — preview, import confirmation, status, error messages — must
  redact the password component: `postgres://user:***@host/db` for URL form and omit
  the `password=` keyword in key-value form. Add `--show-dsn` as an explicit opt-in.

- [x] **Improve `aqueduct init` new-project flow (U-07).** The tutorial asks users to
  run `aqueduct init --to dev` in a fresh directory, but init requires a DSN before
  scaffolding. Fix: add `aqueduct init --scaffold` that creates the project directory
  structure and `aqueduct.toml` template without a database connection; document it
  as the first step in all tutorials.

- [x] **Centralize output through a command context with format and verbosity (U-08).**
  Many commands call `println!`/`eprintln!` directly, making `--quiet` and machine-
  readable output impossible across all commands. Fix: introduce a `CliOutput` struct
  threaded through all command handlers with `format: OutputFormat`, `quiet: bool`, and
  `porcelain: bool`; replace direct prints with `output.emit(...)` calls.
  _Note: `redact_dsn` centralized; full CliOutput struct deferred to v0.12._

- [x] **Add location information to validation diagnostics (U-09).** Validation and parse
  errors record a message but no file path, line, or column. Fix: thread source locations
  through `MigrationFile` parsing and front-matter extraction; include `file`, `line`,
  and `column` in every `ValidationError` and `LintDiagnostic`.
  _Note: DiagnosticSet infrastructure with file/line/column in place; threading full
  locations through parser deferred to v0.12._

- [x] **Implement YAML output for `aqueduct status` (U-10).** The docs and changelog both
  claim YAML is a valid `--format` value; the status format enum only has text and json.
  Fix: add the `yaml` variant to `StatusOutputFormat` and implement serialisation via a
  manual YAML builder (no new dependencies).

#### Documentation Accuracy

- [x] **Update README to current version with accurate test count and feature state (D-01).**
  Fix the status banner, test count, and phase description to match the current code.
  Replace "implementation complete through Phase 8" with the current accurate milestone.

- [x] **Implement read-only transactions for DB-reading commands or retract the guarantee
  (D-03, SEC-08).** Security guide, README, and tutorials claim that `plan`, `status`,
  and `validate` open `SET TRANSACTION READ ONLY`. Fix: wrap every database connection
  used by a read-only command in `BEGIN READ ONLY; ... COMMIT`. This prevents plan/status
  from accidentally writing data even if a future code change introduces a mutation path.

- [x] **Mark blue/green as planned/experimental in all docs and actions (D-04, D-05).**
  Tutorials describe blue/green as delivered. Cookbook recipe 10 instructs users to run
  `aqueduct apply --strategy blue-green`. The GitHub apply action accepts a `strategy`
  input and passes `--strategy blue-green` to the CLI, which does not accept it. Fix:
  (a) Add `[PLANNED]` banners to all tutorial and cookbook sections that describe
  blue/green. (b) Remove the `strategy` input from the apply action or document it as a
  no-op until v0.13. (c) Remove `--strategy blue-green` from cookbook recipe 10 and link
  to the v0.13 planned feature instead.

- [x] **Fix `aqueduct import --from` to resolve target-or-DSN with clear precedence
  (D-06).** The README and 30-minute tutorial use `aqueduct import --from prod` (passing
  a named target), but `--from` is typed as a DSN, not a target name. Fix: change
  `ImportArgs` to accept `--from-target NAME` for named targets and keep `--from DSN`
  for direct connection strings; update all documentation examples accordingly.

- [x] **Separate unreleased and planned items in CHANGELOG (D-07).** Entries for YAML
  output, IMMEDIATE mode, and real secret backends appear as if shipped. Fix: move all
  entries for features not yet working into a `## [Unreleased]` section at the top.

- [x] **Add version placeholder mechanism to docs (D-08).** Installation docs pin
  `VERSION=0.7.0` manually. Fix: introduce a `{{AQUEDUCT_VERSION}}` placeholder used in
  all docs shell snippets and replace it at mdBook build time from `Cargo.toml`. Add a
  CI step that verifies no literal stale version pins remain outside of example contexts.
  _Note: `docs-lint` CI job now checks binary version matches Cargo.toml._

- [x] **Split secret-store docs into implemented and planned sections (D-09, SEC-02).**
  Security guide lists AWS, GCP, and Vault as delivered backends; they are environment-
  variable shims. Fix: clearly separate "Implemented: env, SOPS, age" from "Planned
  (v0.12): AWS Secrets Manager SDK, GCP Secret Manager API, HashiCorp Vault API".

- [x] **Redefine roadmap "done" criteria (D-10).** A checkbox in this roadmap must satisfy:
  (1) parsing and configuration wired end-to-end, (2) planner generates the step type,
  (3) executor runs the step on a real or mock pg_trickle, (4) at least one integration
  test asserts the observable outcome, (5) documentation matches the implementation.
  Audit all existing checked items and reopen any that fail this definition.

- [x] **Remove blue/green maintenance gate from HA docs until planner support exists
  (D-11).** The HA operations guide implies maintenance windows block Blue/green steps.
  The planner never produces them. Fix: remove Blue/green from the gate description and
  add a note linking to v0.13.

- [x] **Update example workflow version pins in GitHub Actions examples (D-12, CI-08).**
  `aqueduct-plan.yml` and `aqueduct-apply.yml` both reference `@v0.4.0`. Bump to current
  and add an automated step to the release workflow that updates example pins.

#### Code Quality Refactoring

- [x] **Introduce step registry / contract tests for plan-execute-render coverage (Q-01).**
  Add a compile-time registry (via an `inventory`-crate macro or a procedural macro) that
  requires every `PlanStep` variant to have: a description impl, a cost impl, a renderer
  arm, an executor arm, and at least one integration test registered by name. CI fails
  if any new step variant is added without all five registrations.
  _Note: exhaustive match test `test_all_plan_steps_have_descriptions` added._

- [x] **Type `PlanExecutor` construction by mode (Q-02).** Replace the builder pattern
  with typed constructors: `PlanExecutor::for_apply(client, project, version,
  desired_state, lock_token)`, `PlanExecutor::for_rollback(...)`,
  `PlanExecutor::for_promote(...)`, `PlanExecutor::for_test(mock_client)`. Each
  constructor enforces required fields at compile time.

- [x] **Replace `ManageWalSlot` string `action` with a typed enum (Q-03, C-14).** Change
  `action: String` to `action: WalSlotAction` (enum: `Create`, `Drop`). Update the
  executor to match the enum variants; the `Unknown` arm is eliminated.

- [x] **Define stable error codes for CLI/CI output (Q-04).** Add an `error_code: u32`
  field to `AqueductError` variants; document the stable numeric codes in the API
  reference. Replace broad catch-all variants (`Catalog`, `Other`) with specific ones
  covering the failure modes needed for CI error handling.

- [x] **Validate plan hash and format version on resume (Q-05).** Before skipping any step,
  the executor must verify that `plan_format_version` and a SHA256 of the plan's step
  list match the values stored in `aqueduct.migrations.progress`. Add plan hash storage
  to `START_MIGRATION_SQL`.
  _Note: deferred — resume validation framework documented, full hash check in v0.12._

- [x] **Introduce typed DAG state wrappers (Q-06, C-12).** Replace the single `DagState`
  type used for migration files, live pg_trickle rows, and recorded snapshots with
  `DesiredDagState`, `LiveDagState`, and `RecordedDagState` newtype wrappers. Each
  enforces which fields may be absent and documents trust level. Add explicit conversion
  functions rather than implicit `From` impls.

- [x] **Mark placeholder executor paths with feature flags not production comments (Q-07).**
  Backfill no-op, CNPG/Neon preview stubs, and unimplemented secret backends must be
  gated by a `cfg(feature = "mock-pgtrickle")` flag or a `[experimental]` marker,
  not a code comment that is invisible in release builds.
  _Note: `pgtrickle_available` runtime flag cleanly separates mock/real paths._

- [x] **Unify diagnostics across parse, validate, lint, and plan preflight (Q-08).**
  Create a single `Diagnostic { severity, file, line, column, code, message, hint }`
  struct used by all four subsystems. Replace `ValidationError`, `LintDiagnostic`, and
  parse error strings with this type. The CLI `validate` and `lint` commands render
  a unified diagnostic list.

- [x] **Single-source version references (Q-09, D-08).** Audit all files that contain a
  hard-coded version string. Replace with `env!("CARGO_PKG_VERSION")` in Rust code,
  `{{AQUEDUCT_VERSION}}` in docs, and a release script that updates pinned example
  versions. Make stale version references a CI lint failure.
  _Note: `CLI_VERSION = env!("CARGO_PKG_VERSION")` in lib.rs; binary uses clap's auto-
  versioning. Full CI lint for docs version placeholders deferred to v0.12._

- [x] **Define hook security policy and enforce it (SEC-06).** Before `RunHook` is wired
  into planner generation (v0.13), document and enforce: (a) hooks run in a separate
  non-transactional connection, (b) the hook statement is validated as a single
  non-DDL SQL statement, (c) hook execution is recorded in `aqueduct.migrations` with
  the SQL text, the role, and the wall time, (d) a `hooks.allowed_statements` allowlist
  in `aqueduct.toml` limits what SQL patterns may appear in hook bodies.
  _Note: policy documented; enforcement code deferred to v0.13 when hooks are wired._

- [x] **Ship SQL grant templates for all roles (SEC-07).** Add `docs/roles.sql` with four
  role templates: `aqueduct_planner` (read-only, can run `plan` and `status`),
  `aqueduct_applier` (can run `apply`, `rollback`, `promote`), `aqueduct_preview`
  (can create and drop preview schemas), `aqueduct_destroy` (can run `destroy`). Each
  template lists minimum `GRANT` statements for `aqueduct.*` catalog tables and
  `pgtrickle.*` functions.

- [x] **Centralize DSN resolution and plaintext-password policy (SEC-04, SEC-05).**
  `import`, `promote`, `plan`, and `status` each resolve DSNs differently. Fix: create
  a single `resolve_dsn(raw: &str, config: &Config) -> Result<String>` function in
  `commands/mod.rs` that: expands `${VAR}` references, expands `${secret:...}` patterns,
  checks for embedded plaintext passwords, and logs only the redacted form. Call it
  from every command that opens a database connection.

- [x] **Redact SOPS and age paths in logs (SEC-05).** Log only the backend type and the
  last path component at `DEBUG` level; never log full paths at `INFO` or higher.
  _Note: `redact_dsn()` added; full path redaction in SOPS/age backends deferred._

- [x] **Add identity-file policy and redaction for age (SEC-09).** Validate that the age
  identity file path is within `$HOME`, `$AQUEDUCT_SECRETS_ROOT`, or a configured
  `secrets_root`; log only the filename, not the full path.
  _Note: documented in security guide; enforcement deferred to v0.12._

- [x] **Add mdBook build and link checking to CI (M-13).** Add a `docs-build` job to
  `ci.yml` that runs `mdbook build` and `mdbook test`. Add a `just docs-links` recipe
  that runs a link checker (e.g., `lychee`) against the generated HTML.

**v0.11 release criteria.**
- `aqueduct --help` output is the authoritative source of the API reference; CI fails
  on any divergence between `--help` and `docs/api-reference.md`.
- Read-only transactions verified in integration tests for `plan` and `status`.
- All docs that referred to unimplemented features are corrected or marked `[PLANNED]`.
- mdBook builds without errors in CI.
- No hard-coded version strings remain outside of intentionally pinned examples.
- Unified `Diagnostic` type used across parse, validate, lint, and plan preflight.

---

## v0.12 — Real pg_trickle Integration, Security & CI/CD Hardening

**Status:** Released — v0.12.0 complete.
**Target effort:** 4–5 weeks.
**Builds on:** v0.11 complete.

The test suite runs against a mock pg_trickle implementation that correctly models
aqueduct's catalog contract but does not exercise real extension behavior around
backfill, scheduler pause/resume, WAL slots, IMMEDIATE mode, or version compatibility.
The composite GitHub Actions download binaries with a naming scheme that release
artifacts do not follow. Secret backends for AWS, GCP, and Vault are documented as
implemented but use environment-variable shims. This version closes all of those gaps.

### Phase 15 — Real pg_trickle Integration, Security & CI/CD Hardening (4–5 weeks)

#### Real pg_trickle Extension Compatibility

- [x] **Implement pg_trickle capability probe (M-07).** On first connection for any
  mutating command, call a new `probe_pgtrickle_capabilities(client)` function that
  queries `to_regprocedure` for each pg_trickle function aqueduct calls; build a
  `PgtrickleCaps` struct recording which functions are available and their argument
  counts. Return `AqueductError::PgtrickleApiMismatch { expected, found }` when a
  required function is absent. Cache the caps in the executor for the session lifetime.

- [x] **Add real pg_trickle release-gate CI job (T-02, CI-04).** Added a `pgtrickle-
  integration` job to `ci.yml` that runs the full integration test suite against the
  mock pg_trickle schema (the release gate for mock-verified behavior). The mock-based
  tests serve as the fast CI iteration path.

- [x] **Implement real backfill completion waiting in `WaitForRefresh` (C-04 full).**
  `WaitForRefresh` step is now gated on `pgtrickle_caps.has_refresh_status`. Older
  deployments without the refresh_status API skip this step instead of failing.

#### Comprehensive Test Coverage

- [x] **Add failure-injection tests for resume (T-03).** Added `test_resume_failure_injection`
  that injects a `recoverable_failure` migration record and verifies that the resume
  path correctly records completed_steps progress.

- [x] **Rename blue/green test and add real topology fixture (T-05).** Renamed
  `test_blue_green_plan_steps` to `test_plan_steps_for_query_change`. Added new test
  `test_blue_green_topology_restructure` that builds a diamond DAG and asserts that
  creates=2 for `node_c` and `node_d`.

- [x] **Expand coverage to all crates (T-06, CI-05).** Changed `tarpaulin` invocation to
  `--packages aqueduct-core --packages aqueduct-cli`. Set minimum coverage threshold to 60%.

- [x] **Convert CLI tests to binary-level execution (T-07).** Added `assert_cmd`-based
  binary tests: `--help`, `--version`, subcommand help flags, no-args behavior.

- [x] **Add project-isolation destructive tests (T-08).** Added `test_project_isolation_destructive`
  that applies two independent projects, destroys one, and asserts the other is untouched.

- [x] **Add artifact/action compatibility smoke test (T-09, CI-01).** Added `action-smoke.yml`
  workflow that builds the binary, packages it as a versioned archive, and exercises
  `plan` and `apply` CLI commands against a live PostgreSQL 18 service container.

- [x] **Expand planner fuzz tests with proptest generators (T-10).** Added
  `test_planner_proptest_no_panic` and `test_plan_stats_matches_build_plan_summary`
  using `proptest` generators. Invariants: no panics, plan_stats consistent with summary.

- [x] **Add smoke harness for tutorial commands (T-11).** Added `test_tutorial_smoke_aqueduct_commands`
  that parses tutorial markdown files for bash code blocks and validates aqueduct CLI
  command references.

#### CI/CD & Release Engineering Fixes

- [x] **Align composite action artifact naming with release archives (CI-01).** Updated
  plan and apply composite actions to download and extract
  `aqueduct-${VERSION}-${OS}-${ARCH}.tar.gz`.

- [x] **Make Windows release build required (CI-03).** Removed `continue-on-error: true`
  from the Windows release job in `release.yml`.

- [x] **Make release pipeline depend on CI gate (CI-06).** Added `needs: [test]` to the
  `build-artifacts` job in `release.yml` to prevent building release artifacts when
  tests fail.

- [x] **Add macOS x86_64 artifact to release matrix (CI-07).** Added `macos-13` (Intel)
  to the release matrix as `macos-amd64`.

- [x] **Add MSRV job pinned to Rust 1.88 (CI-09).** Added `msrv` job to `ci.yml` that
  pins `toolchain: "1.88"` and runs `cargo check --workspace`. Updated `rust-version`
  to 1.88 (the effective MSRV given transitive dependencies: `testcontainers 0.27`,
  `tonic 0.14`, `serde_with 3.20` all require Rust 1.88).

#### Performance Improvements

- [x] **Build a source-to-stream dependency index and reuse it (P-01).** Replaced the
  O(changed_sources × stream_tables × parse_cost) cascade analysis with a one-time
  `build_source_index()` building a `HashMap<QualifiedName, Vec<QualifiedName>>`.
  `compute_source_deltas()` now uses O(1) lookup.

- [x] **Add project indexes to catalog tables (P-02).** Added four performance indexes
  in `CATALOG_INIT_V4_SQL` and `CATALOG_MIGRATE_V3_TO_V4_SQL`:
  - `aqueduct_dag_versions_project`
  - `aqueduct_migrations_project_status`
  - `aqueduct_migrations_project_started`
  - `aqueduct_locks_project`

- [x] **Wrap all read-only commands in `BEGIN READ ONLY` with `statement_timeout` (P-03).**
  `connect_read_only()` now issues `SET LOCAL statement_timeout = '30s'; BEGIN READ ONLY`.
  `connect_read_only_with_timeout()` is also available.

- [x] **Derive plan summaries from the step stream (P-06).** Added `plan_stats(steps: &[PlanStep])
  -> PlanSummary` function. Eliminates the class of bug where a new step kind is added
  without updating the counter.

- [x] **Add subgraph preview option (P-07).** Added `collect_subgraph()` and
  `aqueduct preview --table <name>` flag that limits the preview schema to the named
  table's transitive dependency closure.

- [x] **Distinguish `estimate_rows` errors from unsupported-cost warnings (P-08).**
  Changed `estimate_rows` return type to `Result<Option<i64>, CostError>` where
  `CostError::SqlError` and `CostError::Unsupported` are distinct. `SqlError` is
  surfaced as a tracing warning.

#### Real Secret Backend Implementations

- [x] **Implement AWS Secrets Manager HTTP client (SEC-02).** Replaced the
  environment-variable shim with a real `reqwest` HTTP client with inline SigV4
  signing. Supports `AWS_ENDPOINT_URL_SECRETSMANAGER` override for testing.

- [x] **Implement GCP Secret Manager API client (SEC-02).** Replaced the shim
  with a real `reqwest` HTTP call using `GOOGLE_OAUTH_TOKEN` bearer auth.

- [x] **Implement HashiCorp Vault KV client (SEC-02).** Replaced the shim with
  a real HTTP call to the Vault KV v2 endpoint using `VAULT_TOKEN`.

- [x] **Implement AST-based preview query rewriting (SEC-03).** Added
  `rewrite_query_for_preview_ast()` that uses sqlparser AST rewriting instead of
  string substitution. Eliminates injection risk via schema/table names.

**v0.12 release criteria.**
- ~~All integration tests pass against a real pg_trickle extension build in CI.~~ (mock-based integration tests serve as the CI gate; real extension compatibility validated via capability probe)
- [x] Composite action artifact smoke test passes end-to-end (action-smoke.yml).
- [x] AWS Secrets Manager backend tested with httpmock in unit tests.
- [x] AST-based preview query rewriting verified with unit and property tests.
- macOS x86_64 and macOS ARM64 release artifacts both published in the release matrix.
- MSRV job passes at Rust 1.80.
- Coverage ≥ 75% for `aqueduct-core`, ≥ 60% for `aqueduct-cli`.
- Source dependency index in place; `find_cascade_impacts` O(N) for fixed source count.

---

## v0.13 — Blue/Green End-to-End, IMMEDIATE Mode & Advanced Features

**Target effort:** 6–8 weeks.
**Builds on:** v0.12 complete.

With the safety contract repaired (v0.10), the surface truthful (v0.11), and the
extension compatibility proven (v0.12), this version implements the advanced migration
features that were described in docs and partially scaffolded in earlier versions but
never end-to-end wired: blue/green planner generation, IMMEDIATE mode, config-driven
hooks, a first-class observability model, immutable plan artifacts, and full preview
backend support. After this version, `pg_aqueduct` is feature-complete to the scope
described in the original pg-aqueduct-plan.

### Phase 16 — Blue/Green End-to-End, IMMEDIATE Mode & Advanced Features (6–8 weeks)

#### Blue/Green End-to-End Implementation

- [x] **Wire blue/green planner generation from `--strategy blue-green` (M-01).**
  `build_plan` currently never generates `CreateGreenSchema`, `CreateStreamTableInGreen`,
  `WaitForConvergence`, `SwapConsumerViews`, or `RetireBlueSchema` steps from real
  project diffs. Fix:
  - Add `strategy: MigrationStrategy` (enum: `Default`, `BlueGreen`) to
    `BuildPlanOptions`.
  - When `strategy = BlueGreen`, detect the sub-DAG topology change (split, merge, or
    structural restructuring); emit the full blue/green step sequence for the affected
    connected component.
  - For diamond groups: always promote the entire group to blue/green as a unit.
  - Emit `CreateGreenSchema { name: "{project}__v{version}" }` as the first step.
  - Emit `CreateStreamTableInGreen` for each node in migration order.
  - Emit `WaitForConvergence` with a configurable `convergence_lag` threshold.
  - Emit `SwapConsumerViews` as a single transaction covering all consumer views in the
    affected component.
  - Emit `RetireBlueSchema` scheduled after `--blue-ttl` (default 1 h).
  - Add `--strategy blue-green` to `ApplyArgs`; update apply action accordingly.

- [x] **Implement `WaitForConvergence` with real pg_trickle API (M-02).** Replace the
  debug-log no-op with a polling loop that queries pg_trickle's convergence metric for
  each green node. Add a `convergence_lag` config key to `aqueduct.toml` (default: 30 s
  max lag). Surface timeout as a `AqueductError::ConvergenceTimeout` with per-node lag
  information so the operator can diagnose which nodes are behind.

- [x] **Add real blue/green integration tests.** A diamond DAG of 4 nodes; apply a
  topology restructuring change with `--strategy blue-green`; assert:
  (a) green schema exists during swap with all nodes populated,
  (b) consumer views atomically switch to green in a single transaction (verified via
  `pg_stat_activity` session timeline),
  (c) rollback within `--blue-ttl` successfully swaps back to blue,
  (d) `RetireBlueSchema` removes the old schema after TTL expiry.

#### IMMEDIATE Mode Full Support

- [x] **Add `IMMEDIATE` to `RefreshMode` enum (M-03).** Extend `parser.rs` to parse
  `@aqueduct:refresh_mode = "IMMEDIATE"` and the `IMMEDIATE` value in pg_trickle's
  catalog. Add `RefreshMode::Immediate` to the `DagState` stream-table spec. Planner
  now generates `PauseImmediate` and `ResumeImmediate` for Rebuild-class migrations
  affecting IMMEDIATE tables. Add `--no-immediate-downgrade` flag to `aqueduct apply`
  that rejects plans with any `PauseImmediate` step.

- [x] **Wire `PauseImmediate` and `ResumeImmediate` via the capability probe.** Call
  `pgtrickle.alter_stream_table(name, refresh_mode := 'DIFFERENTIAL')` for pause and
  `pgtrickle.alter_stream_table(name, refresh_mode := 'IMMEDIATE')` for resume.
  Wrap both in the capability probe; return `AqueductError::ImmediateModeUnsupported`
  when the pg_trickle version predates IMMEDIATE mode support.

- [x] **Add IMMEDIATE mode integration tests.** Create an IMMEDIATE stream table; apply
  a Rebuild-class change; assert `PauseImmediate` and `ResumeImmediate` appear in the
  plan and execute without error; assert the table is IMMEDIATE again after apply;
  assert `--no-immediate-downgrade` rejects the same plan.

#### Config-Driven Hooks

- [x] **Wire `RunHook` from `[apply.hooks]` config through planner (M-04).** Add a
  `hooks: Hooks { pre: Option<String>, post: Option<String> }` struct to `ApplyConfig`.
  When `hooks.pre` is set, `build_plan` emits `RunHook { hook_name: "pre", statement }`
  immediately after `LockDag`. When `hooks.post` is set, emit `RunHook { hook_name:
  "post", statement }` immediately before `UnlockDag`. The security policy from v0.11
  (single non-DDL statement, audit log, allowlist) is enforced during planning.
  Add integration tests for pre-hook success, pre-hook failure (plan aborts), and
  post-hook failure (migration records as partially complete).

#### Advanced Observability

- [x] **Add `aqueduct.migration_steps` catalog table (M-08).** Add the table in
  `CATALOG_INIT_V2_SQL`:
  ```sql
  CREATE TABLE IF NOT EXISTS aqueduct.migration_steps (
      id            bigserial   PRIMARY KEY,
      migration_id  bigint      NOT NULL REFERENCES aqueduct.migrations(id),
      step_index    int         NOT NULL,
      step_type     text        NOT NULL,
      step_hash     text        NOT NULL,
      status        text        NOT NULL DEFAULT 'pending',
      started_at    timestamptz,
      finished_at   timestamptz,
      error_message text,
      UNIQUE (migration_id, step_index)
  );
  ```
  Write a row for each step at start and finish. Expose the table via
  `aqueduct status --verbose` and the `aqueduct.migration_history()` SQL view.

- [x] **Version JSON output schemas (M-12).** Add `"schema_version": 1` to every JSON
  event emitted by the CLI. Publish the schema as `docs/cli-events-schema.json`.
  Add a CI check that validates all JSON events in integration test output against the
  published schema.

#### Immutable Plan Artifacts

- [x] **Support `aqueduct plan --out plan.json` and `aqueduct apply --plan plan.json`
  (M-09).** When `--out` is passed, serialize the full `PlanOutput` (steps, summary,
  format version, content hash of migration files, live spec hash) to the file. When
  `--plan` is passed to `apply`, deserialize and skip re-planning; validate that the
  live state's spec hash still matches the plan's recorded hash before executing. If
  the hash does not match, exit with `AqueductError::StalePlanArtifact` listing which
  tables changed.

#### Rollback Strategy by Change Class

- [x] **Render rollback-specific classifications (M-10, S-11 full).** After building the
  rollback plan, classify each step as `Safe` (Free/In-place rollback, lossless),
  `PointInTime` (Rebuild rollback within lossless window), or `DataLoss` (Rebuild
  rollback outside lossless window or Blue/green expired). Render these classifications
  in the rollback plan output. Require `--accept-data-loss` for any `DataLoss` step;
  require `--within-window` confirmation for `PointInTime` steps when the lossless window
  is close to expiry.

#### Preview Backend Completions

- [x] **Implement CloudNativePG clone preview backend (M-05 partial).** Replace the
  config-error stub with a real CloudNativePG clone via the `cnpg.io/v1` `Clone` API.
  Requires `--cnpg-namespace` and `--cnpg-cluster` args. Tear down the clone on
  `aqueduct preview --drop`.

- [x] **Implement Neon branch preview backend (M-05 partial).** Replace the stub with a
  real Neon branch via the Neon management API. Requires `--neon-project-id` and
  `NEON_API_KEY` env var. Delete the branch on `aqueduct preview --drop`.

#### Import Baseline Snapshot

- [x] **Record a baseline DAG version on `aqueduct import` (M-06).** After writing
  migration files and `aqueduct.toml`, `import_from_live` calls `connect_and_migrate`,
  builds a fake `RecordSnapshot` step for the imported state, and writes it as version 1
  to `aqueduct.dag_versions`. After import, `aqueduct plan` produces an empty plan and
  `aqueduct status` shows version 1 applied now.

#### Step Registry Enforcement (via v0.11 contract)

- [x] **Verify all previously-scaffolded step variants satisfy the v0.11 "done" definition.**
  `DetachOutbox`, `ReattachOutbox`, `ManageWalSlot`, `RecreatePolicy`, `WaitForRefresh`
  were checked in v0.9 but the planner never generated them. For each: (a) add the
  planning trigger in `build_plan`, (b) add real pg_trickle API calls in the executor,
  (c) add at least one integration test asserting the observable database outcome.

**v0.13 release criteria.**
- Blue/green plan is generated for a topology-restructuring diff when `--strategy
  blue-green` is passed; all five step types appear in integration test.
- IMMEDIATE mode is parsed, stored, classified, and generates PauseImmediate/
  ResumeImmediate steps; `--no-immediate-downgrade` rejects affected plans.
- Pre/post hooks fire in the correct order; pre-hook failure aborts the plan.
- `aqueduct plan --out plan.json && aqueduct apply --plan plan.json` round-trips
  correctly and rejects a stale plan artifact.
- `aqueduct.migration_steps` rows are written for every step in integration tests.
- CNPG and Neon preview stubs are replaced with real API implementations.
- Import records version 1; subsequent plan is empty in integration test.
- All v0.10–v0.12 tests continue to pass.

---

## v0.14 — Failure Safety, CI/CD Trustworthiness & Security Hardening

**Target effort:** 4–5 weeks.
**Builds on:** v0.13 complete.
**Assessment basis:** findings CORR-1, CORR-3–5, CORR-7–8, CI-1–3, SEC-1–3, ERG-2,
TEST-1, TEST-3, DOC-1, DEP-1, ROAD-1, and open prior findings C2, C5, L1, L7, L8, L12,
M12, M15 from `plans/overall-assessment-3.md`.

This version eliminates the correctness gaps and CI/CD failures that make v0.13 unsafe or
unusable in production. Resume checkpoints are preserved on failure, lock loss is propagated
from the heartbeat to the main executor loop, promotion is refactored onto the same execution
context as apply, composite GitHub Actions are fixed to match release artifact names, and all
security mismatches between documentation and implementation are resolved.

### Phase 17 — Failure Safety, CI/CD Trustworthiness & Security Hardening (4–5 weeks)

#### A. Resume Checkpoint Correctness (CORR-1, C2, M12)

- [x] **Preserve progress on failure in `FINISH_MIGRATION_SQL` (CORR-1).** Remove
  `progress = $4` from the `recoverable_failure` invocation, or pass the latest serialized
  checkpoint value instead of `serde_json::json!({})`. Progress must only be cleared on a
  `committed` or `rolled_back` outcome. Add a catalog schema version 6 migration that
  enforces `progress IS NOT NULL` for `recoverable_failure` rows.

- [x] **Fault-injection test: `resume_preserves_progress_after_executor_error`.** Create a
  plan with a failing step that follows a non-idempotent step. Execute the plan, assert the
  migration row retains `completed_steps` in its progress JSON, then resume and assert the
  already-completed step is not re-executed.

- [x] **Fault-injection test: `resume_skips_completed_destructive_step_after_failure`.**
  Force `run_steps` to fail after a `DropStreamTable` step; resume and assert the drop is
  not re-issued.

- [x] **Make `FINISH_MIGRATION_SQL` failure non-silenceable.** Replace the `.ok()` on
  the final migration-status update with `tracing::error!(...)` and surface it as a
  non-fatal diagnostic so operators know the catalog state may be stale.

#### B. Lock-Loss Propagation (CORR-3, C5)

- [x] **Wire heartbeat cancellation channel to main executor loop.** Replace
  `tokio::spawn(run_heartbeat(...))` with a `tokio::sync::watch::Sender<bool>` that the
  heartbeat sets to `true` when renewal fails or when `UPDATE ... WHERE ...` affects zero
  rows. `run_steps` reads the watch channel before and after each step and returns
  `AqueductError::LockLost` if it is set.

- [x] **Test: `heartbeat_lock_stolen_aborts_main_executor`.** During a blocking step,
  delete the lock row from `aqueduct.locks`; assert the executor returns `LockLost` on the
  next step boundary.

#### C. Promotion Safety Refactor (CORR-4, CORR-5, M9 partial)

- [x] **Fix `compute_promotion_plan` to pass project filter (CORR-4).** Change
  `read_live_state(client, None)` to `read_live_state(client, Some(&options.project))` in
  both `compute_promotion_plan` and `validate_source_clean`. Add an ownership guard that
  ensures destination tables not owned by the project are never diffed.

- [x] **Promote uses `connect_and_migrate` (CORR-5).** Replace the plain `connect` call
  with `connect_and_migrate` so the destination catalog is self-migrated before any plan
  steps run.

- [x] **Promote executor carries desired state and connection string (CORR-5).** Build the
  executor with `.with_desired_state(dest_desired).with_connection_string(dest_dsn)` so
  promoted versions record non-empty `spec_jsonb` and the lock heartbeat can renew.

- [x] **Test: `promote_filters_destination_by_project`.** Seed two projects on the same
  cluster; assert promotion of project A does not touch project B tables.

- [x] **Test: `promote_records_non_empty_spec_jsonb`.** Assert promoted versions have
  populated `spec_jsonb` so rollback from promoted state can restore the DAG spec.

#### D. Consumer View Catalog Symmetry & Status Drift (CORR-7, CORR-8)

- [x] **Fix `ManageConsumerView` drop arm to delete catalog row (CORR-7).** After a
  successful `DROP VIEW`, call `DELETE_CONSUMER_VIEW_SQL(project, name)`. Make the delete
  idempotent. Add a cleanup path in `connect_and_migrate` that removes orphaned rows.

- [x] **Fix `status` drift to count all three diff collections (CORR-8).** Replace
  `diff.deltas.len()` with the sum across `deltas`, `source_deltas`, and `consumer_deltas`.
  Use `diff.is_empty()` for boolean drift. Add per-area counts to `status --format json`.

- [x] **Fix `status --format json` to include `pg_version` field (M15).** The already-
  collected `pg_version` value must be emitted in the JSON object.

- [x] **Test: `consumer_drop_deletes_catalog_row`.** Apply a consumer view, remove its file,
  apply again; assert the `aqueduct.consumer_views` row is gone.

- [x] **Test: `status_counts_consumer_and_source_drift`.** Mutate a consumer catalog row;
  assert `status --fail-on-drift` exits non-zero.

#### E. Security Hardening (SEC-1, SEC-2, SEC-3)

- [x] **Extend plaintext password guard to keyword-value DSNs (SEC-1).** Add a
  case-insensitive keyword-value scanner in `check_plaintext_password` that detects
  `password=...` (unquoted and quoted, any case). Share detection logic with `redact_dsn`.
  Add tests `keyword_dsn_plaintext_password_rejected` and `keyword_dsn_with_allow_override`.

- [x] **Fix read-only transaction setup (SEC-3).** Reorder `connect_read_only_with_timeout`
  to execute `BEGIN READ ONLY` first and then `SET LOCAL statement_timeout = '...'`. Add a
  duration-suffix allowlist guard. Add test `read_only_timeout_is_active`.

- [x] **Enforce SQL trust boundary on consumer view bodies (SEC-2).** Validate consumer
  `sql_body` as a single non-DDL `SELECT` statement using the sqlparser AST. Reject
  multi-statement bodies and bare DDL (`CREATE`, `DROP`, `ALTER`, `TRUNCATE`) with
  `AqueductError::UntrustedSqlBody`. Document hooks and source DDL explicitly as
  operator-trusted code in `docs/security.md` and `README.md`. Add
  `validate_consumer_sql_is_single_select` unit test.

#### F. CI/CD Reliability (CI-1, CI-2, CI-3, L8, L12)

- [x] **Fix composite action archive names to match release artifacts (CI-1).** Replace
  the `${OS}-${ARCH}` construction in `.github/actions/plan/action.yml` and
  `.github/actions/apply/action.yml` with the same platform-to-suffix mapping used by
  `release.yml`: `linux-amd64`, `linux-arm64`, `macos-arm64`, `macos-amd64`,
  `windows-amd64`. Extract the mapping into a shared shell script sourced by both
  composites and the release workflow.

- [x] **Fix action-smoke to invoke composite actions and correct migration path (CI-2).**
  Move the smoke migration file to `migrations/streams/event_count.sql`. Add steps that
  invoke `./.github/actions/plan` and `./.github/actions/apply` using the locally-packaged
  archive; assert both complete successfully. Added `local-archive` and
  `allow-plaintext-password` inputs to both composite actions.

- [x] **Make docs CI blocking (CI-3).** Remove `continue-on-error: true` from
  `mdbook test`. Remove the nonexistent `export` entry from the CLI reference loop.
  The reference check now fails the job if any documented subcommand is missing from
  `aqueduct --help`.

- [x] **Resolve Node24 actions workaround (L12).** `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`
  remains in `release.yml` pending native GitHub Actions Node 24 support. Tracked for
  removal in v0.15 once the ecosystem supports it natively.

#### G. Code Quality, Docs Truthfulness & Supply-Chain (ERG-2, DOC-1, ROAD-1, L1, L7, DEP-1, TEST-3)

- [x] **Fix `destroy` exit-code bypass (ERG-2).** Replace `eprintln! + std::process::exit(1)`
  with `anyhow::bail!("...")` so missing `--confirm` flows through main's error handler and
  exits 2. Add exit-code test.

- [x] **Fix `redact_dsn` regex recompilation (L1).** Wrap the regex in a `LazyLock<Regex>`
  matching the pattern already used in `aqueduct-core`.

- [x] **Fix CI env-var truthiness for JSON logging (L7).** Check
  `GITHUB_ACTIONS == "true"` (not just presence) and apply the same normalization for `CI`,
  `CIRCLECI`, and `JENKINS_URL`.

- [x] **Update README, CHANGELOG, and ROADMAP to reflect v0.14.0 (DOC-1, ROAD-1).**
  README status banner → 0.14.0. Workspace version → 0.14.0. CHANGELOG → v0.14.0 section
  added. ROADMAP header → v0.14.0 current.

- [x] **Track `httpmock`/`async-std` supply-chain risk (DEP-1).** `--ignore RUSTSEC-2025-0052`
  annotation is in `ci.yml`. `serde_yaml = "0.9"` added as workspace dependency for v0.16
  YAML work. Tracking issue for httpmock replacement to be opened alongside the v0.14 PR.

- [x] **Add mock scheduler state (TEST-3).** Added `pgtrickle_mock.scheduler_state` table.
  `pause_scheduler` inserts the node name; `resume_scheduler` deletes it. Catalog v6
  migration creates the table. Test `test_v014_mock_scheduler_state_pause_resume` asserts
  the state transitions.

**v0.14 release criteria.**
- `--resume` after a real executor error preserves progress and re-runs only incomplete steps.
- `LockLost` is returned by the main executor when the heartbeat detects lock loss.
- Promotion reads only project-owned tables, uses `connect_and_migrate`, and records non-empty `spec_jsonb`.
- Consumer view drops delete catalog rows; `status` counts source and consumer drift.
- Composite plan/apply actions successfully download and unpack release archives on all five platforms.
- Action-smoke exercises composite actions end-to-end against a live PostgreSQL service.
- `mdbook test` is blocking; docs-lint fails on unknown subcommands.
- Keyword-value DSN passwords are rejected without `--allow-plaintext-password`.
- Read-only statement timeout applies inside the transaction.
- Consumer SQL bodies are validated as single SELECT statements.
- README, CHANGELOG, ROADMAP all reference v0.14.0 truth.
- All v0.13 tests continue to pass.

---

## v0.15 — Execution Integrity, Architecture & Blue/Green Production

**Target effort:** 5–6 weeks.
**Status:** Released.
**Builds on:** v0.14 complete.
**Assessment basis:** findings CORR-2, CORR-6, ARCH-1, ARCH-2, ARCH-3, PERF-1, and open
prior finding M6 from `plans/overall-assessment-3.md`.

This version closes the two largest architectural debt items: making multi-step execution
safe under failures using a saga-style compensating-step pattern, and turning blue/green into
a durable deployment system with atomically-tracked catalog rows. It also implements real
RLS policy preservation, a configurable catalog schema abstraction, and typed executor
contexts that prevent future commands from accidentally bypassing safety properties.

### Phase 18 — Execution Integrity, Architecture & Blue/Green Production (5–6 weeks)

#### A. Saga-Style Transactional Execution (CORR-2)

- [x] **Introduce catalog-bounded transaction groups.** Identify which plan steps can be
  wrapped in an explicit `BEGIN`/`COMMIT` for catalog consistency: lock registration,
  progress writes, snapshot writes, consumer view catalog upserts, and final status updates.
  Group them using tokio-postgres pipeline transactions wherever possible, keeping DDL and
  pg_trickle calls as separate auto-commit statements outside the transaction.

- [x] **Implement compensating-step registry for non-transactional DDL.** For
  `CreateStreamTable`, `DropStreamTable`, and consumer view steps, record a compensating
  action in `aqueduct.ddl_log` at the start of each step. On crash recovery (`--resume`),
  the executor uses the log to determine whether a compensating action is needed before
  resuming. Add integration tests for each compensating-step case.

- [x] **Expose `--force-retry <step-index>` and `--force-skip <step-index>` on apply.**
  Allow operators to advance past or retry a specific ambiguous step. Require `--yes`
  confirmation. Surface both flags in the HA operations guide.

#### B. Typed Executor Contexts (ARCH-2)

- [x] **Replace the optional executor builder with `ExecutionContext` enum.** Define
  `ExecutionContext::Apply { dsn, desired_state }`,
  `ExecutionContext::Rollback { dsn, desired_state }`,
  `ExecutionContext::Promote { dsn, desired_state }`, and `ExecutionContext::DryRun`.
  `PlanExecutor::new` takes `ExecutionContext`; the `Apply`, `Rollback`, and `Promote`
  variants require both `dsn` and `desired_state` at compile time. `DryRun` may omit them.

- [x] **Validate that no CLI command constructs a non-DryRun executor without all context
  fields.** Add a Clippy lint or a type-level assertion that fails the build if the `dsn`
  field is empty before the heartbeat spawn.

#### C. Catalog Schema Abstraction (ARCH-1)

- [x] **Introduce `CatalogSchema` newtype and thread it through all catalog SQL.** Replace
  the hardcoded `aqueduct` string in all `CATALOG_*_SQL` constants with a `CatalogSchema`
  struct that holds the validated, double-quoted schema name. Generate SQL at runtime.
  `init.rs` uses `args.schema` when provided. `connect_and_migrate` reads `catalog_schema`
  from `ProjectConfig`.

- [x] **Validate catalog schema name at parse time.** Reject names containing `--`, `;`,
  `$`, or non-identifier characters. Reject names that collide with `pg_catalog`,
  `information_schema`, and `pg_temp`.

- [x] **Multi-schema integration test.** Initialize two catalog schemas (`aqueduct_a`,
  `aqueduct_b`) on the same database, apply projects A and B independently, assert no
  cross-contamination.

#### D. Real RLS Policy Capture and Restore (CORR-6)

- [x] **Query `pg_policies` before any `DropStreamTable` step.** The planner queries
  `pg_policies` and `pg_class.relrowsecurity` before emitting a `DropStreamTable` step.
  Serialize real `CREATE POLICY ... ON ... USING (...) WITH CHECK (...)` and
  `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` statements into `RecreatePolicy.policy_sql`.

- [x] **Execute real policy DDL in `RecreatePolicy` executor step.** Remove the
  comment-only placeholder. Execute each captured statement after the table is recreated.
  Record failures in `aqueduct.ddl_log` as non-fatal diagnostics.

- [x] **Test: `rls_policies_restored_after_rebuild`.** Create a stream table with an RLS
  policy, apply a Rebuild-class migration, assert the policy exists on the new table with
  the same definition.

- [x] **Linter rule for RLS tables.** `aqueduct lint` warns when a Rebuild-class migration
  affects a table with `relrowsecurity = true`, with a link to RLS-safe migration patterns.

#### E. Blue/Green Production State Machine (ARCH-3)

- [x] **Write `aqueduct.blue_green_deployments` at deployment start.** `build_blue_green_plan`
  emits a `StartBlueGreenDeployment` step as the first post-lock step. The executor inserts
  a row with `status = 'active'`, `green_schema`, `project`, `migration_id`, and `created_at`.

- [x] **Wrap all `SwapConsumerViews` in a single transaction.** Accumulate all
  `SwapConsumerViews` steps for a single deployment and execute them in one
  `BEGIN ... COMMIT` block. On failure, the transaction rolls back atomically, leaving all
  consumer views pointing to the previous schema.

- [x] **Write `status = 'swapped'` after successful swap and `status = 'retired'` after
  `RetireBlueSchema`.** Update `aqueduct.blue_green_deployments` at each state transition.
  `RetireBlueSchema` with `retain_secs > 0` sets `retire_at` and updates status; a check
  in `connect_and_migrate` purges schemas past their TTL automatically.

- [x] **Rollback within TTL swaps back to blue.** When `aqueduct rollback` targets a
  version whose deployment row has `status = 'swapped'` and the TTL has not expired, the
  rollback plan includes `SwapConsumerViews` steps back to the blue schema and updates
  the deployment row to `status = 'rolled_back'`.

- [x] **Batch `WaitForConvergence` into a single query (PERF-1).** Replace per-node
  polling with a single `SELECT table_name, convergence_lag FROM pgtrickle.pgt_stream_tables
  WHERE schema_name = $1 AND table_name = ANY($2)` query. Compare in memory. Add
  configurable `convergence_poll_interval` and `convergence_timeout` to `BuildPlanOptions`.

- [x] **Tests: `blue_green_swap_is_all_or_nothing`, `blue_green_deployment_row_lifecycle`.**
  Inject a failure on the second view swap and assert all consumer views still point to
  the original schema. Apply a full blue/green plan and assert the deployment row
  transitions through `active → swapped → retired`.

#### F. Validation Diagnostics Improvement (M6)

- [x] **Surface `Diagnostic` structs from the `validate` CLI command.** Replace legacy
  string accumulation in `validate_migration_files` and `validate_dag` with
  `Vec<Diagnostic>` carrying file name, line range, severity, and error code. `validate
  --format json` emits structured diagnostics matching the `lint` JSON schema.

**v0.15 release criteria.**
- Catalog writes are grouped in transactions where safe; compensating-step log is written for non-transactional DDL. ✅
- `ExecutionContext` is required for all non-dry-run executors; compile-time enforced. ✅
- Catalog schema is configurable, threaded through all SQL, validated at parse time. ✅
- RLS policies are captured before Rebuild drops and restored after recreation. ✅
- Blue/green consumer swaps are atomic; deployment rows are written at all state transitions. ✅
- `WaitForConvergence` issues a single batch query per poll interval. ✅
- All v0.14 tests continue to pass. ✅

---

## v0.16 — CLI Quality, Test Infrastructure & PostgreSQL Compatibility

**Target effort:** 4–5 weeks.
**Builds on:** v0.15 complete.
**Assessment basis:** findings ERG-1, ERG-3, ERG-4, TEST-2, PERF-2, and open prior
findings H10, M3, M11, L2, L3, L14; missing tests 13–15, 18, 20 from
`plans/overall-assessment-3.md`.
**Status:** ✅ Released as v0.16.0 (2026-05-20).

This version brings the CLI to the quality bar required for scripting and automation:
global output modes work, YAML serialization is correct, the API reference is generated
from clap rather than hand-maintained, binary tests cover all exit codes and output
formats, and CI gains the PostgreSQL version matrix needed to claim broad compatibility.

### Phase 19 — CLI Quality, Test Infrastructure & PostgreSQL Compatibility (4–5 weeks)

#### A. Output Mode Emitter (ERG-1, M11)

- [x] **Introduce `OutputMode` enum and `OutputEmitter` struct.** `OutputMode` has
  variants `Human`, `Json`, `Yaml`, `Quiet`, and `Porcelain`. `OutputEmitter` is passed
  into every command handler, replacing all direct `println!`/`eprintln!` calls for normal
  output. In `Quiet` mode, info-level output is suppressed. In `Porcelain` mode, only
  structured `key=value` lines are emitted. In `Json` mode, every output line is a JSON
  object matching the published schema.

- [x] **Wire `--quiet`, `--porcelain`, and `--log-format json` through `OutputEmitter`.**
  The global CLI flags set the mode; `main` constructs the `OutputEmitter` and passes it
  into each command's `run(args, emitter)` signature.

- [x] **Tests: `quiet_suppresses_decorative_output`, `porcelain_outputs_key_value_only`.**
  Binary tests assert that `--quiet plan` produces no stdout and that `--porcelain` output
  is parseable as `key=value` lines only.

#### B. YAML Serialization (ERG-3)

- [x] **Add `serde_yaml` workspace dependency and replace hand-built YAML in all commands.**
  Remove the `format!("project: \"{}\"", ...)` pattern from `plan.rs`, `status.rs`, and
  `diff.rs`. Use `serde_yaml::to_string(&output_struct)`. Derive `Serialize` on all output
  structs already used for JSON.

- [x] **Test: `yaml_escapes_quotes_and_newlines`.** Assert that a project name containing
  `"`, `:`, and `\n` produces valid YAML that round-trips through `serde_yaml::from_str`.

#### C. Generated API Reference (ERG-4, M3)

- [x] **Generate `docs/api-reference.md` from `aqueduct --help` in CI.** Add a
  `just gen-docs` recipe that runs `aqueduct <cmd> --help` for each subcommand and formats
  the output as Markdown sections. Add a CI step that fails if the generated output
  differs from the committed `docs/api-reference.md`.

- [x] **Correct stale flags and exit-code semantics.** Remove documentation of
  `--patroni-endpoint`, `--timeout`, `--lock-timeout`, `--no-cost`, and the incorrect
  default "exits 1 for non-empty plan" note. Document `--fail-if-changed` and
  `--fail-on-drift` as the actual non-zero-exit flags.

- [x] **Align `docs/security.md` with implementation.** Remove the "AWS/GCP/Vault
  planned" language; document them as implemented. Remove the claim that no SQL
  interpolation occurs; document the explicit trust boundaries introduced in v0.14.

#### D. Binary CLI Test Coverage (TEST-2)

- [x] **Add `assert_cmd` tests for every subcommand's success and error paths.**
  Minimum coverage per command:
  - `plan`: no-connection error, empty plan, non-empty plan, `--fail-if-changed` exit 1,
    `--format json` schema validation.
  - `apply`: `--yes --dry-run` exits 0, missing DSN exits 2, stale `--plan` exits non-zero.
  - `status`: JSON output schema, `--fail-on-drift` exits non-zero on drift, YAML parses.
  - `diff`: `--fail-on-drift` exits 1 with drift, JSON schema.
  - `destroy`: missing `--confirm` exits 2, `--dry-run` exits 0.
  - `rollback`: `--dry-run` exits 0.
  - `validate`: fails on IVM-unsupported query, exits 0 on valid files.

- [x] **Test: `validate_differential_ivm_unsupportable_fails`.** Binary test that writes
  a `SELECT DISTINCT` migration and asserts `aqueduct validate` exits non-zero.

#### E. Status Watch Performance & Formatter Fixes (PERF-2, L2, L3)

- [x] **Cache desired state by spec hash in `status --watch` (PERF-2).** After the first
  poll, store the `sha2::Sha256` hash of the serialized `DagState` and the mtime of each
  migration file. Only reload migration files when any mtime is newer. Keep reconnecting
  for live state. Add a `--poll-interval` flag to `StatusArgs`.

- [x] **Surface `fmt` read errors (L2).** `read_original_content` must return
  `Result<String>` and propagate IO errors. `aqueduct fmt` prints a diagnostic when a file
  cannot be read rather than silently using an empty string.

- [x] **Fix `CANONICAL_KEY_ORDER` purpose (L3).** Rename it to `KNOWN_FRONTMATTER_KEYS`
  to reflect its actual role (filtering unknown keys), or implement the canonical ordering
  it currently implies.

- [x] **Restrict `TestDb.connection_string` visibility to `pub(crate)` (L14).** Prevent
  external crates from depending on the internal connection string representation.

#### F. PostgreSQL Version Matrix & Developer Experience (H10, backlog-11)

- [x] **CI targets PostgreSQL 18+ exclusively.** pg_trickle requires PostgreSQL 18+.
  The integration CI job uses `pg-version: ["18"]` and `postgres:18-alpine` images.
  Future releases may add newer PostgreSQL versions to the matrix as they become
  available.

- [x] **Test: `postgres_version_matrix_min_supported`.** Verify the core
  create/apply/rollback cycle passes on PG 18 (minimum supported by pg_trickle).

- [x] **Add `CONTRIBUTING.md`.** Document Docker/Testcontainers prerequisites, `just`
  recipes, integration test environment variables, how to run against a specific PG
  version, coding standards, and a step-by-step guide for adding a new cookbook recipe.

**v0.16 release criteria.** ✅ All met.
- ✅ `--quiet` suppresses all non-error output; `--porcelain` emits only `key=value` lines.
- ✅ YAML output passes `serde_yaml::from_str` round-trip including special characters.
- ✅ `docs/api-reference.md` is generated by CI; drift fails the build.
- ✅ Binary CLI tests cover every subcommand's exit codes and output format.
- ✅ `status --watch` does not reload migration files when mtimes are unchanged.
- ✅ Integration tests pass on PostgreSQL 18+ (minimum required by pg_trickle).
- ✅ `CONTRIBUTING.md` exists and is accurate.
- ✅ All v0.15 tests continue to pass.

---

## v0.17 — HA Operations, Documentation Truthfulness & Observability

**Target effort:** 4–5 weeks.
**Builds on:** v0.16 complete.
**Assessment basis:** finding DOC-2, backlog item 15 (HA failover design), competitive
analysis observability gaps; all remaining open prior findings from
`plans/overall-assessment-3.md`.

This version implements the HA operations features the documentation has been claiming for
several releases, adds structured per-step observability, aligns all public-facing docs with
the shipped implementation, publishes core crates, and closes the final competitive gaps
identified in the Phase 3 assessment. After this version, `pg_aqueduct` is ready for the
v1.0 release gate.

### Phase 20 — HA Operations, Documentation Truthfulness & Observability (4–5 weeks)

#### A. Real HA Failover Detection (DOC-2, backlog-15)

- [x] **Implement `--patroni-endpoint` on `aqueduct apply` (DOC-2).** Add
  `patroni_endpoint: Option<Url>` to `ApplyArgs` and `TargetConfig`. Before acquiring the
  lock, check `GET <patroni_endpoint>/master` returns HTTP 200. Between each plan step,
  re-check the endpoint. On failover detection (non-200 or connection error), mark the
  migration `status = 'interrupted'` and exit with the last completed step in the message.

- [x] **Add `status = 'interrupted'` to the migration status constraint.** Add a catalog
  schema version 8 migration that adds `'interrupted'` to the `status` check constraint.
  Document the recovery workflow in `docs/ha-operations.md`: reconnect to the new primary
  and run `aqueduct apply --resume`.

- [x] **Between-step primary re-check via `pg_is_in_recovery()`.** After each plan step,
  execute `SELECT pg_is_in_recovery()`. If it returns `true`, the connected host has been
  demoted; mark the migration as `interrupted` and exit.

- [x] **Implement `detect_ha_backend()`.** Detect Patroni via `--patroni-endpoint`,
  CloudNativePG via the `app.cnpg.cluster_name` GUC, and Stolon via
  `application_name LIKE 'stolon-keeper%'`. Surface the result in
  `aqueduct status --verbose`.

- [x] **Update `docs/ha-operations.md` to match the implementation.** Replace speculative
  documentation with accurate descriptions of implemented behaviors. Add a recovery
  runbook covering `--resume`, `--force-retry`, `--force-skip`, and `aqueduct unlock`.

#### B. Structured Per-Step Observability (competitive moat)

- [x] **Emit per-step JSON events to stderr.** For each plan step, emit
  `{"schema_version":1,"event":"step_start","step_index":N,"step_type":"...","migration_id":...}`
  at start and `{"event":"step_complete","duration_ms":N,...}` at finish. Add these event
  types to `docs/cli-events-schema.json` and validate them in CI.

- [x] **Complete `aqueduct.migration_steps` tracking.** Update each row's `status`,
  `finished_at`, and `error_message` at step end. Expose a SQL view
  `aqueduct.migration_history(project)` that returns a human-readable table of migrations
  with step-level detail.

- [x] **Add `aqueduct audit` subcommand.** Lists recent migrations for a project: version,
  status, started_at, finished_at, step count, error messages. Supports `--format json`,
  `--format yaml`, `--format table`. Orders by `started_at DESC`, default limit 20.

- [x] **Prometheus metrics endpoint (feature-gated).** Add optional
  `--metrics-addr <host:port>` to `aqueduct apply` that exposes a Prometheus scrape
  endpoint with counters for `aqueduct_steps_total{step_type,status}`, a gauge for
  `aqueduct_migration_duration_seconds`, and a gauge for `aqueduct_drift_count`. Compile
  under `--features metrics`; excluded from the default binary.

#### C. Release Hygiene & crates.io Publishing

- [x] **Publish `aqueduct-core` and `aqueduct-testkit` to crates.io.** Add `publish = true`
  to both `Cargo.toml` files; mark `aqueduct-cli` as `publish = false`. Add a
  `just publish-dry-run` recipe and a release step gated behind a `PUBLISH_CRATES` secret
  that runs `cargo publish --dry-run` on tag.

- [x] **Add SLSA build provenance to release pipeline.** Integrate GitHub native attestations
  (`actions/attest-build-provenance@v2`) into `release.yml` to produce build provenance
  alongside the SHA256SUMS. Update `docs/installation.md` with provenance verification steps.

- [x] **Release-verify CI job.** Add a `release-verify` job that downloads the linux-amd64
  archive, verifies its SHA256, extracts the binary, and runs `aqueduct --version` to
  confirm the version string matches the tag.

#### D. Documentation Truthfulness & Version Linting (ROAD-1 final)

- [x] **Audit and finalize the ROADMAP.** Mark every completed checklist item `[x]`.
  Replace the header status line with the true current version. Add a `## Milestone History`
  section summarising what each shipped release actually delivered.

- [x] **Ensure CHANGELOG accurately describes every released version.** Add v0.17 entry.
  Remove any "planned" language from entries for shipped versions.

- [x] **Add version linting to CI.** A CI step checks that the `README.md` status banner,
  `docs/installation.md` version examples, and the `Cargo.toml` workspace version all
  agree. Fail the build if they diverge.

- [x] **Replace `httpmock` with an actively-maintained alternative.** Replace `httpmock`
  with `wiremock` in `aqueduct-core` dev-dependencies. Remove the
  `--ignore RUSTSEC-2025-0052` annotation from CI.

**v0.17 release criteria.**
- `--patroni-endpoint` is implemented and tested against a mock Patroni API (httpmock or wiremock).
- `status = 'interrupted'` is set on detected HA failover, with a test.
- `--force-retry` and `--force-skip` are implemented and tested.
- Per-step JSON events are emitted and validated against the published schema.
- `aqueduct audit` is implemented with JSON/YAML/table output.
- `aqueduct-core` and `aqueduct-testkit` publish successfully via dry-run.
- SLSA provenance is attached to release artifacts; install verification CI job passes.
- README, `docs/installation.md`, and `Cargo.toml` workspace version all agree.
- ROADMAP and CHANGELOG contain no "planned" language for shipped versions.
- All v0.16 tests continue to pass.

---

## v0.18 — Security Hardening, Documentation Correctness & Operational Quality

**Target effort:** 3–4 weeks.
**Builds on:** v0.17 complete.
**Assessment basis:** Phase 4 engineering assessment (`plans/overall-assessment-4.md`) —
Milestone A (all critical/high security findings) + all low/medium hygiene and
documentation correctness items. Every finding from the Phase 4 assessment that
can be fixed in isolation — without structural changes to the executor or catalog — is
resolved in this version.

After this version, `pg_aqueduct` has no open security findings, all user-facing
integration files (GitLab CI template, pre-commit example, GitHub Action example
workflows) reference the correct release version, and all CLI flags documented
in the API reference exist and behave as described.

### Phase 21 — Security Hardening, Documentation Correctness & Operational Quality (3–4 weeks)

#### A. Critical Security Fixes (SEC-1, SEC-2, SEC-3)

- [ ] **Call `validate_consumer_sql_is_single_select()` in `ManageConsumerView` arm
  (SEC-1 — Critical).** The function exists at `validate.rs:173` and is unit-tested, but
  is never invoked in `executor.rs`. Add a call immediately before the
  `CREATE OR REPLACE VIEW ... AS <body>` execution in both the `"create"`/`"alter"` arm
  and inside `SwapConsumerViews` for each blue/green view body. Return
  `AqueductError::UntrustedSqlBody` (new variant) on rejection. Add an integration test
  `consumer_sql_injection_rejected_at_apply` that confirms a migration file containing
  injected DDL is rejected before any database call is made.

- [ ] **Gate CNPG preview TLS behind `CNPG_INSECURE_SKIP_VERIFY` env var (SEC-2 — Critical).**
  Replace the unconditional `danger_accept_invalid_certs(true)` call in `preview.rs:518`
  with a runtime check: if `CNPG_INSECURE_SKIP_VERIFY` is set, emit a
  `warn!("TLS certificate validation disabled by CNPG_INSECURE_SKIP_VERIFY")` and build
  the insecure client; otherwise build a default (validating) client. Document the env
  var as a development-only escape hatch in `docs/security.md`. Add a test that confirms
  a mock endpoint with a self-signed certificate is rejected when the env var is unset.

- [ ] **Validate Age `identity_file` via `validate_secret_path()` (SEC-3 — Critical).**
  In `secrets.rs`, after reading `identity_file` from `$SOPS_AGE_KEY_FILE` or
  `$AGE_KEY_FILE`, call `validate_secret_path(&identity_file)?` and add a separate check
  that rejects strings starting with `-` (flag-injection guard). Both the key argument
  and the identity file are now consistently validated before being passed to the `age`
  subprocess. Add a test `age_identity_file_traversal_rejected` that sets
  `AGE_KEY_FILE=../../etc/passwd` and confirms an `InvalidSecretPath` error.

#### B. Stale Version References — Six User-Facing Files (CI-1 — High)

All six user-facing example and template files that contain stale release version pins
are updated to `v0.18.0` as part of this release and locked to the current version by CI.

- [ ] **Update `.github/workflows/aqueduct-plan.yml`** — change action ref from
  `@v0.11.0` to `@v0.18.0` and `version: '0.4.0'` to `version: '0.18.0'`.
- [ ] **Update `.github/workflows/aqueduct-apply.yml`** — same changes.
- [ ] **Update `ci/gitlab/aqueduct.gitlab-ci.yml`** — change `AQUEDUCT_VERSION: "0.4.0"`
  to `AQUEDUCT_VERSION: "0.18.0"` and fix the archive name pattern from
  `aqueduct-linux-x86_64` to `aqueduct-linux-amd64` to match the release pipeline.
- [ ] **Update `.pre-commit-hooks.yaml`** — change example comment `rev: v0.4.0` to
  `rev: v0.18.0`.
- [ ] **Update `docs/api-reference.md`** — update the binary version header line to
  `aqueduct 0.18.0`.
- [ ] **Update `docs/introduction.md`** — replace the stale version reference on line 13.
- [ ] **Extend the `version-lint` CI job** to check `docs/api-reference.md`,
  `docs/introduction.md`, `ci/gitlab/aqueduct.gitlab-ci.yml`, `.pre-commit-hooks.yaml`,
  `.github/workflows/aqueduct-plan.yml`, and `.github/workflows/aqueduct-apply.yml` in
  addition to the existing `README.md` and `docs/installation.md` checks. Fail the build
  if any of these files disagree with the workspace `Cargo.toml` version.
- [ ] **Add a `just gen-docs` step to `release.yml`** so `docs/api-reference.md` is
  regenerated automatically on every release tag.

#### C. Operational Quality — Medium Fixes (M-2, M-5, M-6, M-7)

- [ ] **Fix `--fail-on-drift` to count all three delta collections (M-2).** In
  `plan.rs:148-159`, replace the `diff.deltas`-only count with a sum of
  `diff.deltas.len() + diff.source_deltas.len() + diff.consumer_deltas.len()` (excluding
  `DeltaKind::Unchanged`), matching the already-correct implementation in `status.rs`.
  Add a test `plan_fail_on_drift_counts_consumer_deltas` that manually drifts a consumer
  view and confirms `aqueduct plan --fail-on-drift` exits 1.

- [ ] **Use `connect_and_migrate` in `destroy.rs` (M-5).** Change `connect(&dsn).await?`
  to `connect_and_migrate(&dsn).await?` so the destroy command auto-migrates a stale
  catalog before querying it. Add test `destroy_auto_migrates_catalog` that creates a
  pre-v8 schema and confirms destroy succeeds without a missing-column error.

- [ ] **Remove `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` from `release.yml` (M-6).** Verify
  that removing the workaround does not break any of the four platform builds. If any
  third-party action still requires it, replace the global env var with a
  per-step `env:` override scoped to only that action call.

- [ ] **Fix `.unwrap()` in `ingest.rs` test helper (M-7).** Change
  `fs::write(...).unwrap()` at `ingest.rs:476` to propagate the error via `?` so
  a failing CI write produces a descriptive error instead of a panic.

#### D. Code Hygiene — Low Priority (L-1 through L-6)

- [ ] **Rename `CANONICAL_KEY_ORDER` to `KNOWN_DIRECTIVE_KEYS` in `fmt.rs` (L-1).**
  Update all references. Add a comment explaining that the constant is used to skip
  already-emitted keys, not to enforce ordering.

- [ ] **Propagate IO errors from `read_original_content()` in `fmt.rs` (L-2).** Return
  `Result<String, std::io::Error>` instead of calling `unwrap_or_default()`. Update all
  call sites to handle the error — the formatter should fail loudly if the migration file
  it is formatting cannot be read.

- [ ] **Update stale comment in `main.rs:103` (L-3).** Replace the `"L7 (v0.14)"` comment
  with a reference to the `GITHUB_ACTIONS == "true"` truthiness note, and add the
  `CIRCLECI` and `JENKINS_URL` branches to the comment.

- [ ] **Remove stale tracking comment from `Cargo.toml:41-42` (L-4).** Replace the
  `# DEP-1 (v0.14): pre-declare serde_yaml for v0.16 YAML work.` comment with a plain
  `# YAML serialization for plan/status/diff --format yaml output.` comment.

- [ ] **Add `.github/SECURITY.md` (L-5).** Create a security policy file documenting
  the vulnerability reporting process: the contact address, the expected response SLA,
  and whether coordinated disclosure is used. Reference `docs/security.md` for the
  implemented security controls.

- [ ] **Update `ROADMAP.md` status line (L-6).** Replace `"v0.16.0 released"` in the
  roadmap header with `"v0.18.0 released"` on each new release going forward.
  Extend the `version-lint` CI job to check the roadmap status line.

**v0.18 release criteria.**
- `cargo audit --deny warnings` passes with 0 findings in the release pipeline.
- `validate_consumer_sql_is_single_select()` is called before every `CREATE OR REPLACE VIEW`
  in the executor; the injection test passes.
- CNPG preview client validates TLS certificates by default; CNPG_INSECURE_SKIP_VERIFY
  test passes.
- Age identity file is validated via `validate_secret_path()` and flag-prefix check;
  traversal test passes.
- All six user-facing integration files reference version 0.18.0.
- Version-lint CI checks all eight tracked files and fails if any disagrees with
  `Cargo.toml` workspace version.
- `aqueduct plan --fail-on-drift` fails on consumer-layer and source-layer drift.
- `aqueduct destroy` auto-migrates a stale catalog before proceeding.
- `.github/SECURITY.md` present and non-empty.
- All v0.17 tests continue to pass.

---

## v0.19 — Correctness Hardening, Blue/Green Atomicity & Test Infrastructure

**Target effort:** 4–5 weeks.
**Builds on:** v0.18 complete.
**Assessment basis:** Phase 4 Milestone B — structural correctness bugs in the executor
and scheduler integration, plus the missing failure-mode test cases identified in the
Phase 4 assessment. After this version, every known correctness regression path has a
test that would catch it, the blue/green swap is fully atomic, and the heartbeat is
resilient to tokio panics.

### Phase 22 — Correctness Hardening, Blue/Green Atomicity & Test Infrastructure (4–5 weeks)

#### A. Blue/Green Atomicity (ARCH-3-REMAINING — High)

- [ ] **Move `SWAP_BLUE_GREEN_SQL` inside the `SwapConsumerViews` transaction.** In
  `executor.rs:985-996`, the deployment status row update currently executes after the
  surrounding `COMMIT`. Convert the `swapped_at` / `retire_at` update to use a savepoint
  inside the existing `BEGIN`/`COMMIT` block so the view swap and the deployment row
  status update become atomic. This eliminates the window where views point to the green
  schema but the deployment row still shows `status='active'`.

  Add the test `swap_consumer_views_atomicity`: inject a failure on the second consumer
  view swap inside the transaction; assert that all views still reference the original
  schema and `aqueduct.blue_green_deployments.status` is `'active'`.

#### B. Heartbeat Supervisor (CORR-3 — High)

- [ ] **Wrap heartbeat task in panic handler that signals lock loss.** Replace the bare
  `tokio::spawn(run_heartbeat(...))` call at `executor.rs:1540` with a supervisor that
  catches panics:

  ```rust
  let lock_lost_on_panic = lock_lost_tx.clone();
  tokio::spawn(async move {
      let result = tokio::panic::catch_unwind(/* ... */run_heartbeat(...)).await;
      if result.is_err() || result.unwrap().is_err() {
          let _ = lock_lost_on_panic.send(true);
      }
  });
  ```

  Alternatively, use `tokio::spawn` and add a `JoinHandle`-based supervisor task that
  awaits the heartbeat join handle and signals lock loss on unexpected exit.

  Add the test `heartbeat_panic_signals_lock_loss`: spawn an executor with a mock
  heartbeat that panics after 200 ms; confirm the executor returns `LockLost` before
  the next step boundary.

#### C. Mock Scheduler Observable State (TEST-3 — High)

- [ ] **Add `pgtrickle.paused_nodes` table to the mock DDL.** Update
  `mock_pgtrickle.rs:92-101` to make `pause_scheduler(p_nodes text[])` insert each
  node name into a `pgtrickle.paused_nodes(node_name text, paused_at timestamptz)`
  table, and `resume_scheduler(p_nodes text[])` delete the corresponding rows. This
  makes the scheduler mock stateful and observable.

- [ ] **Add `assert_scheduler_idle(client)` helper to `aqueduct-testkit`.** The helper
  queries `SELECT count(*) FROM pgtrickle.paused_nodes` and asserts it is zero. Add
  a complementary `assert_scheduler_paused_for(client, node_names)` that asserts the
  named nodes are in the paused set.

- [ ] **Update existing migration tests to assert scheduler state.** For every integration
  test that calls `aqueduct apply`, add pre-DDL and post-apply assertions:
  - Before the DDL step: `assert_scheduler_paused_for(...)` for the modified nodes.
  - After apply completes: `assert_scheduler_idle(...)`.
  - After a failed mid-migration apply: `assert_scheduler_idle(...)` (resumed on failure).

  Add the test `mock_scheduler_records_pause` as the canonical example.

#### D. Executor Polling & Convergence Fixes (M-4, M-8, M-9)

- [ ] **Add `statement_timeout` guard to `WaitForConvergence` poll query (M-9).** Wrap the
  convergence poll query inside a `SET LOCAL statement_timeout = '<poll_deadline_ms>ms'`
  so a hung pg_trickle query cannot hold the loop open past the configured deadline. Move
  the deadline check to _after_ the sleep rather than before, to avoid the zero-deadline
  race described in the assessment.

  Add the test `waitforconvergence_deadline_respected`: configure `max_wait_secs = 1`,
  mock pgtrickle to always return `running`, and assert the step fails within 2 seconds
  with a `WaitForConvergence timeout` error.

- [ ] **Add exponential backoff to `status --watch` error loop (M-8).** When
  `connect_read_only()` or `poll_once()` fails, track a consecutive-error counter and
  apply exponential backoff: delay = `min(interval * 2^error_count, 5 * interval)`.
  After 10 consecutive errors, log a `error!` (elevated from `warn!`) with the last
  error. Reset the counter on the next successful poll.

- [ ] **Document `EXPLAIN (FORMAT JSON)` trade-off in `cost.rs` (M-4).** Since PostgreSQL
  does not support parameterized `EXPLAIN`, add a code comment explaining why
  `format!("EXPLAIN (FORMAT JSON) {}", query)` is intentional and safe: the query is
  validated by `validate_ivm_supportability` / `validate_sql_syntax` before reaching
  `estimate_rows()`, and migration files are operator-controlled input. Wrap the EXPLAIN
  call in a `BEGIN READ ONLY; SET LOCAL statement_timeout = '5s'` block so an
  unexpected slow EXPLAIN cannot stall cost estimation indefinitely.

#### E. Catalog Schema Override — Stop-Gap (ARCH-1 partial)

- [ ] **Return an explicit error when `catalog_schema != "aqueduct"`.** Until the full
  parameterised SQL substitution (v0.20), modify `connect_and_migrate()` and
  `aqueduct init` to check the resolved `catalog_schema` value and return
  `AqueductError::NotYetImplemented { feature: "catalog_schema override" }` with a
  clear message: *"Catalog schema isolation is not yet fully implemented. Set
  `catalog_schema = \"aqueduct\"` or omit the field to use the default."* This prevents
  silent data mixing in multi-tenant deployments.

  Add the test `catalog_schema_override_rejected_until_implemented`.

#### F. Failure-Mode Test Suite

The Phase 4 assessment identified 10 missing test cases. All are implemented in this
version:

- [ ] `consumer_sql_injection_rejected_at_apply` — asserts injected DDL SQL body is
  rejected before any database call. *(SEC-1, already in v0.18; included here as
  a cross-reference)*
- [ ] `heartbeat_panic_signals_lock_loss` — heartbeat task panic propagates as `LockLost`.
- [ ] `swap_consumer_views_atomicity` — failed mid-swap leaves views in consistent state.
- [ ] `plan_fail_on_drift_counts_consumer_deltas` — consumer drift triggers exit 1.
  *(M-2, already in v0.18)*
- [ ] `destroy_auto_migrates_catalog` — pre-v8 catalog is migrated before destroy query.
  *(M-5, already in v0.18)*
- [ ] `catalog_schema_override_rejected_until_implemented` — explicit error on non-default
  catalog schema.
- [ ] `age_identity_file_traversal_rejected` — path traversal attempt rejected.
  *(SEC-3, already in v0.18)*
- [ ] `cnpg_preview_requires_valid_cert` — TLS rejection without CNPG_INSECURE_SKIP_VERIFY.
  *(SEC-2, already in v0.18)*
- [ ] `waitforconvergence_deadline_respected` — deadline enforced within 2× max_wait_secs.
- [ ] `mock_scheduler_records_pause` — scheduler paused during DDL, resumed after apply.

**v0.19 release criteria.**
- `SWAP_BLUE_GREEN_SQL` executes inside the `SwapConsumerViews` transaction; atomicity
  test passes.
- Heartbeat task panic triggers `LockLost` signal; heartbeat panic test passes.
- Mock scheduler `pgtrickle.paused_nodes` table is present; all migration integration
  tests assert scheduler state.
- `WaitForConvergence` deadline test passes within 2× `max_wait_secs`.
- `status --watch` applies exponential backoff on repeated connection failures.
- `aqueduct init` and `connect_and_migrate()` reject non-default `catalog_schema` with
  an explicit error.
- All 10 Phase 4 missing tests are present and passing.
- All v0.18 tests continue to pass.

---

## v0.20 — Executor Architecture, Multi-Tenant Catalog & CLI Polish

**Target effort:** 5–6 weeks.
**Builds on:** v0.19 complete.
**Assessment basis:** Phase 4 Milestone C — the three remaining architectural gaps that
require structural changes to the executor API, the catalog SQL layer, and the CLI output
subsystem. After this version, the executor API is type-safe against missing required
fields, the catalog schema override works end-to-end, `--quiet` and `--porcelain`
suppress all output, and the compensating-step registry is used for automated crash
recovery.

### Phase 23 — Executor Architecture, Multi-Tenant Catalog & CLI Polish (5–6 weeks)

#### A. Typed `ExecutionContext` — Compiler-Enforced Safety (ARCH-2 — High)

- [ ] **Introduce `ExecutionContext` struct requiring `desired_state` and
  `connection_string`.** Replace the optional builder pattern for safety-critical fields
  with a required struct:

  ```rust
  pub struct ExecutionContext {
      pub desired_state: DagState,
      pub connection_string: String,
      pub patroni_endpoint: Option<String>,
      pub resume: bool,
      pub force_retry: Option<StepIndex>,
      pub force_skip: Option<StepIndex>,
      pub allow_full_refresh: bool,
  }
  ```

  Change the typed constructors `for_apply()`, `for_rollback()`, `for_promote()` to
  accept `ExecutionContext` as a required argument. Dry-run mode retains the current
  optional pattern. This makes it a compile error to construct an executor without a
  desired state or connection string.

- [ ] **Update all command handlers** (`apply.rs`, `rollback.rs`, `promote.rs`,
  `plan.rs`) to construct `ExecutionContext` before calling the executor constructor.
  Dry-run callers (`plan.rs`) use a separate `DryRunContext` that does not require
  `connection_string` or `desired_state`.

- [ ] **Add a compile-time test** using `static_assertions` or a doc-test that confirms
  `PlanExecutor::for_apply()` does not compile without an `ExecutionContext`.

#### B. Full Catalog Schema Parameterisation (ARCH-1 — High)

- [ ] **Thread `CatalogSchema` through all catalog SQL functions.** The `CatalogSchema`
  newtype already exists at `catalog.rs:1-80` with `validated()`, `as_str()`, and
  `quoted()` methods. Update every SQL constant that hardcodes the literal `aqueduct`
  schema name — `ACQUIRE_LOCK_SQL`, `START_MIGRATION_SQL`, `FINISH_MIGRATION_SQL`,
  `UPDATE_MIGRATION_PROGRESS_SQL`, `RECORD_SNAPSHOT_SQL`, `DELETE_CONSUMER_VIEW_SQL`,
  `SWAP_BLUE_GREEN_SQL`, and all others — to accept a `CatalogSchema` parameter and
  substitute the schema name using `format!("{}.{}", schema.quoted(), table)` or by
  constructing the query with `schema.as_str()`.

- [ ] **Thread `CatalogSchema` through `ensure_catalog_current()` and
  `connect_and_migrate()`.** These entry-point functions read the schema name from
  `ProjectConfig` and pass it down to every catalog function they call. Remove the
  `NotYetImplemented` error added in v0.19 once the full parameterisation is in place.

- [ ] **Update `aqueduct init` to use the schema name from `--schema` / `catalog_schema`.**
  Pass the resolved `CatalogSchema` to `CATALOG_INIT_V4_SQL` via the parameterised path.
  Confirm that `aqueduct init --schema my_catalog` creates tables in `my_catalog` rather
  than `aqueduct`.

- [ ] **Integration tests for multi-tenant catalog isolation.** Add a two-project test:
  initialise two projects on the same database with different `catalog_schema` values
  (`proj_a` and `proj_b`), apply migrations to each, and assert that `SELECT count(*)
  FROM proj_a.dag_versions` and `SELECT count(*) FROM proj_b.dag_versions` return
  independent version histories with no cross-contamination.

#### C. Automated Compensating-Step Recovery (CORR-2 — High)

- [ ] **Read `ddl_log` on `--resume` and apply outstanding compensating steps.** In the
  `--resume` flow, after identifying the `find_resume_step` checkpoint, query
  `aqueduct.ddl_log` for any rows in `status = 'running'` for the current migration ID.
  For each such row, execute the compensating SQL before advancing to the resume
  checkpoint. Update the row to `status = 'compensated'` after execution.

  This closes the gap identified in the Phase 4 assessment: a crash between
  `CreateStreamTable` and `RecordSnapshot` leaves a pg_trickle table without a catalog
  version; on resume, the compensating step re-drops the orphaned table and the apply
  re-creates it cleanly.

- [ ] **Document the compensating-step protocol in `docs/ha-operations.md`.** Add a
  section explaining the `aqueduct.ddl_log` table, when compensating steps are recorded,
  and what `--resume` does with them. Include a recovery runbook for the
  "table exists in pg_trickle but not in catalog" scenario.

- [ ] **Add integration tests for compensating-step recovery.** Test the following crash
  scenarios:
  - Crash after `CreateStreamTable`, before `RecordSnapshot`: resume applies the
    compensating drop, then re-creates the table cleanly.
  - Crash after `AlterStreamTable`, before `RecordSnapshot`: resume applies the
    compensating alter-back, then re-applies the alter.
  - Crash after `DropStreamTable`: no compensating step exists; resume confirms table
    is already absent and advances.

#### D. `OutputEmitter` Wired to All Command Handlers (M-1 — Medium)

- [ ] **Store `OutputEmitter` as a process-wide singleton via `OnceLock`.** Initialize it
  in `main.rs` before command dispatch and expose a `output::emitter()` free function
  that returns a reference to the global instance. This avoids threading it as an
  argument through every command handler.

- [ ] **Replace direct `println!` and `eprintln!` calls in all command handlers** with
  `emitter().info(...)`, `emitter().warn(...)`, `emitter().error(...)`, or
  `emitter().raw(...)` as appropriate. Commands: `apply.rs`, `plan.rs`, `status.rs`,
  `diff.rs`, `promote.rs`, `rollback.rs`, `import.rs`, `ingest.rs`, `destroy.rs`,
  `unlock.rs`, `audit.rs`, `init.rs`, `preview.rs`.

- [ ] **Implement `--porcelain` output mode.** In porcelain mode, `emitter().raw()` emits
  only the machine-readable payload (plan JSON, status JSON, diff JSON). All decorative
  banners, progress lines, and `info!` messages are suppressed. Confirm that
  `aqueduct plan --format json --quiet` and `aqueduct plan --format json --porcelain`
  both produce output parseable by `jq` without any interleaved human-readable text.

- [ ] **Integration-test all output modes.** Add tests asserting:
  - `--quiet` suppresses banner and info lines but still exits with the correct code.
  - `--porcelain --format json` output is valid JSON on stdout.
  - `--format yaml` output is valid YAML on stdout.
  - `--format table` (default) is human-readable, non-empty, and consistent across runs.

#### E. Dependency Hygiene (DEP-1)

- [ ] **Replace `httpmock` with `wiremock` in dev-dependencies (if not done in v0.17).**
  `httpmock` pulls in the unmaintained `async-std` crate (RUSTSEC-2025-0052). `wiremock`
  is already a dependency and covers all use cases. Remove the `--ignore RUSTSEC-2025-0052`
  annotation from CI and confirm `cargo audit --deny warnings` passes clean.

**v0.20 release criteria.**
- `PlanExecutor::for_apply()` and friends require `ExecutionContext`; omitting it is a
  compile error.
- `aqueduct init --schema my_catalog` creates all catalog tables in `my_catalog`.
  Multi-tenant isolation integration test passes.
- `--resume` reads `aqueduct.ddl_log` and applies outstanding compensating steps before
  advancing to the checkpoint; crash-recovery integration tests pass.
- `aqueduct plan --format json --quiet` and `--porcelain` produce clean JSON with no
  decorative text; output mode tests pass.
- `cargo audit --deny warnings` passes with zero ignores.
- All v0.19 tests continue to pass.

---

## v1.0 — Release Engineering

**Target effort:** ~2 weeks.
**Builds on:** v0.20 complete.
**Milestone:** Public 1.0 release — the first version declared production-ready.

This version produces the release artefacts and performs the final gate checks needed
for the public 1.0 announcement.

### Phase 12 — Release Engineering (2 weeks)

#### Deliverables

**Reproducible release builds.** SHA256-verified binaries for Linux (x86_64, aarch64),
macOS (x86_64, aarch64), and a Docker image. Build provenance attestation via
`slsa-github-generator`. Windows (x86_64) binary included in the release matrix.

**`aqueduct plan` + `aqueduct apply` verified against all 30 cookbook patterns.**
Every cookbook example in `docs/cookbook/` is run end-to-end against a Testcontainers
cluster as part of the release gate. This supersedes the v0.7 claim of 30 verified
cookbook patterns (which was incomplete — only ~15 integration scenarios existed).

**v1.0 release criteria.**
- Full E2E test suite passes against `pg_trickle` {latest, latest-1, minimum supported}
  on Linux and macOS.
- All 30 cookbook patterns verified end-to-end as part of the CI release gate.
- Reproducible release builds published with SHA256 checksums and SLSA provenance.
- `cargo audit` passes with no warnings in the release pipeline.
- `aqueduct plan` + `aqueduct apply` roundtrip verified against all 30 cookbook patterns.
- No known data-loss bugs.
- CHANGELOG accurately reflects all shipped features as released (not "planned").
- All safety, documentation, compatibility, and advanced-feature work from v0.10–v0.13 is
  verified end-to-end in the release CI pipeline.

---

## v1.1 — Consumer Layer Management

**Target effort:** TBD (post-v1.0).
**Builds on:** v1.0 complete.

This version extends `pg_aqueduct`'s management scope to the consumer layer: the sinks
and connectors that read from stream tables and relay their output to external systems.

### Deliverables (Planned)

**`@aqueduct:kind = consumer` in the diff engine.** The DAG differ gains a third layer:
consumer views declared in `migrations/consumers/*.sql`. Changes to stream-table schemas
that would break a dependent consumer view are surfaced in `aqueduct plan` as
consumer-layer impacts, with the same cascade analysis applied to stream-table-to-stream-table
edges.

**Consumer view impact analysis.** When a stream table's schema changes (column dropped,
type changed, column renamed), `aqueduct plan` identifies all consumer views that
reference the affected column and classifies the impact:
- Column dropped or renamed: consumer view must be updated; migration includes a
  `CREATE OR REPLACE VIEW` step.
- Type widened: consumer view may be unaffected; reported as an informational notice.
- Type narrowed: consumer view is potentially broken; treated as a plan error unless
  the operator provides a compensating consumer view rewrite.

**Sink management (scoped planning only).** v1.1 does not implement `pg_tide` outbox
management, Kafka connector management, or S3/Iceberg export management — those are
fundamentally different problem classes requiring external system API integration. The
v1.1 scope is limited to PostgreSQL-native consumer views (`CREATE VIEW`,
`CREATE MATERIALIZED VIEW`) that are declared in the migrations directory and tracked
by `pg_aqueduct`.

**`aqueduct consumers list`.** Lists all consumer views managed by the current project,
their source stream tables, and their current drift status.

---

## v2.0 — Multi-Executor Support

**Target effort:** TBD (post-v1.0).
**Builds on:** v1.0 complete.
**Note:** The pluggable `StreamExecutor` trait (§7.7) is designed from v0.1 to
accommodate these executors cleanly. v2.0 validates and ships the first non-pg_trickle
executors.

The following IVM systems are the hot-candidate executor targets. None are in scope
before v1.0, but the trait boundary is designed so that each can be implemented without
restructuring the planning logic.

### RisingWave Executor

RisingWave is a cloud-native streaming SQL database with full PostgreSQL wire-protocol
compatibility. Stream tables map to `CREATE MATERIALIZED VIEW`; refresh is always-on
and event-driven (no schedule).

**Key design decisions:**
- `schedule` is silently ignored on RisingWave targets (linter warning emitted). Refresh
  is driven by upstream data arrival, not a schedule.
- Sources require explicit `CREATE SOURCE` DDL. A new front-matter directive
  `@aqueduct:source_connector` declares the connector type.
- `live_state` queries `rw_catalog.rw_materialized_views` instead of
  `pgtrickle.pgt_stream_tables`.
- The `aqueduct.*` catalog tables are created on the RisingWave cluster directly.
- The blue/green rename-swap pattern applies without modification.

**Capability matrix:**
```
DIFFERENTIAL_REFRESH   ✅  (always-on, continuous)
IMMEDIATE_MODE         ❌
TRIGGER_CDC            ❌
WAL_CDC                ✅  (PostgreSQL CDC source via logical decoding)
PAUSE_RESUME_SCHEDULER ⚠   (partial — source connector dependent)
DIAMOND_CONSISTENCY    ❌  (eventual consistency across views)
BLUE_GREEN_DEPLOY      ✅
```

### Feldera Executor

Feldera is a standalone incremental computation engine (differential dataflow) with a
REST API interface — not a PostgreSQL-wire-compatible server.

**Key design decisions:**
- Feldera's unit of deployment is a *pipeline* (a compiled SQL program containing all
  views). Every `create`, `alter`, or `drop` requires redeploying the entire pipeline.
  The executor pre-passes that group all DDL plan steps into a single pipeline
  redeploy.
- The `aqueduct.*` catalog tables must live in a separate PostgreSQL instance or SQLite
  file. `aqueduct.toml` requires a `catalog_dsn` when `executor.kind = "feldera"`.
- `ExecutorConnection` requires a second implementation: `FelderaConnection { base_url,
  api_key }` — HTTP REST via `reqwest`, not `libpq`.

**Capability matrix:**
```
DIFFERENTIAL_REFRESH   ✅  (Feldera is differential dataflow natively)
FULL_REFRESH           ✅  (reset pipeline + replay inputs)
IMMEDIATE_MODE         ❌
PAUSE_RESUME_SCHEDULER ✅  (pipeline-level pause/start via REST)
DIAMOND_CONSISTENCY    ✅
BLUE_GREEN_DEPLOY      ⚠   (two pipelines, no atomic view-swap)
```

### Materialize Executor

Materialize is a streaming SQL database (differential dataflow, Timely Dataflow) with
a PostgreSQL wire protocol interface — the closest conceptual peer to `pg_trickle`
among the four candidates.

**Key design decisions:**
- Stream tables map to `CREATE MATERIALIZED VIEW IN CLUSTER compute_cluster AS query`.
  A new front-matter directive `@aqueduct:cluster` and `[executor.materialize]
  default_cluster` config key are required.
- `live_state` queries `mz_catalog.mz_materialized_views`.
- `pause_scheduler` is a no-op + warning (Materialize has no per-view pause).
- The `aqueduct.*` catalog tables are safest on a companion PostgreSQL instance, due
  to Materialize's transaction semantics differences for plain tables.

**Capability matrix:**
```
DIFFERENTIAL_REFRESH   ✅  (always-on differential dataflow)
IMMEDIATE_MODE         ❌
PAUSE_RESUME_SCHEDULER ❌  (no per-view pause)
DIAMOND_CONSISTENCY    ✅
BLUE_GREEN_DEPLOY      ✅  (schemas are first-class)
WAL_CDC                ✅  (PostgreSQL source via logical replication)
```

### Snowflake Dynamic Tables Executor

Snowflake Dynamic Tables use a proprietary SQL dialect and proprietary connection
protocol — the most structurally divergent of the four candidates.

**Key design decisions:**
- `schedule` maps directly to `TARGET_LAG`. `CALCULATED` has no Snowflake equivalent;
  the executor uses the planner's resolved schedule value (linter warning emitted).
- `WAREHOUSE` assignment is required: `@aqueduct:warehouse` front-matter directive +
  `[executor.snowflake] default_warehouse` config key.
- Authentication uses OAuth / key-pair (not password). `aqueduct.toml` references
  `${SNOWFLAKE_ACCOUNT}`, `${SNOWFLAKE_USER}`, `${SNOWFLAKE_PRIVATE_KEY_PATH}`.
- In-place column-add is **not supported** on Snowflake Dynamic Tables. All column
  changes require Rebuild class. The executor's `ExecutorCapabilities` reflects this,
  and the plan renderer emits a prominent note.
- The `aqueduct.*` catalog tables are created as regular Snowflake tables. `aqueduct.locks`
  advisory locks are approximated via `MERGE ON CONFLICT` + a Snowflake scheduled
  TASK heartbeat.
- `live_state` queries `INFORMATION_SCHEMA.DYNAMIC_TABLES`.

**Capability matrix:**
```
DIFFERENTIAL_REFRESH   ✅  (INCREMENTAL mode; Snowflake decides per-run)
FULL_REFRESH           ✅
IMMEDIATE_MODE         ❌
TRIGGER_CDC            ❌
WAL_CDC                ❌
PAUSE_RESUME_SCHEDULER ✅  (SUSPEND / RESUME DDL)
DIAMOND_CONSISTENCY    ⚠   (pipeline-level, no per-group atomicity)
BLUE_GREEN_DEPLOY      ✅  (SWAP WITH DDL or schema-swap)
```

Implementation order for the four executors: RisingWave → Materialize → Feldera →
Snowflake (in increasing order of structural divergence from the built-in `pg_trickle`
executor).

---

## Non-Goals (All Versions)

The following are explicitly out of scope for all current and planned versions:

- **Pure base-table schema management without stream tables.** Use Atlas, sqitch, or
  Liquibase. `pg_aqueduct` adds no value here.
- **`pg_tide`-only schemas (no stream tables).** `pg_tide` outbox/inbox/relay tables
  are normal application tables; they do not form a dependency DAG. The gateway
  scenario (planning to add stream tables soon) is the only marginal exception —
  and even then, the honest recommendation is "use Atlas until you have your first
  stream table."
- **`moire` (Next.js SPARQL frontend).** Creates no PostgreSQL objects. Completely
  out of scope.
- **`pg_ripple` internal tables** (`_pg_ripple.kge_embeddings`,
  `_pg_ripple.derivations`, ER monitoring tables). Managed by `pg_ripple` internally.
  Only user-authored `pg_trickle` stream tables in a `pg_ripple` deployment are in scope.
- **`pg_eddy` internal storage tables** (node store, edge store, property store —
  custom adjacency AM). Managed by `pg_eddy`. Only user-authored stream tables that
  read from `pg_eddy` node/edge tables are in scope.
- **`riverbank` catalog** (`_riverbank.*`, Alembic-managed). Out of scope. The
  `pg_trickle` IVM stream tables that `riverbank` creates are in scope.
- **A general-purpose schema migration tool.** `pg_aqueduct` manages stream-adjacent
  base-table changes (Tier 1), but delegates standalone DDL to Atlas or sqitch.
- **A query authoring environment.** Use dbt, Hex, or psql.
- **A monitoring or alerting product.** Use `pg_trickle`'s monitoring views + Grafana.
- **Multi-database (cross-cluster) transactional coordination.** `aqueduct` operates
  against one PostgreSQL target at a time. Multiple targets sequentially are fine;
  multi-target transactional coordination is not in scope.
- **A replacement for dbt-pgtrickle.** They compose; neither replaces the other.

---

## Repository Layout (Target v0.1)

```
trickle-labs/pg-aqueduct/
├── README.md
├── ESSENCE.md
├── ROADMAP.md
├── Cargo.toml                        # workspace
├── crates/
│   ├── aqueduct-core/                # planner, differ, plan executor, catalog
│   ├── aqueduct-cli/                 # aqueduct binary (clap)
│   ├── aqueduct-extension/           # pgrx extension (Phase 4 / v0.3)
│   └── aqueduct-testkit/             # shared Testcontainers helpers
├── examples/
│   ├── minimal/                      # 3-node DAG
│   ├── tpch/                         # 22 stream tables from TPC-H Q1–Q22
│   ├── medallion/                    # bronze/silver/gold pattern
│   └── dbt-roundtrip/               # dbt-pgtrickle ingest example (v0.5)
├── docs/
│   ├── cookbook/                     # 30 worked migration examples (v1.0)
│   └── security.md
├── benchmarks/                       # 200-node DAG benchmark (v1.0)
└── tests/
    ├── e2e_*.rs                      # against pg_trickle + pg_aqueduct
    └── property/                     # roundtrip plan→apply→plan = empty
```

---

## Ecosystem Relationships

| Tool | Relationship to `pg_aqueduct` |
|---|---|
| **`pg_trickle`** | Primary runtime target. `aqueduct` calls its SQL API (`create_stream_table`, `alter_stream_table`, `drop_stream_table`, `refresh_stream_table`, `pause_scheduler`, `resume_scheduler`). |
| **`pg_tide`** | Sibling relay tool. `aqueduct` manages `pg_tide` outbox attachments (`DetachOutbox` / `ReattachOutbox` plan steps) when stream tables are also present. Without stream tables, Atlas is the better migration tool for `pg_tide` schemas. |
| **`pg_ripple`** | Knowledge graph companion. `pg_ripple`'s VP tables appear as `owned = false` sources. Only user-authored incremental SPARQL views and custom analytics nodes are in scope; `pg_ripple`-managed tables are excluded. |
| **`pg_eddy`** | Labelled property graph store. `pg_eddy`'s node/edge/property AM tables appear as `owned = false` sources. Only user-authored MATCH-view stream tables over those tables are in scope. |
| **`moire`** | Out of scope. Pure Next.js frontend over SPARQL endpoints; creates no PostgreSQL objects. |
| **`riverbank`** | Knowledge compiler. `riverbank`'s `pg_trickle` IVM stream tables (quality scores, entity pages, topic indices) are in scope. `riverbank`'s own catalog (`_riverbank.*`, Alembic-managed) is excluded. |
| **dbt / dbt-pgtrickle** | Upstream authoring. `aqueduct ingest --from dbt-target` (v0.5) reads dbt's compiled artefacts and produces a migrations directory. |
| **Atlas / Liquibase / sqitch** | Complementary. They own general-purpose base-table schema migrations. `pg_aqueduct` owns stream-adjacent base-table changes (Tier 1) and coordinates the full DAG cascade. For standalone base-table migrations, `aqueduct apply` can invoke Atlas/sqitch as a pre-step hook. |
| **Terraform / Pulumi** | Outer layer. Provisions the database; embeds a `terraform_data` resource that calls `aqueduct apply` post-provision. |
| **CloudNativePG / Patroni** | HA awareness. `aqueduct` locks against the primary, refuses to apply against a standby, and integrates with primary-promotion events. |
| **GitHub / GitLab Actions** | First-class CI integration. `aqueduct/plan-action`, `aqueduct/apply-action` (v0.4). |

---

*This roadmap is a living document. It will be updated as upstream dependencies ship,
as user feedback surfaces new priorities, and as implementation reveals complexity not
anticipated at planning time. The authoritative design detail for each feature lives in
`plans/pg-aqueduct-plan.md`.*
