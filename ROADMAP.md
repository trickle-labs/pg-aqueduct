# pg_aqueduct Roadmap

> **Status:** v0.6 implementation complete. See checklist below.
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
| [v1.0](#v10--release-engineering) | Release engineering | 9 | 1 week |
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

## v1.0 — Release Engineering

**Target effort:** ~1 week.
**Builds on:** v0.7 complete.
**Milestone:** Public 1.0 release.

This version produces the release artefacts and performs the final gate checks needed
for the public 1.0 announcement.

### Phase 9 — Release Engineering (1 week)

#### Deliverables

**Reproducible release builds.** SHA256-verified binaries for Linux (x86_64, aarch64),
macOS (x86_64, aarch64), and a Docker image. Build provenance attestation via
`slsa-github-generator`.

**v1.0 release criteria.**
- Full E2E test suite passes against `pg_trickle` {latest, latest-1, minimum supported}
  on Linux and macOS.
- Reproducible release builds published with SHA256 checksums.
- `aqueduct plan` + `aqueduct apply` roundtrip verified against all 30 cookbook patterns.
- No known data-loss bugs.

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
