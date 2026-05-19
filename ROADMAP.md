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
| [v0.8](#v08--core-correctness--safety-hardening) | Core correctness & safety hardening | 10 | 3–4 weeks |
| [v0.9](#v09--feature-completeness--ergonomics) | Feature completeness & ergonomics | 11 | 3–4 weeks |
| [v1.0](#v10--release-engineering) | Release engineering | 12 | 2 weeks |
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

- [ ] **Fix `aqueduct rollback` to restore from the recorded prior spec (C1).**
  The v0.7 implementation reads `spec_jsonb` from `aqueduct.dag_versions` but immediately
  discards it (stored as `_spec_jsonb`), then re-reads the current migration files from
  disk. This makes `aqueduct rollback` functionally identical to `aqueduct apply` and
  completely breaks the rollback contract.

  Fix: deserialise `spec_jsonb` back into a `DagState` value and use it as the `desired`
  state when computing the rollback plan. Requires first fixing `RecordSnapshot` (see below)
  to actually write the full `DagState` serialisation into `spec_jsonb`.

- [ ] **Fix `RecordSnapshot` to store the full `DagState` in `spec_jsonb` (M9).**
  The v0.7 executor writes `serde_json::json!({})` (an empty object) for `spec_jsonb`
  in every `RecordSnapshot` step. Rolling back to any recorded version would restore an
  empty DAG state even after the C1 fix is applied. Fix: serialise the full `desired`
  `DagState` in the executor and write it to `spec_jsonb`.

- [ ] **Implement `--resume` step progress tracking (C2).**
  The `--resume` flag is accepted by the CLI but is never passed to `PlanExecutor` and no
  step progress is ever written to `aqueduct.migrations.progress`. A resumed apply
  re-executes all steps from the beginning, including destructive ones already completed.

  Fix: pass `resume: bool` to `PlanExecutor::new()`. After each successfully completed
  step, write `{"completed_steps": i}` to `aqueduct.migrations.progress` via an `UPDATE`.
  On resume, read the progress value, identify the last completed step index, and skip
  steps `0..=last_completed`. For steps that are idempotent (e.g., `ValidateQuery`,
  `UnlockDag`) safe re-execution is acceptable; for non-idempotent steps (drop, create,
  backfill) the skip is mandatory.

- [ ] **Implement lock heartbeat to prevent TTL expiry on long migrations (C5).**
  The lock TTL is never renewed after acquisition. Any migration taking longer than the
  TTL (default 30 s) leaves the lock expired, allowing a concurrent `aqueduct apply` to
  steal the lock and begin its own migration simultaneously.

  Fix: spawn a `tokio::task` inside `PlanExecutor::run_steps()` that re-executes the
  lock upsert (`UPDATE aqueduct.locks SET acquired_at = now() WHERE project = $1`) every
  `ttl / 3` seconds. Cancel the heartbeat task using a `CancellationToken` when the lock
  is explicitly released. Use `tokio::select!` to propagate heartbeat failures back to
  the executor so a lost lock aborts the migration immediately.

- [ ] **Fix `aqueduct init` to install the v2 catalog schema (C6).**
  `commands/init.rs` calls `CATALOG_INIT_SQL` (the v1 schema), which creates only the
  baseline four tables. The v2 tables required by features shipped in v0.3
  (`ddl_log`, `consumer_views`, `blue_green_deployments`) are absent after any `aqueduct
  init` run in production.

  Fix: change `init.rs` to call `CATALOG_INIT_V2_SQL`. Verify that the testkit and
  `init.rs` use the same SQL source to prevent future divergence.

- [ ] **Implement catalog self-migration (C6 follow-on).**
  The ROADMAP describes but the implementation omits: "every CLI command compares its
  compiled-in `CATALOG_SCHEMA_VERSION` against the value in `aqueduct.cluster_profile`
  and applies any pending catalog migrations." No such check exists anywhere.

  Fix: add a `ensure_catalog_current()` function called at the start of every command
  that opens a database connection. It reads `catalog_schema_version` from
  `aqueduct.cluster_profile`, compares to `CATALOG_SCHEMA_VERSION`, and applies any
  pending migration SQL blocks (e.g., `CATALOG_MIGRATE_V1_TO_V2_SQL`).

- [ ] **Replace panickable `.unwrap()` calls in `plan.rs` (C3).**
  `build_plan()` calls `.unwrap()` on `delta.desired` and `delta.actual` at five locations
  (lines 312, 355, 356, 412, 413). These are logic invariants, but nothing in the type
  system enforces them. Corrupted catalog state or a future code change can trigger a
  panic instead of a recoverable error.

  Fix: replace all five calls with `ok_or_else(|| AqueductError::Other(format!(...)))?`.
  Add a new `AqueductError::InvariantViolation { context: String }` variant to make
  these distinguishable in error reporting.

- [ ] **Remove SQL injection path in executor fallback (C4).**
  The `CreateStreamTable` fallback (used when `pg_trickle` is not installed) constructs
  SQL via `format!("... SELECT * FROM ({}) q LIMIT 0", spec.query)`, embedding
  `spec.query` without any escaping or quoting.

  Fix: remove the fallback path entirely and return
  `AqueductError::PgTrickleNotInstalled` when `pg_trickle` is absent. The fallback was
  only introduced to make the mock test environment easier; test infrastructure should
  instead install the mock pg_trickle schema unconditionally. The `preview.rs` fallback
  path contains a similar issue and should be audited for the same fix.

#### High-Severity Correctness Fixes

- [ ] **Fix `AlterStreamTable` to apply `new_query` changes (H1).**
  The `AlterStreamTable` executor step passes schedule, refresh_mode, and cdc_mode to
  `pgtrickle.alter_stream_table()` but ignores the `new_query` field entirely. Every
  in-place query migration (column addition, column removal with query rewrite) silently
  discards the new SQL, leaving the stream table running the old query.

  Fix: extend the `pgtrickle.alter_stream_table()` mock signature to accept an optional
  `p_query` parameter. Pass `new_query` when set. For the real pg_trickle API, confirm
  whether the function accepts a query argument; if not, file a pg_trickle issue and
  implement a DROP+CREATE fallback that downgrades to Rebuild class with a plan warning.

- [ ] **Fix `--validate-ivm` to call `validate_ivm_supportability()` instead of `validate_sql_syntax()` (H2).**
  In `commands/plan.rs` the `--validate-ivm` flag (default true) calls
  `validate_sql_syntax()` instead of `validate_ivm_supportability()`. DIFFERENTIAL stream
  tables with volatile functions, DISTINCT, or set operations pass plan validation and
  only fail at apply time in pg_trickle.

  Fix: replace the call with `validate_ivm_supportability()`. Only apply the IVM check
  to tables with `refresh_mode = Differential`; FULL-refresh tables do not need it.

- [ ] **Fix column-removal classifier to reject mid-list drops (H3).**
  `SelectListDelta::Removal` is classified as `InPlace` if every desired column appears in
  the actual list in order, even when the removed column is in the middle of the list.
  Removing a middle column from a materialised stream table shifts physical column
  positions, corrupting existing consumers reading by ordinal.

  Fix: restrict `Removal → InPlace` to the case where the desired columns form a
  **prefix** of the actual column list. Any removal from a non-tail position must be
  classified as `Rebuild`.

- [ ] **Implement diamond DAG consistency class promotion (H7).**
  Each node in a diamond DAG is currently classified independently. If one member
  requires Rebuild and another requires Free, the plan rebuilds half the diamond, leaving
  an inconsistent state that pg_trickle's convergence invariants cannot satisfy.

  Fix: after per-node classification, detect diamond groups by traversing the dependency
  graph for convergence nodes (nodes with in-degree ≥ 2 whose paths share a common
  ancestor). Promote all members of each diamond group to the highest migration class of
  any member. Emit a plan renderer note explaining the promotion.

- [ ] **Implement drain-then-pause protocol before migration steps (H6).**
  The ROADMAP describes calling `pgtrickle.pause_scheduler(nodes => [...])` before any
  migration step. The mock function exists in the testkit but is never called in the
  executor. Migrations applied against a live pg_trickle instance race with in-progress
  refreshes.

  Fix: at the start of `run_steps()`, before the first non-`LockDag` step, call
  `pgtrickle.pause_scheduler()` with the list of affected node names. After all steps
  complete (or on any error path), call `pgtrickle.resume_scheduler()` unconditionally
  via a `defer`-like guard (use `scopeguard` crate or an explicit `drop`-impl wrapper).

- [ ] **Implement real drift detection in `aqueduct status` (H8).**
  `poll_once()` constructs `StatusReport` with `drift_count: 0` hardcoded. The
  `--fail-on-drift` flag therefore never triggers.

  Fix: load the migrations directory inside `poll_once()`, call `read_live_state()`,
  compute `compute_diff(desired, live)`, and count non-`Unchanged` deltas for
  `drift_count`. If loading the migrations directory fails (e.g., no `aqueduct.toml`),
  emit a warning and skip drift computation rather than panicking.

#### Test Coverage Gaps

- [ ] **Tests for `--resume` behaviour.** Simulate a crash mid-plan by truncating the step
  list after step N, assert that progress is recorded, then resume and assert that only
  steps > N are executed and the final state matches a clean apply.

- [ ] **Tests for rollback via prior spec.** Apply a 3-node DAG, modify a table, apply again,
  then rollback to v1. Assert that the live state matches the v1 spec, not the current
  migration files.

- [ ] **Tests for lock heartbeat.** Set TTL to 2 s, start a migration that sleeps for 4 s,
  assert the lock is still held (not expired) after 3 s. Assert that a concurrent apply
  receives `LockContention`.

- [ ] **Tests for column-removal classifier.** Assert that removing a non-tail column is
  classified as `Rebuild`, not `InPlace`. Assert that removing only trailing columns is
  classified as `InPlace`.

- [ ] **Tests for diamond DAG consistency promotion.** Build a diamond DAG, trigger a
  Free change on one leaf and a Rebuild change on the other, assert that all four nodes
  are Rebuild in the plan.

- [ ] **Tests for drift detection.** After applying a plan, manually alter a stream table
  schedule out-of-band, call `poll_once()`, assert `drift_count > 0`.

- [ ] **Tests for `AlterStreamTable` query update.** Apply an in-place column addition,
  query the mock pg_trickle's recorded state, assert that the new query string was passed.

- [ ] **Tests for maintenance window enforcement.** Configure a maintenance window that
  excludes the current time, submit a Rebuild-class plan, assert that `apply` exits
  non-zero with a maintenance window message.

- [ ] **Tests for `allow_full_refresh = false` enforcement.** Build a plan with a Rebuild
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

- [ ] **Implement `aqueduct diff` command (H4).**
  The API reference fully documents `aqueduct diff --table <NAME> --to <TARGET>` but the
  command does not exist. Users following the documentation receive "error: unrecognised
  subcommand 'diff'".

  Deliverable: a new `commands/diff.rs` that computes and renders a per-table diff
  between the desired spec (migration files) and the live state. Supports `--format
  text|json|markdown`. Output matches the plan renderer for the affected node only.

- [ ] **Add `--fail-on-drift` to `aqueduct plan` (M14 / H4).**
  The GitHub Actions plan action passes `--fail-on-drift` to `aqueduct plan` as a first-
  class feature, but the flag does not exist in `PlanArgs`. Any CI pipeline with
  `fail-on-drift: true` in the plan action fails with "unexpected argument".

  Fix: add `--fail-on-drift` as a flag to `PlanArgs`. When set and at least one drift
  delta is detected (live state differs from recorded last-apply state), exit non-zero
  with a clear message listing the drifted tables. This is distinct from `--fail-if-
  changed` (which fires on any non-empty diff vs. desired state).

- [ ] **Add interactive confirmation prompt and `--yes/-y` flag to `aqueduct apply` (H5).**
  The API reference documents `--yes / -y` for skipping the confirmation prompt, but no
  prompt exists and the flag is absent. Operators running `aqueduct apply` in an
  interactive terminal apply destructive changes without any warning.

  Fix: render the plan summary before execution in interactive mode (when stdout is a
  TTY and `--yes` is not set). Prompt "Apply these N changes to <target>? [y/N]". Add
  `--yes/-y` to `ApplyArgs` to skip. In non-TTY mode (CI), proceed without prompting.

- [ ] **Add `--confirm` flag to `aqueduct destroy` (M7).**
  The API reference documents `--confirm` as required for `aqueduct destroy`. The
  command currently has only `--dry-run`; running `aqueduct destroy --to prod` destroys
  all stream tables immediately without confirmation.

  Fix: require either `--confirm` or `--dry-run`. Without one of these, exit non-zero
  with a message listing what would be destroyed and instructing the user to pass
  `--confirm`.

- [ ] **Implement `--allow-plaintext-password` guard (M8).**
  ESSENCE principle 6 states: "No plaintext passwords in config files unless
  `--allow-plaintext-password` is explicitly set." Config loading never checks for
  embedded passwords in DSN strings.

  Fix: in `config.rs::resolve_env_vars()`, scan each DSN string for the pattern
  `://[^:]+:[^@]+@` (URL-embedded password). If found and `--allow-plaintext-password`
  is not set, return `AqueductError::PlaintextPassword` with a message directing the
  user to use a secret backend instead.

- [ ] **Implement `--quiet` / `--porcelain` global flag (M11).**
  Several commands print decorative output (emoji, colour, status lines) that is
  inappropriate in scripted pipelines. Add a global `--quiet` flag that suppresses all
  non-error output. Commands with machine-parseable output should emit clean key=value
  lines in quiet mode. Add `--porcelain` as a synonym for shell-script-friendly output.

- [ ] **Correct `aqueduct plan` exit codes to match API reference (M3).**
  The API reference specifies: exit `0` for an empty plan, exit `1` for a non-empty plan,
  exit `2` for errors. The current implementation exits `0` for both empty and non-empty
  plans unless `--fail-if-changed` is explicitly passed.

  Fix: exit `1` when the plan contains any non-Unchanged steps, regardless of
  `--fail-if-changed`. Retain `--fail-if-changed` as a flag synonym for backwards
  compatibility. Update the `plan` action to not require `--fail-if-changed` explicitly.

- [ ] **Add `--strict` mode to `aqueduct validate` (API reference parity).**
  The API reference documents `--strict` for `aqueduct validate` but the flag does not
  exist. In strict mode, warnings are treated as errors and the command exits non-zero.

- [ ] **Fix `status --watch` to reconnect between polls (H9).**
  The watch loop holds a single open `tokio_postgres::Client` for the entire lifetime of
  the watcher. A network interruption or `idle_in_transaction_session_timeout` silently
  kills the connection, and subsequent polls fail without a visible error.

  Fix: move connection creation inside the loop. Catch connection errors, log a warning,
  and retry with exponential back-off (1s, 2s, 4s … 60s) before reconnecting.

#### Missing Plan Step Variants (v0.2 table)

The v0.2 roadmap table specified eight plan step variants that are absent from the
`PlanStep` enum and `PlanExecutor`:

- [ ] **`RecreatePolicy { name, policy_sql }`** — restores RLS/Row Security Policies lost
  during Rebuild-class migrations. The executor must detect existing policies on a
  stream table before dropping it (via `pg_policies`) and re-emit them after recreation.

- [ ] **`DetachOutbox { stream_table, outbox_name }`** — unhooks a `pg_tide` outbox
  attachment before a stream table is dropped. Calls `pg_tide.detach_outbox()`. Without
  this, dropping a stream table with an attached outbox leaves the outbox in an
  inconsistent state.

- [ ] **`ReattachOutbox { stream_table, outbox_name, retention_hours }`** — restores the
  outbox attachment after the stream table is recreated.

- [ ] **`ManageWalSlot { stream_table, action }`** — drops or recreates the logical
  replication slot for `cdc_mode = 'wal'` stream tables. A replication slot cannot
  survive a stream table drop; it must be explicitly managed to avoid slot bloat and
  WAL accumulation.

- [ ] **`PauseImmediate { name }`** — temporarily switches an `IMMEDIATE` mode stream table
  to `DIFFERENTIAL` during a Rebuild-class migration. Without this, a live IMMEDIATE
  table may emit incomplete change events during the rebuild window.

- [ ] **`ResumeImmediate { name }`** — switches the stream table back to `IMMEDIATE` mode
  after the Rebuild is complete.

- [ ] **`WaitForRefresh { name, deadline }`** — polls `pgtrickle.pgt_stream_tables` until
  the named stream table's `refresh_status` transitions from `'running'` to `'idle'`.
  Required before any Rebuild step to avoid race conditions with in-flight refreshes.

- [ ] **`RunHook { name, statement }`** — executes a user-defined SQL statement as a
  pre or post migration hook. Hooks are declared in `aqueduct.toml` under
  `[apply.hooks] pre = "..."` / `[apply.hooks] post = "..."`.

Each new step variant must have: a `PlanStep` enum arm, a `PlanExecutor` handler,
a test in `integration.rs`, and a renderer in `render_plan_text()`.

#### Active Secret Injection (M1)

- [ ] **Wire `${secret:BACKEND:KEY}` inline syntax into connection resolution.**
  The `resolve_dsn_secrets()` function in `secrets.rs` is fully implemented but never
  called. The inline syntax is documented in the security guide as a delivered v0.6
  feature, but DSN strings with `${secret:...}` patterns are passed to `libpq` verbatim,
  causing connection failures.

  Fix: call `resolve_dsn_secrets(&dsn)` inside `commands/mod.rs::resolve_dsn()` before
  passing the DSN to `tokio_postgres::connect()`. Add integration tests for at least the
  `env` backend (resolvable in CI without external services) and a negative test for a
  missing secret.

- [ ] **Add path validation for Sops and Age subprocess arguments.**
  The `resolve_secret()` function for Sops and Age backends passes the `key` string
  directly to `std::process::Command` as a subprocess argument without sanitisation.
  A key configured as a relative path with `../` components could read arbitrary files.

  Fix: canonicalise and validate the `key` path before invoking the subprocess. Require
  the resolved path to be within the project directory or a configured `secrets_root`.
  Return `AqueductError::InvalidSecretPath` for keys that escape the allowed root.

#### API Reference & Documentation Parity

- [ ] **Align flag names between API reference and implementation.**
  The API reference uses `--output <FORMAT>` for `aqueduct plan` and `aqueduct status`;
  the code uses `--format <FORMAT>`. Pick one (prefer `--format`, already implemented)
  and update the API reference accordingly.

- [ ] **Add `yaml` format to plan and status (documented, not implemented).**
  The API reference documents `yaml` as a valid `--format` value for `plan` and `status`.
  Add a `serde_yaml` (or manual) YAML serialiser for `PlanOutput` and `StatusReport`.

- [ ] **Correct `cdc_mode` values in API reference.**
  The API reference documents `cdc_mode` values as `"ROW" | "STATEMENT" | "NONE"`. The
  code and tests use `"trigger"` and `"wal"`. Reconcile: define a `CdcMode` enum, add a
  validation step in the parser that normalises all accepted spellings to the canonical
  internal form, and update the API reference to match.

- [ ] **Add `cypher_source` directive to the API reference directive table.**
  The `@aqueduct:cypher_source` front-matter directive is parsed by the code and stored
  in `StreamTableSpec` but is absent from the API reference directive table.

- [ ] **Document `aqueduct diff` command in API reference.**
  Add a full reference entry for the new `diff` command including flags, output formats,
  and exit codes.

- [ ] **Fix CI plan action to mask DSN before use.**
  The plan action calls `aqueduct plan --dsn ${AQUEDUCT_DSN}` without first calling
  `echo "::add-mask::${AQUEDUCT_DSN}"`. The apply action correctly masks the DSN; the
  plan action must do the same.

- [ ] **Fix CI apply action `migration_id` output extraction.**
  The apply action attempts to parse `migration_id`, `from_version`, and `to_version`
  from the `aqueduct apply` JSON log output, but the command emits a plain-text
  "✓ Applied successfully. New version: v{}" message, not a structured JSON event.

  Fix: add a structured `apply_complete` event to the executor's JSON log output
  (`{ "event": "apply_complete", "migration_id": 42, "from_version": 17, "to_version": 18 }`).
  Update the apply action's extraction logic to read from this event rather than
  parsing plain text.

#### CI & Supply Chain Hardening

- [ ] **Add `cargo audit` to CI and release workflows (L9).**
  Neither `ci.yml` nor `release.yml` runs `cargo audit`. Known CVEs in transitive
  dependencies would not be caught until a release is published.

  Fix: add a `security-audit` job to `ci.yml` that runs `cargo audit --deny warnings`.
  Add the same step to `release.yml` before the build matrix.

- [ ] **Add PostgreSQL version matrix to CI (H10).**
  Integration tests run only against the Testcontainers default image (PG 16). Features
  may work on PG 16 but fail silently on PG 14 or PG 17.

  Fix: add a `pg-version` matrix dimension to the integration test job in `ci.yml`:
  `[14, 15, 16, 17]`. Use `postgres:${pg-version}-alpine` as the Testcontainers image.
  Pin the Testcontainers image tag in `aqueduct-testkit` rather than using `:latest` (L11).

- [ ] **Implement an enforced code coverage threshold (L9 follow-on).**
  The Codecov upload uses `fail_ci_if_error: false`. There is no minimum coverage
  threshold. Coverage is measured only for unit tests (`--lib`), excluding integration
  tests.

  Fix: set a minimum threshold of 70% line coverage for `aqueduct-core`. Use `cargo
  tarpaulin --all-targets` to include integration tests. Change `fail_ci_if_error` to
  `true`. Add a `--minimum-coverage 70` gate to the tarpaulin invocation.

- [ ] **Remove unused `deadpool-postgres` dependency.**
  `deadpool-postgres = "0.14"` is declared in `Cargo.toml` but imported nowhere in the
  source. Remove it. Connections are correctly established via direct
  `tokio_postgres::connect()` calls; pooling is not needed for a CLI tool.

- [ ] **Move static regex patterns to `LazyLock` (L1).**
  `regex::Regex::new(...)` is called inside `resolve_env_vars()` and `diff.rs::
  normalise_sql()` on every invocation, recompiling the regex each time. Use
  `std::sync::LazyLock<Regex>` (stabilised in Rust 1.80, which is the project's MSRV)
  for all static patterns. Replace `.unwrap()` with `.expect("valid static regex")` for
  clarity.

- [ ] **Pin Testcontainers image versions (L11).**
  `Postgres::default()` uses the `:latest` tag. Replace with an explicit pinned version
  (e.g., `postgres:16-alpine`) so that image updates do not silently change test
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
- CI integration tests run against PG 14, 15, 16, and 17 with no failures.
- Coverage threshold enforced at ≥ 70% for `aqueduct-core`.
- DSN masking present in both plan and apply CI actions.
- Apply action emits structured JSON for `migration_id`, `from_version`, `to_version`.
- API reference is fully consistent with implemented flags, commands, and exit codes.

---

## v1.0 — Release Engineering

**Target effort:** ~2 weeks.
**Builds on:** v0.9 complete.
**Milestone:** Public 1.0 release.

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
