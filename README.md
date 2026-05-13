# pg_aqueduct

**Declarative schema evolution and migration for stream-table DAGs.**

`pg_aqueduct` is the missing migration tool for teams running [`pg_trickle`](https://github.com/trickle-labs/pg-trickle) in production. Where Atlas manages relational schema and Terraform manages infrastructure, `pg_aqueduct` manages the third axis that neither tool covers: the *evolution of a streaming, incrementally-maintained DAG of materialized views* over time — without losing differential state, without taking the pipeline offline, and without making the topology of your stream tables your problem to figure out by hand.

> **Status:** Planning / pre-implementation — no code committed yet.

---

## The Problem

If you operate a `pg_trickle` deployment, your stream-table DAG is your most valuable database asset. It is also the hardest one to evolve safely. Adding a column to a base table, changing an aggregation, splitting a node into two — any of these changes today requires a `drop_stream_table()` followed by a full recreate, which discards the materialized rows, triggers an expensive full refresh that can take minutes or hours on a large table, and forces every downstream stream table to also be recreated in exactly the right topological order. There is no tooling that enforces that order, no preview of what will break, and no rollback if something goes wrong mid-migration.

For a small five-node DAG this is an inconvenience. For a 200-node production DAG it is an outage.

The pain comes in five concrete forms. First, drop-and-recreate destroys differential state — consumers see an empty table for the full duration of the rebuild and every CDC offset accumulated since the last full refresh is discarded. Second, topological ordering is entirely the user's responsibility — apply changes in the wrong sequence and downstream queries fail to parse or silently return wrong data. Third, out-of-band `ALTER TABLE` statements on base tables can silently invalidate downstream stream-table queries, and the only response today is another round of drop-and-recreate. Fourth, there is no declarative source of truth — the `pg_trickle` catalog lives in the database, and whatever imperative SQL created it may live in a one-off psql session, a dbt model, a Liquibase changeset, or nowhere at all. Fifth, promoting a DAG change from dev to staging to prod is a hand-rolled sequence of psql calls with no tooling support.

`pg_aqueduct` solves all five.

---

## What It Is

`pg_aqueduct` is distributed as a **Rust CLI binary** (`aqueduct`) — the primary user-facing component — plus an **optional companion Postgres extension** that adds DDL event triggers for passive drift detection and SQL-callable diagnostic views. The CLI talks to PostgreSQL through a standard `libpq` connection and requires no superuser privileges beyond what `pg_trickle` itself already needs.

The user-facing model is intentionally close to Atlas and Terraform. You maintain a directory of `*.sql` migration files and an `aqueduct.toml` project file. The CLI reads that directory, computes a *plan* by diffing the desired DAG state against the live catalog, and executes the plan against the target database in topologically-correct order, preserving materialized state wherever the IVM mathematics allow.

```sh
aqueduct init                     # scaffold a project and bootstrap the catalog
aqueduct import --from prod       # bootstrap from an existing pg_trickle deployment
aqueduct plan --to prod           # diff desired vs actual; produce a human-readable plan
aqueduct apply --to prod          # execute the plan and record the migration
aqueduct status --to prod         # show drift, last migration, refresh lag
aqueduct rollback --to prod       # revert to the previous DAG version
aqueduct validate                 # offline check — no database connection needed
aqueduct preview --branch feat-x  # spin up a sampled preview DAG for a PR
aqueduct destroy --to prod        # tear down the entire project's stream-table DAG
```

Every migration is recorded in `aqueduct.dag_versions`, a catalog table that the CLI bootstraps itself on `aqueduct init` — no `CREATE EXTENSION` is required for core functionality. This is the same approach used by Atlas, Flyway, and Liquibase for their own history tables.

---

## How It Works

### The Plan

`aqueduct plan` computes a diff across three sources of truth simultaneously: the *desired state* parsed from your migrations directory, the *actual state* read from `pgtrickle.pgt_stream_tables` and `pg_class`, and the *recorded history* in `aqueduct.dag_versions`. The result is a typed, ordered plan where every stream-table change is classified into one of four migration kinds:

| Class | Example | Behaviour |
|---|---|---|
| **Free** | Schedule change, CDC mode change | A single `alter_stream_table()` call; no rebuild, no data loss |
| **In-place** | Add a passthrough column, add an aggregate, widen a type | `ALTER` plus a targeted incremental backfill; materialized rows preserved |
| **Rebuild** | Change `GROUP BY` keys, change a join condition | Drop + recreate + full refresh; optionally gated by a maintenance window |
| **Blue/green** | Restructure the topology of a sub-DAG | Build a parallel green DAG, backfill it, atomically swap consumer views |

Before emitting any plan step, `aqueduct plan` validates every new or changed query through two passes: a parse check via `pg_query.rs` and an IVM-supportability check that applies the same rules `pg_trickle` uses at `create_stream_table` time. This means a plan error surfaces at planning time — not halfway through an apply — and gives you a precise explanation of why a query cannot be executed differentially, along with the option to downgrade to a full refresh or rewrite the query.

A typical plan output looks like this:

```
$ aqueduct plan --to prod

Project  checkout-analytics  v18 → v19
Target   prod (pg_trickle 0.62, pg 18.3)

Changes  3 nodes affected

  ~ order_totals     [in-place]  add column discount_total (SUM)
  + promo_summary    [create]    new stream table (DIFFERENTIAL, schedule 30s)
  ! customer_ytd     [rebuild]   WHERE predicate changed → requires full rebuild

  Step                          Rows (est)   Duration (est)   Class
  ────────────────────────────────────────────────────────────────────
  ALTER stream order_totals          —            < 1s         in-place
  BACKFILL order_totals           1.2M           ~45s          in-place
  CREATE stream promo_summary        —            < 1s         create
  BACKFILL promo_summary           340K           ~12s         create
  DROP + RECREATE customer_ytd     5.8M           ~3m          rebuild ⏰

⏰ Rebuild steps will be deferred to the maintenance window (02:00–04:00 UTC).
   Pass --ignore-maintenance-window to run immediately.
```

### The Project Format

A migrations directory contains `*.sql` files with front-matter directives — SQL line comments with an `@aqueduct:` prefix. The format is intentionally minimal so that diffs in pull requests are readable by humans and so that interop with dbt-compiled artefacts is straightforward.

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "{{ var.schedule }}"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM raw.orders
GROUP BY customer_id;
```

The project file (`aqueduct.toml`) declares named targets and per-target variables, so the same migration directory can be applied to dev, staging, and production with different schedules, CDC modes, and connection strings:

```toml
[project]
name = "checkout-analytics"

[targets.dev]
dsn  = "postgresql://localhost/checkout_dev"
vars = { schedule = "5m", cdc_mode = "trigger" }

[targets.prod]
dsn  = "${AQUEDUCT_PROD_DSN}"
vars = { schedule = "30s", cdc_mode = "wal" }

[apply]
lock_timeout       = "30s"
maintenance_window = "02:00-04:00 UTC"
allow_full_refresh = false
```

### Rollback

Every `aqueduct apply` writes a full snapshot of the prior DAG definition into `aqueduct.dag_versions`. Rolling back to a previous version — even skipping multiple intermediate versions — always computes a single forward migration plan from the current state to the target version. There is no cascading replay of individual rollback steps, which avoids the failure modes common to reverse-migration schemes. `aqueduct rollback` enforces a *lossless window*: for rebuild-class migrations, once the new data has replaced the old in a full refresh, rollback requires an explicit `--accept-data-loss` flag and a clear warning explaining what has been lost.

### Locking and Safety

Concurrent `aqueduct apply` runs are serialised through a row-level lock in `aqueduct.locks` — not an advisory lock, but a regular `SELECT FOR UPDATE NOWAIT` that Postgres automatically releases on disconnect. During long-running migrations such as large backfills, a background heartbeat task keeps the lock alive. If the CLI crashes, the heartbeat stops, the lock expires after one TTL interval, and `aqueduct apply --resume` (from the same or a different process) can take over by reading the progress checkpoint stored in `aqueduct.migrations`.

---

## Why Not dbt? Why Not pg_trickle?

`pg_aqueduct` occupies a distinct niche that neither tool covers. dbt orchestrates *builds* — stateless, declarative, recompute-everything-per-run. `pg_trickle` orchestrates *transitions* — the differential delta from one moment to the next. Neither owns the third axis: the evolution of the DAG structure itself over time. dbt has no concept of preserving materialized state across a schema change, no notion of a migration version history, and no understanding of the topological dependencies between stream tables. Adding this to dbt would mean adding a stateful migration engine to a fundamentally stateless tool — the wrong shape.

The existing `dbt-pgtrickle` package gives dbt users a `materialized='stream_table'` macro. That is the right level of integration. `pg_aqueduct` is a separate tool for a separate audience: the platform or SRE team that operates the stream DAG, not the analytics engineer who authors models. The two compose cleanly — dbt-pgtrickle generates the SQL, and `aqueduct ingest --from dbt-target target/` reads the compiled `manifest.json` and produces an aqueduct migration.

`pg_trickle` itself is deliberately scoped to the *runtime engine*: change capture, differential refresh, scheduling, single-query validation. Adding a project-level orchestrator into the extension would bloat its surface, conflate two separate trust boundaries (runtime correctness vs. CI/CD tooling), and force a release cadence mismatch between changes to the IVM engine and changes to the migration planner. The right shape — established by the extraction of `pg_tide` as a standalone relay — is a separate repository, a thin optional extension for the database-side capabilities that require it, and a Rust binary that does the real work over `libpq`.

---

## Ecosystem Integrations

`pg_aqueduct` is designed to compose with the tools already in your stack. It invokes Atlas or sqitch as a pre-step hook for standalone base-table migrations that have no stream-table cascade. For any `ALTER TABLE` on a column that feeds at least one stream table, `pg_aqueduct` generates and executes both the base-table `ALTER` and the full downstream cascade in a single coordinated plan. It understands high-availability setups — refusing to apply against a hot standby and integrating with Patroni and CloudNativePG primary-promotion events. It emits `aqueduct/<project>/<migration_id>` as the `application_name` on every Postgres connection so in-flight migrations are visible in `pg_stat_activity`. In CI environments it detects GitHub Actions, GitLab CI, and CircleCI and automatically switches to structured JSON log output suitable for log aggregation pipelines.

The tool also integrates cleanly with the broader `trickle-labs` ecosystem. `pg_ripple` VP tables (per-predicate RDF storage) and `pg_eddy` node/edge storage tables appear automatically as `owned = false` sources — `pg_aqueduct` tracks their schema for impact analysis but never generates DDL for them. User-authored stream tables over those sources are fully in scope. `riverbank`'s Python IVM creation code can be migrated to managed mode (where `pg_aqueduct` owns the stream table lifecycle) or kept in import-only mode (where `aqueduct import` periodically reconciles what Python created).

---

## When Not to Use It

`pg_aqueduct`'s value is entirely dependent on the presence of a stream-table DAG. Without one, it adds operational complexity with no benefit. If your schema consists only of base tables with no `pg_trickle` stream tables, use Atlas, sqitch, or Liquibase — they are mature, well-documented, and purpose-built for that problem. If you are using `pg_tide` outboxes and inboxes but have no stream tables yet, Atlas remains the right answer; `pg_tide`'s own tables do not form a dependency DAG and `pg_aqueduct` has no analytical leverage over them.

The honest threshold is this: `pg_aqueduct` earns its operational complexity the moment your first stream table exists and that table's defining query references a column that could change. Before that moment, it is a solution to a future problem. The migration from Atlas to `pg_aqueduct` when you do cross that threshold is not painful — the migrations directory simply gains stream-table SQL files over time.

---

## Implementation Roadmap

The project is structured as a Rust workspace in `trickle-labs/pg-aqueduct`, with crates for the core planner and differ (`aqueduct-core`), the CLI binary (`aqueduct-cli`), the optional pgrx companion extension (`aqueduct-extension`), and shared Testcontainers test helpers (`aqueduct-testkit`).

Development is planned across seven phases. **Phase 0** (3 days) bootstraps the repository, Cargo workspace, CI matrix, and `aqueduct init`. **Phase 1** (1.5 weeks) delivers a fully useful read-only `aqueduct plan` and `aqueduct status` — useful for understanding your DAG's evolution even before apply exists. **Phase 2** (2 weeks) adds the plan executor, locking, `aqueduct apply`, crash-resume, rollback, and `aqueduct import` for onboarding existing deployments. **Phase 3** (2 weeks) implements the full migration classifier with in-place evolution paths and the `--explain-cost` flag. **Phase 4** (3 weeks) adds blue/green deployments, preview environments, and the optional companion extension. **Phase 5** (1 week) ships first-class GitHub Actions and GitLab CI integrations, `aqueduct fmt`, and `aqueduct lint`. **Phase 6** (1 week) delivers the dbt interop (`aqueduct ingest`). **Phase 7** (3 weeks) hardens the tool to v1.0 with multi-environment promotion, HA failover integration, fuzzing, and a migration cookbook of 30 worked examples.

Total estimated effort to a usable v0.1 (plan + apply + rollback + import) is **4–8 weeks** for one engineer. v1.0 with online schema evolution, blue/green deployments, drift detection, CI integrations, and the optional extension is a **14–22 week** project.

---

## Architecture Overview

```
trickle-labs/pg-aqueduct/
├── Cargo.toml                        # workspace
├── crates/
│   ├── aqueduct-core/                # planner, differ, plan executor
│   ├── aqueduct-cli/                 # aqueduct binary
│   ├── aqueduct-extension/           # pgrx extension (Phase 4; optional)
│   └── aqueduct-testkit/             # shared Testcontainers helpers
├── examples/
│   ├── minimal/                      # 3-node DAG
│   ├── tpch/                         # 22 stream tables from TPC-H Q1–Q22
│   └── medallion/                    # bronze/silver/gold pattern
├── docs/
└── tests/
    ├── e2e_*.rs                      # against pg_trickle + pg_aqueduct
    └── property/                     # roundtrip plan→apply→plan = empty
```

The planner (`aqueduct-core`) is target-system-agnostic. It operates on abstract `StreamTableSpec` values and emits abstract `Plan` step sequences. All target-system knowledge lives in a thin `StreamExecutor` trait implementation that the planner calls at apply time. This boundary is designed explicitly so that `pg_aqueduct` can serve IVM systems beyond `pg_trickle` — RisingWave, Materialize, Feldera, Snowflake Dynamic Tables — without restructuring any of the planning logic.

---

## License

[MIT](LICENSE) — `trickle-labs/pg-aqueduct`
