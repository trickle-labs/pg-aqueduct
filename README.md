# pg_aqueduct

**Declarative schema evolution and migration for stream-table DAGs.**

`pg_aqueduct` is the missing migration tool for teams running [`pg_trickle`](https://github.com/trickle-labs/pg-trickle) in production. Where Atlas manages relational schema and Terraform manages infrastructure, `pg_aqueduct` manages the third axis that neither tool covers: the *evolution of a streaming, incrementally-maintained DAG of materialized views* over time — without losing differential state, without taking the pipeline offline, and without making the topology of your stream tables your problem to figure out by hand.

> **Status:** v0.7.0 — Documentation & Cookbook release. 197 tests passing (101 unit + 63 integration + 33 CLI). Implementation complete through Phase 8 of the roadmap.

---

## The Problem

If you operate a `pg_trickle` deployment, your stream-table DAG is your most valuable database asset. It is also the hardest one to evolve safely. Adding a column to a base table, changing an aggregation, splitting a node into two — any of these changes today requires a `drop_stream_table()` followed by a full recreate, which discards the materialized rows, triggers an expensive full refresh that can take minutes or hours on a large table, and forces every downstream stream table to also be recreated in exactly the right topological order. There is no tooling that enforces that order, no preview of what will break, and no rollback if something goes wrong mid-migration.

For a small five-node DAG this is an inconvenience. For a 200-node production DAG it is an outage.

The pain comes in five concrete forms. First, drop-and-recreate destroys differential state — consumers see an empty table for the full duration of the rebuild and every CDC offset accumulated since the last full refresh is discarded. Second, topological ordering is entirely the user's responsibility — apply changes in the wrong sequence and downstream queries fail to parse or silently return wrong data. Third, out-of-band `ALTER TABLE` statements on base tables can silently invalidate downstream stream-table queries, and the only response today is another round of drop-and-recreate. Fourth, there is no declarative source of truth — the `pg_trickle` catalog lives in the database, and whatever imperative SQL created it may live in a one-off psql session, a dbt model, a Liquibase changeset, or nowhere at all. Fifth, promoting a DAG change from dev to staging to prod is a hand-rolled sequence of psql calls with no tooling support.

`pg_aqueduct` solves all five.

---

## Quick Start

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

A typical plan output looks like this:

```
$ aqueduct plan --to prod

Project  checkout-analytics  v18 → v19
Target   prod (pg_trickle 0.62, pg 16.3)

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

A migrations directory contains `*.sql` files with front-matter directives — SQL line comments with an `@aqueduct:` prefix:

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM raw.orders
GROUP BY customer_id;
```

The project file (`aqueduct.toml`) declares named targets:

```toml
[project]
name = "checkout-analytics"

[targets.dev]
dsn  = "postgresql://localhost/checkout_dev"

[targets.prod]
dsn  = "${AQUEDUCT_PROD_DSN}"

[apply]
lock_timeout       = "30s"
maintenance_window = "02:00-04:00 UTC"
```

### Rollback

Every `aqueduct apply` writes a full snapshot of the prior DAG definition into `aqueduct.dag_versions`. Rolling back to a previous version always computes a single forward migration plan from the current state to the target version. `aqueduct rollback` enforces a *lossless window*: for rebuild-class migrations, once the new data has replaced the old in a full refresh, rollback requires an explicit `--accept-data-loss` flag.

---

## Performance

For metadata-only changes (Free class), `pg_aqueduct` applies only the changed nodes rather than rebuilding the entire DAG. On a 200-node DAG with a 5-node schedule change:

| Strategy | Apply time |
|----------|-----------|
| pg_aqueduct targeted | ~6 ms |
| Drop/recreate baseline | ~180 s (estimate) |

See [`benchmarks/`](benchmarks/) for full results.

---

## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/api-reference.md) | CLI commands, front-matter directives, `aqueduct.toml` |
| [Cookbook](docs/cookbook/README.md) | 30 worked migration patterns |
| [Security Guide](docs/security.md) | Least-privilege role, secret backends, audit trail |
| [HA Operations](docs/ha-operations.md) | Patroni, CloudNativePG, maintenance windows |
| [ROADMAP](ROADMAP.md) | Development phases and release criteria |
| [CHANGELOG](CHANGELOG.md) | Version history |

---

## Project Structure

```
pg-aqueduct/
├── Cargo.toml                    # workspace (version 0.7.0)
├── crates/
│   ├── aqueduct-core/            # planner, differ, plan executor
│   ├── aqueduct-cli/             # aqueduct binary
│   └── aqueduct-testkit/         # shared Testcontainers helpers
├── examples/
│   ├── minimal/                  # 3-node DAG example
│   └── dbt-roundtrip/            # dbt manifest ingestion example
├── docs/
│   ├── api-reference.md
│   ├── ha-operations.md
│   ├── security.md
│   └── cookbook/                 # 30 worked migration patterns
├── benchmarks/                   # 200-node DAG benchmark results
└── ci/
    ├── gitlab/
    └── hooks/
```

---

## Why Not dbt? Why Not pg_trickle?

`pg_aqueduct` occupies a distinct niche that neither tool covers. dbt orchestrates *builds* — stateless, declarative, recompute-everything-per-run. `pg_trickle` orchestrates *transitions* — the differential delta from one moment to the next. Neither owns the third axis: the evolution of the DAG structure itself over time.

The existing `dbt-pgtrickle` package gives dbt users a `materialized='stream_table'` macro. `pg_aqueduct` is a separate tool for a separate audience: the platform or SRE team that operates the stream DAG, not the analytics engineer who authors models. The two compose cleanly — `aqueduct ingest --from dbt-target target/` reads the compiled `manifest.json` and produces an aqueduct migration.

---

## License

[Apache 2.0](LICENSE) — `trickle-labs/pg-aqueduct`
