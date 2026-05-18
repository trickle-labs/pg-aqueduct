# ESSENCE.md — `pg_aqueduct`

**One sentence:** `pg_aqueduct` is the missing migration tool for teams running `pg_trickle` in production — what Atlas is to a relational schema and Terraform is to infrastructure, `pg_aqueduct` is to a stream-table DAG.

---

## Scope

`pg_aqueduct` manages exactly one thing: **the lifecycle of a stream-table DAG that is powered by `pg_trickle`**. It does not manage base tables (use Atlas or Liquibase), application schemas (use Flyway), or monitoring dashboards (use Grafana). It occupies the narrow seam between "what is in git" and "what is in the `pg_trickle` catalog", and it does that job precisely.

## Non-Goals

- **Pure base-table schema management without stream tables.** If there are no `pg_trickle` stream tables, `pg_aqueduct` adds complexity with no benefit. Use Atlas.
- **`pg_tide`-only schemas.** `pg_tide` outbox/inbox/relay tables are normal application tables with no DAG structure. Use Atlas.
- **A replacement for dbt-pgtrickle.** The two compose cleanly: dbt generates the SQL; `pg_aqueduct` manages the migration lifecycle. Neither replaces the other.
- **A monitoring product.** `aqueduct status` is a quick health check, not a Prometheus exporter.
- **Multi-database transactional coordination.** One PostgreSQL target at a time.

## Why a Standalone Repository?

`pg_aqueduct` is a separate repository from `pg_trickle` for the same reasons `pg_tide` was extracted:

1. **Different trust boundaries.** The migration planner must not destabilise a live refresh loop. Bugs in the planner should never affect `pg_trickle`'s runtime correctness.
2. **Independent release cadence.** `pg_trickle` ships when the IVM engine changes. `pg_aqueduct` ships when CI integrations, file-format changes, or new diffing strategies land.
3. **Multi-version targeting.** `aqueduct` must be able to apply migrations against `pg_trickle` 0.x, 1.x, and future versions from a separately-versioned binary.
4. **Bloat avoidance.** `pg_trickle` deliberately stays below a dozen public SQL functions. A migration tool needs file I/O, git integration, plan/apply state, and CLI ergonomics that have no place in a `LANGUAGE C` Postgres extension.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  aqueduct CLI                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │   plan   │  │  apply   │  │  status  │  │ rollback │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       └─────────────┴─────────────┴──────────────┘         │
│                            │                                │
│               ┌────────────▼────────────┐                  │
│               │     aqueduct-core       │                  │
│               │  planner, differ,       │                  │
│               │  executor, catalog      │                  │
│               └────────────┬────────────┘                  │
└────────────────────────────┼────────────────────────────────┘
                             │  libpq (tokio-postgres)
                             ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │  ┌───────────┐  │
                    │  │pgtrickle  │  │
                    │  │ catalog   │  │
                    │  └───────────┘  │
                    │  ┌───────────┐  │
                    │  │ aqueduct  │  │
                    │  │ catalog   │  │
                    │  └───────────┘  │
                    └─────────────────┘
```

## Design Principles

1. **Plan before apply.** Every `aqueduct apply` computes a plan first. The plan is displayed to the user and recorded in `aqueduct.migrations` before any step executes.
2. **No implicit state.** Every command reads what it needs from the database. There is no local state file.
3. **Conservative classification.** When a delta cannot be proven safe for in-place migration, it falls back to Rebuild. A false Rebuild is annoying; a false In-place is data loss.
4. **Crash safety.** The plan executor checkpoints per-step progress in `aqueduct.migrations.progress`. `aqueduct apply --resume` can pick up after any crash.
5. **Least privilege.** The CLI documents the `aqueduct_admin` role and never requires superuser.
6. **No plaintext passwords in config files** unless `--allow-plaintext-password` is explicitly set.
