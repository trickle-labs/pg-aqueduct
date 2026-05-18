# Changelog

What's new in pg_aqueduct — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
- [Unreleased — Repository Bootstrap](#unreleased--repository-bootstrap)
<!-- TOC end -->

---

## [Unreleased] — Repository Bootstrap

### What's New

This is the initial state of `trickle-labs/pg-aqueduct`. No code has been
committed yet; this entry records the project's founding documents and the
design decisions locked in before implementation begins.

#### Design: Standalone Rust CLI, No Extension Required

`pg_aqueduct` is distributed as a standalone Rust CLI binary (`aqueduct`). It
bootstraps its own `aqueduct.*` catalog schema directly over a plain `libpq`
connection on first `aqueduct init` — the same approach used by Atlas, Flyway,
and Liquibase. No `CREATE EXTENSION` is required for core plan/apply/rollback
functionality, which means the tool works on day one against every managed
PostgreSQL service: RDS, Supabase, Neon, Azure Database, AlloyDB, CloudNativePG,
and Citus.

An optional companion extension (`pg_aqueduct`) will be added in v0.3 to enable
DDL event triggers for passive drift detection and SQL-callable diagnostic views.
Both the CLI-only and CLI+extension configurations are first-class and permanently
supported.

#### Design: Pluggable StreamExecutor Trait

All target-system knowledge is isolated behind a `StreamExecutor` trait. The
planner (DAG differ, classifier, plan renderer) is completely target-system-agnostic
and operates on abstract `StreamTableSpec` values. The built-in executor targets
`pg_trickle`; the trait boundary is designed so that RisingWave, Feldera,
Materialize, and Snowflake Dynamic Tables can be added in v2.0 without
restructuring any planning logic.

#### Design: Migration Classification — Four Cost Classes

Every stream-table change is classified into one of four migration classes before
any DDL is emitted:

| Class | Example | Cost |
|---|---|---|
| **Free** | Schedule change, CDC mode change | Single `pgtrickle.alter_stream_table()` call; no data touched |
| **In-place** | Add a passthrough column, add an aggregate, widen a type | `ALTER` + incremental backfill; existing rows preserved |
| **Rebuild** | Change `GROUP BY` keys, change a `WHERE` predicate, rename a column | Drop + recreate + full refresh |
| **Blue/green** | Restructure DAG topology (split or merge nodes) | Build green DAG in parallel, swap consumer views atomically |

This classification prevents the current state of the art — drop every node and
recreate from scratch — from being the only migration option for non-trivial changes
to a production DAG.

#### Design: Unordered Migrations Directory

Migration files have no sequence numbers or timestamps in their filenames.
`aqueduct plan` derives execution order entirely from DAG topology (dependency
edges extracted by `pg_query.rs`). This eliminates the merge-conflict problem
that plagues Flyway and Alembic when two branches add migrations concurrently:
two PRs can each add a new `.sql` file independently, and when both merge, the
next `aqueduct plan` sees two new nodes and orders them by their dependency
edges.

#### Design: Rollback Semantics and the Lossless Window

`aqueduct rollback` is always a single forward migration plan from the current
state to the desired prior spec — it never replays individual rollbacks in
reverse. For Free and In-place migrations rollback is always lossless. For
Rebuild-class migrations, rollback is lossless only within the current migration
window (before the next full refresh completes). The CLI reports the lossless
window expiry time and requires `--accept-data-loss` if the window has passed.

#### Design: Diamond DAG Consistency During Migration

Stream tables with `diamond_consistency = 'atomic'` membership are always
migrated as a unit. If any member of a diamond group is classified as Rebuild,
all members are upgraded to Rebuild and migrated together in a single locked
batch. The plan renderer explains the promotion. No migration may place diamond
group members in different schemas during a blue/green deployment.

#### Design: Security Posture

The CLI refuses plaintext passwords in config files unless `--allow-plaintext-password`
is explicitly set. DSN values in `aqueduct.toml` use environment variable references
(`${AQUEDUCT_PROD_DSN}`). The `plan` and `status` commands open read-only
transactions (`SET TRANSACTION READ ONLY`) and cannot accidentally mutate data.
Every `aqueduct apply` records the authenticated Postgres role, client IP, CLI
version, and full plan in `aqueduct.migrations` for compliance audit trails. A
documented `aqueduct_admin` least-privilege role requires no superuser, no
`CREATEROLE`, and no replication.

#### Documents Added

- `README.md` — project overview, quickstart, and positioning relative to dbt,
  `pg_trickle`, Atlas, and the broader `trickle-labs` ecosystem.
- `ROADMAP.md` — eight-version roadmap (v0.1 through v2.0) with full deliverable
  detail for each phase.
- `CHANGELOG.md` — this file.
- `plans/pg-aqueduct-plan.md` — the full design document: problem statement,
  architecture, implementation plan, 24 risk and open-question analyses, and the
  multi-executor capability profiles.
