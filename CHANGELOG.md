# Changelog

What's new in pg_aqueduct — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
- [v0.1.0 — Initial Implementation](#v010--initial-implementation)
- [Unreleased — Repository Bootstrap](#unreleased--repository-bootstrap)
<!-- TOC end -->

---

## [v0.1.0] — Initial Implementation

**Released:** 2026-05-18
**Tag:** [`v0.1.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.1.0)

The first working release of `pg_aqueduct`. All Phase 0, Phase 1, and Phase 2
roadmap items are complete. The full plan → apply → rollback → import cycle runs
against real PostgreSQL via Testcontainers with no skipped tests.

### What's New

#### Eight CLI Commands

| Command | Description |
|---|---|
| `aqueduct init` | Bootstrap the `aqueduct.*` catalog schema; optionally scaffold a new project |
| `aqueduct plan` | Compute the migration plan from desired state (`.sql` files) vs live state; output as text, JSON, or Markdown |
| `aqueduct apply` | Execute the plan; supports `--dry-run` and `--resume` for crash recovery |
| `aqueduct status` | Show current version, pg_trickle info, and drift summary |
| `aqueduct validate` | Offline syntax and IVM-supportability check — no database required |
| `aqueduct rollback` | Revert to a prior DAG version via a forward migration plan |
| `aqueduct import` | Bootstrap migration files from a live pg_trickle deployment |
| `aqueduct unlock` | Release a stale project lock (emergency use) |

#### Migration Planning

- Parses `migrations/streams/*.sql` and `migrations/sources/*.sql` with
  `-- @aqueduct:key = value` front-matter for `kind`, `schedule`, `refresh_mode`,
  `cdc_mode`, `depends_on`, and `owned`.
- Infers dependency edges from SQL `FROM`/`JOIN` clauses using `sqlparser` (pure
  Rust, no C build dependencies).
- Detects cycles in the DAG and reports them clearly before any DDL runs.
- Classifies every change into one of four cost classes: **Free**, **In-place**,
  **Rebuild**, or **Blue/green**.
- Template variable substitution: `{{ var.NAME }}` expands from per-target `vars`
  in `aqueduct.toml`; `${ENV_VAR}` expands from the environment.

#### Catalog Schema

An `aqueduct.*` schema is bootstrapped on first `aqueduct init`. It tracks:
- `aqueduct.dag_versions` — full plan snapshot at each successful apply
- `aqueduct.migrations` — every migration run with status, progress, and CLI version
- `aqueduct.locks` — project-level serialisation lock (crash-safe via TTL expiry)
- `aqueduct.cluster_profile` — per-cluster calibration data

#### Safety Features

- `allow_full_refresh = false` in `aqueduct.toml` blocks any Rebuild-class plan.
- `maintenance_window` gates Rebuild and Blue/green steps to a time window.
- `aqueduct apply` refuses to run against a hot standby (`pg_is_in_recovery()`).
- `plan` and `status` open read-only transactions and cannot mutate data.
- Locks are automatically cleaned up on crash via TTL expiry.

#### Observability

- Structured JSON logs on stderr when `--log-format json` is set, or when
  `$CI`, `$GITHUB_ACTIONS`, `$GITLAB_CI`, or `$CIRCLECI` are present.
- `application_name` is set to `aqueduct/<project>/<migration_id>` on every
  connection, making in-flight migrations visible in `pg_stat_activity`.

#### Testing

- **42 unit tests** covering config, parser, DAG, differ, classifier, plan
  builder, renderer, and SQL validator.
- **10 integration tests** using Testcontainers + a mock `pg_trickle` schema
  installed on vanilla PostgreSQL (no real extension required).
- **6 CLI integration tests** covering the full end-to-end lifecycle.
- All 58 tests pass; none are skipped.

#### CI / CD

Five GitHub Actions jobs run on every pull request:
- **Lint** — `cargo fmt --check` + `cargo clippy -D warnings`
- **Unit tests** — `cargo test --lib --all` on Ubuntu and macOS
- **Integration tests** — Testcontainers (Docker) on Ubuntu
- **CLI integration tests** — Testcontainers (Docker) on Ubuntu
- **Coverage** — cargo-tarpaulin on `aqueduct-core`

#### Other

- `examples/minimal/` — 3-node DAG example (`orders` source → `order_totals`
  stream → `customer_tiers` stream) with a complete `aqueduct.toml`.
- `ESSENCE.md` — architecture overview and design principles.
- `justfile` — developer convenience recipes (`test`, `lint`, `fmt`, `coverage`).

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
