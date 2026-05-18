# Changelog

What's new in pg_aqueduct — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
- [v0.5.0 — dbt Interop](#v050--dbt-interop)
- [v0.4.0 — CI Integrations & Ergonomics](#v040--ci-integrations--ergonomics)
- [v0.3.0 — Blue/Green, Preview Environments & Optional Extension](#v030--bluegreen-preview-environments--optional-extension)
- [v0.2.0 — Online Schema Evolution](#v020--online-schema-evolution)
- [v0.1.0 — Initial Implementation](#v010--initial-implementation)
- [Unreleased — Repository Bootstrap](#unreleased--repository-bootstrap)
<!-- TOC end -->

---

## [v0.5.0] — dbt Interop

**Released:** 2026-05-18
**Tag:** [`v0.5.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.5.0)

All Phase 6 roadmap items are complete. v0.5 makes `pg_aqueduct` a first-class
participant in dbt-pgtrickle workflows by introducing `aqueduct ingest`, a new command
that reads compiled dbt artefacts and generates a canonical aqueduct migrations
directory automatically.

### What's New

#### `aqueduct ingest --from dbt-target`

New `aqueduct ingest` command ingests compiled dbt artefacts into an aqueduct
migrations directory:

```bash
# After running `dbt compile`:
aqueduct ingest \
  --from dbt-target \
  --target ./target \
  --project-dir .
```

For each model materialised as `stream_table` via the `dbt-pgtrickle` package, the
command:

1. **Reads the compiled SQL** from `target/compiled/` (the output of `dbt compile`).
2. **Generates `migrations/streams/{model_name}.sql`** with the compiled SQL as the
   query body and front-matter directives populated from the dbt model config:
   - `+schedule` → `@aqueduct:schedule`
   - `+refresh_mode` → `@aqueduct:refresh_mode`
   - `+cdc_mode` → `@aqueduct:cdc_mode`
   - `+schema` → `@aqueduct:schema`
   - `+depends_on` → `@aqueduct:depends_on` (overrides auto-derived dependencies)
3. **Generates `migrations/sources/{name}.sql`** for each dbt source referenced by a
   stream-table model (`owned = false`).
4. **Produces a diff report** of what was created, updated, or left unchanged.

The command is **idempotent**: running it again when nothing has changed in the dbt
project writes nothing and exits cleanly.

Output formats:

```bash
aqueduct ingest --from dbt-target --target ./target           # human-readable text
aqueduct ingest --from dbt-target --target ./target --format json  # machine-readable
```

#### Workflow composition

The two tools compose cleanly; neither replaces the other:

- **dbt** generates the SQL (authoring tool for analytics engineers).
- **`aqueduct ingest`** translates the compiled artefacts into a migrations directory
  (one-time or periodic sync).
- **`aqueduct plan` / `aqueduct apply`** manages the migration lifecycle (platform
  team tool).

Once the migrations directory is in place, teams can run `aqueduct plan` to see
precisely what migration class each dbt model change requires — without any full
rebuilds that aren't strictly necessary.

#### `examples/dbt-roundtrip/`

A new worked example in `examples/dbt-roundtrip/` demonstrates the full round-trip:

1. A dbt project with three `stream_table` models (`order_totals`, `promo_summary`,
   `customer_ltv`).
2. A pre-compiled `target/` directory (the output of `dbt compile`).
3. Step-by-step README showing `ingest` → `validate` → `plan` → `apply` → `rollback`.

```
examples/dbt-roundtrip/
  dbt-project/           dbt source project
    models/
      order_totals.sql
      promo_summary.sql
      customer_ltv.sql
    dbt_project.yml
  target/                pre-compiled dbt target directory
    manifest.json
    compiled/analytics/models/
      order_totals.sql
      promo_summary.sql
      customer_ltv.sql
  aqueduct.toml
  README.md
```

---

## [v0.4.0] — CI Integrations & Ergonomics

**Released:** 2026-05-18
**Tag:** [`v0.4.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.4.0)

All Phase 5 roadmap items are complete. v0.4 wires `aqueduct plan` and `aqueduct apply`
into standard CI/CD pipelines and adds `aqueduct fmt` and `aqueduct lint` to make the
migrations directory a first-class software artefact.

### What's New

#### `aqueduct fmt`

New `aqueduct fmt` command canonicalises every migration file:

- **SQL keywords** are upper-cased (`select` → `SELECT`, `group by` → `GROUP BY`).
- **Front-matter directives** are emitted in a stable canonical order
  (`kind`, `owned`, `schema`, `depends_on`, `schedule`, `refresh_mode`, `cdc_mode`,
  `cypher_source`, `source`, `expose_as`).
- **Trailing whitespace** is stripped from every line.
- A single blank line separates front-matter from the SQL body.

`aqueduct fmt --check` exits non-zero if any file is not in canonical format — ideal
for CI gatekeeping.

```bash
# Fix all files in place:
aqueduct fmt

# Check-only mode (CI):
aqueduct fmt --check
```

#### `aqueduct lint`

New `aqueduct lint` command checks migration files for risky patterns:

| Rule | Level | Description |
|---|---|---|
| `schedule-too-aggressive` | warning | Schedule faster than 5 seconds |
| `full-refresh-no-filter` | warning | FULL refresh mode with no WHERE/LIMIT clause |
| `differential-ivm-unsupportable` | warning | DIFFERENTIAL query contains constructs that may trigger auto-downgrade to FULL |
| `cypher-source-missing` | error | `@aqueduct:cypher_source` points to a non-existent file |
| `consumer-source-not-found` | error | Consumer view references a stream table that does not exist |

```bash
aqueduct lint
aqueduct lint --fail-on-warn   # treat warnings as errors (CI strict mode)
aqueduct lint --format json    # machine-readable output
```

#### GitHub Actions

Two new composite actions ship in `.github/actions/`:

**`trickle-labs/pg-aqueduct/.github/actions/plan@v0.4.0`**
- Installs the `aqueduct` binary (pinned version, optional SHA256 checksum).
- Runs `aqueduct plan --format markdown`.
- Posts the plan as a PR comment (creates or updates via idempotent HTML marker).
- Supports `fail-on-drift`, `fail-if-changed`, `suppress-no-op`, and OIDC-free secret
  injection via a named secret.

**`trickle-labs/pg-aqueduct/.github/actions/apply@v0.4.0`**
- Runs `aqueduct apply` with `--resume` support for idempotent CI runs.
- Masks DSN secrets from logs via `::add-mask::`.
- Supports OIDC credential injection for AWS (via `aws-actions/configure-aws-credentials`)
  and GCP (via `google-github-actions/auth`).

Example workflows are provided in `.github/workflows/`:
- `aqueduct-plan.yml` — runs on PRs touching `migrations/` or `aqueduct.toml`
- `aqueduct-apply.yml` — runs on push to `main`

#### GitLab CI Templates

`ci/gitlab/aqueduct.gitlab-ci.yml` provides includable templates for GitLab CI:

```yaml
include:
  - project: 'trickle-labs/pg-aqueduct'
    ref: v0.4.0
    file: 'ci/gitlab/aqueduct.gitlab-ci.yml'
```

Two jobs: `aqueduct:plan` (posts plan as an MR note via GitLab Notes API) and
`aqueduct:apply` (runs on default branch push).

#### Pre-commit Hooks

`ci/hooks/aqueduct-pre-commit` is a drop-in git pre-commit hook that runs
`aqueduct fmt --check` and `aqueduct validate` on every commit that touches `migrations/`.

`.pre-commit-hooks.yaml` defines three hooks for the
[pre-commit](https://pre-commit.com/) framework:

```yaml
repos:
  - repo: https://github.com/trickle-labs/pg-aqueduct
    rev: v0.4.0
    hooks:
      - id: aqueduct-fmt
      - id: aqueduct-validate
      - id: aqueduct-lint
```

### Test Coverage

- 20 CLI integration tests (up from 14 in v0.3)
- 73 unit tests (up from 60 in v0.3)
- 25 database-backed integration tests (unchanged)
- **118 total** — all pass, none skipped

---

## [v0.3.0] — Blue/Green, Preview Environments & Optional Extension

**Released:** 2025-06-01
**Tag:** [`v0.3.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.3.0)

All Phase 4 roadmap items are complete. v0.3 delivers zero-downtime structural DAG
migrations via blue/green deployment, per-branch preview environments backed by
`TABLESAMPLE`-sampled data, consumer view management, and CLI-side support for the
optional `pg_aqueduct` companion extension.

### What's New

#### Blue/Green Deployer

New plan step variants orchestrate a zero-downtime structural migration:

- **`CreateGreenSchema`** — creates a parallel versioned schema for the new DAG
- **`CreateStreamTableInGreen`** — builds stream tables in the green schema
- **`WaitForConvergence`** — waits until the green DAG catches up with live data
- **`SwapConsumerViews`** — atomically redirects all consumer views to the green schema
- **`RetireBlueSchema`** — drops the old blue schema after the configured TTL

#### Consumer View Management

Stream tables can now be consumed through stable view indirection, enabling zero-
downtime blue/green cutover without breaking downstream consumers.

Declare consumer views in `migrations/consumers/`:

```sql
-- migrations/consumers/api_orders.sql
-- @aqueduct:kind = consumer
-- @aqueduct:source = public.order_totals
-- @aqueduct:expose_as = reporting.orders
SELECT customer_id, total FROM public.order_totals WHERE total > 0;
```

New types: `ConsumerSpec`, `ConsumerDelta`, `ConsumerDeltaKind`.  
New plan step: `ManageConsumerView { spec, action }` (create / alter / drop).  
New catalog table: `aqueduct.consumer_views` (catalog schema version bumped to 2).

#### Preview Environments

`aqueduct preview --branch <name>` creates a throwaway copy of the DAG in a scratch
schema with `TABLESAMPLE`-sampled base data:

```
aqueduct preview --branch feat/new-aggregates --sample 10
aqueduct preview --list
aqueduct preview --branch feat/new-aggregates --cleanup
```

Three backends:
- **Native** (default) — scratch schema in the same database, zero infrastructure
- **CloudNativePG** — stub implementation (full support in a future release)
- **Neon** — stub implementation (full support in a future release)

Preview schema names are always `aqueduct_preview_{sanitised_branch_name}` and are
dropped automatically on cleanup.

#### Optional Companion Extension Support

The CLI now detects and uses the optional `pg_aqueduct` companion extension when
present:

- `aqueduct.ddl_log` table tracks DDL events from the extension's event trigger
- `detect_extension_installed()` — returns `true` when the DDL event trigger exists
- `read_ddl_log()` — reads recent DDL events for drift detection

When the extension is absent, the CLI operates in fallback mode with no loss of core
functionality.

#### Catalog Schema v2

The `aqueduct` catalog schema is now at version 2. New tables:

| Table | Purpose |
|---|---|
| `aqueduct.ddl_log` | DDL event log (populated by companion extension) |
| `aqueduct.consumer_views` | Consumer view registry |
| `aqueduct.blue_green_deployments` | Blue/green deployment tracking |

---

## [v0.2.0] — Online Schema Evolution

**Released:** 2026-05-18
**Tag:** [`v0.2.0`](https://github.com/trickle-labs/pg-aqueduct/releases/tag/v0.2.0)

All Phase 3 roadmap items are complete. Column additions, column removals, and
DIFF→FULL refresh-mode changes are now recognised as zero-rebuild (in-place)
operations. Base-table DDL changes cascade automatically to downstream stream tables.
Per-step cost estimates are available via `aqueduct plan --explain-cost`.

### What's New

#### Full Migration Classifier

The migration classifier now implements the complete v0.2 decision tree:

| Change | Class |
|---|---|
| Schedule, `cdc_mode`, `refresh_mode` DIFF→FULL | Free |
| Add a passthrough or aggregate column | In-place |
| Drop a column from SELECT | In-place |
| Rename a column | Rebuild |
| Change `GROUP BY` keys | Rebuild |
| Change a JOIN condition or add/remove a JOIN | Rebuild |
| Change a `WHERE` predicate | Rebuild |
| Switch `refresh_mode` FULL→DIFF | Rebuild (establishes delta state) |
| Topology change (split or merge nodes) | Blue/green |

Previously, any query change defaulted to Rebuild.  With v0.2, column additions and
removals on structurally-unchanged queries are correctly identified as in-place
migrations — preserving materialized state and avoiding a full table scan.

#### `pg_eddy` Cypher Source Handling

Stream tables backed by `pg_eddy` Cypher queries now use the
`@aqueduct:cypher_source` front-matter directive to point to the `.cypher` source
file.  The directive is recognised by the parser and stored on `StreamTableSpec`
without generating unknown-key lint warnings.

#### ALTER TABLE Cascade Analysis (Tier 1)

When an owned source table's DDL changes, `aqueduct plan` now:
1. Emits an `AlterBaseTable` plan step to execute the DDL.
2. Automatically identifies every stream-table node whose query references the altered
   table (via `sqlparser`-based query analysis).
3. Schedules a coordinated Rebuild cascade for all impacted stream tables in the same
   plan.

This replaces the previous manual workflow of updating source DDL and then separately
planning the stream-table changes.

#### `aqueduct plan --explain-cost`

The new `--explain-cost` flag adds per-step cost estimates to the plan output:

```
Step                          Rows (est)    Duration (est)    Class
───────────────────────────────────────────────────────────────────────────────
ALTER stream order_totals       —             < 1s            rebuild
BACKFILL order_totals           1.2M          ~45s            rebuild
ALTER stream customer_summary   —             < 1s            in-place
BACKFILL customer_summary       340K          ~12s            in-place
```

Row counts are obtained via `EXPLAIN (FORMAT JSON)` and duration is estimated from
`rows × 200 bytes / write_throughput` (default 50 MB/s, calibrated per-cluster by
`aqueduct init`).

#### Testing

- **16 classifier unit tests** covering every decision-tree entry.
- **7 new integration tests** covering: in-place column addition, in-place column
  removal, FULL→DIFF rebuild, `cypher_source` parsing, source DDL cascade analysis,
  `AlterBaseTable` plan step, and `--explain-cost` cost estimation.
- **4 new CLI integration tests** covering: `cypher_source` no-warning, in-place
  plan detection, FULL→DIFF rebuild classification, and cost renderer.
- **Total: 85 tests; none skipped.**

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
