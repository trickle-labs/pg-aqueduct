# pg_aqueduct: A Hands-On Tutorial

This tutorial walks you through everything you need to understand and use
`pg_aqueduct` confidently in a real production workflow. By the end you will have
set up a project from scratch, evolved the schema through several common change types,
promoted changes across environments, and performed a rollback. Plan for about 30
minutes of focused reading and hands-on work.

---

## Part 1 — The Problem Worth Solving

Before writing a single line of configuration, it is worth understanding why
`pg_aqueduct` exists — because the problem it solves is surprisingly sharp.

`pg_trickle` keeps your stream tables alive by continuously applying differential
updates: it reads the change stream, runs your SQL query over only the changed rows,
and merges the results back. This is beautifully efficient at runtime. But it creates
a painful constraint at migration time. A stream table is not just a view you can
`DROP` and recreate — it holds materialized state that took time to build, and it is
connected to other stream tables in a dependency graph. Change one node and the
ripple effects cascade downward.

Today, without `pg_aqueduct`, the standard answer to any stream-table change is
`drop_stream_table()` followed by a manual recreate. That destroys the materialized
rows, triggers an expensive full refresh that can run for minutes or hours on a
large table, and forces you to manually recreate every downstream table in exactly
the right topological order. Get that order wrong and your queries fail to parse or,
worse, silently return stale data. For a five-node DAG this is an inconvenience. For
a 200-node production DAG it is an outage.

`pg_aqueduct` solves this by doing three things. It maintains a declarative source of
truth for your stream-table DAG in version-controlled SQL files. It computes a
semantic diff between that desired state and the live catalog. And it classifies every
change into the cheapest migration that is provably safe, then executes it in the
correct topological order. You describe the end state; `pg_aqueduct` figures out how
to get there without unnecessary data loss.

---

## Part 2 — Concepts and Mental Model

### Stream Tables and DAGs

A stream table is a table maintained by `pg_trickle` that holds the result of an
aggregation query. Unlike a materialized view that is fully recomputed on refresh, a
stream table processes only the rows that changed since the last refresh cycle. This
is the *differential* in DIFFERENTIAL mode.

Stream tables can depend on other stream tables, forming a directed acyclic graph —
a DAG. When you have `order_totals` feeding into `customer_tiers`, which feeds into
`churn_risk_scores`, you have a three-node DAG. Each node has its own refresh
schedule, its own SQL query, and potentially its own refresh mode.

```text
raw.orders (base table)
      │
      ▼
order_totals        (DIFFERENTIAL, every 30s)
      │
      ▼
customer_tiers      (DIFFERENTIAL, every 1m)
      │
      ▼
churn_risk_scores   (DIFFERENTIAL, every 5m)
```

### The Migration Classes

The central concept in `pg_aqueduct` is the migration class. Every change to every
node in your DAG is classified into one of four classes, and `pg_aqueduct` will only
ever apply the cheapest class that is provably safe for the change you made.

**Free class** covers changes that do not touch the query logic or the materialized
data at all. Changing a refresh schedule from 30 seconds to 1 minute is a Free
migration — it calls `alter_stream_table()` once and completes in under a second with
zero impact on consumers. Toggling CDC mode and switching from DIFFERENTIAL to FULL
refresh mode are also Free.

**In-place class** covers additive changes to the SELECT list that do not touch the
GROUP BY keys, the FROM clause, or any WHERE predicates. Adding a `SUM(discount)`
column to a table that already GROUP BYs `customer_id` is In-place: the existing rows
are valid, only the new column is missing. `pg_aqueduct` adds the column and backfills
it incrementally using `pg_trickle`'s targeted refresh mechanism. The table remains
readable and queryable the entire time. For a 1.2 million row table this typically
takes under a minute.

**Rebuild class** covers changes that structurally alter the query: changing GROUP BY
keys, adding or removing a JOIN, adding or changing a WHERE predicate, renaming a
column. These changes invalidate the existing materialized rows, so `pg_aqueduct` has
to drop the stream table and recreate it from scratch. This triggers a full refresh.
Rebuild migrations are gated by your `maintenance_window` configuration — unless you
explicitly pass `--ignore-maintenance-window`, they are deferred until the window
opens. Downstream nodes affected by a rebuild are also rebuilt, in topological order.

**Blue/green class** covers the rarest case: restructuring the topology of a sub-DAG
— splitting a node into two, merging two nodes into one, rewriting the dependency
graph. `pg_aqueduct` handles this by building a parallel "green" DAG alongside the
existing "blue" one, backfilling it while the blue DAG remains live, and then
atomically swapping consumer views so that downstream consumers see the new topology
without a gap in data availability.

### The Project Format

A `pg_aqueduct` project is a directory with an `aqueduct.toml` configuration file and
a `migrations/streams/` subdirectory containing SQL files. Each SQL file defines one
stream table. The SQL is exactly what you would pass to `create_stream_table()` in
`pg_trickle`, preceded by front-matter directives in SQL line-comment form:

```sql
-- @aqueduct:schedule     = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    COUNT(*)    AS order_count
FROM raw.orders
GROUP BY customer_id;
```

The front-matter directives set the `pg_trickle` parameters for the stream table.
The `schedule` directive sets the refresh interval. The `refresh_mode` directive
sets whether the table uses DIFFERENTIAL or FULL refresh. Additional directives
cover CDC mode, explicit `depends_on` declarations, and whether to allow full
refreshes during backfill.

The `aqueduct.toml` file declares named targets (databases) and project-level settings:

```toml
[project]
name = "checkout-analytics"

[targets.dev]
dsn  = "postgresql://localhost/checkout_dev"

[targets.staging]
dsn  = "${AQUEDUCT_STAGING_DSN}"

[targets.prod]
dsn  = "${AQUEDUCT_PROD_DSN}"

[apply]
lock_timeout       = "30s"
maintenance_window = "02:00-04:00 UTC"
```

Note that production credentials are never hardcoded — the DSN uses an environment
variable. `pg_aqueduct` also supports integration with HashiCorp Vault and AWS
Secrets Manager for production secret injection; see the
[Security Guide](security.md) for details.

---

## Part 3 — Setting Up a New Project

### Prerequisites

You need PostgreSQL with `pg_trickle` installed, and the `aqueduct` binary on your
PATH. You can install the binary from the [releases page](https://github.com/trickle-labs/pg-aqueduct/releases)
or build it from source with `cargo install --path crates/aqueduct-cli`.

Set an environment variable for your development database:

```sh
export AQUEDUCT_DEV_DSN="postgresql://localhost/mydb"
```

### Initializing a Project

```sh
mkdir checkout-analytics && cd checkout-analytics
aqueduct init --to dev
```

`aqueduct init` creates the project skeleton:

```text
checkout-analytics/
├── aqueduct.toml
└── migrations/
    └── streams/        ← your SQL files go here
```

It also connects to the target database and creates the `aqueduct` catalog schema,
which holds the migration history, DAG version snapshots, and distributed lock table.
You only need to run `init` once per target database.

### Bootstrapping from an Existing Deployment

If you already have a running `pg_trickle` deployment and want to bring it under
`pg_aqueduct` management, use `import` instead of starting from empty migration files:

```sh
aqueduct import --from prod --output migrations/streams/
```

This reads every row from `pgtrickle.pgt_stream_tables` and generates a
corresponding SQL migration file for each one, with the correct front-matter
directives extracted from the catalog. After running `import`, your local state and
the live state are identical, so `aqueduct plan --to prod` will produce an empty
plan. You are now on a known baseline and can start making changes.

---

## Part 4 — Your First Migration

Let's walk through the three-node checkout analytics DAG from scratch.

### Step 1 — Write the Migration Files

Create three files in `migrations/streams/`:

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

```sql
-- migrations/streams/customer_tiers.sql
-- @aqueduct:schedule      = "1m"
-- @aqueduct:refresh_mode  = "DIFFERENTIAL"
-- @aqueduct:depends_on    = ["public.order_totals"]
SELECT
    customer_id,
    CASE
        WHEN total_amount > 1000 THEN 'gold'
        WHEN total_amount > 100  THEN 'silver'
        ELSE                          'bronze'
    END AS tier
FROM public.order_totals;
```

```sql
-- migrations/streams/churn_risk_scores.sql
-- @aqueduct:schedule      = "5m"
-- @aqueduct:refresh_mode  = "DIFFERENTIAL"
-- @aqueduct:depends_on    = ["public.customer_tiers"]
SELECT
    t.customer_id,
    t.tier,
    CASE WHEN t.tier = 'bronze' AND o.order_count < 2 THEN true
         ELSE false
    END AS at_risk
FROM public.customer_tiers t
JOIN public.order_totals o USING (customer_id);
```

The `@aqueduct:depends_on` directive tells the planner about the dependency graph.
Without it, `pg_aqueduct` infers dependencies from schema-qualified table references
in the SQL, but explicit declarations are more reliable when the SQL is complex.

### Step 2 — Validate Before Connecting

Before touching the database, validate your migration files offline:

```sh
aqueduct validate
```

This checks for syntax errors in the front-matter directives, detects cycles in the
dependency graph, and flags common mistakes like sub-5-second schedules. It runs
entirely without a database connection — ideal for a CI pre-check step.

### Step 3 — Plan

```text
$ aqueduct plan --to dev

Project  checkout-analytics  (initial)
Target   dev (pg_trickle 0.62, pg 18+)

Changes  3 nodes affected

  + order_totals        [create]   level 0
  + customer_tiers      [create]   level 1 (depends on order_totals)
  + churn_risk_scores   [create]   level 2 (depends on customer_tiers, order_totals)

  Step                              Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────────
  CREATE stream order_totals             —            < 1s         create
  BACKFILL order_totals               850K           ~30s          create
  CREATE stream customer_tiers           —            < 1s         create
  BACKFILL customer_tiers             850K           ~30s          create
  CREATE stream churn_risk_scores        —            < 1s         create
  BACKFILL churn_risk_scores           850K           ~30s          create
```

The plan tells you exactly what will happen: three stream tables created in
topological order (level 0 before level 1 before level 2), with estimated row counts
and durations for each backfill. Review it, then apply:

```sh
aqueduct apply --to dev
```

`aqueduct apply` asks for confirmation, displays the plan one more time, then
executes each step. Progress is checkpointed to `aqueduct.migrations.progress` after
every step, so if the process crashes mid-migration, you can resume from where it
left off with `aqueduct apply --to dev --resume`.

### Step 4 — Verify

```text
$ aqueduct status --to dev

Project  checkout-analytics  v1

  Table                  Mode          Schedule   Last Refresh   Lag    Status
  ─────────────────────────────────────────────────────────────────────────────
  order_totals           DIFFERENTIAL  30s        2s ago         0ms    ✓ ok
  customer_tiers         DIFFERENTIAL  1m         58s ago        0ms    ✓ ok
  churn_risk_scores      DIFFERENTIAL  5m         4m ago         0ms    ✓ ok
```

All three tables are running and fresh. You are on version 1 of your DAG.

---

## Part 5 — Evolving the Schema

This is where `pg_aqueduct` earns its place. Let's walk through three common change
types: one Free, one In-place, and one Rebuild.

### Free Change — Update the Refresh Schedule

The SRE team decides `order_totals` is refreshing too aggressively and wants to change
it from 30 seconds to 2 minutes. Edit the file:

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "2m"             ← changed
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT ...
```

Plan it:

```text
$ aqueduct plan --to dev

  ~ order_totals    [free]   schedule: 30s → 2m

  Step                          Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────
  ALTER stream order_totals          —            < 1s         free
```

A single `alter_stream_table()` call. No data movement, no downtime, done in under a
second. This is what Free class looks like.

### In-Place Change — Add a Column

The analytics team wants a `discount_total` column. Add it to the SELECT list without
touching the GROUP BY:

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "2m"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count,
    SUM(discount) AS discount_total     -- new
FROM raw.orders
GROUP BY customer_id;
```

Plan it:

```text
$ aqueduct plan --to dev

  ~ order_totals    [in-place]   add column discount_total (SUM)

  Step                          Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────
  ALTER stream order_totals          —            < 1s         in-place
  BACKFILL order_totals           850K           ~30s          in-place
```

The existing rows survive. The new column is added and backfilled without dropping the
table. During the backfill, `discount_total` is NULL for rows that have not been
touched by the backfill yet — so if you have dashboard queries that cannot tolerate
NULL, consider using `COALESCE(discount_total, 0)` in the query layer until backfill
completes. After `aqueduct apply`, the column is fully populated and the table is on
version 3.

### Rebuild Change — Change the GROUP BY

Now suppose you need to add `product_category` to the GROUP BY, so totals are broken
down by both customer and category. This is a structural change — the existing rows
are keyed by `customer_id` alone and cannot be transformed in place.

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "2m"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    product_category,                   -- added to GROUP BY
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count,
    SUM(discount) AS discount_total
FROM raw.orders
GROUP BY customer_id, product_category;  -- changed
```

Plan it:

```text
$ aqueduct plan --to dev

  ! order_totals       [rebuild]   GROUP BY keys changed
  ! customer_tiers     [rebuild]   upstream (order_totals) rebuilt → cascade
  ! churn_risk_scores  [rebuild]   upstream (customer_tiers) rebuilt → cascade

  Step                              Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────────
  DROP + RECREATE order_totals        850K           ~30s          rebuild ⏰
  DROP + RECREATE customer_tiers      850K           ~30s          rebuild ⏰
  DROP + RECREATE churn_risk_scores   850K           ~30s          rebuild ⏰

⏰ Rebuild steps will be deferred to the maintenance window (02:00–04:00 UTC).
   Pass --ignore-maintenance-window to run immediately.
```

Notice that `pg_aqueduct` automatically cascaded the rebuild to the downstream nodes.
It computed the transitive closure of the dependency graph and included every affected
node — in topological order, rebuilding `order_totals` before `customer_tiers` before
`churn_risk_scores`. You did not have to figure out the order. You did not have to
even know which tables downstream of `order_totals` exist. `pg_aqueduct` read the
dependency graph from the catalog and handled it for you.

In production, this plan would be held until 02:00 UTC. In development, use
`--ignore-maintenance-window` to run it immediately.

---

## Part 6 — Multi-Environment Promotion

One of the most important benefits of `pg_aqueduct` is repeatable, auditable
promotion across environments. The migration files are version-controlled SQL. The
plan is deterministic. You can test a change end-to-end in dev, review the exact plan
in staging, and apply the same plan to prod — knowing the behavior will be identical.

```sh
# Validate offline (no database needed)
aqueduct validate

# Test in dev
aqueduct plan --to dev
aqueduct apply --to dev

# Promote to staging
aqueduct plan --to staging
aqueduct apply --to staging --yes        # skip confirmation in CI

# Promote to prod (plan is shown again for final human review)
aqueduct plan --to prod
aqueduct apply --to prod
```

Because `aqueduct plan` exits with code 1 when there are pending changes and code 0
when the state is already up to date, you can use it as a gate in CI/CD pipelines:

```yaml
# .gitlab-ci.yml (excerpt)
migrate-prod:
  stage: deploy
  script:
    - aqueduct apply --to prod --yes
  only:
    - main
```

In CI environments where you want to assert that no unreviewed drift exists, use
`aqueduct plan --quiet --to prod` — it exits 0 for an empty plan and 1 for any
pending change, which you can turn into a pipeline alert.

---

## Part 7 — Rollback

`pg_aqueduct` stores a full snapshot of the DAG definition at every `aqueduct apply`.
These snapshots live in `aqueduct.dag_versions`. Rolling back restores the previous
snapshot by computing a forward migration plan from the current state to the target
version — it is always a forward migration under the hood, which means all the same
safety guarantees apply.

```sh
aqueduct rollback --to prod
```

By default, `rollback` targets the immediately preceding version. To roll back to a
specific version, use `--to-version <N>`. The command will refuse to proceed through
a Rebuild-class rollback without an explicit `--accept-data-loss` flag, because once
a full refresh has replaced the old data with the new, there is nothing to roll back
to without rerunning the refresh in the other direction.

The lossless rollback window is the key insight: if you are rolling back a Free or
In-place migration, rollback is always safe and fast. If you are rolling back a
Rebuild, you are accepting the cost of another full rebuild in the opposite direction.

---

## Part 8 — Working with the Dependency Graph

### Explicit vs. Inferred Dependencies

When a downstream stream table references an upstream one by a schema-qualified name
— `public.order_totals` — `pg_aqueduct` infers the dependency automatically by
parsing the SQL. You do not need to declare it explicitly. However, when the upstream
table name is constructed dynamically, or when you want to be explicit for clarity,
use the `@aqueduct:depends_on` directive:

```sql
-- @aqueduct:depends_on = ["public.order_totals", "public.customer_tiers"]
```

The planner validates these declarations against the actual catalog and warns if a
declared dependency does not exist or if an inferred dependency is missing from the
declaration.

### Lint Checks

`aqueduct lint` runs a set of checks for common problems:

```sh
aqueduct lint
```

The checks include:
- `L001` — Schedule under 5 seconds (very aggressive, may strain `pg_trickle`)
- `L002` — DIFFERENTIAL mode with a non-deterministic aggregate (e.g., `RANDOM()`)
- `L003` — Cyclic dependency in the graph
- `L004` — Undeclared dependency (inferred from SQL but missing from `depends_on`)
- `L005` — Unknown front-matter directive (typo guard)
- `L006` — Consumer view references a non-managed stream table

Run `aqueduct lint --fix` to automatically fix L004 (adds the missing `depends_on`
declarations without changing anything else).

---

## Part 9 — Advanced: Blue/Green Migrations

When a change is so structural that it cannot be expressed as a sequence of per-node
Rebuild migrations — for example, splitting `order_totals` into two separate tables,
each serving a different downstream — `pg_aqueduct` can orchestrate a blue/green
migration.

A blue/green migration works like this: `pg_aqueduct` builds the entire new sub-DAG
(the "green" side) in parallel with the existing one (the "blue" side), using
consumer views to route reads. While the green side backfills, the blue side remains
fully operational. Once the green side has caught up, `pg_aqueduct` atomically swaps
the consumer views so that downstream queries now read from green. The blue side is
then torn down.

Blue/green is the most expensive migration class but also the most powerful — it
provides zero-downtime topology restructuring. In practice, most changes never reach
this class. The cookbook's [Pattern 30](cookbook/30-full-dag-lifecycle.md) shows a
complete lifecycle including the decision points.

---

## Part 10 — Day-to-Day CLI Cheatsheet

Here is a quick reference for the commands you will use most often:

| Command | What it does |
|---------|-------------|
| `aqueduct validate` | Offline validation — no database |
| `aqueduct lint` | Check for anti-patterns |
| `aqueduct plan --to <env>` | Show pending migrations |
| `aqueduct apply --to <env>` | Execute migrations |
| `aqueduct apply --to <env> --dry-run` | Show plan without executing |
| `aqueduct apply --to <env> --resume` | Resume an interrupted apply |
| `aqueduct status --to <env>` | Live health check of all stream tables |
| `aqueduct status --to <env> --watch` | Re-poll every 5 seconds |
| `aqueduct diff --table <name> --to <env>` | Semantic diff for one table |
| `aqueduct rollback --to <env>` | Rollback to previous version |
| `aqueduct import --from <env>` | Bootstrap migration files from live catalog |
| `aqueduct destroy --to <env> --dry-run` | Preview teardown |
| `aqueduct destroy --to <env> --confirm` | Execute teardown |

---

## What's Next

You now have a complete picture of how `pg_aqueduct` works. The [Migration Cookbook](cookbook/README.md)
has 30 worked examples covering every common pattern, each with the exact plan output
you will see and an explanation of why it classifies the way it does. The
[API Reference](api-reference.md) documents every command, front-matter directive,
and `aqueduct.toml` option in full. For production deployments on Patroni or
CloudNativePG, read the [HA Operations Guide](ha-operations.md).
