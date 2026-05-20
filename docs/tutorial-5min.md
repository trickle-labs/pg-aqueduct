# pg_aqueduct in 5 Minutes

You have a `pg_trickle` deployment. Your stream tables are humming along, ingesting
change events and keeping your aggregates fresh. Then comes the day you need to add a
column. Or change a GROUP BY. Or add a new downstream table that depends on one of the
existing ones. And you discover there is no safe way to do that — only `drop_stream_table()`
followed by a full recreate, which empties your table and triggers a rebuild that takes
minutes or hours on a large dataset.

`pg_aqueduct` is the tool that fixes this. Think of it the way you think of Atlas for
relational schemas or Terraform for infrastructure: you declare the desired state, run
a plan to see exactly what will change and at what cost, then apply it. `pg_aqueduct`
figures out the safest, cheapest migration path — and executes it in the right
topological order without losing your materialized data if it doesn't have to.

---

## The Core Workflow

Every interaction with `pg_aqueduct` follows the same three-step loop:

```sh
aqueduct plan --to prod      # see what would change
aqueduct apply --to prod     # execute the plan
aqueduct status --to prod    # verify the result
```

That is it. `plan` reads your migration files and diffs them against the live catalog.
`apply` executes the plan step by step, recording progress so it can resume if something
goes wrong. `status` shows you the current health of every managed stream table.

---

## A Concrete Example

Imagine you have a stream table called `order_totals` that aggregates orders by
customer. It is defined in a SQL file with a few special comments — front-matter
directives that tell `pg_aqueduct` how to configure the stream:

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    COUNT(*)    AS order_count
FROM raw.orders
GROUP BY customer_id;
```

Your product team asks you to add a `discount_total` column. You update the file:

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:schedule     = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count,
    SUM(discount) AS discount_total     -- new column
FROM raw.orders
GROUP BY customer_id;
```

Now you run a plan:

```text
$ aqueduct plan --to prod

Project  checkout-analytics  v3 → v4
Target   prod

Changes  1 node affected

  ~ order_totals    [in-place]   add column discount_total (SUM)

  Step                          Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────
  ALTER stream order_totals          —            < 1s         in-place
  BACKFILL order_totals           1.2M           ~45s          in-place
```

`pg_aqueduct` recognized that adding a SUM aggregate to an existing GROUP BY is an
**in-place** migration — the existing rows are preserved, only the new column is
backfilled. No drop, no full rebuild, no downtime. You apply it:

```sh
aqueduct apply --to prod
```

Forty-five seconds later your table has a new column and every downstream consumer was
never interrupted. If the team had asked you to change the GROUP BY keys instead, the
plan would have classified it as a **rebuild** and told you upfront that it requires a
maintenance window — before touching a single row.

---

## The Four Migration Classes

`pg_aqueduct` classifies every change into one of four classes, and always applies the
cheapest one that is safe:

| Class | What triggers it | Cost |
|-------|-----------------|------|
| **Free** | Change a refresh schedule, toggle CDC mode | Under 1 second, zero impact |
| **In-place** | Add an aggregate column (same GROUP BY) | Seconds to minutes, zero downtime |
| **Rebuild** | Change GROUP BY keys, add a JOIN, change a WHERE | Minutes, gated by maintenance window |
| **Blue/green** | Restructure the topology of a sub-DAG | Background parallel build + atomic swap |

The principle is conservative: when `pg_aqueduct` cannot prove a change is safe for
in-place migration, it falls back to the next class up. A false Rebuild is an
inconvenience. A false In-place would be data loss.

---

## Where to Go Next

If you want to see the full lifecycle — creating a project, bootstrapping from an
existing deployment, evolving the schema across multiple environments, and rolling back
— read the [30-minute tutorial](tutorial-30min.md). For a deep reference on every
command and directive, see the [API Reference](api-reference.md). For the 30 most
common migration patterns, worked end to end, see the
[Migration Cookbook](cookbook/README.md).
