# Pattern 30 — Full DAG Lifecycle: Create, Evolve, Destroy

**Test:** `test_cookbook_30_full_dag_lifecycle`

## Overview

A complete end-to-end walkthrough of a stream table's lifecycle:

1. **Create** the stream table from a migration file.
2. **Evolve** it with an in-place column addition (zero downtime).
3. **Destroy** all project resources when they are no longer needed.

## Step 1: Create

```sql
-- migrations/streams/c30_totals.sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id, SUM(amount) AS total FROM raw_c30 GROUP BY id;
```

```bash
aqueduct apply --to prod
# Result: creates c30_totals (v1)
```

## Step 2: In-place evolution

```sql
-- c30_totals.sql (updated — add COUNT(*) column, same GROUP BY)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       SUM(amount)  AS total,
       COUNT(*)     AS order_count    -- new column
FROM raw_c30
GROUP BY id;
```

```bash
aqueduct plan --to prod
# Shows: ~ c30_totals [in-place]  add column order_count (COUNT)

aqueduct apply --to prod
# Result: in-place column addition (v2) — zero downtime
```

## Step 3: Destroy

When the project is retired, clean up all resources:

```bash
aqueduct destroy --project cookbook-30 --to prod --dry-run   # preview first
aqueduct destroy --project cookbook-30 --to prod --confirm   # execute
```

The destroy command:
- Drops all stream tables in reverse topological order.
- Drops all consumer views managed by the project.
- Deletes the project's rows from `aqueduct.dag_versions`, `aqueduct.migrations`, and `aqueduct.locks`.
- Does **not** drop the `aqueduct` catalog schema itself (other projects may share it).

## Notes

- Always run `--dry-run` before `--confirm` for destroy.
- Use `aqueduct status` at each step to verify the pipeline state.
