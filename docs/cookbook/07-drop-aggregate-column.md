# Pattern 07 — Drop a Column from the SELECT List

**Class:** In-place  
**Cost:** < 1 second (DDL only)  
**Test:** `test_cookbook_07_drop_aggregate_column`

## When to use

Remove a column that is no longer needed by downstream consumers. Dropping a column
from the SELECT list is an in-place `ALTER TABLE DROP COLUMN` operation on the
materialised table.

## Migration file

```sql
-- migrations/streams/c07_stats.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
-- Removed: COUNT(*) AS order_count
SELECT id,
       SUM(amount) AS total
FROM raw_c07
GROUP BY id;
```

## Plan output

```text
  ~ c07_stats    [in-place]   drop column order_count
```

## Why it's In-place

Dropping a column is always backward-compatible for the materialised table: the
remaining columns are unaffected. `pg_trickle` stops tracking the deleted column's
delta state, freeing the associated storage.

## Important: check downstream consumers first

Before dropping a column, verify that no consumer views, reports, or application code
references the column. Use `aqueduct lint` to detect consumer view references. If a
consumer view references the column, update the consumer view migration file too.

## Notes

- Dropping a column that is part of the GROUP BY is not in-place — that is a
  [GROUP BY key change](11-change-group-by-keys.md) and requires a Rebuild.
