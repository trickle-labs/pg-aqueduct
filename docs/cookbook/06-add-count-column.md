# Pattern 06 — Add a COUNT(*) Column

**Class:** In-place  
**Cost:** Seconds to minutes  
**Test:** `test_cookbook_06_add_count_column`

## When to use

Add an event-count column alongside existing aggregates. A common use case is adding
`COUNT(*) AS num_events` to a table that previously only tracked totals.

## Migration file

```sql
-- migrations/streams/c06_stats.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       SUM(value)  AS total,
       COUNT(*)    AS event_count   -- ← new column
FROM raw_events
GROUP BY id;
```

## Plan output

```
  ~ c06_stats    [in-place]   add column event_count (COUNT)
```

## Why it's In-place

`COUNT(*)` is a simple additive aggregate. It does not change the GROUP BY keys or
the row membership of the stream table. The new column is backfilled over the existing
rows without touching any other column data.

## Notes

- `COUNT(DISTINCT col)` is **not** IVM-supportable for DIFFERENTIAL mode in
  most `pg_trickle` versions. Use `COUNT(*)` instead, or switch the table to
  FULL refresh mode.
