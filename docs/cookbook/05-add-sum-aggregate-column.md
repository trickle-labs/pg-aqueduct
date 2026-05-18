# Pattern 05 — Add a SUM Aggregate Column

**Class:** In-place  
**Cost:** Seconds to minutes (proportional to row count)  
**Test:** `test_cookbook_05_add_sum_aggregate_column`

## When to use

Add a new SUM aggregate to an existing DIFFERENTIAL stream table without rebuilding it.
The new column is backfilled incrementally; the existing materialised data is preserved.

## Migration file

```sql
-- migrations/streams/c05_totals.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       SUM(amount)   AS total,
       SUM(discount) AS discount_total   -- ← new column
FROM raw_orders
GROUP BY id;
```

## Plan output

```
Project  analytics  v11 → v12
Target   prod

Changes  1 node affected

  ~ c05_totals    [in-place]   add column discount_total (SUM)

Cost estimate:

  Step                          Rows (est)   Duration (est)   Class
  ──────────────────────────────────────────────────────────────────
  ALTER stream c05_totals            —            < 1s         in-place
  BACKFILL c05_totals             1.4M           ~52s          in-place
```

## Why it's In-place

Adding a new SUM aggregate column does not change the GROUP BY keys, the FROM clause,
or the WHERE predicate. The existing rows remain valid — only the new column is
`NULL` until backfilled. `pg_trickle` fills the new column incrementally without
re-materialising the entire table.

## Notes

- The same pattern applies to `COUNT`, `MIN`, `MAX`, and `AVG` columns. See
  [Pattern 06](06-add-count-column.md) for COUNT.
- For tables with `allow_full_refresh = false` in `aqueduct.toml`, an In-place column
  addition is always safe.
