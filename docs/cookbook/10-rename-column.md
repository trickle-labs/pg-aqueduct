# Pattern 10 — Rename a Column

**Class:** Rebuild  
**Cost:** Minutes (proportional to row count)  
**Test:** `test_cookbook_10_rename_column_is_rebuild`

## When to use

Rename an output column (e.g., `total` → `revenue`). Despite being a seemingly small
change, a column rename always triggers a full Rebuild.

## Migration file

```sql
-- migrations/streams/c10_agg.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       SUM(amount) AS revenue   -- ← renamed from `total`
FROM raw_c10
GROUP BY id;
```

## Plan output

```
  ! c10_agg    [rebuild]   column renamed: total → revenue → full rebuild required
```

## Why it's a Rebuild

`pg_trickle`'s DIFFERENTIAL refresh tracks deltas by column name. Renaming a column
destroys the column's delta-state metadata — there is no mapping from the old name to
the new name that preserves row-level accuracy. The table must be rebuilt from scratch.

## Minimising downtime

For large tables, use the Blue/green deployment strategy to rename with zero downtime:
1. Create a new stream table with the renamed column.
2. Run it in parallel with the old table.
3. Swap consumer views atomically when the new table has converged.
4. Drop the old table after the blue-ttl expires.

This pattern is implemented by `aqueduct apply --strategy blue-green`.

## Notes

- Plan this change during a [maintenance window](../ha-operations.md#maintenance-windows)
  if the table is large.
- `allow_full_refresh = false` in `aqueduct.toml` prevents this plan from executing
  unless `--allow-rebuild` is passed.
