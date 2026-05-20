# Pattern 04 — Switch Refresh Mode DIFFERENTIAL → FULL

**Class:** Free  
**Cost:** < 1 second  
**Test:** `test_cookbook_04_diff_to_full_refresh`

## When to use

Switch to FULL refresh mode when a stream table's query becomes IVM-unsupportable
(e.g., after adding a volatile function) and you want `pg_trickle` to do a full
re-materialisation on every tick instead of maintaining a delta.

## Migration file

```sql
-- migrations/streams/c04_agg.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "FULL"
SELECT id, SUM(amount) AS total FROM raw_orders GROUP BY id;
```

## Plan output

```text
Project  analytics  v9 → v10
Target   prod

Changes  1 node affected

  ~ c04_agg    [free]   refresh_mode: DIFFERENTIAL → FULL
```

## Why it's Free

Switching DIFFERENTIAL → FULL discards the incremental delta-tracking state managed by
`pg_trickle`. Since FULL mode re-materialises everything on every tick, no schema
change or data migration is required.

## Contrast with FULL → DIFFERENTIAL

The reverse direction (FULL → DIFFERENTIAL) is **Rebuild** — see [Pattern 17](17-full-to-diff-mode.md). Going from FULL to DIFFERENTIAL requires `pg_trickle` to establish delta-tracking state from
scratch, which means the table must be rebuilt.

## Notes

- After switching to FULL, `aqueduct lint` will warn if the table is large and the
  schedule is short (high compute cost per refresh cycle).
