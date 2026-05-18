# Pattern 17 — Switch Refresh Mode FULL to DIFFERENTIAL

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_17_full_to_diff_is_rebuild`

## When to use

Move a stream table from full periodic refreshes to incremental differential refresh to reduce compute cost on large tables.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"    -- was FULL
SELECT id, COUNT(*) AS n FROM raw_c17 GROUP BY id;
```

## Why it's a Rebuild

DIFFERENTIAL mode requires `pg_trickle` to initialise delta-tracking state (change buffers, sequence counters) that did not exist in FULL mode. This can only be done by rebuilding the table from scratch.

## Contrast with DIFF to FULL

The reverse direction (DIFFERENTIAL to FULL) is **Free** — see [Pattern 04](04-diff-to-full-refresh.md).

## Notes

- After switching to DIFFERENTIAL, `aqueduct lint` checks IVM-supportability for the query.
- Queries with `DISTINCT`, volatile functions, or unsupported aggregates cannot be DIFFERENTIAL — `aqueduct validate` will report this as a plan error.
