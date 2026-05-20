# Pattern 02 — Change Schedule to a Slower Interval

**Class:** Free  
**Cost:** < 1 second  
**Test:** `test_cookbook_02_change_schedule_slower`

## When to use

Use this pattern when a stream table is refreshing more often than necessary, consuming
compute resources that could be better used elsewhere.

## Migration file

```sql
-- migrations/streams/fast_stats.sql (after)
-- @aqueduct:schedule = "10m"
-- @aqueduct:refresh_mode = "FULL"
SELECT user_id, SUM(score) AS total_score FROM events GROUP BY user_id;
```

## Plan output

```text
Project  analytics  v5 → v6
Target   prod

Changes  1 node affected

  ~ fast_stats    [free]   schedule: 5s → 10m
```

## Why it's Free

Slowing down a schedule is identical in implementation to speeding it up — a single
`pgtrickle.alter_stream_table()` call updates the metadata. No data is touched.

## Notes

- `aqueduct status` will show the updated schedule immediately after apply.
- For critical reporting tables, consider whether a slower schedule violates any SLA.
