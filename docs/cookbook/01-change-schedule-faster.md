# Pattern 01 — Change Schedule to a Faster Interval

**Class:** Free  
**Cost:** < 1 second  
**Test:** `test_cookbook_01_change_schedule_faster`

## When to use

Use this pattern when you want stream tables to refresh more frequently — for example,
moving from a 1-minute schedule to 10 seconds to reduce analytical latency.

## Migration file

```sql
-- migrations/streams/hourly_stats.sql
-- @aqueduct:schedule = "10s"
-- @aqueduct:refresh_mode = "FULL"
SELECT region, COUNT(*) AS event_count FROM raw_events GROUP BY region;
```

## Before

```toml
# aqueduct.toml excerpt
[targets.prod]
dsn = "${PROD_DSN}"
```

```sql
-- hourly_stats.sql (before)
-- @aqueduct:schedule = "1m"
-- @aqueduct:refresh_mode = "FULL"
SELECT region, COUNT(*) AS event_count FROM raw_events GROUP BY region;
```

## Plan output

```
Project  analytics  v3 → v4
Target   prod

Changes  1 node affected

  ~ hourly_stats    [free]   schedule: 1m → 10s
```

## Why it's Free

`pg_trickle` stores the schedule as metadata alongside the stream table spec. A schedule
change is a single call to `pgtrickle.alter_stream_table(p_schedule => '10s')`. No DDL
is emitted, no data is moved, no rebuild is required.

## Notes

- Schedules as short as `1s` are allowed. Check the pg_trickle documentation for the
  minimum supported schedule for your cluster.
- The `aqueduct lint` command warns when `schedule < 5s` on a large table.
