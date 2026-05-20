# Pattern 03 — Enable CDC Mode

**Class:** Free  
**Cost:** < 1 second  
**Test:** `test_cookbook_03_enable_cdc_mode`

## When to use

Enable CDC (change-data-capture) mode to consume WAL replication events from a source
table instead of polling. This reduces lag and load on the source table for high-write
scenarios.

## Migration file

```sql
-- migrations/streams/event_counts.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "FULL"
-- @aqueduct:cdc_mode = "wal"
SELECT event_type, COUNT(*) AS n FROM raw_events GROUP BY event_type;
```

## Plan output

```text
Project  analytics  v7 → v8
Target   prod

Changes  1 node affected

  ~ event_counts    [free]   cdc_mode: null → wal
```

## Why it's Free

CDC mode is a property of how `pg_trickle` sources changes, not of the materialised
table schema. Enabling it is a single metadata update — the materialised data does not
change.

## Notes

- WAL CDC requires the source table to have `REPLICA IDENTITY FULL` or a primary key.
- Set `REPLICA IDENTITY FULL` with Atlas or a manual `ALTER TABLE` before enabling CDC.
- Disabling CDC is similarly Free.
