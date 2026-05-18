# Pattern 18 — Create a New Stream Table

**Class:** Create | **Cost:** Seconds to minutes (initial backfill) | **Test:** `test_cookbook_18_create_stream_table`

## When to use

Add a new stream table to the project by creating a new migration file in `migrations/streams/`.

## Migration file

```sql
-- migrations/streams/c18_scores.sql
-- @aqueduct:schedule = "1m"
-- @aqueduct:refresh_mode = "FULL"
SELECT id, AVG(score) AS avg_score
FROM raw_c18
GROUP BY id;
```

## Plan output

```
  + c18_scores    [create]    new stream table (FULL, schedule 1m)
```

## Notes

- After `aqueduct apply`, run `aqueduct status` to confirm the table is active and scheduled.
- For DIFFERENTIAL mode, `pg_trickle` performs an initial full backfill before entering differential refresh.
- Cost estimate: depends on row count of the source table and write throughput.
