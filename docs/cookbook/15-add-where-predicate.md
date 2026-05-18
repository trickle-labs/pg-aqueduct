# Pattern 15 — Add a WHERE Predicate

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_15_add_where_predicate_is_rebuild`

## When to use

Add a filter to exclude rows that should not appear in the stream table.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id, SUM(amount) AS total
FROM raw_c15
WHERE status = 'active'    -- new filter
GROUP BY id;
```

## Why it's a Rebuild

A WHERE predicate changes which rows are included in the stream table. Existing rows that don't pass the new filter are now incorrect — the table must be rebuilt from scratch.
