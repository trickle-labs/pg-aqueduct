# Pattern 16 — Change a WHERE Predicate

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_16_change_where_predicate_is_rebuild`

## When to use

Modify an existing WHERE clause — for example, tightening or loosening the filter condition.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id, amount
FROM events
WHERE type = 'purchase';    -- changed from 'click'
```

## Why it's a Rebuild

A different WHERE predicate means a different set of rows qualifies. The materialised state is stale and must be rebuilt.
