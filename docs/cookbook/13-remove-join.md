# Pattern 13 — Remove a JOIN

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_13_remove_join_is_rebuild`

## When to use

Simplify a stream table by removing a JOIN that is no longer needed.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, SUM(amount) AS total
FROM raw_orders        -- JOIN removed
GROUP BY customer_id;
```

## Why it's a Rebuild

Removing a JOIN changes the source set. Rows that previously required a join match may now be included (or different rows may be excluded). The existing materialised state is invalid.
