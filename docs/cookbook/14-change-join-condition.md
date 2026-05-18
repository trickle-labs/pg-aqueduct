# Pattern 14 — Change a JOIN Condition

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_14_change_join_condition_is_rebuild`

## When to use

Tighten or loosen the ON clause of an existing JOIN — for example, adding a filter on the joined table.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
SELECT o.id, p.name
FROM orders o
JOIN products p ON o.product_id = p.id
               AND p.active = true;    -- condition added
```

## Why it's a Rebuild

The JOIN condition directly determines which rows are produced. A change means the current materialised state is incorrect and must be rebuilt.

## Notes

- Even a small addition to a JOIN condition (e.g., `AND t.col = x`) has correctness implications for all existing rows.
