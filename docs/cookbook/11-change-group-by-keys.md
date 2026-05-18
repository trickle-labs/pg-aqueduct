# Pattern 11 — Change GROUP BY Keys

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_11_change_group_by_is_rebuild`

## When to use

Change the granularity of an aggregation — for example, adding `region` to a GROUP BY that previously only grouped by `customer_id`.

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT customer_id, region, SUM(amount) AS total
FROM raw_c11
GROUP BY customer_id, region;
```

## Why it's a Rebuild

The GROUP BY keys define the primary key of the materialised table. Changing them invalidates every existing row. A full DROP + recreate + backfill is required.

## Notes

- Consider adding a separate downstream stream table instead of modifying the existing one (Pattern 21).
- For large tables, use `--strategy blue-green` for zero-downtime rebuild.
