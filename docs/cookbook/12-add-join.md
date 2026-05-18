# Pattern 12 — Add a JOIN

**Class:** Rebuild | **Cost:** Minutes | **Test:** `test_cookbook_12_add_join_is_rebuild`

## When to use

Enrich a stream table by joining to a reference table (e.g., adding customer name by joining to `customers`).

## Migration file

```sql
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT o.customer_id, c.name, SUM(o.amount) AS total
FROM raw_orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY o.customer_id, c.name;
```

## Why it's a Rebuild

A JOIN changes the source set. Rows that previously had no match in the joined table may now be excluded; new rows from the join may appear. The existing materialised state is invalid.

## Notes

- For low-cardinality reference tables, consider denormalising the value into the source table instead to keep the query JOIN-free and In-place-eligible.
