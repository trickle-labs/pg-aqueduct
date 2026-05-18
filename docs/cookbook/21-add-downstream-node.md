# Pattern 21 — Add a Downstream Dependent Node

**Class:** Create (new node only) | **Test:** `test_cookbook_21_add_downstream_node`

## When to use

Add a new stream table that consumes an existing one. Only the new downstream node requires a Create step; the upstream table is unchanged.

## New migration file

```sql
-- migrations/streams/c21_summary.sql  (new)
-- @aqueduct:schedule = "1m"
-- @aqueduct:depends_on = ["public.c21_totals"]
SELECT COUNT(*) AS n FROM public.c21_totals;
```

## Plan output

```
  = c21_totals     [unchanged]
  + c21_summary    [create]    new downstream node
```

## Notes

- The existing upstream table (`c21_totals`) is not touched — zero downtime for consumers reading from it.
- The plan executes the Create step only after confirming the upstream is healthy.
