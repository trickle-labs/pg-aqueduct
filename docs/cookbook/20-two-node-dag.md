# Pattern 20 — Two-Node Dependency DAG

**Class:** Create (both nodes) | **Test:** `test_cookbook_20_two_node_dag`

## When to use

Build a two-level analytics pipeline where a summary stream table depends on a base aggregation stream table.

## Migration files

```sql
-- migrations/streams/c20_totals.sql
-- @aqueduct:schedule = "30s"
SELECT id, SUM(amount) AS total FROM raw_c20 GROUP BY id;
```

```sql
-- migrations/streams/c20_summary.sql
-- @aqueduct:schedule = "1m"
-- @aqueduct:depends_on = ["public.c20_totals"]
SELECT COUNT(*) AS num_customers FROM public.c20_totals;
```

## Plan output

```
  + c20_totals     [create]   level 0
  + c20_summary    [create]   level 1 (depends on c20_totals)
```

## Notes

- `aqueduct` always applies nodes in topological order. `c20_totals` is created before `c20_summary` regardless of file order in `migrations/streams/`.
- Use `@aqueduct:depends_on` to declare explicit dependencies when the planner cannot infer them from schema-qualified names in the SQL.
