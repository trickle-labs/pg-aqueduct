# Pattern 23 — Three-Level Dependency Chain

**Class:** Create (all 3 nodes) | **Test:** `test_cookbook_23_three_level_chain`

## When to use

Build a three-level analytics pipeline: raw data → normalised layer → aggregated summary.

## Migration files

```sql
-- migrations/streams/c23_lvl1.sql
-- @aqueduct:schedule = "30s"
SELECT id, region, amount FROM raw_c23;
```

```sql
-- migrations/streams/c23_lvl2.sql
-- @aqueduct:schedule = "1m"
-- @aqueduct:depends_on = ["public.c23_lvl1"]
SELECT region, SUM(amount) AS total FROM public.c23_lvl1 GROUP BY region;
```

```sql
-- migrations/streams/c23_lvl3.sql
-- @aqueduct:schedule = "5m"
-- @aqueduct:depends_on = ["public.c23_lvl2"]
SELECT COUNT(DISTINCT region) AS num_regions FROM public.c23_lvl2;
```

## Plan output

```
  + c23_lvl1    [create]   level 0
  + c23_lvl2    [create]   level 1
  + c23_lvl3    [create]   level 2
```

## Notes

- The planner uses Kahn's algorithm to compute the topological order.
- Cycles are detected at plan time and reported as hard errors before any DDL is executed.
- Schedules should generally be longer at higher levels to avoid stale intermediate data.
