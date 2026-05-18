# Pattern 08 — Add an Aggregate Column Without Changing GROUP BY

**Class:** In-place  
**Cost:** Seconds to minutes  
**Test:** `test_cookbook_08_add_passthrough_column`

## When to use

Add a new aggregate expression to the SELECT list that uses the same GROUP BY keys and
the same FROM/WHERE structure as the existing query. This is the most common in-place
migration pattern.

## Migration file

```sql
-- migrations/streams/c08_view.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       SUM(amount)   AS total,
       MIN(amount)   AS min_amount   -- ← new aggregate, same GROUP BY
FROM raw_c08
GROUP BY id;
```

## Plan output

```
  ~ c08_view    [in-place]   add column min_amount (MIN)
```

## Key rule

The classifier checks that the GROUP BY clause, the FROM clause, and the WHERE clause
(if any) are **identical** between the old and new query. If any of those structural
parts change, the migration is classified as Rebuild regardless of what the SELECT list
looks like.

In-place column additions that change GROUP BY — such as
`SELECT id, category, SUM(amount) FROM t GROUP BY id, category` — are classified as
**Rebuild** because the GROUP BY has changed.

## Notes

- Supported aggregate functions for in-place addition: `SUM`, `COUNT`, `MIN`, `MAX`,
  `AVG`, and any other aggregate that `pg_trickle` supports for DIFFERENTIAL mode.
- Non-aggregate expressions that reference existing GROUP BY keys (e.g.,
  `id * 2 AS doubled_id`) are also In-place.
