# Pattern 09 — Extend the SELECT List With a New Aggregate

**Class:** In-place  
**Cost:** Seconds to minutes  
**Test:** `test_cookbook_09_widen_column_type`

## When to use

Extend an existing stream table's output by adding a new computed aggregate — for
example, adding `AVG(amount)` alongside `COUNT(*)` — without modifying the GROUP BY,
FROM, or WHERE clauses.

## Migration file

```sql
-- migrations/streams/c09_counts.sql (after)
-- @aqueduct:schedule = "30s"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT id,
       COUNT(*)        AS n,
       AVG(amount)     AS avg_amount   -- ← extended output
FROM raw_c09
GROUP BY id;
```

## Plan output

```text
  ~ c09_counts    [in-place]   add column avg_amount (AVG)
```

## Why it's In-place

The SELECT list grew (superset), but the structural query parts are unchanged. The
planner detects a pure `Addition` in the SELECT list delta — a provably safe in-place
change.

## The `--strict` flag

If you want to disable in-place classification and force every query change to be
reviewed as a potential Rebuild, set `--strict` on `aqueduct plan`. This is useful in
regulated environments where data-accuracy guarantees for every column are required
before applying.
