# Pattern 26 — Source Column Change Cascades to Stream Tables

**Class:** Rebuild (all affected stream tables) | **Test:** `test_cookbook_26_source_column_cascade`

## When to use

An owned source table gains a new column that stream tables should use. `aqueduct plan` detects which stream tables reference the source and marks them for rebuild, generating a single coordinated plan.

## Source migration file (updated)

```sql
-- migrations/sources/raw_c26.sql
-- @aqueduct:owned = true
CREATE TABLE public.raw_c26 (
    id     bigint,
    amount numeric,
    region text    -- new column
);
```

## Updated stream table

```sql
-- migrations/streams/c26_agg.sql (updated to use new column)
-- @aqueduct:schedule = "30s"
SELECT id, region, SUM(amount) AS total
FROM public.raw_c26
GROUP BY id, region;
```

## Plan output

```
  ALTER TABLE public.raw_c26 ADD COLUMN region text;    [base-table DDL]
  ! c26_agg    [rebuild]   base-table DDL change cascade
```

## Notes

- Stream tables that do **not** reference the changed column are unaffected.
- The base-table ALTER and stream-table cascade are always in a single coordinated plan — `aqueduct` will never emit one without the other.
- Tier 2 changes (new tables, non-referenced columns, indexes) are delegated to Atlas/sqitch via a pre-hook.
