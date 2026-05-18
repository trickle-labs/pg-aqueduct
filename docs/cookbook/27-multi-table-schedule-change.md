# Pattern 27 — Change Schedule on Multiple Tables Simultaneously

**Class:** Free (all tables) | **Cost:** < 1 second | **Test:** `test_cookbook_27_multi_table_schedule_change`

## When to use

Adjust the refresh cadence of a set of related stream tables in one `aqueduct apply` — for example, increasing frequency across a whole analytics pipeline before a business-critical period.

## Migration files (updated schedules)

```
c27_a.sql:  -- @aqueduct:schedule = "10s"   (was 1m)
c27_b.sql:  -- @aqueduct:schedule = "10s"   (was 1m)
c27_c.sql:  -- @aqueduct:schedule = "10s"   (was 1m)
```

## Plan output

```
  ~ c27_a    [free]   schedule: 1m → 10s
  ~ c27_b    [free]   schedule: 1m → 10s
  ~ c27_c    [free]   schedule: 1m → 10s
```

## Notes

- All Free steps execute as fast serial metadata updates.
- There is no dependency between Free steps on separate tables — they could in principle be parallelised. The current executor applies them sequentially for simplicity.
- `aqueduct lint` warns when `schedule < 5s` and the estimated row count is large.
