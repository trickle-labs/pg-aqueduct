# Pattern 19 — Drop an Existing Stream Table

**Class:** Drop | **Cost:** < 1 second | **Test:** `test_cookbook_19_drop_stream_table`

## When to use

Remove a stream table that is no longer needed by deleting its migration file.

## Steps

1. Delete (or archive) `migrations/streams/c19_orphan.sql`.
2. Run `aqueduct plan` — the plan shows a Drop step.
3. Run `aqueduct apply` to execute.

## Plan output

```text
  - c19_orphan    [drop]    stream table removed from migrations directory
```

## Important: check downstream dependencies

Before dropping a table, verify:
- No other stream table declares `@aqueduct:depends_on = ["public.c19_orphan"]`.
- No consumer view in `migrations/consumers/` references the table.
- No application code queries it directly.

`aqueduct plan` fails with a hard error if a downstream stream table still depends on the dropped node.
