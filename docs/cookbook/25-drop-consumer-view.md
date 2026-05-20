# Pattern 25 — Drop a Consumer View

**Class:** Drop (consumer view) | **Test:** `test_cookbook_25_drop_consumer_view`

## When to use

Remove a consumer view that is no longer needed by deleting its migration file.

## Steps

1. Delete `migrations/consumers/c25_view.sql`.
2. `aqueduct plan` emits a consumer-drop step.
3. `aqueduct apply` drops the view and removes the entry from `aqueduct.consumer_views`.

## Plan output

```text
  - reporting.c25_view    [consumer drop]
```

## Important: check application dependencies

`aqueduct lint` warns about consumer views that reference non-existent stream tables but does **not** detect external application dependencies. Verify no application code queries the view before dropping it.
