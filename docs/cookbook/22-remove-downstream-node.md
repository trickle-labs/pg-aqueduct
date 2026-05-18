# Pattern 22 — Remove a Downstream Node

**Class:** Drop (removed node only) | **Test:** `test_cookbook_22_remove_downstream_node`

## When to use

Remove a stream table that was consuming an upstream table, without affecting the upstream table.

## Steps

1. Delete `migrations/streams/c22_downstream.sql`.
2. `aqueduct plan` emits a Drop only for `c22_downstream`.
3. Apply.

## Plan output

```
  = c22_base          [unchanged]
  - c22_downstream    [drop]
```

## Notes

- The upstream table (`c22_base`) continues running without interruption.
- In a multi-node DAG, `aqueduct` drops nodes in reverse topological order to avoid foreign-key-style constraint violations in `pg_trickle`.
