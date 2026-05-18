# Pattern 29 — Rollback Across Version Boundaries

**Test:** `test_cookbook_29_rollback_to_prior_state`

## When to use

Revert a deployed migration to a previous DAG version — for example, after discovering a bug in a newly deployed aggregation.

## Commands

```bash
# Roll back to the immediately previous version.
aqueduct rollback --to prod

# Roll back to a specific historical version.
aqueduct rollback --to prod --to-version 3
```

## How rollback works

`aqueduct rollback` is **not** a reverse replay of the forward migration. Instead:

1. It reads the desired DAG spec stored in `aqueduct.dag_versions` for the target version.
2. It computes a **forward** migration plan from the current live state to the target spec.
3. It applies that forward plan.

This means rollback uses the same planner, executor, and safety checks as a normal `aqueduct apply`. If rolling back requires a Rebuild (e.g., after a column rename), the same Rebuild logic applies.

## Example (v1 → v2 → rollback to v1)

```
v1: c29_base only
v2: c29_base + c29_extra (added)
rollback to v1: DROP c29_extra
```

## Lossless guarantee

- **Free and In-place migrations**: always lossless.
- **Rebuild migrations**: lossless only if no full refresh has completed since the migration. `aqueduct rollback` reports the estimated lossless window expiry and requires `--accept-data-loss` if the window has passed.

## Notes

- `aqueduct rollback` never rolls back base-table DDL (Tier 1 changes). Provide a compensating Atlas migration for any base-table changes.
