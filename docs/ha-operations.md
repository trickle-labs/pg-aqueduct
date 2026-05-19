# HA Operations Guide

## Overview

`pg_aqueduct` is designed to operate safely in high-availability PostgreSQL deployments,
including Patroni clusters, CloudNativePG, and Stolon.

## Primary detection

`aqueduct apply` always checks that the target is a primary before executing:

```sql
SELECT pg_is_in_recovery();   -- must return false
```

If the target is a standby, the CLI exits non-zero with a descriptive error.

## Supported HA backends

The CLI detects the HA backend automatically via `detect_ha_backend()`:

| Backend | Detection method |
|---------|-----------------|
| Plain primary | `pg_is_in_recovery() = false` |
| Patroni | HTTP `GET /master` on `--patroni-endpoint` |
| CloudNativePG | `app.cnpg.cluster_name` GUC present |
| Stolon | `application_name` contains `stolon-keeper` |

### Patroni

```bash
aqueduct apply --to prod --patroni-endpoint http://patroni-api:8008
```

The CLI performs a synchronous HTTP check against `GET /master` before any DDL is
executed. If the primary has changed since the connection was established (failover
during a migration), the CLI detects the discrepancy via `system_identifier` and
`timeline_id` checks between plan steps and marks the migration as `interrupted`.

### CloudNativePG

No additional flags required. The CLI reads the `app.cnpg.cluster_name` GUC to detect
a CloudNativePG cluster and uses the standard primary check (`pg_is_in_recovery()`).

## Maintenance windows

Set a maintenance window in `aqueduct.toml` to gate Rebuild steps:

```toml
[apply]
maintenance_window = "02:00-04:00 UTC"
maintenance_window_applies_to = ["rebuild"]
```

- **Free** and **In-place** steps always execute immediately, regardless of the window.
- **Rebuild** steps are blocked outside the window.
- `--ignore-maintenance-window` overrides for emergency runs.

> **Note (D-11):** Blue/green migration strategy is planned for v0.13.
> When implemented, `blue-green` may be added to `maintenance_window_applies_to`.

## Concurrent apply protection

`aqueduct apply` acquires a row-level lock in `aqueduct.locks` before executing:

```sql
SELECT * FROM aqueduct.locks
WHERE project = $1
FOR UPDATE NOWAIT;
```

- The lock is automatically released on crash (unlike advisory locks) when the
  client disconnects.
- A background heartbeat updates `acquired_at` every `ttl / 3` during long migrations.
- If the CLI crashes, the lock expires after one TTL and the next `aqueduct apply
  --resume` can recover.

To release a stale lock manually:

```bash
aqueduct unlock --project my-project --to prod
```

## Crash recovery with --resume

If `aqueduct apply` crashes mid-migration:

```bash
aqueduct apply --to prod --resume
```

`--resume` reads `aqueduct.migrations.progress`, identifies completed steps, and skips
them. For ambiguous steps (where the CLI cannot determine completion from the catalog),
it reports a diagnostic and exits non-zero — the operator must inspect and use
`--force-retry` or `--force-skip` for the ambiguous step.

## Failover during a migration

Between each plan step, `aqueduct apply` re-checks the primary's `system_identifier`
and `timeline_id`. On failover detection:

1. The CLI marks the migration as `status = 'interrupted'` in `aqueduct.migrations`.
2. It exits non-zero with a clear message including the last completed step.
3. Recovery: reconnect to the new primary and run `aqueduct apply --resume`.

## Example: Patroni + maintenance window

```toml
# aqueduct.toml
[targets.prod]
dsn = "${PROD_DSN}"

[apply]
maintenance_window = "01:00-03:00 UTC"
lock_timeout = "60s"
patroni_endpoint = "${PATRONI_ENDPOINT}"
```

```bash
# In a CI pipeline:
aqueduct apply --to prod --yes
# → Exits non-zero with "outside maintenance window" if the run is
#   outside 01:00–03:00 UTC and the plan contains Rebuild steps.
# → Applies immediately if the plan contains only Free/In-place steps.
```
