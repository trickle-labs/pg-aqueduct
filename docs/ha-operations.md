# HA Operations Guide

## Overview

`pg_aqueduct` is designed to operate safely in high-availability PostgreSQL deployments,
including Patroni clusters, CloudNativePG, and Stolon.

## Primary detection (v0.17)

`aqueduct apply` always checks that the target is a primary **before** and **between** every
migration step:

```sql
SELECT pg_is_in_recovery();   -- must return false
```

If the target is a standby (failover happened), the CLI marks the migration as
`interrupted` and exits non-zero with a descriptive error.

## Supported HA backends

The `detect_ha_backend()` function (in `aqueduct_core::ha`) detects the cluster type
automatically:

| Backend | Detection method |
|---------|-----------------|
| Plain primary | `pg_is_in_recovery() = false` |
| Patroni | HTTP `GET /master` on `--patroni-endpoint` (returns HTTP 200 when primary) |
| CloudNativePG | `app.cnpg.cluster_name` GUC present |
| Stolon | `application_name` contains `stolon-keeper` |

### Patroni failover detection

Pass `--patroni-endpoint` to enable Patroni-aware failover detection:

```bash
aqueduct apply --to prod --patroni-endpoint http://patroni-api:8008
```

After **every** migration step, the CLI calls `GET /master` on the Patroni REST API.
A non-200 response (e.g., HTTP 503 when a replica answers) means the primary has
changed. The migration is marked `interrupted` and the CLI exits non-zero.

### CloudNativePG

No additional flags required. The CLI reads the `app.cnpg.cluster_name` GUC to detect
a CloudNativePG cluster and relies on the standard primary check (`pg_is_in_recovery()`).

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

Between each plan step, `aqueduct apply` calls `check_still_primary()` which executes
`SELECT pg_is_in_recovery()`. If the optional `--patroni-endpoint` is set, it also
calls `GET /master` on the Patroni REST API. On failover detection:

1. The CLI marks the migration as `status = 'interrupted'` in `aqueduct.migrations`.
2. It exits non-zero with a clear message including the last completed step.
3. Recovery: reconnect to the new primary and run `aqueduct apply --resume`.

The `interrupted` status is distinct from `failed` — it signals that the migration was
cleanly paused due to an infrastructure event, not a SQL error.

## Observability: per-step JSON events (v0.17)

`aqueduct apply` emits structured JSON events to **stderr** for every migration step:

```json
{"schema_version":1,"event":"step_start","step_index":0,"step_type":"LockDag","migration_id":42,"project":"my-project"}
{"schema_version":1,"event":"step_complete","step_index":0,"step_type":"LockDag","migration_id":42,"project":"my-project","duration_ms":3}
{"schema_version":1,"event":"step_failed","step_index":1,"step_type":"Backfill","migration_id":42,"project":"my-project","duration_ms":1500,"error":"deadlock detected"}
```

The complete event schema is documented in [`cli-events-schema.json`](cli-events-schema.json).

## Migration audit log

Use `aqueduct audit` to review migration history:

```bash
aqueduct audit --to prod                  # table format (default)
aqueduct audit --to prod --format json    # JSON array
aqueduct audit --to prod --limit 50       # show 50 most recent
```

## Example: Patroni + maintenance window

```toml
# aqueduct.toml
[targets.prod]
dsn = "${PROD_DSN}"

[apply]
maintenance_window = "01:00-03:00 UTC"
lock_timeout = "60s"
```

```bash
# In a CI pipeline with Patroni:
aqueduct apply --to prod --patroni-endpoint http://patroni-api:8008 --yes
# → Exits non-zero with "outside maintenance window" if outside 01:00–03:00 UTC
#   and the plan contains Rebuild steps.
# → Applies immediately if the plan contains only Free/In-place steps.
# → Marks migration interrupted (not failed) if Patroni reports a failover.
```
