# API Reference

## CLI Commands

### `aqueduct plan`

Compute and display the migration plan without executing any DDL.

```
USAGE:
    aqueduct plan [OPTIONS] --to <TARGET>

OPTIONS:
    --to <TARGET>           Target database (name from aqueduct.toml)
    --project <NAME>        Project name (defaults to [project].name in aqueduct.toml)
    --format <FORMAT>       Output format: text (default), json, yaml, markdown
    --no-cost               Suppress cost/class column in output
    --fail-on-drift         Exit 1 when drift is detected (live ≠ recorded last-apply)
    --fail-if-changed       Exit 1 when the plan is non-empty (synonym for --fail-on-drift)
    --validate-ivm          Validate IVM supportability before generating the plan
    --allow-plaintext-password  Allow DSN with embedded password (not recommended)
    -q, --quiet             Only exit code (0 = empty plan, 1 = non-empty, 2 = error)
```

**Exit codes:** `0` empty plan, `1` non-empty plan, `2` error.

---

### `aqueduct apply`

Execute the migration plan.

```
USAGE:
    aqueduct apply [OPTIONS] --to <TARGET>

OPTIONS:
    --to <TARGET>               Target database
    --yes / -y                  Skip confirmation prompt
    --dry-run                   Print plan only; do not execute
    --resume                    Resume an interrupted migration
    --ignore-maintenance-window Override maintenance window restrictions
    --patroni-endpoint <URL>    Patroni REST API endpoint for primary check
    --allow-plaintext-password  Allow DSN with embedded password (not recommended)
    --timeout <DURATION>        Timeout per step (default: 30m)
    --lock-timeout <DURATION>   Timeout acquiring the project lock (default: 60s)
```

---

### `aqueduct validate`

Validate all migration files without connecting to a database.

```
USAGE:
    aqueduct validate [OPTIONS]

OPTIONS:
    --project-dir <DIR>   Project root (default: current directory)
    --strict              Treat warnings as errors
```

---

### `aqueduct lint`

Lint migration files for common mistakes and anti-patterns.

```
USAGE:
    aqueduct lint [OPTIONS]

OPTIONS:
    --project-dir <DIR>   Project root (default: current directory)
    --fix                 Auto-fix safe lint warnings
```

Lint checks include:

| Code | Severity | Message |
|------|----------|---------|
| L001 | warn | `schedule < 5s` — very aggressive refresh |
| L002 | warn | DIFFERENTIAL mode with non-deterministic aggregate |
| L003 | error | Cyclic dependency detected |
| L004 | warn | Undeclared dependency (inferred from query but not in `@aqueduct:depends_on`) |
| L005 | error | Unknown front-matter directive |
| L006 | warn | Consumer view references a non-managed stream table |

---

### `aqueduct status`

Show the live state of all managed stream tables.

```
USAGE:
    aqueduct status [OPTIONS] --to <TARGET>

OPTIONS:
    --to <TARGET>         Target database
    --format <FMT>        text (default), json, yaml
    --watch               Re-poll every 5 seconds; reconnects on network error (Ctrl-C to stop)
```

---

### `aqueduct diff`

Compute and display the semantic diff between desired state (migration files) and
live database state, without producing a full migration plan.

```
USAGE:
    aqueduct diff [OPTIONS] --to <TARGET>

OPTIONS:
    --to <TARGET>           Target database
    --project-dir <DIR>     Project root (default: current directory)
    --table <NAME>          Filter to a single table (schema.name)
    --format <FORMAT>       text (default), json, yaml, markdown
    --allow-plaintext-password  Allow DSN with embedded password
```

**Exit codes:** `0` no drift, `1` drift detected, `2` error.

Each delta is labelled with a kind indicator:

| Kind | Symbol | Meaning |
|------|--------|--------|
| `Create` | `+` | Table present in files, absent in live state |
| `Drop` | `-` | Table present in live state, absent in files |
| `AlterQuery` | `~` | SQL query changed |
| `AlterSchedule` | `~` | Schedule changed only |
| `AlterRefreshMode` | `~` | Refresh mode changed |
| `Unchanged` | (omitted) | No change |

---

### `aqueduct import`

Generate migration files from a live `pg_trickle` deployment.

```
USAGE:
    aqueduct import [OPTIONS] --from <TARGET>

OPTIONS:
    --from <TARGET>         Source database
    --output <DIR>          Output directory (default: current directory)
    --exclude-pattern <RE>  Exclude tables matching pattern (repeatable)
    --overwrite             Overwrite existing files
```

---

### `aqueduct destroy`

Remove all project resources from a target database.

```
USAGE:
    aqueduct destroy [OPTIONS] --project <NAME> --to <TARGET>

OPTIONS:
    --to <TARGET>     Target database
    --project <NAME>  Project name
    --dry-run         Preview; do not execute
    --confirm         Execute (required alongside --dry-run absent)
```

---

### `aqueduct rollback`

Roll back to a previous DAG version.

```
USAGE:
    aqueduct rollback [OPTIONS] --to <TARGET>

OPTIONS:
    --to <TARGET>          Target database
    --to-version <N>       Version number to roll back to (default: previous)
    --accept-data-loss     Allow rollback past the lossless window expiry
    --dry-run              Preview plan only
```

---

### `aqueduct unlock`

Release a stale project lock.

```
USAGE:
    aqueduct unlock --project <NAME> --to <TARGET>
```

---

## Front-matter directives

Front-matter directives are single-line SQL comments at the top of a migration file:

```sql
-- @aqueduct:<directive> = "<value>"
```

| Directive | Valid in | Values | Description |
|-----------|---------|--------|-------------|
| `schedule` | streams | duration string | Refresh interval. Examples: `"30s"`, `"1m"`, `"1h"`. |
| `refresh_mode` | streams | `"DIFFERENTIAL"` \| `"FULL"` | Refresh strategy. Default: `"DIFFERENTIAL"`. |
| `cdc_mode` | streams | `"trigger"` \| `"wal"` \| `"none"` | CDC capture granularity. Aliases: `"ROW"` / `"STATEMENT"` → `"trigger"`, `"WAL"` → `"wal"`, `"DISABLED"` → `"none"`. |
| `cypher_source` | streams | relative file path | Path to a Cypher query file used to drive stream-table population. |
| `depends_on` | streams | JSON array of `"schema.table"` | Explicit upstream dependencies. |
| `kind` | consumers | `"consumer"` | Mark file as a consumer-view migration. |
| `source` | consumers | `"schema.table"` | Upstream stream table for a consumer view. |
| `expose_as` | consumers | `"schema.view"` | Target name for the consumer view. |
| `owned` | sources | `"true"` \| `"false"` | Whether aqueduct manages DDL for this base table. |

Duration string format: `<integer><unit>` where unit is `s` (seconds), `m` (minutes),
`h` (hours), `d` (days). Examples: `"5s"`, `"30m"`, `"6h"`, `"1d"`.

---

## `aqueduct.toml` configuration

```toml
[project]
name = "my-analytics"            # required; used as project namespace in catalog
version = "0.7.0"                # optional; recorded in aqueduct.migrations

[targets.prod]
dsn = "${PROD_DSN}"              # required; may use ${ENV_VAR} or ${secret:...}

[targets.staging]
dsn = "${STAGING_DSN}"

[apply]
maintenance_window = "02:00-04:00 UTC"  # optional; gate Rebuild/BlueGreen steps
maintenance_window_applies_to = ["rebuild", "blue-green"]
lock_timeout = "60s"                    # default 60s
step_timeout = "30m"                    # default 30m per plan step
patroni_endpoint = "${PATRONI_ENDPOINT}" # optional; Patroni primary check URL

[lint]
max_refresh_interval = "1h"     # warn if schedule > this
min_refresh_interval = "5s"     # warn if schedule < this
```
