# API Reference

> **Auto-generated** from `aqueduct --help` output.
> Do not edit by hand — run `just gen-docs` to regenerate.

Binary version: `aqueduct 0.20.0`

## aqueduct plan

```text
Compute a migration plan by diffing desired vs. actual DAG state

Usage: aqueduct plan [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --format <FORMAT>            Output format: text (default), json, markdown, or yaml [default: text]
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --fail-if-changed            Exit non-zero if the plan is non-empty (useful in CI). Synonymous with the default exit-code behaviour (exit 1 for non-empty plans)
      --fail-on-drift              Exit non-zero if drift is detected between live state and last-applied version
      --validate-ivm               Check IVM supportability of all queries
      --explain-cost               Show per-step cost estimates (row count, estimated duration)
      --out <OUT>                  Write the plan to a JSON file for use with `apply --plan`
      --allow-plaintext-password   Allow DSN with embedded plaintext password (not recommended outside CI/dev)
  -h, --help                       Print help
```

## aqueduct apply

```text
Apply a migration plan to the target database

Usage: aqueduct apply [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --dry-run                    Show what would be done without executing
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --ignore-maintenance-window  Override the maintenance window restriction
      --allow-rebuild              Allow rebuild-class migrations even when allow_full_refresh = false
      --strategy <STRATEGY>        Migration strategy: "default" or "blue-green" [default: default]
      --plan <PLAN>                Path to a pre-computed plan JSON file (produced by `aqueduct plan --out`). The plan's spec_hash is validated against the current migration files
      --no-immediate-downgrade     Reject plans that would temporarily downgrade an IMMEDIATE refresh-mode table to DIFFERENTIAL during a rebuild
      --resume                     Resume an interrupted migration
      --force-retry <FORCE_RETRY>  Force-retry the step at the given index (CORR-2 / v0.15). Requires --yes
      --force-skip <FORCE_SKIP>    Force-skip the step at the given index (CORR-2 / v0.15). Requires --yes
      --print-plan                 Print the plan before applying
  -y, --yes                        Skip the interactive confirmation prompt (for CI and scripted use)
      --allow-plaintext-password   Allow DSN with embedded plaintext password (not recommended outside CI/dev)
  -h, --help                       Print help
```

## aqueduct diff

```text
Show per-table diff between desired (migration files) and actual (live) state

Usage: aqueduct diff [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --table <TABLE>              Show diff only for the named stream table
      --format <FORMAT>            Output format: text (default), json, markdown, or yaml [default: text]
      --fail-on-drift              Exit 1 when any diff delta is non-Unchanged, exit 0 for a clean diff (U-03)
  -h, --help                       Print help
```

## aqueduct status

```text
Show the current project state and drift status

Usage: aqueduct status [OPTIONS]

Options:
      --dsn <DSN>
          PostgreSQL connection string
      --log-format <LOG_FORMAT>
          Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>
          Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>
          Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>
          Project directory [default: .]
      --quiet
          Suppress all non-error output (useful for scripted pipelines)
      --format <FORMAT>
          Output format: text or json [default: text]
      --porcelain
          Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --fail-on-drift
          Exit non-zero if drift is detected
      --watch
          Watch mode: poll continuously instead of running once
      --interval <INTERVAL>
          Polling interval in watch mode (e.g. "30s", "1m") [default: 30s]
      --max-drift-count <MAX_DRIFT_COUNT>
          Exit after N consecutive drift detections (0 = never exit on drift) [default: 0]
      --poll-interval <POLL_INTERVAL>
          Polling interval for --watch mode (e.g. "30s", "1m"). Alias for --interval [default: 30s]
  -h, --help
          Print help
```

## aqueduct validate

```text
Offline validation of migration files (no database connection required)

Usage: aqueduct validate [OPTIONS]

Options:
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --format <FORMAT>            Output format: text or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --strict                     Treat warnings as errors and exit non-zero if any are present
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
  -h, --help                       Print help
```

## aqueduct lint

```text
Check migration files for risky or non-optimal patterns

Usage: aqueduct lint [OPTIONS]

Options:
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --format <FORMAT>            Output format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --fail-on-warn               Exit non-zero if any warnings are found (in addition to errors)
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
  -h, --help                       Print help
```

## aqueduct import

```text
Import an existing pg_trickle deployment into a migrations directory

Usage: aqueduct import [OPTIONS]

Options:
      --from <FROM>
          PostgreSQL connection string to import from
      --log-format <LOG_FORMAT>
          Log format: text (default) or json [default: text]
      --from-target <FROM_TARGET>
          Target name from aqueduct.toml (used as --from source)
      --log-level <LOG_LEVEL>
          Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --output <OUTPUT>
          Output directory for the generated project [default: .]
      --quiet
          Suppress all non-error output (useful for scripted pipelines)
      --porcelain
          Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --project-dir <PROJECT_DIR>
          Project directory (for resolving config) [default: .]
      --exclude-pattern <EXCLUDE_PATTERN>
          Exclude tables matching these patterns (can be specified multiple times)
      --no-default-exclusions
          Do not apply default exclusion patterns (_pg_ripple.*, _pg_eddy.*, _riverbank.*)
  -h, --help
          Print help
```

## aqueduct rollback

```text
Rollback to a previous DAG version

Usage: aqueduct rollback [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --to-version <TO_VERSION>    Roll back to a specific version (default: previous version)
      --accept-data-loss           Accept data loss for rebuild-class rollbacks where the lossless window has passed
      --within-window              Allow PointInTime rollbacks even when close to window expiry
      --dry-run                    Show what would be done without executing
  -h, --help                       Print help
```

## aqueduct promote

```text
Promote a validated migrations directory from one environment to another

Usage: aqueduct promote [OPTIONS] --from <FROM> --to <TO>

Options:
      --from <FROM>                Source environment name (e.g. "dev")
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Destination environment name (e.g. "staging")
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --skip-source-check          Skip the source-clean validation (use in CI when source is known good)
      --dry-run                    Show what would be done without executing
      --yes                        Auto-approve the promotion (skip interactive prompt)
  -h, --help                       Print help
```

## aqueduct destroy

```text
Destroy all stream tables, consumer views, and catalog entries for a project

Usage: aqueduct destroy [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --confirm                    Confirm the destructive operation (required unless --dry-run is set)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --dry-run                    Show what would be destroyed without executing
      --force-cascade              Also drop dependent database objects (views, etc.) via CASCADE. Without this flag, `destroy` refuses if any stream table has dependents
      --force-unowned              Skip ownership verification. By default, `destroy` refuses to drop stream tables that are not registered as owned by this project
  -h, --help                       Print help
```

## aqueduct unlock

```text
Release a stale project lock (emergency use)

Usage: aqueduct unlock [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --force                      Force unlock regardless of lock holder (requires confirmation)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
  -h, --help                       Print help
```

## aqueduct init

```text
Bootstrap the aqueduct catalog schema in the target database

Usage: aqueduct init [OPTIONS]

Options:
      --dsn <DSN>                  PostgreSQL connection string (DSN). Overrides the config file
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --to <TO>                    Target name from aqueduct.toml
      --project-dir <PROJECT_DIR>  Project directory (default: current directory) [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
      --schema <SCHEMA>            Override the catalog schema name (default: "aqueduct") [default: aqueduct]
      --scaffold                   Scaffold a new project directory with aqueduct.toml and migrations/
      --allow-plaintext-password   Allow DSN with embedded plaintext password (not recommended outside CI/dev)
  -h, --help                       Print help
```

## aqueduct preview

```text
Create, inspect, or tear down a preview environment for a candidate change

Usage: aqueduct preview [OPTIONS]

Options:
      --dsn <DSN>
          PostgreSQL connection string

      --log-format <LOG_FORMAT>
          Log format: text (default) or json
          
          [default: text]

      --log-level <LOG_LEVEL>
          Verbosity: error, warn, info (default), debug, trace
          
          [env: AQUEDUCT_LOG=]
          [default: info]

      --to <TO>
          Target name from aqueduct.toml

      --project-dir <PROJECT_DIR>
          Project directory
          
          [default: .]

      --quiet
          Suppress all non-error output (useful for scripted pipelines)

      --branch <BRANCH>
          Branch name to create the preview for (used to derive the schema name). Example: `feat/my-feature` → `aqueduct_preview_feat_my_feature`

      --porcelain
          Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)

      --backend <BACKEND>
          Preview backend: native (default), cnpg, or neon
          
          [default: native]

      --sample <SAMPLE>
          Sample fraction for TABLESAMPLE (0.0–1.0). Default: 0.1 (10%)
          
          [default: 0.1]

      --recreate
          Drop and recreate the preview schema if it already exists

      --cleanup
          Drop an existing preview environment

      --list
          List all preview environments in the database

      --cnpg-endpoint <CNPG_ENDPOINT>
          CloudNativePG operator endpoint (required for --backend cnpg)

      --neon-api-token <NEON_API_TOKEN>
          Neon API token (required for --backend neon)
          
          [env: NEON_API_TOKEN=]

      --neon-project-id <NEON_PROJECT_ID>
          Neon project ID (required for --backend neon)
          
          [env: NEON_PROJECT_ID=]

      --table <TABLE>
          Restrict the preview to the subgraph anchored at this table name (P-07 / v0.12).
          
          Only the named table, its ancestors (dependencies), and its descendants (tables that depend on it) are included in the preview. Example: `--table public.order_totals`

  -h, --help
          Print help (see a summary with '-h')
```

## aqueduct ingest

```text
Ingest compiled dbt artefacts into an aqueduct migrations directory

Usage: aqueduct ingest [OPTIONS] --from <TYPE> --target <PATH>

Options:
      --from <TYPE>                Source type to ingest from. Currently only `dbt-target` is supported
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --target <PATH>              Path to the dbt target directory (contains `manifest.json` and `compiled/`). Required when `--from dbt-target`
      --project-dir <PROJECT_DIR>  Project directory to write migration files into (default: current directory) [default: .]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --format <FORMAT>            Output format: text (default) or json [default: text]
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
  -h, --help                       Print help
```

## aqueduct fmt

```text
Canonicalise the SQL body and front-matter directives in migration files

Usage: aqueduct fmt [OPTIONS]

Options:
      --log-format <LOG_FORMAT>    Log format: text (default) or json [default: text]
      --project-dir <PROJECT_DIR>  Project directory [default: .]
      --check                      Check formatting only — exit non-zero if any file is not canonical. Does not write any files
      --log-level <LOG_LEVEL>      Verbosity: error, warn, info (default), debug, trace [env: AQUEDUCT_LOG=] [default: info]
      --format <FORMAT>            Output format: text (default) or json [default: text]
      --quiet                      Suppress all non-error output (useful for scripted pipelines)
      --porcelain                  Emit machine-parseable key=value output on stdout (implies --quiet for decorative output)
  -h, --help                       Print help
```

