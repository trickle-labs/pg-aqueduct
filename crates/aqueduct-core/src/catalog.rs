/// SQL statements for bootstrapping the aqueduct catalog schema.
/// These are embedded directly in the binary via include_str!() in a real deployment;
/// for v0.1 we keep them as a constant in this module.
pub const CATALOG_SCHEMA_VERSION: u32 = 3;

/// The SQL to create the aqueduct catalog schema (v1, baseline).
pub const CATALOG_INIT_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS aqueduct;

-- Records a full snapshot of the DAG spec at each successful apply.
CREATE TABLE IF NOT EXISTS aqueduct.dag_versions (
    version    bigserial PRIMARY KEY,
    project    text      NOT NULL,
    spec_hash  bytea     NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    applied_by text      NOT NULL,
    plan_jsonb jsonb     NOT NULL,
    spec_jsonb jsonb     NOT NULL
);

-- Tracks each migration run with its steps and progress.
CREATE TABLE IF NOT EXISTS aqueduct.migrations (
    id              bigserial PRIMARY KEY,
    project         text      NOT NULL,
    from_version    bigint REFERENCES aqueduct.dag_versions(version),
    to_version      bigint REFERENCES aqueduct.dag_versions(version),
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text NOT NULL DEFAULT 'running',
    plan            jsonb NOT NULL DEFAULT '{}',
    progress        jsonb NOT NULL DEFAULT '{}',
    cli_version     text,
    plan_format_version int NOT NULL DEFAULT 1,
    CONSTRAINT status_check CHECK (status IN ('running', 'committed', 'failed', 'rolled_back', 'recoverable_failure'))
);

-- Serialises concurrent apply runs per project.
CREATE TABLE IF NOT EXISTS aqueduct.locks (
    project      text PRIMARY KEY,
    holder       text      NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    ttl          interval  NOT NULL DEFAULT '30s',
    paused_nodes jsonb     NOT NULL DEFAULT '[]'
);

-- Per-cluster calibration data.
CREATE TABLE IF NOT EXISTS aqueduct.cluster_profile (
    key         text PRIMARY KEY,
    value_jsonb jsonb NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now()
);

-- Record the catalog schema version.
INSERT INTO aqueduct.cluster_profile (key, value_jsonb, measured_at)
VALUES ('catalog_schema_version', '1'::jsonb, now())
ON CONFLICT (key) DO NOTHING;
"#;

/// Catalog migration from v1 to v2: adds DDL event log table for companion extension support.
pub const CATALOG_MIGRATE_V1_TO_V2_SQL: &str = r#"
-- DDL event log (populated by the optional pg_aqueduct companion extension).
-- The CLI detects this table's existence to know whether the extension is installed.
-- When the companion extension is absent, the CLI creates this table but leaves it empty
-- (falling back to polling-based drift detection).
CREATE TABLE IF NOT EXISTS aqueduct.ddl_log (
    id           bigserial PRIMARY KEY,
    object_type  text      NOT NULL,
    schema_name  text      NOT NULL,
    object_name  text      NOT NULL,
    command_tag  text      NOT NULL,
    command_text text,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    pg_role      text      NOT NULL DEFAULT current_role
);

-- Consumer view registry: tracks consumer views managed by aqueduct.
CREATE TABLE IF NOT EXISTS aqueduct.consumer_views (
    id          bigserial PRIMARY KEY,
    project     text      NOT NULL,
    name        text      NOT NULL,
    expose_as   text      NOT NULL,  -- "schema.view_name"
    source      text      NOT NULL,  -- "schema.table_name"
    sql_body    text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project, name)
);

-- Blue/green deployment tracking.
CREATE TABLE IF NOT EXISTS aqueduct.blue_green_deployments (
    id             bigserial PRIMARY KEY,
    project        text      NOT NULL,
    from_version   bigint    REFERENCES aqueduct.dag_versions(version),
    to_version     bigint    REFERENCES aqueduct.dag_versions(version),
    blue_schema    text      NOT NULL,
    green_schema   text      NOT NULL,
    status         text      NOT NULL DEFAULT 'active',
    started_at     timestamptz NOT NULL DEFAULT now(),
    swapped_at     timestamptz,
    retired_at     timestamptz,
    retire_at      timestamptz,  -- scheduled retirement time
    CONSTRAINT bg_status_check CHECK (status IN ('active', 'swapped', 'retired', 'failed'))
);

-- Bump catalog version to 2.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '2'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Catalog migration from v2 to v3: adds stream_table_ownership table and
/// recoverable_failure status for the migrations table.
pub const CATALOG_MIGRATE_V2_TO_V3_SQL: &str = r#"
-- Multi-project ownership registry: tracks which project owns each stream table.
-- Populated on CreateStreamTable, removed on DropStreamTable.
CREATE TABLE IF NOT EXISTS aqueduct.stream_table_ownership (
    project      text        NOT NULL,
    schema_name  text        NOT NULL,
    table_name   text        NOT NULL,
    managed_since timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name)
);

-- Add recoverable_failure status to migrations constraint.
-- We add the value by temporarily dropping and recreating the constraint.
ALTER TABLE aqueduct.migrations DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE aqueduct.migrations
    ADD CONSTRAINT status_check
    CHECK (status IN ('running', 'committed', 'failed', 'rolled_back', 'recoverable_failure'));

-- Bump catalog version to 3.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '3'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Combined init SQL for fresh installations (creates v2 schema directly).
/// Kept for backward-compatibility; new code should use CATALOG_INIT_V3_SQL.
pub const CATALOG_INIT_V2_SQL: &str = CATALOG_INIT_V3_SQL;

/// Combined init SQL for fresh installations (creates v3 schema directly).
pub const CATALOG_INIT_V3_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS aqueduct;

-- Records a full snapshot of the DAG spec at each successful apply.
CREATE TABLE IF NOT EXISTS aqueduct.dag_versions (
    version    bigserial PRIMARY KEY,
    project    text      NOT NULL,
    spec_hash  bytea     NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    applied_by text      NOT NULL,
    plan_jsonb jsonb     NOT NULL,
    spec_jsonb jsonb     NOT NULL
);

-- Tracks each migration run with its steps and progress.
CREATE TABLE IF NOT EXISTS aqueduct.migrations (
    id              bigserial PRIMARY KEY,
    project         text      NOT NULL,
    from_version    bigint REFERENCES aqueduct.dag_versions(version),
    to_version      bigint REFERENCES aqueduct.dag_versions(version),
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text NOT NULL DEFAULT 'running',
    plan            jsonb NOT NULL DEFAULT '{}',
    progress        jsonb NOT NULL DEFAULT '{}',
    cli_version     text,
    plan_format_version int NOT NULL DEFAULT 1,
    CONSTRAINT status_check CHECK (status IN ('running', 'committed', 'failed', 'rolled_back', 'recoverable_failure'))
);

-- Serialises concurrent apply runs per project.
CREATE TABLE IF NOT EXISTS aqueduct.locks (
    project      text PRIMARY KEY,
    holder       text      NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    ttl          interval  NOT NULL DEFAULT '30s',
    paused_nodes jsonb     NOT NULL DEFAULT '[]'
);

-- Per-cluster calibration data.
CREATE TABLE IF NOT EXISTS aqueduct.cluster_profile (
    key         text PRIMARY KEY,
    value_jsonb jsonb NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now()
);

-- DDL event log (populated by the optional pg_aqueduct companion extension).
CREATE TABLE IF NOT EXISTS aqueduct.ddl_log (
    id           bigserial PRIMARY KEY,
    object_type  text      NOT NULL,
    schema_name  text      NOT NULL,
    object_name  text      NOT NULL,
    command_tag  text      NOT NULL,
    command_text text,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    pg_role      text      NOT NULL DEFAULT current_role
);

-- Consumer view registry.
CREATE TABLE IF NOT EXISTS aqueduct.consumer_views (
    id          bigserial PRIMARY KEY,
    project     text      NOT NULL,
    name        text      NOT NULL,
    expose_as   text      NOT NULL,
    source      text      NOT NULL,
    sql_body    text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project, name)
);

-- Blue/green deployment tracking.
CREATE TABLE IF NOT EXISTS aqueduct.blue_green_deployments (
    id             bigserial PRIMARY KEY,
    project        text      NOT NULL,
    from_version   bigint    REFERENCES aqueduct.dag_versions(version),
    to_version     bigint    REFERENCES aqueduct.dag_versions(version),
    blue_schema    text      NOT NULL,
    green_schema   text      NOT NULL,
    status         text      NOT NULL DEFAULT 'active',
    started_at     timestamptz NOT NULL DEFAULT now(),
    swapped_at     timestamptz,
    retired_at     timestamptz,
    retire_at      timestamptz,
    CONSTRAINT bg_status_check CHECK (status IN ('active', 'swapped', 'retired', 'failed'))
);

-- Multi-project ownership registry: tracks which project owns each stream table.
-- Populated on CreateStreamTable, removed on DropStreamTable.
CREATE TABLE IF NOT EXISTS aqueduct.stream_table_ownership (
    project       text        NOT NULL,
    schema_name   text        NOT NULL,
    table_name    text        NOT NULL,
    managed_since timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name)
);

-- Record the catalog schema version.
INSERT INTO aqueduct.cluster_profile (key, value_jsonb, measured_at)
VALUES ('catalog_schema_version', '3'::jsonb, now())
ON CONFLICT (key) DO NOTHING;
"#;

/// SQL to check whether the aqueduct catalog already exists.
pub const CATALOG_EXISTS_SQL: &str = r#"
SELECT EXISTS (
    SELECT 1
    FROM information_schema.schemata
    WHERE schema_name = 'aqueduct'
)
"#;

/// SQL to get the current catalog schema version.
pub const CATALOG_VERSION_SQL: &str = r#"
SELECT value_jsonb::int
FROM aqueduct.cluster_profile
WHERE key = 'catalog_schema_version'
"#;

/// SQL to check whether the pg_aqueduct companion extension DDL log is active.
/// The extension populates this table via a ddl_command_end event trigger.
/// When the table exists but has no trigger, it was created by the CLI (fallback mode).
pub const DETECT_EXTENSION_SQL: &str = r#"
SELECT EXISTS (
    SELECT 1
    FROM pg_event_trigger
    WHERE evtname = 'aqueduct_ddl_logger'
    AND evtenabled <> 'D'
)
"#;

/// SQL to read recent DDL events from the log (if the companion extension is installed).
pub const READ_DDL_LOG_SQL: &str = r#"
SELECT id, object_type, schema_name, object_name, command_tag, command_text, recorded_at, pg_role
FROM aqueduct.ddl_log
ORDER BY recorded_at DESC
LIMIT $1
"#;

/// SQL to insert a DAG version record.
pub const INSERT_DAG_VERSION_SQL: &str = r#"
INSERT INTO aqueduct.dag_versions
    (project, spec_hash, applied_by, plan_jsonb, spec_jsonb)
VALUES ($1, $2, $3, $4, $5)
RETURNING version
"#;

/// SQL to get the latest DAG version for a project.
pub const GET_LATEST_VERSION_SQL: &str = r#"
SELECT version, spec_jsonb, applied_at, applied_by
FROM aqueduct.dag_versions
WHERE project = $1
ORDER BY version DESC
LIMIT 1
"#;

/// SQL to start a migration record.
pub const START_MIGRATION_SQL: &str = r#"
INSERT INTO aqueduct.migrations
    (project, from_version, started_at, status, plan, cli_version, plan_format_version)
VALUES ($1, $2, now(), 'running', $3, $4, 1)
RETURNING id
"#;

/// SQL to finish a migration record.
pub const FINISH_MIGRATION_SQL: &str = r#"
UPDATE aqueduct.migrations
SET finished_at = now(), status = $2, to_version = $3, progress = $4
WHERE id = $1
"#;

/// SQL to acquire an advisory lock on the project (in locks table).
pub const ACQUIRE_LOCK_SQL: &str = r#"
INSERT INTO aqueduct.locks (project, holder, acquired_at, ttl)
VALUES ($1, $2, now(), $3::text::interval)
ON CONFLICT (project) DO UPDATE
    SET holder = EXCLUDED.holder,
        acquired_at = EXCLUDED.acquired_at,
        ttl = EXCLUDED.ttl
WHERE aqueduct.locks.acquired_at + aqueduct.locks.ttl < now()
RETURNING project
"#;

/// SQL to release a lock.
pub const RELEASE_LOCK_SQL: &str = r#"
DELETE FROM aqueduct.locks WHERE project = $1 AND holder = $2
"#;

/// SQL to get the current lock holder.
pub const GET_LOCK_SQL: &str = r#"
SELECT holder, acquired_at, ttl
FROM aqueduct.locks
WHERE project = $1
"#;

/// SQL to check whether the database is a primary (not a hot standby).
pub const IS_PRIMARY_SQL: &str = r#"
SELECT NOT pg_is_in_recovery()
"#;

/// SQL to update migration progress (step index completed).
pub const UPDATE_MIGRATION_PROGRESS_SQL: &str = r#"
UPDATE aqueduct.migrations SET progress = $2 WHERE id = $1
"#;

/// SQL to renew (heartbeat) a lock by updating acquired_at.
/// Includes holder check so a different process cannot renew a lock it doesn't own.
pub const HEARTBEAT_LOCK_SQL: &str = r#"
UPDATE aqueduct.locks SET acquired_at = now()
WHERE project = $1 AND holder = $2
"#;

/// SQL to look up the running migration for a project and return its progress.
pub const GET_RUNNING_MIGRATION_SQL: &str = r#"
SELECT id, progress
FROM aqueduct.migrations
WHERE project = $1 AND status = 'running'
ORDER BY started_at DESC
LIMIT 1
"#;

/// SQL to look up running OR recoverable_failure migrations for resume.
pub const GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL: &str = r#"
SELECT id, progress, plan
FROM aqueduct.migrations
WHERE project = $1 AND status IN ('running', 'recoverable_failure')
ORDER BY started_at DESC
LIMIT 1
"#;

/// SQL to register stream table ownership.
pub const REGISTER_OWNERSHIP_SQL: &str = r#"
INSERT INTO aqueduct.stream_table_ownership (project, schema_name, table_name, managed_since)
VALUES ($1, $2, $3, now())
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET project = EXCLUDED.project, managed_since = EXCLUDED.managed_since
"#;

/// SQL to deregister stream table ownership.
pub const DEREGISTER_OWNERSHIP_SQL: &str = r#"
DELETE FROM aqueduct.stream_table_ownership
WHERE schema_name = $1 AND table_name = $2
"#;

/// SQL to look up the owner of a stream table.
pub const GET_OWNERSHIP_SQL: &str = r#"
SELECT project FROM aqueduct.stream_table_ownership
WHERE schema_name = $1 AND table_name = $2
"#;

/// SQL to list all stream tables owned by a project.
pub const LIST_OWNED_TABLES_SQL: &str = r#"
SELECT schema_name, table_name FROM aqueduct.stream_table_ownership
WHERE project = $1
ORDER BY schema_name, table_name
"#;

/// Ensure the catalog schema is up-to-date with the compiled-in schema version.
///
/// Every command that opens a database connection should call this before doing
/// anything else. It reads `catalog_schema_version` from `aqueduct.cluster_profile`
/// and applies any pending catalog migrations from the embedded SQL bundle.
pub async fn ensure_catalog_current(client: &tokio_postgres::Client) -> crate::error::Result<()> {
    // If the aqueduct schema does not exist yet, nothing to do — the user must
    // run `aqueduct init` first.
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'aqueduct')",
            &[],
        )
        .await?
        .get(0);

    if !schema_exists {
        return Ok(());
    }

    // Read the current catalog version.
    let row = client.query_opt(CATALOG_VERSION_SQL, &[]).await?;

    let current_version: i32 = row.map(|r| r.get::<_, i32>(0)).unwrap_or(1);

    if current_version < CATALOG_SCHEMA_VERSION as i32 {
        tracing::info!(
            "Upgrading catalog schema from v{} to v{}",
            current_version,
            CATALOG_SCHEMA_VERSION
        );
        // Apply the v1→v2 migration if needed.
        if current_version < 2 {
            client
                .batch_execute(CATALOG_MIGRATE_V1_TO_V2_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
        // Apply the v2→v3 migration if needed.
        if current_version < 3 {
            client
                .batch_execute(CATALOG_MIGRATE_V2_TO_V3_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
    }

    Ok(())
}

/// SQL to upsert a consumer view registration in the catalog.
pub const UPSERT_CONSUMER_VIEW_SQL: &str = r#"
INSERT INTO aqueduct.consumer_views (project, name, expose_as, source, sql_body, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, now(), now())
ON CONFLICT (project, name) DO UPDATE
    SET expose_as = EXCLUDED.expose_as,
        source    = EXCLUDED.source,
        sql_body  = EXCLUDED.sql_body,
        updated_at = now()
"#;

/// SQL to delete a consumer view registration from the catalog.
pub const DELETE_CONSUMER_VIEW_SQL: &str = r#"
DELETE FROM aqueduct.consumer_views WHERE project = $1 AND name = $2
"#;

/// SQL to record a blue/green deployment start.
pub const START_BLUE_GREEN_SQL: &str = r#"
INSERT INTO aqueduct.blue_green_deployments
    (project, from_version, blue_schema, green_schema, status, started_at)
VALUES ($1, $2, $3, $4, 'active', now())
RETURNING id
"#;

/// SQL to record a blue/green deployment swap.
pub const SWAP_BLUE_GREEN_SQL: &str = r#"
UPDATE aqueduct.blue_green_deployments
SET status = 'swapped', swapped_at = now(), to_version = $2, retire_at = now() + ($3 || ' seconds')::interval
WHERE id = $1
"#;

/// SQL to retire a blue/green deployment's blue schema.
pub const RETIRE_BLUE_GREEN_SQL: &str = r#"
UPDATE aqueduct.blue_green_deployments
SET status = 'retired', retired_at = now()
WHERE id = $1
"#;
