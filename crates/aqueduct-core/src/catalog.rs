/// SQL statements for bootstrapping the aqueduct catalog schema.
/// These are embedded directly in the binary via include_str!() in a real deployment;
/// for v0.1 we keep them as a constant in this module.
pub const CATALOG_SCHEMA_VERSION: u32 = 2;

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
    CONSTRAINT status_check CHECK (status IN ('running', 'committed', 'failed', 'rolled_back'))
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

/// Combined init SQL for fresh installations (creates v2 schema directly).
pub const CATALOG_INIT_V2_SQL: &str = r#"
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
    CONSTRAINT status_check CHECK (status IN ('running', 'committed', 'failed', 'rolled_back'))
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

-- Record the catalog schema version.
INSERT INTO aqueduct.cluster_profile (key, value_jsonb, measured_at)
VALUES ('catalog_schema_version', '2'::jsonb, now())
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
