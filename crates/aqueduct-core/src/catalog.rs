/// A validated, injection-safe catalog schema name (ARCH-1 / v0.15).
///
/// Wraps the schema name string and enforces at construction time:
/// - Only valid PostgreSQL identifier characters (letters, digits, `_`).
/// - Does not start with a digit.
/// - Does not collide with system schemas (`pg_catalog`, `information_schema`,
///   `pg_temp`, `public`).
/// - Does not contain SQL-injection markers (`--`, `;`, `$`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSchema(String);

impl CatalogSchema {
    /// Validate and construct a `CatalogSchema`.
    ///
    /// Returns `Err` with a human-readable message if the name is invalid.
    pub fn new(name: &str) -> crate::error::Result<Self> {
        if name.is_empty() {
            return Err(crate::error::AqueductError::Other(
                "catalog schema name must not be empty".to_string(),
            ));
        }
        // Check for SQL injection markers.
        if name.contains("--") || name.contains(';') || name.contains('$') {
            return Err(crate::error::AqueductError::Other(format!(
                "catalog schema name '{}' contains forbidden characters ('--', ';', '$')",
                name
            )));
        }
        // Only allow valid PostgreSQL identifier characters.
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(crate::error::AqueductError::Other(format!(
                "catalog schema name '{}' contains non-identifier characters \
                 (only letters, digits, and '_' are allowed)",
                name
            )));
        }
        // Must not start with a digit.
        if name
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        {
            return Err(crate::error::AqueductError::Other(format!(
                "catalog schema name '{}' must not start with a digit",
                name
            )));
        }
        // Reject system schema names.
        let reserved = ["pg_catalog", "information_schema", "pg_temp", "pg_toast"];
        let lower = name.to_lowercase();
        if reserved.contains(&lower.as_str()) || lower.starts_with("pg_") {
            return Err(crate::error::AqueductError::Other(format!(
                "catalog schema name '{}' collides with a reserved PostgreSQL schema name",
                name
            )));
        }
        Ok(Self(name.to_string()))
    }

    /// Returns the double-quoted, SQL-safe schema identifier.
    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }

    /// Returns the unquoted schema name (validated).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CatalogSchema {
    fn default() -> Self {
        // The default schema name is always valid — no need to validate.
        Self("aqueduct".to_string())
    }
}

impl std::fmt::Display for CatalogSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// SQL statements for bootstrapping the aqueduct catalog schema.
/// These are embedded directly in the binary via include_str!() in a real deployment;
/// for v0.1 we keep them as a constant in this module.
pub const CATALOG_SCHEMA_VERSION: u32 = 8;

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

/// Catalog migration from v3 to v4: adds performance indexes for common query patterns (P-02 / v0.12).
pub const CATALOG_MIGRATE_V3_TO_V4_SQL: &str = r#"
-- Performance indexes for high-traffic catalog queries (P-02 / v0.12).
-- These indexes cover the most common access patterns:
-- 1. Listing DAG versions for a project (plan/status/rollback commands).
-- 2. Checking migration status for a project (status/resume commands).
-- 3. Querying in-flight migrations by project and start time (heartbeat/resume).
-- 4. Looking up project locks (LockDag/UnlockDag steps).

CREATE INDEX IF NOT EXISTS aqueduct_dag_versions_project
    ON aqueduct.dag_versions (project, version DESC);

CREATE INDEX IF NOT EXISTS aqueduct_migrations_project_status
    ON aqueduct.migrations (project, status)
    WHERE status IN ('running', 'recoverable_failure');

CREATE INDEX IF NOT EXISTS aqueduct_migrations_project_started
    ON aqueduct.migrations (project, started_at DESC);

CREATE INDEX IF NOT EXISTS aqueduct_locks_project
    ON aqueduct.locks (project);

-- Bump catalog version to 4.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '4'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Catalog migration from v4 to v5: adds migration_steps table for per-step
/// observability (M-08 / v0.13).
pub const CATALOG_MIGRATE_V4_TO_V5_SQL: &str = r#"
-- Per-step execution tracking for advanced observability (M-08 / v0.13).
CREATE TABLE IF NOT EXISTS aqueduct.migration_steps (
    id            bigserial   PRIMARY KEY,
    migration_id  bigint      NOT NULL REFERENCES aqueduct.migrations(id),
    step_index    int         NOT NULL,
    step_type     text        NOT NULL,
    step_hash     text        NOT NULL,
    status        text        NOT NULL DEFAULT 'pending',
    started_at    timestamptz,
    finished_at   timestamptz,
    error_message text,
    UNIQUE (migration_id, step_index)
);

CREATE INDEX IF NOT EXISTS aqueduct_migration_steps_migration
    ON aqueduct.migration_steps (migration_id, step_index);

-- Bump catalog version to 5.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '5'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Combined init SQL for fresh installations (creates v2 schema directly).
/// Kept for backward-compatibility; new code should use CATALOG_INIT_V5_SQL.
pub const CATALOG_INIT_V2_SQL: &str = CATALOG_INIT_V5_SQL;

/// Combined init SQL for fresh installations (creates v3 schema directly).
/// Kept for backward-compatibility; new code should use CATALOG_INIT_V5_SQL.
pub const CATALOG_INIT_V3_SQL: &str = CATALOG_INIT_V5_SQL;

/// Combined init SQL for fresh installations at v4 (includes performance indexes).
/// Kept for backward-compatibility; new code should use CATALOG_INIT_V5_SQL.
pub const CATALOG_INIT_V4_SQL: &str = CATALOG_INIT_V5_SQL;

/// Combined init SQL for fresh installations at v5 (includes migration_steps table).
pub const CATALOG_INIT_V5_SQL: &str = r#"
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
    CONSTRAINT status_check CHECK (status IN ('running', 'committed', 'failed', 'rolled_back', 'recoverable_failure', 'interrupted'))
);

-- Per-step execution tracking for advanced observability (M-08 / v0.13).
CREATE TABLE IF NOT EXISTS aqueduct.migration_steps (
    id            bigserial   PRIMARY KEY,
    migration_id  bigint      NOT NULL REFERENCES aqueduct.migrations(id),
    step_index    int         NOT NULL,
    step_type     text        NOT NULL,
    step_hash     text        NOT NULL,
    status        text        NOT NULL DEFAULT 'pending',
    started_at    timestamptz,
    finished_at   timestamptz,
    error_message text,
    UNIQUE (migration_id, step_index)
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
CREATE TABLE IF NOT EXISTS aqueduct.stream_table_ownership (
    project       text        NOT NULL,
    schema_name   text        NOT NULL,
    table_name    text        NOT NULL,
    managed_since timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name)
);

-- Performance indexes (P-02 / v0.12).
CREATE INDEX IF NOT EXISTS aqueduct_dag_versions_project
    ON aqueduct.dag_versions (project, version DESC);

CREATE INDEX IF NOT EXISTS aqueduct_migrations_project_status
    ON aqueduct.migrations (project, status)
    WHERE status IN ('running', 'recoverable_failure');

CREATE INDEX IF NOT EXISTS aqueduct_migrations_project_started
    ON aqueduct.migrations (project, started_at DESC);

CREATE INDEX IF NOT EXISTS aqueduct_locks_project
    ON aqueduct.locks (project);

CREATE INDEX IF NOT EXISTS aqueduct_migration_steps_migration
    ON aqueduct.migration_steps (migration_id, step_index);

-- Migration history view: human-readable table of migrations with step detail.
CREATE OR REPLACE VIEW aqueduct.migration_history AS
SELECT
    m.id          AS migration_id,
    m.project,
    m.status,
    m.started_at,
    m.finished_at,
    dv.version    AS to_version,
    m.cli_version,
    COUNT(ms.id)  AS step_count,
    SUM(CASE WHEN ms.status = 'failed' THEN 1 ELSE 0 END) AS failed_steps,
    MAX(ms.error_message) AS last_error
FROM aqueduct.migrations m
LEFT JOIN aqueduct.dag_versions dv ON dv.version = m.to_version
LEFT JOIN aqueduct.migration_steps ms ON ms.migration_id = m.id
GROUP BY m.id, m.project, m.status, m.started_at, m.finished_at, dv.version, m.cli_version
ORDER BY m.started_at DESC;

-- Record the catalog schema version.
INSERT INTO aqueduct.cluster_profile (key, value_jsonb, measured_at)
VALUES ('catalog_schema_version', '8'::jsonb, now())
ON CONFLICT (key) DO NOTHING;
"#;

/// Combined init SQL for fresh installations at v8 (current canonical schema).
/// Alias kept for clarity; use this constant in new code.
pub const CATALOG_INIT_V8_SQL: &str = CATALOG_INIT_V5_SQL;

/// Catalog migration from v5 to v6 (CORR-1 / TEST-3 / v0.14):
/// - Adds pgtrickle_mock.scheduler_state table for mock-based scheduler pause/resume
///   assertion tests (TEST-3).
/// - Enforces progress IS NOT NULL for recoverable_failure rows (CORR-1).
pub const CATALOG_MIGRATE_V5_TO_V6_SQL: &str = r#"
-- Scheduler state table for mock pg_trickle integration tests (TEST-3 / v0.14).
-- In production pgtrickle this is a no-op; in the mock schema this table is
-- populated by pause_scheduler and cleared by resume_scheduler so tests can
-- assert the scheduler was correctly paused and resumed.
CREATE SCHEMA IF NOT EXISTS pgtrickle_mock;

CREATE TABLE IF NOT EXISTS pgtrickle_mock.scheduler_state (
    node_name   text PRIMARY KEY,
    paused_at   timestamptz NOT NULL DEFAULT now()
);

-- Bump catalog version to 6.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '6'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Catalog migration from v6 to v7 (CORR-2 / ARCH-1 / v0.15):
/// - Adds `migration_id` and `compensating_sql` columns to `aqueduct.ddl_log` to
///   support the saga-style compensating-step registry (CORR-2).
/// - Adds `rollback_status` column to `aqueduct.blue_green_deployments` to support
///   the TTL rollback path (ARCH-3).
pub const CATALOG_MIGRATE_V6_TO_V7_SQL: &str = r#"
-- Add compensating-step support to ddl_log (CORR-2 / v0.15).
ALTER TABLE aqueduct.ddl_log
    ADD COLUMN IF NOT EXISTS migration_id bigint REFERENCES aqueduct.migrations(id),
    ADD COLUMN IF NOT EXISTS compensating_sql text;

CREATE INDEX IF NOT EXISTS aqueduct_ddl_log_migration
    ON aqueduct.ddl_log (migration_id)
    WHERE migration_id IS NOT NULL;

-- Add rollback_status to blue_green_deployments to track TTL rollbacks (ARCH-3 / v0.15).
ALTER TABLE aqueduct.blue_green_deployments
    ADD COLUMN IF NOT EXISTS rolled_back_at timestamptz;

ALTER TABLE aqueduct.blue_green_deployments DROP CONSTRAINT IF EXISTS bg_status_check;
ALTER TABLE aqueduct.blue_green_deployments
    ADD CONSTRAINT bg_status_check
    CHECK (status IN ('active', 'swapped', 'retired', 'failed', 'rolled_back'));

-- Bump catalog version to 7.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '7'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
"#;

/// Catalog migration from v7 to v8 (DOC-2 / v0.17):
/// - Adds `'interrupted'` to the `status` check constraint in `aqueduct.migrations`
///   to support HA failover detection (DOC-2).
/// - Creates `aqueduct.migration_history` view for human-readable migration audit.
pub const CATALOG_MIGRATE_V7_TO_V8_SQL: &str = r#"
-- Add 'interrupted' status to migrations constraint (DOC-2 / v0.17).
ALTER TABLE aqueduct.migrations DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE aqueduct.migrations
    ADD CONSTRAINT status_check
    CHECK (status IN ('running', 'committed', 'failed', 'rolled_back', 'recoverable_failure', 'interrupted'));

-- Update partial index to include 'interrupted' for resume queries.
DROP INDEX IF EXISTS aqueduct_migrations_project_status;
CREATE INDEX IF NOT EXISTS aqueduct_migrations_project_status
    ON aqueduct.migrations (project, status)
    WHERE status IN ('running', 'recoverable_failure', 'interrupted');

-- Migration history view: human-readable table of migrations with step detail.
CREATE OR REPLACE VIEW aqueduct.migration_history AS
SELECT
    m.id          AS migration_id,
    m.project,
    m.status,
    m.started_at,
    m.finished_at,
    dv.version    AS to_version,
    m.cli_version,
    COUNT(ms.id)  AS step_count,
    SUM(CASE WHEN ms.status = 'failed' THEN 1 ELSE 0 END) AS failed_steps,
    MAX(ms.error_message) AS last_error
FROM aqueduct.migrations m
LEFT JOIN aqueduct.dag_versions dv ON dv.version = m.to_version
LEFT JOIN aqueduct.migration_steps ms ON ms.migration_id = m.id
GROUP BY m.id, m.project, m.status, m.started_at, m.finished_at, dv.version, m.cli_version
ORDER BY m.started_at DESC;

-- Bump catalog version to 8.
UPDATE aqueduct.cluster_profile
SET value_jsonb = '8'::jsonb, measured_at = now()
WHERE key = 'catalog_schema_version';
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

/// SQL to finish a migration record (committed or rolled_back — clears progress).
pub const FINISH_MIGRATION_SQL: &str = r#"
UPDATE aqueduct.migrations
SET finished_at = now(), status = $2, to_version = $3, progress = $4
WHERE id = $1
"#;

/// SQL to mark a migration as recoverable_failure WITHOUT resetting progress (CORR-1 / v0.14).
/// Progress must only be cleared on a committed or rolled_back outcome so that --resume
/// can re-use it to skip already-completed steps.
pub const FINISH_MIGRATION_RECOVERABLE_SQL: &str = r#"
UPDATE aqueduct.migrations
SET finished_at = now(), status = 'recoverable_failure'
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
        // Apply the v3→v4 migration if needed (P-02 / v0.12).
        if current_version < 4 {
            client
                .batch_execute(CATALOG_MIGRATE_V3_TO_V4_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
        // Apply the v4→v5 migration if needed (M-08 / v0.13).
        if current_version < 5 {
            client
                .batch_execute(CATALOG_MIGRATE_V4_TO_V5_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
        // Apply the v5→v6 migration if needed (CORR-1 / TEST-3 / v0.14).
        if current_version < 6 {
            client
                .batch_execute(CATALOG_MIGRATE_V5_TO_V6_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
        // Apply the v6→v7 migration if needed (CORR-2 / ARCH-1 / v0.15).
        if current_version < 7 {
            client
                .batch_execute(CATALOG_MIGRATE_V6_TO_V7_SQL)
                .await
                .map_err(|e| crate::error::AqueductError::Catalog(e.to_string()))?;
        }
        // Apply the v7→v8 migration if needed (DOC-2 / v0.17).
        if current_version < 8 {
            client
                .batch_execute(CATALOG_MIGRATE_V7_TO_V8_SQL)
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

/// SQL to roll back a blue/green deployment within the TTL (ARCH-3 / v0.15).
pub const ROLLBACK_BLUE_GREEN_SQL: &str = r#"
UPDATE aqueduct.blue_green_deployments
SET status = 'rolled_back', rolled_back_at = now()
WHERE id = $1
"#;

/// SQL to find an active or swapped blue/green deployment for a project (ARCH-3 / v0.15).
pub const GET_ACTIVE_BLUE_GREEN_SQL: &str = r#"
SELECT id, green_schema, blue_schema, status, retire_at
FROM aqueduct.blue_green_deployments
WHERE project = $1 AND status IN ('active', 'swapped')
ORDER BY started_at DESC
LIMIT 1
"#;

/// SQL to write a compensating-step entry to ddl_log (CORR-2 / v0.15).
///
/// Parameters: $1=migration_id, $2=object_type, $3=schema_name, $4=object_name,
///             $5=command_tag, $6=compensating_sql.
pub const INSERT_COMPENSATING_STEP_SQL: &str = r#"
INSERT INTO aqueduct.ddl_log
    (migration_id, object_type, schema_name, object_name, command_tag, compensating_sql, pg_role)
VALUES ($1, $2, $3, $4, $5, $6, current_role)
"#;

/// SQL to retrieve the compensating SQL for a step from ddl_log (CORR-2 / v0.15).
///
/// Used during `--resume` to determine if a DDL step needs to be compensated.
pub const GET_COMPENSATING_STEP_SQL: &str = r#"
SELECT compensating_sql, command_tag
FROM aqueduct.ddl_log
WHERE migration_id = $1 AND object_name = $2
ORDER BY id DESC
LIMIT 1
"#;

/// SQL to start a migration step row (M-08 / v0.13).
pub const START_MIGRATION_STEP_SQL: &str = r#"
INSERT INTO aqueduct.migration_steps
    (migration_id, step_index, step_type, step_hash, status, started_at)
VALUES ($1, $2, $3, $4, 'running', now())
ON CONFLICT (migration_id, step_index) DO UPDATE
    SET status = 'running', started_at = now(), error_message = NULL
"#;

/// SQL to finish a migration step row successfully (M-08 / v0.13).
pub const FINISH_MIGRATION_STEP_SQL: &str = r#"
UPDATE aqueduct.migration_steps
SET status = 'done', finished_at = now()
WHERE migration_id = $1 AND step_index = $2
"#;

/// SQL to record a failed migration step (M-08 / v0.13).
pub const FAIL_MIGRATION_STEP_SQL: &str = r#"
UPDATE aqueduct.migration_steps
SET status = 'failed', finished_at = now(), error_message = $3
WHERE migration_id = $1 AND step_index = $2
"#;

/// SQL to list all steps for a migration (for `aqueduct status --verbose`).
pub const LIST_MIGRATION_STEPS_SQL: &str = r#"
SELECT step_index, step_type, status, started_at, finished_at, error_message
FROM aqueduct.migration_steps
WHERE migration_id = $1
ORDER BY step_index
"#;

/// SQL to mark a migration as 'interrupted' due to HA failover (DOC-2 / v0.17).
pub const MARK_MIGRATION_INTERRUPTED_SQL: &str = r#"
UPDATE aqueduct.migrations
SET finished_at = now(), status = 'interrupted'
WHERE id = $1
"#;

/// SQL for `aqueduct audit`: list recent migrations for a project (v0.17).
///
/// Parameters: $1 = project, $2 = limit.
pub const LIST_MIGRATIONS_FOR_AUDIT_SQL: &str = r#"
SELECT
    m.id          AS migration_id,
    m.project,
    m.status,
    m.started_at,
    m.finished_at,
    m.to_version,
    m.cli_version,
    COUNT(ms.id)              AS step_count,
    SUM(CASE WHEN ms.status = 'failed' THEN 1 ELSE 0 END) AS failed_steps,
    MAX(ms.error_message)     AS last_error
FROM aqueduct.migrations m
LEFT JOIN aqueduct.migration_steps ms ON ms.migration_id = m.id
WHERE m.project = $1
GROUP BY m.id, m.project, m.status, m.started_at, m.finished_at, m.to_version, m.cli_version
ORDER BY m.started_at DESC
LIMIT $2
"#;
