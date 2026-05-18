/// SQL statements for bootstrapping the aqueduct catalog schema.
/// These are embedded directly in the binary via include_str!() in a real deployment;
/// for v0.1 we keep them as a constant in this module.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

/// The SQL to create the aqueduct catalog schema.
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
