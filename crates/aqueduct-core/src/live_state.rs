use crate::dag::{ConsumerSpec, DagState, QualifiedName, RefreshMode, StreamTableSpec};
use crate::error::Result;

/// Read the live stream-table state from the pg_trickle catalog.
pub async fn read_live_state(client: &tokio_postgres::Client) -> Result<DagState> {
    // Check if pg_trickle is installed by looking for the pgtrickle schema.
    let pgtrickle_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
            &[],
        )
        .await?
        .get(0);

    if !pgtrickle_exists {
        return Ok(DagState::default());
    }

    let rows = client
        .query(
            r#"
            SELECT
                schema_name,
                table_name,
                query,
                refresh_mode,
                schedule,
                cdc_mode
            FROM pgtrickle.pgt_stream_tables
            ORDER BY schema_name, table_name
            "#,
            &[],
        )
        .await?;

    let mut stream_tables = Vec::new();
    for row in rows {
        let schema_name: String = row.get(0);
        let table_name: String = row.get(1);
        let query: String = row.get(2);
        let refresh_mode_str: String = row.get(3);
        let schedule: String = row.get(4);
        let cdc_mode: Option<String> = row.get(5);

        let refresh_mode: RefreshMode = refresh_mode_str.parse().unwrap_or_default();

        stream_tables.push(StreamTableSpec {
            qualified_name: QualifiedName::new(schema_name, table_name),
            query,
            refresh_mode,
            schedule,
            cdc_mode,
            explicit_depends_on: vec![],
            depends_on: vec![],
            cypher_source: None,
        });
    }

    // Read live consumer views from the aqueduct catalog if it exists.
    let consumers = read_live_consumers(client).await.unwrap_or_default();

    Ok(DagState {
        stream_tables,
        sources: vec![],
        consumers,
    })
}

/// Read live consumer views from the aqueduct catalog.
async fn read_live_consumers(client: &tokio_postgres::Client) -> Result<Vec<ConsumerSpec>> {
    // Check if the consumer_views table exists.
    let table_exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'aqueduct' AND table_name = 'consumer_views'
            )",
            &[],
        )
        .await?
        .get(0);

    if !table_exists {
        return Ok(vec![]);
    }

    let rows = client
        .query(
            "SELECT name, expose_as, source, sql_body FROM aqueduct.consumer_views ORDER BY name",
            &[],
        )
        .await?;

    let consumers = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let expose_as_str: String = row.get(1);
            let source_str: String = row.get(2);
            let sql_body: Option<String> = row.get(3);

            ConsumerSpec {
                name,
                expose_as: QualifiedName::from_str_parts(&expose_as_str),
                source: QualifiedName::from_str_parts(&source_str),
                sql_body,
            }
        })
        .collect();

    Ok(consumers)
}

/// Check the pg_trickle version installed on the target database.
/// Returns the version string, or None if pg_trickle is not installed.
pub async fn check_pgtrickle_version(client: &tokio_postgres::Client) -> Result<Option<String>> {
    let row = client
        .query_opt("SELECT pgtrickle.pgt_extension_version()", &[])
        .await?;

    Ok(row.map(|r| r.get::<_, String>(0)))
}

/// Read the current DAG version for a project from the aqueduct catalog.
pub async fn get_latest_dag_version(
    client: &tokio_postgres::Client,
    project: &str,
) -> Result<Option<u64>> {
    // Check if catalog exists first.
    let catalog_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'aqueduct')",
            &[],
        )
        .await?
        .get(0);

    if !catalog_exists {
        return Ok(None);
    }

    let row = client
        .query_opt(
            "SELECT version FROM aqueduct.dag_versions WHERE project = $1 ORDER BY version DESC LIMIT 1",
            &[&project],
        )
        .await?;

    Ok(row.map(|r| r.get::<_, i64>(0) as u64))
}

/// Check whether the connected database is a primary (not a hot standby).
pub async fn is_primary(client: &tokio_postgres::Client) -> Result<bool> {
    let row = client
        .query_one("SELECT NOT pg_is_in_recovery()", &[])
        .await?;
    Ok(row.get(0))
}

/// Get the count of stream tables in the pg_trickle catalog.
pub async fn get_stream_table_count(client: &tokio_postgres::Client) -> Result<i64> {
    let pgtrickle_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
            &[],
        )
        .await?
        .get(0);

    if !pgtrickle_exists {
        return Ok(0);
    }

    let row = client
        .query_one("SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables", &[])
        .await?;
    Ok(row.get(0))
}

/// Detect whether the pg_aqueduct companion extension is installed and active.
///
/// The companion extension registers a `ddl_command_end` event trigger named
/// `aqueduct_ddl_logger`. When this trigger is present and enabled, the extension
/// is active. When absent, the CLI operates in polling-based fallback mode.
///
/// Returns:
/// - `true` if the extension is installed and the DDL event trigger is active
/// - `false` if operating in fallback mode (polling-based drift detection)
pub async fn detect_extension_installed(client: &tokio_postgres::Client) -> Result<bool> {
    // First check if the aqueduct schema exists (extension always creates it).
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'aqueduct')",
            &[],
        )
        .await?
        .get(0);

    if !schema_exists {
        return Ok(false);
    }

    // Check for the DDL event trigger that the extension registers.
    let trigger_exists: bool = client
        .query_one(crate::catalog::DETECT_EXTENSION_SQL, &[])
        .await?
        .get(0);

    Ok(trigger_exists)
}

/// A DDL event recorded by the companion extension.
#[derive(Debug, Clone)]
pub struct DdlEvent {
    pub id: i64,
    pub object_type: String,
    pub schema_name: String,
    pub object_name: String,
    pub command_tag: String,
    pub command_text: Option<String>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    pub pg_role: String,
}

/// Read recent DDL events from the log table.
///
/// Returns an empty list when the `aqueduct.ddl_log` table does not exist or
/// when the companion extension is not installed (CLI fallback mode).
pub async fn read_ddl_log(client: &tokio_postgres::Client, limit: i64) -> Result<Vec<DdlEvent>> {
    // Check if the table exists first.
    let table_exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'aqueduct' AND table_name = 'ddl_log'
            )",
            &[],
        )
        .await?
        .get(0);

    if !table_exists {
        return Ok(vec![]);
    }

    let rows = client
        .query(crate::catalog::READ_DDL_LOG_SQL, &[&limit])
        .await?;

    let events = rows
        .iter()
        .map(|row| DdlEvent {
            id: row.get(0),
            object_type: row.get(1),
            schema_name: row.get(2),
            object_name: row.get(3),
            command_tag: row.get(4),
            command_text: row.get(5),
            recorded_at: row.get(6),
            pg_role: row.get(7),
        })
        .collect();

    Ok(events)
}

#[cfg(test)]
mod tests {
    // Integration tests are in tests/integration.rs (require a database).
}
