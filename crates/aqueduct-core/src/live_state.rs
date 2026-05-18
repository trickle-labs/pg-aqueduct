use crate::dag::{DagState, QualifiedName, RefreshMode, StreamTableSpec};
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
        });
    }

    Ok(DagState {
        stream_tables,
        sources: vec![],
    })
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

#[cfg(test)]
mod tests {
    // Integration tests are in tests/integration.rs (require a database).
}
