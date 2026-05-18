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

/// HA backend detected for the connected PostgreSQL cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaBackend {
    /// Standard PostgreSQL primary (not in recovery).
    Primary,
    /// Patroni-managed cluster.
    Patroni { endpoint: Option<String> },
    /// CloudNativePG-managed cluster (detected via `cnpg.io/cluster` annotation).
    CloudNativePg { cluster_name: Option<String> },
    /// Stolon-managed cluster.
    Stolon,
    /// Unknown / not applicable.
    Unknown,
}

/// Detect the HA backend by interrogating `pg_stat_activity` application names
/// and cluster settings.
///
/// This is a lightweight heuristic; it does not make external HTTP calls.
/// For authoritative Patroni primary discovery, pass `--patroni-endpoint` on
/// the CLI and use `verify_patroni_primary`.
pub async fn detect_ha_backend(client: &tokio_postgres::Client) -> Result<HaBackend> {
    // Check for CloudNativePG: it sets `application_name` to `streaming_replica`
    // from the operator, and the cluster name is available via a GUC.
    let cnpg_cluster: Option<String> = client
        .query_opt(
            "SELECT setting FROM pg_settings WHERE name = 'app.cnpg.cluster_name'",
            &[],
        )
        .await?
        .map(|r| r.get(0));

    if cnpg_cluster.is_some() {
        return Ok(HaBackend::CloudNativePg {
            cluster_name: cnpg_cluster,
        });
    }

    // Check for Patroni: it writes `application_name = 'patroni'` in `pg_stat_activity`.
    let patroni_present: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE application_name ILIKE '%patroni%'
                LIMIT 1
            )",
            &[],
        )
        .await?
        .get(0);

    if patroni_present {
        return Ok(HaBackend::Patroni { endpoint: None });
    }

    // Check for Stolon: `stolonctl` sets `application_name = 'stolon'`.
    let stolon_present: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE application_name ILIKE '%stolon%'
                LIMIT 1
            )",
            &[],
        )
        .await?
        .get(0);

    if stolon_present {
        return Ok(HaBackend::Stolon);
    }

    // Default to plain primary / unknown.
    let primary: bool = client
        .query_one("SELECT NOT pg_is_in_recovery()", &[])
        .await?
        .get(0);

    if primary {
        Ok(HaBackend::Primary)
    } else {
        Ok(HaBackend::Unknown)
    }
}

/// Verify primary status against a Patroni REST endpoint.
///
/// Patroni exposes a health endpoint at `GET /master` (or `/primary` in newer
/// versions) that returns HTTP 200 only on the current leader.
///
/// Returns `Ok(true)` if the Patroni endpoint confirms this node is the primary,
/// `Ok(false)` if it is a standby, and `Err` if the endpoint is unreachable.
///
/// In v0.6 this function uses a minimal HTTP check via `std::net::TcpStream`
/// to avoid adding an HTTP client dependency.  A future version can adopt
/// `reqwest` for full REST API integration.
pub fn verify_patroni_primary(endpoint: &str) -> Result<bool> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    // Parse host:port from the endpoint URL.
    let addr = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");

    let addr_with_port = if addr.contains(':') {
        addr.to_string()
    } else {
        format!("{}:8008", addr) // Patroni default port
    };

    let host_path: Vec<&str> = addr_with_port.splitn(2, '/').collect();
    let host_port = host_path[0];
    let path = if host_path.len() > 1 {
        format!("/{}", host_path[1])
    } else {
        "/master".to_string()
    };

    let mut stream = TcpStream::connect(host_port).map_err(|e| {
        crate::error::AqueductError::Config(format!(
            "Cannot connect to Patroni endpoint '{}': {}",
            endpoint, e
        ))
    })?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let request = format!(
        "GET {} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host_port
    );
    stream.write_all(request.as_bytes()).map_err(|e| {
        crate::error::AqueductError::Config(format!("Patroni HTTP write error: {}", e))
    })?;

    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(|e| {
        crate::error::AqueductError::Config(format!("Patroni HTTP read error: {}", e))
    })?;

    // HTTP 200 means primary, 503 means standby/replica.
    let is_primary = response
        .lines()
        .next()
        .map(|l| l.contains("200"))
        .unwrap_or(false);

    Ok(is_primary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ha_backend_eq() {
        assert_eq!(HaBackend::Primary, HaBackend::Primary);
        assert_ne!(HaBackend::Primary, HaBackend::Stolon);
    }

    #[test]
    fn test_verify_patroni_primary_unreachable() {
        // Should return an error for an unreachable endpoint.
        let result = verify_patroni_primary("127.0.0.1:19999");
        assert!(result.is_err(), "Unreachable endpoint should return error");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Cannot connect") || msg.contains("Patroni"),
            "Error should mention connection issue: {}",
            msg
        );
    }
}
