pub mod mock_pgtrickle;

pub use mock_pgtrickle::MOCK_PGTRICKLE_SQL;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

/// Default pinned PostgreSQL image version used by Testcontainers.
/// pg_trickle requires PostgreSQL 18+; override with `AQUEDUCT_TEST_PG_IMAGE`
/// only to test against a newer release (e.g., `postgres:19-alpine`).
const DEFAULT_PG_IMAGE: &str = "postgres:18-alpine";

/// A running PostgreSQL test container with a connected client.
pub struct TestDb {
    /// Keep the container alive for the lifetime of this struct.
    _container: ContainerAsync<Postgres>,
    pub client: tokio_postgres::Client,
    /// Internal connection string.  Not part of the public API; use
    /// [`TestDb::client`] for database access.
    #[allow(dead_code)]
    pub(crate) connection_string: String,
}

impl TestDb {
    /// Start a fresh PostgreSQL container.
    ///
    /// The PostgreSQL image defaults to `postgres:18-alpine` (the minimum
    /// supported by pg_trickle) but can be overridden by setting
    /// `AQUEDUCT_TEST_PG_IMAGE` (e.g., `postgres:19-alpine`).
    pub async fn new() -> anyhow::Result<Self> {
        let image_tag = std::env::var("AQUEDUCT_TEST_PG_IMAGE")
            .unwrap_or_else(|_| DEFAULT_PG_IMAGE.to_string());

        // Parse "postgres:VERSION" or "postgres:VERSION-alpine" into the tag.
        let pg_image = if let Some(tag) = image_tag.strip_prefix("postgres:") {
            Postgres::default().with_tag(tag)
        } else {
            Postgres::default().with_tag("18-alpine")
        };

        let container = pg_image.start().await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to start Postgres container (image={}): {}",
                image_tag,
                e
            )
        })?;

        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432).await?;

        let connection_string = format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            host, port
        );

        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

        // Spawn the connection driver.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("TestDb connection error: {}", e);
            }
        });

        Ok(Self {
            _container: container,
            client,
            connection_string,
        })
    }

    /// Install the mock pg_trickle schema.
    pub async fn install_mock_pgtrickle(&self) -> anyhow::Result<()> {
        self.client
            .batch_execute(MOCK_PGTRICKLE_SQL)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to install mock pg_trickle: {}", e))?;
        Ok(())
    }

    /// Install the aqueduct catalog schema.
    pub async fn install_aqueduct_catalog(&self) -> anyhow::Result<()> {
        self.client
            // Use the canonical v8 catalog SQL from aqueduct-core to stay in sync.
            .batch_execute(aqueduct_core::catalog::CATALOG_INIT_V8_SQL)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to install aqueduct catalog: {}", e))?;
        Ok(())
    }

    /// Get the PostgreSQL server version.
    pub async fn pg_version(&self) -> anyhow::Result<String> {
        let row = self.client.query_one("SELECT version()", &[]).await?;
        Ok(row.get(0))
    }

    /// Return the connection string for this test database.
    ///
    /// L14 (v0.16): The raw field is `pub(crate)`; external callers use this accessor.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

/// TEST-3 (v0.19): Assert the scheduler has no paused nodes.
///
/// Queries `pgtrickle.paused_nodes` (the canonical mock scheduler state) and
/// panics with a descriptive message if any nodes are paused.
pub async fn assert_scheduler_idle(client: &tokio_postgres::Client) {
    let count: i64 = client
        .query_one("SELECT count(*) FROM pgtrickle.paused_nodes", &[])
        .await
        .expect("query pgtrickle.paused_nodes")
        .get(0);
    assert_eq!(
        count, 0,
        "TEST-3/v0.19: expected scheduler to be idle (no paused nodes), but found {} paused node(s)",
        count
    );
}

/// TEST-3 (v0.19): Assert that exactly the given node names are in the paused set.
///
/// Queries `pgtrickle.paused_nodes` and panics if the set differs from
/// `expected_nodes`. Order does not matter.
pub async fn assert_scheduler_paused_for(client: &tokio_postgres::Client, expected_nodes: &[&str]) {
    let rows = client
        .query(
            "SELECT node_name FROM pgtrickle.paused_nodes ORDER BY node_name",
            &[],
        )
        .await
        .expect("query pgtrickle.paused_nodes");
    let actual: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let mut expected_sorted: Vec<&str> = expected_nodes.to_vec();
    expected_sorted.sort_unstable();
    let expected_strings: Vec<String> = expected_sorted.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        actual, expected_strings,
        "TEST-3/v0.19: expected paused nodes {:?} but found {:?}",
        expected_strings, actual
    );
}
