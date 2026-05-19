pub mod mock_pgtrickle;

pub use mock_pgtrickle::MOCK_PGTRICKLE_SQL;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

/// Default pinned PostgreSQL image version used by Testcontainers.
/// Override with `AQUEDUCT_TEST_PG_IMAGE` environment variable to test
/// against a different PostgreSQL version (e.g., `postgres:14-alpine`).
const DEFAULT_PG_IMAGE: &str = "postgres:18-alpine";

/// A running PostgreSQL test container with a connected client.
pub struct TestDb {
    /// Keep the container alive for the lifetime of this struct.
    _container: ContainerAsync<Postgres>,
    pub client: tokio_postgres::Client,
    pub connection_string: String,
}

impl TestDb {
    /// Start a fresh PostgreSQL container.
    ///
    /// The PostgreSQL image defaults to `postgres:16-alpine` but can be
    /// overridden by setting `AQUEDUCT_TEST_PG_IMAGE` (e.g., `postgres:17-alpine`).
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
            // Use the canonical v4 catalog SQL from aqueduct-core to stay in sync.
            .batch_execute(aqueduct_core::catalog::CATALOG_INIT_V4_SQL)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to install aqueduct catalog: {}", e))?;
        Ok(())
    }

    /// Get the PostgreSQL server version.
    pub async fn pg_version(&self) -> anyhow::Result<String> {
        let row = self.client.query_one("SELECT version()", &[]).await?;
        Ok(row.get(0))
    }
}
