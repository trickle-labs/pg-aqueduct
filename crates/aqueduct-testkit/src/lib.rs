pub mod mock_pgtrickle;

pub use mock_pgtrickle::MOCK_PGTRICKLE_SQL;

use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

/// A running PostgreSQL test container with a connected client.
pub struct TestDb {
    /// Keep the container alive for the lifetime of this struct.
    _container: ContainerAsync<Postgres>,
    pub client: tokio_postgres::Client,
    pub connection_string: String,
}

impl TestDb {
    /// Start a fresh PostgreSQL container.
    pub async fn new() -> anyhow::Result<Self> {
        let container = Postgres::default()
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Postgres container: {}", e))?;

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
            .batch_execute(aqueduct_catalog_sql())
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

fn aqueduct_catalog_sql() -> &'static str {
    // This is inlined from aqueduct-core::catalog::CATALOG_INIT_SQL but we
    // duplicate it here to avoid a circular dependency.
    r#"
CREATE SCHEMA IF NOT EXISTS aqueduct;

CREATE TABLE IF NOT EXISTS aqueduct.dag_versions (
    version    bigserial PRIMARY KEY,
    project    text      NOT NULL,
    spec_hash  bytea     NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    applied_by text      NOT NULL,
    plan_jsonb jsonb     NOT NULL,
    spec_jsonb jsonb     NOT NULL
);

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

CREATE TABLE IF NOT EXISTS aqueduct.locks (
    project      text PRIMARY KEY,
    holder       text      NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    ttl          interval  NOT NULL DEFAULT '30s',
    paused_nodes jsonb     NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS aqueduct.cluster_profile (
    key         text PRIMARY KEY,
    value_jsonb jsonb NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO aqueduct.cluster_profile (key, value_jsonb, measured_at)
VALUES ('catalog_schema_version', '1'::jsonb, now())
ON CONFLICT (key) DO NOTHING;
"#
}
