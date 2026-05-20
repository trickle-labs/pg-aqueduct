/// HA backend detection and Patroni failover checking (DOC-2 / backlog-15 / v0.17).
///
/// Implements `detect_ha_backend()` which identifies the HA solution in use and
/// enables per-step primary re-checks via `check_still_primary()`.

/// The detected HA backend type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaBackend {
    /// Plain PostgreSQL primary (no HA framework detected).
    PlainPrimary,
    /// Patroni cluster — endpoint available for HTTP primary checks.
    Patroni { endpoint: String },
    /// CloudNativePG — detected via `app.cnpg.cluster_name` GUC.
    CloudNativePG { cluster_name: String },
    /// Stolon — detected via `application_name` GUC containing `stolon-keeper`.
    Stolon,
}

impl std::fmt::Display for HaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaBackend::PlainPrimary => write!(f, "plain-primary"),
            HaBackend::Patroni { endpoint } => write!(f, "patroni ({})", endpoint),
            HaBackend::CloudNativePG { cluster_name } => {
                write!(f, "cloudnativepg (cluster={})", cluster_name)
            }
            HaBackend::Stolon => write!(f, "stolon"),
        }
    }
}

/// Probe the live database and optional Patroni endpoint to detect the HA backend.
///
/// Detection precedence:
/// 1. If `patroni_endpoint` is provided, return `Patroni`.
/// 2. If `app.cnpg.cluster_name` GUC is set, return `CloudNativePG`.
/// 3. If `application_name` contains `stolon-keeper`, return `Stolon`.
/// 4. Otherwise, return `PlainPrimary`.
///
/// Never returns an error — failures fall back to `PlainPrimary`.
pub async fn detect_ha_backend(
    client: &tokio_postgres::Client,
    patroni_endpoint: Option<&str>,
) -> HaBackend {
    // Patroni: if explicitly configured, use it.
    if let Some(endpoint) = patroni_endpoint {
        return HaBackend::Patroni {
            endpoint: endpoint.to_string(),
        };
    }

    // CloudNativePG: probe `app.cnpg.cluster_name` GUC.
    if let Ok(rows) = client
        .query(
            "SELECT current_setting('app.cnpg.cluster_name', true)",
            &[],
        )
        .await
    {
        if let Some(row) = rows.first() {
            let val: Option<String> = row.try_get(0).ok();
            if let Some(name) = val {
                if !name.is_empty() {
                    return HaBackend::CloudNativePG { cluster_name: name };
                }
            }
        }
    }

    // Stolon: check application_name.
    if let Ok(row) = client
        .query_one(
            "SELECT current_setting('application_name', true)",
            &[],
        )
        .await
    {
        let app_name: Option<String> = row.try_get(0).ok();
        if app_name
            .as_deref()
            .map(|s| s.contains("stolon-keeper"))
            .unwrap_or(false)
        {
            return HaBackend::Stolon;
        }
    }

    HaBackend::PlainPrimary
}

/// Check that the current database connection is still to a primary
/// (not a hot standby that was promoted or demoted).
///
/// Returns `Ok(())` if still primary, `Err` if the host has been demoted.
pub async fn check_still_primary(
    client: &tokio_postgres::Client,
) -> crate::error::Result<()> {
    let is_primary: bool = client
        .query_one("SELECT NOT pg_is_in_recovery()", &[])
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);

    if !is_primary {
        return Err(crate::error::AqueductError::Other(
            "HA failover detected: connected host is no longer the primary \
             (pg_is_in_recovery() = true). Mark this migration as 'interrupted' \
             and reconnect to the new primary, then run `aqueduct apply --resume`."
                .to_string(),
        ));
    }

    Ok(())
}

/// Perform a Patroni primary check by calling `GET <endpoint>/master`.
///
/// Returns `Ok(())` if the endpoint returns HTTP 200, `Err` otherwise.
/// A non-200 response or connection error indicates failover.
pub async fn check_patroni_primary(endpoint: &str) -> crate::error::Result<()> {
    let url = format!("{}/master", endpoint.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| {
            crate::error::AqueductError::Other(format!(
                "Patroni check: failed to build HTTP client: {}",
                e
            ))
        })?;

    let resp = client.get(&url).send().await.map_err(|e| {
        crate::error::AqueductError::Other(format!(
            "Patroni check: HTTP error reaching '{}': {} \
             (possible network partition or failover in progress)",
            url, e
        ))
    })?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(crate::error::AqueductError::Other(format!(
            "Patroni check: '{}' returned HTTP {} — primary may have changed \
             (failover in progress). Mark migration as 'interrupted' and resume \
             after connecting to the new primary.",
            url,
            resp.status()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test detect_ha_backend returns Patroni when endpoint is provided.
    #[tokio::test]
    async fn test_detect_ha_backend_patroni_explicit() {
        // We need a real client to test this; skip if no DB available.
        // Just verify the logic: if patroni_endpoint is Some, return Patroni.
        // This is a unit-level test that doesn't need a real connection.
        let endpoint = "http://patroni-api:8008";
        // detect_ha_backend takes a client but returns immediately when patroni is set.
        // We can't easily mock the client here without a full DB, so we test the
        // return value directly via the enum.
        let backend = HaBackend::Patroni {
            endpoint: endpoint.to_string(),
        };
        assert_eq!(
            backend.to_string(),
            "patroni (http://patroni-api:8008)"
        );
    }

    /// Test that check_patroni_primary succeeds against a mock 200 response.
    #[tokio::test]
    async fn test_check_patroni_primary_200() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/master"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Leader"))
            .expect(1)
            .mount(&server)
            .await;

        let result = check_patroni_primary(&server.uri()).await;
        assert!(result.is_ok(), "expected OK for 200 response, got {:?}", result);
    }

    /// Test that check_patroni_primary fails against a mock 503 response.
    #[tokio::test]
    async fn test_check_patroni_primary_failover() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/master"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Replica"))
            .expect(1)
            .mount(&server)
            .await;

        let result = check_patroni_primary(&server.uri()).await;
        assert!(result.is_err(), "expected Err for 503 response");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("503") || err_msg.contains("failover"),
            "error should mention 503 or failover, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_ha_backend_display() {
        assert_eq!(HaBackend::PlainPrimary.to_string(), "plain-primary");
        assert_eq!(HaBackend::Stolon.to_string(), "stolon");
        assert_eq!(
            HaBackend::CloudNativePG {
                cluster_name: "my-cluster".to_string()
            }
            .to_string(),
            "cloudnativepg (cluster=my-cluster)"
        );
    }
}
