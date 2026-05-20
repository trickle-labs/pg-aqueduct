//! Optional Prometheus metrics endpoint (feature = "metrics").
//!
//! Exposes a `/metrics` HTTP scrape endpoint while `aqueduct apply` runs.
//! Uses a hand-written Prometheus text format exposition (no external crate) to
//! avoid transitive dependencies with known security advisories.
//!
//! Metrics exposed:
//! - `aqueduct_steps_total{step_type,status}` — steps by type and outcome
//! - `aqueduct_migration_duration_seconds` — total duration gauge (placeholder)
//! - `aqueduct_drift_count` — number of drift detections (placeholder)

use anyhow::Result;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Shared metrics state.
#[derive(Default)]
pub struct Metrics {
    pub steps_ok: u64,
    pub steps_failed: u64,
    pub migration_duration_seconds: f64,
    pub drift_count: u64,
}

impl Metrics {
    fn to_prometheus_text(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP aqueduct_steps_total Total migration steps by status\n");
        out.push_str("# TYPE aqueduct_steps_total counter\n");
        out.push_str(&format!(
            "aqueduct_steps_total{{status=\"ok\"}} {}\n",
            self.steps_ok
        ));
        out.push_str(&format!(
            "aqueduct_steps_total{{status=\"failed\"}} {}\n",
            self.steps_failed
        ));
        out.push_str("# HELP aqueduct_migration_duration_seconds Duration of the current migration in seconds\n");
        out.push_str("# TYPE aqueduct_migration_duration_seconds gauge\n");
        out.push_str(&format!(
            "aqueduct_migration_duration_seconds {:.3}\n",
            self.migration_duration_seconds
        ));
        out.push_str("# HELP aqueduct_drift_count Number of drift detections\n");
        out.push_str("# TYPE aqueduct_drift_count gauge\n");
        out.push_str(&format!("aqueduct_drift_count {}\n", self.drift_count));
        out
    }
}

/// A lightweight handle to the background metrics server task.
pub struct MetricsServer {
    _handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    pub metrics: Arc<Mutex<Metrics>>,
}

/// Start a minimal HTTP server that responds to GET /metrics with a Prometheus
/// text exposition. Returns a handle; dropping the `_handle` does not stop the
/// server, but the server exits when the TcpListener is dropped with the process.
pub async fn start_metrics_server(addr: &str) -> Result<MetricsServer> {
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid --metrics-addr '{}': {}", addr, e))?;

    let metrics = Arc::new(Mutex::new(Metrics::default()));
    let metrics_clone = metrics.clone();

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Cannot bind metrics endpoint on {}: {}", addr, e))?;

    tracing::info!(
        "Prometheus metrics endpoint listening on http://{}/metrics",
        addr
    );

    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _peer) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };

            let metrics = metrics_clone.clone();
            tokio::spawn(async move {
                // Read the request (we only care about responding, not routing).
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                let body = {
                    let m = metrics.lock().unwrap_or_else(|e| e.into_inner());
                    m.to_prometheus_text()
                };

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(body.as_bytes()).await;
            });
        }
    });

    Ok(MetricsServer {
        _handle: handle,
        metrics,
    })
}
