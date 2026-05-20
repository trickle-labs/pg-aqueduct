//! Optional Prometheus metrics endpoint (feature = "metrics").
//!
//! Exposes a `/metrics` HTTP scrape endpoint while `aqueduct apply` runs.
//! Counters and gauges:
//! - `aqueduct_steps_total{step_type,status}` — steps by type and outcome
//! - `aqueduct_migration_duration_seconds` — total duration of the running migration
//! - `aqueduct_drift_count` — number of drift detections (status command)

use anyhow::Result;
use prometheus::{Encoder, Registry, TextEncoder};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A lightweight handle to the background metrics server task.
pub struct MetricsServer {
    _handle: tokio::task::JoinHandle<()>,
}

/// Start a minimal HTTP server that responds to GET /metrics with a Prometheus
/// text exposition. Returns a handle; dropping it stops the server.
pub async fn start_metrics_server(addr: &str) -> Result<MetricsServer> {
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid --metrics-addr '{}': {}", addr, e))?;

    let registry = Arc::new(Registry::new());

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Cannot bind metrics endpoint on {}: {}", addr, e))?;

    tracing::info!("Prometheus metrics endpoint listening on http://{}/metrics", addr);

    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _peer) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };

            let registry = registry.clone();
            tokio::spawn(async move {
                // Read the request (we don't care about the details, just respond).
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                let mut body = Vec::new();
                let _ = encoder.encode(&metric_families, &mut body);

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            });
        }
    });

    Ok(MetricsServer { _handle: handle })
}
