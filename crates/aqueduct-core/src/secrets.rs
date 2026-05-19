//! Encrypted secret handling for aqueduct DSN resolution.
//!
//! Supports resolving DSN passwords / full DSNs from:
//! - Environment variables (default, always available)
//! - AWS Secrets Manager (via AWS SigV4 + reqwest HTTP client; LocalStack-compatible)
//! - GCP Secret Manager (via OAuth2 Bearer token + reqwest HTTP client)
//! - HashiCorp Vault KV v2 (via VAULT_TOKEN + reqwest HTTP client)
//! - SOPS-encrypted files (via `sops -d`)
//! - age-encrypted files (via `age -d`)
//!
//! All backends ultimately produce a plain-text secret value that is used
//! once and never written to disk.

use std::sync::LazyLock;

use crate::error::{AqueductError, Result};

/// Regex for `${secret:BACKEND:KEY}` inline syntax.
static SECRET_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\$\{secret:([^:]+):([^}]+)\}").expect("valid static regex")
});

/// The secret backend to use for resolving secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretBackend {
    /// Use environment variables only (default, no external calls).
    Env,
    /// AWS Secrets Manager.  The key is the secret name/ARN.
    AwsSecretsManager { region: String },
    /// GCP Secret Manager.  The key is `projects/{project}/secrets/{name}/versions/{version}`.
    GcpSecretManager { project_id: String },
    /// HashiCorp Vault KV v2.  The key is the secret path.
    HashicorpVault { vault_addr: String },
    /// SOPS-encrypted file.  The key is the file path.
    Sops,
    /// age-encrypted file.  The key is the file path.
    Age { identity_file: String },
}

impl SecretBackend {
    /// Parse a backend name from a CLI flag value.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "env" | "" => Ok(SecretBackend::Env),
            "aws" | "aws-secrets-manager" => {
                let region = std::env::var("AWS_REGION")
                    .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                    .unwrap_or_else(|_| "us-east-1".to_string());
                Ok(SecretBackend::AwsSecretsManager { region })
            }
            "gcp" | "gcp-secret-manager" => {
                let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")
                    .or_else(|_| std::env::var("GCLOUD_PROJECT"))
                    .unwrap_or_else(|_| "unknown".to_string());
                Ok(SecretBackend::GcpSecretManager { project_id })
            }
            "vault" | "hashicorp-vault" => {
                let vault_addr = std::env::var("VAULT_ADDR")
                    .unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
                Ok(SecretBackend::HashicorpVault { vault_addr })
            }
            "sops" => Ok(SecretBackend::Sops),
            "age" => {
                let identity_file = std::env::var("SOPS_AGE_KEY_FILE")
                    .or_else(|_| std::env::var("AGE_KEY_FILE"))
                    .unwrap_or_else(|_| "~/.age/key.txt".to_string());
                Ok(SecretBackend::Age { identity_file })
            }
            other => Err(AqueductError::Config(format!(
                "Unknown secret backend '{}'. Valid values: env, aws, gcp, vault, sops, age",
                other
            ))),
        }
    }
}

/// Resolve a secret value from the configured backend.
///
/// For `Env`, the key is treated as an environment variable name.
/// For other backends, the key is the backend-specific identifier.
pub async fn resolve_secret(backend: &SecretBackend, key: &str) -> Result<String> {
    match backend {
        SecretBackend::Env => std::env::var(key).map_err(|_| {
            AqueductError::Config(format!(
                "Environment variable '{}' not set. \
                     Ensure the secret is exported before running aqueduct.",
                key
            ))
        }),

        SecretBackend::AwsSecretsManager { region } => fetch_aws_secret(region, key).await,

        SecretBackend::GcpSecretManager { project_id: _ } => fetch_gcp_secret(key).await,

        SecretBackend::HashicorpVault { vault_addr } => fetch_vault_secret(vault_addr, key).await,

        SecretBackend::Sops => {
            // Validate that the key path does not escape via `../`.
            validate_secret_path(key)?;
            // Attempt to run `sops -d <key>` where key is a file path.
            tracing::info!(backend = "sops", file = %key, "Decrypting with SOPS");
            let output = std::process::Command::new("sops")
                .args(["-d", key])
                .output()
                .map_err(|e| {
                    AqueductError::Config(format!(
                        "Failed to run sops: {}. Ensure sops is installed and on PATH.",
                        e
                    ))
                })?;

            if !output.status.success() {
                return Err(AqueductError::Config(format!(
                    "sops decryption failed for '{}': {}",
                    key,
                    String::from_utf8_lossy(&output.stderr)
                )));
            }

            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }

        SecretBackend::Age { identity_file } => {
            // Validate that the key path does not escape via `../`.
            validate_secret_path(key)?;
            tracing::info!(
                backend = "age",
                file = %key,
                identity = %identity_file,
                "Decrypting with age"
            );
            let output = std::process::Command::new("age")
                .args(["-d", "-i", identity_file, key])
                .output()
                .map_err(|e| {
                    AqueductError::Config(format!(
                        "Failed to run age: {}. Ensure age is installed and on PATH.",
                        e
                    ))
                })?;

            if !output.status.success() {
                return Err(AqueductError::Config(format!(
                    "age decryption failed for '{}': {}",
                    key,
                    String::from_utf8_lossy(&output.stderr)
                )));
            }

            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }
    }
}

// ── AWS Secrets Manager HTTP client (SEC-02 / v0.12) ─────────────────────────

/// Compute HMAC-SHA256 using the SHA-256 primitive we already depend on.
/// Implements RFC 2104 HMAC using sha2::Sha256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    // Pad the key to 64 bytes (SHA-256 block size).
    let mut key_padded = [0u8; 64];
    if key.len() > 64 {
        let hashed = Sha256::digest(key);
        key_padded[..32].copy_from_slice(&hashed);
    } else {
        key_padded[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= key_padded[i];
        opad[i] ^= key_padded[i];
    }

    let mut inner_hasher = Sha256::new();
    inner_hasher.update(ipad);
    inner_hasher.update(data);
    let inner_hash = inner_hasher.finalize();

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(opad);
    outer_hasher.update(inner_hash);
    outer_hasher.finalize().into()
}

/// Derive the AWS SigV4 signing key:
/// HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4" + secret_key, date), region), service), "aws4_request")
fn aws_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Build AWS SigV4 Authorization header.
/// Returns None when AWS credentials are not available in the environment.
#[allow(clippy::too_many_arguments)]
fn aws_sigv4_auth(
    method: &str,
    host: &str,
    path: &str,
    region: &str,
    service: &str,
    payload: &[u8],
    datetime: &str, // YYYYMMDDTHHmmSSZ format
    date: &str,     // YYYYMMDD format
) -> Option<String> {
    use sha2::{Digest, Sha256};

    let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

    let payload_hash = hex::encode(Sha256::digest(payload));
    let content_type = "application/x-amz-json-1.1";
    let amz_target = "secretsmanager.GetSecretValue";

    // Canonical headers (sorted by header name, lowercased).
    let canon_headers = if let Some(ref token) = session_token {
        format!(
            "content-type:{}\nhost:{}\nx-amz-date:{}\nx-amz-security-token:{}\nx-amz-target:{}\n",
            content_type, host, datetime, token, amz_target
        )
    } else {
        format!(
            "content-type:{}\nhost:{}\nx-amz-date:{}\nx-amz-target:{}\n",
            content_type, host, datetime, amz_target
        )
    };
    let signed_headers = if session_token.is_some() {
        "content-type;host;x-amz-date;x-amz-security-token;x-amz-target"
    } else {
        "content-type;host;x-amz-date;x-amz-target"
    };

    let canon_request = format!(
        "{}\n{}\n\n{}\n{}\n{}",
        method, path, canon_headers, signed_headers, payload_hash
    );

    let credential_scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        datetime,
        credential_scope,
        hex::encode(Sha256::digest(canon_request.as_bytes()))
    );

    let signing_key = aws_signing_key(&secret_key, date, region, service);
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key, credential_scope, signed_headers, signature
    );
    Some(auth)
}

/// Hex-encode bytes using the workspace `hex` crate.
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        ::hex::encode(bytes)
    }
}

async fn fetch_aws_secret(region: &str, secret_name: &str) -> Result<String> {
    // Support LocalStack and other endpoint overrides (CI-04/T-02).
    let endpoint = std::env::var("AWS_ENDPOINT_URL_SECRETSMANAGER")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .unwrap_or_else(|_| format!("https://secretsmanager.{}.amazonaws.com", region));

    let body = serde_json::json!({ "SecretId": secret_name }).to_string();
    let payload = body.as_bytes();
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(&endpoint);

    // Current datetime for signing.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let datetime = format_aws_datetime(now);
    let date = &datetime[..8];

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| AqueductError::Config(format!("Failed to build HTTP client: {}", e)))?;

    let mut request = client
        .post(&endpoint)
        .header("X-Amz-Target", "secretsmanager.GetSecretValue")
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Date", &datetime)
        .body(body.clone());

    // Add SigV4 authentication when credentials are available.
    if let Some(auth) = aws_sigv4_auth(
        "POST",
        host,
        "/",
        region,
        "secretsmanager",
        payload,
        &datetime,
        date,
    ) {
        request = request.header("Authorization", auth);
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            request = request.header("X-Amz-Security-Token", token);
        }
    }

    let resp = request
        .send()
        .await
        .map_err(|e| AqueductError::Config(format!("AWS SM HTTP error: {}", e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(AqueductError::Config(format!(
            "AWS Secrets Manager request failed {}: {}",
            status, text
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| AqueductError::Config(format!("AWS SM parse error: {}", e)))?;

    json.get("SecretString")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            AqueductError::Config(format!(
                "AWS Secrets Manager: SecretString not found in response for '{}'",
                secret_name
            ))
        })
}

/// Format a Unix timestamp as YYYYMMDDTHHmmSSZ for AWS SigV4.
fn format_aws_datetime(secs: u64) -> String {
    // Simple manual formatting without chrono dependency in this module.
    // Converts epoch seconds to UTC datetime string.
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Compute year, month, day from days_since_epoch (simplified Gregorian calendar).
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_ymd(days: u64) -> (u32, u32, u32) {
    // Gregorian calendar conversion (simplified).
    let mut remaining = days as i64;
    let mut year = 1970u32;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }
    let month_days: [i64; 12] = [
        31,
        if is_leap_year(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u32;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }
    (year, month, remaining as u32 + 1)
}

fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

// ── GCP Secret Manager HTTP client (SEC-02 / v0.12) ──────────────────────────

async fn fetch_gcp_secret(secret_name: &str) -> Result<String> {
    // Support a mock endpoint override (httpmock in tests).
    let base_url = std::env::var("GCP_SECRET_MANAGER_ENDPOINT_URL")
        .unwrap_or_else(|_| "https://secretmanager.googleapis.com".to_string());

    let token = std::env::var("GOOGLE_OAUTH_TOKEN")
        .or_else(|_| std::env::var("GOOGLE_APPLICATION_TOKEN"))
        .map_err(|_| {
            AqueductError::Config(
                "GCP Secret Manager: GOOGLE_OAUTH_TOKEN env var is not set. \
                 Obtain a token via `gcloud auth print-access-token` or \
                 the Workload Identity token endpoint."
                    .to_string(),
            )
        })?;

    let url = format!("{}/v1/{}:access", base_url, secret_name);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| AqueductError::Config(format!("Failed to build HTTP client: {}", e)))?;

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| AqueductError::Config(format!("GCP SM HTTP error: {}", e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(AqueductError::Config(format!(
            "GCP Secret Manager request failed {}: {}",
            status, text
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| AqueductError::Config(format!("GCP SM parse error: {}", e)))?;

    // Response: { "payload": { "data": "<base64-encoded secret>" } }
    let b64 = json
        .get("payload")
        .and_then(|p| p.get("data"))
        .and_then(|d| d.as_str())
        .ok_or_else(|| {
            AqueductError::Config(format!(
                "GCP Secret Manager: payload.data not found in response for '{}'",
                secret_name
            ))
        })?;

    // Decode base64.
    let bytes = base64_decode(b64)
        .map_err(|e| AqueductError::Config(format!("GCP SM: base64 decode error: {}", e)))?;

    String::from_utf8(bytes)
        .map_err(|_| AqueductError::Config("GCP SM: secret value is not valid UTF-8".to_string()))
}

/// Minimal base64 decoder (standard alphabet, no padding validation strictness).
fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut decode_table = [0xFF_u8; 256];
    for (i, &c) in TABLE.iter().enumerate() {
        decode_table[c as usize] = i as u8;
    }

    let input = input.trim_end_matches('=');
    let mut output = Vec::with_capacity(input.len() * 3 / 4);

    let bytes: Vec<u8> = input.bytes().collect();
    for chunk in bytes.chunks(4) {
        let a = if !chunk.is_empty() {
            decode_table[chunk[0] as usize]
        } else {
            0
        };
        let b = if chunk.len() > 1 {
            decode_table[chunk[1] as usize]
        } else {
            0
        };
        let c = if chunk.len() > 2 {
            decode_table[chunk[2] as usize]
        } else {
            0
        };
        let d = if chunk.len() > 3 {
            decode_table[chunk[3] as usize]
        } else {
            0
        };
        if a == 0xFF || b == 0xFF {
            return Err("invalid base64 character".to_string());
        }
        output.push((a << 2) | (b >> 4));
        if chunk.len() > 2 && c != 0xFF {
            output.push(((b & 0x0F) << 4) | (c >> 2));
        }
        if chunk.len() > 3 && d != 0xFF {
            output.push(((c & 0x03) << 6) | d);
        }
    }
    Ok(output)
}

// ── HashiCorp Vault KV v2 HTTP client (SEC-02 / v0.12) ───────────────────────

async fn fetch_vault_secret(vault_addr: &str, path: &str) -> Result<String> {
    let token = std::env::var("VAULT_TOKEN").map_err(|_| {
        AqueductError::Config(format!(
            "Vault secret at path '{}' could not be resolved. \
             Set VAULT_TOKEN and ensure VAULT_ADDR='{}' is reachable.",
            path, vault_addr
        ))
    })?;

    // Vault KV v2: the path is typically "secret/data/<key>".
    // If the caller passes a path without "data/", we insert it.
    let kv_path = if path.contains("/data/") {
        path.to_string()
    } else {
        // Convert "secret/my-key" → "secret/data/my-key".
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        match parts.as_slice() {
            [mount, rest] => format!("{}/data/{}", mount, rest),
            _ => format!("secret/data/{}", path),
        }
    };

    let url = format!("{}/v1/{}", vault_addr.trim_end_matches('/'), kv_path);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| AqueductError::Config(format!("Failed to build HTTP client: {}", e)))?;

    let resp = client
        .get(&url)
        .header("X-Vault-Token", &token)
        .send()
        .await
        .map_err(|e| AqueductError::Config(format!("Vault HTTP error: {}", e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(AqueductError::Config(format!(
            "Vault request failed {}: {}",
            status, text
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| AqueductError::Config(format!("Vault parse error: {}", e)))?;

    // KV v2 response: { "data": { "data": { "key": "value" } } }
    let data = json
        .get("data")
        .and_then(|d| d.get("data"))
        .ok_or_else(|| {
            AqueductError::Config(format!(
                "Vault: data.data not found in response for path '{}'",
                path
            ))
        })?;

    // Return the first string value found in the data map.
    if let Some(obj) = data.as_object() {
        if let Some(v) = obj.values().find_map(|v| v.as_str()) {
            return Ok(v.to_string());
        }
    }
    // If data is a string itself, return it directly.
    if let Some(s) = data.as_str() {
        return Ok(s.to_string());
    }

    Err(AqueductError::Config(format!(
        "Vault: no string value found in secret at path '{}'",
        path
    )))
}

/// Validate that a secret file path does not contain path traversal components.
///
/// Rejects any key that contains `..` as a path component.
fn validate_secret_path(key: &str) -> Result<()> {
    use std::path::Path;
    let p = Path::new(key);
    for component in p.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(AqueductError::InvalidSecretPath {
                path: key.to_string(),
            });
        }
    }
    Ok(())
}

/// Inject secrets into a DSN string by resolving `${secret:BACKEND:KEY}` references.
///
/// Syntax: `postgresql://user:${secret:vault:database/dsn}@host/db`
///
/// For compatibility, plain `${VAR}` references are resolved from environment
/// variables as before (delegated to `config::resolve_env_vars`).
pub async fn resolve_dsn_secrets(dsn: &str, backend: &SecretBackend) -> Result<String> {
    let mut result = dsn.to_string();

    for cap in SECRET_RE.captures_iter(dsn) {
        let full_match = cap.get(0).unwrap().as_str();
        let backend_name = cap.get(1).unwrap().as_str();
        let secret_key = cap.get(2).unwrap().as_str();

        let resolved_backend = SecretBackend::from_str(backend_name)?;
        let _ = backend; // The inline syntax overrides the global backend.
        let value = resolve_secret(&resolved_backend, secret_key).await?;
        result = result.replace(full_match, &value);
    }

    // Fall back to plain env var resolution for any remaining `${VAR}` references.
    crate::config::resolve_env_vars(&result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_from_str_env() {
        let b = SecretBackend::from_str("env").unwrap();
        assert_eq!(b, SecretBackend::Env);
    }

    #[test]
    fn test_backend_from_str_empty() {
        let b = SecretBackend::from_str("").unwrap();
        assert_eq!(b, SecretBackend::Env);
    }

    #[test]
    fn test_backend_from_str_unknown() {
        let err = SecretBackend::from_str("nonexistent").unwrap_err();
        assert!(err.to_string().contains("Unknown secret backend"));
    }

    #[tokio::test]
    async fn test_resolve_secret_env_found() {
        std::env::set_var("AQUEDUCT_TEST_SECRET_KEY", "supersecret");
        let value = resolve_secret(&SecretBackend::Env, "AQUEDUCT_TEST_SECRET_KEY")
            .await
            .unwrap();
        assert_eq!(value, "supersecret");
        std::env::remove_var("AQUEDUCT_TEST_SECRET_KEY");
    }

    #[tokio::test]
    async fn test_resolve_secret_env_missing() {
        std::env::remove_var("AQUEDUCT_DEFINITELY_NOT_SET_XYZ");
        let err = resolve_secret(&SecretBackend::Env, "AQUEDUCT_DEFINITELY_NOT_SET_XYZ")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not set"));
    }

    #[tokio::test]
    async fn test_resolve_dsn_secrets_plain_env() {
        std::env::set_var("TEST_DSN_HOST", "localhost");
        let dsn = "postgresql://user@${TEST_DSN_HOST}/db";
        let resolved = resolve_dsn_secrets(dsn, &SecretBackend::Env).await.unwrap();
        assert_eq!(resolved, "postgresql://user@localhost/db");
        std::env::remove_var("TEST_DSN_HOST");
    }

    #[test]
    fn test_backend_from_str_sops() {
        let b = SecretBackend::from_str("sops").unwrap();
        assert_eq!(b, SecretBackend::Sops);
    }

    #[test]
    fn test_backend_from_str_aws() {
        let b = SecretBackend::from_str("aws").unwrap();
        assert!(matches!(b, SecretBackend::AwsSecretsManager { .. }));
    }

    #[test]
    fn test_backend_from_str_gcp() {
        let b = SecretBackend::from_str("gcp").unwrap();
        assert!(matches!(b, SecretBackend::GcpSecretManager { .. }));
    }

    #[test]
    fn test_backend_from_str_vault() {
        let b = SecretBackend::from_str("vault").unwrap();
        assert!(matches!(b, SecretBackend::HashicorpVault { .. }));
    }

    #[test]
    fn test_validate_secret_path_safe() {
        assert!(validate_secret_path("secrets/my-secret.enc").is_ok());
        assert!(validate_secret_path("my-secret.enc").is_ok());
        assert!(validate_secret_path("/absolute/path/secret.enc").is_ok());
    }

    #[test]
    fn test_validate_secret_path_traversal() {
        let err = validate_secret_path("../etc/passwd").unwrap_err();
        assert!(err.to_string().contains("Invalid secret path"));
        let err2 = validate_secret_path("secrets/../../etc/passwd").unwrap_err();
        assert!(err2.to_string().contains("Invalid secret path"));
    }

    #[tokio::test]
    async fn test_resolve_dsn_secrets_inline_env() {
        std::env::set_var("AQUEDUCT_INLINE_SECRET_TEST", "mypassword");
        let dsn = "postgresql://user:${secret:env:AQUEDUCT_INLINE_SECRET_TEST}@localhost/db";
        let resolved = resolve_dsn_secrets(dsn, &SecretBackend::Env).await.unwrap();
        assert_eq!(resolved, "postgresql://user:mypassword@localhost/db");
        std::env::remove_var("AQUEDUCT_INLINE_SECRET_TEST");
    }

    #[tokio::test]
    async fn test_resolve_dsn_secrets_missing_env_secret() {
        std::env::remove_var("AQUEDUCT_INLINE_SECRET_MISSING_XYZ");
        let dsn = "postgresql://user:${secret:env:AQUEDUCT_INLINE_SECRET_MISSING_XYZ}@localhost/db";
        let err = resolve_dsn_secrets(dsn, &SecretBackend::Env)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not set")
                || err
                    .to_string()
                    .contains("AQUEDUCT_INLINE_SECRET_MISSING_XYZ")
        );
    }

    /// SEC-02: AWS Secrets Manager HTTP client — mock server test.
    /// Uses httpmock to intercept the HTTP call and return a fake secret.
    #[tokio::test]
    async fn test_aws_sm_http_client_mock() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("X-Amz-Target", "secretsmanager.GetSecretValue")
                .header("Content-Type", "application/x-amz-json-1.1");
            then.status(200)
                .header("Content-Type", "application/x-amz-json-1.1")
                .body(r#"{"SecretString":"test_secret_value","Name":"my-db-password"}"#);
        });

        // Point the AWS endpoint at the mock server.
        std::env::set_var("AWS_ENDPOINT_URL_SECRETSMANAGER", server.base_url());
        // Clear any real AWS credentials to avoid accidentally hitting real AWS.
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");

        let result = fetch_aws_secret("us-east-1", "my-db-password").await;
        mock.assert();
        assert_eq!(result.unwrap(), "test_secret_value");

        std::env::remove_var("AWS_ENDPOINT_URL_SECRETSMANAGER");
    }

    /// SEC-02: AWS Secrets Manager — error response propagates correctly.
    #[tokio::test]
    async fn test_aws_sm_error_response() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(400)
                .body(r#"{"__type":"ResourceNotFoundException","message":"Secrets Manager can't find the specified secret."}"#);
        });

        std::env::set_var("AWS_ENDPOINT_URL_SECRETSMANAGER", server.base_url());
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");

        let result = fetch_aws_secret("us-east-1", "non-existent-secret").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("400"));

        std::env::remove_var("AWS_ENDPOINT_URL_SECRETSMANAGER");
    }

    /// SEC-02: GCP Secret Manager HTTP client — mock server test.
    #[tokio::test]
    async fn test_gcp_sm_http_client_mock() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        let secret_name = "projects/my-project/secrets/my-secret/versions/latest";
        let server_mock = server.mock(|when, then| {
            when.method(GET).path(format!("/v1/{}:access", secret_name));
            then.status(200)
                .header("Content-Type", "application/json")
                // "aGVsbG93b3JsZA==" is base64("helloworld")
                .body(r#"{"payload":{"data":"aGVsbG93b3JsZA=="}}"#);
        });

        std::env::set_var("GCP_SECRET_MANAGER_ENDPOINT_URL", server.base_url());
        std::env::set_var("GOOGLE_OAUTH_TOKEN", "fake-test-token");

        let result = fetch_gcp_secret(secret_name).await;
        server_mock.assert();
        assert_eq!(result.unwrap(), "helloworld");

        std::env::remove_var("GCP_SECRET_MANAGER_ENDPOINT_URL");
        std::env::remove_var("GOOGLE_OAUTH_TOKEN");
    }

    /// SEC-02: HashiCorp Vault HTTP client — mock server test.
    #[tokio::test]
    async fn test_vault_http_client_mock() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        let server_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/secret/data/my-db-password")
                .header("X-Vault-Token", "test-vault-token");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(r#"{"data":{"data":{"value":"vault_secret_value"}}}"#);
        });

        let vault_addr = server.base_url();
        std::env::set_var("VAULT_TOKEN", "test-vault-token");

        let result = fetch_vault_secret(&vault_addr, "secret/my-db-password").await;
        server_mock.assert();
        assert_eq!(result.unwrap(), "vault_secret_value");

        std::env::remove_var("VAULT_TOKEN");
    }

    /// SEC-02: Vault error response propagates correctly.
    #[tokio::test]
    async fn test_vault_error_response() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET);
            then.status(403).body(r#"{"errors":["permission denied"]}"#);
        });

        std::env::set_var("VAULT_TOKEN", "bad-token");
        let result = fetch_vault_secret(&server.base_url(), "secret/my-secret").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("403"));

        std::env::remove_var("VAULT_TOKEN");
    }

    /// SEC-02: Vault missing token returns config error.
    #[tokio::test]
    async fn test_vault_missing_token() {
        std::env::remove_var("VAULT_TOKEN");
        let result = fetch_vault_secret("http://127.0.0.1:8200", "secret/my-secret").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("VAULT_TOKEN"));
    }

    /// SEC-02: GCP missing token returns config error.
    #[tokio::test]
    async fn test_gcp_missing_token() {
        std::env::remove_var("GOOGLE_OAUTH_TOKEN");
        std::env::remove_var("GOOGLE_APPLICATION_TOKEN");
        let result = fetch_gcp_secret("projects/p/secrets/s/versions/1").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("GOOGLE_OAUTH_TOKEN"));
    }

    #[test]
    fn test_hmac_sha256_produces_consistent_output() {
        let key = b"test-key";
        let data = b"test-data";
        let result1 = hmac_sha256(key, data);
        let result2 = hmac_sha256(key, data);
        assert_eq!(result1, result2);
        assert_ne!(result1, [0u8; 32]);
    }

    #[test]
    fn test_base64_decode_roundtrip() {
        // "helloworld" → "aGVsbG93b3JsZA=="
        let decoded = base64_decode("aGVsbG93b3JsZA==").unwrap();
        assert_eq!(decoded, b"helloworld");
    }

    #[test]
    fn test_format_aws_datetime_structure() {
        let dt = format_aws_datetime(0);
        // 1970-01-01T00:00:00Z → "19700101T000000Z"
        assert_eq!(dt, "19700101T000000Z");
        assert_eq!(dt.len(), 16);
    }
}
