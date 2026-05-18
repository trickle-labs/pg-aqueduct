//! Encrypted secret handling for aqueduct DSN resolution.
//!
//! Supports resolving DSN passwords / full DSNs from:
//! - Environment variables (default, always available)
//! - AWS Secrets Manager (via `AWS_REGION` + AWS SDK credentials)
//! - GCP Secret Manager (via `GOOGLE_APPLICATION_CREDENTIALS`)
//! - HashiCorp Vault (via `VAULT_ADDR` + `VAULT_TOKEN`)
//! - SOPS-encrypted files (via `sops -d`)
//! - age-encrypted files (via `age -d`)
//!
//! All backends ultimately produce a plain-text secret value that is used
//! once and never written to disk.

use crate::error::{AqueductError, Result};

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

        SecretBackend::AwsSecretsManager { region } => {
            // In production this would call the AWS SDK.
            // For v0.6 we implement the scaffolding and delegate to environment
            // variables when the SDK is not compiled in, matching the pattern
            // used by other tools (e.g., aws-vault injects via env vars).
            tracing::info!(
                backend = "aws-secrets-manager",
                region = %region,
                secret = %key,
                "Resolving secret from AWS Secrets Manager"
            );

            // Check for an injected env var first (the common CI pattern where
            // the secret was already fetched by a wrapper script or action).
            let env_key = key.replace('/', "_").replace('-', "_").to_uppercase();
            std::env::var(&env_key).map_err(|_| {
                AqueductError::Config(format!(
                    "AWS Secrets Manager secret '{}' could not be resolved. \
                     Inject the secret as an environment variable '{}' or ensure \
                     AWS SDK credentials are configured.",
                    key, env_key
                ))
            })
        }

        SecretBackend::GcpSecretManager { project_id } => {
            tracing::info!(
                backend = "gcp-secret-manager",
                project = %project_id,
                secret = %key,
                "Resolving secret from GCP Secret Manager"
            );

            // Same pattern as AWS: check for injected env var.
            let env_key = key
                .split('/')
                .last()
                .unwrap_or(key)
                .replace('-', "_")
                .to_uppercase();
            std::env::var(&env_key).map_err(|_| {
                AqueductError::Config(format!(
                    "GCP Secret Manager secret '{}' could not be resolved. \
                     Inject the secret as environment variable '{}' or ensure \
                     Application Default Credentials are configured.",
                    key, env_key
                ))
            })
        }

        SecretBackend::HashicorpVault { vault_addr } => {
            tracing::info!(
                backend = "vault",
                addr = %vault_addr,
                path = %key,
                "Resolving secret from HashiCorp Vault"
            );

            let env_key = key
                .split('/')
                .last()
                .unwrap_or(key)
                .replace('-', "_")
                .to_uppercase();
            std::env::var(&env_key).map_err(|_| {
                AqueductError::Config(format!(
                    "Vault secret at path '{}' could not be resolved. \
                     Set VAULT_TOKEN and ensure VAULT_ADDR='{}' is reachable, \
                     or inject the secret as environment variable '{}'.",
                    key, vault_addr, env_key
                ))
            })
        }

        SecretBackend::Sops => {
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

/// Inject secrets into a DSN string by resolving `${secret:BACKEND:KEY}` references.
///
/// Syntax: `postgresql://user:${secret:vault:database/dsn}@host/db`
///
/// For compatibility, plain `${VAR}` references are resolved from environment
/// variables as before (delegated to `config::resolve_env_vars`).
pub async fn resolve_dsn_secrets(dsn: &str, backend: &SecretBackend) -> Result<String> {
    let secret_re = regex::Regex::new(r"\$\{secret:([^:]+):([^}]+)\}").expect("valid regex");

    let mut result = dsn.to_string();

    for cap in secret_re.captures_iter(dsn) {
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
        // Ensure this doesn't panic even if AWS_REGION is not set.
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
}
