use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::{AqueductError, Result};

/// Top-level `aqueduct.toml` configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AqueductConfig {
    pub project: ProjectConfig,

    #[serde(default)]
    pub targets: HashMap<String, TargetConfig>,

    #[serde(default)]
    pub apply: ApplyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectConfig {
    pub name: String,

    /// Override the catalog schema name (default: "aqueduct").
    #[serde(default = "default_catalog_schema")]
    pub catalog_schema: String,
}

fn default_catalog_schema() -> String {
    "aqueduct".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetConfig {
    /// Connection string (libpq DSN). May reference env vars as `${VAR}`.
    pub dsn: String,

    /// Template variables substituted into migration front-matter.
    #[serde(default)]
    pub vars: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApplyConfig {
    /// Maximum time to wait for an advisory lock, e.g. "30s".
    #[serde(default = "default_lock_timeout")]
    pub lock_timeout: String,

    /// Maintenance window in the form "HH:MM-HH:MM TZ", e.g. "02:00-04:00 UTC".
    pub maintenance_window: Option<String>,

    /// If true, any plan step that would trigger a FULL refresh is a plan error.
    #[serde(default)]
    pub allow_full_refresh: bool,

    /// Default migration strategy.
    #[serde(default = "default_strategy")]
    pub default_strategy: String,

    /// Which migration classes are gated by the maintenance window.
    #[serde(default = "default_maintenance_window_applies_to")]
    pub maintenance_window_applies_to: Vec<String>,

    /// Pre/post hooks.
    #[serde(default)]
    pub hooks: HooksConfig,
}

fn default_lock_timeout() -> String {
    "30s".to_string()
}

fn default_strategy() -> String {
    "in-place".to_string()
}

fn default_maintenance_window_applies_to() -> Vec<String> {
    vec!["rebuild".to_string(), "blue-green".to_string()]
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HooksConfig {
    pub pre: Option<String>,
    pub post: Option<String>,
}

impl AqueductConfig {
    /// Load and parse `aqueduct.toml` from the given directory (or file path).
    pub fn load(path: &Path) -> Result<Self> {
        let config_path = if path.is_dir() {
            path.join("aqueduct.toml")
        } else {
            path.to_path_buf()
        };

        let content = std::fs::read_to_string(&config_path).map_err(|e| {
            AqueductError::Config(format!("Cannot read {}: {}", config_path.display(), e))
        })?;

        let mut config: AqueductConfig =
            toml::from_str(&content).map_err(|e| AqueductError::Config(e.to_string()))?;

        // Resolve environment variable references in DSN values.
        for target in config.targets.values_mut() {
            target.dsn = resolve_env_vars(&target.dsn)?;
        }

        Ok(config)
    }

    /// Get a target configuration by name, returning an error if not found.
    pub fn target(&self, name: &str) -> Result<&TargetConfig> {
        self.targets.get(name).ok_or_else(|| {
            AqueductError::Config(format!(
                "Target '{}' not found in aqueduct.toml. Available targets: {}",
                name,
                self.targets.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })
    }
}

/// Resolve `${VAR_NAME}` references in a string from environment variables.
pub fn resolve_env_vars(s: &str) -> Result<String> {
    let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    let mut result = s.to_string();
    let mut errors: Vec<String> = Vec::new();

    for cap in re.captures_iter(s) {
        let full_match = cap.get(0).unwrap().as_str();
        let var_name = cap.get(1).unwrap().as_str();
        match std::env::var(var_name) {
            Ok(val) => {
                result = result.replace(full_match, &val);
            }
            Err(_) => {
                errors.push(var_name.to_string());
            }
        }
    }

    if !errors.is_empty() {
        return Err(AqueductError::MissingVariable(errors.join(", ")));
    }

    Ok(result)
}

/// Substitute `{{ var.NAME }}` template variables in a string.
pub fn substitute_vars(s: &str, vars: &HashMap<String, String>) -> Result<String> {
    let re = regex::Regex::new(r"\{\{\s*var\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}").unwrap();
    let mut result = s.to_string();
    let mut errors: Vec<String> = Vec::new();

    for cap in re.captures_iter(s) {
        let full_match = cap.get(0).unwrap().as_str();
        let var_name = cap.get(1).unwrap().as_str();

        // Check AQUEDUCT_VAR_* env vars first (higher priority).
        let env_key = format!("AQUEDUCT_VAR_{}", var_name.to_uppercase());
        let value = std::env::var(&env_key)
            .ok()
            .or_else(|| vars.get(var_name).cloned());

        match value {
            Some(val) => {
                result = result.replace(full_match, &val);
            }
            None => {
                errors.push(var_name.to_string());
            }
        }
    }

    if !errors.is_empty() {
        return Err(AqueductError::MissingVariable(errors.join(", ")));
    }

    Ok(result)
}

/// Returns the default project directory (current working directory).
pub fn default_project_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn make_config(content: &str) -> (TempDir, AqueductConfig) {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("aqueduct.toml"), content).unwrap();
        let cfg = AqueductConfig::load(dir.path()).unwrap();
        (dir, cfg)
    }

    #[test]
    fn test_load_minimal_config() {
        let (_dir, cfg) = make_config(
            r#"
[project]
name = "test-project"

[targets.dev]
dsn = "postgresql://localhost/test"
"#,
        );
        assert_eq!(cfg.project.name, "test-project");
        assert_eq!(cfg.project.catalog_schema, "aqueduct");
        assert_eq!(cfg.targets["dev"].dsn, "postgresql://localhost/test");
    }

    #[test]
    fn test_load_config_with_vars() {
        let (_dir, cfg) = make_config(
            r#"
[project]
name = "my-project"

[targets.prod]
dsn = "postgresql://localhost/prod"
vars = { schedule = "30s", cdc_mode = "wal" }
"#,
        );
        assert_eq!(cfg.targets["prod"].vars["schedule"], "30s");
        assert_eq!(cfg.targets["prod"].vars["cdc_mode"], "wal");
    }

    #[test]
    fn test_target_not_found() {
        let (_dir, cfg) = make_config(
            r#"
[project]
name = "x"
"#,
        );
        assert!(cfg.target("nonexistent").is_err());
    }

    #[test]
    fn test_resolve_env_vars_no_vars() {
        let result = resolve_env_vars("postgresql://localhost/db").unwrap();
        assert_eq!(result, "postgresql://localhost/db");
    }

    #[test]
    fn test_resolve_env_vars_missing() {
        let result = resolve_env_vars("${AQUEDUCT_TEST_MISSING_XYZ_VAR}");
        assert!(result.is_err());
    }

    #[test]
    fn test_substitute_vars() {
        let mut vars = HashMap::new();
        vars.insert("schedule".to_string(), "30s".to_string());
        let result = substitute_vars("{{ var.schedule }}", &vars).unwrap();
        assert_eq!(result, "30s");
    }

    #[test]
    fn test_substitute_vars_missing() {
        let vars = HashMap::new();
        let result = substitute_vars("{{ var.missing_var }}", &vars);
        assert!(result.is_err());
    }
}
