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

impl ApplyConfig {
    /// Returns true if `now_utc` falls within the configured maintenance window.
    ///
    /// Window format: "HH:MM-HH:MM UTC" or "HH:MM-HH:MM +HH:MM".
    /// Returns true if no maintenance window is configured (no restriction).
    pub fn is_in_maintenance_window(&self, now_utc: chrono::DateTime<chrono::Utc>) -> bool {
        let Some(ref window) = self.maintenance_window else {
            return true; // No window configured → always allowed.
        };
        parse_maintenance_window_check(window, now_utc).unwrap_or(true)
    }

    /// Returns true if the plan's migration classes require maintenance window gating.
    pub fn plan_requires_window(&self, rebuild_count: usize, blue_green_count: usize) -> bool {
        let has_rebuild = rebuild_count > 0 && self.maintenance_window_applies_to.contains(&"rebuild".to_string());
        let has_bg = blue_green_count > 0 && self.maintenance_window_applies_to.contains(&"blue-green".to_string());
        has_rebuild || has_bg
    }
}

/// Parse "HH:MM-HH:MM [UTC|+HH:MM|-HH:MM]" and check if `now_utc` is in the window.
fn parse_maintenance_window_check(
    window: &str,
    now_utc: chrono::DateTime<chrono::Utc>,
) -> Option<bool> {
    use chrono::Timelike;
    // Split into "HH:MM-HH:MM" and optional TZ.
    let parts: Vec<&str> = window.trim().splitn(2, ' ').collect();
    let times_part = parts[0];
    let tz_offset_minutes: i64 = if parts.len() > 1 {
        parse_tz_offset_minutes(parts[1]).unwrap_or(0)
    } else {
        0
    };

    // Parse start-end as "HH:MM-HH:MM".
    let dash_pos = times_part.rfind('-')?;
    let start_str = &times_part[..dash_pos];
    let end_str = &times_part[dash_pos + 1..];

    let (start_h, start_m) = parse_hhmm(start_str)?;
    let (end_h, end_m) = parse_hhmm(end_str)?;

    // Convert now_utc to the local window timezone.
    let now_local = now_utc + chrono::Duration::minutes(tz_offset_minutes);
    let current_minutes = (now_local.hour() as i64) * 60 + (now_local.minute() as i64);
    let start_minutes = start_h * 60 + start_m;
    let end_minutes = end_h * 60 + end_m;

    let in_window = if start_minutes <= end_minutes {
        // Normal window (e.g. 02:00-04:00)
        current_minutes >= start_minutes && current_minutes < end_minutes
    } else {
        // Wraps midnight (e.g. 22:00-02:00)
        current_minutes >= start_minutes || current_minutes < end_minutes
    };

    Some(in_window)
}

fn parse_hhmm(s: &str) -> Option<(i64, i64)> {
    let parts: Vec<&str> = s.trim().splitn(2, ':').collect();
    if parts.len() != 2 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    if h > 23 || m > 59 {
        return None;
    }
    Some((h, m))
}

fn parse_tz_offset_minutes(tz: &str) -> Option<i64> {
    let tz = tz.trim();
    if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("Z") {
        return Some(0);
    }
    // "+HH:MM" or "-HH:MM"
    if tz.starts_with('+') || tz.starts_with('-') {
        let sign: i64 = if tz.starts_with('+') { 1 } else { -1 };
        let (h, m) = parse_hhmm(&tz[1..])?;
        return Some(sign * (h * 60 + m));
    }
    None
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
