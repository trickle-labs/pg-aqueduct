use thiserror::Error;

#[derive(Debug, Error)]
pub enum AqueductError {
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Parse error in {file}: {message}")]
    Parse { file: String, message: String },

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("IVM-unsupportable query in {table}: {reason}")]
    IvmUnsupportable { table: String, reason: String },

    #[error("Cycle detected in DAG: {0}")]
    Cycle(String),

    #[error("Missing variable: {0}")]
    MissingVariable(String),

    #[error("Lock contention on project '{project}': held by '{holder}'")]
    LockContention { project: String, holder: String },

    #[error("Not connected to a primary (pg_is_in_recovery() = true)")]
    NotPrimary,

    #[error("pg_trickle not installed or version too old: {0}")]
    PgTrickleVersion(String),

    #[error("Catalog schema error: {0}")]
    Catalog(String),

    #[error("Maintenance window active: plan contains rebuild-class steps deferred to {window}")]
    MaintenanceWindow { window: String },

    #[error("Full refresh not allowed (allow_full_refresh = false): {0}")]
    FullRefreshNotAllowed(String),

    #[error("Migration resumption error: {0}")]
    Resume(String),

    #[error("pg_trickle is not installed on the target database")]
    PgTrickleNotInstalled,

    #[error("Invariant violation in {context}")]
    InvariantViolation { context: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML error: {0}")]
    Toml(String),

    #[error("{0}")]
    Other(String),
}

impl From<toml::de::Error> for AqueductError {
    fn from(e: toml::de::Error) -> Self {
        AqueductError::Toml(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AqueductError>;
