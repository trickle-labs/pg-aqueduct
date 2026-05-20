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

    #[error("Plaintext password detected in DSN. Use a secret backend or pass --allow-plaintext-password to override.")]
    PlaintextPassword,

    #[error("Invalid secret path '{path}': must be within the allowed root")]
    InvalidSecretPath { path: String },

    /// Returned when the lock heartbeat confirms the lock was lost mid-migration.
    #[error("Lock lost mid-migration: the project lock was stolen or expired")]
    LockLost,

    /// Returned when a rollback plan contains Rebuild-class steps and
    /// `--accept-data-loss` was not passed.
    #[error("Rollback would cause data loss. Pass --accept-data-loss to proceed.")]
    DataLossRequired,

    /// Returned when `destroy` cannot verify ownership and `--force-unowned`
    /// was not passed.
    #[error(
        "Cannot verify ownership for stream table '{table}'. Pass --force-unowned to proceed."
    )]
    OwnershipRequired { table: String },

    /// Returned when the probed pg_trickle API does not match the expected
    /// function signatures (M-07 / v0.12).
    #[error(
        "pg_trickle API mismatch: expected function '{expected}' but it was not found or has a different signature"
    )]
    PgtrickleApiMismatch { expected: String, found: String },

    #[error("{0}")]
    Other(String),
}

impl AqueductError {
    /// Return the stable numeric error code for this variant (Q-04).
    ///
    /// These codes are stable across releases and can be used in CI scripts
    /// to detect and handle specific error conditions.
    ///
    /// Range conventions:
    /// - 1000–1099: database connectivity / primary checks
    /// - 1100–1199: configuration / parsing
    /// - 1200–1299: planning / classification
    /// - 1300–1399: execution / safety
    /// - 1400–1499: catalog / schema management
    pub fn error_code(&self) -> u32 {
        match self {
            AqueductError::Database(_) => 1000,
            AqueductError::NotPrimary => 1001,
            AqueductError::PgTrickleNotInstalled => 1002,
            AqueductError::PgTrickleVersion(_) => 1003,
            AqueductError::Config(_) => 1100,
            AqueductError::Parse { .. } => 1101,
            AqueductError::SqlParse(_) => 1102,
            AqueductError::MissingVariable(_) => 1103,
            AqueductError::Toml(_) => 1104,
            AqueductError::PlaintextPassword => 1105,
            AqueductError::InvalidSecretPath { .. } => 1106,
            AqueductError::IvmUnsupportable { .. } => 1200,
            AqueductError::Cycle(_) => 1201,
            AqueductError::InvariantViolation { .. } => 1202,
            AqueductError::FullRefreshNotAllowed(_) => 1203,
            AqueductError::MaintenanceWindow { .. } => 1204,
            AqueductError::LockContention { .. } => 1300,
            AqueductError::LockLost => 1301,
            AqueductError::Resume(_) => 1302,
            AqueductError::DataLossRequired => 1303,
            AqueductError::OwnershipRequired { .. } => 1304,
            AqueductError::PgtrickleApiMismatch { .. } => 1005,
            AqueductError::Catalog(_) => 1400,
            AqueductError::Io(_) => 1500,
            AqueductError::Json(_) => 1501,
            AqueductError::Other(_) => 9999,
        }
    }
}

impl From<toml::de::Error> for AqueductError {
    fn from(e: toml::de::Error) -> Self {
        AqueductError::Toml(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AqueductError>;
