use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod commands;
use commands::{apply, fmt, import, init, lint, plan, preview, rollback, status, unlock, validate};

/// Declarative schema evolution and migration for stream-table DAGs.
#[derive(Debug, Parser)]
#[command(
    name = "aqueduct",
    version,
    about = "Declarative schema evolution and migration for stream-table DAGs",
    long_about = None,
)]
struct Cli {
    /// Log format: text (default) or json.
    #[arg(long, default_value = "text", global = true)]
    log_format: String,

    /// Verbosity: error, warn, info (default), debug, trace.
    #[arg(long, default_value = "info", global = true, env = "AQUEDUCT_LOG")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Bootstrap the aqueduct catalog schema in the target database.
    Init(init::InitArgs),

    /// Compute a migration plan by diffing desired vs. actual DAG state.
    Plan(plan::PlanArgs),

    /// Apply a migration plan to the target database.
    Apply(apply::ApplyArgs),

    /// Show the current project state and drift status.
    Status(status::StatusArgs),

    /// Offline validation of migration files (no database connection required).
    Validate(validate::ValidateArgs),

    /// Rollback to a previous DAG version.
    Rollback(rollback::RollbackArgs),

    /// Import an existing pg_trickle deployment into a migrations directory.
    Import(import::ImportArgs),

    /// Release a stale project lock (emergency use).
    Unlock(unlock::UnlockArgs),

    /// Create, inspect, or tear down a preview environment for a candidate change.
    Preview(preview::PreviewArgs),

    /// Canonicalise the SQL body and front-matter directives in migration files.
    Fmt(fmt::FmtArgs),

    /// Check migration files for risky or non-optimal patterns.
    Lint(lint::LintArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialise structured logging.
    let is_ci = std::env::var("CI").is_ok()
        || std::env::var("GITHUB_ACTIONS").is_ok()
        || std::env::var("GITLAB_CI").is_ok()
        || std::env::var("CIRCLECI").is_ok();

    let use_json = cli.log_format == "json" || is_ci;

    let filter = EnvFilter::try_new(&cli.log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    if use_json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let result = match cli.command {
        Commands::Init(args) => init::run(args).await,
        Commands::Plan(args) => plan::run(args).await,
        Commands::Apply(args) => apply::run(args).await,
        Commands::Status(args) => status::run(args).await,
        Commands::Validate(args) => validate::run(args).await,
        Commands::Rollback(args) => rollback::run(args).await,
        Commands::Import(args) => import::run(args).await,
        Commands::Unlock(args) => unlock::run(args).await,
        Commands::Preview(args) => preview::run(args).await,
        Commands::Fmt(args) => fmt::run(args).await,
        Commands::Lint(args) => lint::run(args).await,
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}
