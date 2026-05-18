use aqueduct_core::{
    config::AqueductConfig,
    dag::build_dag_state,
    preview::{
        create_preview_cnpg, create_preview_native, create_preview_neon, drop_preview_native,
        list_preview_schemas, PreviewBackend, PreviewConfig,
    },
};
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct PreviewArgs {
    /// PostgreSQL connection string.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Branch name to create the preview for (used to derive the schema name).
    /// Example: `feat/my-feature` → `aqueduct_preview_feat_my_feature`.
    #[arg(long)]
    pub branch: Option<String>,

    /// Preview backend: native (default), cnpg, or neon.
    #[arg(long, default_value = "native")]
    pub backend: String,

    /// Sample fraction for TABLESAMPLE (0.0–1.0). Default: 0.1 (10%).
    #[arg(long, default_value = "0.1")]
    pub sample: f64,

    /// Drop and recreate the preview schema if it already exists.
    #[arg(long)]
    pub recreate: bool,

    /// Drop an existing preview environment.
    #[arg(long)]
    pub cleanup: bool,

    /// List all preview environments in the database.
    #[arg(long)]
    pub list: bool,

    /// CloudNativePG operator endpoint (required for --backend cnpg).
    #[arg(long)]
    pub cnpg_endpoint: Option<String>,

    /// Neon API token (required for --backend neon).
    #[arg(long, env = "NEON_API_TOKEN")]
    pub neon_api_token: Option<String>,

    /// Neon project ID (required for --backend neon).
    #[arg(long, env = "NEON_PROJECT_ID")]
    pub neon_project_id: Option<String>,
}

pub async fn run(args: PreviewArgs) -> anyhow::Result<()> {
    let dsn =
        super::resolve_dsn(args.dsn.as_deref(), args.to.as_deref(), &args.project_dir).await?;
    let client = connect(&dsn).await?;

    // ── List mode ──────────────────────────────────────────────────────────────
    if args.list {
        let schemas = list_preview_schemas(&client).await?;
        if schemas.is_empty() {
            println!("No preview environments found.");
        } else {
            println!("Preview environments:");
            for schema in &schemas {
                println!("  {}", schema);
            }
        }
        return Ok(());
    }

    // ── Cleanup mode ───────────────────────────────────────────────────────────
    if args.cleanup {
        let branch = args
            .branch
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--branch is required with --cleanup"))?;

        let config = PreviewConfig::new_native(branch);
        let schema = config.schema_name();

        tracing::info!("Dropping preview environment '{}'", schema);
        drop_preview_native(&client, &schema).await?;
        println!("Preview environment '{}' dropped.", schema);
        return Ok(());
    }

    // ── Create mode ────────────────────────────────────────────────────────────
    let branch = args
        .branch
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--branch is required to create a preview environment"))?;

    // Load config and migration files.
    let vars = load_vars(&args.project_dir, args.to.as_deref());
    let files = aqueduct_core::parser::load_migrations(&args.project_dir, &vars)?;
    let desired = build_dag_state(&files, true)?;

    let backend = match args.backend.as_str() {
        "native" => PreviewBackend::Native,
        "cnpg" => {
            let endpoint = args.cnpg_endpoint.clone().ok_or_else(|| {
                anyhow::anyhow!("--cnpg-endpoint is required for the cnpg backend")
            })?;
            PreviewBackend::CloudNativePg { endpoint }
        }
        "neon" => {
            let api_token = args.neon_api_token.clone().ok_or_else(|| {
                anyhow::anyhow!("NEON_API_TOKEN is required for the neon backend")
            })?;
            let project_id = args.neon_project_id.clone().ok_or_else(|| {
                anyhow::anyhow!("NEON_PROJECT_ID is required for the neon backend")
            })?;
            PreviewBackend::Neon {
                api_token,
                project_id,
            }
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unknown preview backend '{}'. Use: native, cnpg, or neon.",
                other
            ));
        }
    };

    let mut config = PreviewConfig::new_native(branch);
    config.backend = backend.clone();
    config.sample_fraction = args.sample;
    config.recreate = args.recreate;

    let env = match &config.backend {
        PreviewBackend::Native => create_preview_native(&client, &config, &desired).await?,
        PreviewBackend::CloudNativePg { endpoint } => {
            create_preview_cnpg(endpoint, &config, &desired).await?
        }
        PreviewBackend::Neon {
            api_token,
            project_id,
        } => create_preview_neon(api_token, project_id, &config, &desired).await?,
    };

    println!(
        "Preview environment created: schema = '{}', branch = '{}', tables = {}",
        env.schema_name,
        env.branch,
        env.stream_tables.join(", ")
    );
    println!(
        "Connect and explore:\n  psql {} -c '\\dt {}.*'",
        dsn, env.schema_name
    );
    println!(
        "Cleanup when done:\n  aqueduct preview --branch {} --cleanup",
        branch
    );

    Ok(())
}

/// Load template variables from the config file (best-effort).
fn load_vars(
    project_dir: &std::path::Path,
    target: Option<&str>,
) -> std::collections::HashMap<String, String> {
    if let Ok(config) = AqueductConfig::load(project_dir) {
        if let Some(t) = target {
            if let Ok(target_cfg) = config.target(t) {
                return target_cfg.vars.clone();
            }
        }
    }
    std::collections::HashMap::new()
}
