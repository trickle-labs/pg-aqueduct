use aqueduct_core::catalog::CATALOG_INIT_V4_SQL;
use clap::Args;

use super::connect;

#[derive(Debug, Args)]
pub struct InitArgs {
    /// PostgreSQL connection string (DSN). Overrides the config file.
    #[arg(long)]
    pub dsn: Option<String>,

    /// Target name from aqueduct.toml.
    #[arg(long)]
    pub to: Option<String>,

    /// Project directory (default: current directory).
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Override the catalog schema name (default: "aqueduct").
    #[arg(long, default_value = "aqueduct")]
    pub schema: String,

    /// Scaffold a new project directory with aqueduct.toml and migrations/.
    #[arg(long)]
    pub scaffold: bool,

    /// Allow DSN with embedded plaintext password (not recommended outside CI/dev).
    #[arg(long)]
    pub allow_plaintext_password: bool,
}

pub async fn run(args: InitArgs) -> anyhow::Result<()> {
    // ARCH-1 (v0.19): Reject non-default catalog schema until full
    // parameterised SQL substitution is implemented in v0.20.
    aqueduct_core::catalog::validate_catalog_schema_not_overridden(&args.schema)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let dsn = super::resolve_dsn_with_opts(
        args.dsn.as_deref(),
        args.to.as_deref(),
        &args.project_dir,
        args.allow_plaintext_password,
    )
    .await?;

    let client = connect(&dsn).await?;

    // Check that we're connecting to a primary.
    let is_primary: bool = client
        .query_one("SELECT NOT pg_is_in_recovery()", &[])
        .await?
        .get(0);
    if !is_primary {
        anyhow::bail!("Cannot initialise aqueduct catalog on a hot standby.");
    }

    // Create the catalog schema (v4 — includes all tables and performance indexes).
    client.batch_execute(CATALOG_INIT_V4_SQL).await?;

    let pg_version: String = client.query_one("SELECT version()", &[]).await?.get(0);

    let pgtrickle_installed: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
            &[],
        )
        .await?
        .get(0);

    let pgtrickle_version = if pgtrickle_installed {
        client
            .query_opt("SELECT pgtrickle.pgt_extension_version()", &[])
            .await?
            .map(|r| r.get::<_, String>(0))
    } else {
        None
    };

    println!("✓ Aqueduct catalog initialised successfully.");
    println!(
        "  PostgreSQL: {}",
        pg_version.lines().next().unwrap_or(&pg_version)
    );
    if let Some(v) = pgtrickle_version {
        println!("  pg_trickle: {}", v);
    } else {
        println!("  pg_trickle: not installed");
    }

    // Scaffold project files if requested.
    if args.scaffold {
        scaffold_project(&args.project_dir)?;
        println!("  Project scaffolded at {}", args.project_dir.display());
    }

    Ok(())
}

fn scaffold_project(dir: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir.join("migrations").join("streams"))?;
    std::fs::create_dir_all(dir.join("migrations").join("sources"))?;

    let toml_path = dir.join("aqueduct.toml");
    if !toml_path.exists() {
        let content = r#"[project]
name = "my-project"

[targets.dev]
dsn = "${AQUEDUCT_DEV_DSN}"
vars = { schedule = "5m", cdc_mode = "trigger" }

[targets.prod]
dsn = "${AQUEDUCT_PROD_DSN}"
vars = { schedule = "30s", cdc_mode = "wal" }

[apply]
lock_timeout = "30s"
allow_full_refresh = true
"#;
        std::fs::write(&toml_path, content)?;
    }

    let example_stream = dir.join("migrations").join("streams").join("example.sql");
    if !example_stream.exists() {
        let content = r#"-- @aqueduct:schedule     = "{{ var.schedule }}"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
-- Replace this with your stream table query.
SELECT 1 AS placeholder;
"#;
        std::fs::write(&example_stream, content)?;
    }

    Ok(())
}
