use aqueduct_core::{
    fmt::{format_migrations, FmtResult},
    parser::load_migrations,
};
use clap::Args;
use std::collections::HashMap;

#[derive(Debug, Args)]
pub struct FmtArgs {
    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Check formatting only — exit non-zero if any file is not canonical.
    /// Does not write any files.
    #[arg(long)]
    pub check: bool,

    /// Output format: text (default) or json.
    #[arg(long, default_value = "text")]
    pub format: String,
}

pub async fn run(args: FmtArgs) -> anyhow::Result<()> {
    let files = load_migrations(&args.project_dir, &HashMap::new())?;

    let result: FmtResult = format_migrations(&files, args.check);

    match args.format.as_str() {
        "json" => {
            let changed: Vec<_> = result
                .changed
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            let unchanged: Vec<_> = result
                .unchanged
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            let errors: Vec<_> = result
                .errors
                .iter()
                .map(|(p, e)| {
                    serde_json::json!({
                        "file": p.display().to_string(),
                        "error": e,
                    })
                })
                .collect();
            let output = serde_json::json!({
                "schema_version": 1,
                "changed": changed,
                "unchanged": unchanged,
                "errors": errors,
                "check_only": args.check,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        _ => {
            for path in &result.changed {
                if args.check {
                    println!("  would reformat: {}", path.display());
                } else {
                    println!("  formatted:      {}", path.display());
                }
            }
            for (path, err) in &result.errors {
                eprintln!("  error:          {}: {}", path.display(), err);
            }
            if result.changed.is_empty() && result.errors.is_empty() {
                println!(
                    "✓ All {} file(s) are already canonical.",
                    result.unchanged.len()
                );
            } else if !args.check {
                println!(
                    "Formatted {} file(s). {} file(s) unchanged.",
                    result.changed.len(),
                    result.unchanged.len()
                );
            }
        }
    }

    if result.had_errors() {
        anyhow::bail!("fmt encountered {} error(s).", result.errors.len());
    }

    if args.check && !result.changed.is_empty() {
        anyhow::bail!(
            "{} file(s) are not in canonical format. Run `aqueduct fmt` to fix.",
            result.changed.len()
        );
    }

    Ok(())
}
