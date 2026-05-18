use aqueduct_core::{dag::build_dag_state, lint::lint_migrations, parser::load_migrations};
use clap::Args;
use std::collections::HashMap;

#[derive(Debug, Args)]
pub struct LintArgs {
    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text (default) or json.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Exit non-zero if any warnings are found (in addition to errors).
    #[arg(long)]
    pub fail_on_warn: bool,
}

pub async fn run(args: LintArgs) -> anyhow::Result<()> {
    let files = load_migrations(&args.project_dir, &HashMap::new())?;
    let state = build_dag_state(&files, true)?;

    let result = lint_migrations(&files, &state);

    match args.format.as_str() {
        "json" => {
            let diags: Vec<_> = result
                .diagnostics
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "rule": d.rule,
                        "level": d.level.to_string(),
                        "message": d.message,
                        "file": d.file.as_ref().map(|p| p.display().to_string()),
                    })
                })
                .collect();
            let output = serde_json::json!({
                "files": files.len(),
                "diagnostics": diags,
                "warnings": result.warnings().count(),
                "errors": result.errors().count(),
                "ok": !result.has_errors(),
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        _ => {
            let total_files = files.len();
            println!(
                "Linting {} migration file{}...",
                total_files,
                if total_files == 1 { "" } else { "s" }
            );

            for diag in &result.diagnostics {
                let file_str = diag
                    .file
                    .as_ref()
                    .map(|p| format!("{}:", p.display()))
                    .unwrap_or_default();
                println!(
                    "  [{}] {}{} ({})",
                    diag.level, file_str, diag.message, diag.rule
                );
            }

            if result.is_clean() {
                println!("✓ No lint issues found.");
            } else {
                let warn_count = result.warnings().count();
                let err_count = result.errors().count();
                if err_count > 0 {
                    println!("{} error(s), {} warning(s).", err_count, warn_count);
                } else {
                    println!("{} warning(s).", warn_count);
                }
            }
        }
    }

    if result.has_errors() {
        anyhow::bail!("Lint found {} error(s).", result.errors().count());
    }

    if args.fail_on_warn && result.warnings().count() > 0 {
        anyhow::bail!(
            "Lint found {} warning(s) (--fail-on-warn is set).",
            result.warnings().count()
        );
    }

    Ok(())
}
