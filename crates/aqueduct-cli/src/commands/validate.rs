use aqueduct_core::{
    dag::build_dag_state,
    parser::load_migrations,
    validate::{validate_dag, validate_migration_files},
};
use clap::Args;
use std::collections::HashMap;

#[derive(Debug, Args)]
pub struct ValidateArgs {
    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text or json.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Treat warnings as errors and exit non-zero if any are present.
    #[arg(long)]
    pub strict: bool,
}

pub async fn run(args: ValidateArgs) -> anyhow::Result<()> {
    let files = load_migrations(&args.project_dir, &HashMap::new())?;

    // File-level validation.
    let mut file_result = validate_migration_files(&files);

    // DAG-level validation.
    let state = build_dag_state(&files, true)?;
    let dag_result = validate_dag(&state);

    // Merge results.
    for e in &dag_result.errors {
        file_result.errors.push(e.clone());
    }
    for w in &dag_result.warnings {
        file_result.warnings.push(w.clone());
    }

    let total_files = files.len();

    match args.format.as_str() {
        "json" => {
            let output = serde_json::json!({
                "files": total_files,
                "errors": file_result.errors,
                "warnings": file_result.warnings,
                "ok": file_result.is_ok(),
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        _ => {
            println!(
                "Validated {} migration file{}.",
                total_files,
                if total_files == 1 { "" } else { "s" }
            );
            for w in &file_result.warnings {
                println!("  warning: {}", w);
            }
            for e in &file_result.errors {
                println!("  error:   {}", e);
            }
            if file_result.is_ok() && (!args.strict || file_result.warnings.is_empty()) {
                println!("✓ All checks passed.");
            }
        }
    }

    if !file_result.is_ok() {
        anyhow::bail!(
            "Validation failed with {} error{}.",
            file_result.errors.len(),
            if file_result.errors.len() == 1 {
                ""
            } else {
                "s"
            }
        );
    }

    // In --strict mode, warnings are treated as errors.
    if args.strict && !file_result.warnings.is_empty() {
        anyhow::bail!(
            "Strict mode: validation failed with {} warning{} (treated as errors).",
            file_result.warnings.len(),
            if file_result.warnings.len() == 1 {
                ""
            } else {
                "s"
            }
        );
    }

    Ok(())
}
