//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory of `.sqlc` template files, composes them into final SQL
//! with dialect-specific placeholders, writes `.sql` output files, and
//! runs `cargo sqlx prepare` to keep the query cache up to date.

use clap::{Parser, ValueEnum};
use sql_composer::composer::Composer;
use sql_composer::parser;
use sql_composer::types::{Dialect, TemplateSource};
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Clone, ValueEnum)]
enum DialectArg {
    Postgres,
    Mysql,
    Sqlite,
}

impl From<DialectArg> for Dialect {
    fn from(d: DialectArg) -> Self {
        match d {
            DialectArg::Postgres => Dialect::Postgres,
            DialectArg::Mysql => Dialect::Mysql,
            DialectArg::Sqlite => Dialect::Sqlite,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "cargo-sqlc",
    bin_name = "cargo",
    about = "SQL template composer"
)]
enum Cli {
    /// Compose SQL templates into final SQL files.
    Sqlc(SqlcArgs),
}

#[derive(Debug, Parser)]
struct SqlcArgs {
    #[command(subcommand)]
    command: SqlcCommand,
}

#[derive(Debug, Parser)]
enum SqlcCommand {
    /// Compose template files into output SQL files.
    Compose(ComposeArgs),
}

#[derive(Debug, Parser)]
struct ComposeArgs {
    /// Source directory containing .sqlc template files.
    /// Falls back to SQLC_SOURCE_DIR env var, then "sqlc".
    #[arg(long, env = "SQLC_SOURCE_DIR", default_value = "sqlc")]
    source: PathBuf,

    /// Target directory for composed .sql files.
    /// Falls back to SQLC_TARGET_DIR env var, then "sql".
    #[arg(long, env = "SQLC_TARGET_DIR", default_value = "sql")]
    target: PathBuf,

    /// Target database dialect for placeholder syntax.
    #[arg(long, default_value = "postgres")]
    dialect: DialectArg,

    /// Skip running `cargo sqlx prepare` after composing.
    #[arg(long)]
    skip_prepare: bool,
}

fn main() {
    let Cli::Sqlc(args) = Cli::parse();

    match args.command {
        SqlcCommand::Compose(compose_args) => {
            if let Err(e) = run_compose(&compose_args) {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn run_compose(args: &ComposeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = &args.source;
    let target_dir = &args.target;
    let dialect: Dialect = args.dialect.clone().into();

    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    std::fs::create_dir_all(target_dir)?;

    let mut composer = Composer::new(dialect);
    composer.add_search_path(source_dir.to_path_buf());

    let mut count = 0;

    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sqlc") {
            let content = std::fs::read_to_string(&path)?;
            let template = parser::parse_template(&content, TemplateSource::File(path.clone()))?;

            let result = composer.compose(&template)?;

            let stem = path.file_stem().unwrap().to_string_lossy();
            let output_path = target_dir.join(format!("{stem}.sql"));

            std::fs::write(&output_path, &result.sql)?;

            println!("Composing {} -> {}", path.display(), output_path.display());
            count += 1;
        }
    }

    println!("Composed {count} template(s)");

    if !args.skip_prepare {
        run_sqlx_prepare()?;
    }

    Ok(())
}

fn run_sqlx_prepare() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running cargo sqlx prepare...");

    let status = Command::new("cargo").args(["sqlx", "prepare"]).status()?;

    if !status.success() {
        return Err(format!("cargo sqlx prepare failed with {status}").into());
    }

    println!("cargo sqlx prepare succeeded");
    Ok(())
}
