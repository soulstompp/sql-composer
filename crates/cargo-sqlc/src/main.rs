//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory of `.tql` template files, composes them into final SQL
//! with dialect-specific placeholders, and writes `.sql` output files.

use clap::{Parser, ValueEnum};
use sql_composer::composer::Composer;
use sql_composer::parser;
use sql_composer::types::{Dialect, TemplateSource};
use std::path::PathBuf;

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
    /// Template directory containing .tql files.
    #[arg(long, default_value = "templates")]
    input: PathBuf,

    /// Output directory for composed .sql files.
    #[arg(long, default_value = "sql")]
    output: PathBuf,

    /// Target database dialect for placeholder syntax.
    #[arg(long, default_value = "postgres")]
    dialect: DialectArg,
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
    let input_dir = &args.input;
    let output_dir = &args.output;
    let dialect: Dialect = args.dialect.clone().into();

    if !input_dir.exists() {
        return Err(format!("Input directory does not exist: {}", input_dir.display()).into());
    }

    std::fs::create_dir_all(output_dir)?;

    let mut composer = Composer::new(dialect);
    composer.add_search_path(input_dir.to_path_buf());

    let mut count = 0;

    for entry in std::fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "tql") {
            let content = std::fs::read_to_string(&path)?;
            let template = parser::parse_template(&content, TemplateSource::File(path.clone()))?;

            let result = composer.compose(&template)?;

            let stem = path.file_stem().unwrap().to_string_lossy();
            let output_path = output_dir.join(format!("{stem}.sql"));

            std::fs::write(&output_path, &result.sql)?;

            println!("Composing {} -> {}", path.display(), output_path.display());
            count += 1;
        }
    }

    println!("Composed {count} template(s)");
    Ok(())
}
