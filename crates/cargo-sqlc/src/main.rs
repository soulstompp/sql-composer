//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory tree of `.sqlc` template files, composes them into final
//! SQL with dialect-specific placeholders, and writes `.sql` output files
//! mirroring the source directory structure.

use clap::{Parser, ValueEnum};
use sql_composer::composer::Composer;
use sql_composer::error::Error as ComposeError;
use sql_composer::parser;
use sql_composer::types::{Dialect, TemplateSource};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

#[derive(Debug, Clone, ValueEnum)]
enum DialectArg {
    Postgres,
    Mysql,
    Mariadb,
    Sqlite,
    Duckdb,
}

impl From<DialectArg> for Dialect {
    fn from(d: DialectArg) -> Self {
        match d {
            DialectArg::Postgres => Dialect::Postgres,
            DialectArg::Mysql => Dialect::Mysql,
            DialectArg::Mariadb => Dialect::Mariadb,
            DialectArg::Sqlite => Dialect::Sqlite,
            DialectArg::Duckdb => Dialect::Duckdb,
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
    /// Falls back to SQLC_TARGET_DIR env var, then ".sql".
    #[arg(long, env = "SQLC_TARGET_DIR", default_value = ".sql")]
    target: PathBuf,

    /// The database to compose for: its placeholders, and the words it refuses as a name.
    #[arg(long, default_value = "postgres")]
    dialect: DialectArg,

    /// Skip running `cargo sqlx prepare` after composing.
    #[arg(long)]
    skip_prepare: bool,

    /// Verify that composed output matches existing target files.
    /// Exits with code 1 if any files differ or are missing.
    #[arg(long)]
    verify: bool,

    /// A view registry: every `CREATE VIEW <name> AS :define(<path>)` in this template makes
    /// the composed SQL say `SELECT * FROM <name>` wherever `<path>` is composed, instead of
    /// inlining it. `:define` is what puts a body where the name would otherwise go, so the
    /// registry needs no exemption of its own, and a statement can give a relation a name for
    /// its own duration by defining it in a `WITH` clause. Without this, everything inlines.
    #[arg(long, conflicts_with = "with")]
    views: Option<PathBuf>,

    /// Write every relation a statement reads once, named, at the head of that statement: one
    /// `WITH`, each relation after the relations it reads, the statement last. A relation is
    /// named after its file and directories (`layers/figures.sqlc` is `layers_figures`). A
    /// template that binds a parameter, or is composed with its slots filled, is written where
    /// it is composed.
    #[arg(long)]
    with: bool,

    /// Another directory to look for templates in, after the source directory. A template under
    /// the source can compose one that lives only here, named after its path here, and nothing is
    /// composed for the templates here themselves. Repeat it to search more than one.
    #[arg(long = "search-path", value_name = "DIR")]
    search_path: Vec<PathBuf>,
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

/// Collect all `.sqlc` files under `source_dir` recursively and return them
/// as a sorted map of relative path (with `.sql` extension) → composed SQL.
fn compose_all(
    source_dir: &Path,
    search_paths: &[PathBuf],
    dialect: Dialect,
    views: Option<&Path>,
    with: bool,
) -> Result<BTreeMap<PathBuf, String>, Box<dyn std::error::Error>> {
    let mut composer = Composer::new(dialect);
    composer.add_search_path(source_dir.to_path_buf());
    for path in search_paths {
        composer.add_search_path(path.clone());
    }
    composer.name_relations = with;
    if let Some(registry) = views {
        composer.load_view_registry(registry)?;
    }

    let mut results = BTreeMap::new();

    for entry in WalkDir::new(source_dir) {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() || !path.extension().is_some_and(|ext| ext == "sqlc") {
            continue;
        }

        let rel_path = path
            .strip_prefix(source_dir)
            .expect("walkdir entry must be under source_dir");

        let output_rel = rel_path.with_extension("sql");

        let content = std::fs::read_to_string(path)?;
        let template = parser::parse_template(&content, TemplateSource::File(path.to_path_buf()))?;
        let result = match composer.compose(&template) {
            Ok(r) => r,
            Err(ComposeError::MissingSlot { .. }) => {
                // Template has unfilled slots — it's a shared template meant
                // to be composed by callers, not standalone. Skip it.
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        results.insert(output_rel, result.sql);
    }

    Ok(results)
}

fn run_compose(args: &ComposeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = &args.source;
    let target_dir = &args.target;
    let dialect: Dialect = args.dialect.clone().into();

    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    let composed = compose_all(
        source_dir,
        &args.search_path,
        dialect,
        args.views.as_deref(),
        args.with,
    )?;

    if composed.is_empty() {
        println!("No .sqlc files found in {}", source_dir.display());
        return Ok(());
    }

    if args.verify {
        return run_verify(&composed, target_dir);
    }

    // Write to a temp directory first, then swap into place.
    let parent = target_dir.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent)?;

    for (rel_path, sql) in &composed {
        let out_path = tmp_dir.path().join(rel_path);

        if let Some(dir) = out_path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        std::fs::write(&out_path, sql)?;
        println!(
            "Composed {}/{}",
            source_dir.display(),
            rel_path.with_extension("sqlc").display()
        );
    }

    // All writes succeeded — swap into place.
    if target_dir.exists() {
        std::fs::remove_dir_all(target_dir)?;
    }

    // keep() prevents the temp dir from being cleaned up, then rename it.
    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, target_dir)?;

    println!(
        "Composed {} template(s) into {}",
        composed.len(),
        target_dir.display()
    );

    if !args.skip_prepare {
        run_sqlx_prepare()?;
    }

    Ok(())
}

fn run_verify(
    composed: &BTreeMap<PathBuf, String>,
    target_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut mismatches = Vec::new();

    // Check each composed file against the existing target.
    for (rel_path, expected_sql) in composed {
        let target_path = target_dir.join(rel_path);

        match std::fs::read_to_string(&target_path) {
            Ok(existing) if existing == *expected_sql => {}
            Ok(existing) => {
                mismatches.push(format!("CHANGED: {}", rel_path.display()));
                print_diff(rel_path, &existing, expected_sql);
            }
            Err(_) => {
                mismatches.push(format!("MISSING: {}", rel_path.display()));
            }
        }
    }

    // Check for stale files in the target that have no corresponding source.
    if target_dir.exists() {
        for entry in WalkDir::new(target_dir) {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() || !path.extension().is_some_and(|ext| ext == "sql") {
                continue;
            }

            let rel_path = path
                .strip_prefix(target_dir)
                .expect("walkdir entry must be under target_dir");

            if !composed.contains_key(rel_path) {
                mismatches.push(format!("STALE: {}", rel_path.display()));
            }
        }
    }

    if mismatches.is_empty() {
        println!("Verify OK: all {} file(s) match", composed.len());
        Ok(())
    } else {
        eprintln!("Verify failed:");
        for m in &mismatches {
            eprintln!("  {m}");
        }
        Err(format!(
            "{} file(s) out of sync — run `cargo sqlc compose` to update",
            mismatches.len()
        )
        .into())
    }
}

/// Print a simple line-by-line diff between existing and expected content.
fn print_diff(rel_path: &Path, existing: &str, expected: &str) {
    eprintln!("--- {} (target)", rel_path.display());
    eprintln!("+++ {} (composed)", rel_path.display());

    let existing_lines: Vec<&str> = existing.lines().collect();
    let expected_lines: Vec<&str> = expected.lines().collect();

    let max = existing_lines.len().max(expected_lines.len());
    for i in 0..max {
        let old = existing_lines.get(i).copied().unwrap_or("");
        let new = expected_lines.get(i).copied().unwrap_or("");
        if old != new {
            if !old.is_empty() {
                eprintln!("- {old}");
            }
            if !new.is_empty() {
                eprintln!("+ {new}");
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_only_sqlc_files_are_composed() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("parts.sqlc"), "SELECT 1").unwrap();
        std::fs::write(source.path().join("notes"), "SELECT 2").unwrap();
        std::fs::write(source.path().join("notes.txt"), "SELECT 3").unwrap();

        let composed = compose_all(source.path(), &[], Dialect::Postgres, None, false).unwrap();

        assert_eq!(
            composed.keys().collect::<Vec<_>>(),
            vec![Path::new("parts.sql")]
        );
    }

    #[test]
    fn test_each_dialect_is_a_value_of_the_flag() {
        for (value, dialect) in [
            ("postgres", Dialect::Postgres),
            ("mysql", Dialect::Mysql),
            ("mariadb", Dialect::Mariadb),
            ("sqlite", Dialect::Sqlite),
            ("duckdb", Dialect::Duckdb),
        ] {
            let Cli::Sqlc(args) =
                Cli::try_parse_from(["cargo", "sqlc", "compose", "--dialect", value]).unwrap();
            let SqlcCommand::Compose(compose) = args.command;
            assert_eq!(Dialect::from(compose.dialect), dialect, "{value}");
        }
    }

    #[test]
    fn test_verify_reads_only_sql_files_in_the_target() {
        let target = tempfile::tempdir().unwrap();
        std::fs::write(target.path().join("parts.sql"), "SELECT 1").unwrap();
        std::fs::write(
            target.path().join("README"),
            "kept beside the composed files",
        )
        .unwrap();
        let composed = BTreeMap::from([(PathBuf::from("parts.sql"), "SELECT 1".to_string())]);

        assert!(run_verify(&composed, target.path()).is_ok());

        std::fs::write(target.path().join("old.sql"), "SELECT 2").unwrap();
        assert!(run_verify(&composed, target.path()).is_err());
    }

    /// A source whose one template composes a relation that lives only in another directory.
    fn trees() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let shared = dir.path().join("shared");
        std::fs::create_dir_all(source.join("walk")).unwrap();
        std::fs::create_dir_all(shared.join("layers")).unwrap();
        std::fs::write(
            source.join("walk/step.sqlc"),
            "SELECT layer AS camada FROM (\n    :compose(layers/figures.sqlc)\n) f;\n",
        )
        .unwrap();
        std::fs::write(
            shared.join("layers/figures.sqlc"),
            "SELECT layer FROM layer\n",
        )
        .unwrap();
        (dir, source, shared)
    }

    #[test]
    fn test_a_template_composes_a_relation_found_on_another_search_path() {
        let (_dir, source, shared) = trees();
        let composed = compose_all(&source, &[shared], Dialect::Postgres, None, true).unwrap();
        let keys: Vec<_> = composed.keys().cloned().collect();
        assert_eq!(keys, vec![PathBuf::from("walk/step.sql")]);
        let sql = &composed[&PathBuf::from("walk/step.sql")];
        assert!(sql.contains("WITH layers_figures AS ("), "{sql}");
        assert!(sql.contains("SELECT * FROM layers_figures"), "{sql}");
    }

    #[test]
    fn test_without_that_search_path_the_relation_is_not_found() {
        let (_dir, source, _shared) = trees();
        let err = compose_all(&source, &[], Dialect::Postgres, None, true).unwrap_err();
        assert!(err.to_string().contains("layers/figures.sqlc"), "{err}");
    }
}
