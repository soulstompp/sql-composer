//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory tree of `.sqlc` template files, composes them into final
//! SQL with dialect-specific placeholders, and writes `.sql` output files
//! mirroring the source directory structure.

use clap::{Parser, ValueEnum};
use sql_composer::composer::Composer;
use sql_composer::error::Error as ComposeError;
use sql_composer::parser;
use sql_composer::types::{ComposeRef, ComposeTarget, Dialect, Element, Template, TemplateSource};
use std::collections::{BTreeMap, BTreeSet};
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

/// What `cargo sqlc compose` makes of one template under the source.
#[derive(Debug, PartialEq)]
enum Standing {
    /// Composed: the SQL its file holds.
    Written(String),
    /// A shape: the slots of its own a caller fills. It is composed only inside that caller.
    Shape(Vec<String>),
    /// A caller that leaves a slot open: the slot, and the template it stands in, as `:compose`
    /// names it.
    Skipped { slot: String, template: String },
}

/// Every `.sqlc` file under `source_dir`, at any depth, by its path under `source_dir`, with
/// what composing it makes of it.
fn compose_all(
    source_dir: &Path,
    search_paths: &[PathBuf],
    dialect: Dialect,
    views: Option<&Path>,
    with: bool,
) -> Result<BTreeMap<PathBuf, Standing>, Box<dyn std::error::Error>> {
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

        let content = std::fs::read_to_string(path)?;
        let template = parser::parse_template(&content, TemplateSource::File(path.to_path_buf()))?;
        let standing = match composer.compose(&template) {
            Ok(r) => Standing::Written(r.sql),
            Err(ComposeError::MissingSlot {
                name,
                template: within,
            }) => {
                let own = open_slots(&template);
                if own.is_empty() {
                    let roots: Vec<&Path> = std::iter::once(source_dir)
                        .chain(search_paths.iter().map(PathBuf::as_path))
                        .collect();
                    Standing::Skipped {
                        slot: name,
                        template: as_composed(Path::new(&within), &roots),
                    }
                } else {
                    Standing::Shape(own)
                }
            }
            Err(e) => return Err(e.into()),
        };

        results.insert(rel_path.to_path_buf(), standing);
    }

    Ok(results)
}

/// The slots a template leaves for its caller to fill, each named once, in order.
fn open_slots(template: &Template) -> Vec<String> {
    let mut slots = BTreeSet::new();
    for element in &template.elements {
        if let Element::Compose(ComposeRef {
            target: ComposeTarget::Slot(name),
            ..
        }) = element
        {
            slots.insert(name.clone());
        }
    }
    slots.into_iter().collect()
}

/// `template` as a `:compose` names it: its path under the first of `roots` that holds it.
fn as_composed(template: &Path, roots: &[&Path]) -> String {
    let resolved = std::fs::canonicalize(template).unwrap_or_else(|_| template.to_path_buf());
    roots
        .iter()
        .filter_map(|root| std::fs::canonicalize(root).ok())
        .find_map(|root| resolved.strip_prefix(&root).ok().map(Path::to_path_buf))
        .unwrap_or_else(|| template.to_path_buf())
        .display()
        .to_string()
}

fn run_compose(args: &ComposeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = &args.source;
    let target_dir = &args.target;
    let dialect: Dialect = args.dialect.clone().into();

    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    let standings = compose_all(
        source_dir,
        &args.search_path,
        dialect,
        args.views.as_deref(),
        args.with,
    )?;

    if standings.is_empty() {
        println!("No .sqlc files found in {}", source_dir.display());
        return Ok(());
    }

    if args.verify {
        for (rel_path, standing) in &standings {
            if let Standing::Skipped { .. } = standing {
                eprintln!("{}", line(source_dir, rel_path, standing));
            }
        }
        return run_verify(&files(&standings), target_dir);
    }

    // Write to a temp directory first, then swap into place.
    let parent = target_dir.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent)?;

    for (rel_path, standing) in &standings {
        if let Standing::Written(sql) = standing {
            let out_path = tmp_dir.path().join(rel_path.with_extension("sql"));
            if let Some(dir) = out_path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(&out_path, sql)?;
        }
        match standing {
            Standing::Skipped { .. } => eprintln!("{}", line(source_dir, rel_path, standing)),
            _ => println!("{}", line(source_dir, rel_path, standing)),
        }
    }

    // All writes succeeded — swap into place.
    if target_dir.exists() {
        std::fs::remove_dir_all(target_dir)?;
    }

    // keep() prevents the temp dir from being cleaned up, then rename it.
    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, target_dir)?;

    println!("{}", summary(&standings, target_dir));

    if !args.skip_prepare {
        run_sqlx_prepare()?;
    }

    Ok(())
}

/// The composed files, by their path under the target: one for each template written.
fn files(standings: &BTreeMap<PathBuf, Standing>) -> BTreeMap<PathBuf, String> {
    standings
        .iter()
        .filter_map(|(rel_path, standing)| match standing {
            Standing::Written(sql) => Some((rel_path.with_extension("sql"), sql.clone())),
            _ => None,
        })
        .collect()
}

/// The line a template under `source_dir` is reported with.
fn line(source_dir: &Path, rel_path: &Path, standing: &Standing) -> String {
    let path = format!("{}/{}", source_dir.display(), rel_path.display());
    match standing {
        Standing::Written(_) => format!("Composed {path}"),
        Standing::Shape(slots) => {
            let slots: Vec<String> = slots.iter().map(|slot| format!("@{slot}")).collect();
            format!("Shape {path}: {} open", slots.join(", "))
        }
        Standing::Skipped { slot, template } => {
            format!("Skipped {path}: @{slot} of {template} left open")
        }
    }
}

/// The line a compose ends with: how many templates were written, of how many, and what the
/// others are.
fn summary(standings: &BTreeMap<PathBuf, Standing>, target_dir: &Path) -> String {
    let count = |of: fn(&Standing) -> bool| standings.values().filter(|s| of(s)).count();
    format!(
        "Composed {} of {} template(s) into {}: {} shape(s), {} skipped",
        count(|s| matches!(s, Standing::Written(_))),
        standings.len(),
        target_dir.display(),
        count(|s| matches!(s, Standing::Shape(_))),
        count(|s| matches!(s, Standing::Skipped { .. })),
    )
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
            vec![Path::new("parts.sqlc")]
        );
    }

    #[test]
    fn test_each_template_takes_one_standing() {
        let source = tempfile::tempdir().unwrap();
        let write = |name: &str, text: &str| {
            let path = source.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write("parts.sqlc", "SELECT part FROM parts");
        write(
            "shared/shape.sqlc",
            "SELECT * FROM (\n:compose(@rows)\n) r JOIN (\n:compose(@keys)\n) k USING (part)",
        );
        write(
            "filled.sqlc",
            ":compose(shared/shape.sqlc, @rows = parts.sqlc, @keys = parts.sqlc)",
        );
        write(
            "caller.sqlc",
            ":compose(shared/shape.sqlc, @rows = parts.sqlc)",
        );

        for with in [false, true] {
            let composed = compose_all(source.path(), &[], Dialect::Postgres, None, with).unwrap();
            let standing = |name: &str| &composed[Path::new(name)];
            assert!(matches!(standing("parts.sqlc"), Standing::Written(_)));
            assert!(matches!(standing("filled.sqlc"), Standing::Written(_)));
            assert_eq!(
                standing("shared/shape.sqlc"),
                &Standing::Shape(vec!["keys".into(), "rows".into()])
            );
            assert_eq!(
                standing("caller.sqlc"),
                &Standing::Skipped {
                    slot: "keys".into(),
                    template: "shared/shape.sqlc".into()
                },
                "with: {with}"
            );
            assert_eq!(composed.len(), 4);
        }
    }

    /// The brick example's templates, copied into a directory of their own.
    fn brick() -> (tempfile::TempDir, PathBuf) {
        let from = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("sqlc");
        for entry in WalkDir::new(&from) {
            let entry = entry.unwrap();
            let to = source.join(entry.path().strip_prefix(&from).unwrap());
            if entry.file_type().is_dir() {
                std::fs::create_dir_all(&to).unwrap();
            } else {
                std::fs::copy(entry.path(), &to).unwrap();
            }
        }
        (dir, source)
    }

    #[test]
    fn test_every_template_under_the_source_is_reported_once() {
        let (_dir, source) = brick();
        let caller = source.join("sets/select_colored_parts.sqlc");
        let text = std::fs::read_to_string(&caller).unwrap();
        let unfilled = text.replace(", @filter = filters/by_color.sqlc", "");
        assert_ne!(unfilled, text);
        std::fs::write(&caller, unfilled).unwrap();

        let templates: Vec<String> = WalkDir::new(&source)
            .into_iter()
            .map(|entry| entry.unwrap().into_path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "sqlc"))
            .map(|path| path.display().to_string())
            .collect();
        for with in [false, true] {
            let composed = compose_all(&source, &[], Dialect::Postgres, None, with).unwrap();
            let lines: Vec<String> = composed
                .iter()
                .map(|(rel_path, standing)| line(&source, rel_path, standing))
                .collect();
            let reported = |template: &String| {
                lines
                    .iter()
                    .filter(|line| {
                        let path = line.split(' ').nth(1).unwrap_or("");
                        path.trim_end_matches(':') == template
                    })
                    .count()
            };
            let unclassified: Vec<_> = templates.iter().filter(|t| reported(t) == 0).collect();
            let doubled: Vec<_> = templates.iter().filter(|t| reported(t) > 1).collect();
            assert!(unclassified.is_empty(), "unclassified: {unclassified:?}");
            assert!(doubled.is_empty(), "doubled: {doubled:?}");
            assert_eq!(lines.len(), templates.len());
            assert_eq!(
                composed[Path::new("sets/select_colored_parts.sqlc")],
                Standing::Skipped {
                    slot: "filter".into(),
                    template: "shared/filtered_set_parts.sqlc".into()
                }
            );
        }
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
        assert_eq!(keys, vec![PathBuf::from("walk/step.sqlc")]);
        let Standing::Written(sql) = &composed[&PathBuf::from("walk/step.sqlc")] else {
            panic!("{composed:?}");
        };
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
