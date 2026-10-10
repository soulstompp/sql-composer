//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory tree of `.sqlc` template files, composes them into final
//! SQL with dialect-specific placeholders, and writes `.sql` output files
//! mirroring the source directory structure, with `manifest.json` beside them.

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use sql_composer::source::{self, Flags, Standing};
use sql_composer::types::Dialect;
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

    /// Target directory for composed .sql files and their manifest.json.
    /// Falls back to SQLC_TARGET_DIR env var, then ".sql".
    #[arg(long, env = "SQLC_TARGET_DIR", default_value = ".sql")]
    target: PathBuf,

    /// The database to compose for: its placeholders, and the words it refuses as a name.
    /// `postgres` when it is left out.
    #[arg(long)]
    dialect: Option<DialectArg>,

    /// Skip running `cargo sqlx prepare` after composing.
    #[arg(long)]
    skip_prepare: bool,

    /// Verify that composed output matches existing target files, manifest.json among them.
    /// Exits with code 1 if any files differ or are missing. A flag left out is read from the
    /// target's manifest.json, and a flag given that it records otherwise is refused.
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

/// The file at the root of the target that records how its files were composed.
const MANIFEST: &str = "manifest.json";

/// What `manifest.json` holds: the flags the target's files were composed with, and each
/// composed file's names.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Manifest {
    /// `--dialect`, as it is named.
    dialect: String,
    /// `--with`.
    with: bool,
    /// `--views`, as it was given.
    views: Option<PathBuf>,
    /// Each `--search-path`, as it was given, in order.
    search_path: Vec<PathBuf>,
    /// Each composed file, by its path under the target.
    files: BTreeMap<String, Names>,
}

/// The names a composed file's placeholders stand for.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Names {
    /// The bind names, in placeholder order.
    bind_params: Vec<String>,
    /// Where each statement's names begin in `bind_params`, one entry per statement.
    statement_starts: Vec<usize>,
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

    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    let flags = if args.verify {
        recorded_flags(args)?
    } else {
        given_flags(args)
    };
    let standings = source::compose_all(source_dir, &flags)?;

    if standings.is_empty() {
        println!("No .sqlc files found in {}", source_dir.display());
        return Ok(());
    }

    let manifest = manifest_text(&manifest(&flags, &standings))?;

    if args.verify {
        for (rel_path, standing) in &standings {
            if let Standing::Skipped { .. } = standing {
                eprintln!("{}", line(source_dir, rel_path, standing));
            }
        }
        let mut expected = files(&standings);
        expected.insert(PathBuf::from(MANIFEST), manifest);
        return run_verify(&expected, target_dir);
    }

    // Write to a temp directory first, then swap into place.
    let parent = target_dir.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent)?;

    for (rel_path, standing) in &standings {
        if let Standing::Written(composed) = standing {
            let out_path = tmp_dir.path().join(rel_path.with_extension("sql"));
            if let Some(dir) = out_path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(&out_path, &composed.sql)?;
        }
        match standing {
            Standing::Skipped { .. } => eprintln!("{}", line(source_dir, rel_path, standing)),
            _ => println!("{}", line(source_dir, rel_path, standing)),
        }
    }
    std::fs::write(tmp_dir.path().join(MANIFEST), manifest)?;

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

/// The flags as given, each left out at its default.
fn given_flags(args: &ComposeArgs) -> Flags {
    let mut flags = Flags::new(
        args.dialect
            .clone()
            .map_or(Dialect::Postgres, Dialect::from),
    );
    flags.with = args.with;
    flags.views = args.views.clone();
    flags.search_paths = args.search_path.clone();
    flags
}

/// The flags `--verify` composes with: each one given, and for each left out the one the
/// target's manifest records. A flag given that the manifest records otherwise is refused, with
/// both. Without a manifest, the flags as given.
fn recorded_flags(args: &ComposeArgs) -> Result<Flags, Box<dyn std::error::Error>> {
    let path = args.target.join(MANIFEST);
    let manifest: Manifest = match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(given_flags(args)),
        Err(e) => return Err(e.into()),
    };
    let at = path.display();

    let recorded: Dialect = manifest.dialect.parse()?;
    let dialect = match args.dialect.clone().map(Dialect::from) {
        Some(given) if given != recorded => {
            return Err(format!(
                "--dialect {} is given, and {at} records --dialect {}",
                given.name(),
                recorded.name()
            )
            .into())
        }
        _ => recorded,
    };
    if args.with && !manifest.with {
        return Err(
            format!("--with is given, and {at} records the files composed without it").into(),
        );
    }
    if let Some(given) = &args.views {
        if manifest.views.as_ref() != Some(given) {
            let recorded = match &manifest.views {
                Some(views) => format!("--views {}", views.display()),
                None => "no --views".to_string(),
            };
            return Err(format!(
                "--views {} is given, and {at} records {recorded}",
                given.display()
            )
            .into());
        }
    }
    if !args.search_path.is_empty() && args.search_path != manifest.search_path {
        let listed = |paths: &[PathBuf]| match paths {
            [] => "no --search-path".to_string(),
            paths => paths
                .iter()
                .map(|path| format!("--search-path {}", path.display()))
                .collect::<Vec<_>>()
                .join(" "),
        };
        return Err(format!(
            "{} is given, and {at} records {}",
            listed(&args.search_path),
            listed(&manifest.search_path)
        )
        .into());
    }

    let mut flags = Flags::new(dialect);
    flags.with = manifest.with;
    flags.views = manifest.views;
    flags.search_paths = manifest.search_path;
    Ok(flags)
}

/// The manifest of a compose with `flags`: one entry for each template written.
fn manifest(flags: &Flags, standings: &BTreeMap<PathBuf, Standing>) -> Manifest {
    Manifest {
        dialect: flags.dialect.name().to_string(),
        with: flags.with,
        views: flags.views.clone(),
        search_path: flags.search_paths.clone(),
        files: standings
            .iter()
            .filter_map(|(rel_path, standing)| match standing {
                Standing::Written(composed) => Some((
                    slashed(&rel_path.with_extension("sql")),
                    Names {
                        bind_params: composed.bind_params.clone(),
                        statement_starts: composed.statement_starts.clone(),
                    },
                )),
                _ => None,
            })
            .collect(),
    }
}

/// The manifest as its file holds it.
fn manifest_text(manifest: &Manifest) -> Result<String, Box<dyn std::error::Error>> {
    Ok(serde_json::to_string_pretty(manifest)? + "\n")
}

/// A path under the target as the manifest names it, its parts joined by `/`.
fn slashed(path: &Path) -> String {
    path.components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The composed files, by their path under the target: one for each template written.
fn files(standings: &BTreeMap<PathBuf, Standing>) -> BTreeMap<PathBuf, String> {
    standings
        .iter()
        .filter_map(|(rel_path, standing)| match standing {
            Standing::Written(composed) => {
                Some((rel_path.with_extension("sql"), composed.sql.clone()))
            }
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
        other => format!("{path}: {other:?}"),
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
    use sql_composer::source::compose_all;

    /// Composing for PostgreSQL, `--with` or not, after the source in `search_paths`.
    fn postgres(with: bool, search_paths: &[PathBuf]) -> Flags {
        let mut flags = Flags::new(Dialect::Postgres);
        flags.with = with;
        flags.search_paths = search_paths.to_vec();
        flags
    }

    #[test]
    fn test_only_sqlc_files_are_composed() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("parts.sqlc"), "SELECT 1").unwrap();
        std::fs::write(source.path().join("notes"), "SELECT 2").unwrap();
        std::fs::write(source.path().join("notes.txt"), "SELECT 3").unwrap();

        let composed = compose_all(source.path(), &postgres(false, &[])).unwrap();

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
            let composed = compose_all(source.path(), &postgres(with, &[])).unwrap();
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
            let composed = compose_all(&source, &postgres(with, &[])).unwrap();
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

            let target = source.with_file_name(if with { "with" } else { "plain" });
            let (from, to) = (source.display().to_string(), target.display().to_string());
            let flags = ["--source", &from, "--target", &to];
            run_compose(&compose_args(
                &[&flags[..], if with { &["--with"] } else { &[] }].concat(),
            ))
            .unwrap();
            let manifest: Manifest =
                serde_json::from_str(&std::fs::read_to_string(target.join(MANIFEST)).unwrap())
                    .unwrap();
            let listed = |file: &String| manifest.files.keys().filter(|key| *key == file).count();
            let on_disk: Vec<String> = WalkDir::new(&target)
                .into_iter()
                .map(|entry| entry.unwrap().into_path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
                .map(|path| slashed(path.strip_prefix(&target).unwrap()))
                .collect();
            let written: Vec<String> = composed
                .iter()
                .filter(|(_, standing)| matches!(standing, Standing::Written(_)))
                .map(|(rel_path, _)| slashed(&rel_path.with_extension("sql")))
                .collect();
            let unlisted: Vec<_> = written.iter().filter(|f| listed(f) == 0).collect();
            let doubled: Vec<_> = written.iter().filter(|f| listed(f) > 1).collect();
            let unwritten: Vec<_> = manifest
                .files
                .keys()
                .filter(|f| !written.contains(f))
                .collect();
            assert!(
                unlisted.is_empty(),
                "written, not in the manifest: {unlisted:?}"
            );
            assert!(doubled.is_empty(), "in the manifest twice: {doubled:?}");
            assert!(
                unwritten.is_empty(),
                "in the manifest, not written: {unwritten:?}"
            );
            assert_eq!(on_disk.len(), manifest.files.len());
            assert!(on_disk.iter().all(|f| listed(f) == 1), "{on_disk:?}");
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
            assert_eq!(Dialect::from(compose.dialect.unwrap()), dialect, "{value}");
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
        let composed = compose_all(&source, &postgres(true, &[shared])).unwrap();
        let keys: Vec<_> = composed.keys().cloned().collect();
        assert_eq!(keys, vec![PathBuf::from("walk/step.sqlc")]);
        let Standing::Written(written) = &composed[&PathBuf::from("walk/step.sqlc")] else {
            panic!("{composed:?}");
        };
        let sql = &written.sql;
        assert!(sql.contains("WITH layers_figures AS ("), "{sql}");
        assert!(sql.contains("SELECT * FROM layers_figures"), "{sql}");
    }

    /// `cargo sqlc compose` parsed from `flags`.
    fn compose_args(flags: &[&str]) -> ComposeArgs {
        let args = ["cargo", "sqlc", "compose", "--skip-prepare"]
            .into_iter()
            .chain(flags.iter().copied());
        let Cli::Sqlc(args) = Cli::try_parse_from(args).unwrap();
        let SqlcCommand::Compose(compose) = args.command;
        compose
    }

    /// A source of two templates, one a script of two statements that bind, and a target
    /// beside it.
    fn binding() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("sqlc");
        std::fs::create_dir_all(source.join("sets")).unwrap();
        std::fs::write(
            source.join("sets/by_year.sqlc"),
            "SELECT set_num FROM sets WHERE year = :bind(year) AND theme_id = :bind(theme_id);\n\
             SELECT name FROM themes WHERE id = :bind(theme_id);\n",
        )
        .unwrap();
        std::fs::write(source.join("themes.sqlc"), "SELECT id, name FROM themes\n").unwrap();
        let target = dir.path().join(".sql");
        let (source, target) = (source.display().to_string(), target.display().to_string());
        (dir, source, target)
    }

    #[test]
    fn test_the_manifest_holds_each_composed_files_names_and_its_flags() {
        let (_dir, source, target) = binding();
        let args = compose_args(&["--source", &source, "--target", &target]);
        run_compose(&args).unwrap();
        let text = std::fs::read_to_string(Path::new(&target).join("manifest.json")).unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            manifest,
            serde_json::json!({
                "dialect": "postgres",
                "with": false,
                "views": null,
                "search_path": [],
                "files": {
                    "sets/by_year.sql": {
                        "bind_params": ["theme_id", "year", "theme_id"],
                        "statement_starts": [0, 2]
                    },
                    "themes.sql": { "bind_params": [], "statement_starts": [0] }
                }
            })
        );

        let args = compose_args(&[
            "--source",
            &source,
            "--target",
            &target,
            "--dialect",
            "mysql",
        ]);
        run_compose(&args).unwrap();
        let text = std::fs::read_to_string(Path::new(&target).join("manifest.json")).unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(manifest["dialect"], "mysql");
        assert_eq!(
            manifest["files"]["sets/by_year.sql"]["bind_params"],
            serde_json::json!(["year", "theme_id", "theme_id"])
        );
    }

    #[test]
    fn test_verify_reads_the_flags_from_the_manifest() {
        let (_dir, source, target) = binding();
        let composed = [
            "--source",
            &source,
            "--target",
            &target,
            "--dialect",
            "mysql",
            "--with",
        ];
        run_compose(&compose_args(&composed)).unwrap();

        let verify = compose_args(&["--source", &source, "--target", &target, "--verify"]);
        run_compose(&verify).unwrap();
        let verify = compose_args(&[
            "--source",
            &source,
            "--target",
            &target,
            "--verify",
            "--dialect",
            "mysql",
            "--with",
        ]);
        run_compose(&verify).unwrap();
    }

    #[test]
    fn test_verify_refuses_a_flag_the_manifest_disagrees_with() {
        let (_dir, source, target) = binding();
        let composed = [
            "--source",
            &source,
            "--target",
            &target,
            "--dialect",
            "mysql",
        ];
        run_compose(&compose_args(&composed)).unwrap();

        let verify = ["--source", &source, "--target", &target, "--verify"];
        let err = run_compose(&compose_args(
            &[&verify[..], &["--dialect", "sqlite"]].concat(),
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("sqlite") && err.contains("mysql"), "{err}");
        let err = run_compose(&compose_args(&[&verify[..], &["--with"]].concat()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--with"), "{err}");
        let err = run_compose(&compose_args(
            &[&verify[..], &["--search-path", "elsewhere"]].concat(),
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("elsewhere"), "{err}");
    }

    #[test]
    fn test_verify_compares_the_manifest() {
        let (_dir, source, target) = binding();
        run_compose(&compose_args(&["--source", &source, "--target", &target])).unwrap();
        let verify = compose_args(&["--source", &source, "--target", &target, "--verify"]);
        run_compose(&verify).unwrap();

        let path = Path::new(&target).join("manifest.json");
        let text = std::fs::read_to_string(&path).unwrap();
        let changed = text.replace("\"year\"", "\"years\"");
        assert_ne!(changed, text);
        std::fs::write(&path, changed).unwrap();
        assert!(run_compose(&verify).is_err());

        std::fs::remove_file(&path).unwrap();
        assert!(run_compose(&verify).is_err());
    }

    #[test]
    fn test_without_that_search_path_the_relation_is_not_found() {
        let (_dir, source, _shared) = trees();
        let err = compose_all(&source, &postgres(true, &[])).unwrap_err();
        assert!(err.to_string().contains("layers/figures.sqlc"), "{err}");
    }
}
