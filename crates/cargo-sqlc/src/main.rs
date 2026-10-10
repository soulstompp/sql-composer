//! `cargo sqlc` — cargo subcommand for composing SQL templates.
//!
//! Scans a directory tree of `.sqlc` template files, composes them into final
//! SQL with dialect-specific placeholders, and writes `.sql` output files
//! mirroring the source directory structure, with `manifest.json` beside them.

use clap::{Parser, ValueEnum};
use sql_composer::composer::{Held, Reach};
use sql_composer::parser;
use sql_composer::source::{self, Flags, Manifest, Standing, MANIFEST};
use sql_composer::spellings;
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
    /// Print each composed file's templates as a tree: every template it holds, one for each
    /// path to it, with the template each slot is filled by.
    Tree(TreeArgs),
    /// Lay out a template's spellings: the other ways to write it that return the same rows,
    /// counted at each place each axis applies, then each written with every other place as
    /// written. A template the templates under --source and --search-path take in is offered no
    /// WITH query at its head; one taken in only from another tree is not seen. Join orders are
    /// laid out to be read: spell writes none, since an order can change the plan PostgreSQL
    /// makes.
    Spellings(SpellingsArgs),
    /// Write the templates under the source in the spellings picked in its spellings.toml, as
    /// `cargo fmt` writes code. A template the templates under --source and --search-path take
    /// in is written no WITH query at its head; one taken in only from another tree is not seen.
    /// Join order is never written, its pick confirmed or not, since an order can change the plan
    /// PostgreSQL makes.
    Spell(SpellArgs),
}

/// The flags that decide what a template composes to.
#[derive(Debug, clap::Args)]
struct Composition {
    /// Source directory containing .sqlc template files.
    /// Falls back to SQLC_SOURCE_DIR env var, then "sqlc".
    #[arg(long, env = "SQLC_SOURCE_DIR", default_value = "sqlc")]
    source: PathBuf,

    /// The database to compose for: its placeholders, and the words it refuses as a name.
    /// `postgres` when it is left out.
    #[arg(long)]
    dialect: Option<DialectArg>,

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

#[derive(Debug, Parser)]
struct ComposeArgs {
    #[command(flatten)]
    composition: Composition,

    /// Target directory for composed .sql files and their manifest.json.
    /// Falls back to SQLC_TARGET_DIR env var, then ".sql".
    #[arg(long, env = "SQLC_TARGET_DIR", default_value = ".sql")]
    target: PathBuf,

    /// Skip running `cargo sqlx prepare` after composing.
    #[arg(long)]
    skip_prepare: bool,

    /// Verify that composed output matches existing target files, manifest.json among them.
    /// Exits with code 1 if any files differ or are missing. A flag left out is read from the
    /// target's manifest.json, and a flag given that it records otherwise is refused.
    #[arg(long)]
    verify: bool,

    /// Respell each composed statement for PostgreSQL where its own text proves the same rows: a
    /// union of arms that each read one roster, cut to their own rows of it and left joined ON
    /// true to what they read, is read as one join, the roster read once. Every other byte is
    /// written as composed, and so is a statement that does not parse. PostgreSQL only.
    #[arg(long)]
    respell: bool,
}

#[derive(Debug, Parser)]
struct TreeArgs {
    #[command(flatten)]
    composition: Composition,

    /// The templates whose composed files to print, by their path under the source. Every
    /// template written when none is given.
    templates: Vec<PathBuf>,

    /// Print, for this template, the composed files that hold it and how many copies of it each
    /// holds.
    #[arg(
        long,
        short = 'i',
        value_name = "TEMPLATE",
        conflicts_with = "templates"
    )]
    invert: Option<PathBuf>,
}

/// Where the spelling commands read templates.
#[derive(Debug, clap::Args)]
struct Templates {
    /// Source directory containing .sqlc template files, and spellings.toml at its root.
    /// Falls back to SQLC_SOURCE_DIR env var, then "sqlc".
    #[arg(long, env = "SQLC_SOURCE_DIR", default_value = "sqlc")]
    source: PathBuf,

    /// Another directory to look for the templates a template composes in, after the source.
    /// Repeat it to search more than one.
    #[arg(long = "search-path", value_name = "DIR")]
    search_path: Vec<PathBuf>,
}

#[derive(Debug, Parser)]
struct SpellingsArgs {
    #[command(flatten)]
    templates: Templates,

    /// The template, by its path under the source.
    template: PathBuf,

    /// Write the spellings of this axis alone, by its key in spellings.toml: join_order, naming,
    /// conditions, negation or semi_join. Every axis is counted.
    #[arg(long, value_name = "KEY")]
    axis: Option<String>,
}

#[derive(Debug, Parser)]
struct SpellArgs {
    #[command(flatten)]
    templates: Templates,

    /// Report each template not written in the picks, and write nothing. Exits with code 1 if
    /// any is not.
    #[arg(long)]
    check: bool,
}

fn main() {
    let Cli::Sqlc(args) = Cli::parse();

    let ran = match args.command {
        SqlcCommand::Compose(compose_args) => run_compose(&compose_args),
        SqlcCommand::Tree(tree_args) => run_tree(&tree_args),
        SqlcCommand::Spellings(spellings_args) => {
            run_spellings(&spellings_args, &mut std::io::stdout())
        }
        SqlcCommand::Spell(spell_args) => run_spell(&spell_args, &mut std::io::stdout()),
    };
    if let Err(e) = ran {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run_compose(args: &ComposeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = &args.composition.source;
    let target_dir = &args.target;

    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    let flags = if args.verify {
        recorded_flags(args)?
    } else {
        given_flags(&args.composition, args.respell)
    };
    let mut composed = flags.clone();
    composed.respell = false;
    let mut standings = source::compose_all(source_dir, &composed)?;
    let respelled = if flags.respell {
        source::respell_all(&mut standings, flags.dialect)?
    } else {
        BTreeMap::new()
    };

    if standings.is_empty() {
        println!("No .sqlc files found in {}", source_dir.display());
        return Ok(());
    }

    let manifest = Manifest::new(&flags, &standings).to_json()?;

    if args.verify {
        for (rel_path, standing) in &standings {
            if let Standing::Skipped { .. } = standing {
                eprintln!("{}", line(source_dir, rel_path, standing));
            }
            for (error, line) in respelled_lines(source_dir, rel_path, respelled.get(rel_path)) {
                if error {
                    eprintln!("{line}");
                }
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
        for (error, line) in respelled_lines(source_dir, rel_path, respelled.get(rel_path)) {
            if error {
                eprintln!("{line}");
            } else {
                println!("{line}");
            }
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

    let mut last = summary(&standings, target_dir);
    if flags.respell {
        let unions: usize = respelled.values().map(|r| r.unions.len()).sum();
        last.push_str(&format!(", {unions} union(s) respelled"));
    }
    println!("{last}");

    if !args.skip_prepare {
        run_sqlx_prepare()?;
    }

    Ok(())
}

fn run_tree(args: &TreeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = &args.composition.source;
    if !source_dir.exists() {
        return Err(format!("Source directory does not exist: {}", source_dir.display()).into());
    }

    let flags = given_flags(&args.composition, false);
    let standings = source::compose_all(source_dir, &flags)?;
    for template in &args.templates {
        match standings.get(template) {
            Some(Standing::Written(_)) => {}
            Some(standing) => {
                return Err(format!(
                    "{}, so it has no composed file",
                    line(source_dir, template, standing)
                )
                .into())
            }
            None => {
                return Err(format!(
                    "{} is no template under {}",
                    template.display(),
                    source_dir.display()
                )
                .into())
            }
        }
    }

    let composer = source::composer(source_dir, &flags)?;
    let mut trees = Vec::new();
    for (rel_path, standing) in &standings {
        if !matches!(standing, Standing::Written(_))
            || !(args.templates.is_empty() || args.templates.contains(rel_path))
        {
            continue;
        }
        let template = parser::parse_template_file(&source_dir.join(rel_path))?;
        trees.push(composer.tree(&template)?);
    }

    let lines = match &args.invert {
        Some(template) => invert_lines(template, &trees),
        None => trees
            .iter()
            .map(tree_lines)
            .collect::<Vec<_>>()
            .join(&[String::new()][..]),
    };
    for line in lines {
        println!("{line}");
    }
    Ok(())
}

/// Reads a template another composes, under the source and then each search path.
fn resolver(templates: &Templates) -> impl Fn(&Path) -> Option<String> + '_ {
    move |path: &Path| {
        roots(templates).find_map(|root| std::fs::read_to_string(root.join(path)).ok())
    }
}

/// The source, then each search path.
fn roots(templates: &Templates) -> impl Iterator<Item = &PathBuf> {
    std::iter::once(&templates.source).chain(&templates.search_path)
}

fn run_spellings(
    args: &SpellingsArgs,
    out: &mut impl std::io::Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = &args.templates.source;
    let path = source.join(&args.template);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let only = match &args.axis {
        Some(key) => Some(
            *spellings::Axis::ALL
                .iter()
                .find(|axis| axis.key() == key)
                .ok_or_else(|| {
                    let keys: Vec<&str> = spellings::Axis::ALL.iter().map(|a| a.key()).collect();
                    format!("{key} is no axis: {}", keys.join(", "))
                })?,
        ),
        None => None,
    };
    let resolve = resolver(&args.templates);
    let trees: Vec<PathBuf> = roots(&args.templates).cloned().collect();
    let beside = spellings::Beside::of_trees(&resolve, &trees)?;
    let layout = spellings::lay_out(&text, &args.template, &beside);
    for line in spellings_lines(&path, &layout, only) {
        writeln!(out, "{line}")?;
    }
    Ok(())
}

/// The lines `cargo sqlc spellings` prints for the template at `path`: what is left as written,
/// the count of its spellings axis by axis and place by place, then each spelling.
fn spellings_lines(
    path: &Path,
    layout: &spellings::Layout,
    only: Option<spellings::Axis>,
) -> Vec<String> {
    let mut lines = Vec::new();
    let reading = &layout.reading;
    if let Some(why) = &reading.unread {
        lines.push(format!("{}: left as written: {why}", path.display()));
        return lines;
    }
    lines.push(format!(
        "{}: {} spelling(s), counted an axis at a time",
        path.display(),
        layout.count()
    ));
    for (at, why) in &reading.unparsed {
        lines.push(format!(
            "  statement {} of {} left as written, which the parser does not read: {why}",
            at + 1,
            reading.statements
        ));
    }
    for axis in &layout.axes {
        let counts: Vec<String> = axis
            .places
            .iter()
            .map(|p| p.spellings.len().to_string())
            .collect();
        let product = if counts.len() > 1 {
            format!(" = {}", counts.join(" × "))
        } else {
            String::new()
        };
        let unwritten = if axis.axis == spellings::Axis::JoinOrder {
            ", laid out to be read: cargo sqlc spell writes no join order, since an order can \
             change the plan PostgreSQL makes"
        } else {
            ""
        };
        lines.push(format!(
            "  {}: {}{product}{unwritten}",
            axis.axis.words(),
            axis.count()
        ));
        for place in &axis.places {
            let of = match place.relations {
                Some(n) => format!(
                    " of the {n}! = {} orders",
                    (1..=n as u128).product::<u128>()
                ),
                None => String::new(),
            };
            lines.push(format!("    {}: {}{of}", place.what, place.spellings.len()));
        }
    }
    for axis in &layout.axes {
        if only.is_some_and(|only| only != axis.axis) {
            continue;
        }
        for place in &axis.places {
            let n = place.spellings.len();
            for (k, spelling) in place.spellings.iter().enumerate() {
                let mut notes = Vec::new();
                if k == 0 {
                    notes.push("as written".to_string());
                }
                notes.extend(spelling.picks.iter().cloned());
                let notes = if notes.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", notes.join("; "))
                };
                lines.push(String::new());
                lines.push(format!(
                    "-- {}, {}: {} of {n}, {}{notes}",
                    axis.axis.words(),
                    place.what,
                    k + 1,
                    spelling.name
                ));
                lines.extend(spelling.text.lines().map(str::to_string));
            }
        }
    }
    lines
}

fn run_spell(
    args: &SpellArgs,
    out: &mut impl std::io::Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = &args.templates.source;
    if !source.exists() {
        return Err(format!("Source directory does not exist: {}", source.display()).into());
    }
    let picks = spellings::Picks::of_source(source)?;
    let resolve = resolver(&args.templates);
    let mut templates: Vec<PathBuf> = WalkDir::new(source)
        .into_iter()
        .map(|entry| entry.map(walkdir::DirEntry::into_path))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "sqlc"))
        .collect();
    templates.sort();

    if let Some(pick) = &picks.join_order {
        match spellings::confirm(pick, &resolve) {
            spellings::JoinRule::Confirmed { template, order } => {
                writeln!(
                    out,
                    "Join order: outermost first, which orders {} as picked: {}",
                    template.display(),
                    order.join(", ")
                )?;
                let shown = templates.iter().find_map(|path| {
                    let rel_path = path.strip_prefix(source).ok()?;
                    if rel_path == template {
                        return None;
                    }
                    let text = std::fs::read_to_string(path).ok()?;
                    let first = spellings::outermost_first(&text, &resolve)
                        .into_iter()
                        .next()?;
                    Some((rel_path.to_path_buf(), first))
                });
                match shown {
                    Some((rel_path, (written, rule))) => writeln!(
                        out,
                        "Join order: on {} it orders {} as {}",
                        rel_path.display(),
                        written.join(", "),
                        rule.join(", ")
                    )?,
                    None => writeln!(
                        out,
                        "Join order: no other template's join it orders otherwise"
                    )?,
                }
            }
            spellings::JoinRule::Parted { template, at } => {
                writeln!(out, "Join order: on {}, {at}", template.display())?
            }
            other => writeln!(out, "Join order: {other:?}")?,
        }
        writeln!(
            out,
            "Join order is not written: an order can change the plan PostgreSQL makes, and \
             cargo sqlc spellings lays the orders out to be read"
        )?;
    }

    let trees: Vec<PathBuf> = roots(&args.templates).cloned().collect();
    let beside = spellings::Beside::of_trees(&resolve, &trees)?;
    let mut differ = Vec::new();
    for path in &templates {
        let text = std::fs::read_to_string(path)?;
        let rel_path = path.strip_prefix(source).unwrap_or(path);
        let spelled = spellings::spell(&text, rel_path, &picks, &beside);
        if spelled == text {
            continue;
        }
        if args.check {
            writeln!(out, "Not in the picks: {}", path.display())?;
        } else {
            std::fs::write(path, &spelled)?;
            writeln!(out, "Spelled {}", path.display())?;
        }
        differ.push(path.clone());
    }
    if args.check {
        if differ.is_empty() {
            writeln!(
                out,
                "Spell check OK: all {} template(s) in the picks",
                templates.len()
            )?;
            return Ok(());
        }
        return Err(format!(
            "{} of {} template(s) not in the picks — run `cargo sqlc spell` to write them",
            differ.len(),
            templates.len()
        )
        .into());
    }
    writeln!(
        out,
        "Spelled {} of {} template(s) under {} in the picks",
        differ.len(),
        templates.len(),
        source.display()
    )?;
    Ok(())
}

/// A composed file's tree, drawn as `cargo tree` draws one, then a line for each template it
/// holds more than once.
fn tree_lines(tree: &Held) -> Vec<String> {
    let mut lines = vec![label(tree)];
    branches(&tree.holds, "", &mut lines);
    for (template, copies) in tree.copies() {
        if copies > 1 {
            lines.push(format!("{}: {copies} copies", template.display()));
        }
    }
    lines
}

/// `holds` drawn under a node whose own lines begin with `prefix`.
fn branches(holds: &[Held], prefix: &str, lines: &mut Vec<String>) {
    for (i, held) in holds.iter().enumerate() {
        let (branch, under) = if i + 1 == holds.len() {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };
        lines.push(format!("{prefix}{branch}{}", label(held)));
        branches(&held.holds, &format!("{prefix}{under}"), lines);
    }
}

/// A template as its tree names it: its path, then the slot it fills, a `:define`, and the name
/// it is written as.
fn label(held: &Held) -> String {
    let mut notes = Vec::new();
    match &held.reach {
        Reach::Slot(slot) => notes.push(format!("@{slot}")),
        Reach::Define => notes.push(":define".to_string()),
        _ => {}
    }
    match (&held.reach, &held.name) {
        (Reach::Head, Some(name)) => notes.push(format!("WITH {name}")),
        (_, Some(name)) => notes.push(name.clone()),
        _ => {}
    }
    if notes.is_empty() {
        held.template.display().to_string()
    } else {
        format!("{} ({})", held.template.display(), notes.join(", "))
    }
}

/// `template`, then each composed file that holds it, with how many copies of it the file
/// holds.
fn invert_lines(template: &Path, trees: &[Held]) -> Vec<String> {
    fn holds(held: &Held, template: &Path) -> bool {
        held.template == template || held.holds.iter().any(|h| holds(h, template))
    }
    let holding: Vec<&Held> = trees.iter().filter(|tree| holds(tree, template)).collect();
    let mut lines = vec![template.display().to_string()];
    for (i, tree) in holding.iter().enumerate() {
        let branch = if i + 1 == holding.len() {
            "└── "
        } else {
            "├── "
        };
        let copies = tree.copies().get(template).copied().unwrap_or(0);
        let counted = if copies == 1 { "copy" } else { "copies" };
        lines.push(format!(
            "{branch}{} ({copies} {counted})",
            tree.template.display()
        ));
    }
    lines
}

/// The flags as given, each left out at its default.
fn given_flags(args: &Composition, respell: bool) -> Flags {
    let mut flags = Flags::new(
        args.dialect
            .clone()
            .map_or(Dialect::Postgres, Dialect::from),
    );
    flags.with = args.with;
    flags.views = args.views.clone();
    flags.search_paths = args.search_path.clone();
    flags.respell = respell;
    flags
}

/// The flags `--verify` composes with: each one given, and for each left out the one the
/// target's manifest records. A flag given that the manifest records otherwise is refused, with
/// both. Without a manifest, the flags as given.
fn recorded_flags(args: &ComposeArgs) -> Result<Flags, Box<dyn std::error::Error>> {
    let path = args.target.join(MANIFEST);
    let manifest: Manifest = match std::fs::read_to_string(&path) {
        Ok(text) => Manifest::from_json(&text).map_err(|e| format!("{}: {e}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(given_flags(&args.composition, args.respell))
        }
        Err(e) => return Err(e.into()),
    };
    let at = path.display();

    let recorded: Dialect = manifest.dialect.parse()?;
    let dialect = match args.composition.dialect.clone().map(Dialect::from) {
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
    if args.composition.with && !manifest.with {
        return Err(
            format!("--with is given, and {at} records the files composed without it").into(),
        );
    }
    if let Some(given) = &args.composition.views {
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
    if !args.composition.search_path.is_empty()
        && args.composition.search_path != manifest.search_path
    {
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
            listed(&args.composition.search_path),
            listed(&manifest.search_path)
        )
        .into());
    }

    if args.respell && !manifest.respell {
        return Err(
            format!("--respell is given, and {at} records the files composed without it").into(),
        );
    }

    let mut flags = Flags::new(dialect);
    flags.with = manifest.with;
    flags.views = manifest.views;
    flags.search_paths = manifest.search_path;
    flags.respell = manifest.respell;
    Ok(flags)
}

/// The lines a template's respelling is reported with, each saying whether it goes to standard
/// error: the unions respelled, and the statements written as composed because they do not parse.
fn respelled_lines(
    source_dir: &Path,
    rel_path: &Path,
    respelled: Option<&sql_composer::respell::Respelled>,
) -> Vec<(bool, String)> {
    let Some(respelled) = respelled else {
        return vec![];
    };
    let path = format!("{}/{}", source_dir.display(), rel_path.display());
    let mut lines = Vec::new();
    if !respelled.unions.is_empty() {
        let arms: Vec<String> = respelled.unions.iter().map(|n| n.to_string()).collect();
        lines.push((
            false,
            format!(
                "Respelled {path}: {} union(s), of {} arms",
                respelled.unions.len(),
                arms.join(", ")
            ),
        ));
    }
    if !respelled.unparsed.is_empty() {
        let unparsed: Vec<String> = respelled
            .unparsed
            .iter()
            .map(|n| (n + 1).to_string())
            .collect();
        lines.push((
            true,
            format!(
                "Unparsed {path}: statement(s) {} of {}, written as composed",
                unparsed.join(", "),
                respelled.statements
            ),
        ));
    }
    lines
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

    /// `path` with its parts joined by `/`.
    fn slashed(path: &Path) -> String {
        path.components()
            .map(|part| part.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }

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
            let manifest =
                Manifest::from_json(&std::fs::read_to_string(target.join(MANIFEST)).unwrap())
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
            let absent: Vec<_> = manifest
                .files
                .keys()
                .filter(|f| !on_disk.contains(f))
                .collect();
            assert!(
                absent.is_empty(),
                "in the manifest, not on disk: {absent:?}"
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
            let SqlcCommand::Compose(compose) = args.command else {
                panic!("{value}");
            };
            assert_eq!(
                Dialect::from(compose.composition.dialect.unwrap()),
                dialect,
                "{value}"
            );
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
        let SqlcCommand::Compose(compose) = args.command else {
            panic!("{flags:?}");
        };
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
                "respell": false,
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
    fn test_respell_is_refused_for_a_dialect_other_than_postgres() {
        let (_dir, source, target) = binding();
        let args = ["--source", &source, "--target", &target, "--respell"];
        for dialect in ["mysql", "mariadb", "sqlite", "duckdb"] {
            let err = run_compose(&compose_args(
                &[&args[..], &["--dialect", dialect]].concat(),
            ))
            .unwrap_err()
            .to_string();
            assert!(err.contains("--respell") && err.contains(dialect), "{err}");
        }
    }

    #[test]
    fn test_the_manifest_records_respell_and_verify_reads_it() {
        let (_dir, source, target) = binding();
        run_compose(&compose_args(&["--source", &source, "--target", &target])).unwrap();
        let verify = ["--source", &source, "--target", &target, "--verify"];
        let err = run_compose(&compose_args(&[&verify[..], &["--respell"]].concat()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--respell"), "{err}");

        run_compose(&compose_args(&[
            "--source",
            &source,
            "--target",
            &target,
            "--respell",
        ]))
        .unwrap();
        let text = std::fs::read_to_string(Path::new(&target).join(MANIFEST)).unwrap();
        assert!(Manifest::from_json(&text).unwrap().respell, "{text}");
        run_compose(&compose_args(&verify)).unwrap();
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

    /// The brick example's tree of `template`, `--with` or not.
    fn brick_tree(template: &str, with: bool) -> Held {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let composer = source::composer(&source, &postgres(with, &[])).unwrap();
        let template = parser::parse_template_file(&source.join(template)).unwrap();
        composer.tree(&template).unwrap()
    }

    #[test]
    fn test_the_tree_draws_every_path_to_a_template_with_its_copies() {
        assert_eq!(
            tree_lines(&brick_tree("reports/shared_moulds.sqlc", false)),
            [
                "reports/shared_moulds.sqlc",
                "├── queries/technic_moulds.sqlc",
                "│   └── shared/scope_moulds.sqlc",
                "│       └── scopes/technic.sqlc (@scope)",
                "│           └── shared/theme_closure.sqlc",
                "└── queries/city_moulds.sqlc",
                "    └── shared/scope_moulds.sqlc",
                "        └── scopes/city.sqlc (@scope)",
                "            └── shared/theme_closure.sqlc",
                "shared/scope_moulds.sqlc: 2 copies",
                "shared/theme_closure.sqlc: 2 copies",
            ]
        );
        assert_eq!(
            tree_lines(&brick_tree("reports/shared_moulds.sqlc", true)),
            [
                "reports/shared_moulds.sqlc",
                "├── shared/theme_closure.sqlc (WITH shared_theme_closure)",
                "├── queries/technic_moulds.sqlc",
                "│   └── shared/scope_moulds.sqlc",
                "│       └── scopes/technic.sqlc (@scope)",
                "│           └── shared/theme_closure.sqlc (shared_theme_closure)",
                "└── queries/city_moulds.sqlc",
                "    └── shared/scope_moulds.sqlc",
                "        └── scopes/city.sqlc (@scope)",
                "            └── shared/theme_closure.sqlc (shared_theme_closure)",
                "shared/scope_moulds.sqlc: 2 copies",
            ]
        );
    }

    #[test]
    fn test_the_inverted_tree_gives_each_composed_file_its_copies() {
        let trees = [
            brick_tree("reports/shared_moulds.sqlc", false),
            brick_tree("sets/select_colored_parts.sqlc", false),
            brick_tree("scopes/city.sqlc", false),
        ];
        assert_eq!(
            invert_lines(Path::new("shared/theme_closure.sqlc"), &trees),
            [
                "shared/theme_closure.sqlc",
                "├── reports/shared_moulds.sqlc (2 copies)",
                "└── scopes/city.sqlc (1 copy)",
            ]
        );
    }

    #[test]
    fn test_the_tree_of_a_template_with_no_composed_file_is_refused() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let source = source.display().to_string();
        for template in ["shared/filtered_set_parts.sqlc", "nowhere.sqlc"] {
            let Cli::Sqlc(args) =
                Cli::try_parse_from(["cargo", "sqlc", "tree", "--source", &source, template])
                    .unwrap();
            let SqlcCommand::Tree(tree) = args.command else {
                panic!("{template}");
            };
            let err = run_tree(&tree).unwrap_err().to_string();
            assert!(err.contains(template), "{err}");
        }
    }

    #[test]
    fn test_without_that_search_path_the_relation_is_not_found() {
        let (_dir, source, _shared) = trees();
        let err = compose_all(&source, &postgres(true, &[])).unwrap_err();
        assert!(err.to_string().contains("layers/figures.sqlc"), "{err}");
    }

    /// `cargo sqlc` parsed from `args`, after `cargo sqlc`.
    fn command(args: &[&str]) -> SqlcCommand {
        let Cli::Sqlc(parsed) =
            Cli::try_parse_from(["cargo", "sqlc"].into_iter().chain(args.iter().copied())).unwrap();
        parsed.command
    }

    /// What `cargo sqlc` prints, run with `args`, and whether it was refused.
    fn printed(args: &[&str]) -> (String, Option<String>) {
        let mut out = Vec::new();
        let ran = match command(args) {
            SqlcCommand::Spellings(a) => run_spellings(&a, &mut out),
            SqlcCommand::Spell(a) => run_spell(&a, &mut out),
            other => panic!("{other:?}"),
        };
        (
            String::from_utf8(out).unwrap(),
            ran.err().map(|e| e.to_string()),
        )
    }

    #[test]
    fn spellings_counts_each_place_before_it_writes_each_spelling() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let source = source.display().to_string();
        let (out, refused) = printed(&["spellings", "--source", &source, "shared/scope_sets.sqlc"]);
        assert_eq!(refused, None);
        let lines: Vec<&str> = out.lines().collect();
        // Other templates take this one in, so it is offered no WITH query at its head.
        assert_eq!(
            &lines[..11],
            [
                &*format!(
                    "{source}/shared/scope_sets.sqlc: 108 spelling(s), counted an axis at a time"
                ),
                "  join order: 6, laid out to be read: cargo sqlc spell writes no join order, since \
                 an order can change the plan PostgreSQL makes",
                "    the join of s, sc, scope: 6 of the 3! = 6 orders",
                "  how a relation read once is named: 1",
                "  where a condition sits: 18 = 3 × 3 × 2",
                "    the condition s.year >= :bind(min_year): 3",
                "    the condition sc.theme_id = s.theme_id: 3",
                "    the condition scope.id = sc.scope_id: 2",
                "  negation: 1",
                "  a semi-join's form: 1",
                "",
            ]
        );
        let first_spelling = lines.iter().position(|l| l.starts_with("-- ")).unwrap();
        assert_eq!(first_spelling, 11, "nothing is written before the count");
        for (place, count) in [
            ("join order, the join of s, sc, scope", 6),
            (
                "where a condition sits, the condition s.year >= :bind(min_year)",
                3,
            ),
            (
                "where a condition sits, the condition sc.theme_id = s.theme_id",
                3,
            ),
            (
                "where a condition sits, the condition scope.id = sc.scope_id",
                2,
            ),
        ] {
            let written: Vec<&&str> = lines
                .iter()
                .filter(|l| l.starts_with(&format!("-- {place}: ")))
                .collect();
            assert_eq!(written.len(), count, "{place}");
            assert!(
                written[0].contains(&format!(": 1 of {count}, "))
                    && written[0].contains("(as written"),
                "{}",
                written[0]
            );
        }
        let (out, _) = printed(&[
            "spellings",
            "--source",
            &source,
            "sets/never_bought.sqlc",
            "--axis",
            "naming",
        ]);
        // Five templates take in shared/purchase_sets.sqlc, so its :compose is not written inline.
        assert!(
            out.contains("    the relation purchase_sets (a WITH query): 2\n"),
            "{out}"
        );
        assert_eq!(out.lines().filter(|l| l.starts_with("-- ")).count(), 2);
        let (_, refused) = printed(&[
            "spellings",
            "--source",
            &source,
            "shared/scope_sets.sqlc",
            "--axis",
            "order",
        ]);
        assert!(refused.unwrap().contains("order is no axis"));
    }

    #[test]
    fn spell_check_reports_and_writes_nothing_and_spell_writes_the_picks() {
        let (_dir, source) = brick();
        let src = source.display().to_string();
        let (out, refused) = printed(&["spell", "--source", &src, "--check"]);
        assert_eq!(refused, None, "{out}");
        assert!(
            out.ends_with("Spell check OK: all 51 template(s) in the picks\n"),
            "{out}"
        );

        std::fs::write(source.join("spellings.toml"), "conditions = \"where\"\n").unwrap();
        let before: Vec<(PathBuf, String)> = WalkDir::new(&source)
            .into_iter()
            .map(|e| e.unwrap().into_path())
            .filter(|p| p.is_file())
            .map(|p| (p.clone(), std::fs::read_to_string(&p).unwrap()))
            .collect();
        let (out, refused) = printed(&["spell", "--source", &src, "--check"]);
        let reported: Vec<&str> = out
            .lines()
            .filter(|l| l.starts_with("Not in the picks: "))
            .collect();
        assert!(!reported.is_empty(), "{out}");
        assert_eq!(
            refused.unwrap(),
            format!(
                "{} of 51 template(s) not in the picks — run `cargo sqlc spell` to write them",
                reported.len()
            )
        );
        for (path, text) in &before {
            assert_eq!(
                &std::fs::read_to_string(path).unwrap(),
                text,
                "--check wrote {}",
                path.display()
            );
        }

        let (out, refused) = printed(&["spell", "--source", &src]);
        assert_eq!(refused, None);
        assert!(
            out.ends_with(&format!(
                "Spelled {} of 51 template(s) under {src} in the picks\n",
                reported.len()
            )),
            "{out}"
        );
        let scope_sets = std::fs::read_to_string(source.join("shared/scope_sets.sqlc")).unwrap();
        assert!(
            scope_sets.contains("CROSS JOIN lego_themes scope\nWHERE s.year >= :bind(min_year)"),
            "{scope_sets}"
        );
        let (out, refused) = printed(&["spell", "--source", &src, "--check"]);
        assert_eq!(refused, None, "{out}");
        for (path, text) in &before {
            let rel = path.strip_prefix(&source).unwrap().display().to_string();
            if !reported.iter().any(|r| r.ends_with(&rel)) {
                assert_eq!(&std::fs::read_to_string(path).unwrap(), text, "{rel}");
            }
        }
        let composed = compose_all(&source, &postgres(false, &[])).unwrap();
        assert_eq!(composed.len(), 51);
    }

    #[test]
    fn spell_writes_no_join_order_even_with_its_pick_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("sqlc");
        std::fs::create_dir_all(&source).unwrap();
        let b = "SELECT d.filing, n.size, s.name\nFROM (SELECT x.filing, x.layer, x.size FROM demand x) d\nJOIN (SELECT y.filing, y.layer, y.size FROM nameplate y) n USING (filing, layer)\nJOIN (SELECT z.filing, z.name FROM scope z) s USING (filing)\n";
        std::fs::write(
            source.join("a.sqlc"),
            "SELECT s.id, t.id FROM s JOIN t ON t.s = s.id\n",
        )
        .unwrap();
        std::fs::write(source.join("b.sqlc"), b).unwrap();
        let src = source.display().to_string();
        let not_written = "Join order is not written: an order can change the plan PostgreSQL \
                           makes, and cargo sqlc spellings lays the orders out to be read";

        std::fs::write(
            source.join("spellings.toml"),
            "[join_order]\ntemplate = \"a.sqlc\"\norder = [\"t\", \"s\"]\n",
        )
        .unwrap();
        let (out, refused) = printed(&["spell", "--source", &src]);
        assert_eq!(refused, None);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines,
            [
                "Join order: on a.sqlc, outermost first orders it s, t: at place 1 it puts s, \
                 joined by 1 column where the pick puts t, joined by 1 column",
                not_written,
                &*format!("Spelled 0 of 2 template(s) under {src} in the picks"),
            ]
        );

        std::fs::write(
            source.join("spellings.toml"),
            "[join_order]\ntemplate = \"a.sqlc\"\norder = [\"s\", \"t\"]\n",
        )
        .unwrap();
        for check in [false, true] {
            let args = ["spell", "--source", &src, "--check"];
            let (out, refused) = printed(&args[..if check { 4 } else { 3 }]);
            assert_eq!(refused, None, "{out}");
            let lines: Vec<&str> = out.lines().collect();
            assert_eq!(
                lines,
                [
                    "Join order: outermost first, which orders a.sqlc as picked: s, t",
                    "Join order: on b.sqlc it orders d, n, s as s, d, n",
                    not_written,
                    &*if check {
                        "Spell check OK: all 2 template(s) in the picks".to_string()
                    } else {
                        format!("Spelled 0 of 2 template(s) under {src} in the picks")
                    },
                ]
            );
            assert_eq!(std::fs::read_to_string(source.join("b.sqlc")).unwrap(), b);
        }
    }
}
