//! The fit's laws: what `cargo sqlc tree` says each composed file holds, held to the plan
//! PostgreSQL makes for it. Every statement is planned by `EXPLAIN (COSTS OFF)` and never run.
//!
//! 1. Each composed file, plain and with `--with`, scans each table as often as its text writes
//!    it: each template the tree holds reads the tables its own text names, once for every copy
//!    of it the tree counts. Where the plan scans a table more or less often, the difference is
//!    named by what the planner did, and a difference nothing names fails the law.
//! 2. Every template under the source takes exactly one standing. It needs no database, so it is
//!    `cargo-sqlc`'s test `test_every_template_under_the_source_is_reported_once`.
//! 3. Each spelling `cargo sqlc spell` can write of each template, written in its place, leaves
//!    every composed file that holds the template, plain and with `--with`, planning the tree it
//!    plans as written. Join orders are laid out and never written, and the law fails on them:
//!    nine relations from either end, eight with three equated on one key, and the orders of
//!    `shared/set_roots.sqlc`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sql_composer::composer::{ComposedSql, Composer};
use sql_composer::parser::{parse_template, parse_template_file};
use sql_composer::source::{self, Flags, Standing};
use sql_composer::spellings::{lay_out, Axis, Beside};
use sql_composer::types::{Dialect, Element, Template, TemplateSource};
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::verdict;

/// How many times each table is scanned, by its name.
type Scans = BTreeMap<String, usize>;

/// What a template's own text does with each table: the tables it reads, and those it reads
/// through a `LEFT JOIN`, each with how many times.
#[derive(Default)]
struct Own {
    reads: Scans,
    left_joined: Scans,
}

/// One table a composed file's text and its plan disagree on: how many scans of it each holds,
/// and what the planner did, when that is known.
struct Difference {
    table: String,
    written: usize,
    planned: usize,
    kind: Option<&'static str>,
}

/// A composed file held to its plan: how many tables they agree on, and each they do not.
struct Fit {
    agree: usize,
    differences: Vec<Difference>,
}

impl Fit {
    /// Whether every difference is named.
    fn holds(&self) -> bool {
        self.differences.iter().all(|d| d.kind.is_some())
    }

    /// The tables agreed on, then each difference with what the planner did.
    fn detail(&self) -> String {
        let mut detail = match self.agree {
            1 => "1 table agrees".to_string(),
            n => format!("{n} tables agree"),
        };
        for d in &self.differences {
            detail.push_str(&format!(
                "; {} {} written, {} planned: {}",
                d.table,
                d.written,
                d.planned,
                d.kind.unwrap_or("NOTHING NAMES IT")
            ));
        }
        detail
    }
}

/// The tables of `schema`, by name.
async fn tables(pool: &PgPool, schema: &str) -> BTreeSet<String> {
    sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'm', 'f')",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect()
}

/// The words of `text`, in order, outside its strings and comments: each name lowercased, a quoted
/// name as it is written, and every other character that is not a space as a word of its own.
fn words(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let is_name = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let at = |i: usize, s: &str| {
        s.chars()
            .enumerate()
            .all(|(k, c)| chars.get(i + k) == Some(&c))
    };
    let mut words = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if at(i, "--") {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if at(i, "/*") {
            // A block comment, which may hold others.
            let mut depth = 0;
            while i < chars.len() {
                if at(i, "/*") {
                    depth += 1;
                    i += 2;
                } else if at(i, "*/") {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == '\'' || c == '"' {
            // A string or a quoted name, which ends at the first quote that is not doubled. In a
            // string with an E in front, a backslash takes the character after it into the string.
            let escapes = c == '\''
                && i > 0
                && matches!(chars[i - 1], 'e' | 'E')
                && (i < 2 || !is_name(chars[i - 2]));
            let start = i + 1;
            i += 1;
            while i < chars.len() {
                let doubled = chars[i] == c && chars.get(i + 1) == Some(&c);
                if doubled || (escapes && chars[i] == '\\') {
                    i += 2;
                } else if chars[i] == c {
                    break;
                } else {
                    i += 1;
                }
            }
            if c == '"' {
                let quoted: String = chars[start..i.min(chars.len())].iter().collect();
                words.push(quoted.replace("\"\"", "\""));
            }
            i += 1;
        } else if c == '$' {
            // A dollar-quoted string, $$…$$ or $tag$…$tag$, skipped to its closing tag.
            let end = (i + 1..chars.len())
                .find(|&j| !(chars[j].is_alphanumeric() || chars[j] == '_'))
                .filter(|&j| chars[j] == '$');
            match end {
                Some(end) => {
                    let tag: String = chars[i..=end].iter().collect();
                    i = (end + 1..chars.len())
                        .find(|&j| at(j, &tag))
                        .map_or(chars.len(), |j| j + tag.chars().count());
                }
                None => {
                    words.push(c.to_string());
                    i += 1;
                }
            }
        } else if is_name(c) {
            let start = i;
            while i < chars.len() && is_name(chars[i]) {
                i += 1;
            }
            words.push(chars[start..i].iter().collect::<String>().to_lowercase());
        } else {
            words.push(c.to_string());
            i += 1;
        }
    }
    words
}

/// What a template's own text does with each of `tables`: every name of one in its SQL, outside
/// what it composes and binds, its strings and its comments. A name with a `.` after it qualifies
/// a column and is not read, and the table an `INSERT INTO` names is written, not read.
fn own(template: &Template, tables: &BTreeSet<String>) -> Own {
    let text: String = template
        .elements
        .iter()
        .map(|element| match element {
            Element::Sql(sql) => sql.as_str(),
            _ => " ",
        })
        .collect();
    let words = words(&text);
    let mut own = Own::default();
    for (at, word) in words.iter().enumerate() {
        let before = |n: usize| at.checked_sub(n).map(|i| words[i].as_str());
        let qualifies = words.get(at + 1).is_some_and(|next| next == ".");
        let inserted = before(1) == Some("into") && before(2) == Some("insert");
        if !tables.contains(word) || qualifies || inserted {
            continue;
        }
        *own.reads.entry(word.clone()).or_default() += 1;
        if matches!(
            (before(1), before(2), before(3)),
            (Some("join"), Some("left"), _) | (Some("join"), Some("outer"), Some("left"))
        ) {
            *own.left_joined.entry(word.clone()).or_default() += 1;
        }
    }
    own
}

/// What a composed file's text writes: each template's own reads and left joins, once for every
/// copy of it the file holds. Each template's text is read once, into `texts`.
fn written(
    sqlc_dir: &Path,
    copies: &BTreeMap<PathBuf, usize>,
    texts: &mut BTreeMap<PathBuf, Own>,
    tables: &BTreeSet<String>,
) -> Own {
    let mut written = Own::default();
    for (template, n) in copies {
        let text = texts.entry(template.clone()).or_insert_with(|| {
            let parsed = parse_template_file(&sqlc_dir.join(template))
                .unwrap_or_else(|e| panic!("parse {}: {e}", template.display()));
            own(&parsed, tables)
        });
        for (table, times) in &text.reads {
            *written.reads.entry(table.clone()).or_default() += times * n;
        }
        for (table, times) in &text.left_joined {
            *written.left_joined.entry(table.clone()).or_default() += times * n;
        }
    }
    written
}

/// The node a line of a plan prints, without its indent and arrow, when the line prints one.
fn node(line: &str, first: bool) -> Option<&str> {
    let line = line.trim_start();
    match line.strip_prefix("->  ") {
        Some(node) => Some(node),
        None if first => Some(line),
        None => None,
    }
}

/// The table a plan node scans, when it scans one: a sequential, index, bitmap heap, TID, sample
/// or foreign scan, parallel or not. A bitmap index scan names the index, under the heap scan
/// that names the table, and is not counted.
fn scanned(node: &str) -> Option<&str> {
    const SCANS: [&str; 10] = [
        "Seq Scan",
        "Index Scan",
        "Index Scan Backward",
        "Index Only Scan",
        "Index Only Scan Backward",
        "Bitmap Heap Scan",
        "Tid Scan",
        "Tid Range Scan",
        "Sample Scan",
        "Foreign Scan",
    ];
    let node = node.strip_prefix("Parallel ").unwrap_or(node);
    let (kind, rest) = node.split_once(" on ")?;
    let kind = kind.split(" using ").next().unwrap_or(kind);
    let table = rest.split(' ').next().unwrap_or(rest);
    SCANS.contains(&kind).then(|| table.trim_matches('"'))
}

/// The scans of each of `tables` a plan holds.
fn planned(lines: &[String], tables: &BTreeSet<String>) -> Scans {
    let mut scans = Scans::new();
    for (i, line) in lines.iter().enumerate() {
        if let Some(table) = node(line, i == 0).and_then(scanned) {
            if tables.contains(table) {
                *scans.entry(table.to_string()).or_default() += 1;
            }
        }
    }
    scans
}

/// The plan of a composed statement, as `EXPLAIN (COSTS OFF)` prints it, line by line: its generic
/// plan when it binds parameters. It goes as a simple query, inside a read-only transaction that
/// is rolled back; prepared, its placeholders would be the EXPLAIN's own, and want values. A file
/// of more than one statement is refused, since a simple query runs every statement after the
/// first.
async fn explain(pool: &PgPool, composed: &ComposedSql) -> Result<Vec<String>, String> {
    if composed.statement_starts.len() > 1 {
        return Err(format!(
            "it holds {} statements, and only one is planned at a time",
            composed.statement_starts.len()
        ));
    }
    let options = if composed.bind_params.is_empty() {
        "COSTS OFF"
    } else {
        "COSTS OFF, GENERIC_PLAN"
    };
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    sqlx::raw_sql("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    let rows = sqlx::raw_sql(AssertSqlSafe(format!(
        "EXPLAIN ({options}) {}",
        composed.sql
    )))
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(rows.iter().map(|row| row.get::<String, _>(0)).collect())
}

/// `composed` held to its plan, with `copies` the copies of each template its tree counts. A
/// table the plan scans fewer times than the text writes is a `LEFT JOIN` the planner removed,
/// when the text left joins it at least as many times as the plan leaves out: PostgreSQL drops a
/// left join when nothing above it reads the table and its condition matches at most one row.
async fn fit(
    pool: &PgPool,
    sqlc_dir: &Path,
    composed: &ComposedSql,
    copies: &BTreeMap<PathBuf, usize>,
    texts: &mut BTreeMap<PathBuf, Own>,
    tables: &BTreeSet<String>,
) -> Result<Fit, String> {
    let written = written(sqlc_dir, copies, texts, tables);
    let planned = planned(&explain(pool, composed).await?, tables);
    let mut names: BTreeSet<&String> = written.reads.keys().collect();
    names.extend(planned.keys());
    let mut fit = Fit {
        agree: 0,
        differences: vec![],
    };
    for table in names {
        let count = |scans: &Scans| scans.get(table).copied().unwrap_or(0);
        let (w, p) = (count(&written.reads), count(&planned));
        if w == p {
            fit.agree += 1;
            continue;
        }
        let kind = (p < w && w - p <= count(&written.left_joined))
            .then_some("a LEFT JOIN the planner removed");
        fit.differences.push(Difference {
            table: table.clone(),
            written: w,
            planned: p,
            kind,
        });
    }
    Ok(fit)
}

/// The composer and each template's standing, for PostgreSQL, plain or with `--with`.
fn composed(sqlc_dir: &Path, with: bool) -> (Composer, BTreeMap<PathBuf, Standing>) {
    let mut flags = Flags::new(Dialect::Postgres);
    flags.with = with;
    let composer = source::composer(sqlc_dir, &flags).expect("compose failed");
    let standings = source::compose_all(sqlc_dir, &flags).expect("compose failed");
    (composer, standings)
}

/// The copies of each template the tree of the template at `rel_path` counts.
fn copies(composer: &Composer, sqlc_dir: &Path, rel_path: &Path) -> BTreeMap<PathBuf, usize> {
    let template = parse_template_file(&sqlc_dir.join(rel_path))
        .unwrap_or_else(|e| panic!("parse {}: {e}", rel_path.display()));
    composer
        .tree(&template)
        .unwrap_or_else(|e| panic!("tree of {}: {e}", rel_path.display()))
        .copies()
}

/// Whether the law fails on a seed, which is reported with what the law said of it.
fn fails_on(seed: &str, fit: Result<Fit, String>) -> bool {
    let (fails, detail) = match fit {
        Ok(fit) => (!fit.holds(), fit.detail()),
        Err(e) => (false, format!("it could not be planned: {e}")),
    };
    println!(
        "SEED {seed}: {} — {detail}",
        if fails {
            "the law FAILS on it, as it must"
        } else {
            "THE LAW HOLDS ON IT"
        }
    );
    fails
}

/// Law 1's seeds, composed plain: a copy the text holds and the tree does not count, and a copy
/// the tree counts and the text does not hold. Whether the law fails on each.
async fn seeds(
    pool: &PgPool,
    sqlc_dir: &Path,
    texts: &mut BTreeMap<PathBuf, Own>,
    tables: &BTreeSet<String>,
) -> bool {
    let (composer, standings) = composed(sqlc_dir, false);

    // sets/never_bought.sqlc with shared/purchase_sets.sqlc composed twice in its text, held to
    // the tree of the template as it is, which holds it once.
    let never_bought = Path::new("sets/never_bought.sqlc");
    let path = sqlc_dir.join(never_bought);
    let once = ":compose(shared/purchase_sets.sqlc)";
    let seed = format!(
        "{}, its text holding {once} twice and its tree once",
        never_bought.display()
    );
    let text = std::fs::read_to_string(&path).expect("read failed");
    let first = if text.contains(once) {
        let doubled = text.replace(once, &format!("{once}\n    UNION ALL\n    {once}"));
        let doubled = parse_template(&doubled, TemplateSource::File(path)).expect("parse failed");
        let doubled = composer.compose(&doubled).expect("compose failed");
        let copies = copies(&composer, sqlc_dir, never_bought);
        fails_on(
            &seed,
            fit(pool, sqlc_dir, &doubled, &copies, texts, tables).await,
        )
    } else {
        println!("SEED {seed}: CANNOT BE MADE — the template no longer holds {once}");
        false
    };

    // reports/shared_moulds.sqlc as it is, held to its tree with one more copy of
    // shared/theme_closure.sqlc than it counts.
    let shared_moulds = Path::new("reports/shared_moulds.sqlc");
    let closure = PathBuf::from("shared/theme_closure.sqlc");
    let mut counted = copies(&composer, sqlc_dir, shared_moulds);
    let held = counted.get(&closure).copied().unwrap_or(0);
    counted.insert(closure.clone(), held + 1);
    let seed = format!(
        "{}, its tree counting {} copies of {} where its text holds {held}",
        shared_moulds.display(),
        held + 1,
        closure.display()
    );
    let second = match standings.get(shared_moulds) {
        Some(Standing::Written(composed)) => fails_on(
            &seed,
            fit(pool, sqlc_dir, composed, &counted, texts, tables).await,
        ),
        _ => {
            println!("SEED {seed}: CANNOT BE MADE — the template has no composed file");
            false
        }
    };

    first && second
}

/// Where two plans part, when they do: the first line that differs, with each plan's.
fn parting(first: &[String], second: &[String]) -> Option<String> {
    let at = (0..first.len().max(second.len())).find(|&i| first.get(i) != second.get(i))?;
    let line = |plan: &[String]| plan.get(at).map_or("nothing", |l| l.trim()).to_string();
    Some(format!(
        "line {}, {} against {}",
        at + 1,
        line(first),
        line(second)
    ))
}

/// A line of a plan cut into its words, each a run of letters, digits and `_`, and the characters
/// between them, each apart.
fn cut(line: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut in_word = false;
    for c in line.chars() {
        let word = c.is_alphanumeric() || c == '_';
        match parts.last_mut() {
            Some(part) if word && in_word => part.push(c),
            _ => parts.push(c.to_string()),
        }
        in_word = word;
    }
    parts
}

/// The names a plan reads relations by: a subquery's, a WITH query's or a function's, and each
/// table's alias, but never a table's own name.
fn aliases(plan: &[String], tables: &BTreeSet<String>) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in plan {
        let Some((_, after)) = line.split_once(" on ") else {
            continue;
        };
        let words: Vec<&str> = after
            .split_whitespace()
            .take_while(|w| !w.starts_with('('))
            .collect();
        let name = match words.as_slice() {
            [name] => name,
            [_, alias, ..] => alias,
            [] => continue,
        };
        if !tables.contains(*name) {
            found.insert(name.to_string());
        }
    }
    found
}

/// The conditions of a plan line such as `Hash Cond: ((a = b) AND (c = d))`, each apart and
/// sorted, with what the line calls them; none where it holds one condition alone.
fn conditions(line: &str) -> Option<(&str, Vec<&str>)> {
    let (label, expr) = line.split_once(": ")?;
    let expr = expr.trim();
    let mut depth = 0;
    for (i, c) in expr.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && i + 1 < expr.len() {
            return None;
        }
    }
    let inner = expr.strip_prefix('(')?.strip_suffix(')')?;
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (i, c) in inner.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && inner[i..].starts_with(" AND ") {
            parts.push(&inner[start..i]);
            start = i + " AND ".len();
        }
    }
    parts.push(&inner[start..]);
    if parts.len() < 2 {
        return None;
    }
    parts.sort();
    Some((label, parts))
}

/// What two plans that part differ by, where they differ only in how the plan is printed: a
/// node's conditions in another order, or a relation read by another name, each name of the one
/// plan standing for one name of the other; none where anything else differs.
fn named(first: &[String], second: &[String], tables: &BTreeSet<String>) -> Option<String> {
    if first.len() != second.len() {
        return None;
    }
    let (from, to) = (aliases(first, tables), aliases(second, tables));

    // The names of the first plan the second reads by another, where its lines differ in names
    // alone.
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    for (a, b) in first.iter().zip(second) {
        let (a, b) = (cut(a), cut(b));
        if a.len() != b.len() {
            continue;
        }
        for (x, y) in a.into_iter().zip(b) {
            if x != y && from.contains(&x) && to.contains(&y) {
                names.entry(x).or_insert(y);
            }
        }
    }
    let mut taken = BTreeSet::new();
    for (x, y) in &names {
        let kept_by_another = from.contains(y) && !names.contains_key(y);
        if kept_by_another || !taken.insert(y) || x == y {
            return None;
        }
    }

    // Every line of the first, its names renamed, is the second's line, or holds its conditions
    // in another order.
    let mut reordered = 0;
    for (a, b) in first.iter().zip(second) {
        let renamed: String = cut(a)
            .into_iter()
            .map(|part| names.get(&part).cloned().unwrap_or(part))
            .collect();
        if renamed == *b {
            continue;
        }
        match (conditions(&renamed), conditions(b)) {
            (Some(x), Some(y)) if x == y => reordered += 1,
            _ => return None,
        }
    }
    let mut kinds = Vec::new();
    if reordered > 0 {
        kinds.push(format!(
            "{reordered} line(s) of the same conditions in another order"
        ));
    }
    if !names.is_empty() {
        let renamed: Vec<String> = names.iter().map(|(a, b)| format!("{a} as {b}")).collect();
        kinds.push(format!(
            "relations read by another name, {}",
            renamed.join(", ")
        ));
    }
    Some(kinds.join(" and "))
}

/// Every file under `from`, copied to the same place under `to`.
fn copy_tree(from: &Path, to: &Path) {
    for entry in walkdir::WalkDir::new(from) {
        let entry = entry.expect("read failed");
        let target = to.join(entry.path().strip_prefix(from).expect("under the source"));
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&target).expect("write failed");
        } else {
            std::fs::copy(entry.path(), &target).expect("write failed");
        }
    }
}

/// One template's spellings held to the plans of the composed files that hold it: how many
/// spellings and files, each way a plan parts that nothing names, and each plan printed otherwise.
struct Round {
    rel_path: PathBuf,
    spellings: usize,
    files: usize,
    parted: Vec<String>,
    printed_otherwise: BTreeSet<String>,
}

impl Round {
    /// The round in words: each parting, or that each spelling plans the same tree.
    fn detail(&self) -> String {
        if !self.parted.is_empty() {
            return self.parted.join("; ");
        }
        if self.printed_otherwise.is_empty() {
            return "each plans the same tree".to_string();
        }
        let printed: Vec<&str> = self.printed_otherwise.iter().map(String::as_str).collect();
        format!(
            "each plans the same tree, printed otherwise in {}",
            printed.join("; ")
        )
    }
}

/// Each template's spellings on the axes `only` takes, composed plain or with `--with`: each
/// written into a copy of the source in its place, and every composed file holding the template
/// planned against its plan as written, a difference [`named`] names counted as the same tree.
/// Each composed file that cannot be planned as written is returned apart, with why.
async fn rounds(
    pool: &PgPool,
    sqlc_dir: &Path,
    with: bool,
    tables: &BTreeSet<String>,
    only: impl Fn(&Path, Axis) -> bool,
) -> (Vec<Round>, Vec<(PathBuf, String)>) {
    let mut flags = Flags::new(Dialect::Postgres);
    flags.with = with;
    let (composer, standings) = composed(sqlc_dir, with);

    // Each composed file's plan as written, and the templates its tree holds.
    let mut files: BTreeMap<PathBuf, (Vec<String>, BTreeSet<PathBuf>)> = BTreeMap::new();
    let mut unplanned = Vec::new();
    for (rel_path, standing) in &standings {
        let Standing::Written(composed) = standing else {
            continue;
        };
        match explain(pool, composed).await {
            Ok(plan) => {
                let holds = copies(&composer, sqlc_dir, rel_path).into_keys().collect();
                files.insert(rel_path.clone(), (plan, holds));
            }
            Err(e) => unplanned.push((rel_path.clone(), e)),
        }
    }

    let copy = tempfile::tempdir().expect("write failed");
    copy_tree(sqlc_dir, copy.path());
    let read = |path: &Path| std::fs::read_to_string(sqlc_dir.join(path)).ok();
    let beside = Beside::of_trees(&read, &[sqlc_dir.to_path_buf()]).expect("read failed");
    let mut found = Vec::new();
    for entry in walkdir::WalkDir::new(sqlc_dir).sort_by_file_name() {
        let path = entry.expect("read failed").into_path();
        if path.extension().is_none_or(|ext| ext != "sqlc") {
            continue;
        }
        let rel_path = path.strip_prefix(sqlc_dir).expect("under the source");
        let text = read(rel_path).expect("read failed");
        let layout = lay_out(&text, rel_path, &beside);
        let mut spellings = Vec::new();
        for axis in layout.axes.iter().filter(|a| only(rel_path, a.axis)) {
            for place in &axis.places {
                for spelling in place.spellings.iter().skip(1) {
                    let what = format!("{}, {}: {}", axis.axis.words(), place.what, spelling.name);
                    spellings.push((what, &spelling.text));
                }
            }
        }
        let holding: Vec<&PathBuf> = files
            .iter()
            .filter(|(_, (_, holds))| holds.contains(rel_path))
            .map(|(file, _)| file)
            .collect();
        if spellings.is_empty() || holding.is_empty() {
            continue;
        }
        let mut round = Round {
            rel_path: rel_path.to_path_buf(),
            spellings: spellings.len(),
            files: holding.len(),
            parted: vec![],
            printed_otherwise: BTreeSet::new(),
        };
        for (what, spelled) in &spellings {
            std::fs::write(copy.path().join(rel_path), spelled).expect("write failed");
            let spelled_standings = match source::compose_all(copy.path(), &flags) {
                Ok(standings) => standings,
                Err(e) => {
                    round
                        .parted
                        .push(format!("{what}: the source does not compose: {e}"));
                    continue;
                }
            };
            for file in &holding {
                let Some(Standing::Written(composed)) = spelled_standings.get(*file) else {
                    round
                        .parted
                        .push(format!("{what}: {} is not composed", file.display()));
                    continue;
                };
                let (written, _) = &files[*file];
                match explain(pool, composed).await {
                    Ok(plan) => {
                        let Some(at) = parting(written, &plan) else {
                            continue;
                        };
                        match named(written, &plan, tables) {
                            Some(kind) => {
                                round
                                    .printed_otherwise
                                    .insert(format!("{}: {kind}", file.display()));
                            }
                            None => round
                                .parted
                                .push(format!("{what}: {} parts at {at}", file.display())),
                        }
                    }
                    Err(e) => round.parted.push(format!(
                        "{what}: {} could not be planned: {e}",
                        file.display()
                    )),
                }
            }
        }
        std::fs::write(copy.path().join(rel_path), &text).expect("write failed");
        found.push(round);
    }
    (found, unplanned)
}

/// Law 3, composed plain or with `--with`: each spelling `cargo sqlc spell` can write leaves every
/// composed file holding its template planning the tree it plans as written. Join orders are laid
/// out and never written, so they are the seeds' ([`seeds_of_the_round`]).
async fn same_round(pool: &PgPool, sqlc_dir: &Path, with: bool, tables: &BTreeSet<String>) -> bool {
    let mode = if with { "--with" } else { "plain" };
    let (found, unplanned) = rounds(pool, sqlc_dir, with, tables, |_, axis| {
        axis != Axis::JoinOrder
    })
    .await;
    let mut all_hold = true;
    for (rel_path, e) in unplanned {
        let law = format!("{}, {mode}, is planned as written", rel_path.display());
        all_hold &= verdict(&law, false, &e);
    }
    for round in found {
        let law = format!(
            "{}, {mode}, plans as written in each of its {} spelling(s), in the {} composed file(s) \
             holding it",
            round.rel_path.display(),
            round.spellings,
            round.files
        );
        all_hold &= verdict(&law, round.parted.is_empty(), &round.detail());
    }
    all_hold
}

/// A join of nine relations, each with its alias and the condition joining it to the next, on keys
/// no third relation shares.
const KEYED: [(&str, &str, &str); 9] = [
    ("lego_cities", "ci", "ci.city_id = pc.city_id"),
    ("lego_postcodes", "pc", "pc.postcode_id = st.postcode_id"),
    ("lego_streets", "st", "st.street_id = b.street_id"),
    ("lego_builders", "b", "b.builder_id = co.builder_id"),
    ("lego_collection", "co", "co.set_num = s.set_num"),
    ("lego_sets", "s", "s.theme_id = t.id"),
    ("lego_themes", "t", "t.parent_id = pt.id"),
    ("lego_themes", "pt", "pt.parent_id = gt.id"),
    ("lego_themes", "gt", ""),
];

/// A join of eight relations, as [`KEYED`] is written, three of them equated on one key: the
/// collection row's set, the set and the set's inventory.
const SHARED: [(&str, &str, &str); 8] = [
    ("lego_cities", "ci", "ci.city_id = pc.city_id"),
    ("lego_postcodes", "pc", "pc.postcode_id = st.postcode_id"),
    ("lego_streets", "st", "st.street_id = b.street_id"),
    ("lego_builders", "b", "b.builder_id = co.builder_id"),
    ("lego_collection", "co", "co.set_num = s.set_num"),
    ("lego_sets", "s", "s.set_num = i.set_num"),
    ("lego_inventories", "i", "i.id = ip.inventory_id"),
    ("lego_inventory_parts", "ip", ""),
];

/// `links` as one statement, joined from the first or from the last, each relation joined to the
/// one already beside it, and one city named.
fn chain(links: &[(&str, &str, &str)], from_last: bool) -> String {
    let n = links.len();
    let mut order: Vec<usize> = (0..n).collect();
    if from_last {
        order.reverse();
    }
    let mut sql = format!("SELECT ci.name, {}.* FROM ", links[n - 1].1);
    for (k, &r) in order.iter().enumerate() {
        let (table, alias, _) = links[r];
        if k == 0 {
            sql.push_str(&format!("{table} {alias}"));
            continue;
        }
        let on = if from_last {
            links[r].2
        } else {
            links[r - 1].2
        };
        sql.push_str(&format!(" JOIN {table} {alias} ON {on}"));
    }
    sql.push_str(" WHERE ci.name = 'Lisboa'");
    sql
}

/// Where `links` joined from its first relation and from its last part their plans, when they do.
async fn orders_part(pool: &PgPool, links: &[(&str, &str, &str)]) -> Option<String> {
    let mut plans = Vec::new();
    for from_last in [false, true] {
        let template = parse_template(
            &chain(links, from_last),
            TemplateSource::Literal("chain".into()),
        )
        .expect("parse failed");
        let composed = Composer::new(Dialect::Postgres)
            .compose(&template)
            .expect("compose failed");
        match explain(pool, &composed).await {
            Ok(plan) => plans.push(plan),
            Err(e) => return Some(format!("it could not be planned: {e}")),
        }
    }
    parting(&plans[0], &plans[1])
}

/// Whether the law fails on a seed, which is reported with what the law said of it.
fn seed_fails(seed: &str, fails: bool, detail: &str) -> bool {
    println!(
        "SEED {seed}: {} — {detail}",
        if fails {
            "the law FAILS on it, as it must"
        } else {
            "THE LAW HOLDS ON IT"
        }
    );
    fails
}

/// Law 3's seeds, each a join written in another order, where the law must fail: the join of nine
/// relations from its first or its last, which PostgreSQL plans in the pieces it is written in;
/// eight relations from either end, three of them equated on one key; and the join orders
/// `cargo sqlc spellings` lays out for `shared/set_roots.sqlc`, composed plain. Whether the law
/// fails on each.
async fn seeds_of_the_round(pool: &PgPool, sqlc_dir: &Path, tables: &BTreeSet<String>) -> bool {
    let mut all_fail = true;
    for (seed, links) in [
        (
            "a join of nine relations, from its first or its last",
            &KEYED[..],
        ),
        (
            "a join of eight relations, three of them equated on one key, from its first or its \
             last",
            &SHARED[..],
        ),
    ] {
        let parted = orders_part(pool, links).await;
        let detail = parted
            .as_deref()
            .map_or("the two orders plan alike".into(), |at| {
                format!("the orders part at {at}")
            });
        all_fail &= seed_fails(seed, parted.is_some(), &detail);
    }
    let set_roots = Path::new("shared/set_roots.sqlc");
    let (found, _) = rounds(pool, sqlc_dir, false, tables, |path, axis| {
        path == set_roots && axis == Axis::JoinOrder
    })
    .await;
    let seed = format!("{}'s join orders, plain", set_roots.display());
    all_fail &= match found.first() {
        Some(round) => seed_fails(&seed, !round.parted.is_empty(), &round.detail()),
        None => seed_fails(&seed, false, "CANNOT BE MADE — it lays out no join order"),
    };
    all_fail
}

pub(crate) async fn cmd_fit(pool: &PgPool, sqlc_dir: &Path, schema: &str) -> bool {
    println!("\n=== The fit's laws ===");
    let tables = tables(pool, schema).await;
    let mut texts = BTreeMap::new();
    let mut all_hold = true;

    // 1. Each composed file, plain and with --with, scans each table as often as its copies read
    //    it, or the planner's reason is named.
    for with in [false, true] {
        let mode = if with { "--with" } else { "plain" };
        let (composer, standings) = composed(sqlc_dir, with);
        for (rel_path, standing) in &standings {
            let Standing::Written(composed) = standing else {
                continue;
            };
            let copies = copies(&composer, sqlc_dir, rel_path);
            let law = format!("{}, {mode}, scans what its copies read", rel_path.display());
            all_hold &= match fit(pool, sqlc_dir, composed, &copies, &mut texts, &tables).await {
                Ok(fit) => verdict(&law, fit.holds(), &fit.detail()),
                Err(e) => verdict(&law, false, &format!("it could not be planned: {e}")),
            };
        }
    }
    let seeds_fail = seeds(pool, sqlc_dir, &mut texts, &tables).await;

    // 3. Each spelling `cargo sqlc spell` can write, plain and with --with, plans as written in
    //    every file holding its template; a join written in another order parts the plans.
    for with in [false, true] {
        all_hold &= same_round(pool, sqlc_dir, with, &tables).await;
    }
    let seeds_fail = seeds_fail && seeds_of_the_round(pool, sqlc_dir, &tables).await;

    println!(
        "=== The fit's laws: {} ===",
        match (all_hold, seeds_fail) {
            (true, true) => "all hold, and each fails on its seed",
            (false, _) => "SOME FAIL",
            (true, false) => "all hold, and A SEED DOES NOT FAIL",
        }
    );
    all_hold && seeds_fail
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|l| l.to_string()).collect()
    }

    fn tables() -> BTreeSet<String> {
        ["lego_sets", "lego_themes"].map(String::from).into()
    }

    #[test]
    fn a_plan_printed_otherwise_is_named() {
        let written = plan(&[
            "Hash Join",
            "  Hash Cond: ((s.a = t.a) AND (s.b = t.b))",
            "  ->  Subquery Scan on shared_theme_closure",
            "  ->  Seq Scan on lego_sets s",
            "        Filter: (shared_theme_closure.id = s.theme_id)",
        ]);
        let spelled = plan(&[
            "Hash Join",
            "  Hash Cond: ((s.b = t.b) AND (s.a = t.a))",
            "  ->  Subquery Scan on tc",
            "  ->  Seq Scan on lego_sets s",
            "        Filter: (tc.id = s.theme_id)",
        ]);
        assert_eq!(
            named(&written, &spelled, &tables()).as_deref(),
            Some(
                "1 line(s) of the same conditions in another order and relations read by another \
                 name, shared_theme_closure as tc"
            )
        );
    }

    #[test]
    fn a_plan_that_differs_otherwise_is_not_named() {
        let written = plan(&[
            "Merge Join",
            "  Merge Cond: (s.id = r.id)",
            "  ->  Seq Scan on lego_sets s",
            "  ->  Seq Scan on lego_themes r",
        ]);
        let renamed = plan(&[
            "Merge Join",
            "  Merge Cond: (x.id = r.id)",
            "  ->  Seq Scan on lego_sets x",
            "  ->  Seq Scan on lego_themes r",
        ]);
        assert_eq!(
            named(&written, &renamed, &tables()).as_deref(),
            Some("relations read by another name, s as x")
        );
        let other = |at: usize, line: &str| {
            let mut spelled = written.clone();
            spelled[at] = line.to_string();
            spelled
        };
        for (spelled, why) in [
            (other(1, "  Merge Cond: (r.id = s.id)"), "sides swapped"),
            (other(2, "  ->  Seq Scan on lego_themes s"), "another table"),
            (
                other(1, "  Merge Cond: (s.id = r.parent_id)"),
                "another column",
            ),
            (
                other(2, "  ->  Seq Scan on lego_sets x"),
                "an alias renamed in one line alone",
            ),
            (
                other(2, "  ->  Seq Scan on lego_sets r"),
                "an alias renamed to one the plan holds",
            ),
            (written[..3].to_vec(), "a line fewer"),
        ] {
            assert!(parting(&written, &spelled).is_some(), "{why}");
            assert_eq!(named(&written, &spelled, &tables()), None, "{why}");
        }
    }
}
