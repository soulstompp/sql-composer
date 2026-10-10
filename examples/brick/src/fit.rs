//! The fit's laws: what `cargo sqlc tree` says each composed file holds, held to the plan
//! PostgreSQL makes for it. Every statement is planned by `EXPLAIN (COSTS OFF)` and never run.
//!
//! 1. Each composed file, plain and with `--with`, scans each table as often as its text writes
//!    it: each template the tree holds reads the tables its own text names, once for every copy
//!    of it the tree counts. Where the plan scans a table more or less often, the difference is
//!    named by what the planner did, and a difference nothing names fails the law.
//! 2. Every template under the source takes exactly one standing. It needs no database, so it is
//!    `cargo-sqlc`'s test `test_every_template_under_the_source_is_reported_once`.
//! 3. Each spelling's same round: it waits for the spellings.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sql_composer::composer::{ComposedSql, Composer};
use sql_composer::parser::{parse_template, parse_template_file};
use sql_composer::source::{self, Flags, Standing};
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

    // 3. Each spelling's same round comes here, once the spellings are written.

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
