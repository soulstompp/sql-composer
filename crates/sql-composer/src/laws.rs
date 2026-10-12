//! The laws: each spelling of a template, and the respelling of a composed file, returns the rows
//! the template as written returns, as many times each, on tables holding the rows the nearest
//! rewrite that is not a spelling fails on, and that rewrite returns other rows there. Each runs on
//! a PostgreSQL server, on tables of those rows and again on the same rows given as mock tables,
//! with the server's URL in `SQLC_TEST_POSTGRES_URL`: `cargo test --all-features -- --ignored`.

use postgres::types::ToSql;
use postgres::{Client, NoTls};

use crate::composer::Composer;
use crate::mock::MockTable;
use crate::parser::parse_template;
use crate::types::{Dialect, TemplateSource};

/// The rows a trap holds, and the values its templates bind.
pub(crate) struct Trap {
    /// The tables, each with its rows.
    pub(crate) tables: Vec<MockTable>,
    /// Each bound name with its value.
    pub(crate) binds: Vec<(&'static str, i32)>,
    /// Templates a template composes, by their path, where it composes any.
    pub(crate) templates: Vec<(&'static str, String)>,
}

/// What a law found: how many rows each statement returned.
#[derive(Debug)]
pub(crate) struct Found {
    /// The rows the template as written returned, on the tables and on the mocks.
    pub(crate) written: usize,
}

/// The server the laws run on.
fn client() -> Client {
    let url = std::env::var("SQLC_TEST_POSTGRES_URL").expect("SQLC_TEST_POSTGRES_URL");
    Client::connect(&url, NoTls).expect("a PostgreSQL server at SQLC_TEST_POSTGRES_URL")
}

/// `text` composed for PostgreSQL, the trap's tables mocked where `mocked`.
fn composed(text: &str, trap: &Trap, mocked: bool) -> (String, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    for (path, template) in &trap.templates {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, template).unwrap();
    }
    let mut composer = Composer::new(Dialect::Postgres);
    composer.add_search_path(dir.path().to_path_buf());
    if mocked {
        for table in &trap.tables {
            composer.add_mock_table(table.clone());
        }
    }
    let template = parse_template(text, TemplateSource::Literal("law".into())).unwrap();
    let composed = composer.compose(&template).unwrap();
    let sql = composed.sql.trim_end().trim_end_matches(';').to_string();
    (sql, composed.bind_params)
}

/// The rows `sql` returns, each as its text, sorted.
fn rows(
    client: &mut postgres::Transaction,
    sql: &str,
    names: &[String],
    trap: &Trap,
) -> Vec<String> {
    let values: Vec<i32> = names
        .iter()
        .map(|name| {
            trap.binds
                .iter()
                .find(|(n, _)| n == name)
                .unwrap_or_else(|| panic!("no value for {name}"))
                .1
        })
        .collect();
    let params: Vec<&(dyn ToSql + Sync)> =
        values.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
    let mut rows: Vec<String> = client
        .query(&format!("SELECT r::text FROM ({sql}) r"), &params)
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    rows.sort();
    rows
}

/// The rows each of `texts` returns on the trap's tables, then on its mocks.
fn run<const N: usize>(texts: [&str; N], trap: &Trap) -> Vec<[Vec<String>; N]> {
    let mut client = client();
    let mut found = Vec::new();
    for mocked in [false, true] {
        let mut tx = client.transaction().unwrap();
        if !mocked {
            for table in &trap.tables {
                tx.batch_execute(&format!(
                    "CREATE TEMP TABLE {} AS {}",
                    table.name,
                    table.to_sql(Dialect::Postgres)
                ))
                .unwrap();
            }
        }
        let mut each = |text: &str| {
            let (sql, names) = composed(text, trap, mocked);
            rows(&mut tx, &sql, &names, trap)
        };
        found.push(texts.map(&mut each));
        tx.rollback().unwrap();
    }
    found
}

/// The law of a spelling: `spelled`, `written` in another spelling, returns its rows on the
/// trap's tables and on its mocks, as many times each and some rows at least; and `near.1`, the
/// nearest rewrite of `near.0` that is not a spelling, returns other rows than it on both.
pub(crate) fn holds(written: &str, spelled: &str, near: (&str, &str), trap: &Trap) -> Found {
    assert_ne!(written, spelled, "the spelling wrote nothing");
    let mut counted = 0;
    for [as_written, as_spelled, near_written, near_spelled] in
        run([written, spelled, near.0, near.1], trap)
    {
        assert!(
            !as_written.is_empty(),
            "the law compared no rows: {written}"
        );
        assert_eq!(as_spelled, as_written, "{spelled}\nagainst\n{written}");
        assert_ne!(
            near_spelled, near_written,
            "the rewrite that is no spelling kept the rows: {}\nagainst\n{}",
            near.1, near.0
        );
        counted = as_written.len();
    }
    Found { written: counted }
}
