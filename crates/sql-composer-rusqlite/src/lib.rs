//! rusqlite driver for sql-composer.
//!
//! Provides [`SqliteConnection`], a thin wrapper around [`rusqlite::Connection`]
//! that implements [`ComposerConnection`] for composing SQL templates with bind
//! values against SQLite databases.
//!
//! # Example
//!
//! ```no_run
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnection;
//! use sql_composer::parser::parse_template;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_rusqlite::SqliteConnection;
//!
//! let conn = SqliteConnection::open_in_memory().unwrap();
//! conn.execute("CREATE TABLE lego_sets (set_num TEXT, name TEXT, year INTEGER)", []).unwrap();
//!
//! let input = "SELECT set_num, name FROM lego_sets WHERE year = :bind(year)";
//! let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
//! let composer = Composer::new(Dialect::Sqlite);
//!
//! let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn rusqlite::types::ToSql>]);
//! let (sql, params) = conn.compose(&composer, &template, values).unwrap();
//!
//! let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
//! let mut stmt = conn.prepare(&sql).unwrap();
//! let _rows = stmt.query(refs.as_slice()).unwrap();
//! ```

pub use rusqlite;

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use sql_composer::composer::Composer;
use sql_composer::driver::{self, ComposerConnection};
use sql_composer::types::Template;

/// Error type for sql-composer-rusqlite operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the sql-composer core.
    #[error(transparent)]
    Composer(#[from] sql_composer::Error),

    /// An error from rusqlite.
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
}

/// A wrapper around [`rusqlite::Connection`] that implements [`ComposerConnection`].
///
/// Dereferences to the inner `rusqlite::Connection`, so all native methods
/// are available directly.
pub struct SqliteConnection(pub rusqlite::Connection);

impl SqliteConnection {
    /// Open an in-memory SQLite database.
    pub fn open_in_memory() -> Result<Self, rusqlite::Error> {
        rusqlite::Connection::open_in_memory().map(Self)
    }

    /// Open a SQLite database at the given path.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, rusqlite::Error> {
        rusqlite::Connection::open(path).map(Self)
    }

    /// Wrap an existing `rusqlite::Connection`.
    pub fn from_connection(conn: rusqlite::Connection) -> Self {
        Self(conn)
    }
}

impl Deref for SqliteConnection {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SqliteConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ComposerConnection for SqliteConnection {
    type Value = Box<dyn rusqlite::types::ToSql>;
    type Statement = String;
    type Error = Error;

    fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        mut values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), Error> {
        let composed = composer
            .compose_with_values_and_nulls(template, &values, |value| is_null(value.as_ref()))?;
        let ordered = driver::resolve_values(&composed, &mut values)?;
        Ok((composed.sql, ordered))
    }
}

/// Whether a value is NULL, as the database will read it.
fn is_null(value: &dyn rusqlite::types::ToSql) -> bool {
    use rusqlite::types::{ToSqlOutput, Value, ValueRef};
    matches!(
        value.to_sql(),
        Ok(ToSqlOutput::Borrowed(ValueRef::Null) | ToSqlOutput::Owned(Value::Null))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_composer::bind_values;
    use sql_composer::parser::parse_template;
    use sql_composer::types::{Dialect, TemplateSource};

    fn boxed(v: impl rusqlite::types::ToSql + 'static) -> Box<dyn rusqlite::types::ToSql> {
        Box::new(v)
    }

    #[test]
    fn test_compose_and_query() {
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
            .unwrap();
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')", [])
            .unwrap();

        let input = "SELECT id, name FROM users WHERE id = :bind(user_id)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Sqlite);

        let values = bind_values!("user_id" => [boxed(1i32)]);
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(sql, "SELECT id, name FROM users WHERE id = ?1");

        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<(i32, String)> = stmt
            .query_map(refs.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows, vec![(1, "Alice".to_string())]);
    }

    #[test]
    fn test_compose_multi_value_in_clause() {
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (2, 'b')", [])
            .unwrap();
        conn.execute("INSERT INTO items (id, label) VALUES (3, 'c')", [])
            .unwrap();

        let input = "SELECT id, label FROM items WHERE id IN (:bind(ids)) ORDER BY id";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Sqlite);

        let values = bind_values!("ids" => [boxed(1i32), boxed(3i32)]);
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(
            sql,
            "SELECT id, label FROM items WHERE id IN (?1, ?2) ORDER BY id"
        );

        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<(i32, String)> = stmt
            .query_map(refs.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows, vec![(1, "a".to_string()), (3, "c".to_string())]);
    }

    #[test]
    fn test_compose_returns_correct_sql() {
        let conn = SqliteConnection::open_in_memory().unwrap();

        let input = "SELECT :bind(a) AS col_1, :bind(b) AS col_2";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Sqlite);

        let values = bind_values!(
            "a" => [boxed("hello")],
            "b" => [boxed("world")]
        );
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(sql, "SELECT ?1 AS col_1, ?2 AS col_2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_a_mocked_table_is_read_where_no_table_exists() {
        let conn = SqliteConnection::open_in_memory().unwrap();

        let input = "SELECT set_num FROM lego_sets WHERE year >= :bind(year) ORDER BY set_num";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let mut composer = Composer::new(Dialect::Sqlite);
        composer.add_mock_table(
            sql_composer::MockTable::new("lego_sets")
                .column("set_num", "text")
                .column("year", "integer")
                .row(&[Some("6000-1"), Some("1990")])
                .row(&[Some("6001-1"), Some("1992")])
                .row(&[Some("6002-1"), None]),
        );

        let values = bind_values!("year" => [boxed(1991i32)]);
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<String> = stmt
            .query_map(refs.as_slice(), |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows, vec!["6001-1".to_string()]);
    }

    #[test]
    fn test_a_distinct_count_of_several_sources_runs() {
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE a (id INTEGER)", []).unwrap();
        conn.execute("CREATE TABLE b (id INTEGER)", []).unwrap();
        conn.execute("INSERT INTO a (id) VALUES (1), (2)", [])
            .unwrap();
        conn.execute("INSERT INTO b (id) VALUES (2), (3)", [])
            .unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.sqlc"), "SELECT id FROM a").unwrap();
        std::fs::write(dir.path().join("b.sqlc"), "SELECT id FROM b").unwrap();
        let input = ":count(DISTINCT id OF a.sqlc, b.sqlc)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let mut composer = Composer::new(Dialect::Sqlite);
        composer.add_search_path(dir.path().to_path_buf());

        let (sql, _) = conn.compose(&composer, &template, BTreeMap::new()).unwrap();
        let count: i64 = conn.query_row(&sql, [], |row| row.get(0)).unwrap();

        assert_eq!(count, 3);
    }

    #[test]
    fn test_a_null_is_refused_where_the_bind_does_not_take_it() {
        let conn = SqliteConnection::open_in_memory().unwrap();
        let composer = Composer::new(Dialect::Sqlite);
        let refusing = parse_template(
            "SELECT :bind(a) AS a",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();
        let taking = parse_template(
            "SELECT :bind(a NULL) AS a",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();

        for null in [boxed(None::<i32>), boxed(rusqlite::types::Null)] {
            let refused = conn.compose(&composer, &refusing, bind_values!("a" => [null]));
            assert!(matches!(
                refused,
                Err(Error::Composer(sql_composer::Error::NullValue { .. }))
            ));
        }
        assert!(conn
            .compose(&composer, &refusing, bind_values!("a" => [boxed(7)]))
            .is_ok());

        let (sql, params) = conn
            .compose(
                &composer,
                &taking,
                bind_values!("a" => [boxed(None::<i32>)]),
            )
            .unwrap();
        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let a: Option<i32> = conn
            .query_row(&sql, refs.as_slice(), |row| row.get(0))
            .unwrap();
        assert_eq!(a, None);
    }

    #[test]
    fn test_the_reserved_words_are_the_keywords_sqlite_refuses_as_a_name() {
        let conn = SqliteConnection::open_in_memory().unwrap();
        let refuses = |word: &str| {
            conn.prepare(&format!("WITH {word} AS (SELECT 1) SELECT * FROM {word}"))
                .is_err()
        };
        let reserved = Dialect::Sqlite.reserved_words();
        let count = unsafe { rusqlite::ffi::sqlite3_keyword_count() };
        assert!(count > 100, "SQLite listed {count} keywords");

        let mut wrong = Vec::new();
        for i in 0..count {
            let mut name: *const std::os::raw::c_char = std::ptr::null();
            let mut len: std::os::raw::c_int = 0;
            unsafe { rusqlite::ffi::sqlite3_keyword_name(i, &mut name, &mut len) };
            let bytes = unsafe { std::slice::from_raw_parts(name.cast::<u8>(), len as usize) };
            let word = std::str::from_utf8(bytes).unwrap().to_ascii_lowercase();
            if refuses(&word) != reserved.contains(&word.as_str()) {
                wrong.push(word);
            }
        }
        wrong.extend(
            reserved
                .iter()
                .filter(|w| !refuses(w))
                .map(|w| w.to_string()),
        );
        assert!(wrong.is_empty(), "listed wrongly for SQLite: {wrong:?}");
    }
}
