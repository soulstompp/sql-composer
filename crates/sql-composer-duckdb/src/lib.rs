//! DuckDB driver for sql-composer.
//!
//! Provides [`DuckDbConnection`], a thin wrapper around [`duckdb::Connection`]
//! that implements [`ComposerConnection`] for composing SQL templates with bind
//! values against DuckDB databases.
//!
//! # Example
//!
//! ```no_run
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnection;
//! use sql_composer::parser::parse_template;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_duckdb::DuckDbConnection;
//!
//! let conn = DuckDbConnection::open_in_memory().unwrap();
//! conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", []).unwrap();
//!
//! let input = "SELECT * FROM users WHERE id = :bind(user_id)";
//! let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
//! let composer = Composer::new(Dialect::Duckdb);
//!
//! let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn duckdb::ToSql>]);
//! let (sql, params) = conn.compose(&composer, &template, values).unwrap();
//!
//! let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|v| v.as_ref()).collect();
//! let mut stmt = conn.prepare(&sql).unwrap();
//! let _rows = stmt.query(refs.as_slice()).unwrap();
//! ```

pub use duckdb;

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use sql_composer::composer::Composer;
use sql_composer::driver::{self, ComposerConnection};
use sql_composer::types::Template;

/// Error type for sql-composer-duckdb operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the sql-composer core.
    #[error(transparent)]
    Composer(#[from] sql_composer::Error),

    /// An error from duckdb.
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
}

/// A wrapper around [`duckdb::Connection`] that implements [`ComposerConnection`].
///
/// Dereferences to the inner `duckdb::Connection`, so all native methods
/// are available directly.
pub struct DuckDbConnection(pub duckdb::Connection);

impl DuckDbConnection {
    /// Open an in-memory DuckDB database.
    pub fn open_in_memory() -> Result<Self, duckdb::Error> {
        duckdb::Connection::open_in_memory().map(Self)
    }

    /// Open a DuckDB database at the given path.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, duckdb::Error> {
        duckdb::Connection::open(path).map(Self)
    }

    /// Wrap an existing `duckdb::Connection`.
    pub fn from_connection(conn: duckdb::Connection) -> Self {
        Self(conn)
    }
}

impl Deref for DuckDbConnection {
    type Target = duckdb::Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DuckDbConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ComposerConnection for DuckDbConnection {
    type Value = Box<dyn duckdb::ToSql>;
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
fn is_null(value: &dyn duckdb::ToSql) -> bool {
    use duckdb::types::{ToSqlOutput, Value, ValueRef};
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

    fn boxed(v: impl duckdb::ToSql + 'static) -> Box<dyn duckdb::ToSql> {
        Box::new(v)
    }

    #[test]
    fn test_compose_and_query() {
        let conn = DuckDbConnection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
            .unwrap();
        conn.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')", [])
            .unwrap();

        // DuckDB numbers its placeholders: $1, $2
        let input = "SELECT id, name FROM users WHERE id = :bind(user_id)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Duckdb);

        let values = bind_values!("user_id" => [boxed(1i32)]);
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(sql, "SELECT id, name FROM users WHERE id = $1");

        let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|v| v.as_ref()).collect();
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
        let conn = DuckDbConnection::open_in_memory().unwrap();
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
        let composer = Composer::new(Dialect::Duckdb);

        let values = bind_values!("ids" => [boxed(1i32), boxed(3i32)]);
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(
            sql,
            "SELECT id, label FROM items WHERE id IN ($1, $2) ORDER BY id"
        );

        let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|v| v.as_ref()).collect();
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
        let conn = DuckDbConnection::open_in_memory().unwrap();

        let input = "SELECT :bind(a) AS col_1, :bind(b) AS col_2";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Duckdb);

        let values = bind_values!(
            "a" => [boxed("hello")],
            "b" => [boxed("world")]
        );
        let (sql, params) = conn.compose(&composer, &template, values).unwrap();

        assert_eq!(sql, "SELECT $1 AS col_1, $2 AS col_2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_a_null_is_refused_where_the_bind_does_not_take_it() {
        let conn = DuckDbConnection::open_in_memory().unwrap();
        let composer = Composer::new(Dialect::Duckdb);
        let refusing = parse_template(
            "SELECT CAST(:bind(a) AS INTEGER) AS a",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();
        let taking = parse_template(
            "SELECT CAST(:bind(a NULL) AS INTEGER) AS a",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();

        for null in [boxed(None::<i32>), boxed(duckdb::types::Null)] {
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
        let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let a: Option<i32> = conn
            .query_row(&sql, refs.as_slice(), |row| row.get(0))
            .unwrap();
        assert_eq!(a, None);
    }

    #[test]
    fn test_the_reserved_words_are_the_keywords_duckdb_refuses_as_a_name() {
        let conn = DuckDbConnection::open_in_memory().unwrap();
        let refuses = |word: &str| {
            conn.prepare(&format!("WITH {word} AS (SELECT 1) SELECT * FROM {word}"))
                .is_err()
        };
        let reserved = Dialect::Duckdb.reserved_words();
        let keywords: Vec<String> = conn
            .prepare("SELECT keyword_name FROM duckdb_keywords()")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            keywords.len() > 100,
            "DuckDB listed {} keywords",
            keywords.len()
        );

        let mut wrong: Vec<String> = keywords
            .into_iter()
            .filter(|word| refuses(word) != reserved.contains(&word.as_str()))
            .collect();
        wrong.extend(
            reserved
                .iter()
                .filter(|w| !refuses(w))
                .map(|w| w.to_string()),
        );
        assert!(wrong.is_empty(), "listed wrongly for DuckDB: {wrong:?}");
    }

    #[test]
    fn test_a_mock_reads_back_as_written() {
        let conn = DuckDbConnection::open_in_memory().unwrap();
        let written = [r"C:\temp", "O'Brien", r"a\'b"];
        let mock = written.iter().fold(
            sql_composer::MockTable::new("names").column("name", "varchar"),
            |mock, name| mock.row(&[Some(name)]),
        );
        let sql = format!(
            "SELECT name FROM ({}) m ORDER BY name",
            mock.to_sql(Dialect::Duckdb)
        );
        let read: Vec<String> = conn
            .prepare(&sql)
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(read, written);
    }
}
