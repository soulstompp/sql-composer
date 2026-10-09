//! Mock tables: rows of your own that stand for a table in a composed statement.
//!
//! A composed statement that names a mocked table gets the mock at its head, as
//! `<name> AS (<rows>)` in its `WITH`. A `WITH` name stands for the table of the same name wherever
//! the statement names it bare, so the statement reads the mock's rows instead of the table's,
//! and the composer still never parses the SQL it splices.

use crate::types::Dialect;

/// A column of a mock table: its name, and its type as the dialect spells it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockColumn {
    /// The column's name.
    pub name: String,
    /// The column's type, written into each value's `CAST`.
    pub sql_type: String,
}

/// Rows of your own for a table, given to a [`Composer`](crate::Composer) with
/// [`add_mock_table`](crate::Composer::add_mock_table).
///
/// ```
/// use sql_composer::{Dialect, MockTable};
///
/// let sets = MockTable::new("lego_sets")
///     .column("set_num", "varchar")
///     .column("year", "integer")
///     .row(&[Some("6000-1"), Some("1990")])
///     .row(&[Some("6001-1"), None]);
///
/// assert_eq!(
///     sets.to_sql(Dialect::Postgres),
///     "SELECT CAST('6000-1' AS varchar) AS set_num, CAST('1990' AS integer) AS year\n\
///      UNION ALL\n\
///      SELECT CAST('6001-1' AS varchar), CAST(NULL AS integer)"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockTable {
    /// The table the rows stand for, as statements name it, bare.
    pub name: String,
    /// The columns, in order.
    pub columns: Vec<MockColumn>,
    /// The rows, one value for each column; `None` is `NULL`.
    pub rows: Vec<Vec<Option<String>>>,
}

impl MockTable {
    /// A mock of the table `name`, with no columns and no rows yet.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Add a column, after the ones already declared, with its type as the dialect spells it.
    pub fn column(mut self, name: impl Into<String>, sql_type: impl Into<String>) -> Self {
        self.columns.push(MockColumn {
            name: name.into(),
            sql_type: sql_type.into(),
        });
        self
    }

    /// Add a row: one value for each column, in the order the columns were declared, and `None`
    /// for `NULL`.
    ///
    /// # Panics
    ///
    /// When the row does not hold one value for each declared column.
    pub fn row(mut self, values: &[Option<&str>]) -> Self {
        assert_eq!(
            values.len(),
            self.columns.len(),
            "a row of the mock {} holds {} values for {} columns",
            self.name,
            values.len(),
            self.columns.len()
        );
        self.rows
            .push(values.iter().map(|v| v.map(str::to_string)).collect());
        self
    }

    /// The mock's rows as a query: one `SELECT` for each row, joined by `UNION ALL`, every value
    /// cast to its column's type and the first row naming the columns. A mock with no rows is
    /// that first row of `NULL`s with `LIMIT 0`, so its columns exist and it returns nothing.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        let select = |row: &[Option<String>], named: bool| {
            let values: Vec<String> = self
                .columns
                .iter()
                .zip(row)
                .map(|(column, value)| {
                    let literal = match value {
                        Some(v) => literal(v, dialect),
                        None => "NULL".to_string(),
                    };
                    let cast = format!("CAST({literal} AS {})", column.sql_type);
                    if named {
                        format!("{cast} AS {}", column.name)
                    } else {
                        cast
                    }
                })
                .collect();
            format!("SELECT {}", values.join(", "))
        };

        if self.rows.is_empty() {
            let nulls = vec![None; self.columns.len()];
            return format!("{} LIMIT 0", select(&nulls, true));
        }
        self.rows
            .iter()
            .enumerate()
            .map(|(i, row)| select(row, i == 0))
            .collect::<Vec<_>>()
            .join("\nUNION ALL\n")
    }
}

/// `value` as a string literal of the dialect: quotes doubled, and on MySQL and MariaDB, whose
/// strings take backslash escapes, backslashes doubled too.
fn literal(value: &str, dialect: Dialect) -> String {
    let mut escaped = value.replace('\'', "''");
    if matches!(dialect, Dialect::Mysql | Dialect::Mariadb) {
        escaped = escaped.replace('\\', "\\\\");
    }
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn people() -> MockTable {
        MockTable::new("people")
            .column("id", "integer")
            .column("name", "varchar")
    }

    #[test]
    fn test_one_row_names_and_casts_its_columns() {
        let mock = people().row(&[Some("1"), Some("Alice")]);
        assert_eq!(
            mock.to_sql(Dialect::Postgres),
            "SELECT CAST('1' AS integer) AS id, CAST('Alice' AS varchar) AS name"
        );
    }

    #[test]
    fn test_later_rows_are_joined_by_union_all_unnamed() {
        let mock = people()
            .row(&[Some("1"), Some("Alice")])
            .row(&[Some("2"), Some("Bob")]);
        assert_eq!(
            mock.to_sql(Dialect::Postgres),
            "SELECT CAST('1' AS integer) AS id, CAST('Alice' AS varchar) AS name\nUNION ALL\n\
             SELECT CAST('2' AS integer), CAST('Bob' AS varchar)"
        );
    }

    #[test]
    fn test_none_is_a_typed_null_and_the_word_null_is_a_string() {
        let mock = people()
            .row(&[Some("1"), None])
            .row(&[Some("2"), Some("NULL")]);
        let sql = mock.to_sql(Dialect::Postgres);
        assert!(sql.contains("CAST(NULL AS varchar) AS name"), "{sql}");
        assert!(sql.contains("CAST('NULL' AS varchar)"), "{sql}");
    }

    #[test]
    fn test_quotes_are_doubled() {
        let mock = people().row(&[Some("1"), Some("O'Brien")]);
        assert!(mock.to_sql(Dialect::Sqlite).contains("'O''Brien'"));
    }

    #[test]
    fn test_backslashes_are_doubled_on_mysql_and_mariadb_only() {
        let mock = people().row(&[Some("1"), Some(r"C:\temp")]);
        for dialect in [Dialect::Mysql, Dialect::Mariadb] {
            assert!(mock.to_sql(dialect).contains(r"'C:\\temp'"), "{dialect:?}");
        }
        for dialect in [Dialect::Postgres, Dialect::Sqlite, Dialect::Duckdb] {
            assert!(mock.to_sql(dialect).contains(r"'C:\temp'"), "{dialect:?}");
        }
    }

    #[test]
    fn test_no_rows_keeps_the_columns_and_returns_nothing() {
        assert_eq!(
            people().to_sql(Dialect::Postgres),
            "SELECT CAST(NULL AS integer) AS id, CAST(NULL AS varchar) AS name LIMIT 0"
        );
    }

    #[test]
    #[should_panic(expected = "a row of the mock people holds 1 values for 2 columns")]
    fn test_a_row_must_hold_a_value_for_each_column() {
        let _ = people().row(&[Some("1")]);
    }
}
