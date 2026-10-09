//! sqlx integration for sql-composer.
//!
//! Provides verification of composed SQL against a live database connection
//! and optional syntax validation via sqlparser.

pub use sql_composer;

use sql_composer::composer::ComposedSql;

/// Errors specific to the sqlx integration.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from sql-composer core.
    #[error("composer error: {0}")]
    Composer(#[from] sql_composer::Error),

    /// An error from sqlx during verification.
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// SQL syntax validation failed (requires `validate` feature).
    #[error("SQL syntax error: {0}")]
    Syntax(String),
}

/// A specialized `Result` type for sqlx integration operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Verify composed SQL statements against a PostgreSQL database.
///
/// Connects to the database and attempts to `PREPARE` each statement.
/// This validates that the SQL syntax is correct and that referenced
/// tables/columns exist.
#[cfg(feature = "postgres")]
pub async fn verify_postgres(database_url: &str, statements: &[&ComposedSql]) -> Result<()> {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{AssertSqlSafe, Executor};

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for (i, stmt) in statements.iter().enumerate() {
        pool.execute(sqlx::query(AssertSqlSafe(format!(
            "PREPARE _sqlc_verify_{i} AS {}",
            stmt.sql
        ))))
        .await?;

        pool.execute(sqlx::query(AssertSqlSafe(format!(
            "DEALLOCATE _sqlc_verify_{i}"
        ))))
        .await?;
    }

    pool.close().await;
    Ok(())
}

/// Verify composed SQL statements against a MySQL database.
///
/// Connects to the database and prepares each statement on the server, which checks its
/// syntax and that the tables and columns it names exist.
#[cfg(feature = "mysql")]
pub async fn verify_mysql(database_url: &str, statements: &[&ComposedSql]) -> Result<()> {
    use sqlx::mysql::MySqlPoolOptions;
    use sqlx::{AssertSqlSafe, Executor, SqlSafeStr};

    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for stmt in statements {
        pool.prepare(AssertSqlSafe(stmt.sql.clone()).into_sql_str())
            .await?;
    }

    pool.close().await;
    Ok(())
}

/// Validate SQL syntax without a database connection.
///
/// Uses sqlparser to check that the composed SQL is syntactically valid.
/// This does not check table/column existence.
#[cfg(feature = "validate")]
pub fn validate_syntax(sql: &str, dialect: sql_composer::Dialect) -> Result<()> {
    use sqlparser::dialect::{DuckDbDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
    use sqlparser::parser::Parser;

    let dialect: Box<dyn sqlparser::dialect::Dialect> = match dialect {
        sql_composer::Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        sql_composer::Dialect::Mysql => Box::new(MySqlDialect {}),
        sql_composer::Dialect::Sqlite => Box::new(SQLiteDialect {}),
        sql_composer::Dialect::Mariadb => Box::new(MySqlDialect {}),
        sql_composer::Dialect::Duckdb => Box::new(DuckDbDialect {}),
        other => return Err(Error::Syntax(format!("no syntax check for {other:?}"))),
    };

    // Replace placeholders with literal values for parsing
    let normalized = normalize_placeholders(sql);
    Parser::parse_sql(dialect.as_ref(), &normalized).map_err(|e| Error::Syntax(e.to_string()))?;

    Ok(())
}

/// Replace dialect-specific placeholders with literal `1` for syntax validation.
#[cfg(feature = "validate")]
fn normalize_placeholders(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' || ch == '?' {
            // Skip the placeholder number
            let mut has_digits = false;
            while let Some(&next) = chars.peek() {
                if next.is_ascii_digit() {
                    chars.next();
                    has_digits = true;
                } else {
                    break;
                }
            }
            if has_digits || ch == '?' {
                result.push('1');
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {

    /// Tests that need a server, run with `cargo test --all-features -- --ignored` and the
    /// server's URL in the variable the test names.
    mod verify_tests {
        use sql_composer::composer::ComposedSql;

        fn statement(sql: &str) -> ComposedSql {
            ComposedSql {
                sql: sql.into(),
                bind_params: vec![],
                ..Default::default()
            }
        }

        fn run<F: std::future::Future>(future: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future)
        }

        #[cfg(feature = "postgres")]
        #[test]
        #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
        fn test_verify_postgres_refuses_what_the_server_refuses() {
            let url = std::env::var("SQLC_TEST_POSTGRES_URL").expect("SQLC_TEST_POSTGRES_URL");
            let good = statement("SELECT $1::integer + 1 AS n");
            run(async {
                crate::verify_postgres(&url, &[&good]).await.unwrap();
                for broken in ["SELEC 1", "SELECT * FROM sqlc_no_such_table"] {
                    let refused = crate::verify_postgres(&url, &[&good, &statement(broken)]).await;
                    assert!(refused.is_err(), "{broken}");
                }
            });
        }

        #[cfg(feature = "mysql")]
        #[test]
        #[ignore = "needs a MySQL server: set SQLC_TEST_MYSQL_URL"]
        fn test_verify_mysql_refuses_what_the_server_refuses() {
            let url = std::env::var("SQLC_TEST_MYSQL_URL").expect("SQLC_TEST_MYSQL_URL");
            let good = statement("SELECT ? + 1 AS n");
            run(async {
                crate::verify_mysql(&url, &[&good]).await.unwrap();
                for broken in ["SELEC 1", "SELECT * FROM sqlc_no_such_table"] {
                    let refused = crate::verify_mysql(&url, &[&good, &statement(broken)]).await;
                    assert!(refused.is_err(), "{broken}");
                }
            });
        }

        /// The words of `keywords` the server refuses as a `WITH` name and the dialect does not
        /// list, and the listed words it accepts.
        fn listed_wrongly(
            dialect: sql_composer::Dialect,
            keywords: Vec<String>,
            refuses: impl Fn(&str) -> bool,
        ) -> Vec<String> {
            let reserved = dialect.reserved_words();
            assert!(
                keywords.len() > 100,
                "the server listed {} keywords",
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
            wrong
        }

        #[cfg(feature = "postgres")]
        #[test]
        #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
        fn test_the_reserved_words_are_the_keywords_postgres_refuses_as_a_name() {
            use sqlx::{AssertSqlSafe, SqlSafeStr};
            let url = std::env::var("SQLC_TEST_POSTGRES_URL").expect("SQLC_TEST_POSTGRES_URL");
            run(async {
                let pool = sqlx::PgPool::connect(&url).await.unwrap();
                let keywords: Vec<String> =
                    sqlx::query_scalar("SELECT word FROM pg_get_keywords()")
                        .fetch_all(&pool)
                        .await
                        .unwrap();
                let mut refused = std::collections::HashSet::new();
                for word in keywords.iter().cloned().chain(
                    sql_composer::Dialect::Postgres
                        .reserved_words()
                        .iter()
                        .map(|w| w.to_string()),
                ) {
                    let sql = format!("WITH {word} AS (SELECT 1) SELECT * FROM {word}");
                    if sqlx::query(AssertSqlSafe(sql).into_sql_str())
                        .fetch_all(&pool)
                        .await
                        .is_err()
                    {
                        refused.insert(word);
                    }
                }
                let wrong = listed_wrongly(sql_composer::Dialect::Postgres, keywords, |w| {
                    refused.contains(w)
                });
                assert!(wrong.is_empty(), "listed wrongly for PostgreSQL: {wrong:?}");
            });
        }

        /// The words a MySQL or MariaDB server at `url` lists as keywords and `dialect` lists
        /// wrongly. MariaDB lists its operators among its keywords; only words can name a file.
        #[cfg(feature = "mysql")]
        fn listed_wrongly_by_mysql_server(
            url: &str,
            dialect: sql_composer::Dialect,
        ) -> Vec<String> {
            use sqlx::{AssertSqlSafe, SqlSafeStr};
            run(async {
                let pool = sqlx::MySqlPool::connect(url).await.unwrap();
                let keywords: Vec<String> = sqlx::query_scalar(
                    "SELECT CAST(LOWER(WORD) AS CHAR) FROM INFORMATION_SCHEMA.KEYWORDS \
                     WHERE WORD REGEXP '^[A-Za-z_][A-Za-z0-9_]*$'",
                )
                .fetch_all(&pool)
                .await
                .unwrap();
                let mut refused = std::collections::HashSet::new();
                for word in keywords
                    .iter()
                    .cloned()
                    .chain(dialect.reserved_words().iter().map(|w| w.to_string()))
                {
                    let sql = format!("WITH {word} AS (SELECT 1) SELECT * FROM {word}");
                    if sqlx::query(AssertSqlSafe(sql).into_sql_str())
                        .fetch_all(&pool)
                        .await
                        .is_err()
                    {
                        refused.insert(word);
                    }
                }
                listed_wrongly(dialect, keywords, |w| refused.contains(w))
            })
        }

        #[cfg(feature = "mysql")]
        #[test]
        #[ignore = "needs a MySQL server: set SQLC_TEST_MYSQL_URL"]
        fn test_the_reserved_words_are_the_keywords_mysql_refuses_as_a_name() {
            let url = std::env::var("SQLC_TEST_MYSQL_URL").expect("SQLC_TEST_MYSQL_URL");
            let wrong = listed_wrongly_by_mysql_server(&url, sql_composer::Dialect::Mysql);
            assert!(wrong.is_empty(), "listed wrongly for MySQL: {wrong:?}");
        }

        #[cfg(feature = "mysql")]
        #[test]
        #[ignore = "needs a MariaDB server: set SQLC_TEST_MARIADB_URL"]
        fn test_the_reserved_words_are_the_keywords_mariadb_refuses_as_a_name() {
            let url = std::env::var("SQLC_TEST_MARIADB_URL").expect("SQLC_TEST_MARIADB_URL");
            let wrong = listed_wrongly_by_mysql_server(&url, sql_composer::Dialect::Mariadb);
            assert!(wrong.is_empty(), "listed wrongly for MariaDB: {wrong:?}");
        }
    }
    #[cfg(feature = "validate")]
    mod validate_tests {
        use crate::{normalize_placeholders, validate_syntax};
        use sql_composer::Dialect;

        #[test]
        fn test_validate_syntax_postgres() {
            validate_syntax("SELECT 1", Dialect::Postgres).unwrap();
        }

        #[test]
        fn test_validate_syntax_mysql() {
            validate_syntax("SELECT 1", Dialect::Mysql).unwrap();
        }

        #[test]
        fn test_validate_syntax_sqlite() {
            validate_syntax("SELECT 1", Dialect::Sqlite).unwrap();
        }

        #[test]
        fn test_validate_syntax_mariadb_and_duckdb() {
            validate_syntax("SELECT * FROM t WHERE a = ? AND b = ?", Dialect::Mariadb).unwrap();
            validate_syntax("SELECT * FROM t WHERE a = $1", Dialect::Duckdb).unwrap();
            assert!(validate_syntax("SELECTT 1", Dialect::Mariadb).is_err());
            assert!(validate_syntax("SELECTT 1", Dialect::Duckdb).is_err());
        }

        #[test]
        fn test_validate_syntax_invalid() {
            let result = validate_syntax("SELECTT 1 FROMM", Dialect::Postgres);
            assert!(result.is_err());
        }

        #[test]
        fn test_validate_syntax_with_placeholders() {
            // Placeholders get normalized to `1` before parsing
            validate_syntax("SELECT * FROM users WHERE id = $1", Dialect::Postgres).unwrap();
        }

        #[test]
        fn test_normalize_placeholders_postgres() {
            assert_eq!(normalize_placeholders("$1"), "1");
            assert_eq!(normalize_placeholders("$10"), "1");
            assert_eq!(
                normalize_placeholders("WHERE a = $1 AND b = $2"),
                "WHERE a = 1 AND b = 1"
            );
        }

        #[test]
        fn test_normalize_placeholders_mysql() {
            assert_eq!(normalize_placeholders("?"), "1");
            assert_eq!(
                normalize_placeholders("WHERE a = ? AND b = ?"),
                "WHERE a = 1 AND b = 1"
            );
        }

        #[test]
        fn test_normalize_placeholders_sqlite() {
            assert_eq!(normalize_placeholders("?1"), "1");
            assert_eq!(
                normalize_placeholders("WHERE a = ?1 AND b = ?2"),
                "WHERE a = 1 AND b = 1"
            );
        }

        #[test]
        fn test_normalize_preserves_dollar_without_digits() {
            // A bare $ not followed by digits should be preserved
            assert_eq!(normalize_placeholders("$"), "$");
        }
    }
}
