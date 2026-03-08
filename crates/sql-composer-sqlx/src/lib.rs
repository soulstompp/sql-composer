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
    use sqlx::Executor;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for (i, stmt) in statements.iter().enumerate() {
        pool.execute(sqlx::query(&format!(
            "PREPARE _sqlc_verify_{i} AS {}",
            stmt.sql
        )))
        .await?;

        pool.execute(sqlx::query(&format!("DEALLOCATE _sqlc_verify_{i}")))
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
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
    use sqlparser::parser::Parser;

    let dialect: Box<dyn sqlparser::dialect::Dialect> = match dialect {
        sql_composer::Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        sql_composer::Dialect::Mysql => Box::new(MySqlDialect {}),
        sql_composer::Dialect::Sqlite => Box::new(SQLiteDialect {}),
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
