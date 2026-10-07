//! MySQL driver for sql-composer (sync and async).
//!
//! Provides both sync and async wrappers for composing SQL templates
//! with bind values against MySQL databases.
//!
//! - **Async**: [`MysqlConn`] wraps [`mysql_async::Conn`] (feature `async`, enabled by default)
//! - **Sync**: [`MysqlConnection`] wraps [`mysql::Conn`] (feature `sync`, enabled by default)
//!
//! # Async Example
//!
//! ```ignore
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnectionAsync;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_mysql::MysqlConn;
//!
//! let pool = mysql_async::Pool::new("mysql://root@localhost/test");
//! let conn = pool.get_conn().await?;
//! let conn = MysqlConn::from_conn(conn);
//!
//! let template = sql_composer::parser::parse_template(
//!     "SELECT * FROM users WHERE id = :bind(user_id)",
//!     TemplateSource::Literal("example".into()),
//! )?;
//! let composer = Composer::new(Dialect::Mysql);
//! let values = bind_values!("user_id" => [mysql_async::Value::from(1i32)]);
//! let (sql, params) = conn.compose(&composer, &template, values).await?;
//! ```
//!
//! # Sync Example
//!
//! ```ignore
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnection;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_mysql::MysqlConnection;
//!
//! let conn = mysql::Conn::new("mysql://root@localhost/test")?;
//! let conn = MysqlConnection::from_conn(conn);
//!
//! let template = sql_composer::parser::parse_template(
//!     "SELECT * FROM users WHERE id = :bind(user_id)",
//!     TemplateSource::Literal("example".into()),
//! )?;
//! let composer = Composer::new(Dialect::Mysql);
//! let values = bind_values!("user_id" => [mysql::Value::from(1i32)]);
//! let (sql, params) = conn.compose(&composer, &template, values)?;
//! ```

#[cfg(feature = "async")]
pub use mysql_async;

#[cfg(feature = "sync")]
pub use mysql;

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use sql_composer::composer::Composer;
use sql_composer::driver;
use sql_composer::types::Template;

/// Compose for MySQL's positional `?`: a name used twice is filled at each use, and a value
/// `is_null` calls NULL is refused where its `:bind` does not say `NULL`.
fn compose_positional<V: Clone>(
    composer: &Composer,
    template: &Template,
    values: &BTreeMap<String, Vec<V>>,
    is_null: impl Fn(&V) -> bool,
) -> Result<(String, Vec<V>), sql_composer::Error> {
    let composed = composer.compose_with_values_and_nulls(template, values, is_null)?;
    let ordered = driver::resolve_values_by_use(&composed, values)?;
    Ok((composed.sql, ordered))
}

#[cfg(feature = "async")]
fn is_null_async(value: &mysql_async::Value) -> bool {
    matches!(value, mysql_async::Value::NULL)
}

#[cfg(feature = "sync")]
fn is_null_sync(value: &mysql::Value) -> bool {
    matches!(value, mysql::Value::NULL)
}

// ---------------------------------------------------------------------------
// Async: MysqlConn (mysql_async)
// ---------------------------------------------------------------------------

/// Error type for async sql-composer-mysql operations.
#[cfg(feature = "async")]
#[derive(Debug, thiserror::Error)]
pub enum AsyncError {
    /// An error from the sql-composer core.
    #[error(transparent)]
    Composer(#[from] sql_composer::Error),

    /// An error from mysql_async.
    #[error(transparent)]
    Mysql(#[from] mysql_async::Error),
}

/// A wrapper around [`mysql_async::Conn`] that implements [`sql_composer::driver::ComposerConnectionAsync`].
///
/// Dereferences to the inner `mysql_async::Conn`, so all native async
/// methods are available directly.
#[cfg(feature = "async")]
pub struct MysqlConn(pub mysql_async::Conn);

#[cfg(feature = "async")]
impl MysqlConn {
    /// Wrap an existing `mysql_async::Conn`.
    pub fn from_conn(conn: mysql_async::Conn) -> Self {
        Self(conn)
    }
}

#[cfg(feature = "async")]
impl Deref for MysqlConn {
    type Target = mysql_async::Conn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "async")]
impl DerefMut for MysqlConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(feature = "async")]
impl driver::ComposerConnectionAsync for MysqlConn {
    type Value = mysql_async::Value;
    type Statement = String;
    type Error = AsyncError;

    async fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), AsyncError> {
        Ok(compose_positional(
            composer,
            template,
            &values,
            is_null_async,
        )?)
    }
}

// ---------------------------------------------------------------------------
// Sync: MysqlConnection (mysql)
// ---------------------------------------------------------------------------

/// Error type for sync sql-composer-mysql operations.
#[cfg(feature = "sync")]
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// An error from the sql-composer core.
    #[error(transparent)]
    Composer(#[from] sql_composer::Error),

    /// An error from mysql.
    #[error(transparent)]
    Mysql(#[from] mysql::Error),
}

/// A wrapper around [`mysql::Conn`] that implements [`sql_composer::driver::ComposerConnection`].
///
/// Dereferences to the inner `mysql::Conn`, so all native sync methods
/// are available directly.
#[cfg(feature = "sync")]
pub struct MysqlConnection(pub mysql::Conn);

#[cfg(feature = "sync")]
impl MysqlConnection {
    /// Wrap an existing `mysql::Conn`.
    pub fn from_conn(conn: mysql::Conn) -> Self {
        Self(conn)
    }
}

#[cfg(feature = "sync")]
impl Deref for MysqlConnection {
    type Target = mysql::Conn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "sync")]
impl DerefMut for MysqlConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(feature = "sync")]
impl driver::ComposerConnection for MysqlConnection {
    type Value = mysql::Value;
    type Statement = String;
    type Error = SyncError;

    fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), SyncError> {
        Ok(compose_positional(
            composer,
            template,
            &values,
            is_null_sync,
        )?)
    }
}

// Backward-compatible type alias when both features are enabled
/// Error type alias — resolves to [`AsyncError`] for backward compatibility.
#[cfg(feature = "async")]
pub type Error = AsyncError;

#[cfg(test)]
mod tests {
    use sql_composer::composer::Composer;
    use sql_composer::parser::parse_template;
    use sql_composer::types::{Dialect, TemplateSource};

    #[test]
    fn test_compose_single_bind_mysql() {
        let input = "SELECT * FROM users WHERE id = :bind(user_id)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Mysql);
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id = ?");
        assert_eq!(result.bind_params, vec!["user_id"]);
    }

    #[test]
    fn test_compose_multiple_binds_mysql() {
        let input = "SELECT * FROM users WHERE name = :bind(name) AND active = :bind(active)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Mysql);
        let result = composer.compose(&template).unwrap();
        // MySQL: document order, bare ?
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE name = ? AND active = ?"
        );
        assert_eq!(result.bind_params, vec!["name", "active"]);
    }

    #[test]
    fn test_compose_with_values_multi_bind_mysql() {
        let input = "SELECT * FROM users WHERE id IN (:bind(ids))";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Mysql);
        let values = sql_composer::bind_values!("ids" => [10, 20, 30]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN (?, ?, ?)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_a_name_used_twice_gets_its_value_twice() {
        let input = "SELECT * FROM users WHERE id = :bind(id) OR manager_id = :bind(id)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Mysql);
        let values = sql_composer::bind_values!("id" => [mysql_async::Value::from(7)]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ? OR manager_id = ?"
        );
        let ordered = sql_composer::driver::resolve_values_by_use(&result, &values).unwrap();
        assert_eq!(
            ordered,
            vec![mysql_async::Value::from(7), mysql_async::Value::from(7)]
        );
    }

    #[test]
    fn test_a_null_is_refused_where_the_bind_does_not_take_it() {
        let composer = Composer::new(Dialect::Mysql);
        let refusing =
            parse_template("SELECT :bind(a)", TemplateSource::Literal("test".into())).unwrap();
        let taking = parse_template(
            "SELECT :bind(a NULL)",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();

        #[cfg(feature = "async")]
        {
            use mysql_async::Value;
            let null = sql_composer::bind_values!("a" => [Value::NULL]);
            let seven = sql_composer::bind_values!("a" => [Value::from(7)]);
            assert!(matches!(
                super::compose_positional(&composer, &refusing, &null, super::is_null_async),
                Err(sql_composer::Error::NullValue { .. })
            ));
            assert!(
                super::compose_positional(&composer, &refusing, &seven, super::is_null_async)
                    .is_ok()
            );
            let (_, ordered) =
                super::compose_positional(&composer, &taking, &null, super::is_null_async).unwrap();
            assert_eq!(ordered, vec![Value::NULL]);
        }
        #[cfg(feature = "sync")]
        {
            use mysql::Value;
            let null = sql_composer::bind_values!("a" => [Value::NULL]);
            assert!(matches!(
                super::compose_positional(&composer, &refusing, &null, super::is_null_sync),
                Err(sql_composer::Error::NullValue { .. })
            ));
            assert!(
                super::compose_positional(&composer, &taking, &null, super::is_null_sync).is_ok()
            );
        }
    }
}
