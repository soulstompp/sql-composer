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
        mut values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), AsyncError> {
        let composed = composer.compose_with_values(template, &values)?;
        let ordered = driver::resolve_values(&composed, &mut values)?;
        Ok((composed.sql, ordered))
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
        mut values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), SyncError> {
        let composed = composer.compose_with_values(template, &values)?;
        let ordered = driver::resolve_values(&composed, &mut values)?;
        Ok((composed.sql, ordered))
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
}
