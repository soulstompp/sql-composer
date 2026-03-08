//! PostgreSQL driver for sql-composer (sync and async).
//!
//! Provides both sync and async wrappers for composing SQL templates
//! with bind values against PostgreSQL databases.
//!
//! - **Async**: [`PgClient`] wraps [`tokio_postgres::Client`] (feature `async`, enabled by default)
//! - **Sync**: [`PgConnection`] wraps [`postgres::Client`] (feature `sync`, enabled by default)
//!
//! # Async Example
//!
//! ```ignore
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnectionAsync;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_postgres::{PgClient, boxed_params};
//!
//! let (client, connection) = tokio_postgres::connect("host=localhost", tokio_postgres::NoTls).await?;
//! tokio::spawn(connection);
//! let client = PgClient::from_client(client);
//!
//! let template = sql_composer::parser::parse_template(
//!     "SELECT * FROM users WHERE id = :bind(user_id)",
//!     TemplateSource::Literal("example".into()),
//! )?;
//! let composer = Composer::new(Dialect::Postgres);
//! let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>]);
//! let (sql, params) = client.compose(&composer, &template, values).await?;
//! let refs = boxed_params(&params);
//! let rows = client.query(&sql as &str, &refs).await?;
//! ```
//!
//! # Sync Example
//!
//! ```ignore
//! use sql_composer::composer::Composer;
//! use sql_composer::driver::ComposerConnection;
//! use sql_composer::types::{Dialect, TemplateSource};
//! use sql_composer::bind_values;
//! use sql_composer_postgres::{PgConnection, boxed_params_sync};
//!
//! let mut client = postgres::Client::connect("host=localhost", postgres::NoTls)?;
//! let conn = PgConnection::from_client(client);
//!
//! let template = sql_composer::parser::parse_template(
//!     "SELECT * FROM users WHERE id = :bind(user_id)",
//!     TemplateSource::Literal("example".into()),
//! )?;
//! let composer = Composer::new(Dialect::Postgres);
//! let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn postgres::types::ToSql + Sync>]);
//! let (sql, params) = conn.compose(&composer, &template, values)?;
//! let refs = boxed_params_sync(&params);
//! let rows = conn.query(&sql as &str, &refs)?;
//! ```

pub use tokio_postgres;

#[cfg(feature = "sync")]
pub use postgres;

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use sql_composer::composer::Composer;
use sql_composer::driver;
use sql_composer::types::Template;

/// Error type for sql-composer-postgres operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the sql-composer core.
    #[error(transparent)]
    Composer(#[from] sql_composer::Error),

    /// An error from tokio-postgres (shared by both sync and async postgres crates).
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}

// ---------------------------------------------------------------------------
// Async: PgClient (tokio-postgres)
// ---------------------------------------------------------------------------

/// A wrapper around [`tokio_postgres::Client`] that implements
/// [`sql_composer::driver::ComposerConnectionAsync`].
///
/// Dereferences to the inner `tokio_postgres::Client`, so all native async
/// methods are available directly.
#[cfg(feature = "async")]
pub struct PgClient(pub tokio_postgres::Client);

#[cfg(feature = "async")]
impl PgClient {
    /// Wrap an existing `tokio_postgres::Client`.
    pub fn from_client(client: tokio_postgres::Client) -> Self {
        Self(client)
    }
}

#[cfg(feature = "async")]
impl Deref for PgClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "async")]
impl DerefMut for PgClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Helper to convert boxed async params into the reference slice
/// that tokio-postgres query methods expect.
#[cfg(feature = "async")]
pub fn boxed_params(
    params: &[Box<dyn tokio_postgres::types::ToSql + Sync + Send>],
) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
    params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect()
}

#[cfg(feature = "async")]
impl driver::ComposerConnectionAsync for PgClient {
    type Value = Box<dyn tokio_postgres::types::ToSql + Sync + Send>;
    type Statement = String;
    type Error = Error;

    async fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        mut values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), Error> {
        let composed = composer.compose_with_values(template, &values)?;
        let ordered = driver::resolve_values(&composed, &mut values)?;
        Ok((composed.sql, ordered))
    }
}

// ---------------------------------------------------------------------------
// Sync: PgConnection (postgres)
// ---------------------------------------------------------------------------

/// A wrapper around [`postgres::Client`] that implements [`sql_composer::driver::ComposerConnection`].
///
/// Dereferences to the inner `postgres::Client`, so all native sync methods
/// are available directly.
#[cfg(feature = "sync")]
pub struct PgConnection(pub postgres::Client);

#[cfg(feature = "sync")]
impl PgConnection {
    /// Wrap an existing `postgres::Client`.
    pub fn from_client(client: postgres::Client) -> Self {
        Self(client)
    }
}

#[cfg(feature = "sync")]
impl Deref for PgConnection {
    type Target = postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "sync")]
impl DerefMut for PgConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Helper to convert boxed sync params into the reference slice
/// that postgres query methods expect.
#[cfg(feature = "sync")]
pub fn boxed_params_sync(
    params: &[Box<dyn postgres::types::ToSql + Sync>],
) -> Vec<&(dyn postgres::types::ToSql + Sync)> {
    params
        .iter()
        .map(|p| p.as_ref() as &(dyn postgres::types::ToSql + Sync))
        .collect()
}

#[cfg(feature = "sync")]
impl driver::ComposerConnection for PgConnection {
    type Value = Box<dyn postgres::types::ToSql + Sync>;
    type Statement = String;
    type Error = Error;

    fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        mut values: BTreeMap<String, Vec<Self::Value>>,
    ) -> Result<(String, Vec<Self::Value>), Error> {
        let composed = composer.compose_with_values(template, &values)?;
        let ordered = driver::resolve_values(&composed, &mut values)?;
        Ok((composed.sql, ordered))
    }
}

#[cfg(test)]
mod tests {
    use sql_composer::composer::Composer;
    use sql_composer::parser::parse_template;
    use sql_composer::types::{Dialect, TemplateSource};

    #[test]
    fn test_compose_single_bind_postgres() {
        let input = "SELECT * FROM users WHERE id = :bind(user_id)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Postgres);
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id = $1");
        assert_eq!(result.bind_params, vec!["user_id"]);
    }

    #[test]
    fn test_compose_multiple_binds_postgres() {
        let input = "SELECT * FROM users WHERE name = :bind(name) AND active = :bind(active)";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Postgres);
        let result = composer.compose(&template).unwrap();
        // Alphabetical: active=$1, name=$2
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE name = $2 AND active = $1"
        );
        assert_eq!(result.bind_params, vec!["active", "name"]);
    }

    #[test]
    fn test_compose_with_values_multi_bind_postgres() {
        let input = "SELECT * FROM users WHERE id IN (:bind(ids))";
        let template = parse_template(input, TemplateSource::Literal("test".into())).unwrap();
        let composer = Composer::new(Dialect::Postgres);
        let values = sql_composer::bind_values!("ids" => [10, 20, 30]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN ($1, $2, $3)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }
}
