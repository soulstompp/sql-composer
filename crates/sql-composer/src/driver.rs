//! Trait interface for database drivers and helpers for bind value resolution.
//!
//! Driver crates implement [`ComposerConnection`] (sync) or
//! [`ComposerConnectionAsync`] (async) for their connection types.
//! This module contains no database dependencies — only the interface.

use std::collections::BTreeMap;

use crate::composer::{ComposedSql, Composer};
use crate::error::{Error, Result};
use crate::types::Template;

/// Trait for synchronous database drivers that can compose and prepare SQL.
///
/// Each driver crate implements this for its connection type, providing the
/// bridge between sql-composer's template system and the database's API.
///
/// # Example
///
/// ```ignore
/// let (sql, values) = conn.compose(&composer, &template, bind_values!("id" => [1]))?;
/// let mut stmt = conn.prepare(&sql)?;
/// let rows = stmt.query_map(params_from_iter(values.iter().map(|v| v.as_ref())), |row| { ... })?;
/// ```
pub trait ComposerConnection {
    /// The database-specific value type for bind parameters.
    ///
    /// e.g. `Box<dyn rusqlite::types::ToSql>` or `Box<dyn duckdb::ToSql>`
    type Value;

    /// The composed SQL string (callers use this to prepare statements).
    type Statement;

    /// The error type for this driver.
    type Error: From<Error>;

    /// Compose a template with bind values, returning prepared SQL and ordered values.
    ///
    /// Takes the composer, a parsed template, and a map of named bind values.
    /// Resolves bind parameter order and returns the SQL string with ordered
    /// values ready for execution.
    #[allow(clippy::type_complexity)]
    fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        values: BTreeMap<String, Vec<Self::Value>>,
    ) -> std::result::Result<(Self::Statement, Vec<Self::Value>), Self::Error>;
}

/// Async version of [`ComposerConnection`] for async database drivers
/// (e.g. tokio-postgres, mysql_async).
pub trait ComposerConnectionAsync {
    /// The database-specific value type for bind parameters.
    type Value;

    /// The composed SQL string.
    type Statement;

    /// The error type for this driver.
    type Error: From<Error>;

    /// Compose a template with bind values asynchronously.
    #[allow(clippy::type_complexity)]
    fn compose(
        &self,
        composer: &Composer,
        template: &Template,
        values: BTreeMap<String, Vec<Self::Value>>,
    ) -> impl std::future::Future<
        Output = std::result::Result<(Self::Statement, Vec<Self::Value>), Self::Error>,
    > + Send;
}

/// Given a [`ComposedSql`] with ordered bind param names and a map of named values,
/// produce the ordered value vector matching placeholder order.
///
/// For single-value bindings, each name maps to one value.
/// For multi-value bindings (e.g. IN clauses), the composed SQL already has
/// the correct number of placeholders per binding name, so we flatten all
/// values for each occurrence.
pub fn resolve_values<V>(
    composed: &ComposedSql,
    values: &mut BTreeMap<String, Vec<V>>,
) -> Result<Vec<V>> {
    let mut result = Vec::with_capacity(composed.bind_params.len());

    for name in &composed.bind_params {
        let vs = values
            .get_mut(name)
            .ok_or_else(|| Error::MissingBinding { name: name.clone() })?;

        if vs.is_empty() {
            return Err(Error::MissingBinding { name: name.clone() });
        }

        // Take the first value — each placeholder in bind_params corresponds
        // to exactly one value. Multi-value bindings have been expanded by
        // compose_with_values() so each placeholder gets its own entry.
        result.push(vs.remove(0));
    }

    Ok(result)
}

/// Build a `BTreeMap<String, Vec<V>>` of named bind values.
///
/// # Example
///
/// ```
/// use sql_composer::bind_values;
///
/// let values: std::collections::BTreeMap<String, Vec<i32>> = bind_values!(
///     "user_id" => [42],
///     "status" => [1, 2, 3],
/// );
/// assert_eq!(values["user_id"], vec![42]);
/// assert_eq!(values["status"], vec![1, 2, 3]);
/// ```
#[macro_export]
macro_rules! bind_values {
    ($($key:literal => [$($value:expr),+ $(,)?]),+ $(,)?) => {{
        let mut map = std::collections::BTreeMap::new();
        $(
            map.insert($key.to_string(), vec![$($value),+]);
        )+
        map
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_values_basic() {
        let composed = ComposedSql {
            sql: "SELECT * FROM t WHERE a = $1 AND b = $2".into(),
            bind_params: vec!["a".into(), "b".into()],
        };
        let mut values: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        values.insert("a".into(), vec!["hello"]);
        values.insert("b".into(), vec!["world"]);

        let result = resolve_values(&composed, &mut values).unwrap();
        assert_eq!(result, vec!["hello", "world"]);
    }

    #[test]
    fn test_resolve_values_missing_binding() {
        let composed = ComposedSql {
            sql: "SELECT * FROM t WHERE a = $1".into(),
            bind_params: vec!["missing".into()],
        };
        let mut values: BTreeMap<String, Vec<&str>> = BTreeMap::new();

        let err = resolve_values(&composed, &mut values).unwrap_err();
        assert!(matches!(err, Error::MissingBinding { ref name } if name == "missing"));
    }

    #[test]
    fn test_resolve_values_multi_value_expanded() {
        // After compose_with_values, a multi-value binding like ids=[1,2,3]
        // produces bind_params = ["ids", "ids", "ids"] with placeholders $1, $2, $3.
        let composed = ComposedSql {
            sql: "SELECT * FROM t WHERE id IN ($1, $2, $3)".into(),
            bind_params: vec!["ids".into(), "ids".into(), "ids".into()],
        };
        let mut values: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        values.insert("ids".into(), vec![10, 20, 30]);

        let result = resolve_values(&composed, &mut values).unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn test_bind_values_macro() {
        let values: BTreeMap<String, Vec<i32>> = bind_values!(
            "a" => [1, 2],
            "b" => [3],
        );
        assert_eq!(values["a"], vec![1, 2]);
        assert_eq!(values["b"], vec![3]);
    }
}
