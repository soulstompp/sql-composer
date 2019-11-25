use std::collections::{BTreeMap, HashMap};

use futures::{Future, Stream};
use futures_state_stream::StateStream;

use tokio_postgres::Connection;
use tokio_postgres::stmt::Statement;
use tokio_postgres::types::ToSql;

#[cfg(feature = "composer-serde")]
use tokio_postgres::types::{IsNull, Type};

use super::{Composer, ComposerConfig, ComposerConnection};

use crate::types::{ParsedItem, SqlComposition, SqlCompositionAlias};

use crate::error::Result;

#[cfg(feature = "composer-serde")]
use crate::types::SerdeValue;

#[cfg(feature = "composer-serde")]
use serde_value::Value;

#[cfg(feature = "composer-serde")]
use std::error::Error;

/*
impl<'a> ComposerConnection<'a> for Connection {
    type Composer = TokioPostgresComposer<'a>;
    type Value = &'a (dyn ToSql + 'a);
    type Statement = Statement;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<&'a dyn ToSql>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>)> {
        let c = TokioPostgresComposer {
            #[allow(dead_code)]
            config: TokioPostgresComposer::config(),
            values,
            root_mock_values,
            mock_values,
        };

        let (sql, bind_vars) = c.compose(s)?;

        //TODO: support a DriverError type to handle this better
        let stmt = self.prepare(&sql);

        Ok((stmt, bind_vars))
    }
}

#[cfg(feature = "composer-serde")]
impl ToSql for SerdeValue {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut Vec<u8>,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        match &self.0 {
            Value::String(s) => {
                <String as ToSql>::to_sql(s, ty, w)
                    .expect("unable to convert Value::String via to_sql");
            }
            Value::I64(i) => {
                <i64 as ToSql>::to_sql(i, ty, w)
                    .expect("unable to convert Value::String via to_sql");
            }
            Value::F64(f) => {
                <f64 as ToSql>::to_sql(f, ty, w)
                    .expect("unable to convert Value::String via to_sql");
            }
            _ => unimplemented!("unable to convert unexpected Value type"),
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        if <String as ToSql>::accepts(ty)
            || <i64 as ToSql>::accepts(ty)
            || <f64 as ToSql>::accepts(ty)
        {
            true
        }
        else {
            false
        }
    }

    to_sql_checked!();
}
*/

//TODO: the config needs a Binder::{Question, PositionalQuestion, Dollar, PositionalDollar}
//
//TODO: the config needs a Dialect
//Dialect::{MySQL, SQLite, PostgreSQL}
#[derive(Default)]
pub struct TokioPostgresComposer<'a> {
    #[allow(dead_code)]
    config: ComposerConfig,
    pub values: BTreeMap<String, Vec<&'a dyn ToSql>>,
    root_mock_values: Vec<BTreeMap<String, &'a dyn ToSql>>,
    mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a dyn ToSql>>>,
}

impl<'a> TokioPostgresComposer<'a> {
    pub fn new() -> Self {
        Self {
            config:           Self::config(),
            values:           BTreeMap::new(),
            root_mock_values: Vec::new(),
            mock_values:      HashMap::new(),
        }
    }
}

impl<'a> Composer for TokioPostgresComposer<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn place_holder(&self, u: usize, _name: String) -> Result<String> {
        self._build_place_holder("$", true, u)
    }

    fn compose_count_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        self.compose_count_default_command(composition, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        self.compose_union_default_command(composition, offset, child)
    }

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>> {
        self.values.get(&name)
    }

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> () {
        self.values.insert(name, values);
    }

    fn root_mock_values(&self) -> &Vec<BTreeMap<String, Self::Value>> {
        &self.root_mock_values
    }

    fn mock_values(&self) -> &HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>> {
        &self.mock_values
    }
}

#[cfg(test)]
mod tests {
    //use super::{Composer, ComposerConnection, TokioPostgresComposer};
    use futures::prelude::*;
    use futures::{Future, Stream};
    use futures_state_stream::StateStream;

    use tokio_core::reactor::{Core, Interval};


    use super::{Composer, TokioPostgresComposer};

    use crate::{bind_values, mock_db_object_values, mock_path_values, mock_values};

    use crate::types::{SqlComposition, SqlCompositionAlias, SqlDbObject};

    use tokio_postgres::rows::Row;
    use tokio_postgres::types::ToSql;
    use tokio_postgres::{Connection, TlsMode};

    use std::collections::HashMap;

    use dotenv::dotenv;
    use std::env;

    #[derive(Debug, PartialEq)]
    struct Person {
        id:   i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    #[test]
    fn it_runs_example() {
        let mut l = Core::new().unwrap();

        dotenv().ok();

        let mut composer = TokioPostgresComposer::new();

        let person = Person {
            id:   0,
            name: "Steven".to_string(),
            data: None,
        };

        composer.values = bind_values!(&dyn ToSql:
                                       "name" => [&person.name],
                                       "data" => [&person.data]
        );

        let insert_stmt = SqlComposition::parse(
            "INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));",
            None,
        ).unwrap();

        let (insert_sql, insert_bindings) = composer
            .compose(&insert_stmt.item)
            .expect("compose should work");

        let expected_insert_sql = "INSERT INTO person (name, data) VALUES ( $1, $2 );";

        assert_eq!(insert_sql, expected_insert_sql, "insert basic bindings");


        let select_stmt = SqlComposition::parse("SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';", None).unwrap();

        let (select_sql, select_bindings) = composer
            .compose(&select_stmt.item)
            .expect("compose should work");

        let expected_select_sql = "SELECT id, name, data FROM person WHERE name = $1 AND name = $2;";

        assert_eq!(select_sql, expected_select_sql, "select multi-use bindings");


        let done = Connection::connect(
            env::var("PG_DATABASE_URL").expect("Missing variable PG_DATABASE_URL"),
            TlsMode::None,
            &l.handle(),
        ).then(|c| {
            c.unwrap().batch_execute(
                "CREATE TEMPORARY TABLE person (
                        id              SERIAL PRIMARY KEY,
                        name            VARCHAR NOT NULL,
                        data            BYTEA
                      )",
            )
        })
        .and_then(|c| {
            c.prepare(&insert_sql)
        })
        .and_then(|(s, c)| c.execute(&s, &insert_bindings))
        .and_then(|(s, c)| {
            c.prepare(&select_sql)
        })
        .and_then(|(s, c)| {
            c.query(&s, &select_bindings)
            .for_each(|row| {
                let found = Person {
                    id:   row.get(0),
                    name: row.get(1),
                    data: row.get(2),
                };

                assert_eq!(found.name, person.name, "person's name");
                assert_eq!(found.data, person.data, "person's data");
            })
        });

        l.run(done).unwrap();
    }

    fn get_row_values(row: Row) -> Vec<String> {
        (0..4).fold(Vec::new(), |mut acc, i| {
            acc.push(row.get(i));
            acc
        })
    }
}
