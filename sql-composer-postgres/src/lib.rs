// this is used during tests, must be at root
#[allow(unused_imports)]
#[macro_use]
extern crate sql_composer;

#[cfg(feature = "composer-serde")]
#[macro_use]
extern crate postgres;

use std::collections::{BTreeMap, HashMap};

use postgres::stmt::Statement;
use postgres::types::ToSql;

#[cfg(feature = "composer-serde")]
use postgres::types::{IsNull, Type};

pub use postgres::{Connection, TlsMode};

use sql_composer::composer::{ComposerConfig, ComposerTrait};

use sql_composer::types::{ParsedSqlComposition, SqlComposition, SqlCompositionAlias};

use sql_composer::error::Result;

#[cfg(feature = "composer-serde")]
use serde_value::Value;

#[cfg(feature = "composer-serde")]
#[derive(Clone, Debug)]
pub struct SerdeValue(pub Value);

#[cfg(feature = "composer-serde")]
impl PartialEq for SerdeValue {
    fn eq(&self, rhs: &Self) -> bool {
        self.0 == rhs.0
    }
}

#[cfg(feature = "composer-serde")]
impl ToSql for SerdeValue {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        match &self.0 {
            Value::String(s) => {
                <String as ToSql>::to_sql(&s, ty, w).expect("unable to convert Value::String via to_sql");
            }
            Value::I64(i) => {
                <i64 as ToSql>::to_sql(&i, ty, w).expect("unable to convert Value::String via to_sql");            }
            Value::F64(f) => {
                <f64 as ToSql>::to_sql(&f, ty, w).expect("unable to convert Value::String via to_sql");
            }
            _ => unimplemented!("unable to convert unexpected Value type"),
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        <String as ToSql>::accepts(ty) || <i64 as ToSql>::accepts(ty) || <f64 as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "composer-serde")]
use std::error::Error;

pub trait ComposerConnection<'a> {
    type Composer;
    //TODO: this should be Composer::Value but can't be specified as Self::Value::Connection
    type Value;
    type Statement;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<Self::Value>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>)>;
}

impl<'a> ComposerConnection<'a> for Connection {
    type Composer = Composer<'a>;
    type Value = &'a (dyn ToSql + 'a);
    type Statement = Statement<'a>;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<&'a dyn ToSql>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>)> {
        let c = Composer {
            config: Composer::config(),
            values,
            root_mock_values,
            mock_values,
        };

        let (sql, bind_vars) = c.compose(s)?;

        let stmt = self.prepare(&sql)?;

        Ok((stmt, bind_vars))
    }
}

#[derive(Default)]
pub struct Composer<'a> {
    // config field is unused in this crate but is included
    // for compat with composers which do use config (e.g. rusqlite)
    #[allow(dead_code)]
    config:           ComposerConfig,
    values:           BTreeMap<String, Vec<&'a dyn ToSql>>,
    root_mock_values: Vec<BTreeMap<String, &'a dyn ToSql>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a dyn ToSql>>>,
}

impl<'a> Composer<'a> {
    pub fn new() -> Self {
        Self {
            config:           Self::config(),
            values:           BTreeMap::new(),
            ..Default::default()
        }
    }
}

impl<'a> ComposerTrait for Composer<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn binding_tag(&self, u: usize, _name: String) -> Result<String> {
        Ok(format!("${}", u))
    }

    fn compose_count_command(
        &self,
        composition: &ParsedSqlComposition,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        self.compose_count_default_command(composition, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedSqlComposition,
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
    use super::{Composer, ComposerConnection, ComposerTrait};

    use sql_composer::error::Result;
    use sql_composer::types::{ParsedSqlComposition, SqlCompositionAlias, SqlDbObject};

    use postgres::rows::Row;
    use postgres::types::ToSql;
    use postgres::{Connection, TlsMode};

    use std::collections::HashMap;
    use std::path::PathBuf;

    use dotenv::dotenv;
    use std::env;

    use std::convert::TryInto;

    // Return empty result to allow use of ? in tests
    type EmptyResult = Result<()>;

    #[derive(Debug, Default, PartialEq)]
    struct Person {
        id:   i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    fn setup_db() -> Connection {
        dotenv().ok();

        Connection::connect(
            env::var("PG_DATABASE_URL").expect("Missing variable PG_DATABASE_URL"),
            TlsMode::None,
        )
        .unwrap()
    }

    #[test]
    fn test_db_binding() -> EmptyResult {
        let conn = setup_db();

        // TODO: why doesn't it work to do this in setup_db()?
        conn.execute("DROP TABLE IF EXISTS person;", &[])?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS person (
                        id              SERIAL PRIMARY KEY,
                        name            VARCHAR NOT NULL,
                        data            BYTEA
                      )",
            &[],
        )?;

        let person = Person {
            id:   0,
            name: "Steven".to_string(),
            ..Default::default()
        };

        let insert_stmt = ParsedSqlComposition::parse(
            "INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));"
        )?;

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "name" => [&person.name],
                                       "data" => [&person.data]
        );

        let (bound_sql, bindings) = composer.compose(&insert_stmt.item)?;

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ( $1, $2 );";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        conn.execute(&bound_sql, &bindings)?;

        let select_stmt = ParsedSqlComposition::parse("SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';")?;

        let (bound_sql, bindings) = composer.compose(&select_stmt.item)?;

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = $1 AND name = $2;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let stmt = conn.prepare(&bound_sql)?;

        let mut people: Vec<Person> = vec![];

        for row in &stmt.query(&bindings)? {
            people.push(Person {
                id:   row.get(0),
                name: row.get(1),
                data: row.get(2),
            });
        }

        assert_eq!(people.len(), 1, "found 1 person");
        let found = &people[0];

        assert_eq!(found.name, person.name, "person's name");
        assert_eq!(found.data, person.data, "person's data");
        Ok(())
    }

    fn get_row_values(row: Row) -> Vec<String> {
        (0..4).fold(Vec::new(), |mut acc, i| {
            acc.push(row.get(i));
            acc
        })
    }

    #[test]
    fn test_bind_simple_template() -> EmptyResult {
        let conn = setup_db();

        let stmt: ParsedSqlComposition = "src/tests/values/simple.tql".try_into()?;

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"]
        );

        let mock_values = mock_values!(&dyn ToSql: {
            "col_1" => &"a_value",
            "col_2" => &"b_value",
            "col_3" => &"c_value",
            "col_4" => &"d_value"
        });

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0)?;

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];
        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        let mock_prep_stmt = conn.prepare(&mock_bound_sql)?;

        for row in &mock_prep_stmt.query(&mock_bindings)? {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_bind_include_template() -> EmptyResult {
        let conn = setup_db();

        let stmt: ParsedSqlComposition = PathBuf::from("src/tests/values/include.tql").try_into()?;

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"]);

        let mock_values = mock_values!(&dyn ToSql: {
            "col_1" => &"e_value",
            "col_2" => &"d_value",
            "col_3" => &"b_value",
            "col_4" => &"a_value"
        },
        {
            "col_1" => &"a_value",
            "col_2" => &"b_value",
            "col_3" => &"c_value",
            "col_4" => &"d_value"
        });

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0)?;

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        let mock_prep_stmt = conn.prepare(&bound_sql)?;

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &mock_prep_stmt.query(&mock_bindings)? {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_bind_double_include_template() -> EmptyResult {
        let conn = setup_db();

        let stmt: ParsedSqlComposition = PathBuf::from("src/tests/values/double-include.tql").try_into()?;

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"]
        );

        let mock_values = mock_values!(&dyn ToSql: {
            "col_1" => &"d_value",
            "col_2" => &"f_value",
            "col_3" => &"b_value",
            "col_4" => &"a_value"
        },
        {
            "col_1" => &"e_value",
            "col_2" => &"d_value",
            "col_3" => &"b_value",
            "col_4" => &"a_value"
        },
        {
            "col_1" => &"a_value",
            "col_2" => &"b_value",
            "col_3" => &"c_value",
            "col_4" => &"d_value"
        });

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0)?;

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");

        let mock_prep_stmt = conn.prepare(&bound_sql)?;

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &mock_prep_stmt.query(&mock_bindings)? {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(values, mock_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_multi_value_bind() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse("SELECT col_1, col_2, col_3, col_4 FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));")?;

        let expected_bound_sql = "SELECT col_1, col_2, col_3, col_4 FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"d_value", &"a_value"],
                                       "col_3_values" => [&"b_value", &"c_value"]
        );

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        assert_eq!(values, expected_values, "expected values");
        Ok(())
    }

    #[test]
    fn test_count_command() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse(":count(src/tests/values/double-include.tql);")?;

        let expected_bound_sql = "SELECT COUNT(1) FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS count_main";

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"d_value", &"a_value"],
                                       "col_3_values" => [&"b_value", &"c_value"]
        );

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<i64>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(vec![row.get(0)]);
        }

        let expected_values: Vec<Vec<i64>> = vec![vec![3]];

        assert_eq!(values, expected_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_union_command() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse(":union(src/tests/values/double-include.tql, src/tests/values/include.tql, src/tests/values/double-include.tql);")?;

        let expected_bound_sql = "SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 UNION SELECT $13 AS col_1, $14 AS col_2, $15 AS col_3, $16 AS col_4 UNION ALL SELECT $17 AS col_1, $18 AS col_2, $19 AS col_3, $20 AS col_4 UNION SELECT $21 AS col_1, $22 AS col_2, $23 AS col_3, $24 AS col_4 UNION ALL SELECT $25 AS col_1, $26 AS col_2, $27 AS col_3, $28 AS col_4 UNION ALL SELECT $29 AS col_1, $30 AS col_2, $31 AS col_3, $32 AS col_4";

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"d_value", &"a_value"],
                                       "col_3_values" => [&"b_value", &"c_value"]
        );

        let (bound_sql, bindings) = composer.compose(&stmt.item)?;

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        // TODO: why are values 0 and 1 swapped vs mysql?
        let expected_values = vec![
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        assert_eq!(values, expected_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_include_mock_multi_value_bind() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));")?;

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 ) AS main WHERE col_1 in ( $9, $10 ) AND col_3 IN ( $11, $12 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"ee_value", &"d_value"],
                                       "col_3_values" => [&"bb_value", &"b_value"]
        );

        composer.mock_values = mock_path_values!(&dyn ToSql: "src/tests/values/include.tql" => [{
            "col_1" => &"ee_value",
            "col_2" => &"dd_value",
            "col_3" => &"bb_value",
            "col_4" => &"aa_value"
        }]);

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false)?;
        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        assert_eq!(values, expected_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));")?;

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"dd_value", &"aa_value"],
                                       "col_3_values" => [&"bb_value", &"cc_value"]
        );

        composer.mock_values = mock_path_values!(&dyn ToSql: "src/tests/values/double-include.tql" => [{
            "col_1" => &"dd_value",
            "col_2" => &"ff_value",
            "col_3" => &"bb_value",
            "col_4" => &"aa_value"
        },
        {
            "col_1" => &"dd_value",
            "col_2" => &"ff_value",
            "col_3" => &"bb_value",
            "col_4" => &"aa_value"
        },
        {
            "col_1" => &"aa_value",
            "col_2" => &"bb_value",
            "col_3" => &"cc_value",
            "col_4" => &"dd_value"
        }]);

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false)?;
        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        assert_eq!(values, expected_values, "exected values");
        Ok(())
    }

    #[test]
    fn test_mock_db_object() -> EmptyResult {
        let conn = setup_db();

        let stmt = ParsedSqlComposition::parse("SELECT * FROM main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));")?;

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"dd_value", &"aa_value"],
                                       "col_3_values" => [&"bb_value", &"cc_value"]
        );

        composer.mock_values = mock_db_object_values!(&dyn ToSql: "main" => [{
            "col_1" => &"dd_value",
            "col_2" => &"ff_value",
            "col_3" => &"bb_value",
            "col_4" => &"aa_value"
        },
        {
            "col_1" => &"dd_value",
            "col_2" => &"ff_value",
            "col_3" => &"bb_value",
            "col_4" => &"aa_value"
        },
        {
            "col_1" => &"aa_value",
            "col_2" => &"bb_value",
            "col_3" => &"cc_value",
            "col_4" => &"dd_value"
        }]);

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false)?;
        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql)?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        assert_eq!(values, expected_values, "exected values");
        Ok(())
    }

    #[test]
    fn it_composes_from_connection() -> EmptyResult {
        let conn = setup_db();

        let stmt: ParsedSqlComposition = "src/tests/values/simple.tql".try_into()?;

        let bind_values = bind_values!(&dyn ToSql:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"]
        );

        let (prep_stmt, bindings) =
            conn.compose(&stmt.item, bind_values, vec![], HashMap::new())?;

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings)? {
            values.push(get_row_values(row));
        }

        let expected: Vec<Vec<String>> = vec![vec![
            "a_value".into(),
            "b_value".into(),
            "c_value".into(),
            "d_value".into(),
        ]];

        assert_eq!(values, expected, "exected values");
        Ok(())
    }
}
