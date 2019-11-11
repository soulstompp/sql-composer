use std::collections::{BTreeMap, HashMap};

use postgres::stmt::Statement;
use postgres::types::ToSql;
#[cfg(feature = "composer-serde")]
use postgres::types::{IsNull, Type};
use postgres::Connection;

use super::{Composer, ComposerConfig, ComposerConnection};

use crate::types::{ParsedItem, SqlComposition, SqlCompositionAlias};

use crate::error::{ErrorKind, Result};

#[cfg(feature = "composer-serde")]
use crate::types::SerdeValue;

#[cfg(feature = "composer-serde")]
use serde_value::Value;

#[cfg(feature = "composer-serde")]
use std::error::Error;

impl<'a> ComposerConnection<'a> for Connection {
    type Composer = PostgresComposer<'a>;
    type Value = &'a (dyn ToSql + 'a);
    type Statement = Statement<'a>;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<&'a dyn ToSql>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>)> {
        let c = PostgresComposer {
            #[allow(dead_code)]
            config: PostgresComposer::config(),
            values,
            root_mock_values,
            mock_values,
        };

        let (sql, bind_vars) = c.compose(s)?;

        //TODO: support a DriverError type to handle this better
        let stmt = self.prepare(&sql).or_else(|_| Err("can't prepare sql"))?;

        Ok((stmt, bind_vars))
    }
}

#[cfg(feature = "composer-serde")]
impl ToSql for SerdeValue {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        match &self.0 {
            Value::String(s) => {
                <String as ToSql>::to_sql(s, ty, w).expect("unable to convert Value::String via to_sql");
            }
            Value::I64(i) => {
                <i64 as ToSql>::to_sql(i, ty, w).expect("unable to convert Value::String via to_sql");            }
            Value::F64(f) => {
                <f64 as ToSql>::to_sql(f, ty, w).expect("unable to convert Value::String via to_sql");
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

#[derive(Default)]
pub struct PostgresComposer<'a> {
    #[allow(dead_code)]
    config:           ComposerConfig,
    values:           BTreeMap<String, Vec<&'a dyn ToSql>>,
    root_mock_values: Vec<BTreeMap<String, &'a dyn ToSql>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a dyn ToSql>>>,
}

impl<'a> PostgresComposer<'a> {
    pub fn new() -> Self {
        Self {
            config:           Self::config(),
            values:           BTreeMap::new(),
            root_mock_values: Vec::new(),
            mock_values:      HashMap::new(),
        }
    }
}

impl<'a> Composer for PostgresComposer<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn binding_tag(&self, u: usize, _name: String) -> String {
        format!("${}", u)
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
    use super::{Composer, ComposerConnection, PostgresComposer};

    use crate::{bind_values, mock_db_object_values, mock_path_values, mock_values};

    use crate::parser::parse_template;

    use crate::types::{Span, SqlComposition, SqlCompositionAlias, SqlDbObject};

    use postgres::rows::Row;
    use postgres::types::ToSql;
    use postgres::{Connection, TlsMode};

    use std::collections::HashMap;

    use dotenv::dotenv;
    use std::env;

    #[derive(Debug, PartialEq)]
    struct Person {
        id:   i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    fn setup_db() -> Connection {
        dotenv().ok();

        Connection::connect(
            env::var("PG_DATABASE_URL").expect("Missing variable PG_DATABASE_URL"),
            TlsMode::None
        ).unwrap()
    }

    #[test]
    fn test_binding() {
        let conn = setup_db();

        conn.execute("DROP TABLE IF EXISTS person;", &[]).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS person (
                        id              SERIAL PRIMARY KEY,
                        name            VARCHAR NOT NULL,
                        data            BYTEA
                      )",
            &[],
        )
        .unwrap();

        let person = Person {
            id:   0,
            name: "Steven".to_string(),
            data: None,
        };

        let (remaining, insert_stmt) = parse_template(
            Span::new("INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));".into()),
            None,
        )
        .unwrap();

        assert_eq!(remaining.fragment, "", "insert stmt nothing remaining");

        let mut composer = PostgresComposer::new();

        composer.values = bind_values!(&dyn ToSql:
                                       "name" => [&person.name],
                                       "data" => [&person.data]
        );

        let (bound_sql, bindings) = composer
            .compose(&insert_stmt.item)
            .expect("compose should work");

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ( $1, $2 );";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        conn.execute(&bound_sql, &bindings).unwrap();

        let (remaining, select_stmt) = parse_template(Span::new("SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';".into()), None).unwrap();

        assert_eq!(remaining.fragment, "", "select stmt nothing remaining");

        let (bound_sql, bindings) = composer
            .compose(&select_stmt.item)
            .expect("compose should work");

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = $1 AND name = $2;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let stmt = conn.prepare(&bound_sql).unwrap();

        let mut people: Vec<Person> = vec![];

        for row in &stmt.query(&bindings).unwrap() {
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
    }

    fn get_row_values(row: Row) -> Vec<String> {
        (0..4).fold(Vec::new(), |mut acc, i| {
            acc.push(row.get(i));
            acc
        })
    }

    #[test]
    fn test_bind_simple_template() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/simple.tql".into()).unwrap();

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0).expect("mock_compose should work");

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];
        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mock_prep_stmt = conn.prepare(&mock_bound_sql).unwrap();

        for row in &mock_prep_stmt.query(&mock_bindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/include.tql".into()).unwrap();

        let mut composer = PostgresComposer::new();

        composer.values = bind_values!(&dyn ToSql:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"],
        "e" => [&"e_value"]
        );

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0).expect("mock_compose should work");

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &mock_prep_stmt.query(&mock_bindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let conn = setup_db();

        let stmt =
            SqlComposition::from_path_name("src/tests/values/double-include.tql".into()).unwrap();

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0).expect("mock_compose should work");

        mock_bound_sql.push(';');

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in &mock_prep_stmt.query(&mock_bindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT col_1, col_2, col_3, col_4 FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));".into()), None).unwrap();

        let expected_sql = "SELECT col_1, col_2, col_3, col_4 FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");

        assert_eq!(bound_sql, expected_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }
        assert_eq!(values, expected_values, "expected values");
    }

    #[test]
    fn test_count_command() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(
            Span::new(":count(src/tests/values/double-include.tql);".into()),
            None,
        )
        .unwrap();

        let expected_bound_sql = "SELECT COUNT(1) FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS count_main";

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<Option<i64>>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(vec![row.get(0)]);
        }

        let expected_values: Vec<Vec<Option<i64>>> = vec![vec![Some(3)]];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_union_command() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new(":union(src/tests/values/double-include.tql, src/tests/values/include.tql, src/tests/values/double-include.tql);".into()), None).unwrap();

        let expected_bound_sql = "SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 UNION SELECT $13 AS col_1, $14 AS col_2, $15 AS col_3, $16 AS col_4 UNION ALL SELECT $17 AS col_1, $18 AS col_2, $19 AS col_3, $20 AS col_4 UNION SELECT $21 AS col_1, $22 AS col_2, $23 AS col_3, $24 AS col_4 UNION ALL SELECT $25 AS col_1, $26 AS col_2, $27 AS col_3, $28 AS col_4 UNION ALL SELECT $29 AS col_1, $30 AS col_2, $31 AS col_3, $32 AS col_4";

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        let expected_values = vec![
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_include_mock_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 ) AS main WHERE col_1 in ( $9, $10 ) AND col_3 IN ( $11, $12 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut composer = PostgresComposer::new();

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

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 1, false)
            .expect("compose_statement should work");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = PostgresComposer::new();

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

        composer.mock_values = mock_path_values!(&dyn ToSql: "src/tests/values/double-include.tql" => [
                    {
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
                    }

        ]);

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 1, false)
            .expect("compose_statement should work");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_db_object() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4 ) AS main WHERE col_1 in ( $13, $14 ) AND col_3 IN ( $15, $16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = PostgresComposer::new();

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

        let mock_values = mock_db_object_values!(&dyn ToSql: "main" => [{
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

        composer.mock_values = mock_values;

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 1, false)
            .expect("compose_statement should work");

        let prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn it_composes_from_connection() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/simple.tql".into()).unwrap();

        let bind_values = bind_values!(&dyn ToSql:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"]
        );

        let (prep_stmt, bindings) = conn
            .compose(&stmt.item, bind_values, vec![], HashMap::new())
            .unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in &prep_stmt.query(&bindings).unwrap() {
            values.push(get_row_values(row));
        }

        let expected: Vec<Vec<String>> = vec![vec![
            "a_value".into(),
            "b_value".into(),
            "c_value".into(),
            "d_value".into(),
        ]];

        assert_eq!(values, expected, "exected values");
    }
}
