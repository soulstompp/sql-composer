// this is used during tests, must be at root
#[allow(unused_imports)]
#[macro_use]
extern crate sql_composer;

use std::collections::{BTreeMap, HashMap};

use mysql::{prelude::ToValue, Stmt};

#[cfg(feature = "composer-serde")]
pub use mysql::Value;

use sql_composer::composer::{ComposerConfig, ComposerTrait};

use sql_composer::types::{ParsedItem, SqlComposition, SqlCompositionAlias};

use sql_composer::error::Result;

#[cfg(feature = "composer-serde")]
pub use serde_value::Value as SerdeValueEnum;

use mysql::Pool;

#[cfg(feature = "composer-serde")]
#[derive(Clone, Debug)]
pub struct SerdeValue(pub SerdeValueEnum);

#[cfg(feature = "composer-serde")]
impl PartialEq for SerdeValue {
    fn eq(&self, rhs: &Self) -> bool {
        self.0 == rhs.0
    }
}

#[cfg(feature = "composer-serde")]
impl Into<Value> for SerdeValue {
    fn into(self) -> Value {
        match self.0 {
            SerdeValueEnum::String(s) => Value::Bytes(s.into_bytes()),
            SerdeValueEnum::I64(i) => Value::Int(i),
            SerdeValueEnum::F64(f) => Value::Float(f),
            _ => unimplemented!("unable to convert unexpected ComposerValue type"),
        }
    }
}

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

impl<'a> ComposerConnection<'a> for Pool {
    type Composer = Composer<'a>;
    type Value = &'a (dyn ToValue + 'a);
    type Statement = Stmt<'a>;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<Self::Value>>,
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

        //TODO: support a DriverError type to handle this better
        let stmt = self.prepare(&sql).or_else(|_| Err("unable to prepare"))?;

        Ok((stmt, bind_vars))
    }
}

pub struct Composer<'a> {
    #[allow(dead_code)]
    config:           ComposerConfig,
    values:           BTreeMap<String, Vec<&'a dyn ToValue>>,
    root_mock_values: Vec<BTreeMap<String, &'a dyn ToValue>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a dyn ToValue>>>,
}

impl<'a> Composer<'a> {
    pub fn new() -> Self {
        Self {
            config:           Self::config(),
            values:           BTreeMap::new(),
            root_mock_values: vec![],
            mock_values:      HashMap::new(),
        }
    }
}

impl<'a> ComposerTrait for Composer<'a> {
    type Value = &'a (dyn ToValue + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn binding_tag(&self, _u: usize, _name: String) -> Result<String> {
        Ok(format!("?"))
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
    //use crate::{bind_values, mock_db_object_values, mock_path_values, mock_values};

    use super::{Composer, ComposerConnection, ComposerTrait};

    use mysql::prelude::ToValue;
    use mysql::{from_row, Pool, Row};
    use sql_composer::types::{SqlComposition, SqlCompositionAlias, SqlDbObject};

    use std::collections::HashMap;

    use dotenv::dotenv;
    use std::env;

    #[derive(Debug, PartialEq)]
    struct Person {
        id:   i32,
        name: String,
        data: Option<String>,
    }

    fn setup_db() -> Pool {

        dotenv().ok();
        let pool = Pool::new(
            env::var("MYSQL_DATABASE_URL").expect("Missing variable MYSQL_DATABASE_URL")
        ).unwrap();

        pool.prep_exec("DROP TABLE IF EXISTS person;", ()).unwrap();

        pool.prep_exec(
            "CREATE TABLE IF NOT EXISTS person (
                          id              INT NOT NULL AUTO_INCREMENT,
                          name            VARCHAR(50) NOT NULL,
                          data            TEXT,
                          PRIMARY KEY(id)
                        )",
            (),
        )
        .unwrap();

        pool
    }

    #[test]
    fn test_binding() {
        let pool = setup_db();

        let person = Person {
            id:   0,
            name: "Steven".to_string(),
            data: None,
        };

        let mut composer = Composer::new();

        let insert_stmt = SqlComposition::parse(
            "INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));",
            None,
        )
        .unwrap();

        composer.values = bind_values!(&dyn ToValue:
        "name" => [&person.name],
        "data" => [&person.data]
        );

        let (bound_sql, bindings) = composer
            .compose(&insert_stmt.item)
            .expect("compose should work");

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ( ?, ? );";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let _res = &pool.prep_exec(&bound_sql, &bindings.as_slice());

        let select_stmt = SqlComposition::parse("SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';", None).unwrap();

        let (bound_sql, bindings) = composer
            .compose(&select_stmt.item)
            .expect("compose should work");

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = ? AND name = ?;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let people: Vec<Person> = pool
            .prep_exec(&bound_sql, &bindings.as_slice())
            .map(|result| {
                result
                    .map(|x| x.unwrap())
                    .map(|row| Person {
                        id:   row.get(0).unwrap(),
                        name: row.get(1).unwrap(),
                        data: row.get(2).unwrap(),
                    })
                    .collect()
            })
            .unwrap();

        assert_eq!(people.len(), 1, "found 1 person");
        let found = &people[0];

        assert_eq!(found.name, person.name, "person's name");
        assert_eq!(found.data, person.data, "person's data");
    }

    fn get_row_values(row: Row) -> Vec<String> {
        let mut c: Vec<String> = vec![];

        let (col_1, col_2, col_3, col_4) = from_row::<(String, String, String, String)>(row);
        c.push(col_1);
        c.push(col_2);
        c.push(col_3);
        c.push(col_4);

        c
    }

    #[test]
    fn test_mock_bind_simple_template() {
        let pool = setup_db();

        let stmt = SqlComposition::from_path("src/tests/values/simple.tql").unwrap();

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"]
        );

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");
        composer.root_mock_values = mock_values!(&dyn ToValue: {"col_1" => &"a_value", "col_2" => &"b_value", "col_3" => &"c_value", "col_4" => &"d_value"});

        let (mock_bound_sql, mock_bindings) =
            composer.compose(&stmt.item).expect("compose should work");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];
        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let _mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        for row in prep_stmt.execute(mock_bindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let pool = setup_db();

        let stmt = SqlComposition::from_path("src/tests/values/include.tql").unwrap();

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"]);

        let mock_values = mock_values!(&dyn ToValue: {
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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(&bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in mock_prep_stmt.execute(&mock_bindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let pool = setup_db();

        let stmt =
            SqlComposition::from_path("src/tests/values/double-include.tql").unwrap();

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"],
        "e" => [&"e_value"],
        "f" => [&"f_value"]
        );

        let mock_values = mock_values!(&dyn ToValue: {
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
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 1).expect("mock_compose should work");

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(&bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        for row in mock_prep_stmt.execute(&mock_bindings.as_slice()).unwrap() {
            let mut c: Vec<String> = vec![];

            let (col_1, col_2, col_3, col_4) =
                from_row::<(String, String, String, String)>(row.unwrap());
            c.push(col_1);
            c.push(col_2);
            c.push(col_3);
            c.push(col_4);

            mock_values.push(c);
        }

        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_multi_value_bind() {
        let pool = setup_db();

        let stmt = SqlComposition::parse("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"d_value",
                                       &"a_value"],
                                       "col_3_values" => [&"b_value",
                                       &"c_value"]
        );

        let (bound_sql, bindings) = composer.compose(&stmt.item).expect("compose should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_count_command() {
        let pool = setup_db();

        let stmt = SqlComposition::parse(":count(src/tests/values/double-include.tql);", None).unwrap();

        let expected_bound_sql = "SELECT COUNT(1) FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS count_main";

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<usize>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            let count = from_row::<(usize)>(row.unwrap());
            values.push(vec![count]);
        }

        let expected_values: Vec<Vec<usize>> = vec![vec![3]];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_union_command() {
        let pool = setup_db();

        let stmt = SqlComposition::parse(":union(src/tests/values/double-include.tql, src/tests/values/include.tql, src/tests/values/double-include.tql);", None).unwrap();

        let expected_bound_sql = "SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4";

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_include_mock_multi_value_bind() {
        let pool = setup_db();

        let stmt = SqlComposition::parse("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
                                       "a" => [&"a_value"],
                                       "b" => [&"b_value"],
                                       "c" => [&"c_value"],
                                       "d" => [&"d_value"],
                                       "e" => [&"e_value"],
                                       "f" => [&"f_value"],
                                       "col_1_values" => [&"ee_value", &"d_value"],
                                       "col_3_values" => [&"bb_value", &"b_value"]
        );

        composer.mock_values = mock_path_values!(&dyn ToValue: "src/tests/values/include.tql" => [
        {
        "col_1" => &"ee_value",
        "col_2" => &"dd_value",
        "col_3" => &"bb_value",
        "col_4" => &"aa_value"
        }]);

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 0, false)
            .expect("compose_statement should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(&bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() {
        let pool = setup_db();

        let stmt = SqlComposition::parse("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"],
        "e" => [&"e_value"],
        "f" => [&"f_value"],
        "col_1_values" => [&"dd_value", &"aa_value"],
        "col_3_values" => [&"bb_value", &"cc_value"]
        );

        composer.mock_values = mock_path_values!(&dyn ToValue: "src/tests/values/double-include.tql" => [
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
        }]);

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 0, false)
            .expect("compose_statement should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(&bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_db_object() {
        let pool = setup_db();

        let stmt = SqlComposition::parse("SELECT * FROM main WHERE col_1 in (:bind(col_1_values EXPECTING MIN 1)) AND col_3 IN (:bind(col_3_values EXPECTING MIN 1));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"],
        "e" => [&"e_value"],
        "f" => [&"f_value"],
        "col_1_values" => [&"dd_value", &"aa_value"],
        "col_3_values" => [&"bb_value", &"cc_value"]
        );

        composer.mock_values = mock_db_object_values!(&dyn ToValue: "main" => [{
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

        let (bound_sql, bindings) = composer
            .compose_statement(&stmt, 0, false)
            .expect("compose_statement should work");

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(&bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn it_composes_from_connection() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path("src/tests/values/simple.tql").unwrap();

        let bind_values = bind_values!(&dyn ToValue:
        "a" => [&"a_value"],
        "b" => [&"b_value"],
        "c" => [&"c_value"],
        "d" => [&"d_value"]
        );

        let (mut prep_stmt, bindings) = conn
            .compose(&stmt.item, bind_values, vec![], HashMap::new())
            .unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
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
