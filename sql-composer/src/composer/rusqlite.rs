use std::collections::{BTreeMap, HashMap};

use rusqlite::types::{ToSqlOutput, Value as DriverValue, ValueRef};
use rusqlite::{Connection, Rows, Statement, NO_PARAMS};


pub use rusqlite::types::{Null, ToSql};

use super::{Composer, ComposerConfig, ComposerConnection};

use crate::types::{CompleteStr, ParsedItem, Span, SqlComposition, SqlCompositionAlias};

use serde::ser::Serialize;

use crate::types::value::{Column, Row, ToValue, Value};

use std::convert::From;

impl <'a>ComposerConnection<'a> for Connection {
    type Composer = RusqliteComposer<'a>;
    type Value = &'a (dyn ToSql + 'a);
    type Statement = Statement<'a>;

    fn compose(&'a self, s: &SqlComposition, values: BTreeMap<String, Vec<&'a ToSql>>, root_mock_values: Vec<BTreeMap<String, Self::Value>>, mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>) -> Result<(Self::Statement, Vec<Self::Value>), ()> {
        let c = RusqliteComposer {
            config: RusqliteComposer::config(),
            values,
            root_mock_values,
            mock_values,
        };

        let (sql, bind_vars) = c.compose(s);

        println!("compose bind sql: {}", sql);

        //TODO: support a DriverError type to handle this better
        let stmt = self.prepare(&sql).or_else(|_| Err(()))?;

        Ok((stmt, bind_vars))
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Text(t) => Ok(ToSqlOutput::from(t.as_str())),
            Value::Integer(i) => Ok(ToSqlOutput::from(*i)),
            Value::Real(r) => Ok(ToSqlOutput::from(*r)),
            _ => unimplemented!("unsupported type"),
        }
    }
}

pub struct RusqliteComposer<'a> {
    pub config:           ComposerConfig,
    pub values:           BTreeMap<String, Vec<&'a ToSql>>,
    pub root_mock_values: Vec<BTreeMap<String, &'a ToSql>>,
    pub mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a ToSql>>>,
}

impl<'a> RusqliteComposer<'a> {
    pub fn new() -> Self {
        Self {
            config:           Self::config(),
            values:           BTreeMap::new(),
            root_mock_values: vec![],
            mock_values:      HashMap::new(),
        }
    }
}

impl<'a> Composer for RusqliteComposer<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn bind_var_tag(&self, u: usize, _name: String) -> String {
        format!("?{}", u)
    }

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Self::Value>) {
        let mut sql = String::new();
        let mut new_values = vec![];

        let i = offset;

        match self.values.get(&name) {
            Some(v) => {
                for iv in v.iter() {
                    if new_values.len() > 0 {
                        sql.push_str(", ");
                    }

                    sql.push_str(&self.bind_var_tag(new_values.len() + offset, name.to_string()));

                    new_values.push(*iv);
                }
            }
            None => panic!("no value for binding: {}", i),
        };

        (sql, new_values)
    }

    fn compose_count_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        self.compose_count_default_command(composition, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
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

    /*
    fn get_mock_values(&self, name: String) -> Option<&BTreeMap<String, Self::Value>> {
        self.values.get(&name)
    }
    */
}

#[cfg(test)]
mod tests {
    use super::{Composer, RusqliteComposer, ComposerConnection};

    use crate::types::{Span, SqlComposition, SqlCompositionAlias, SqlDbObject};

    use crate::parser::parse_template;

    use rusqlite::Row;
    use rusqlite::{Connection, NO_PARAMS};
    use time::Timespec;

    use rusqlite::types::ToSql;

    use std::collections::{BTreeMap, HashMap};

    #[derive(Debug, PartialEq)]
    struct Person {
        id:           i32,
        name:         String,
        time_created: Timespec,
        data:         Option<Vec<u8>>,
    }

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute(
            "CREATE TABLE person (
               id              INTEGER PRIMARY KEY,
               name            TEXT NOT NULL,
               time_created    TEXT NOT NULL,
               data            BLOB
             )",
            NO_PARAMS,
        )
        .unwrap();

        conn
    }

    #[test]
    fn test_db_binding() {
        let conn = setup_db();

        let person = Person {
            id:           0,
            name:         "Steven".to_string(),
            time_created: time::get_time(),
            data:         None,
        };

        let (remaining, insert_stmt) = parse_template(Span::new("INSERT INTO person (name, time_created, data) VALUES (:bind(name), :bind(time_created), :bind(data));".into()), None).unwrap();

        println!("remaining: {}", remaining.to_string());
        assert_eq!(*remaining.fragment, "", "nothing remaining");

        /*
        let mut composer = ComposerBuilder::default();

        composer.uri("sqlite://:memory")
            .bind("name".into(), vec![&person.name])
            .bind("time_created".into(), vec![&person.time_created])
            .bind("data".into(), vec![&person.data]);

        composer = composer.build();
        */

        let mut composer = RusqliteComposer::new();

        composer.values.insert("name".into(), vec![&person.name]);
        composer
            .values
            .insert("time_created".into(), vec![&person.time_created]);
        composer.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = composer.compose(&insert_stmt.item);

        let expected_bound_sql =
            "INSERT INTO person (name, time_created, data) VALUES ( ?1, ?2, ?3 );";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        conn.execute(&bound_sql, &rebindings).unwrap();

        let (remaining, select_stmt) = parse_template(Span::new("SELECT id, name, time_created, data FROM person WHERE name = ':bind(name)' AND time_created = ':bind(time_created)' AND name = ':bind(name)' AND time_created = ':bind(time_created)'".into()), None).unwrap();

        assert_eq!(*remaining.fragment, "", "nothing remaining");

        let (bound_sql, bindings) = composer.compose(&select_stmt.item);

        let expected_bound_sql = "SELECT id, name, time_created, data FROM person WHERE name = ?1 AND time_created = ?2 AND name = ?3 AND time_created = ?4";

        assert_eq!(&bound_sql, expected_bound_sql, "select multi-use bindings");

        let mut stmt = conn.prepare(&bound_sql).unwrap();

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let person_iter = stmt
            .query_map(&rebindings, |row| {
                Ok(Person {
                    id:           row.get(0).unwrap(),
                    name:         row.get(1).unwrap(),
                    time_created: row.get(2).unwrap(),
                    data:         row.get(3).unwrap(),
                })
            })
            .unwrap();

        let mut people: Vec<Person> = vec![];

        for p in person_iter {
            people.push(p.unwrap());
        }

        assert_eq!(people.len(), 1, "found 1 person");
        let found = &people[0];

        assert_eq!(found.name, person.name, "person's name");
        assert_eq!(
            found.time_created, person.time_created,
            "person's time_created"
        );
        assert_eq!(found.data, person.data, "person's data");
    }

    fn get_row_values(row: Row) -> Vec<String> {
        (0..4).fold(Vec::new(), |mut acc, i| {
            acc.push(row.get(i).unwrap());
            acc
        })
    }

    #[test]
    fn test_bind_simple_template() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/simple.tql".into()).unwrap();

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn ToSql>> = vec![BTreeMap::new()];

        mock_values[0].insert("col_1".into(), &"a_value");
        mock_values[0].insert("col_2".into(), &"b_value");
        mock_values[0].insert("col_3".into(), &"c_value");
        mock_values[0].insert("col_4".into(), &"d_value");

        let (bound_sql, bindings) = composer.compose(&stmt.item);
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];
        let mut mock_values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        let mut mock_prep_stmt = conn.prepare(&mock_bound_sql).unwrap();

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = mock_prep_stmt
            .query_map(&mock_rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            mock_values.push(row.unwrap());
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/include.tql".into()).unwrap();

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn ToSql>> = vec![];

        mock_values.push(BTreeMap::new());
        mock_values[0].insert("col_1".into(), &"e_value");
        mock_values[0].insert("col_2".into(), &"d_value");
        mock_values[0].insert("col_3".into(), &"b_value");
        mock_values[0].insert("col_4".into(), &"a_value");

        mock_values.push(BTreeMap::new());
        mock_values[1].insert("col_1".into(), &"a_value");
        mock_values[1].insert("col_2".into(), &"b_value");
        mock_values[1].insert("col_3".into(), &"c_value");
        mock_values[1].insert("col_4".into(), &"d_value");

        let (bound_sql, bindings) = composer.compose(&stmt.item);
        let (mut mock_bound_sql, mock_bindings) = composer.mock_compose(&mock_values, 0);

        mock_bound_sql.push(';');

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        let mut mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = mock_prep_stmt
            .query_map(&mock_rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            mock_values.push(row.unwrap());
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/double-include.tql").unwrap();

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn ToSql>> = vec![];

        mock_values.push(BTreeMap::new());
        mock_values[0].insert("col_1".into(), &"d_value");
        mock_values[0].insert("col_2".into(), &"f_value");
        mock_values[0].insert("col_3".into(), &"b_value");
        mock_values[0].insert("col_4".into(), &"a_value");

        mock_values.push(BTreeMap::new());
        mock_values[1].insert("col_1".into(), &"e_value");
        mock_values[1].insert("col_2".into(), &"d_value");
        mock_values[1].insert("col_3".into(), &"b_value");
        mock_values[1].insert("col_4".into(), &"a_value");

        mock_values.push(BTreeMap::new());
        mock_values[2].insert("col_1".into(), &"a_value");
        mock_values[2].insert("col_2".into(), &"b_value");
        mock_values[2].insert("col_3".into(), &"c_value");
        mock_values[2].insert("col_4".into(), &"d_value");

        let (bound_sql, bindings) = composer.compose(&stmt.item);
        let (mut mock_bound_sql, _mock_bindings) = composer.mock_compose(&mock_values, 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        let mut mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = mock_prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            mock_values.push(row.unwrap());
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT col_1, col_2, col_3, col_4 FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_sql = "SELECT col_1, col_2, col_3, col_4 FROM ( SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 UNION ALL SELECT ?9 AS col_1, ?10 AS col_2, ?11 AS col_3, ?12 AS col_4 ) AS main WHERE col_1 in ( ?13, ?14 ) AND col_3 IN ( ?15, ?16 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        println!("setup composer");
        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"d_value", &"a_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"b_value", &"c_value"]);

        println!("binding");
        let (bound_sql, bindings) = composer.compose(&stmt.item);

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        assert_eq!(bound_sql, expected_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_count_command() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(
            Span::new(":count(src/tests/values/double-include.tql);".into()),
            None,
        )
        .unwrap();

        println!("made it through parse");
        let expected_bound_sql = "SELECT COUNT(1) FROM ( SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 UNION ALL SELECT ?9 AS col_1, ?10 AS col_2, ?11 AS col_3, ?12 AS col_4 ) AS count_main";

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"d_value", &"a_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"b_value", &"c_value"]);

        let (bound_sql, bindings) = composer.compose(&stmt.item);

        println!("bound_sql: {}", bound_sql);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<Option<i64>>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| Ok(vec![row.get(0).unwrap()]))
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        let expected_values: Vec<Vec<Option<i64>>> = vec![vec![Some(3)]];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_union_command() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new(":union(src/tests/values/double-include.tql, src/tests/values/include.tql, src/tests/values/double-include.tql);".into()), None).unwrap();

        println!("made it through parse");
        let expected_bound_sql = "SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 UNION ALL SELECT ?9 AS col_1, ?10 AS col_2, ?11 AS col_3, ?12 AS col_4 UNION SELECT ?13 AS col_1, ?14 AS col_2, ?15 AS col_3, ?16 AS col_4 UNION ALL SELECT ?17 AS col_1, ?18 AS col_2, ?19 AS col_3, ?20 AS col_4 UNION SELECT ?21 AS col_1, ?22 AS col_2, ?23 AS col_3, ?24 AS col_4 UNION ALL SELECT ?25 AS col_1, ?26 AS col_2, ?27 AS col_3, ?28 AS col_4 UNION ALL SELECT ?29 AS col_1, ?30 AS col_2, ?31 AS col_3, ?32 AS col_4";

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"d_value", &"a_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"b_value", &"c_value"]);

        let (bound_sql, bindings) = composer.compose(&stmt.item);

        println!("bound_sql: {}", bound_sql);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        let expected_values = vec![
            vec!["a_value", "b_value", "c_value", "d_value"],
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["e_value", "d_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_include_mock_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 ) AS main WHERE col_1 in ( ?9, ?10 ) AND col_3 IN ( ?11, ?12 );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"ee_value", &"d_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"bb_value", &"b_value"]);

        let mut mock_values: HashMap<
            SqlCompositionAlias,
            Vec<BTreeMap<std::string::String, &dyn ToSql>>,
        > = HashMap::new();

        {
            let path_entry = mock_values
                .entry(SqlCompositionAlias::Path(
                    "src/tests/values/include.tql".into(),
                ))
                .or_insert(Vec::new());

            path_entry.push(BTreeMap::new());
            path_entry[0].insert("col_1".into(), &"ee_value");
            path_entry[0].insert("col_2".into(), &"dd_value");
            path_entry[0].insert("col_3".into(), &"bb_value");
            path_entry[0].insert("col_4".into(), &"aa_value");
        }

        composer.mock_values = mock_values;

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false);

        println!("bound sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 UNION ALL SELECT ?9 AS col_1, ?10 AS col_2, ?11 AS col_3, ?12 AS col_4 ) AS main WHERE col_1 in ( ?13, ?14 ) AND col_3 IN ( ?15, ?16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"dd_value", &"aa_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"bb_value", &"cc_value"]);

        let mut mock_values: HashMap<
            SqlCompositionAlias,
            Vec<BTreeMap<std::string::String, &dyn ToSql>>,
        > = HashMap::new();

        {
            let path_entry = mock_values
                .entry(SqlCompositionAlias::Path(
                    "src/tests/values/double-include.tql".into(),
                ))
                .or_insert(Vec::new());

            path_entry.push(BTreeMap::new());
            path_entry[0].insert("col_1".into(), &"dd_value");
            path_entry[0].insert("col_2".into(), &"ff_value");
            path_entry[0].insert("col_3".into(), &"bb_value");
            path_entry[0].insert("col_4".into(), &"aa_value");

            path_entry.push(BTreeMap::new());
            path_entry[1].insert("col_1".into(), &"dd_value");
            path_entry[1].insert("col_2".into(), &"ff_value");
            path_entry[1].insert("col_3".into(), &"bb_value");
            path_entry[1].insert("col_4".into(), &"aa_value");

            path_entry.push(BTreeMap::new());
            path_entry[2].insert("col_1".into(), &"aa_value");
            path_entry[2].insert("col_2".into(), &"bb_value");
            path_entry[2].insert("col_3".into(), &"cc_value");
            path_entry[2].insert("col_4".into(), &"dd_value");
        }

        composer.mock_values = mock_values;

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false);

        println!("bound sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_db_object() {
        let conn = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ?1 AS col_1, ?2 AS col_2, ?3 AS col_3, ?4 AS col_4 UNION ALL SELECT ?5 AS col_1, ?6 AS col_2, ?7 AS col_3, ?8 AS col_4 UNION ALL SELECT ?9 AS col_1, ?10 AS col_2, ?11 AS col_3, ?12 AS col_4 ) AS main WHERE col_1 in ( ?13, ?14 ) AND col_3 IN ( ?15, ?16 );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = RusqliteComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);
        composer
            .values
            .insert("col_1_values".into(), vec![&"dd_value", &"aa_value"]);
        composer
            .values
            .insert("col_3_values".into(), vec![&"bb_value", &"cc_value"]);

        let mut mock_values: HashMap<
            SqlCompositionAlias,
            Vec<BTreeMap<std::string::String, &dyn ToSql>>,
        > = HashMap::new();

        {
            let path_entry = mock_values
                .entry(SqlCompositionAlias::DbObject(
                    SqlDbObject::new("main".into(), None).unwrap(),
                ))
                .or_insert(Vec::new());

            path_entry.push(BTreeMap::new());
            path_entry[0].insert("col_1".into(), &"dd_value");
            path_entry[0].insert("col_2".into(), &"ff_value");
            path_entry[0].insert("col_3".into(), &"bb_value");
            path_entry[0].insert("col_4".into(), &"aa_value");

            path_entry.push(BTreeMap::new());
            path_entry[1].insert("col_1".into(), &"dd_value");
            path_entry[1].insert("col_2".into(), &"ff_value");
            path_entry[1].insert("col_3".into(), &"bb_value");
            path_entry[1].insert("col_4".into(), &"aa_value");

            path_entry.push(BTreeMap::new());
            path_entry[2].insert("col_1".into(), &"aa_value");
            path_entry[2].insert("col_2".into(), &"bb_value");
            path_entry[2].insert("col_3".into(), &"cc_value");
            path_entry[2].insert("col_4".into(), &"dd_value");
        }

        composer.mock_values = mock_values;

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 1, false);

        println!("bound sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
        }

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn it_composes_from_connection() {
        let conn = setup_db();

        let stmt = SqlComposition::from_path_name("src/tests/values/simple.tql".into()).unwrap();

        let mut composer = RusqliteComposer::new();

        let mut values: BTreeMap<String, Vec<&ToSql>> = BTreeMap::new();
        values.insert("a".into(), vec![&"a_value"]);
        values.insert("b".into(), vec![&"b_value"]);
        values.insert("c".into(), vec![&"c_value"]);
        values.insert("d".into(), vec![&"d_value"]);

        let (mut prep_stmt, bindings) = conn.compose(&stmt.item, values, vec![], HashMap::new()).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let rows = prep_stmt
            .query_map(&rebindings, |row| {
                (0..4).fold(Ok(Vec::new()), |mut acc, i| {
                    if let Ok(mut acc) = acc {
                        acc.push(row.get(i).unwrap());
                        Ok(acc)
                    }
                    else {
                        acc
                    }
                })
            })
            .unwrap();

        for row in rows {
            values.push(row.unwrap());
            println!("values: {:?}", values);
        }

        let expected: Vec<Vec<String>> = vec![
            vec!["a_value".into(), "b_value".into(), "c_value".into(), "d_value".into()],
        ];

        assert_eq!(values, expected, "exected values");
    }
}
