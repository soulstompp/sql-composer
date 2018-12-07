use std::collections::HashMap;

use mysql::prelude::ToValue;

use super::{Binder, BinderConfig};

struct MysqlBinder<'a> {
    config: BinderConfig,
    values: HashMap<String, Vec<&'a ToValue>>,
}

impl <'a>MysqlBinder<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new(),
        }
    }
}

impl <'a>Binder for MysqlBinder<'a> {
    type Value = &'a (dyn ToValue + 'a);

    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
    }

    fn bind_var_tag(&self, _u: usize, _name: String) -> String {
        format!("?")
    }

    fn bind_values<'b>(&self, name: String, offset: usize) -> (String, Vec<Self::Value>) {
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
            },
            None => panic!("no value for binding: {}, {}", i, name)
        };

        (sql, new_values)
    }

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>> {
        self.values.get(&name)
    }

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> () {
        self.values.insert(name, values);
    }
}

#[cfg(test)]
mod tests {
    use super::{Binder, MysqlBinder};
    use ::parser::{SqlStatement, parse_template};
    use mysql::prelude::*;
    use mysql::{Pool, from_row, Row};

    use std::collections::BTreeMap;

    #[derive(Debug, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        data: Option<String>,
    }

    fn setup_db() -> Pool {
        let pool = Pool::new("mysql://vagrant:password@localhost:3306/vagrant").unwrap();

        pool.prep_exec("DROP TABLE IF EXISTS person;", ()).unwrap();

        pool.prep_exec("CREATE TABLE IF NOT EXISTS person (
                          id              INT NOT NULL AUTO_INCREMENT,
                          name            VARCHAR(50) NOT NULL,
                          data            TEXT,
                          PRIMARY KEY(id)
                        )", ()).unwrap();

        pool
    }

    #[test]
    fn test_binding() {
        let pool = setup_db();

        let person = Person {
            id: 0,
            name: "Steven".to_string(),
            data: None,
        };

        let mut bv = MysqlBinder::new();

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, data) VALUES (:name:, :data:);").unwrap();

        assert_eq!(remaining, b"", "insert stmt nothing remaining");

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES (?, ?);";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let _res = &pool.prep_exec(
            &bound_sql,
            bindings.as_slice(),
        );

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, data FROM person WHERE name = ':name:' AND name = ':name:';").unwrap();

        assert_eq!(remaining, b"", "select stmt nothing remaining");

        let (bound_sql, bindings) = bv.bind(select_stmt);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = ? AND name = ?;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let people:Vec<Person> = pool.prep_exec(&bound_sql, bindings.as_slice()).map(|result| {
            result.map(|x| x.unwrap()).map(|row| {
                Person {
                    id: row.get(0).unwrap(),
                    name: row.get(1).unwrap(),
                    data: row.get(2).unwrap(),
                }
            }).collect()
        }).unwrap();


        assert_eq!(people.len(), 1, "found 1 person");
        let found = &people[0];

        assert_eq!(found.name, person.name, "person's name");
        assert_eq!(found.data, person.data, "person's data");
    }

    fn parse(input: &str) -> SqlStatement {
        let (remaining, stmt) = parse_template(input.as_bytes()).unwrap();

        stmt
    }

    fn get_row_values(row: Row) -> Vec<String> {
        let mut c:Vec<String>  = vec![];

        let (col_1, col_2, col_3, col_4) = from_row::<(String, String, String, String)>(row);
        c.push(col_1);
        c.push(col_2);
        c.push(col_3);
        c.push(col_4);

        c
    }

    #[test]
    fn test_bind_simple_template() {
        let pool = setup_db();

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/simple.tql").unwrap();

        let mut binder = MysqlBinder::new();

        binder.values.insert("a".into(), vec![&"a_value"]);
        binder.values.insert("b".into(), vec![&"b_value"]);
        binder.values.insert("c".into(), vec![&"c_value"]);
        binder.values.insert("d".into(), vec![&"d_value"]);

        let mut mock_values:Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> = vec![BTreeMap::new()];

        mock_values[0].insert("col_1".into(), &"a_value");
        mock_values[0].insert("col_2".into(), &"b_value");
        mock_values[0].insert("col_3".into(), &"c_value");
        mock_values[0].insert("col_4".into(), &"d_value");

        let (bound_sql, bindings) = binder.bind(stmt);
        let (mut mock_bound_sql, mock_bindings) = binder.mock_bind(mock_values, 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];
        let mut mock_values:Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        for row in prep_stmt.execute(mock_bindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let pool = setup_db();

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/include.tql").unwrap();

        let mut binder = MysqlBinder::new();

        binder.values.insert("a".into(), vec![&"a_value"]);
        binder.values.insert("b".into(), vec![&"b_value"]);
        binder.values.insert("c".into(), vec![&"c_value"]);
        binder.values.insert("d".into(), vec![&"d_value"]);
        binder.values.insert("e".into(), vec![&"e_value"]);

        let mut mock_values:Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> = vec![];

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

        let (bound_sql, bindings) = binder.bind(stmt);
        let (mut mock_bound_sql, mock_bindings) = binder.mock_bind(mock_values, 0);

        mock_bound_sql.push(';');

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        for row in mock_prep_stmt.execute(mock_bindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let pool = setup_db();

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/double-include.tql").unwrap();

        let mut binder = MysqlBinder::new();

        binder.values.insert("a".into(), vec![&"a_value"]);
        binder.values.insert("b".into(), vec![&"b_value"]);
        binder.values.insert("c".into(), vec![&"c_value"]);
        binder.values.insert("d".into(), vec![&"d_value"]);
        binder.values.insert("e".into(), vec![&"e_value"]);
        binder.values.insert("f".into(), vec![&"f_value"]);

        let mut mock_values:Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> = vec![];

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

        let (bound_sql, bindings) = binder.bind(stmt);
        let (mut mock_bound_sql, mock_bindings) = binder.mock_bind(mock_values, 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        for row in mock_prep_stmt.execute(mock_bindings.as_slice()).unwrap() {
            let mut c:Vec<String>  = vec![];

            let (col_1, col_2, col_3, col_4) = from_row::<(String, String, String, String)>(row.unwrap());
            c.push(col_1);
            c.push(col_2);
            c.push(col_3);
            c.push(col_4);

            mock_values.push(c);
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_multi_value_bind() {
        let pool = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT * FROM (::src/tests/values/double-include.tql::) AS main WHERE col_1 in (:col_1_values:) AND col_3 IN (:col_3_values:);").unwrap();

        let expected_sql = "SELECT * FROM (SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4) AS main WHERE col_1 in (?, ?) AND col_3 IN (?, ?);";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut binder = MysqlBinder::new();

        binder.values.insert("a".into(), vec![&"a_value"]);
        binder.values.insert("b".into(), vec![&"b_value"]);
        binder.values.insert("c".into(), vec![&"c_value"]);
        binder.values.insert("d".into(), vec![&"d_value"]);
        binder.values.insert("e".into(), vec![&"e_value"]);
        binder.values.insert("f".into(), vec![&"f_value"]);
        binder.values.insert("col_1_values".into(), vec![&"d_value", &"a_value"]);
        binder.values.insert("col_3_values".into(), vec![&"b_value", &"c_value"]);

        let (bound_sql, bindings) = binder.bind(stmt);

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        for row in prep_stmt.execute(bindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, expected_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }
}
