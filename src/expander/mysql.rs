use std::collections::HashMap;

use mysql::prelude::ToValue;

use super::{Expander, ExpanderConfig};

use std::rc::Rc;

struct MysqlExpander<'a> {
    config: ExpanderConfig,
    values: HashMap<String, Vec<Rc<&'a ToValue>>>,
}

impl <'a>MysqlExpander<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new(),
        }
    }
}

impl <'a>Expander for MysqlExpander<'a> {
    type Value = &'a (dyn ToValue + 'a);

    fn config() -> ExpanderConfig {
        ExpanderConfig {
            start: 0
        }
    }

    fn bind_var_tag(&self, _u: usize, _name: String) -> String {
        format!("?")
    }

    fn bind_values<'b>(&self, name: String, offset: usize) -> (String, Vec<Rc<Self::Value>>) {
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

                    new_values.push(Rc::clone(iv));
                }
            },
            None => panic!("no value for binding: {}, {}", i, name)
        };

        (sql, new_values)
    }

    fn get_values(&self, name: String) -> Option<&Vec<Rc<Self::Value>>> {
        self.values.get(&name)
    }

    fn insert_value(&mut self, name: String, values: Vec<Rc<Self::Value>>) -> () {
        self.values.insert(name, values);
    }

    /*
    fn get_mock_values(&self, name: String) -> Option<Vec<Self::Value>> {
        let values = vec![];

        for row in mock_values.iter() {
            if r > 0 {
                sql.push_str(" UNION ");
            }

            sql.push_str("SELECT ");

            for (name, value) in row {
                i += 1;
                c += 1;

                if c > 1 {
                    sql.push_str(", ")
                }

                sql.push_str(&self.bind_var_tag(i + offset, name.to_string()));
                sql.push_str(&format!(" AS {}", &name));

                values.push(*value);
            }
        }

        Some(values)
    }
    */
}

#[cfg(test)]
mod tests {
    use super::{Expander, MysqlExpander};
    use crate::parser::{SqlComposition, parse_template};
    use mysql::prelude::*;
    use mysql::{Pool, from_row, Row};

    use std::collections::{BTreeMap, HashMap};
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::rc::Rc;

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

        let mut expander = MysqlExpander::new();

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));", None).unwrap();

        assert_eq!(remaining, b"", "insert stmt nothing remaining");

        expander.values.insert("name".into(), vec![Rc::new(&person.name)]);
        expander.values.insert("data".into(), vec![Rc::new(&person.data)]);

        let (bound_sql, bindings) = expander.expand(&insert_stmt);

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES (?, ?);";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        let _res = &pool.prep_exec(
            &bound_sql,
            &rebindings.as_slice(),
        );

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';", None).unwrap();

        assert_eq!(remaining, b"", "select stmt nothing remaining");

        let (bound_sql, bindings) = expander.expand(&select_stmt);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = ? AND name = ?;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        let people:Vec<Person> = pool.prep_exec(&bound_sql, &rebindings.as_slice()).map(|result| {
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

    fn parse(input: &str) -> SqlComposition {
        let (remaining, stmt) = parse_template(input.as_bytes(), None).unwrap();

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

        let stmt = SqlComposition::from_utf8_path_name(b"src/tests/values/simple.tql").unwrap();

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn mysql::prelude::ToValue>>> = vec![BTreeMap::new()];

        mock_values[0].insert("col_1".into(), Rc::new(&"a_value"));
        mock_values[0].insert("col_2".into(), Rc::new(&"b_value"));
        mock_values[0].insert("col_3".into(), Rc::new(&"c_value"));
        mock_values[0].insert("col_4".into(), Rc::new(&"d_value"));

        let (bound_sql, bindings) = expander.expand(&stmt);
        let (mut mock_bound_sql, mock_bindings) = expander.mock_expand(&stmt, &mock_values, &HashMap::new(), 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];
        let mut mock_values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(mock_rebindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let pool = setup_db();

        let stmt = SqlComposition::from_utf8_path_name(b"src/tests/values/include.tql").unwrap();

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn mysql::prelude::ToValue>>> = vec![];

        mock_values.push(BTreeMap::new());
        mock_values[0].insert("col_1".into(), Rc::new(&"e_value"));
        mock_values[0].insert("col_2".into(), Rc::new(&"d_value"));
        mock_values[0].insert("col_3".into(), Rc::new(&"b_value"));
        mock_values[0].insert("col_4".into(), Rc::new(&"a_value"));

        mock_values.push(BTreeMap::new());
        mock_values[1].insert("col_1".into(), Rc::new(&"a_value"));
        mock_values[1].insert("col_2".into(), Rc::new(&"b_value"));
        mock_values[1].insert("col_3".into(), Rc::new(&"c_value"));
        mock_values[1].insert("col_4".into(), Rc::new(&"d_value"));

        let (bound_sql, bindings) = expander.expand(&stmt);
        let (mut mock_bound_sql, mock_bindings) = expander.mock_expand(&stmt, &mock_values, &HashMap::new(), 0);

        mock_bound_sql.push(';');

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in mock_prep_stmt.execute(&mock_rebindings.as_slice()).unwrap() {
            mock_values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let pool = setup_db();

        let stmt = SqlComposition::from_utf8_path_name(b"src/tests/values/double-include.tql").unwrap();

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn mysql::prelude::ToValue>>> = vec![];

        mock_values.push(BTreeMap::new());
        mock_values[0].insert("col_1".into(), Rc::new(&"d_value"));
        mock_values[0].insert("col_2".into(), Rc::new(&"f_value"));
        mock_values[0].insert("col_3".into(), Rc::new(&"b_value"));
        mock_values[0].insert("col_4".into(), Rc::new(&"a_value"));

        mock_values.push(BTreeMap::new());
        mock_values[1].insert("col_1".into(), Rc::new(&"e_value"));
        mock_values[1].insert("col_2".into(), Rc::new(&"d_value"));
        mock_values[1].insert("col_3".into(), Rc::new(&"b_value"));
        mock_values[1].insert("col_4".into(), Rc::new(&"a_value"));

        mock_values.push(BTreeMap::new());
        mock_values[2].insert("col_1".into(), Rc::new(&"a_value"));
        mock_values[2].insert("col_2".into(), Rc::new(&"b_value"));
        mock_values[2].insert("col_3".into(), Rc::new(&"c_value"));
        mock_values[2].insert("col_4".into(), Rc::new(&"d_value"));

        let (bound_sql, bindings) = expander.expand(&stmt);
        let (mut mock_bound_sql, mock_bindings) = expander.mock_expand(&stmt, &mock_values, &HashMap::new(), 1);

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in mock_prep_stmt.execute(&mock_rebindings.as_slice()).unwrap() {
            let mut c:Vec<String>  = vec![];

            let (col_1, col_2, col_3, col_4) = from_row::<(String, String, String, String)>(row.unwrap());
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

        let (remaining, stmt) = parse_template(b"SELECT * FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM (SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4) AS main WHERE col_1 in (?, ?) AND col_3 IN (?, ?);";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"d_value"), Rc::new(&"a_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"b_value"), Rc::new(&"c_value")]);

        let (bound_sql, bindings) = expander.expand(&stmt);

        println!("bound_sql: {}", bound_sql);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_count_command() {
        let pool = setup_db();

        let (remaining, stmt) = parse_template(b":count(src/tests/values/double-include.tql);", None).unwrap();

        println!("made it through parse");
        let expected_bound_sql = "SELECT COUNT(*) FROM (SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4) AS count_main";

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"d_value"), Rc::new(&"a_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"b_value"), Rc::new(&"c_value")]);

        let (bound_sql, bindings) = expander.expand(&stmt);

        println!("bound_sql: {}", bound_sql);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<usize>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
            let count = from_row::<(usize)>(row.unwrap());
            values.push(vec![count]);
        }

        let expected_values:Vec<Vec<usize>> = vec![
            vec![3],
        ];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_include_mock_multi_value_bind() {
        let pool = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT * FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM (SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4) AS main WHERE col_1 in (?, ?) AND col_3 IN (?, ?);";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"ee_value"), Rc::new(&"d_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"bb_value"), Rc::new(&"b_value")]);

        let mut path_mock_values:HashMap<PathBuf, Vec<BTreeMap<std::string::String, Rc<&dyn mysql::prelude::ToValue>>>> = HashMap::new();

        {
            let mut mock_path_entry = path_mock_values.entry(PathBuf::from("src/tests/values/include.tql")).or_insert(Vec::new());

            mock_path_entry.push(BTreeMap::new());
            mock_path_entry[0].insert("col_1".into(), Rc::new(&"ee_value"));
            mock_path_entry[0].insert("col_2".into(), Rc::new(&"dd_value"));
            mock_path_entry[0].insert("col_3".into(), Rc::new(&"bb_value"));
            mock_path_entry[0].insert("col_4".into(), Rc::new(&"aa_value"));
        }

        let (bound_sql, bindings) = expander.mock_expand(&stmt, &vec![], &path_mock_values, 0);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() {
        let pool = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT * FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM (SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4) AS main WHERE col_1 in (?, ?) AND col_3 IN (?, ?);";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut expander = MysqlExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"dd_value"), Rc::new(&"aa_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"bb_value"), Rc::new(&"cc_value")]);

        let mut path_mock_values:HashMap<PathBuf, Vec<BTreeMap<std::string::String, Rc<&dyn mysql::prelude::ToValue>>>> = HashMap::new();

        {
            let mut mock_path_entry = path_mock_values.entry(PathBuf::from("src/tests/values/double-include.tql")).or_insert(Vec::new());

            mock_path_entry.push(BTreeMap::new());
            mock_path_entry[0].insert("col_1".into(), Rc::new(&"dd_value"));
            mock_path_entry[0].insert("col_2".into(), Rc::new(&"ff_value"));
            mock_path_entry[0].insert("col_3".into(), Rc::new(&"bb_value"));
            mock_path_entry[0].insert("col_4".into(), Rc::new(&"aa_value"));

            mock_path_entry.push(BTreeMap::new());
            mock_path_entry[1].insert("col_1".into(), Rc::new(&"dd_value"));
            mock_path_entry[1].insert("col_2".into(), Rc::new(&"ff_value"));
            mock_path_entry[1].insert("col_3".into(), Rc::new(&"bb_value"));
            mock_path_entry[1].insert("col_4".into(), Rc::new(&"aa_value"));

            mock_path_entry.push(BTreeMap::new());
            mock_path_entry[2].insert("col_1".into(), Rc::new(&"aa_value"));
            mock_path_entry[2].insert("col_2".into(), Rc::new(&"bb_value"));
            mock_path_entry[2].insert("col_3".into(), Rc::new(&"cc_value"));
            mock_path_entry[2].insert("col_4".into(), Rc::new(&"dd_value"));
        }

        let (bound_sql, bindings) = expander.mock_expand(&stmt, &vec![], &path_mock_values, 0);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }
}
