use std::collections::{HashMap, BTreeMap};

use std::path::PathBuf;

use postgres::types::ToSql;

use std::rc::Rc;

use ::parser::{SqlStatement, parse_template};
use super::{Expander, ExpanderConfig};

struct PostgresExpander<'a> {
    config: ExpanderConfig,
    values: HashMap<String, Vec<Rc<&'a ToSql>>>,
    root_mock_values: Vec<BTreeMap<String, Box<ToSql>>>,
    mock_values: HashMap<PathBuf, Vec<BTreeMap<String, Box<ToSql>>>>,
}

impl<'a> PostgresExpander<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new(),
         root_mock_values: Vec::new(),
         mock_values: HashMap::new()
        }
    }

}

impl <'a>Expander for PostgresExpander<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> ExpanderConfig {
        ExpanderConfig {
            start: 0
        }
    }

    fn bind_var_tag(&self, u: usize, _name: String) -> String {
        format!("${}", u)
    }

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Rc<Self::Value>>) {
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

                    new_values.push(Rc::to_owned(iv));
                }
            },
            None => panic!("no value for binding: {}", new_values.len())
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
    fn get_mock_values(&self, name: String) -> Option<&BTreeMap<String, Self::Value>> {
        self.values.get(&name)
    }
    */
}

#[cfg(test)]
mod tests {
    use super::{Expander, PostgresExpander};

    use ::parser::{SqlStatement, parse_template};

    use postgres::{Connection, TlsMode};
    use postgres::rows::{Row, Rows};
    use postgres::types::ToSql;

    use std::collections::{BTreeMap, HashMap};

    use std::path::PathBuf;

    use std::rc::Rc;

    use std::borrow::Borrow;

    use std::ops::Deref;

    #[derive(Debug, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    fn setup_db() -> Connection {
        Connection::connect("postgres://vagrant:vagrant@localhost:5432", TlsMode::None).unwrap()
    }

    #[test]
    fn test_binding() {
        let conn = setup_db();

        conn.execute("DROP TABLE IF EXISTS person;", &[]).unwrap();

        conn.execute("CREATE TABLE IF NOT EXISTS person (
                        id              SERIAL PRIMARY KEY,
                        name            VARCHAR NOT NULL,
                        data            BYTEA
                      )", &[]).unwrap();


        let person = Person {
            id: 0,
            name: "Steven".to_string(),
            data: None,
        };

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));", None).unwrap();

        assert_eq!(remaining, b"", "insert stmt nothing remaining");

        let mut expander = PostgresExpander::new();

        expander.values.insert("name".into(), vec![Rc::new(&person.name)]);
        expander.values.insert("data".into(), vec![Rc::new(&person.data)]);

        let (bound_sql, bindings) = expander.expand(&insert_stmt);

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ($1, $2);";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        conn.execute(
            &bound_sql,
            &rebindings,
        ).unwrap();

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';", None).unwrap();

        assert_eq!(remaining, b"", "select stmt nothing remaining");

        let (bound_sql, bindings) = expander.expand(&select_stmt);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = $1 AND name = $2;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let stmt = conn.prepare(&bound_sql).unwrap();

        let mut people:Vec<Person> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &stmt.query(&rebindings).unwrap() {
            people.push(Person {
                id: row.get(0),
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

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/simple.tql").unwrap();

        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn ToSql>>> = vec![BTreeMap::new()];

        mock_values[0].insert("col_1".into(), Rc::new(&"a_value"));
        mock_values[0].insert("col_2".into(), Rc::new(&"b_value"));
        mock_values[0].insert("col_3".into(), Rc::new(&"c_value"));
        mock_values[0].insert("col_4".into(), Rc::new(&"d_value"));

        let (bound_sql, bindings) = expander.expand(&stmt);
        let (mut mock_bound_sql, mock_bindings) = expander.mock_expand(&stmt, &mock_values, &HashMap::new(), 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();


        let mut values:Vec<Vec<String>> = vec![];
        let mut mock_values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mut mock_prep_stmt = conn.prepare(&mock_bound_sql).unwrap();

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &mock_prep_stmt.query(&mock_rebindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_include_template() {
        let conn = setup_db();

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/include.tql").unwrap();

        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn ToSql>>> = vec![];

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

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mut mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &mock_prep_stmt.query(&mock_rebindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_bind_double_include_template() {
        let conn = setup_db();

        let stmt = SqlStatement::from_utf8_path_name(b"src/tests/values/double-include.tql").unwrap();

        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);

        let mut mock_values:Vec<BTreeMap<std::string::String, Rc<&dyn ToSql>>> = vec![];

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
        let (mut mock_bound_sql, mock_bindings) = expander.mock_expand(&stmt, &mock_values, &HashMap::new(), 0);

        mock_bound_sql.push(';');

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }

        let mut mock_prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut mock_values:Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &mock_prep_stmt.query(&mock_rebindings).unwrap() {
            mock_values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");
        assert_eq!(values, mock_values, "exected values");
    }

    #[test]
    fn test_multi_value_bind() {
        let conn = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT col_1, col_2, col_3, col_4 FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_sql = "SELECT col_1, col_2, col_3, col_4 FROM (SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4) AS main WHERE col_1 in ($13, $14) AND col_3 IN ($15, $16);";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        println!("setup expander");
        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"d_value"), Rc::new(&"a_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"b_value"), Rc::new(&"c_value")]);

        println!("binding");
        let (bound_sql, bindings) = expander.expand(&stmt);

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }

        assert_eq!(bound_sql, expected_sql, "preparable statements match");
        assert_eq!(values, expected_values, "expected values");
    }
    
    #[test]
    fn test_include_mock_multi_value_bind() {
        let conn = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT * FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM (SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4) AS main WHERE col_1 in ($9, $10) AND col_3 IN ($11, $12);";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"ee_value"), Rc::new(&"d_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"bb_value"), Rc::new(&"b_value")]);

        let mut path_mock_values:HashMap<PathBuf, Vec<BTreeMap<std::string::String, Rc<&dyn ToSql>>>> = HashMap::new();

        {
            let mut mock_path_entry = path_mock_values.entry(PathBuf::from("src/tests/values/include.tql")).or_insert(Vec::new());

            mock_path_entry.push(BTreeMap::new());
            mock_path_entry[0].insert("col_1".into(), Rc::new(&"ee_value"));
            mock_path_entry[0].insert("col_2".into(), Rc::new(&"dd_value"));
            mock_path_entry[0].insert("col_3".into(), Rc::new(&"bb_value"));
            mock_path_entry[0].insert("col_4".into(), Rc::new(&"aa_value"));
        }

        let (bound_sql, bindings) = expander.mock_expand(&stmt, &vec![], &path_mock_values, 1);

        println!("bound sql: {}",  bound_sql);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }
        
        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_double_include_multi_value_bind() {
        let conn = setup_db();

        let (remaining, stmt) = parse_template(b"SELECT * FROM (:expand(<src/tests/values/double-include.tql>)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));", None).unwrap();

        let expected_bound_sql = "SELECT * FROM (SELECT $1 AS col_1, $2 AS col_2, $3 AS col_3, $4 AS col_4 UNION ALL SELECT $5 AS col_1, $6 AS col_2, $7 AS col_3, $8 AS col_4 UNION ALL SELECT $9 AS col_1, $10 AS col_2, $11 AS col_3, $12 AS col_4) AS main WHERE col_1 in ($13, $14) AND col_3 IN ($15, $16);";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut expander = PostgresExpander::new();

        expander.values.insert("a".into(), vec![Rc::new(&"a_value")]);
        expander.values.insert("b".into(), vec![Rc::new(&"b_value")]);
        expander.values.insert("c".into(), vec![Rc::new(&"c_value")]);
        expander.values.insert("d".into(), vec![Rc::new(&"d_value")]);
        expander.values.insert("e".into(), vec![Rc::new(&"e_value")]);
        expander.values.insert("f".into(), vec![Rc::new(&"f_value")]);
        expander.values.insert("col_1_values".into(), vec![Rc::new(&"dd_value"), Rc::new(&"aa_value")]);
        expander.values.insert("col_3_values".into(), vec![Rc::new(&"bb_value"), Rc::new(&"cc_value")]);

        let mut path_mock_values:HashMap<PathBuf, Vec<BTreeMap<std::string::String, Rc<&dyn ToSql>>>> = HashMap::new();

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

        let (bound_sql, bindings) = expander.mock_expand(&stmt, &vec![], &path_mock_values, 1);

        let mut prep_stmt = conn.prepare(&bound_sql).unwrap();

        let mut values:Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*Rc::deref(x));
            acc
        });

        for row in &prep_stmt.query(&rebindings).unwrap() {
            values.push(get_row_values(row));
        }
        
        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");
        assert_eq!(values, expected_values, "exected values");
    }
}
