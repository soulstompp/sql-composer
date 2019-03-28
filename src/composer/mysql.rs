use std::collections::{BTreeMap, HashMap};

use mysql::prelude::ToValue;

use super::{Composer, ComposerConfig};

use crate::types::SqlCompositionAlias;

#[derive(Default)]
struct MysqlComposer<'a> {
    config:           ComposerConfig,
    values:           HashMap<String, Vec<&'a ToValue>>,
    root_mock_values: Vec<BTreeMap<String, &'a ToValue>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a ToValue>>>,
}

impl<'a> MysqlComposer<'a> {
    fn new() -> Self {
        Self {
            config: Self::config(),
            ..Default::default()
        }
    }
}

impl<'a> Composer for MysqlComposer<'a> {
    type Value = &'a (dyn ToValue + 'a);

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
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
            }
            None => panic!("no value for binding: {}, {}", i, name),
        };

        (sql, new_values)
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
    use super::{Composer, MysqlComposer};
    use crate::parser::parse_template;
    use crate::types::{ParsedItem, Span, SqlComposition, SqlCompositionAlias, SqlDbObject};
    use mysql::{from_row, Pool, Row};

    use std::collections::{BTreeMap, HashMap};

    #[derive(Debug, PartialEq)]
    struct Person {
        id:   i32,
        name: String,
        data: Option<String>,
    }

    fn setup_db() -> Pool {
        let pool = Pool::new("mysql://vagrant:password@localhost:3306/vagrant").unwrap();

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

        let mut composer = MysqlComposer::new();

        let (remaining, insert_stmt) = parse_template(
            Span::new("INSERT INTO person (name, data) VALUES (:bind(name), :bind(data));".into()),
            None,
        )
        .unwrap();

        assert_eq!(*remaining.fragment, "", "insert stmt nothing remaining");

        composer.values.insert("name".into(), vec![&person.name]);
        composer.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = composer.compose(&insert_stmt.item);

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ( ?, ? );";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let _res = &pool.prep_exec(&bound_sql, &rebindings.as_slice());

        let (remaining, select_stmt) = parse_template(Span::new("SELECT id, name, data FROM person WHERE name = ':bind(name)' AND name = ':bind(name)';".into()), None).unwrap();

        assert_eq!(*remaining.fragment, "", "select stmt nothing remaining");

        let (bound_sql, bindings) = composer.compose(&select_stmt.item);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = ? AND name = ?;";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        let people: Vec<Person> = pool
            .prep_exec(&bound_sql, &rebindings.as_slice())
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

    fn parse(input: &str) -> ParsedItem<SqlComposition> {
        let (_remaining, stmt) = parse_template(Span::new(input.into()), None).unwrap();

        stmt
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

        let stmt = SqlComposition::from_path_name("src/tests/values/simple.tql".into()).unwrap();

        let mut composer = MysqlComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> =
            vec![BTreeMap::new()];

        mock_values[0].insert("col_1".into(), &"a_value");
        mock_values[0].insert("col_2".into(), &"b_value");
        mock_values[0].insert("col_3".into(), &"c_value");
        mock_values[0].insert("col_4".into(), &"d_value");

        let (bound_sql, bindings) = composer.compose(&stmt.item);
        composer.root_mock_values = mock_values;

        let (mock_bound_sql, mock_bindings) = composer.compose(&stmt.item);

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];
        let mut mock_values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let _mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
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

        let stmt = SqlComposition::from_path_name("src/tests/values/include.tql".into()).unwrap();

        let mut composer = MysqlComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> =
            vec![];

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
        let (mut mock_bound_sql, mock_bindings) =
            composer.mock_compose(&mock_values, 0);

        mock_bound_sql.push(';');

        println!("bound_sql: {}", bound_sql);

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
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

        let stmt =
            SqlComposition::from_path_name("src/tests/values/double-include.tql".into()).unwrap();

        let mut composer = MysqlComposer::new();

        composer.values.insert("a".into(), vec![&"a_value"]);
        composer.values.insert("b".into(), vec![&"b_value"]);
        composer.values.insert("c".into(), vec![&"c_value"]);
        composer.values.insert("d".into(), vec![&"d_value"]);
        composer.values.insert("e".into(), vec![&"e_value"]);
        composer.values.insert("f".into(), vec![&"f_value"]);

        let mut mock_values: Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>> =
            vec![];

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
        let (mut mock_bound_sql, mock_bindings) =
            composer.mock_compose(&mock_values, 1);

        mock_bound_sql.push(';');

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(bound_sql, mock_bound_sql, "preparable statements match");

        let mut mock_prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut mock_values: Vec<Vec<String>> = vec![];

        let mock_rebindings = mock_bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in mock_prep_stmt.execute(&mock_rebindings.as_slice()).unwrap() {
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

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["a_value", "b_value", "c_value", "d_value"],
        ];

        let mut composer = MysqlComposer::new();

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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
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

        let (_remaining, stmt) = parse_template(
            Span::new(":count(src/tests/values/double-include.tql);".into()),
            None,
        )
        .unwrap();

        println!("made it through parse");
        let expected_bound_sql = "SELECT COUNT(1) FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS count_main";

        let mut composer = MysqlComposer::new();

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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<usize>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
            let count = from_row::<(usize)>(row.unwrap());
            values.push(vec![count]);
        }

        let expected_values: Vec<Vec<usize>> = vec![vec![3]];

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_union_command() {
        let pool = setup_db();

        let (_remaining, stmt) = parse_template(Span::new(":union(src/tests/values/double-include.tql, src/tests/values/include.tql, src/tests/values/double-include.tql);".into()), None).unwrap();

        println!("made it through parse");
        let expected_bound_sql = "SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4";

        let mut composer = MysqlComposer::new();

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

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(rebindings.as_slice()).unwrap() {
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

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["d_value", "f_value", "b_value", "a_value"],
            vec!["ee_value", "dd_value", "bb_value", "aa_value"],
        ];

        let mut composer = MysqlComposer::new();

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
            Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>>,
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

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 0, false);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
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

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM (:compose(src/tests/values/double-include.tql)) AS main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = MysqlComposer::new();

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
            Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>>,
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

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 0, false);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }

    #[test]
    fn test_mock_db_object() {
        let pool = setup_db();

        let (_remaining, stmt) = parse_template(Span::new("SELECT * FROM main WHERE col_1 in (:bind(col_1_values)) AND col_3 IN (:bind(col_3_values));".into()), None).unwrap();

        let expected_bound_sql = "SELECT * FROM ( SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 UNION ALL SELECT ? AS col_1, ? AS col_2, ? AS col_3, ? AS col_4 ) AS main WHERE col_1 in ( ?, ? ) AND col_3 IN ( ?, ? );";

        let expected_values = vec![
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["dd_value", "ff_value", "bb_value", "aa_value"],
            vec!["aa_value", "bb_value", "cc_value", "dd_value"],
        ];

        let mut composer = MysqlComposer::new();

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
            Vec<BTreeMap<std::string::String, &dyn mysql::prelude::ToValue>>,
        > = HashMap::new();

        {
            let path_entry = mock_values
                .entry(SqlCompositionAlias::DbObject(SqlDbObject {
                    object_name:  "main".into(),
                    object_alias: None,
                }))
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

        let (bound_sql, bindings) = composer.compose_statement(&stmt, 0, false);

        assert_eq!(bound_sql, expected_bound_sql, "preparable statements match");

        let mut prep_stmt = pool.prepare(&bound_sql).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let rebindings = bindings.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(*x);
            acc
        });

        for row in prep_stmt.execute(&rebindings.as_slice()).unwrap() {
            values.push(get_row_values(row.unwrap()));
        }

        assert_eq!(values, expected_values, "exected values");
    }
}
