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

impl <'a>Binder<Vec<&'a (dyn ToValue + 'a)>> for MysqlBinder<'a> {
    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
    }

    fn bind_var(_u: usize, _name: String) -> String {
        format!("?")
    }

    fn values<'b>(&self, names: Vec<String>) -> Vec<&'a (dyn ToValue + 'a)> {
        let mut acc = vec![];

        for n in names {
            match self.values.get(&n) {
                Some(v) => {
                    for iv in v.iter() {
                        acc.push(*iv);
                    }
                },
                None => panic!("no value for binding: {}", n)
            }
        }

        acc
    }
}

#[cfg(test)]
mod tests {
    use super::{Binder, MysqlBinder};
    use super::super::parse_template;
    use mysql::{Pool};

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

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, data) VALUES (:name:, :data:)").unwrap();

        assert_eq!(remaining, b"", "insert stmt nothing remaining");

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        println!("bound sql: {}", bound_sql);
        let expected_bound_sql = "INSERT INTO person (name, data) VALUES (?, ?)";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let _res = &pool.prep_exec(
            &bound_sql,
            bindings.as_slice(),
        );

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, data FROM person WHERE name = ':name:' AND name = ':name:'").unwrap();

        assert_eq!(remaining, b"", "select stmt nothing remaining");

        println!("select: {}", select_stmt);

        let (bound_sql, bindings) = bv.bind(select_stmt);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = ? AND name = ?";

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
}
