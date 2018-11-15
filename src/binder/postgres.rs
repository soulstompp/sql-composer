use std::collections::HashMap;

use postgres::{Connection, TlsMode};
use postgres::types::ToSql;

use super::{Binder, BinderConfig, parse_template};

struct PostgresBinder<'a> {
    config: BinderConfig,
    values: HashMap<String, Vec<&'a postgres::types::ToSql>>
}

impl<'a> PostgresBinder<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new()
        }
    }

}

impl <'a>Binder<Vec<&'a (dyn postgres::types::ToSql + 'a)>> for PostgresBinder<'a> {
    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
    }

    fn bind_var(u: usize, name: String) -> String {
        format!("${}", u)
    }

    fn values(&self, names: Vec<String>) -> Vec<&'a (dyn postgres::types::ToSql + 'a)> {
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
    use super::{Binder, BinderConfig, Connection, TlsMode, PostgresBinder, parse_template};
    use std::collections::HashMap;
    use time::Timespec;
    use postgres::types::ToSql;
    use std::cmp::PartialEq;

    #[derive(Debug, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    fn setup_db() -> Connection {
        let conn = Connection::connect("postgres://vagrant:vagrant@localhost:5432", TlsMode::None).unwrap();
        conn.execute("DROP TABLE IF EXISTS person;", &[]).unwrap();

        conn.execute("CREATE TABLE IF NOT EXISTS person (
                        id              SERIAL PRIMARY KEY,
                        name            VARCHAR NOT NULL,
                        data            BYTEA
                      )", &[]).unwrap();

        conn
    }

    #[test]
    fn test_binding() {
        let conn = setup_db();

        let person = Person {
            id: 0,
            name: "Steven".to_string(),
            data: None,
        };

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, data) VALUES (:name:, :data:)").unwrap();

        let mut bv = PostgresBinder::new();

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        println!("bound sql: {}", bound_sql);

        let expected_bound_sql = "INSERT INTO person (name, data) VALUES ($1, $2)";

        let expected_bind_values = &[&person.name as &ToSql, &person.data];

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        conn.execute(
            &bound_sql,
            &bindings,
        ).unwrap();

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, data FROM person WHERE name = ':name:' AND name = ':name:'").unwrap();


        println!("select: {}", select_stmt);

        let (bound_sql, bindings) = bv.bind(select_stmt);

        let expected_bound_sql = "SELECT id, name, data FROM person WHERE name = $1 AND name = $2";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let mut stmt = conn.prepare(&bound_sql).unwrap();

        let mut people:Vec<Person> = vec![];

        for row in &stmt.query(&bindings).unwrap() {
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
}
