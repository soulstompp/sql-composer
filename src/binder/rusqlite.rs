use std::collections::HashMap;

use rusqlite::{Connection, NO_PARAMS};
use rusqlite::types::ToSql;

use super::{Binder, BinderConfig, parse_template};

struct RusqliteBinder<'a> {
    config: BinderConfig,
    values: HashMap<String, Vec<&'a rusqlite::types::ToSql>>
}

impl<'a> RusqliteBinder<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new()
        }
    }

}

impl <'a>Binder<Vec<&'a (dyn rusqlite::types::ToSql + 'a)>> for RusqliteBinder<'a> {
    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
    }
    fn bind_var(u: usize, name: String) -> String {
        format!("?{}", u)
    }

    fn values(&self, names: Vec<String>) -> Vec<&'a (dyn rusqlite::types::ToSql + 'a)> {
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
    use super::{Binder, BinderConfig, Connection, NO_PARAMS, RusqliteBinder, parse_template};
    use std::collections::HashMap;
    use time::Timespec;
    use rusqlite::types::ToSql;
    use std::cmp::PartialEq;

    #[derive(Debug, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        time_created: Timespec,
        data: Option<Vec<u8>>,
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
        ).unwrap();

        conn
    }

    #[test]
    fn test_binding() {
        let conn = setup_db();

        let person = Person {
            id: 0,
            name: "Steven".to_string(),
            time_created: time::get_time(),
            data: None,
        };

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, time_created, data) VALUES (:name:, :time_created:, :data:)").unwrap();

        let mut bv = RusqliteBinder::new();

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("time_created".into(), vec![&person.time_created]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        println!("bound sql: {}", bound_sql);

        let expected_bound_sql = "INSERT INTO person (name, time_created, data) VALUES (?1, ?2, ?3)";

        let expected_bind_values = &[&person.name as &ToSql, &person.time_created, &person.data];

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        conn.execute(
            &bound_sql,
            &bindings,
        ).unwrap();

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, time_created, data FROM person WHERE name = ':name:' AND time_created = ':time_created:' AND name = ':name:' AND time_created = ':time_created:'").unwrap();


        println!("select: {}", select_stmt);

        let (bound_sql, bindings) = bv.bind(select_stmt);

        let expected_bound_sql = "SELECT id, name, time_created, data FROM person WHERE name = ?1 AND time_created = ?2 AND name = ?3 AND time_created = ?4";

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");

        let mut stmt = conn.prepare(&bound_sql).unwrap();

        let person_iter = stmt.query_map(&bindings, |row| Person {
            id: row.get(0),
            name: row.get(1),
            time_created: row.get(2),
            data: row.get(3),
        }).unwrap();

        let mut people:Vec<Person> = vec![];

        for p in person_iter {
            people.push(p.unwrap());
        }

        assert_eq!(people.len(), 1, "found 1 person");
        let found = &people[0];

        assert_eq!(found.name, person.name, "person's name");
        assert_eq!(found.time_created, person.time_created, "person's time_created");
        assert_eq!(found.data, person.data, "person's data");
    }
}
