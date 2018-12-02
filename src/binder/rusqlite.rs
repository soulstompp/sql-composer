use std::collections::HashMap;

use rusqlite::types::ToSql;

use super::{Binder, BinderConfig};

struct RusqliteBinder<'a> {
    config: BinderConfig,
    values: HashMap<String, Vec<&'a ToSql>>
}

impl<'a> RusqliteBinder<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new()
        }
    }
}

impl <'a>Binder for RusqliteBinder<'a> {
    type Value = &'a (dyn ToSql + 'a);

    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
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
                        sql.push_str("'");
                    }

                    sql.push_str(&self.bind_var_tag(new_values.len() + offset, name.to_string()));

                    new_values.push(*iv);
                }
            },
            None => panic!("no value for binding: {}", i)
        };

        (sql, new_values)
    }
}

#[cfg(test)]
mod tests {
    use super::{Binder, RusqliteBinder};
    use super::super::parse_template;

    use std::collections::HashMap;
    use time::Timespec;
    use rusqlite::{Connection, NO_PARAMS};

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

        assert_eq!(remaining, b"", "nothing remaining");

        let mut bv = RusqliteBinder::new();

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("time_created".into(), vec![&person.time_created]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        let expected_bound_sql = "INSERT INTO person (name, time_created, data) VALUES (?1, ?2, ?3)";

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        conn.execute(
            &bound_sql,
            &bindings,
        ).unwrap();

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, time_created, data FROM person WHERE name = ':name:' AND time_created = ':time_created:' AND name = ':name:' AND time_created = ':time_created:'").unwrap();


        assert_eq!(remaining, b"", "nothing remaining");

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
