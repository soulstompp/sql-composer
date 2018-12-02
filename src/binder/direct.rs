use std::collections::HashMap;

use super::{Binder, BinderConfig};

use ::parser::SqlText;

use ::types::value::{Value, ToValue};

use chrono::prelude::*;

struct DirectBinder<'a> {
    config: BinderConfig,
    values: HashMap<String, Vec<&'a ToValue>>
}

impl<'a> DirectBinder<'a> {
    fn new() -> Self {
        Self{
         config: Self::config(),
         values: HashMap::new()
        }
    }
}

impl <'a>Binder for DirectBinder<'a> {
    type Value = &'a str;

    fn config() -> BinderConfig {
        BinderConfig {
            start: 0
        }
    }

    //TODO: error handling
    fn bind_var_tag(&self, _u: usize, name: String) -> String {
        let mut s = String::new();

        if let Some(values) = self.values.get(&name) {
            for value in values {
                if s.len() > 0 {
                    s.push(',');
                }

                s.push_str(&value.to_sql_text().unwrap().to_string());
            }
        }
        else {
            panic!("don't have proper error handling yet!");
        }

        s
    }

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Self::Value>) {
        (self.bind_var_tag(offset, name), vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::{Binder, DirectBinder, Value, ToValue};
    use super::super::parse_template;

    use std::collections::HashMap;
    use chrono::prelude::*;
    use rusqlite::{Connection, NO_PARAMS};

    #[derive(Debug, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        time_created: DateTime<Local>,
        data: Option<Vec<u8>>,
    }

    #[test]
    fn test_binding() {
        let now = Local::now();

        let person = Person {
            id: 0,
            name: "Steven".to_string(),
            time_created: now,
            data: None,
        };

        let (remaining, insert_stmt) = parse_template(b"INSERT INTO person (name, time_created, data) VALUES (:name:, :time_created:, :data:)").unwrap();

        assert_eq!(remaining, b"", "nothing remaining");

        let mut bv = DirectBinder::new();

        bv.values.insert("name".into(), vec![&person.name]);
        bv.values.insert("time_created".into(), vec![&person.time_created]);
        bv.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, bindings) = bv.bind(insert_stmt);

        let now_value = now.with_timezone(&Utc).format("%Y-%m-%dT%H:%M:%S%.f");

        let expected_bound_sql = format!("INSERT INTO person (name, time_created, data) VALUES ('{}', '{}', {})", "Steven", now_value, "NULL");

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let (remaining, select_stmt) = parse_template(b"SELECT id, name, time_created, data FROM person WHERE name = ':name:' AND time_created = ':time_created:' AND name = ':name:' AND time_created = ':time_created:'").unwrap();


        assert_eq!(remaining, b"", "nothing remaining");

        let (bound_sql, bindings) = bv.bind(select_stmt);

        let expected_bound_sql = format!("SELECT id, name, time_created, data FROM person WHERE name = '{}' AND time_created = '{}' AND name = '{}' AND time_created = '{}'", &person.name, now_value, &person.name, now_value);

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");
    }
}
