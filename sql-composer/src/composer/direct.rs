use std::collections::{BTreeMap, HashMap};

use super::{Composer, ComposerConfig};

use crate::types::{ParsedItem, SqlComposition, SqlCompositionAlias};

use crate::types::value::ToValue;

pub struct Connection();

#[derive(Default)]
pub struct DirectComposer<'a> {
    config:           ComposerConfig,
    values:           BTreeMap<String, Vec<&'a ToValue>>,
    root_mock_values: Vec<BTreeMap<String, &'a str>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a str>>>,
}

impl<'a> DirectComposer<'a> {
    pub fn new() -> Self {
        Self {
            config: Self::config(),
            values: BTreeMap::new(),
            ..Default::default()
        }
    }
}

impl<'a> Composer for DirectComposer<'a> {
    type Value = &'a str;

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
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

    fn get_values(&self, _name: String) -> Option<&Vec<Self::Value>> {
        None
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

    fn insert_value(&mut self, _name: String, _values: Vec<Self::Value>) -> () {
        //self.values.insert(name, values);
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

    /*
    fn set_parsed_bind_values(&mut self, v: BTreeMap<String, Vec<Value>>) -> Result<(), ()> {
        unimplemented!("not here yet");
    }
    */
}

#[cfg(test)]
mod tests {
    use super::{Composer, DirectComposer};
    use crate::parser::parse_template;

    use crate::types::Span;

    use chrono::prelude::*;

    #[derive(Debug, PartialEq)]
    struct Person {
        id:           i32,
        name:         String,
        time_created: DateTime<Local>,
        data:         Option<Vec<u8>>,
    }

    #[test]
    fn test_binding() {
        let now = Local::now();

        let person = Person {
            id:           0,
            name:         "Steven".to_string(),
            time_created: now,
            data:         None,
        };

        let (remaining, insert_stmt) = parse_template(Span::new("INSERT INTO person (name, time_created, data) VALUES (:bind(name), :bind(time_created), :bind(data));".into()), None).unwrap();

        assert_eq!(*remaining.fragment, "", "nothing remaining");

        let mut composer = DirectComposer::new();

        composer.values.insert("name".into(), vec![&person.name]);
        composer
            .values
            .insert("time_created".into(), vec![&person.time_created]);
        composer.values.insert("data".into(), vec![&person.data]);

        let (bound_sql, _bindings) = composer.compose(&insert_stmt.item);

        let now_value = now.with_timezone(&Utc).format("%Y-%m-%dT%H:%M:%S%.f");

        let expected_bound_sql = format!(
            "INSERT INTO person (name, time_created, data) VALUES ( '{}', '{}', {} );",
            "Steven", now_value, "NULL"
        );

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let (remaining, select_stmt) = parse_template(Span::new("SELECT id, name, time_created, data FROM person WHERE name = ':bind(name)' AND time_created = ':bind(time_created)' AND name = ':bind(name)' AND time_created = ':bind(time_created)';".into()), None).unwrap();

        assert_eq!(*remaining.fragment, "", "nothing remaining");

        let (bound_sql, _bindings) = composer.compose(&select_stmt.item);

        let expected_bound_sql = format!("SELECT id, name, time_created, data FROM person WHERE name = '{}' AND time_created = '{}' AND name = '{}' AND time_created = '{}';", &person.name, now_value, &person.name, now_value);

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");
    }

    #[test]
    fn test_union_command() {}
}
