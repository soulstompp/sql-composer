// this is used during tests, must be at root
#[allow(unused_imports)]
#[macro_use]
extern crate sql_composer;

use std::collections::{BTreeMap, HashMap};

use sql_composer::composer::{ComposerConfig, ComposerTrait};
use sql_composer::error::Result;
use sql_composer::types::value::ToValue;
use sql_composer::types::{ParsedSqlComposition, SqlBinding, SqlCompositionAlias};

pub struct Connection();

#[derive(Default)]
pub struct Composer<'a> {
    #[allow(dead_code)]
    config:           ComposerConfig,
    values:           BTreeMap<String, Vec<&'a dyn ToValue>>,
    root_mock_values: Vec<BTreeMap<String, &'a str>>,
    mock_values:      HashMap<SqlCompositionAlias, Vec<BTreeMap<String, &'a str>>>,
}

impl<'a> Composer<'a> {
    pub fn new() -> Self {
        Self {
            config: Self::config(),
            values: BTreeMap::new(),
            ..Default::default()
        }
    }
}

impl<'a> ComposerTrait for Composer<'a> {
    type Value = &'a str;

    fn config() -> ComposerConfig {
        ComposerConfig { start: 0 }
    }

    fn binding_tag(&self, _u: usize, name: String) -> Result<String> {
        let mut s = String::new();

        if let Some(values) = self.values.get(&name) {
            for value in values {
                if s.len() > 0 {
                    s.push(',');
                }

                let v = &value.to_sql_text()?;
                s.push_str(&v.to_string());
            }
        }
        else {
            unimplemented!("unexpected binding_tag error!");
        }

        Ok(s)
    }

    fn compose_binding(
        &self,
        binding: SqlBinding,
        offset: usize,
    ) -> Result<(String, Vec<Self::Value>)> {
        Ok((self.binding_tag(offset, binding.name)?, vec![]))
    }

    fn get_values(&self, _name: String) -> Option<&Vec<Self::Value>> {
        None
    }

    fn compose_count_command(
        &self,
        composition: &ParsedSqlComposition,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        self.compose_count_default_command(composition, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedSqlComposition,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
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
}

#[cfg(test)]
mod tests {
    use super::{Composer, ComposerTrait, ParsedSqlComposition, Result, ToValue};

    type EmptyResult = Result<()>;

    use chrono::prelude::*;

    #[derive(Debug, PartialEq)]
    struct Person {
        id:           i32,
        name:         String,
        time_created: DateTime<Local>,
        data:         Option<Vec<u8>>,
    }

    #[test]
    fn test_db_binding() -> EmptyResult {
        let now = Local::now();

        let person = Person {
            id:           0,
            name:         "Steven".to_string(),
            time_created: now,
            data:         None,
        };

        let insert_stmt = ParsedSqlComposition::parse("INSERT INTO person (name, time_created, data) VALUES (:bind(name), :bind(time_created), :bind(data));")?;

        let mut composer = Composer::new();

        composer.values = bind_values!(&dyn ToValue:
                                       "name"         => [&person.name],
                                       "time_created" => [&person.time_created],
                                       "data"         => [&person.data]
        );

        let (bound_sql, _bindings) = composer.compose(&insert_stmt.item)?;

        let now_value = now.with_timezone(&Utc).format("%Y-%m-%dT%H:%M:%S%.f");

        let expected_bound_sql = format!(
            "INSERT INTO person (name, time_created, data) VALUES ( '{}', '{}', {} );",
            "Steven", now_value, "NULL"
        );

        assert_eq!(bound_sql, expected_bound_sql, "insert basic bindings");

        let select_stmt = ParsedSqlComposition::parse("SELECT id, name, time_created, data FROM person WHERE name = ':bind(name)' AND time_created = ':bind(time_created)' AND name = ':bind(name)' AND time_created = ':bind(time_created)';")?;

        let (bound_sql, _bindings) = composer.compose(&select_stmt.item)?;

        let expected_bound_sql = format!("SELECT id, name, time_created, data FROM person WHERE name = '{}' AND time_created = '{}' AND name = '{}' AND time_created = '{}';", &person.name, now_value, &person.name, now_value);

        assert_eq!(bound_sql, expected_bound_sql, "select multi-use bindings");
        Ok(())
    }

    #[test]
    fn test_union_command() {}
}
