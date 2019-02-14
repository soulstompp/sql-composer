pub mod direct;

pub mod mysql;
pub mod postgres;
pub mod rusqlite;

pub use super::parser::parse_template;
use crate::types::{Sql, SqlComposition, SqlCompositionAlias};
use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::rc::Rc;

use std::error::Error;

#[derive(Default)]
pub struct ComposerConfig {
    start: usize,
}

pub enum ComposerErrorMessage {
    Declined,
    Error(String),
}

pub trait Composer: Sized {
    type Value: Copy;

    fn compose(&self, s: &SqlComposition) -> (String, Vec<Self::Value>) {
        self.compose_statement(s, 1usize, false)
    }

    fn compose_statement(
        &self,
        sc: &SqlComposition,
        offset: usize,
        child: bool,
    ) -> (String, Vec<Self::Value>) {
        let mut i = offset;

        let mut sql = String::new();

        let mut values: Vec<Self::Value> = vec![];

        if sc.command.is_some() {
            return self.compose_wrapper(&sc, i, true).unwrap();
        }

        for c in &sc.sql {
            let (sub_sql, sub_values) = match c {
                Sql::Literal(t) => (t.to_string(), vec![]),
                Sql::Binding(b) => self.bind_values(b.name.to_string(), i),
                Sql::Composition((ss, _aliases)) => self.compose_statement(&ss, i, true),
                Sql::Ending(e) => {
                    if child {
                        ("".to_string(), vec![])
                    }
                    else {
                        (e.to_string(), vec![])
                    }
                }
            };

            sql.push_str(&sub_sql);

            for sv in sub_values {
                values.push(sv);
            }

            i = values.len() + offset;
        }

        (sql, values)
    }

    fn compose_wrapper<'c>(
        &self,
        composition: &SqlComposition,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        match &composition.command {
            Some(s) => {
                match s.to_lowercase().as_str() {
                    "count" => {
                        let mut out = SqlComposition::default();

                        out.push_literal("SELECT COUNT(");

                        let columns = composition.column_list().unwrap();

                        if let Some(c) = columns {
                            out.push_literal(&c);
                        }
                        else {
                            out.push_literal("1");
                        }

                        out.push_literal(") FROM ");

                        for alias in composition.of.iter() {
                            out.push_literal("(");
                            match composition.aliases.get(&alias) {
                                Some(sc) => {
                                    out.push_sub_comp(sc.clone());
                                }
                                None => {
                                    panic!("no alias found with alias: {:?}", alias);
                                }
                            }

                            out.push_literal(") AS count_main");
                        }

                        out.end(";");

                        Ok(self.compose_statement(&out, offset, child))
                    }
                    "compose" => {
                        let mut out = composition.clone();

                        out.command = None;

                        match &out.of[0].path() {
                            Some(path) => match self.mock_values().get(path) {
                                Some(e) => Ok(self.mock_compose(
                                    &out.aliases.get(&out.of[0]).unwrap(),
                                    e,
                                    offset,
                                )),
                                None => Ok(self.compose_statement(
                                    &out.aliases.get(&out.of[0]).unwrap(),
                                    offset,
                                    child,
                                )),
                            },
                            None => Ok(self.compose_statement(
                                &out.aliases.get(&out.of[0]).unwrap(),
                                offset,
                                child,
                            )),
                        }
                    }
                    "union" => self.union_compose(composition, offset, child),
                    // TODO: handle this error better
                    _ => panic!("unknown call"),
                }
            }
            None => Ok(self.compose_statement(&composition, offset, child)),
        }
    }

    fn union_compose(
        &self,
        composition: &SqlComposition,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        let mut out = SqlComposition::default();

        // columns in this case would mean an compose on each side of the union literal
        let _columns = composition.column_list().unwrap();

        let mut i = 0usize;

        if composition.of.len() < 2 {
            panic!("union requires 2 of arguments");
        }

        for alias in composition.of.iter() {
            if i > 0 {
                out.push_literal(" UNION ");
            }

            match composition.aliases.get(&alias) {
                Some(sc) => {
                    out.push_sub_comp(sc.clone());
                }
                None => {
                    panic!("no alias found with alias: {:?}", alias);
                }
            }

            i += 1;
        }

        out.end(";");

        Ok(self.compose_statement(&out, offset, child))
    }

    fn bind_var_tag(&self, u: usize, name: String) -> String;

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Self::Value>);

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>>;

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> ();

    fn config() -> ComposerConfig;

    //fn insert_mock_values(&mut self, alias: SqlCompositionAlias, values: Vec<Self::Value>) -> ();

    fn root_mock_values(&self) -> &Vec<BTreeMap<String, Self::Value>>;

    fn mock_values(&self) -> &HashMap<PathBuf, Vec<BTreeMap<String, Self::Value>>>;

    fn mock_compose(
        &self,
        _stmt: &SqlComposition,
        mock_values: &Vec<BTreeMap<String, Self::Value>>,
        offset: usize,
    ) -> (String, Vec<Self::Value>) {
        let mut sql = String::new();
        let mut values: Vec<Self::Value> = vec![];

        let mut i = offset;
        let mut r = 0;
        let mut c = 0;

        if i == 0 {
            i = 1
        }

        let mut expected_columns: Option<u8> = None;

        if mock_values.is_empty() {
            panic!("mock_values cannot be empty");
        }
        else {
            for row in mock_values.iter() {
                if r > 0 {
                    sql.push_str(" UNION ALL ");
                }

                sql.push_str("SELECT ");

                for (name, value) in row {
                    c += 1;

                    if c > 1 {
                        sql.push_str(", ")
                    }

                    sql.push_str(&self.bind_var_tag(i, name.to_string()));
                    sql.push_str(&format!(" AS {}", &name));

                    values.push(*value);

                    i += 1;
                }

                if let Some(ec) = expected_columns {
                    if c != ec {
                        panic!("expected {} columns found {} for row {}", ec, c, r);
                    }
                }
                else {
                    expected_columns = Some(c);
                }

                r += 1;
                c = 0;
            }
        }

        (sql, values)
    }
}
