pub mod direct;

#[cfg(feature = "dbd-mysql")]
pub mod mysql;
#[cfg(feature = "dbd-postgres")]
pub mod postgres;
#[cfg(feature = "dbd-rusqlite")]
pub mod rusqlite;

#[cfg(feature = "composer-serde")]
pub use crate::parser::bind_value_named_set;
pub use crate::parser::parse_template;

use crate::types::{ParsedItem, Sql, SqlBinding, SqlComposition, SqlCompositionAlias, SqlDbObject};
use std::collections::{BTreeMap, HashMap};

pub trait ComposerConnection<'a> {
    type Composer;
    //TODO: this should be Composer::Value but can't be specified as Self::Value::Connection
    type Value;
    type Statement;

    fn compose(
        &'a self,
        s: &SqlComposition,
        values: BTreeMap<String, Vec<Self::Value>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>), ()>;
}

#[macro_export]
macro_rules! mock_values(
    ($to_type:ty: $({$($key:literal => $value:expr), +}), +) => {
        {
            let mut mv = vec![];

            $(
                let mut m = ::std::collections::BTreeMap::new();

                $(
                    m.insert($key.to_string(), $value as $to_type);
                )+

                mv.push(m);
            )+

            mv
        }
     };
);

#[macro_export]
macro_rules! mock_path_values(
    ($to_type:ty: $($path:expr => [$({$($key:expr => $value:expr), +}), +]); +) => {
        {
            let mut mocks = HashMap::new();

            $(
                    let mut mv = vec![];

                    $(
                        let mut m = ::std::collections::BTreeMap::new();

                        $(
                            m.insert($key.to_string(), $value as $to_type);
                        )+

                        mv.push(m);
                    )+

                mocks.insert(SqlCompositionAlias::Path($path.into()), mv);
            )+

            mocks
        }
     };
);

#[macro_export]
macro_rules! mock_db_object_values(
    ($to_type:ty: $($object:expr => [$({$($key:expr => $value:expr), +}), +]); +) => {
        {
            let mut mocks = HashMap::new();

            $(
                    let mut mv = vec![];

                    $(
                        let mut m = ::std::collections::BTreeMap::new();

                        $(
                            m.insert($key.to_string(), $value as $to_type);
                        )+

                        mv.push(m);
                    )+

                mocks.insert(SqlCompositionAlias::DbObject(SqlDbObject {
                    object_name:  "main".into(),
                    object_alias: None,
                }), mv);
            )+

            mocks
        }
     };
);

#[macro_export]
macro_rules! bind_values(
    ($to_type:ty: $( $key:literal => [$($value:expr), +]), +)  => {
        {
            let mut m = ::std::collections::BTreeMap::new();

            $(
                let mut mv = vec![];

                    $(
                        mv.push($value as $to_type);
                    )+

                    m.insert($key.to_string(), mv);
            )+

            m
        }
     };
);

#[derive(Default)]
pub struct ComposerConfig {
    start: usize,
}

pub trait Composer: Sized {
    type Value: Copy;

    fn compose(&self, s: &SqlComposition) -> Result<(String, Vec<Self::Value>), ()> {
        let item = ParsedItem::generated(s.clone(), None).unwrap();

        self.compose_statement(&item, 1usize, false)
    }

    fn compose_statement(
        &self,
        sc: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        let mut i = offset;

        let mut sql = String::new();

        let mut values: Vec<Self::Value> = vec![];

        if sc.item.command.is_some() {
            return self.compose_command(&sc, i, true);
        }

        let mut pad = true;
        let mut skip_this = false;
        let mut skip_next = false;

        for c in &sc.item.sql {
            pad = true;
            skip_this = skip_next;
            skip_next = false;

            let (sub_sql, sub_values) = match c {
                Sql::Literal(t) => (t.to_string(), vec![]),
                Sql::Binding(b) => self.compose_binding(b.item.clone(), i)?,
                Sql::Composition((ss, _aliases)) => self.compose_statement(&ss, i, true)?,
                Sql::Ending(e) => {
                    pad = false;

                    if child {
                        ("".to_string(), vec![])
                    }
                    else {
                        (e.to_string(), vec![])
                    }
                }
                Sql::DbObject(dbo) => {
                    let dbo_alias = SqlCompositionAlias::DbObject(
                        SqlDbObject::new(dbo.item.object_name.to_string(), None).unwrap(),
                    );

                    if let Some(mv) = self.mock_values().get(&dbo_alias) {
                        let (mock_sql, mock_values) = self.mock_compose(mv, i);

                        //TODO: this should call the alias function on dbo_alias, which uses
                        //object_alias but falls back to object_name
                        let mock_sql = format!("( {} ) AS {}", mock_sql, dbo.item.object_name);

                        (mock_sql, mock_values)
                    }
                    else {
                        (dbo.to_string(), vec![])
                    }
                }
                Sql::Keyword(k) => (k.to_string(), vec![]),
            };

            if sub_sql.len() == 0 {
                continue;
            }

            if sub_sql == "," {
                skip_this = true;
            }

            if !skip_this && pad && sql.len() > 0 {
                sql.push(' ');
            }

            sql.push_str(&sub_sql);

            for sv in sub_values {
                values.push(sv);
            }

            i = values.len() + offset;
        }

        Ok((sql, values))
    }

    fn compose_command<'c>(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        match &composition.item.command {
            Some(s) => {
                match s.item().to_lowercase().as_str() {
                    "compose" => {
                        let mut out = composition.clone();

                        out.item.command = None;

                        match &out.item.of[0].item().path() {
                            Some(path) => match self
                                .mock_values()
                                .get(&SqlCompositionAlias::Path(path.into()))
                            {
                                Some(e) => Ok(self.mock_compose(e, offset)),
                                None => self.compose_statement(
                                    &out.item.aliases.get(&out.item.of[0].item()).unwrap(),
                                    offset,
                                    child,
                                ),
                            },
                            None => self.compose_statement(
                                &out.item.aliases.get(&out.item.of[0].item()).unwrap(),
                                offset,
                                child,
                            ),
                        }
                    }
                    "count" => self.compose_count_command(composition, offset, child),
                    "union" => self.compose_union_command(composition, offset, child),
                    // TODO: handle this error better
                    _ => panic!("unknown call"),
                }
            }
            None => self.compose_statement(&composition, offset, child),
        }
    }

    fn compose_count_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()>;

    fn compose_count_default_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        let mut out = SqlComposition::default();

        let mut select = String::from("SELECT COUNT(");

        let columns = composition.item.column_list().unwrap();

        if let Some(c) = columns {
            select.push_str(&c);
        }
        else {
            select.push('1');
        }

        select.push_str(") FROM ");

        out.push_generated_literal(&select, Some("COUNT".into()))
            .unwrap();

        for position in composition.item.of.iter() {
            out.push_generated_literal("(", Some("COUNT".into()))
                .unwrap();
            match composition.item.aliases.get(&position.item()) {
                Some(sc) => {
                    out.push_sub_comp(sc.clone()).unwrap();
                }
                None => {
                    panic!("no position found with position: {:?}", position);
                }
            }

            out.push_generated_literal(") AS count_main", Some("COUNT".into()))
                .unwrap();
        }

        out.push_generated_end(Some("COUNT".into())).unwrap();

        let item = ParsedItem::generated(out, Some("COUNT".into())).unwrap();

        self.compose_statement(&item, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()>;

    fn compose_union_default_command(
        &self,
        composition: &ParsedItem<SqlComposition>,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>), ()> {
        let mut out = SqlComposition::default();

        // columns in this case would mean an compose on each side of the union literal
        let _columns = composition.item.column_list().unwrap();

        let mut i = 0usize;

        if composition.item.of.len() < 2 {
            panic!("union requires 2 of arguments");
        }

        for position in composition.item.of.iter() {
            if i > 0 {
                out.push_generated_literal("UNION ", Some("UNION".into()))
                    .unwrap();
            }

            match composition.item.aliases.get(&position.item()) {
                Some(sc) => {
                    out.push_sub_comp(sc.clone()).unwrap();
                }
                None => {
                    panic!("no alias found with alias: {:?}", position.item());
                }
            }

            i += 1;
        }

        out.push_generated_end(Some("UNION".into())).unwrap();

        let item = ParsedItem::generated(out, Some("UNION".into())).unwrap();

        self.compose_statement(&item, offset, child)
    }

    fn compose_binding(&self, binding: SqlBinding, offset: usize) -> Result<(String, Vec<Self::Value>), ()> {
        let name = &binding.name;
        let mut sql = String::new();
        let mut new_values = vec![];

        let i = offset;

        match self.get_values(name.to_string()) {
            Some(v) => {
                let mut found = 0;

                for iv in v.iter() {
                    if new_values.len() > 0 {
                        sql.push_str(", ");
                    }

                    sql.push_str(&self.binding_tag(new_values.len() + offset, name.to_string()));

                    new_values.push(*iv);

                    found += 1;
                }

                if found == 0 {
                    if binding.nullable {
                      sql.push_str("NULL");

                      return Ok((sql, new_values));
                    }
                    else {
                        return Err(());
                    }
                }

                if let Some(min) = binding.min_values {
                    if found < min {
                        //TODO: useful error
                        return Err(());

                    }
                }

                if let Some(max) = binding.max_values {
                    if found > max {
                        //TODO: useful error
                        return Err(());

                    }
                }
                else {
                    if binding.min_values.is_none() && found > 1 {
                        //TODO: useful error
                        return Err(());
                    }

                }

            }
            //TODO: error "no value for binding {} of {}", i, name),
            //only error that isn't
            None => panic!("no value for binding {} of {}", i, name),
        };

        Ok((sql, new_values))
    }

    fn binding_tag(&self, u: usize, name: String) -> String;

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>>;

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> ();

    fn config() -> ComposerConfig;

    fn root_mock_values(&self) -> &Vec<BTreeMap<String, Self::Value>>;

    fn mock_values(&self) -> &HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>;

    fn mock_compose(
        &self,
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

                    sql.push_str(&self.binding_tag(i, name.to_string()));
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
