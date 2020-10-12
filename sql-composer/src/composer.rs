use crate::types::{GeneratedSpan, ParsedItem, ParsedSqlMacro, ParsedSqlStatement, Position, Sql,
                   SqlBinding, SqlComposition, SqlCompositionAlias, SqlDbObject};

use std::collections::{BTreeMap, HashMap};

use crate::error::{Error, ErrorKind, Result};

use std::convert::TryInto;

pub trait ComposerConnection<'a> {
    type Composer;
    //TODO: this should be Composer::Value but can't be specified as Self::Value::Connection
    type Value;
    type Statement;

    fn compose(
        &'a self,
        ipss: impl TryInto<ParsedSqlStatement, Error = Error>,
        values: BTreeMap<String, Vec<Self::Value>>,
        root_mock_values: Vec<BTreeMap<String, Self::Value>>,
        mock_values: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>,
    ) -> Result<(Self::Statement, Vec<Self::Value>)>;
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
                    id: None,
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
    #[allow(dead_code)]
    pub start: usize,
}

pub trait ComposerTrait: Sized {
    type Value: Copy;

    fn compose(
        &self,
        stmt_item: impl TryInto<ParsedSqlStatement, Error = Error>,
    ) -> Result<SqlComposition<Self::Value>> {
        let p: ParsedSqlStatement = stmt_item.try_into()?;
        self.compose_statement(&p, 1usize, None)
    }

    fn compose_statement(
        &self,
        stmt_item: &ParsedSqlStatement,
        offset: usize,
        parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>> {
        let mut i = offset;

        let mut sc = SqlComposition::new();

        let mut pad;
        let mut skip_padding;

        for c in &stmt_item.item.sql {
            pad = true;
            skip_padding = false;

            let mut sub_sc = SqlComposition::new();

            match &c.item {
                Sql::Literal(t) => sub_sc.push_sql(&t.to_string()),
                Sql::Binding(b) => sub_sc.append(self.compose_binding(b.clone(), i)?),
                Sql::Macro(sm) => {
                    let out = ParsedItem::new(sm.clone(), Some(c.position.clone()));

                    //could this just take a position?
                    sub_sc.append(self.compose_command(&out, i, Some(c.position.clone()))?)
                }
                Sql::Ending(e) => {
                    pad = false;

                    if parent.is_some() {
                        sub_sc.push_sql("");
                    }
                    else {
                        sub_sc.push_sql(&e.to_string());
                    }
                }
                Sql::DbObject(dbo) => {
                    let dbo_alias = SqlCompositionAlias::DbObject(SqlDbObject::new(
                        dbo.object_name.to_string(),
                        None,
                    )?);

                    if let Some(mv) = self.mock_values().get(&dbo_alias) {
                        let mock_sc = self.mock_compose(mv, i)?;

                        //TODO: this should call the alias function on dbo_alias, which uses
                        //object_alias but falls back to object_name
                        let mock_sql = format!("( {} ) AS {}", mock_sc.sql(), dbo.object_name);

                        sub_sc.push(&mock_sql, mock_sc.values().to_vec());
                    }
                    else {
                        sub_sc.push_sql(&dbo.to_string());
                    }
                }
                Sql::Keyword(k) => sub_sc.push_sql(&k.to_string()),
            };

            if sub_sc.sql_len() == 0 {
                continue;
            }

            if &sub_sc.sql() == "," {
                skip_padding = true;
            }

            if !skip_padding && pad && sc.sql_len() > 0 {
                sc.push_sql(" ");
            }

            sc.append(sub_sc);

            i = sc.values_len() + offset;
        }

        Ok(sc)
    }

    fn compose_command<'c>(
        &self,
        composition: &ParsedSqlMacro,
        offset: usize,
        parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>> {
        match composition.item.command.item.as_str() {
            "compose" => {
                if let Some(e) = self.mock_values().get(&composition.item.of[0].item()) {
                    return Ok(self.mock_compose(e, offset)?);
                }
                else {
                    let alias = composition.item.of[0].item();
                    let stmt: ParsedSqlStatement = alias.try_into()?;

                    return self.compose_statement(
                        //TODO: find a way around unwrapping here, the match seems to make things weird
                        &stmt, offset, parent,
                    );
                }
            }
            "count" => self.compose_count_command(composition, offset, parent),
            "union" => self.compose_union_command(composition, offset, parent),
            c @ _ => bail!(ErrorKind::CompositionCommandUnknown(c.into())),
        }
    }

    fn compose_count_command(
        &self,
        composition: &ParsedSqlMacro,
        offset: usize,
        parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>>;

    fn compose_count_default_command(
        &self,
        psm: &ParsedSqlMacro,
        offset: usize,
        _parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>> {
        let mut sc = SqlComposition::new();

        sc.push_sql("SELECT COUNT(");

        let columns = psm.item.column_list()?;

        if let Some(c) = columns {
            sc.push_sql(&c);
        }
        else {
            sc.push_sql("1");
        }

        sc.push_sql(") FROM ");

        for alias in psm.item.of.iter() {
            sc.push_sql("(");

            let stmt = alias.item.clone().try_into()?;
            let asc = self.compose_statement(
                &stmt,
                offset,
                Some(Position::Generated(GeneratedSpan {
                    command: Some("count".to_string()),
                })),
            )?;

            sc.append(asc);

            sc.push_sql(") AS count_main");
        }

        Ok(sc)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedSqlMacro,
        offset: usize,
        parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>>;

    fn compose_union_default_command(
        &self,
        composition: &ParsedSqlMacro,
        _offset: usize,
        _parent: Option<Position>,
    ) -> Result<SqlComposition<Self::Value>> {
        let mut sc = SqlComposition::new();

        // columns in this case would mean an compose on each side of the union literal
        let _columns = composition.item.column_list()?;

        let mut i = 0usize;

        if composition.item.of.len() < 2 {
            bail!(ErrorKind::CompositionCommandArgInvalid(
                "union".into(),
                "requires 2 or more alias names".into()
            ));
        }

        for alias in composition.item.of.iter() {
            if i > 0 {
                sc.push_sql(" UNION ");
            }

            let stmt = alias.item.clone().try_into()?;
            let asc = self.compose_statement(
                &stmt,
                sc.values_len() + 1,
                Some(Position::Generated(GeneratedSpan {
                    command: Some("count".to_string()),
                })),
            )?;

            sc.append(asc);

            i += 1;
        }

        Ok(sc)
    }

    fn compose_binding(
        &self,
        binding: SqlBinding,
        offset: usize,
    ) -> Result<SqlComposition<Self::Value>> {
        let name = &binding.name;

        let mut sc = SqlComposition::new();

        match self.get_values(name.to_string()) {
            Some(v) => {
                let mut found = 0;

                for iv in v.iter() {
                    if sc.values_len() > 0 {
                        sc.push_sql(", ");
                    }

                    sc.push_sql(&self.binding_tag(sc.values_len() + offset, name.to_string())?);
                    sc.push_value(*iv);

                    found += 1;
                }

                if found == 0 {
                    if binding.nullable {
                        sc.push_sql("NULL");

                        return Ok(sc);
                    }
                    else {
                        bail!(ErrorKind::CompositionBindingValueInvalid(
                            name.into(),
                            "cannot be NULL and no value provided".into()
                        ));
                    }
                }

                if let Some(min) = binding.min_values {
                    if found < min {
                        bail!(ErrorKind::CompositionBindingValueCount(
                            name.into(),
                            format!("found {} > min {}", found, min)
                        ));
                    }
                }

                if let Some(max) = binding.max_values {
                    if found > max {
                        bail!(ErrorKind::CompositionBindingValueCount(
                            name.into(),
                            format!("found {} < max {}", found, max)
                        ));
                    }
                }
                else {
                    if binding.min_values.is_none() && found > 1 {
                        bail!(ErrorKind::CompositionBindingValueCount(
                            name.into(),
                            "does not accept more than one value".into()
                        ));
                    }
                }
            }
            None => bail!(ErrorKind::CompositionBindingValueCount(
                name.into(),
                "requires a value".into()
            )),
        };

        Ok(sc)
    }

    fn binding_tag(&self, u: usize, name: String) -> Result<String>;

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>>;

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> ();

    fn config() -> ComposerConfig;

    fn root_mock_values(&self) -> &Vec<BTreeMap<String, Self::Value>>;

    fn mock_values(&self) -> &HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Self::Value>>>;

    fn mock_compose(
        &self,
        mock_values: &Vec<BTreeMap<String, Self::Value>>,
        offset: usize,
    ) -> Result<SqlComposition<Self::Value>> {
        let mut sc = SqlComposition::new();

        let mut i = offset;
        let mut r = 0;
        let mut c = 0;

        if i == 0 {
            i = 1
        }

        let mut expected_columns: Option<u8> = None;

        if mock_values.is_empty() {
            bail!(ErrorKind::MockCompositionArgsInvalid(
                "mock_values cannot be empty".into()
            ));
        }
        else {
            for row in mock_values.iter() {
                if r > 0 {
                    sc.push_sql(" UNION ALL ");
                }

                sc.push_sql("SELECT ");

                for (name, value) in row {
                    c += 1;

                    if c > 1 {
                        sc.push_sql(", ")
                    }

                    sc.push_sql(&self.binding_tag(i, name.to_string())?);
                    sc.push_sql(&format!(" AS {}", &name));

                    sc.push_value(*value);

                    i += 1;
                }

                if let Some(ec) = expected_columns {
                    if c != ec {
                        bail!(ErrorKind::MockCompositionColumnCountInvalid(r, c, ec));
                    }
                }
                else {
                    expected_columns = Some(c);
                }

                r += 1;
                c = 0;
            }
        }

        Ok(sc)
    }
}
