use crate::types::{ParsedItem, ParsedSql, ParsedSqlComposition, ParsedSqlStatement, Sql,
                   SqlBinding, SqlComposition, SqlCompositionAlias, SqlDbObject, SqlEnding,
                   SqlLiteral, SqlStatement};

use std::collections::{BTreeMap, HashMap};

use crate::error::{ErrorKind, Result};

use std::convert::{From, TryFrom};

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

    fn compose(&self, s: &SqlStatement) -> Result<(String, Vec<Self::Value>)> {
        let item = ParsedItem::generated(s.clone(), None)?;

        self.compose_statement(&item, 1usize, false)
    }

    fn compose_statement(
        &self,
        stmt_item: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        let mut i = offset;

        let mut sql = String::new();

        let mut values: Vec<Self::Value> = vec![];

        let mut pad;
        let mut skip_padding;

        for c in &stmt_item.item.sql {
            pad = true;
            skip_padding = false;

            let (sub_sql, sub_values) = match &c.item {
                Sql::Literal(t) => (t.to_string(), vec![]),
                Sql::Binding(b) => self.compose_binding(b.clone(), i)?,
                Sql::Composition((ss, _aliases)) => {
                    let out = ParsedItem::new(
                        SqlStatement {
                            sql:      vec![ParsedItem::new(
                                Sql::Composition((ss.clone(), vec![])),
                                Some(c.position.clone()),
                            )],
                            complete: true,
                        },
                        Some(c.position.clone()),
                    );

                    //could this just take a position?
                    self.compose_statement(&out, i, true)?
                }
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
                    let dbo_alias = SqlCompositionAlias::DbObject(SqlDbObject::new(
                        dbo.object_name.to_string(),
                        None,
                    )?);

                    if let Some(mv) = self.mock_values().get(&dbo_alias) {
                        let (mock_sql, mock_values) = self.mock_compose(mv, i)?;

                        //TODO: this should call the alias function on dbo_alias, which uses
                        //object_alias but falls back to object_name
                        let mock_sql = format!("( {} ) AS {}", mock_sql, dbo.object_name);

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
                skip_padding = true;
            }

            if !skip_padding && pad && sql.len() > 0 {
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
        stmt_item: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        let composition = ParsedSqlComposition::try_from(stmt_item.clone())?;

        match &composition.item.command {
            Some(s) => match s.item().to_lowercase().as_str() {
                "compose" => {
                    let mut out = composition.clone();

                    out.item.command = None;

                    match &out.item.of[0].item().path() {
                        Some(path) => match self
                            .mock_values()
                            .get(&SqlCompositionAlias::Path(path.into()))
                        {
                            Some(e) => Ok(self.mock_compose(e, offset)?),
                            None => self.compose_statement(
                                &out.item
                                    .aliases
                                    .get(&out.item.of[0].item())
                                    .expect("get called but none found at that position")
                                    .clone()
                                    .into(),
                                offset,
                                child,
                            ),
                        },
                        None => self.compose_statement(
                            &out.item
                                .aliases
                                .get(&out.item.of[0].item())
                                .expect("get called but none found at that position")
                                .clone()
                                .into(),
                            offset,
                            child,
                        ),
                    }
                }
                "count" => self.compose_count_command(&composition.into(), offset, child),
                "union" => self.compose_union_command(&composition.into(), offset, child),
                c @ _ => bail!(ErrorKind::CompositionCommandUnknown(c.into())),
            },
            None => self.compose_statement(&composition.into(), offset, child),
        }
    }

    fn compose_count_command(
        &self,
        composition: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)>;

    fn compose_count_default_command(
        &self,
        stmt_item: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        let mut out = SqlStatement::default();

        let composition = ParsedSqlComposition::try_from(stmt_item.clone())?;

        let mut select = String::from("SELECT COUNT(");

        let columns = composition.item.column_list()?;

        if let Some(c) = columns {
            select.push_str(&c);
        }
        else {
            select.push('1');
        }

        select.push_str(") FROM ");

        out.push_sql(ParsedItem::generated(
            SqlLiteral::new(select)?.into(),
            Some("COUNT".into()),
        )?)?;

        for alias in composition.item.of.iter() {
            out.push_sql(ParsedItem::generated(
                SqlLiteral::new("(".into())?.into(),
                Some("COUNT".into()),
            )?)?;

            match composition.item.aliases.get(&alias.item()) {
                Some(sc) => {
                    out.push_sql(ParsedSql::from(sc.clone()))?;
                }
                None => {
                    bail!(ErrorKind::CompositionAliasUnknown(alias.to_string()));
                }
            }

            out.push_sql(ParsedItem::generated(
                SqlLiteral::new(") AS count_main".into())?.into(),
                Some("COUNT".into()),
            )?)?;
        }

        out.push_sql(ParsedItem::generated(
            SqlEnding::new(";".into())?.into(),
            Some("COUNT".into()),
        )?)?;

        let item = ParsedItem::generated(out.into(), Some("COUNT".into()))?;

        self.compose_statement(&item, offset, child)
    }

    fn compose_union_command(
        &self,
        composition: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)>;

    fn compose_union_default_command(
        &self,
        stmt_item: &ParsedSqlStatement,
        offset: usize,
        child: bool,
    ) -> Result<(String, Vec<Self::Value>)> {
        let mut out = SqlStatement::default();

        let composition = ParsedSqlComposition::try_from(stmt_item.clone())?;

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
                //out.push_generated_literal("UNION ", Some("UNION".into()))?;
                out.push_sql(ParsedItem::generated(
                    SqlLiteral::new("UNION ".into())?.into(),
                    Some("UNION".into()),
                )?)?;
            }

            match composition.item.aliases.get(&alias.item()) {
                Some(sc) => out.sql.push(sc.clone().into()),
                None => {
                    bail!(ErrorKind::CompositionAliasUnknown(alias.to_string()));
                }
            }

            i += 1;
        }

        out.push_sql(ParsedItem::generated(
            SqlEnding::new(";".into())?.into(),
            Some("UNION".into()),
        )?)?;

        let item = ParsedItem::generated(out, Some("UNION".into()))?;

        self.compose_statement(&item, offset, child)
    }

    fn compose_binding(
        &self,
        binding: SqlBinding,
        offset: usize,
    ) -> Result<(String, Vec<Self::Value>)> {
        let name = &binding.name;
        let mut sql = String::new();
        let mut new_values = vec![];

        match self.get_values(name.to_string()) {
            Some(v) => {
                let mut found = 0;

                for iv in v.iter() {
                    if new_values.len() > 0 {
                        sql.push_str(", ");
                    }

                    sql.push_str(&self.binding_tag(new_values.len() + offset, name.to_string())?);

                    new_values.push(*iv);

                    found += 1;
                }

                if found == 0 {
                    if binding.nullable {
                        sql.push_str("NULL");

                        return Ok((sql, new_values));
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

        Ok((sql, new_values))
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
    ) -> Result<(String, Vec<Self::Value>)> {
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
            bail!(ErrorKind::MockCompositionArgsInvalid(
                "mock_values cannot be empty".into()
            ));
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

                    sql.push_str(&self.binding_tag(i, name.to_string())?);
                    sql.push_str(&format!(" AS {}", &name));

                    values.push(*value);

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

        Ok((sql, values))
    }
}
