use crate::types::{ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding, SqlCompositionAlias,
                   SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral};

use std::fmt::Debug;

use std::path::PathBuf;

pub fn build_parsed_item<T: Debug + Default + PartialEq + Clone>(
    item: T,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<T> {
    let ps = Position::Parsed(ParsedSpan {
        alias,
        start: start.into(),
        end: end.into(),
    });

    ParsedItem {
        item,
        position: ps.into(),
    }
}

#[allow(dead_code)]
pub fn build_parsed_string(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<String> {
    build_parsed_item(item.to_string(), alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_binding_item(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlBinding> {
    let binding = SqlBinding::new(name.to_string(), false, min, max, nullable).unwrap();

    build_parsed_item(binding, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_sql_binding(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    build_parsed_item(
        SqlBinding {
            name: name.to_string(),
            min_values: min,
            max_values: max,
            nullable,
            quoted: false,
        }
        .into(),
        alias,
        start,
        end,
    )
}

#[allow(dead_code)]
pub fn build_parsed_quoted_binding_item(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlBinding> {
    let quoted_binding = SqlBinding::new(name.to_string(), true, min, max, nullable).unwrap();

    build_parsed_item(quoted_binding, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_sql_quoted_binding(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    build_parsed_item(
        SqlBinding {
            name: name.to_string(),
            min_values: min,
            max_values: max,
            nullable,
            quoted: true,
        }
        .into(),
        alias,
        start,
        end,
    )
}

#[allow(dead_code)]
pub fn build_parsed_literal_item(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlLiteral> {
    let literal = SqlLiteral::new(item.to_string()).unwrap();

    build_parsed_item(literal, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_sql_literal(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    let literal = SqlLiteral::new(item.to_string()).unwrap();

    build_parsed_item(literal.into(), alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_db_object_item(
    item: &str,
    table_alias: Option<String>,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlDbObject> {
    let object = SqlDbObject::new(item.to_string(), table_alias).unwrap();

    build_parsed_item(object, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_db_object(
    item: &str,
    table_alias: Option<String>,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    build_parsed_item(
        SqlDbObject::new(item.to_string(), table_alias)
            .unwrap()
            .into(),
        alias,
        start,
        end,
    )
}

#[allow(dead_code)]
pub fn build_parsed_keyword_item(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlKeyword> {
    let keyword = SqlKeyword::new(item.to_string()).unwrap();

    build_parsed_item(keyword, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_sql_keyword(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    let keyword = SqlKeyword::new(item.to_string()).unwrap();

    build_parsed_item(keyword.into(), alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_ending_item(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<SqlEnding> {
    let ending = SqlEnding::new(item.to_string()).unwrap();

    build_parsed_item(ending, alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_sql_ending(
    item: &str,
    alias: Option<SqlCompositionAlias>,
    start: (u32, usize),
    end: (u32, usize),
) -> ParsedItem<Sql> {
    let ending = SqlEnding::new(item.to_string()).unwrap();

    build_parsed_item(ending.into(), alias, start, end)
}

#[allow(dead_code)]
pub fn build_parsed_path_position(
    path: PathBuf,
    start: (u32, usize),
    end: (u32, usize),
) -> Position {
    let span = ParsedSpan {
        alias: Some(path.into()),
        start: start.into(),
        end:   end.into(),
    };

    Position::Parsed(span)
}

#[allow(dead_code)]
pub fn build_span(line: Option<u32>, offset: Option<usize>, fragment: &str) -> Span {
    Span {
        line: line.unwrap_or(1),
        offset: offset.unwrap_or(0),
        fragment,
        extra: (),
    }
}
