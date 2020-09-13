use crate::types::{ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding, SqlCompositionAlias,
                   SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral};

use std::fmt::Debug;

use std::path::PathBuf;

pub fn build_parsed_item<T: Debug + Default + PartialEq + Clone>(
    item: T,
    alias: Option<SqlCompositionAlias>,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<T> {
    let span = Span {
        line:     line.unwrap_or(1),
        offset:   offset.unwrap_or(0),
        fragment,
        extra:    (),
    };

    ParsedItem::from_span(item, span, alias)
        .expect("expected Ok from ParsedItem::from_span in build_parsed_item()")
}

#[allow(dead_code)]
pub fn build_parsed_string(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<String> {
    build_parsed_item(item.to_string(), None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_binding_item(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlBinding> {
    let binding = SqlBinding::new(name.to_string(), false, min, max, nullable).unwrap();

    build_parsed_item(binding, None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_binding(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
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
        None,
        line,
        offset,
        fragment,
    )
}

#[allow(dead_code)]
pub fn build_parsed_quoted_binding_item(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlBinding> {
    let quoted_binding = SqlBinding::new(name.to_string(), true, min, max, nullable).unwrap();

    build_parsed_item(quoted_binding, None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_quoted_binding(
    name: &str,
    min: Option<u32>,
    max: Option<u32>,
    nullable: bool,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
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
        None,
        line,
        offset,
        fragment,
    )
}

#[allow(dead_code)]
pub fn build_parsed_literal_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlLiteral> {
    let literal = SqlLiteral::new(item.to_string()).unwrap();

    build_parsed_item(literal, None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_literal(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    let literal = SqlLiteral::new(item.to_string()).unwrap();

    build_parsed_item(literal.into(),None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_db_object_item(
    item: &str,
    alias: Option<String>,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlDbObject> {
    let object = SqlDbObject::new(item.to_string(), alias).unwrap();

    build_parsed_item(object,None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_db_object(
    item: &str,
    alias: Option<String>,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    build_parsed_item(
        SqlDbObject::new(item.to_string(), alias).unwrap().into(),
        None,
        line,
        offset,
        fragment,
    )
}

#[allow(dead_code)]
pub fn build_parsed_keyword_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlKeyword> {
    let keyword = SqlKeyword::new(item.to_string()).unwrap();

    build_parsed_item(keyword, None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_keyword(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    let keyword = SqlKeyword::new(item.to_string()).unwrap();

    build_parsed_item(keyword.into(), None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_ending_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlEnding> {
    let ending = SqlEnding::new(item.to_string()).unwrap();

    build_parsed_item(ending, None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_ending(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    let ending = SqlEnding::new(item.to_string()).unwrap();

    build_parsed_item(ending.into(), None, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_path_position(
    path: PathBuf,
    line: u32,
    offset: usize,
    fragment: &str,
) -> Position {
    let alias = SqlCompositionAlias::from_path(&path);

    let span = ParsedSpan {
        alias: Some(alias),
        offset,
        line,
        fragment: fragment.to_string(),
    };

    Position::Parsed(span)
}

#[allow(dead_code)]
pub fn build_span(line: Option<u32>, offset: Option<usize>, fragment: &str) -> Span {
    Span {
        line:     line.unwrap_or(1),
        offset:   offset.unwrap_or(0),
        fragment: fragment,
        extra:    (),
    }
}
