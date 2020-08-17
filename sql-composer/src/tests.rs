use crate::types::{ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding, SqlCompositionAlias,
                   SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral};

use std::fmt::Debug;

use std::path::PathBuf;

pub fn build_parsed_item<T: Debug + Default + PartialEq + Clone>(
    item: T,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<T> {
    let fs = fragment.to_string();

    let span = Span {
        line:     line.unwrap_or(1),
        offset:   offset.unwrap_or(0),
        fragment: &fs,
        extra:    (),
    };

    ParsedItem::from_span(item, span)
        .expect("expected Ok from ParsedItem::from_span in build_parsed_time()")
}

#[allow(dead_code)]
pub fn build_parsed_string(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<String> {
    build_parsed_item(item.to_string(), line, offset, fragment)
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

    build_parsed_item(binding, line, offset, fragment)
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
        Sql::Binding(build_parsed_binding_item(
            name, min, max, nullable, line, offset, fragment,
        )),
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

    build_parsed_item(quoted_binding, line, offset, fragment)
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
        Sql::Binding(build_parsed_quoted_binding_item(
            name, min, max, nullable, line, offset, fragment,
        )),
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

    build_parsed_item(literal, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_literal(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    build_parsed_item(
        Sql::Literal(build_parsed_literal_item(item, line, offset, fragment)),
        line,
        offset,
        fragment,
    )
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

    build_parsed_item(object, line, offset, fragment)
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
        Sql::DbObject(build_parsed_db_object_item(
            item, alias, line, offset, fragment,
        )),
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

    build_parsed_item(keyword, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_keyword(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    build_parsed_item(
        Sql::Keyword(build_parsed_keyword_item(item, line, offset, fragment)),
        line,
        offset,
        fragment,
    )
}

pub fn build_parsed_ending_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlEnding> {
    let ending = SqlEnding::new(item.to_string()).unwrap();

    build_parsed_item(ending, line, offset, fragment)
}

#[allow(dead_code)]
pub fn build_parsed_sql_ending(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<Sql> {
    build_parsed_item(
        Sql::Ending(build_parsed_ending_item(item, line, offset, fragment)),
        line,
        offset,
        fragment,
    )
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
