use crate::types::{CompleteStr, ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding,
                   SqlCompositionAlias, SqlEnding, SqlLiteral};

use std::fmt;
use std::fmt::Debug;

use std::path::{Path, PathBuf};

pub fn build_parsed_item<T: Debug + Default + PartialEq + Clone>(
    item: T,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<T> {
    let fs = fragment.to_string();

    let span = Span {
        offset:   offset.unwrap_or(0),
        line:     line.unwrap_or(1),
        fragment: CompleteStr(&fs),
    };

    ParsedItem::from_span(item, span, None)
        .expect("expected Ok from ParsedItem::from_span in build_parsed_time()")
}

pub fn build_parsed_string(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<String> {
    build_parsed_item(item.to_string(), line, offset, fragment)
}

pub fn build_parsed_binding_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlBinding> {
    let binding = SqlBinding::new(item.to_string()).unwrap();

    build_parsed_item(binding, line, offset, fragment)
}

pub fn build_parsed_sql_binding(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> Sql {
    Sql::Binding(build_parsed_binding_item(item, line, offset, fragment))
}

pub fn build_parsed_quoted_binding_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlBinding> {
    let quoted_binding = SqlBinding::new_quoted(item.to_string()).unwrap();

    build_parsed_item(quoted_binding, line, offset, fragment)
}

pub fn build_parsed_sql_quoted_binding(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> Sql {
    Sql::Binding(build_parsed_quoted_binding_item(
        item, line, offset, fragment,
    ))
}

pub fn build_parsed_literal_item(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> ParsedItem<SqlLiteral> {
    let literal = SqlLiteral::new(item.to_string()).unwrap();

    build_parsed_item(literal, line, offset, fragment)
}

pub fn build_parsed_sql_literal(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> Sql {
    Sql::Literal(build_parsed_literal_item(item, line, offset, fragment))
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

pub fn build_parsed_sql_ending(
    item: &str,
    line: Option<u32>,
    offset: Option<usize>,
    fragment: &str,
) -> Sql {
    Sql::Ending(build_parsed_ending_item(item, line, offset, fragment))
}

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

pub fn build_span(offset: Option<usize>, line: Option<u32>, fragment: &str) -> Span {
    Span {
        offset:   offset.unwrap(),
        line:     line.unwrap(),
        fragment: CompleteStr(fragment),
    }
}
