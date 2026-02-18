//! Parser for `:count(...)` and `:union(...)` command macros.

use std::path::PathBuf;

use winnow::combinator::{alt, opt, separated, trace};
use winnow::error::ParserError;
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::{literal, take_while};
use winnow::Parser;

use crate::types::{Command, CommandKind};

/// Parse optional whitespace within command parentheses.
fn ws<'i, Input, Error>(input: &mut Input) -> Result<(), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    take_while(0.., |c: <Input as Stream>::Token| {
        let ch = c.as_char();
        ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
    })
    .void()
    .parse_next(input)
}

/// Parse the DISTINCT keyword.
fn distinct<'i, Input, Error>(input: &mut Input) -> Result<bool, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("distinct", move |input: &mut Input| {
        literal("DISTINCT").parse_next(input)?;
        ws(input)?;
        Ok(true)
    })
    .parse_next(input)
}

/// Parse the ALL keyword.
fn all_kw<'i, Input, Error>(input: &mut Input) -> Result<bool, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("all", move |input: &mut Input| {
        literal("ALL").parse_next(input)?;
        ws(input)?;
        Ok(true)
    })
    .parse_next(input)
}

/// Parse a column name: alphanumeric + underscore + dot.
fn column_name<'i, Input, Error>(input: &mut Input) -> Result<String, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    let name = take_while(1.., |c: <Input as Stream>::Token| {
        let ch = c.as_char();
        ch.is_alphanumeric() || ch == '_' || ch == '.'
    })
    .parse_next(input)?;
    let name = String::from_utf8_lossy(name.as_bstr()).to_string();
    Ok(name)
}

/// Parse a comma separator with optional surrounding whitespace.
fn comma_sep<'i, Input, Error>(input: &mut Input) -> Result<(), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    ws(input)?;
    literal(",").parse_next(input)?;
    ws(input)?;
    Ok(())
}

/// Parse a column list followed by `OF`: `col1, col2, col3 OF`.
fn columns_of<'i, Input, Error>(input: &mut Input) -> Result<Vec<String>, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("columns_of", move |input: &mut Input| {
        let cols: Vec<String> = separated(1.., column_name, comma_sep).parse_next(input)?;
        ws(input)?;
        literal("OF").parse_next(input)?;
        ws(input)?;
        Ok(cols)
    })
    .parse_next(input)
}

/// Parse a source path: one or more non-whitespace, non-`)`, non-`,` characters.
fn source_path<'i, Input, Error>(input: &mut Input) -> Result<PathBuf, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    let path_str = take_while(1.., |c: <Input as Stream>::Token| {
        let ch = c.as_char();
        ch != ')' && ch != ',' && ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r'
    })
    .parse_next(input)?;
    let path_str = String::from_utf8_lossy(path_str.as_bstr()).to_string();
    Ok(PathBuf::from(path_str))
}

/// Parse the command kind from the prefix keyword.
///
/// This parses `count(` or `union(` and returns the command kind.
pub fn command_kind<'i, Input, Error>(input: &mut Input) -> Result<CommandKind, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("command_kind", move |input: &mut Input| {
        alt((
            literal("count(").map(|_| CommandKind::Count),
            literal("union(").map(|_| CommandKind::Union),
        ))
        .parse_next(input)
    })
    .parse_next(input)
}

/// Parse the body of a command after `count(` or `union(` has been consumed.
///
/// Grammar: `[DISTINCT] [ALL] [columns OF] source1[, source2, ...] )`
pub fn command_body<'i, Input, Error>(
    input: &mut Input,
    kind: CommandKind,
) -> Result<Command, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("command_body", move |input: &mut Input| {
        ws(input)?;
        let is_distinct = opt(distinct).parse_next(input)?.unwrap_or(false);
        let is_all = opt(all_kw).parse_next(input)?.unwrap_or(false);
        let columns = opt(columns_of).parse_next(input)?;
        let sources: Vec<PathBuf> = separated(1.., source_path, comma_sep).parse_next(input)?;
        ws(input)?;
        literal(")").parse_next(input)?;

        Ok(Command {
            kind,
            distinct: is_distinct,
            all: is_all,
            columns,
            sources,
        })
    })
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::error::ContextError;

    type TestInput<'a> = &'a str;

    #[test]
    fn test_command_kind_count() {
        let mut input: TestInput = "count(";
        let result = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        assert_eq!(result, CommandKind::Count);
    }

    #[test]
    fn test_command_kind_union() {
        let mut input: TestInput = "union(";
        let result = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        assert_eq!(result, CommandKind::Union);
    }

    #[test]
    fn test_command_simple_count() {
        let mut input: TestInput = "count(templates/get_user.tql)";
        let kind = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        let result = command_body::<_, ContextError>(&mut input, kind).unwrap();
        assert_eq!(result.kind, CommandKind::Count);
        assert!(!result.distinct);
        assert!(!result.all);
        assert_eq!(result.columns, None);
        assert_eq!(result.sources, vec![PathBuf::from("templates/get_user.tql")]);
    }

    #[test]
    fn test_command_union_multiple_sources() {
        let mut input: TestInput = "union(a.tql, b.tql, c.tql)";
        let kind = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        let result = command_body::<_, ContextError>(&mut input, kind).unwrap();
        assert_eq!(result.kind, CommandKind::Union);
        assert_eq!(
            result.sources,
            vec![
                PathBuf::from("a.tql"),
                PathBuf::from("b.tql"),
                PathBuf::from("c.tql")
            ]
        );
    }

    #[test]
    fn test_command_with_distinct() {
        let mut input: TestInput = "union(DISTINCT a.tql, b.tql)";
        let kind = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        let result = command_body::<_, ContextError>(&mut input, kind).unwrap();
        assert!(result.distinct);
        assert!(!result.all);
    }

    #[test]
    fn test_command_with_all() {
        let mut input: TestInput = "union(ALL a.tql, b.tql)";
        let kind = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        let result = command_body::<_, ContextError>(&mut input, kind).unwrap();
        assert!(!result.distinct);
        assert!(result.all);
    }

    #[test]
    fn test_command_with_columns() {
        let mut input: TestInput = "count(id, name OF templates/get_user.tql)";
        let kind = command_kind::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        let result = command_body::<_, ContextError>(&mut input, kind).unwrap();
        assert_eq!(
            result.columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
        assert_eq!(result.sources, vec![PathBuf::from("templates/get_user.tql")]);
    }
}
