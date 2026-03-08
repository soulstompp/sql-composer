//! Parser for `:compose(path)` macros.

use std::path::PathBuf;

use winnow::combinator::trace;
use winnow::error::ParserError;
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::{literal, take_while};
use winnow::Parser;

use crate::types::ComposeRef;

/// Parse a file path inside a compose macro: one or more non-`)` characters.
pub fn compose_path<'i, Input, Error>(input: &mut Input) -> Result<PathBuf, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("compose_path", move |input: &mut Input| {
        let path_str = take_while(1.., |c: <Input as Stream>::Token| {
            let ch = c.as_char();
            ch != ')' && ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r'
        })
        .parse_next(input)?;
        let path_str = String::from_utf8_lossy(path_str.as_bstr()).to_string();
        Ok(PathBuf::from(path_str))
    })
    .parse_next(input)
}

/// Parse a complete `:compose(path)` macro.
///
/// Assumes the `:compose(` prefix has already been consumed. Parses the path
/// and the closing `)`.
pub fn compose<'i, Input, Error>(input: &mut Input) -> Result<ComposeRef, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("compose", move |input: &mut Input| {
        let path = compose_path(input)?;
        literal(")").parse_next(input)?;
        Ok(ComposeRef { path })
    })
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::error::ContextError;

    type TestInput<'a> = &'a str;

    #[test]
    fn test_compose_simple() {
        let mut input: TestInput = "templates/get_user.tql)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.path, PathBuf::from("templates/get_user.tql"));
        assert_eq!(input, "");
    }

    #[test]
    fn test_compose_relative_path() {
        let mut input: TestInput = "src/tests/simple-template.tql)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.path, PathBuf::from("src/tests/simple-template.tql"));
    }

    #[test]
    fn test_compose_with_trailing() {
        let mut input: TestInput = "get_user.tql) AND active";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.path, PathBuf::from("get_user.tql"));
        assert_eq!(input, " AND active");
    }
}
