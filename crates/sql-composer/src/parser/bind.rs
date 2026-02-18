//! Parser for `:bind(name [EXPECTING min[..max]] [NULL])` macros.

use winnow::combinator::{opt, preceded, trace};
use winnow::error::ParserError;
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::{literal, take_while};
use winnow::Parser;

use crate::types::Binding;

/// Parse a bind parameter name: one or more alphanumeric or underscore characters.
pub fn bind_name<'i, Input, Error>(input: &mut Input) -> Result<String, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("bind_name", move |input: &mut Input| {
        let name = take_while(1.., |c: <Input as Stream>::Token| {
            let ch = c.as_char();
            ch.is_alphanumeric() || ch == '_'
        })
        .parse_next(input)?;
        let name = String::from_utf8_lossy(name.as_bstr()).to_string();
        Ok(name)
    })
    .parse_next(input)
}

/// Parse optional whitespace (spaces and tabs only, not newlines within macro parens).
fn ws<'i, Input, Error>(input: &mut Input) -> Result<(), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    take_while(0.., |c: <Input as Stream>::Token| {
        let ch = c.as_char();
        ch == ' ' || ch == '\t'
    })
    .void()
    .parse_next(input)
}

/// Parse a u32 value from decimal digits.
fn parse_u32<'i, Input, Error>(input: &mut Input) -> Result<u32, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    let digits = take_while(1.., |c: <Input as Stream>::Token| c.as_char().is_ascii_digit())
        .parse_next(input)?;
    let s = String::from_utf8_lossy(digits.as_bstr());
    let n = s
        .parse::<u32>()
        .map_err(|_| ParserError::from_input(input))?;
    Ok(n)
}

/// Parse `EXPECTING min[..max]` clause.
fn expecting<'i, Input, Error>(input: &mut Input) -> Result<(u32, Option<u32>), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("expecting", move |input: &mut Input| {
        literal("EXPECTING").parse_next(input)?;
        ws(input)?;
        let min = parse_u32(input)?;
        let max = opt(preceded(literal(".."), parse_u32)).parse_next(input)?;
        Ok((min, max))
    })
    .parse_next(input)
}

/// Parse a complete `:bind(name [EXPECTING min[..max]] [NULL])` macro.
///
/// Assumes the `:bind(` prefix has already been consumed. Parses the contents
/// up to and including the closing `)`.
pub fn bind<'i, Input, Error>(input: &mut Input) -> Result<Binding, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("bind", move |input: &mut Input| {
        let name = bind_name(input)?;
        ws(input)?;

        let expecting_result = opt(expecting).parse_next(input)?;
        ws(input)?;

        let null_kw = opt(literal("NULL")).parse_next(input)?;
        ws(input)?;

        literal(")").parse_next(input)?;

        let (min_values, max_values) = match expecting_result {
            Some((min, max)) => (Some(min), max),
            None => (None, None),
        };

        Ok(Binding {
            name,
            min_values,
            max_values,
            nullable: null_kw.is_some(),
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
    fn test_bind_simple() {
        let mut input: TestInput = "user_id)";
        let result = bind::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.name, "user_id");
        assert_eq!(result.min_values, None);
        assert_eq!(result.max_values, None);
        assert!(!result.nullable);
        assert_eq!(input, "");
    }

    #[test]
    fn test_bind_with_expecting() {
        let mut input: TestInput = "values EXPECTING 1..10)";
        let result = bind::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.name, "values");
        assert_eq!(result.min_values, Some(1));
        assert_eq!(result.max_values, Some(10));
        assert!(!result.nullable);
    }

    #[test]
    fn test_bind_with_expecting_min_only() {
        let mut input: TestInput = "values EXPECTING 3)";
        let result = bind::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.name, "values");
        assert_eq!(result.min_values, Some(3));
        assert_eq!(result.max_values, None);
        assert!(!result.nullable);
    }

    #[test]
    fn test_bind_nullable() {
        let mut input: TestInput = "email NULL)";
        let result = bind::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.name, "email");
        assert!(result.nullable);
    }

    #[test]
    fn test_bind_full() {
        let mut input: TestInput = "tags EXPECTING 1..5 NULL)";
        let result = bind::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.name, "tags");
        assert_eq!(result.min_values, Some(1));
        assert_eq!(result.max_values, Some(5));
        assert!(result.nullable);
    }

    #[test]
    fn test_bind_name_only() {
        let mut input: TestInput = "active";
        let result = bind_name::<_, ContextError>
            .parse_next(&mut input)
            .unwrap();
        assert_eq!(result, "active");
    }
}
