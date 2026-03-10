//! Parser for `:compose(target, @slot = path, ...)` macros.

use std::path::PathBuf;

use winnow::combinator::trace;
use winnow::error::ParserError;
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::{literal, take_while};
use winnow::Parser;

use crate::types::{ComposeRef, ComposeTarget, SlotAssignment};

/// Parse a file path inside a compose macro: one or more characters that are
/// not `)`, `,`, or whitespace.
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
            ch != ')' && ch != ',' && ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r'
        })
        .parse_next(input)?;
        let path_str = String::from_utf8_lossy(path_str.as_bstr()).to_string();
        Ok(PathBuf::from(path_str))
    })
    .parse_next(input)
}

/// Parse a slot name after `@`: one or more alphanumeric, hyphen, or underscore chars.
/// Returns the name without the `@` prefix.
fn slot_name<'i, Input, Error>(input: &mut Input) -> Result<String, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("slot_name", move |input: &mut Input| {
        literal("@").parse_next(input)?;
        let name = take_while(1.., |c: <Input as Stream>::Token| {
            let ch = c.as_char();
            ch.is_alphanumeric() || ch == '-' || ch == '_'
        })
        .parse_next(input)?;
        Ok(String::from_utf8_lossy(name.as_bstr()).to_string())
    })
    .parse_next(input)
}

/// Parse optional whitespace (spaces and tabs).
fn opt_ws<'i, Input, Error>(input: &mut Input) -> Result<(), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    let _ = take_while(0.., |c: <Input as Stream>::Token| {
        let ch = c.as_char();
        ch == ' ' || ch == '\t'
    })
    .parse_next(input)?;
    Ok(())
}

/// Parse a slot assignment: `@name = path`.
fn slot_assignment<'i, Input, Error>(input: &mut Input) -> Result<SlotAssignment, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("slot_assignment", move |input: &mut Input| {
        let name = slot_name(input)?;
        opt_ws(input)?;
        literal("=").parse_next(input)?;
        opt_ws(input)?;
        let path = compose_path(input)?;
        Ok(SlotAssignment { name, path })
    })
    .parse_next(input)
}

/// Parse a complete `:compose(target, @slot = path, ...)` macro.
///
/// Assumes the `:compose(` prefix has already been consumed. Parses the target
/// (path or slot reference), optional slot assignments, and the closing `)`.
pub fn compose<'i, Input, Error>(input: &mut Input) -> Result<ComposeRef, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("compose", move |input: &mut Input| {
        // Try to parse target as a slot reference (@name) or a file path.
        let checkpoint = input.checkpoint();
        let target = if let Ok(name) = slot_name::<_, Error>(input) {
            // Check what follows — if `=` follows, this was actually a slot assignment
            // as the first arg, which is invalid (target must come first). But actually,
            // the target IS a slot reference like @filter, and `=` would only appear
            // in a slot assignment. After a slot target, we expect `)` or `,`.
            let ws_check = input.checkpoint();
            opt_ws::<_, Error>(input).ok();
            if literal::<_, _, Error>("=").parse_next(input).is_ok() {
                // This looks like `@name = ...` which is not a valid target.
                // Reset and try as a path (which will fail on `@`).
                input.reset(&checkpoint);
                let path = compose_path(input)?;
                ComposeTarget::Path(path)
            } else {
                input.reset(&ws_check);
                ComposeTarget::Slot(name)
            }
        } else {
            input.reset(&checkpoint);
            let path = compose_path(input)?;
            ComposeTarget::Path(path)
        };

        // Parse optional slot assignments: `, @name = path` repeated
        let mut slots = Vec::new();
        loop {
            opt_ws::<_, Error>(input).ok();
            let comma_check = input.checkpoint();
            if literal::<_, _, Error>(",").parse_next(input).is_ok() {
                opt_ws::<_, Error>(input).ok();
                let assignment = slot_assignment(input)?;
                slots.push(assignment);
            } else {
                input.reset(&comma_check);
                break;
            }
        }

        literal(")").parse_next(input)?;
        Ok(ComposeRef { target, slots })
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
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("templates/get_user.tql"))
        );
        assert!(result.slots.is_empty());
        assert_eq!(input, "");
    }

    #[test]
    fn test_compose_relative_path() {
        let mut input: TestInput = "src/tests/simple-template.tql)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("src/tests/simple-template.tql"))
        );
        assert!(result.slots.is_empty());
    }

    #[test]
    fn test_compose_with_trailing() {
        let mut input: TestInput = "get_user.tql) AND active";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("get_user.tql"))
        );
        assert!(result.slots.is_empty());
        assert_eq!(input, " AND active");
    }

    #[test]
    fn test_compose_single_slot() {
        let mut input: TestInput = "shared/base.sqlc, @filter = filters/by_color.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("shared/base.sqlc"))
        );
        assert_eq!(result.slots.len(), 1);
        assert_eq!(result.slots[0].name, "filter");
        assert_eq!(
            result.slots[0].path,
            PathBuf::from("filters/by_color.sqlc")
        );
    }

    #[test]
    fn test_compose_multiple_slots() {
        let mut input: TestInput =
            "shared/report.sqlc, @source = shared/details.sqlc, @filter = filters/color.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("shared/report.sqlc"))
        );
        assert_eq!(result.slots.len(), 2);
        assert_eq!(result.slots[0].name, "source");
        assert_eq!(
            result.slots[0].path,
            PathBuf::from("shared/details.sqlc")
        );
        assert_eq!(result.slots[1].name, "filter");
        assert_eq!(
            result.slots[1].path,
            PathBuf::from("filters/color.sqlc")
        );
    }

    #[test]
    fn test_compose_slot_reference() {
        let mut input: TestInput = "@filter)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Slot("filter".into())
        );
        assert!(result.slots.is_empty());
    }

    #[test]
    fn test_compose_slot_reference_with_assignments() {
        let mut input: TestInput = "@slot, @inner = some_file.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Slot("slot".into())
        );
        assert_eq!(result.slots.len(), 1);
        assert_eq!(result.slots[0].name, "inner");
        assert_eq!(result.slots[0].path, PathBuf::from("some_file.sqlc"));
    }

    #[test]
    fn test_slot_names_with_hyphens_underscores() {
        let mut input: TestInput = "base.sqlc, @my-filter = f.sqlc, @other_slot = g.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.slots.len(), 2);
        assert_eq!(result.slots[0].name, "my-filter");
        assert_eq!(result.slots[1].name, "other_slot");
    }

    #[test]
    fn test_whitespace_around_equals() {
        let mut input: TestInput = "base.sqlc, @filter  =  filters/x.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.slots.len(), 1);
        assert_eq!(result.slots[0].name, "filter");
        assert_eq!(result.slots[0].path, PathBuf::from("filters/x.sqlc"));
    }

    #[test]
    fn test_path_stops_at_comma() {
        let mut input: TestInput = "shared/base.sqlc, @s = f.sqlc)";
        let result = compose::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            result.target,
            ComposeTarget::Path(PathBuf::from("shared/base.sqlc"))
        );
    }
}
