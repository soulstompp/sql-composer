//! Top-level template parser that dispatches between macros and literal SQL.
//!
//! The key insight of this parser is that SQL is treated as opaque literal text.
//! Only the `:bind(...)`, `:compose(...)`, `:count(...)`, and `:union(...)`
//! macros are parsed; everything else passes through unchanged.
//!
//! Lines or trailing portions beginning with `#` are template comments and are
//! silently stripped during parsing — they never appear in composed SQL output.

use winnow::combinator::{alt, repeat, trace};
use winnow::error::ParserError;
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::{any, literal};
use winnow::Parser;

use crate::types::Element;

use super::bind::bind;
use super::command::{command_body, command_kind};
use super::compose::compose;

/// Parse a single macro invocation after the `:` prefix.
///
/// Tries `bind(`, `compose(`, `count(`, or `union(` in order.
fn macro_invocation<'i, Input, Error>(input: &mut Input) -> Result<Element, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("macro_invocation", move |input: &mut Input| {
        literal(":").parse_next(input)?;

        alt((
            literal("bind(").flat_map(|_| bind).map(Element::Bind),
            literal("compose(")
                .flat_map(|_| compose)
                .map(Element::Compose),
            |input: &mut Input| {
                let kind = command_kind(input)?;
                let cmd = command_body(input, kind)?;
                Ok(Element::Command(cmd))
            },
        ))
        .parse_next(input)
    })
    .parse_next(input)
}

/// Parse literal SQL text: everything up to the next `:` that starts a macro,
/// or to the end of input.
///
/// Accumulates characters one at a time, stopping when we encounter a `:`
/// followed by a known macro name and `(`.
fn sql_literal<'i, Input, Error>(input: &mut Input) -> Result<Element, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("sql_literal", move |input: &mut Input| {
        let mut sql = String::new();

        loop {
            // Check if we're at a macro start
            let checkpoint = input.checkpoint();
            if literal::<_, _, Error>(":").parse_next(input).is_ok() {
                // Check if this is followed by a known macro name + "("
                let is_macro = alt((
                    literal::<_, Input, Error>("bind(").void(),
                    literal::<_, Input, Error>("compose(").void(),
                    literal::<_, Input, Error>("count(").void(),
                    literal::<_, Input, Error>("union(").void(),
                ))
                .parse_next(input)
                .is_ok();

                // Reset to before the ":"
                input.reset(&checkpoint);

                if is_macro {
                    break;
                }
            } else {
                input.reset(&checkpoint);
            }

            // Try to consume one character
            match any::<_, Error>.parse_next(input) {
                Ok(c) => {
                    let ch = c.as_char();
                    if ch == '#' {
                        // Comment: skip to end of line (or EOF)
                        loop {
                            match any::<_, Error>.parse_next(input) {
                                Ok(c) if c.clone().as_char() == '\n' => break,
                                Ok(_) => continue,
                                Err(_) => break, // EOF
                            }
                        }
                    } else {
                        sql.push(ch);
                    }
                }
                Err(_) => break, // EOF
            }
        }

        if sql.is_empty() {
            return Err(ParserError::from_input(input));
        }

        Ok(Element::Sql(sql))
    })
    .parse_next(input)
}

/// Parse a single template element: either a macro invocation or literal SQL.
fn element<'i, Input, Error>(input: &mut Input) -> Result<Element, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("element", move |input: &mut Input| {
        alt((macro_invocation, sql_literal)).parse_next(input)
    })
    .parse_next(input)
}

/// Parse a complete template into a sequence of elements.
///
/// This is the top-level parser entry point for template content.
pub fn template<'i, Input, Error>(input: &mut Input) -> Result<Vec<Element>, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("template", move |input: &mut Input| {
        let elements: Vec<Element> = repeat(0.., element).parse_next(input)?;
        Ok(elements)
    })
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, CommandKind, ComposeRef};
    use std::path::PathBuf;
    use winnow::error::ContextError;

    type TestInput<'a> = &'a str;

    #[test]
    fn test_plain_sql() {
        let mut input: TestInput = "SELECT id, name FROM users";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT id, name FROM users".into()));
    }

    #[test]
    fn test_sql_with_bind() {
        let mut input: TestInput = "SELECT * FROM users WHERE id = :bind(user_id)";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            Element::Sql("SELECT * FROM users WHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }

    #[test]
    fn test_sql_with_compose() {
        let mut input: TestInput = "SELECT COUNT(*) FROM (\n  :compose(templates/get_user.tql)\n)";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Element::Sql("SELECT COUNT(*) FROM (\n  ".into()));
        assert_eq!(
            result[1],
            Element::Compose(ComposeRef {
                path: PathBuf::from("templates/get_user.tql"),
            })
        );
        assert_eq!(result[2], Element::Sql("\n)".into()));
    }

    #[test]
    fn test_multiple_binds() {
        let mut input: TestInput = "WHERE id = :bind(user_id) AND active = :bind(active)";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Element::Sql("WHERE id = ".into()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(" AND active = ".into()));
        assert_eq!(
            result[3],
            Element::Bind(Binding {
                name: "active".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }

    #[test]
    fn test_colon_not_a_macro() {
        let mut input: TestInput = "SELECT '10:30' FROM t";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT '10:30' FROM t".into()));
    }

    #[test]
    fn test_command_in_template() {
        let mut input: TestInput = ":count(templates/get_user.tql)";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            Element::Command(cmd) => {
                assert_eq!(cmd.kind, CommandKind::Count);
                assert_eq!(cmd.sources, vec![PathBuf::from("templates/get_user.tql")]);
            }
            other => panic!("expected Command, got {:?}", other),
        }
    }

    #[test]
    fn test_full_template() {
        let mut input: TestInput =
            "SELECT id, name, email\nFROM users\nWHERE id = :bind(user_id)\n  AND active = :bind(active);";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(
            result[0],
            Element::Sql("SELECT id, name, email\nFROM users\nWHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql("\n  AND active = ".into()));
        assert_eq!(
            result[3],
            Element::Bind(Binding {
                name: "active".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[4], Element::Sql(";".into()));
    }

    #[test]
    fn test_semicolon_after_bind() {
        let mut input: TestInput = "WHERE id = :bind(user_id);";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Element::Sql("WHERE id = ".into()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(";".into()));
    }

    #[test]
    fn test_empty_input() {
        let mut input: TestInput = "";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_comment_standalone_line() {
        let mut input: TestInput = "# comment\nSELECT 1;";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_inline() {
        let mut input: TestInput = "SELECT 1; # comment\nSELECT 2;";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1; SELECT 2;".into()));
    }

    #[test]
    fn test_comment_with_macro_text() {
        let mut input: TestInput = "# :bind(x)\nSELECT 1;";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_before_macro() {
        let mut input: TestInput = "# get user\nSELECT * FROM users WHERE id = :bind(id);";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            Element::Sql("SELECT * FROM users WHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(";".into()));
    }

    #[test]
    fn test_only_comments() {
        let mut input: TestInput = "# just a comment";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_multiple_comment_lines() {
        let mut input: TestInput = "# line 1\n# line 2\nSELECT 1;";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_at_eof_no_newline() {
        let mut input: TestInput = "SELECT 1;\n# trailing";
        let result = template::<_, ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;\n".into()));
    }
}
