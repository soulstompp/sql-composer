//! The template reader: SQL passed through as written, the directives parsed where they stand in
//! SQL itself, and `#` comments stripped.
//!
//! The reader reads the SQL around a directive as each dialect's database reads its quotes and
//! comments. Inside a string, a quoted name, a comment or a psql meta-command, a `#` and a
//! directive are SQL like any other. Inside a dollar quote a directive composes, and a `:bind` is
//! refused, since no value reaches it there. `#>`, `#>>` and `#-` are SQL wherever they stand.
//!
//! A `#` comment in SQL itself runs to the end of the line, the newline with it, and never
//! reaches the composed SQL. A template that two databases read differently at a `#`, a
//! directive, a `;` or its end is refused, with both readings.

use winnow::combinator::{alt, trace};
use winnow::error::{ContextError, ParserError};
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::literal;
use winnow::Parser;

use crate::error::{Error, Result};
use crate::scan::{Scanner, Stand};
use crate::types::{Dialect, Element};

use super::bind::bind;
use super::command::{command_body, command_kind};
use super::compose::{compose, define};

/// The directives, each by the name between its `:` and its `(`.
const DIRECTIVES: [&str; 7] = [
    "bind",
    "compose",
    "define",
    "count",
    "union",
    "intersect",
    "except",
];

/// Parse a single macro invocation after the `:` prefix.
///
/// Tries `bind(`, `compose(`, `define(`, then the commands `count(`, `union(`, `intersect(` and
/// `except(`, in order.
fn macro_invocation<'i, Input, Error>(input: &mut Input) -> std::result::Result<Element, Error>
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
            literal("define(")
                .flat_map(|_| define)
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

/// Read a template into its elements: SQL, directives, and nothing for its `#` comments.
pub(crate) fn template(input: &str) -> Result<Vec<Element>> {
    Reader::new(input).read()
}

/// What the reader does at a byte one database's reading of the template decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    /// Keep it as SQL.
    Keep,
    /// Read it as the template's own: a directive, a `#` comment, the end of a statement.
    Read,
    /// Refuse it: a `:bind` where no value reaches.
    Refuse,
}

struct Reader<'i> {
    input: &'i str,
    pos: usize,
    /// Each database's reading of the SQL read so far.
    readings: Vec<(Dialect, Scanner)>,
    elements: Vec<Element>,
    sql: String,
    /// Whether a `#` comment was stripped since the last element.
    stripped: bool,
}

impl<'i> Reader<'i> {
    fn new(input: &'i str) -> Self {
        Reader {
            input,
            pos: 0,
            readings: Dialect::ALL
                .iter()
                .map(|dialect| (*dialect, Scanner::new(dialect.regime())))
                .collect(),
            elements: Vec::new(),
            sql: String::new(),
            stripped: false,
        }
    }

    fn read(mut self) -> Result<Vec<Element>> {
        while let Some(next) = self.input[self.pos..].chars().next() {
            let read = match next {
                ':' => self.directive()?,
                '#' => self.comment()?,
                ';' => {
                    self.agree("this ;", |stand| match stand {
                        Stand::Code => (Act::Read, "as the end of a statement"),
                        _ => (Act::Keep, ""),
                    })?;
                    false
                }
                _ => false,
            };
            if !read {
                self.keep(next);
            }
        }
        self.end_element();
        self.agree("the template's end", |stand| match stand {
            Stand::Code => (Act::Read, "in SQL"),
            _ => (Act::Keep, ""),
        })?;
        Ok(self.elements)
    }

    /// The directive at the reader's place, read where every database reads it in SQL itself
    /// or in a dollar quote. Returns whether it was read.
    fn directive(&mut self) -> Result<bool> {
        let rest = &self.input[self.pos + 1..];
        let Some(name) = DIRECTIVES.iter().find(|name| {
            rest.strip_prefix(**name)
                .is_some_and(|r| r.starts_with('('))
        }) else {
            return Ok(false);
        };
        let bind = *name == "bind";
        let what = format!("this :{name}");
        let act = self.agree(&what, |stand| match stand {
            Stand::Code => (Act::Read, "as a directive"),
            Stand::Dollar if bind => (Act::Refuse, ""),
            Stand::Dollar => (Act::Read, "as a directive"),
            Stand::Inside => (Act::Keep, ""),
        })?;
        match act {
            Act::Keep => Ok(false),
            Act::Refuse => Err(self.refusal(format!(
                ":{name} inside a dollar quote: no value reaches it there"
            ))),
            Act::Read => {
                let input = self.input;
                let mut rest = &input[self.pos..];
                let element = macro_invocation::<_, ContextError>
                    .parse_next(&mut rest)
                    .map_err(|error| self.refusal(format!(":{name} does not parse: {error}")))?;
                self.end_element();
                self.elements.push(element);
                self.pos = self.input.len() - rest.len();
                Ok(true)
            }
        }
    }

    /// The `#` at the reader's place, stripped to the end of its line, newline and all, where
    /// every database reads it in SQL itself. `#>`, `#>>` and `#-` are SQL, and `#--` is a
    /// comment. Returns whether it was stripped.
    fn comment(&mut self) -> Result<bool> {
        let after = &self.input[self.pos + 1..];
        let operator =
            after.starts_with('>') || (after.starts_with('-') && !after.starts_with("--"));
        let act = self.agree("this #", |stand| match stand {
            Stand::Code if !operator => (Act::Read, "as a template comment"),
            _ => (Act::Keep, ""),
        })?;
        if act != Act::Read {
            return Ok(false);
        }
        self.pos = match after.find('\n') {
            Some(newline) => self.pos + 1 + newline + 1,
            None => self.input.len(),
        };
        self.stripped = true;
        Ok(true)
    }

    /// `next` kept as SQL, and read by every database.
    fn keep(&mut self, next: char) {
        let at = self.sql.len();
        let mut bytes = [0; 4];
        for (i, byte) in next.encode_utf8(&mut bytes).bytes().enumerate() {
            for (_, scanner) in &mut self.readings {
                scanner.step(at + i, byte, &mut |_, _| {});
            }
        }
        self.sql.push(next);
        self.pos += next.len_utf8();
    }

    /// The SQL read since the last element, as an element of its own, and a break for every
    /// database's reading, as the composer's split takes it.
    fn end_element(&mut self) {
        if !self.sql.is_empty() || self.stripped {
            self.elements
                .push(Element::Sql(std::mem::take(&mut self.sql)));
        }
        self.stripped = false;
        for (_, scanner) in &mut self.readings {
            scanner.settle(&mut |_, _| {});
        }
    }

    /// What every database does at the reader's place, as `act` says from where it stands there;
    /// refused where two differ, naming each reading. `act` gives the words for a stand that
    /// is not the scanner's own: a directive, a comment, the end of a statement.
    fn agree(&self, what: &str, act: impl Fn(Stand) -> (Act, &'static str)) -> Result<Act> {
        let acts: Vec<(Dialect, Act, &'static str)> = self
            .readings
            .iter()
            .map(|(dialect, scanner)| {
                let (done, words) = act(scanner.stands());
                let words = if words.is_empty() {
                    scanner.reading()
                } else {
                    words
                };
                (*dialect, done, words)
            })
            .collect();
        let first = acts[0].1;
        if acts.iter().all(|(_, done, _)| *done == first) {
            return Ok(first);
        }
        let mut groups: Vec<(&'static str, Vec<&'static str>)> = Vec::new();
        for (dialect, _, words) in &acts {
            match groups.iter_mut().find(|(said, _)| said == words) {
                Some((_, names)) => names.push(dialect.name()),
                None => groups.push((words, vec![dialect.name()])),
            }
        }
        let readings: Vec<String> = groups
            .iter()
            .enumerate()
            .map(|(i, (words, names))| {
                let verb = if i == 0 {
                    format!(" read {what}")
                } else {
                    String::new()
                };
                format!("{}{verb} {words}", listed(names))
            })
            .collect();
        let (last, rest) = readings.split_last().expect("two readings differ");
        Err(self.refusal(format!(
            "{}, and {last}; write it so every database reads it alike",
            rest.join(", ")
        )))
    }

    /// A parse error at the reader's place, by line and column.
    fn refusal(&self, message: String) -> Error {
        let before = &self.input[..self.pos];
        let line = before.matches('\n').count() + 1;
        let column = before[before.rfind('\n').map_or(0, |at| at + 1)..]
            .chars()
            .count()
            + 1;
        Error::Parse {
            location: format!("line {line}, column {column}"),
            message,
        }
    }
}

/// `a`, `a and b`, `a, b and c`.
fn listed<S: AsRef<str>>(items: &[S]) -> String {
    match items {
        [] => String::new(),
        [one] => one.as_ref().to_string(),
        [rest @ .., last] => format!(
            "{} and {}",
            rest.iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>()
                .join(", "),
            last.as_ref()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, CommandKind, ComposeRef, ComposeTarget};
    use std::path::PathBuf;

    #[test]
    fn test_plain_sql() {
        let input = "SELECT id, name FROM users";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT id, name FROM users".into()));
    }

    #[test]
    fn test_sql_with_bind() {
        let input = "SELECT * FROM users WHERE id = :bind(user_id)";
        let result = template(input).unwrap();
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
        let input = "SELECT COUNT(*) FROM (\n  :compose(templates/get_user.tql)\n)";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Element::Sql("SELECT COUNT(*) FROM (\n  ".into()));
        assert_eq!(
            result[1],
            Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("templates/get_user.tql")),
                slots: vec![],
            })
        );
        assert_eq!(result[2], Element::Sql("\n)".into()));
    }

    #[test]
    fn test_multiple_binds() {
        let input = "WHERE id = :bind(user_id) AND active = :bind(active)";
        let result = template(input).unwrap();
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
        let input = "SELECT '10:30' FROM t";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT '10:30' FROM t".into()));
    }

    #[test]
    fn test_command_in_template() {
        let input = ":count(templates/get_user.tql)";
        let result = template(input).unwrap();
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
        let input =
            "SELECT id, name, email\nFROM users\nWHERE id = :bind(user_id)\n  AND active = :bind(active);";
        let result = template(input).unwrap();
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
        let input = "WHERE id = :bind(user_id);";
        let result = template(input).unwrap();
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
        let input = "";
        let result = template(input).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_comment_standalone_line() {
        let input = "# comment\nSELECT 1;";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_inline() {
        let input = "SELECT 1; # comment\nSELECT 2;";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1; SELECT 2;".into()));
    }

    #[test]
    fn test_comment_with_macro_text() {
        let input = "# :bind(x)\nSELECT 1;";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_before_macro() {
        let input = "# get user\nSELECT * FROM users WHERE id = :bind(id);";
        let result = template(input).unwrap();
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
        let input = "# just a comment";
        let result = template(input).unwrap();
        // Comment-only input produces a single empty SQL element (stripped comments).
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql(String::new()));
    }

    #[test]
    fn test_multiple_comment_lines() {
        let input = "# line 1\n# line 2\nSELECT 1;";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_at_eof_no_newline() {
        let input = "SELECT 1;\n# trailing";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;\n".into()));
    }

    #[test]
    fn test_at_sign_in_sql_literal() {
        let input = "SELECT @variable FROM t";
        let result = template(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT @variable FROM t".into()));
    }

    #[test]
    fn test_comments_before_macro() {
        let input = "# comment line 1\n# comment line 2\n:bind(id)";
        let result = template(input).unwrap();
        // Comments are stripped; the macro is still parsed.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Element::Sql(String::new()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }
}
