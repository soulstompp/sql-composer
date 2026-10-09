//! Reading templates: the SQL passed through as written, and the directives `:bind(...)`,
//! `:compose(...)`, `:define(...)`, `:count(...)`, `:union(...)`, `:intersect(...)` and
//! `:except(...)` parsed, each by its winnow 1 parser, where they stand in SQL itself.

pub mod bind;
pub mod command;
pub mod compose;
mod template;

use crate::error;
use crate::types::{Template, TemplateSource};

/// Parse a template string into a [`Template`].
///
/// This is the main entry point for parsing template content from a string.
pub fn parse_template(input: &str, source: TemplateSource) -> error::Result<Template> {
    let elements = template::template(input)?;
    Ok(Template { elements, source })
}

/// Parse a template from a file path.
///
/// Reads the file content and parses it as a template.
pub fn parse_template_file(path: &std::path::Path) -> error::Result<Template> {
    let content = std::fs::read_to_string(path)?;
    parse_template(&content, TemplateSource::File(path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, Element};

    #[test]
    fn test_parse_template_literal() {
        let tpl = parse_template(
            "SELECT * FROM users WHERE id = :bind(user_id);",
            TemplateSource::Literal("test".into()),
        )
        .unwrap();

        assert_eq!(tpl.elements.len(), 3);
        assert_eq!(
            tpl.elements[0],
            Element::Sql("SELECT * FROM users WHERE id = ".into())
        );
        assert_eq!(
            tpl.elements[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(tpl.elements[2], Element::Sql(";".into()));
    }

    #[test]
    fn test_parse_template_multiline() {
        let input = "SELECT id, name, email\nFROM users\nWHERE id = :bind(user_id)\n  AND active = :bind(active);";
        let tpl = parse_template(input, TemplateSource::Literal("test".into())).unwrap();

        // Count bindings
        let bind_count = tpl
            .elements
            .iter()
            .filter(|e| matches!(e, Element::Bind(_)))
            .count();
        assert_eq!(bind_count, 2);
    }

    fn elements(input: &str) -> Vec<Element> {
        parse_template(input, TemplateSource::Literal("test".into()))
            .unwrap()
            .elements
    }

    fn refusal(input: &str) -> (String, String) {
        match parse_template(input, TemplateSource::Literal("test".into())) {
            Err(error::Error::Parse { location, message }) => (location, message),
            other => panic!("{input:?} was not refused: {other:?}"),
        }
    }

    fn sql(text: &str) -> Vec<Element> {
        vec![Element::Sql(text.into())]
    }

    #[test]
    fn test_a_hash_inside_quotes_and_comments_is_sql() {
        for input in [
            "SELECT '#1' AS a\n",
            "SELECT \"a#b\" FROM t\n",
            "SELECT 1 -- note # here\nFROM t\n",
            "SELECT 1 /* # */ FROM t\n",
            "\\echo #\nSELECT 1\n",
        ] {
            assert_eq!(elements(input), sql(input), "{input:?}");
        }
    }

    #[test]
    fn test_a_hash_in_sql_itself_begins_a_comment() {
        assert_eq!(elements("SELECT 1 # note\nFROM t"), sql("SELECT 1 FROM t"));
        assert_eq!(elements("SELECT 1 #---- x\nFROM t"), sql("SELECT 1 FROM t"));
    }

    #[test]
    fn test_the_jsonb_operators_are_sql() {
        let input = "SELECT d #> '{a}', d #>> '{a}', d #- '{a}' FROM t\n";
        assert_eq!(elements(input), sql(input));
        assert_eq!(elements("#>> 'x'\n"), sql("#>> 'x'\n"));
    }

    #[test]
    fn test_an_apostrophe_in_a_comment_opens_nothing() {
        assert_eq!(
            elements("# it's a note\nSELECT 1 # don't\n FROM t # stripped\n"),
            sql("SELECT 1  FROM t ")
        );
        assert_eq!(
            elements("SELECT 1 -- it's\nFROM t # stripped\n"),
            sql("SELECT 1 -- it's\nFROM t ")
        );
    }

    #[test]
    fn test_a_directive_inside_quotes_and_comments_is_sql() {
        for input in [
            "SELECT ':bind(a)' AS a",
            "SELECT 1 -- :bind(a)\nFROM t",
            "SELECT 1 /* :compose(x.sqlc) */ FROM t",
            "\\set x :bind(a)\nSELECT 1",
        ] {
            assert_eq!(elements(input), sql(input), "{input:?}");
        }
        assert!(matches!(
            elements("SELECT ':' || :bind(a)")[..],
            [Element::Sql(_), Element::Bind(_)]
        ));
    }

    #[test]
    fn test_a_template_two_databases_read_differently_is_refused() {
        let (location, message) = refusal("SELECT 1;\nSELECT E'it\\'s' # note\n");
        assert_eq!(location, "line 2, column 17");
        assert_eq!(
            message,
            "postgres, mysql, mariadb and duckdb read this # as a template comment, and sqlite \
             inside a string; write it so every database reads it alike"
        );
        let (location, message) = refusal("SELECT 1 --x;\nSELECT 2");
        assert_eq!(location, "line 1, column 13");
        assert_eq!(
            message,
            "postgres, sqlite and duckdb read this ; inside a comment, and mysql and mariadb as \
             the end of a statement; write it so every database reads it alike"
        );
        let (_, message) = refusal("DO $$ BEGIN PERFORM :bind(a); END $$");
        assert_eq!(
            message,
            "postgres, mysql and duckdb read this :bind inside a dollar quote, and mariadb and \
             sqlite as a directive; write it so every database reads it alike"
        );
        let (location, message) = refusal("SELECT 1 --x");
        assert_eq!(location, "line 1, column 13");
        assert_eq!(
            message,
            "postgres, sqlite and duckdb read the template's end inside a comment, and mysql and \
             mariadb in SQL; write it so every database reads it alike"
        );
    }

    #[test]
    fn test_a_directive_that_does_not_parse_is_refused() {
        let (location, message) = refusal("SELECT 1\nWHERE a = :bind(a b) AND b = 2;\nSELECT 3;");
        assert_eq!(location, "line 2, column 11");
        assert!(message.starts_with(":bind does not parse"), "{message}");
    }
}
