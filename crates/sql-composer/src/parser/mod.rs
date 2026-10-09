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
/// This is the main entry point for parsing template content from a string. A template refused
/// is placed by line and column, after its file's path when it has one.
pub fn parse_template(input: &str, source: TemplateSource) -> error::Result<Template> {
    let (dialects, elements) =
        template::template(input).map_err(|error| match (error, &source) {
            (error::Error::Parse { location, message }, TemplateSource::File(path)) => {
                error::Error::Parse {
                    location: format!("{}, {location}", path.display()),
                    message,
                }
            }
            (error, _) => error,
        })?;
    Ok(Template {
        elements,
        source,
        dialects,
    })
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
             inside a string; write it so they read it alike, or declare the dialects the template is written for with \
             :dialect(…)"
        );
        let (location, message) = refusal("SELECT 1 --x;\nSELECT 2");
        assert_eq!(location, "line 1, column 13");
        assert_eq!(
            message,
            "postgres, sqlite and duckdb read this ; inside a comment, and mysql and mariadb as \
             the end of a statement; write it so they read it alike, or declare the dialects the template is written for with \
             :dialect(…)"
        );
        let (_, message) = refusal("DO $$ BEGIN PERFORM :bind(a); END $$");
        assert_eq!(
            message,
            "postgres, mysql and duckdb read this :bind inside a dollar quote, and mariadb and \
             sqlite as a directive; write it so they read it alike, or declare the dialects the template is written for with \
             :dialect(…)"
        );
        let (location, message) = refusal("SELECT 1 --x");
        assert_eq!(location, "line 1, column 13");
        assert_eq!(
            message,
            "postgres, sqlite and duckdb read the template's end inside a comment, and mysql and \
             mariadb in SQL; write it so they read it alike, or declare the dialects the template is written for with \
             :dialect(…)"
        );
    }

    #[test]
    fn test_a_directive_that_does_not_parse_is_refused() {
        let (location, message) = refusal("SELECT 1\nWHERE a = :bind(a b) AND b = 2;\nSELECT 3;");
        assert_eq!(location, "line 2, column 11");
        assert_eq!(message, ":bind does not parse");
        match parse_template(
            "SELECT :bind(a b)",
            TemplateSource::File("sqlc/t.sqlc".into()),
        ) {
            Err(error::Error::Parse { location, .. }) => {
                assert_eq!(location, "sqlc/t.sqlc, line 1, column 8")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn test_a_declared_template_is_read_by_its_own_dialects() {
        let script = "DO $$ BEGIN PERFORM 1; END $$;\n";
        refusal(script);
        assert_eq!(
            elements(&format!(":dialect(postgres)\n{script}")),
            sql(script)
        );
        assert_eq!(
            elements(&format!(
                "# PostgreSQL's own\n:dialect(postgres, duckdb) \n{script}"
            )),
            sql(script)
        );
        assert_eq!(
            elements(":dialect(mysql)\nSELECT `a#b` FROM t\n"),
            sql("SELECT `a#b` FROM t\n")
        );
        assert_eq!(
            elements(":dialect(postgres)\nSELECT E'it\\'s'; SELECT 2; # note\n"),
            sql("SELECT E'it\\'s'; SELECT 2; ")
        );
    }

    #[test]
    fn test_inside_a_dollar_quote_a_compose_composes_and_a_bind_is_refused() {
        let function =
            ":dialect(postgres)\nCREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$\n\
                        #variable_conflict use_column\n\
                        BEGIN RETURN (SELECT n FROM (:compose(n.sqlc)) c); END $$;\n";
        match &elements(function)[..] {
            [Element::Sql(head), Element::Compose(_), Element::Sql(_)] => {
                assert!(
                    head.ends_with("#variable_conflict use_column\nBEGIN RETURN (SELECT n FROM (")
                )
            }
            other => panic!("{other:?}"),
        }
        let (location, message) =
            refusal(":dialect(postgres)\nDO $$ BEGIN PERFORM :bind(a); END $$;\n");
        assert_eq!(location, "line 2, column 21");
        assert_eq!(
            message,
            ":bind inside a dollar quote: no value reaches it there"
        );
    }

    #[test]
    fn test_a_declaration_names_dialects_once_at_the_head() {
        let elsewhere =
            ":dialect declares the template's dialects once, at its head, before any SQL";
        for (input, location, message) in [
            (
                ":dialect(oracle)\nSELECT 1",
                "line 1, column 1",
                "oracle is not a dialect: postgres, mysql, mariadb, sqlite or duckdb",
            ),
            (
                ":dialect()\nSELECT 1",
                "line 1, column 1",
                ":dialect names no dialect",
            ),
            (
                "SELECT 1;\n:dialect(postgres)\n",
                "line 2, column 1",
                elsewhere,
            ),
            (
                ":dialect(postgres)\n:dialect(mysql)\n",
                "line 2, column 1",
                elsewhere,
            ),
        ] {
            assert_eq!(
                refusal(input),
                (location.into(), message.into()),
                "{input:?}"
            );
        }
    }
}
