//! winnow 0.7 parsers for sql-composer template macros.
//!
//! The parser treats SQL text as opaque literals and only recognizes the
//! template macro syntax: `:bind(...)`, `:compose(...)`, `:count(...)`,
//! and `:union(...)`.

pub mod bind;
pub mod command;
pub mod compose;
pub mod template;

use winnow::error::ContextError;
use winnow::Parser;

use crate::error;
use crate::types::{Element, Template, TemplateSource};

/// Parse a template string into a [`Template`].
///
/// This is the main entry point for parsing template content from a string.
pub fn parse_template(input: &str, source: TemplateSource) -> error::Result<Template> {
    let mut remaining = input;
    let elements: Vec<Element> = template::template::<_, ContextError>
        .parse_next(&mut remaining)
        .map_err(|e| error::Error::Parse {
            location: format!("offset {}", input.len() - remaining.len()),
            message: e.to_string(),
        })?;

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
}
