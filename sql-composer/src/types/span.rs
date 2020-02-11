//! Span, GeneratedSpan and ParsedSpan

use std::convert::From;
use std::fmt;

use crate::types::SqlCompositionAlias;

pub use nom_locate::LocatedSpan;
pub type Span<'a> = LocatedSpan<&'a str>;

/// GeneratedSpan is for ...
#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct GeneratedSpan {
    pub command: Option<String>,
}

impl From<Option<String>> for GeneratedSpan {
    fn from(os: Option<String>) -> Self {
        Self { command: os }
    }
}

/// ParsedSpan is for ...
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ParsedSpan {
    pub alias:    Option<SqlCompositionAlias>,
    pub line:     u32,
    pub offset:   usize,
    pub fragment: String,
}

/// line number should default to 1
impl Default for ParsedSpan {
    fn default() -> Self {
        Self {
            line:     1,
            alias:    None,
            offset:   0,
            fragment: "".to_string(),
        }
    }
}

impl ParsedSpan {
    pub fn new(span: Span, alias: Option<SqlCompositionAlias>) -> Self {
        Self {
            alias:    alias,
            line:     span.line,
            offset:   span.offset,
            fragment: span.fragment.to_string(),
        }
    }

    pub fn from_span(span: Span) -> Self {
        Self {
            line: span.line,
            offset: span.offset,
            fragment: span.fragment.to_string(),
            ..Default::default()
        }
    }
}

// explicit lifetime for Span is required: Span<'a>
// because "implicit elided lifetime is not allowed here"
impl<'a> From<Span<'a>> for ParsedSpan {
    fn from(span: Span) -> Self {
        Self {
            line: span.line,
            offset: span.offset,
            fragment: span.fragment.to_string(),
            ..Default::default()
        }
    }
}

impl fmt::Display for ParsedSpan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "character {}, line {}", self.line, self.offset)?;

        match &self.alias {
            Some(a) => write!(f, " of {}:", a)?,
            None => write!(f, ":")?,
        };

        write!(f, "{}", self.fragment)
    }
}
