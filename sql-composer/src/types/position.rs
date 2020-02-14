//! an enum for holding Generated and Parsed spans

use std::convert::{From, Into};
use std::fmt;
use std::fmt::Debug;

use crate::types::{GeneratedSpan, ParsedSpan};

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Position {
    Generated(GeneratedSpan),
    Parsed(ParsedSpan),
}

impl<P> From<P> for Position
where
    P: Into<ParsedSpan> + Debug,
{
    fn from(p: P) -> Self {
        Self::Parsed(p.into())
    }
}

impl From<GeneratedSpan> for Position {
    fn from(gs: GeneratedSpan) -> Self {
        Self::Generated(gs)
    }
}

// Shortcut from Option<String> -> GeneratedSpan -> Position
impl From<Option<String>> for Position {
    fn from(command: Option<String>) -> Self {
        Self::Generated(command.into())
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Position::Generated(gs) => write!(
                f,
                "command {}",
                match &gs.command {
                    Some(c) => c.to_string(),
                    None => "<None>".to_string(),
                }
            ),
            Position::Parsed(ps) => {
                match &ps.alias {
                    Some(a) => write!(f, "composition {} ", a)?,
                    None => write!(f, "")?,
                }

                write!(f, "character {} line {}", ps.offset, ps.line)
            }
        }
    }
}
