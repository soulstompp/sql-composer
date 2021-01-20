//! ParsedItem contains an Item along with the Position where it was found.
//!
//! This allows for improved error messages and ...
//!
//!## Notes
//!### TryFrom trait implementation
//!
//! We have implemented several concrete implementations of `TryFrom`.
//! We would have preferred to implement `TryFrom` with a generic type constraint of `Into<PathBuf>`,
//! but this is blocked by [Bug 50133].
//!
//! The underlying method, `SqlComposition::from_path()` provides a
//! work around for any missing concrete implementations.
//!
//! [Bug 50133]: <https://github.com/rust-lang/rust/issues/50133>
//!
//! ``` ignore
//! // Will not compile due to rust-lang/rust#50133
//! impl ParsedSqlComposition {
//!     pub fn try_from<P>(path: P) -> Result<Self>
//!         where P: Into<PathBuf> + Debug {
//!             let path = path.into();
//!        let path = path.into();
//!              let path = path.into();
//!                   SqlComposition::parse(SqlCompositionAlias::from(path.to_path_buf()))
//!         }
//! }
//! ```

use crate::error::Result;

use std::convert::Into;
use std::fmt;
use std::fmt::{Debug, Display};

use crate::types::{Offset, ParsedSpan, Position, Span};

pub enum Item<T: Debug + Default + Display + PartialEq + Eq + Clone> {
    Generated(GeneratedItem<T>),
    Parsed(ParsedItem<T>),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GeneratedItem<T> {
    item: T,
    offset: Offset,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ParsedItem<T>
where
    T: Debug + Default + PartialEq + Clone,
{
    pub item:     T,
    pub position: Position,
}

impl<T> Item<T>
where
    T: Debug + Default + Display + PartialEq + Eq + Clone
{
    pub fn from_span(item: T, span: Span) -> Result<Self> {
        let ps: ParsedSpan = span.into();
        Ok(Self::Parsed (
            ParsedItem {
                item:     item,
                position: ps.into(),
            }
        ))
    }
    pub fn generated(item: T, command: Option<String>) -> Result<Self> {
        Ok(Self::Generated (
            GeneratedItem {
                item:     item,
                offset: Offset::default(),
            }
        ))
    }

    pub fn item(&self) -> T {
        match self {
            Self::Generated(i) => i.clone(),
            Self::Parsed(i) => i.clone(),
        }
    }
}


impl<T> Default for ParsedItem<T>
where
    T: Debug + Default + PartialEq + Clone,
{
    fn default() -> Self {
        Self {
            item:     T::default(),
            position: Position::default(),
        }
    }
}

impl<T> fmt::Display for ParsedItem<T>
where
    T: fmt::Display + Debug + Default + PartialEq + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.item)
    }
}
