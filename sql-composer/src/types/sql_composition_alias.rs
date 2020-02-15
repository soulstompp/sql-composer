use crate::error::Result;

use std::convert::{From, Into};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use crate::error::Error;
use crate::types::{Span, SqlDbObject, SqlLiteral};

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub enum SqlCompositionAlias {
    Path(PathBuf),
    DbObject(SqlDbObject),
    SqlLiteral(SqlLiteral),
}

impl SqlCompositionAlias {
    pub fn from_span(s: Span) -> Result<Self> {
        Self::from_str(s.fragment)
    }

    fn from_str(s: &str) -> Result<Self> {
        let (_is_name, _is_path) = s.chars().fold((true, false), |mut acc, u| {
            let c = u as char;

            match c {
                'a'..='z' => {}
                '0'..='9' => {}
                '-' | '_' => {}
                '.' | '/' | '\\' => acc.1 = true,
                _ => acc = (false, false),
            }

            acc
        });

        Ok(s.into())
    }

    pub fn from_path<P>(path: P) -> Self
    where
        P: Into<PathBuf> + std::fmt::Debug,
    {
        path.into().into()
    }

    /// Return an owned copy of the PathBuf for SqlCompositionAlias::Path types.
    pub fn path(&self) -> Option<PathBuf> {
        match self {
            // PathBuf doesn't impl Copy, so use to_path_buf for a new one
            Self::Path(p) => Some(p.to_path_buf()),
            _ => None,
        }
    }

    pub fn read_raw_sql(&self) -> Result<String> {
        match self {
            SqlCompositionAlias::DbObject(dbo) => Ok(dbo.to_string()),
            SqlCompositionAlias::Path(path) => Ok(fs::read_to_string(&path)?),
            SqlCompositionAlias::SqlLiteral(s) => Ok(s.to_string()),
        }
    }
}

// str and Span will need to be moved to TryFrom
// if the from_str match gets implemented
//impl<P> From<P> for SqlCompositionAlias
//where
//    P: Into<PathBuf> + std::fmt::Debug,
//{
//    fn from(path: P) -> Self {
//        SqlCompositionAlias::Path(path.into())
//    }
//}

impl From<PathBuf> for SqlCompositionAlias {
    fn from(path: PathBuf) -> Self {
        SqlCompositionAlias::Path(path.into())
    }
}

impl From<&PathBuf> for SqlCompositionAlias {
    fn from(path: &PathBuf) -> Self {
        SqlCompositionAlias::Path(path.into())
    }
}

impl From<&str> for SqlCompositionAlias {
    fn from(s: &str) -> Self {
        SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s.to_string(),
            generated: false,
        })
    }
}

impl From<String> for SqlCompositionAlias {
    fn from(s: String) -> Self {
        SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s,
            generated: false,
        })
    }
}

/// Destructively convert a SqlCompositionAlias into a PathBuf
impl Into<Option<PathBuf>> for SqlCompositionAlias {
    fn into(self) -> Option<PathBuf> {
        match self {
            Self::Path(p) => Some(p),
            _ => None,
        }
    }
}

impl Default for SqlCompositionAlias {
    fn default() -> Self {
        //TODO: better default
        Self::DbObject(SqlDbObject {
            id:           None,
            object_name:  "DUAL".to_string(),
            object_alias: None,
        })
    }
}

impl FromStr for SqlCompositionAlias {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s.to_string(),
            generated: false,
        }))
    }
}

impl fmt::Display for SqlCompositionAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Path(p) => write!(f, ", {}", p.to_string_lossy()),
            Self::DbObject(dbo) => write!(f, ", {}", dbo),
            Self::SqlLiteral(l) => write!(f, "{}", l),
        }
    }
}
