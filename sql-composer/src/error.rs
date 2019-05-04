use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::result;
use std::str;

use crate::types::{Position, SqlComposition, SqlCompositionAlias};

use nom::types::CompleteStr;
use nom_locate::LocatedSpan;

type Span<'a> = LocatedSpan<CompleteStr<'a>>;

//NOTE: this mod borrowed heavily from rust-csv's csv::error:Error to get started

/// A crate private constructor for `Error`.
pub fn new_error(kind: ErrorKind) -> Error {
    // use `pub(crate)` when it stabilizes.
    Error(Box::new(kind))
}

/// A type alias for `Result<T, sql-composer::Error>`.
pub type Result<T> = result::Result<T, Error>;

/// An error can occur when building or expanding a SqlCompostion
///
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl Error {
    /// Return the specific type of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    /// Unwrap this error into its underlying type.
    pub fn into_kind(self) -> ErrorKind {
        *self.0
    }

    /// Returns true if this is an I/O error.
    ///
    /// If this is true, the underlying `ErrorKind` is guaranteed to be
    /// `ErrorKind::Io`.
    pub fn is_io_error(&self) -> bool {
        match *self.0 {
            ErrorKind::Io(_) => true,
            _ => false,
        }
    }
}

/// The specific type of an error.
#[derive(Debug)]
pub enum ErrorKind {
    /// An I/O error that occurred while parsing SQL macros.
    Io(io::Error),
    /// A UTF-8 decoding error that occured while reading SQL macros into rust
    /// `String`s.
    Utf8 {
        position: Option<Position>,
        /// The corresponding UTF-8 error.
        err: Utf8Error,
    },
    AliasConflict {
        position: Option<Position>,
        /// The corresponding UTF-8 error.
        err: AliasConflictError,
    },
    CompositionIncomplete {
        position: Option<Position>,
        /// The corresponding UTF-8 error.
        err: CompositionIncompleteError,
    },
    /// Hints that destructuring shou
    #[doc(hidden)]
    __Nonexhaustive,
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Error {
        let utf8e = err.utf8_error();

        new_error(ErrorKind::Utf8 {
            position: None,
            err:      Utf8Error {
                error_len:   utf8e.error_len(),
                valid_up_to: utf8e.valid_up_to(),
            },
        })
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self.0 {
            ErrorKind::Io(ref err) => err.description(),
            ErrorKind::Utf8 { ref err, .. } => err.description(),
            _ => unreachable!(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self.0 {
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::Utf8 { ref err, .. } => Some(err),
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            ErrorKind::Io(ref err) => err.fmt(f),
            ErrorKind::Utf8 {
                position: Some(ref position),
                ref err,
            } => write!(f, "parse error: position: {}: {}", position, err),
            ErrorKind::Utf8 {
                position: None,
                ref err,
            } => write!(
                f,
                "file read parse error: \
                 (position: {}, err: {}",
                "<None>", err
            ),
            ErrorKind::CompositionIncomplete {
                position: Some(ref position),
                ref err,
            } => write!(
                f,
                "incomplete composition: \
                 (position: {}, err: {}",
                position, err
            ),
            _ => unreachable!(),
        }
    }
}

///
/// A UTF-8 validation error during record conversion.
///
/// This occurs when attempting to convert a `ByteRecord` into a
/// `StringRecord`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FromUtf8Error {
    alias: Option<SqlCompositionAlias>,
    err:   Utf8Error,
}

/// Create a new FromUtf8Error.
pub fn new_from_utf8_error(alias: Option<SqlCompositionAlias>, err: Utf8Error) -> FromUtf8Error {
    FromUtf8Error {
        alias: alias,
        err:   err,
    }
}

impl FromUtf8Error {
    /// Access the underlying UTF-8 validation error.
    pub fn utf8_error(&self) -> &Utf8Error {
        &self.err
    }
}

impl fmt::Display for FromUtf8Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl StdError for FromUtf8Error {
    fn description(&self) -> &str {
        self.err.description()
    }
    fn cause(&self) -> Option<&StdError> {
        Some(&self.err)
    }
}

/// A UTF-8 validation error.
///
/// This occurs when attempting to convert a `ByteRecord` into a
/// `StringRecord`.
///
/// The error includes the index of the error_len that failed validation, and the
/// last byte at which valid UTF-8 was verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Utf8Error {
    /// The lenght of the problematic value
    error_len: Option<usize>,
    /// The index into the given field up to which valid UTF-8 was verified.
    valid_up_to: usize,
}

/// Create a new UTF-8 error.
pub fn new_utf8_error(error_len: Option<usize>, valid_up_to: usize) -> Utf8Error {
    Utf8Error {
        error_len:   error_len,
        valid_up_to: valid_up_to,
    }
}

impl Utf8Error {
    /// The error_len index of a byte record in which UTF-8 validation failed.
    pub fn error_len(&self) -> Option<usize> {
        self.error_len
    }

    /// The index into the given field up to which valid UTF-8 was verified.
    pub fn valid_up_to(&self) -> usize {
        self.valid_up_to
    }
}

impl StdError for Utf8Error {
    fn description(&self) -> &str {
        "invalid utf-8 in CSV record"
    }
}

impl fmt::Display for Utf8Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "invalid utf-8: invalid UTF-8 character after byte index {}",
            self.valid_up_to
        )?;

        if let Some(l) = self.error_len {
            write!(f, "and remains invalid for {} bytes", l)?;
        }

        write!(f, "")
    }
}

impl From<AliasConflictError> for Error {
    fn from(err: AliasConflictError) -> Error {
        new_error(ErrorKind::AliasConflict {
            position: None,
            err:      err,
        })
    }
}

pub fn new_alias_conflict_error(existing: Position, new: Position) -> AliasConflictError {
    AliasConflictError {
        existing_position: existing,
        new_position:      new,
    }
}

#[derive(Debug)]
pub struct AliasConflictError {
    existing_position: Position,
    new_position:      Position,
}

impl StdError for AliasConflictError {
    fn description(&self) -> &str {
        "a new alias would conflicts with an already defined alias"
    }
}

impl fmt::Display for AliasConflictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.new_position {
            Position::Parsed(np) => {
                let alias = &np.alias;

                write!(
                    f,
                    "{} at character {} on {}",
                    alias.clone().expect("an alias conflict error should always supply an alias for the existing position"),
                    np.offset,
                    np.line,
                    )?;
            }
            _ => {
                panic!("AliasConflictError's can't have any new_position type other than Parsed");
            }
        }

        write!(f, "conflicts with existing alias")?;

        match &self.existing_position {
            Position::Parsed(ep) => {
                let alias = &ep.alias;

                write!(
                    f,
                    "{} at character {} on {} ",
                    alias.clone().expect("an alias conflict error should always supply an alias for the existing position"),
                    ep.offset,
                    ep.line,
                    )
            }
            _ => {
                panic!("AliasConflictError's can't have any new_position type other than Parsed");
            }
        }
    }
}

impl From<CompositionIncompleteError> for Error {
    fn from(err: CompositionIncompleteError) -> Error {
        new_error(ErrorKind::CompositionIncomplete {
            position: None,
            err:      err,
        })
    }
}

pub fn new_incomplete_composition_error(
    position: Position,
    composition: SqlComposition,
    notes: String,
) -> CompositionIncompleteError {
    CompositionIncompleteError {
        position,
        composition,
        notes,
    }
}

#[derive(Debug)]
pub struct CompositionIncompleteError {
    position:    Position,
    composition: SqlComposition,
    notes:       String,
}

impl StdError for CompositionIncompleteError {
    fn description(&self) -> &str {
        "a composition's end was found but the composition is incomplete"
    }
}

impl fmt::Display for CompositionIncompleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ends at {} without completing", self.position)
    }
}
