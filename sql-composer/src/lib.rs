/// `sql-composer` provides macros for creating and modifying sql statements
///
/// `sql-composer` extends standard SQL to allow composing statements together
/// and automating complex parameter substitution.
///
/// For parameter substitution, write SQL as normal, declaring placeholders
/// with the added keyword `:bind(var_name)`.  Calling :bind(var_name) tells the composer where to
/// add placeholders and what values are needed for the database driver to
/// prepare the SQL statement.
///
/// The output of a composer provides both the string of SQL with the appropriate bind parameters to hand to your SQL driver
/// as well as the bind parameters. This helps to abstract away the differences in bind parameter syntax
/// between mysql, postgresql and sqlite.
///
/// `:compose(pathbuf or string)`.
/// * `:bind(var_name)` ::  handles SQL named bind parameters
/// * `:compose(pathbuf or string)` :: composes a complete statement of SQL into the current SQL
///
/// * A composer can be reused for a single statement multiple times. You could call compose()
/// multiple times with different sets of values and get SQL that looks quite different due to the
/// difference in number of placeholders and size/shape of the bind values returned as well.

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate nom;
#[macro_use]
extern crate nom_locate;

#[macro_use]
pub mod composer;

#[cfg(all(feature = "dbd-postgres", feature="composer-serde"))]
#[macro_use]
extern crate postgres;

#[cfg(all(feature = "dbd-mysql-async"))]
#[macro_use]
extern crate mysql_async;
#[cfg(all(feature = "dbd-mysql-async"))]
extern crate tokio;


pub mod error;
pub mod parser;
mod tests;
pub mod types;
