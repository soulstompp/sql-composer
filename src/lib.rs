/// `sql-composer` provides macros for creating and modifying sql statements
///
/// `sql-composer` extends standard SQL to allow composing statements together
/// and automating complex parameter substitution.
///
/// For parameter substitution, write SQL as normal, declaring placeholders
/// with the added keyword `:bind(var_name)`.  Set values for each binding
/// and then expand, composer will return the SQL string with placeholders
/// inserted for your dialect of SQL.
///
/// The output of sql-composer provides both the string of SQL to hand to your SQL driver
/// as well as the bind parameters, abstracting away the differences in bind syntax
/// between mysql, postgresql and sqlite.
///
/// `:expand(pathbuf or string)`.  
/// * `:bind(var_name)` ::  handles SQL named parameters
/// * `:expand(pathbuf or string)` :: pulls in one full statement of SQL, not just a snippet.
///
/// * each expander is like a "prepare statement" call.  Because you could set values
/// and expand a stmt, then change values and run the stmt through again.


#[macro_use]
extern crate nom;
extern crate chrono;
extern crate time;

extern crate mysql;
extern crate postgres;
extern crate rusqlite;

mod composer;
mod error;
mod parser;
mod types;
