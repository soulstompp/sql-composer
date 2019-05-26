use quicli::prelude::*;
use structopt::StructOpt;

use sql_composer::types::{value::Value, SqlComposition};
use std::collections::{BTreeMap, HashMap};

use sql_composer::composer::ComposerConnection;
use sql_composer::parser::bind_value_named_set;
use sql_composer::types::{CompleteStr, Span};

use serde_value::Value as SerdeValue;

use std::io;

#[cfg(feature = "dbd-mysql")]
use mysql::prelude::ToValue as MySqlToSql;
#[cfg(feature = "dbd-mysql")]
use mysql::Pool;
#[cfg(feature = "dbd-mysql")]
use mysql::Value as MySqlValue;

#[cfg(feature = "dbd-postgres")]
use postgres::types as pg_types;
#[cfg(feature = "dbd-postgres")]
use postgres::types::ToSql as PgToSql;
#[cfg(feature = "dbd-postgres")]
use postgres::{Connection as PgConnection, TlsMode as PgTlsMode};

#[cfg(feature = "dbd-rusqlite")]
pub use rusqlite::types::{Null, ToSql as RusqliteToSql, ValueRef as RusqliteValueRef};
#[cfg(feature = "dbd-rusqlite")]
use rusqlite::Connection as RusqliteConnection;

#[derive(Debug, StructOpt)]
struct QueryArgs {
    #[structopt(flatten)]
    verbosity: Verbosity,
    /// Uri to the database
    #[structopt(long = "uri", short = "u")]
    uri: String,
    /// Path to the template
    #[structopt(long = "path", short = "p")]
    path: String,
    /// a comma seperated list of key:value pairs
    #[structopt(long = "bind", short = "b")]
    bind: Option<String>,
    /// values to use in place of a path, made up of a comma seperated list of [] containing key:value pairs
    #[structopt(long = "mock-path")]
    mock_path: Vec<String>,
    /// values to use in place of a table, made up of a comma seperated list of [] containing key:value pairs
    #[structopt(long = "mock-table")]
    mock_table: Vec<String>,
}

#[derive(Debug, StructOpt)]
struct ParseArgs {
    #[structopt(flatten)]
    verbosity: Verbosity,
    /// Uri to the database
    #[structopt(long = "uri", short = "u")]
    uri: String,
    /// Path to the template
    #[structopt(long = "path", short = "p")]
    path: String,
}

#[derive(Debug, StructOpt)]
enum Cli {
    #[structopt(name = "query")]
    Query(QueryArgs),
}

/*
../target/release/sqlc query --uri mysql://vagrant:password@localhost:3306/vagrant --path /vol/projects/sql-composer/src/tests/values/double-include.tql --bind "[a: ['a_value'], b: ['b_value'], c: ['c_value'], d: ['d_value'], e: ['e_value'], f: ['f_value']]" -vvv

../target/release/sqlc query --uri sqlite://:memory: --path /vol/projects/sql-composer/src/tests/values/double-include.tql --bind "[a: ['a_value'], b: ['b_value'], c: ['c_value'], d: ['d_value'], e: ['e_value'], f: ['f_value']]" -vvv

../target/release/sqlc query --uri postgres://vagrant:vagrant@localhost:5432 --path /vol/projects/sql-composer/src/tests/values/double-include.tql --bind "[a: ['a_value'], b: ['b_value'], c: ['c_value'], d: ['d_value'], e: ['e_value'], f: ['f_value']]" -vvv
*/
fn main() -> CliResult {
    let args = Cli::from_args();

    match args {
        Cli::Query(r) => query(r),
    }
}

fn setup(verbosity: Verbosity) -> CliResult {
    verbosity
        .setup_env_logger(&env!("CARGO_PKG_NAME"))
        .expect("unable to setup evn_logger");

    Ok(())
}

fn parse(args: QueryArgs) -> CliResult {
    setup(args.verbosity)?;

    Ok(())
}

fn query(args: QueryArgs) -> CliResult {
    setup(args.verbosity)?;

    let parsed_comp = SqlComposition::from_path_name(&args.path).unwrap();
    let comp = parsed_comp.item;

    let uri = args.uri;

    let mut parsed_values: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    if let Some(b) = args.bind {
        let (_remaining, bvns) = bind_value_named_set(Span::new(CompleteStr(&b))).unwrap();

        parsed_values = bvns;
    }

    if uri.starts_with("mysql://") {
        if cfg!(feature = "dbd-mysql") == false {
            panic!("cli not built with dbd-mysql feature");
        }

        #[cfg(feature = "dbd-mysql")]
        query_mysql(uri, comp, parsed_values)?;
    }
    else if uri.starts_with("postgres://") {
        if cfg!(feature = "dbd-postgres") == false {
            panic!("cli not built with dbd-postgres feature");
        }

        #[cfg(feature = "dbd-postgres")]
        query_postgres(uri, comp, parsed_values)?;
    }
    else if uri.starts_with("sqlite://") {
        if cfg!(feature = "dbd-rusqlite") == false {
            panic!("cli not built with dbd-rusqlite feature");
        }

        #[cfg(feature = "dbd-rusqlite")]
        query_rusqlite(uri, comp, parsed_values)?;
    }
    else {
        panic!("unknown uri type: {}", uri);
    }

    Ok(())
}

#[cfg(feature = "dbd-mysql")]
fn query_mysql(
    uri: String,
    comp: SqlComposition,
    params: BTreeMap<String, Vec<Value>>,
) -> CliResult {
    let pool = Pool::new(uri).unwrap();

    let values: BTreeMap<String, Vec<&MySqlToSql>> =
        params.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);
            *entry = v.iter().map(|x| x as &MySqlToSql).collect();

            acc
        });

    let (mut prep_stmt, bindings) = pool.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let mut values: Vec<Vec<String>> = vec![];

    let driver_rows = prep_stmt.execute(bindings.as_slice()).unwrap();

    let vv = driver_rows
        .into_iter()
        .fold(vec![], |mut value_maps, driver_row| {
            let driver_row = driver_row.unwrap();

            let bt: BTreeMap<SerdeValue, SerdeValue> = driver_row
                .columns_ref()
                .iter()
                .enumerate()
                .fold(BTreeMap::new(), |mut acc, (i, column)| {
                    let v = match driver_row.as_ref(i) {
                        Some(MySqlValue::NULL) => SerdeValue::Unit,
                        Some(MySqlValue::Bytes(b)) => {
                            SerdeValue::String(String::from_utf8(b.to_vec()).unwrap())
                        }
                        Some(MySqlValue::Int(i)) => SerdeValue::I64(*i),
                        Some(MySqlValue::UInt(u)) => SerdeValue::U64(*u),
                        Some(MySqlValue::Float(f)) => SerdeValue::F64(*f),
                        Some(MySqlValue::Date(
                            year,
                            month,
                            day,
                            hour,
                            minutes,
                            seconds,
                            micro_seconds,
                        )) => SerdeValue::Seq(vec![
                            SerdeValue::U16(*year),
                            SerdeValue::U8(*month),
                            SerdeValue::U8(*day),
                            SerdeValue::U8(*hour),
                            SerdeValue::U8(*minutes),
                            SerdeValue::U8(*seconds),
                            SerdeValue::U32(*micro_seconds),
                        ]),
                        Some(MySqlValue::Time(
                            is_negative,
                            days,
                            hours,
                            minutes,
                            seconds,
                            micro_seconds,
                        )) => SerdeValue::Seq(vec![
                            SerdeValue::Bool(*is_negative),
                            SerdeValue::U32(*days),
                            SerdeValue::U8(*hours),
                            SerdeValue::U8(*minutes),
                            SerdeValue::U8(*seconds),
                            SerdeValue::U32(*micro_seconds),
                        ]),
                        None => unreachable!("A none value isn't right"),
                        _ => unreachable!("unmatched mysql value"),
                    };

                    let _ = acc
                        .entry(SerdeValue::String(column.name_str().to_string()))
                        .or_insert(v);

                    acc
                });

            value_maps.push(SerdeValue::Map(bt));
            value_maps
        });

    output(SerdeValue::Seq(vv));

    Ok(())
}

#[cfg(feature = "dbd-postgres")]
fn query_postgres(
    uri: String,
    comp: SqlComposition,
    params: BTreeMap<String, Vec<Value>>,
) -> CliResult {
    let conn = PgConnection::connect("postgres://vagrant:vagrant@localhost:5432", PgTlsMode::None)
        .unwrap();

    let values: BTreeMap<String, Vec<&PgToSql>> =
        params.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);
            *entry = v.iter().map(|x| x as &PgToSql).collect();

            acc
        });

    let (mut prep_stmt, bindings) = conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let mut values: Vec<Vec<String>> = vec![];

    let driver_rows = &prep_stmt.query(&bindings).unwrap();

    let vv = driver_rows
        .iter()
        .fold(vec![], |mut value_maps, driver_row| {
            let bt: BTreeMap<SerdeValue, SerdeValue> = driver_row
                .columns()
                .iter()
                .enumerate()
                .fold(BTreeMap::new(), |mut acc, (i, column)| {
                    let v = match *column.type_() {
                        pg_types::BOOL => SerdeValue::Bool(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::CHAR => SerdeValue::I8(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT2 => SerdeValue::I16(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT4 => SerdeValue::I32(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::OID => SerdeValue::U32(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT8 => SerdeValue::I64(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::VARCHAR | pg_types::TEXT | pg_types::NAME => {
                            SerdeValue::String(driver_row.get_opt(i).unwrap().unwrap())
                        }
                        pg_types::FLOAT4 => {
                            SerdeValue::F32(driver_row.get_opt(i).unwrap().unwrap())
                        }
                        pg_types::FLOAT8 => {
                            SerdeValue::F64(driver_row.get_opt(i).unwrap().unwrap())
                        }
                        _ => unreachable!("shouldn't get here!"),
                    };

                    let _ = acc
                        .entry(SerdeValue::String(column.name().to_string()))
                        .or_insert(v);

                    acc
                });

            value_maps.push(SerdeValue::Map(bt));
            value_maps
        });

    output(SerdeValue::Seq(vv));

    Ok(())
}

#[cfg(feature = "dbd-rusqlite")]
fn query_rusqlite(
    uri: String,
    comp: SqlComposition,
    params: BTreeMap<String, Vec<Value>>,
) -> CliResult {
    //TODO: base off of uri
    let conn = match uri.as_str() {
        "sqlite://:memory:" => RusqliteConnection::open_in_memory().unwrap(),
        _ => unimplemented!("not currently passing uri correctly"),
    };

    let values: BTreeMap<String, Vec<&RusqliteToSql>> =
        params.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);
            *entry = v.iter().map(|x| x as &RusqliteToSql).collect();

            acc
        });

    let (mut prep_stmt, bindings) = conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let column_names: Vec<String> = prep_stmt
        .column_names()
        .into_iter()
        .map(String::from)
        .collect();

    let driver_rows = prep_stmt
        .query_map(&bindings, |driver_row| {
            let map = column_names.iter().enumerate().fold(
                BTreeMap::new(),
                |mut acc, (i, column_name)| {
                    let _ = acc
                        .entry(SerdeValue::String(column_name.to_string()))
                        .or_insert(match driver_row.get_raw(i) {
                            RusqliteValueRef::Null => SerdeValue::Unit,
                            RusqliteValueRef::Integer(int) => SerdeValue::I64(int),
                            RusqliteValueRef::Real(r) => SerdeValue::F64(r),
                            RusqliteValueRef::Text(t) => SerdeValue::String(t.to_string()),
                            RusqliteValueRef::Blob(vc) => {
                                let s = std::string::String::from_utf8(vc.to_vec()).unwrap();

                                SerdeValue::String(s)
                            }
                        });

                    acc
                },
            );

            Ok(map)
        })
        .unwrap();

    let mut seq = vec![];

    for driver_row in driver_rows {
        seq.push(SerdeValue::Map(driver_row.unwrap()));
    }

    output(SerdeValue::Seq(seq));

    Ok(())
}

fn output(v: SerdeValue) {
    let mut serializer = serde_json::Serializer::new(io::stdout());
    serde_transcode::transcode(v, &mut serializer).unwrap();
}
