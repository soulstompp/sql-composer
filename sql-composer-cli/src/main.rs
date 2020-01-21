use quicli::prelude::{CliResult, Verbosity};
use structopt::StructOpt;

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use sql_composer_serde::bind_value_named_set;

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use sql_composer::types::{ParsedSqlComposition, Span, SqlComposition};

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use std::collections::{BTreeMap, HashMap};

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use std::convert::TryFrom;

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use serde_value::Value;

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
use std::io;

#[cfg(feature = "dbd-mysql")]
use mysql::{prelude::ToValue as MySqlToSql, Pool};
#[cfg(feature = "dbd-mysql")]
use sql_composer_mysql::{ComposerConnection as MysqlComposerConnection,
                         SerdeValue as MySqlSerdeValue, Value as MySqlValue};

#[cfg(feature = "dbd-postgres")]
use postgres::types as pg_types;
#[cfg(feature = "dbd-postgres")]
use postgres::types::ToSql as PgToSql;
#[cfg(feature = "dbd-postgres")]
use sql_composer_postgres::{ComposerConnection as PgComposerConnection,
                            Connection as PgConnection, SerdeValue as PgSerdeValue,
                            TlsMode as PgTlsMode};

#[cfg(feature = "dbd-rusqlite")]
pub use rusqlite::types::{Null, ToSql as RusqliteToSql, ValueRef as RusqliteValueRef};
#[cfg(feature = "dbd-rusqlite")]
use sql_composer_rusqlite::{ComposerConnection as RusqliteComposerConnection,
                            Connection as RusqliteConnection, SerdeValue as RusqliteSerdeValue};
#[cfg(feature = "dbd-rusqlite")]
use std::path::Path;

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

fn query(args: QueryArgs) -> CliResult {
    setup(args.verbosity)?;

    #[cfg(any(
        feature = "dbd-mysql",
        feature = "dbd-postgres",
        feature = "dbd-rusqlite"
    ))]
    let comp = ParsedSqlComposition::try_from(args.path).unwrap().item;

    let uri = args.uri;

    if uri.starts_with("mysql://") {
        if cfg!(feature = "dbd-mysql") == false {
            panic!("cli not built with dbd-mysql feature");
        }

        #[cfg(feature = "dbd-mysql")]
        query_mysql(uri, comp, args.bind)?;
    }
    else if uri.starts_with("postgres://") {
        if cfg!(feature = "dbd-postgres") == false {
            panic!("cli not built with dbd-postgres feature");
        }

        #[cfg(feature = "dbd-postgres")]
        query_postgres(uri, comp, args.bind)?;
    }
    else if uri.starts_with("sqlite://") {
        if cfg!(feature = "dbd-rusqlite") == false {
            panic!("cli not built with dbd-rusqlite feature");
        }

        #[cfg(feature = "dbd-rusqlite")]
        query_rusqlite(uri, comp, args.bind)?;
    }
    else {
        panic!("unknown uri type: {}", uri);
    }

    Ok(())
}

#[cfg(feature = "dbd-mysql")]
fn query_mysql(uri: String, comp: SqlComposition, bindings: Option<String>) -> CliResult {
    let pool = Pool::new(uri)?;

    let mut parsed_values: BTreeMap<String, Vec<MySqlSerdeValue>> = BTreeMap::new();

    if let Some(b) = bindings {
        let (_remaining, bvns) = bind_value_named_set(Span::new(&b)).unwrap();

        parsed_values = bvns.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);

            *entry = v.iter().map(|x| MySqlSerdeValue(x.clone())).collect();

            acc
        });
    }

    let values: BTreeMap<String, Vec<&dyn MySqlToSql>> =
        parsed_values
            .iter()
            .fold(BTreeMap::new(), |mut acc, (k, v)| {
                let entry = acc.entry(k.to_string()).or_insert(vec![]);

                *entry = v.iter().map(|x| x as &dyn MySqlToSql).collect();

                acc
            });

    let (mut prep_stmt, bindings) = pool.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let driver_rows = prep_stmt.execute(bindings.as_slice())?;

    let vv = driver_rows
        .into_iter()
        .fold(vec![], |mut value_maps, driver_row| {
            let driver_row = driver_row.unwrap();

            let bt: BTreeMap<Value, Value> = driver_row.columns_ref().iter().enumerate().fold(
                BTreeMap::new(),
                |mut acc, (i, column)| {
                    let v = match driver_row.as_ref(i) {
                        Some(MySqlValue::NULL) => Value::Unit,
                        Some(MySqlValue::Bytes(b)) => {
                            Value::String(String::from_utf8(b.to_vec()).unwrap())
                        }
                        Some(MySqlValue::Int(i)) => Value::I64(*i),
                        Some(MySqlValue::UInt(u)) => Value::U64(*u),
                        Some(MySqlValue::Float(f)) => Value::F64(*f),
                        Some(MySqlValue::Date(
                            year,
                            month,
                            day,
                            hour,
                            minutes,
                            seconds,
                            micro_seconds,
                        )) => Value::Seq(vec![
                            Value::U16(*year),
                            Value::U8(*month),
                            Value::U8(*day),
                            Value::U8(*hour),
                            Value::U8(*minutes),
                            Value::U8(*seconds),
                            Value::U32(*micro_seconds),
                        ]),
                        Some(MySqlValue::Time(
                            is_negative,
                            days,
                            hours,
                            minutes,
                            seconds,
                            micro_seconds,
                        )) => Value::Seq(vec![
                            Value::Bool(*is_negative),
                            Value::U32(*days),
                            Value::U8(*hours),
                            Value::U8(*minutes),
                            Value::U8(*seconds),
                            Value::U32(*micro_seconds),
                        ]),
                        None => unreachable!("A none value isn't right"),
                    };

                    let _ = acc
                        .entry(Value::String(column.name_str().to_string()))
                        .or_insert(v);

                    acc
                },
            );

            value_maps.push(Value::Map(bt));
            value_maps
        });

    output(Value::Seq(vv));

    Ok(())
}

#[cfg(feature = "dbd-postgres")]
fn query_postgres(uri: String, comp: SqlComposition, bindings: Option<String>) -> CliResult {
    let conn = PgConnection::connect(uri, PgTlsMode::None)?;

    let mut parsed_values: BTreeMap<String, Vec<PgSerdeValue>> = BTreeMap::new();

    if let Some(b) = bindings {
        let (_remaining, bvns) = bind_value_named_set(Span::new(&b)).unwrap();

        parsed_values = bvns.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);

            *entry = v.iter().map(|x| PgSerdeValue(x.clone())).collect();

            acc
        });
    }

    let values: BTreeMap<String, Vec<&dyn PgToSql>> =
        parsed_values
            .iter()
            .fold(BTreeMap::new(), |mut acc, (k, v)| {
                let entry = acc.entry(k.to_string()).or_insert(vec![]);
                *entry = v.iter().map(|x| x as &dyn PgToSql).collect();

                acc
            });

    let (prep_stmt, bindings) = conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let driver_rows = &prep_stmt.query(&bindings)?;

    let vv = driver_rows
        .iter()
        .fold(vec![], |mut value_maps, driver_row| {
            let bt: BTreeMap<Value, Value> = driver_row.columns().iter().enumerate().fold(
                BTreeMap::new(),
                |mut acc, (i, column)| {
                    let v = match *column.type_() {
                        pg_types::BOOL => Value::Bool(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::CHAR => Value::I8(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT2 => Value::I16(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT4 => Value::I32(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::OID => Value::U32(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::INT8 => Value::I64(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::VARCHAR | pg_types::TEXT | pg_types::NAME => {
                            Value::String(driver_row.get_opt(i).unwrap().unwrap())
                        }
                        pg_types::FLOAT4 => Value::F32(driver_row.get_opt(i).unwrap().unwrap()),
                        pg_types::FLOAT8 => Value::F64(driver_row.get_opt(i).unwrap().unwrap()),
                        _ => unreachable!("shouldn't get here!"),
                    };

                    let _ = acc
                        .entry(Value::String(column.name().to_string()))
                        .or_insert(v);

                    acc
                },
            );

            value_maps.push(Value::Map(bt));
            value_maps
        });

    output(Value::Seq(vv));

    Ok(())
}

#[cfg(feature = "dbd-rusqlite")]
fn query_rusqlite(uri: String, comp: SqlComposition, bindings: Option<String>) -> CliResult {
    //TODO: base off of uri
    let conn = match uri.as_str() {
        "sqlite://:memory:" => RusqliteConnection::open_in_memory()?,
        u @ _ => {
            if u.starts_with("sqlite://") {
                let (_, path_str) = u.split_at(9);

                RusqliteConnection::open(Path::new(path_str))?
            }
            else {
                unreachable!("query_rusqlite called with the wrong type of uri");
            }
        }
    };

    let mut parsed_values: BTreeMap<String, Vec<RusqliteSerdeValue>> = BTreeMap::new();

    if let Some(b) = bindings {
        let (_remaining, bvns) = bind_value_named_set(Span::new(&b)).unwrap();

        parsed_values = bvns.iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let entry = acc.entry(k.to_string()).or_insert(vec![]);

            *entry = v.iter().map(|x| RusqliteSerdeValue(x.clone())).collect();

            acc
        });
    }

    let values: BTreeMap<String, Vec<&dyn RusqliteToSql>> =
        parsed_values
            .iter()
            .fold(BTreeMap::new(), |mut acc, (k, v)| {
                let entry = acc.entry(k.to_string()).or_insert(vec![]);
                *entry = v.iter().map(|x| x as &dyn RusqliteToSql).collect();

                acc
            });

    let (mut prep_stmt, bindings) = conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

    let column_names: Vec<String> = prep_stmt
        .column_names()
        .into_iter()
        .map(String::from)
        .collect();

    let driver_rows = prep_stmt.query_map(&bindings, |driver_row| {
        let map =
            column_names
                .iter()
                .enumerate()
                .fold(BTreeMap::new(), |mut acc, (i, column_name)| {
                    let _ = acc.entry(Value::String(column_name.to_string())).or_insert(
                        match driver_row.get_raw(i) {
                            RusqliteValueRef::Null => Value::Unit,
                            RusqliteValueRef::Integer(int) => Value::I64(int),
                            RusqliteValueRef::Real(r) => Value::F64(r),
                            RusqliteValueRef::Text(t) => Value::String(t.to_string()),
                            RusqliteValueRef::Blob(vc) => {
                                let s = std::string::String::from_utf8(vc.to_vec()).unwrap();

                                Value::String(s)
                            }
                        },
                    );

                    acc
                });

        Ok(map)
    })?;

    let mut seq = vec![];

    for driver_row in driver_rows {
        seq.push(Value::Map(driver_row?));
    }

    output(Value::Seq(seq));

    Ok(())
}

#[cfg(any(
    feature = "dbd-mysql",
    feature = "dbd-postgres",
    feature = "dbd-rusqlite"
))]
fn output(v: Value) {
    let mut serializer = serde_json::Serializer::new(io::stdout());
    serde_transcode::transcode(v, &mut serializer).unwrap();
}
