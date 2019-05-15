use quicli::prelude::*;
use structopt::StructOpt;

use sql_composer::composer::{ComposerBuilder, ComposerConnection};
use sql_composer::types::{value::Value, SqlComposition};
use std::collections::{BTreeMap, HashMap};

use sql_composer::parser::bind_value_named_set;
use sql_composer::types::{CompleteStr, Span};

use postgres::types as pg_types;
use postgres::types::ToSql as PgToSql;
use postgres::{Connection as PgConnection, TlsMode as PgTlsMode};

pub use rusqlite::types::{Null, ToSql as RusqliteToSql, ValueRef as RusqliteValueRef};
use rusqlite::Connection as RusqliteConnection;

use serde_postgres::de::Deserializer as PgDeserializer;

use serde_value::Value as SerdeValue;

use std::io;

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
../target/debug/sqlc query --uri sqlite://:memory: --path /vol/projects/sql-composer/sql-composer/src/tests/values/double-include.tql --bind "[a: ['a_value'], b: ['b_value'], c: ['c_value'], d: ['d_value'], e: ['e_value'], f: ['f_value']]" -vvv
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

    let mut builder = ComposerBuilder::default();

    builder.uri(&uri);

    let mut parsed_values: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    if let Some(b) = args.bind {
        let (_remaining, bvns) = bind_value_named_set(Span::new(CompleteStr(&b))).unwrap();

        parsed_values = bvns;
    }

    if uri.starts_with("sqlite://") {
        //TODO: base off of uri
        let conn = match uri.as_str() {
            "sqlite://:memory:" => RusqliteConnection::open_in_memory().unwrap(),
            _ => unimplemented!("not currently passing uri correctly"),
        };

        let values: BTreeMap<String, Vec<&RusqliteToSql>> =
            parsed_values
                .iter()
                .fold(BTreeMap::new(), |mut acc, (k, v)| {
                    let entry = acc.entry(k.to_string()).or_insert(vec![]);
                    *entry = v.iter().map(|x| x as &RusqliteToSql).collect();

                    acc
                });

        let (mut prep_stmt, bindings) =
            conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

        let driver_rows = prep_stmt
            .query_map(&bindings, |driver_row| {
                (0..driver_row.column_count()).fold(
                    Ok(vec![]),
                    |acc: Result<Vec<String>, rusqlite::Error>, i| {
                        if let Ok(mut acc) = acc {
                            let raw = driver_row.get_raw(i);

                            acc.push(
                                match raw {
                                    RusqliteValueRef::Null => "NULL".to_string(),
                                    RusqliteValueRef::Integer(int) => int.to_string(),
                                    RusqliteValueRef::Real(r) => r.to_string(),
                                    RusqliteValueRef::Text(t) => t.to_string(),
                                    RusqliteValueRef::Blob(vc) => {
                                        std::string::String::from_utf8(vc.to_vec()).unwrap()
                                    }
                                }
                                .into(),
                            );

                            Ok(acc)
                        }
                        else {
                            acc
                        }
                    },
                )
            })
            .unwrap();

        let mut rows = vec![];

        for driver_row in driver_rows {
            rows.push(driver_row);
        }

        for row in rows {
            println!("row: {:?}", row);
        }
    }
    else if uri.starts_with("postgres://") {
        let conn =
            PgConnection::connect("postgres://vagrant:vagrant@localhost:5432", PgTlsMode::None)
                .unwrap();

        let values: BTreeMap<String, Vec<&PgToSql>> =
            parsed_values
                .iter()
                .fold(BTreeMap::new(), |mut acc, (k, v)| {
                    let entry = acc.entry(k.to_string()).or_insert(vec![]);
                    *entry = v.iter().map(|x| x as &PgToSql).collect();

                    acc
                });

        let (mut prep_stmt, bindings) =
            conn.compose(&comp, values, vec![], HashMap::new()).unwrap();

        let mut values: Vec<Vec<String>> = vec![];

        let mut serializer = serde_json::Serializer::new(io::stdout());

        let driver_rows = &prep_stmt.query(&bindings).unwrap();

        for driver_row in driver_rows {
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

            serde_transcode::transcode(SerdeValue::Map(bt), &mut serializer).unwrap();
        }
    }
    else {
        panic!("unknown uri type: {}", uri);
    }

    Ok(())
}
