use quicli::prelude::*;
use structopt::StructOpt;


// use sql_composer::composer::rusqlite::RusqliteComposer;
use sql_composer::types::{SqlComposition};
use sql_composer::composer::{Composer, ComposerBuilder, ComposerDriver};

#[derive(Debug, StructOpt)]
struct QueryArgs {
    #[structopt(flatten)]
    verbosity: Verbosity,
    /// Uri to the database
    #[structopt(long="uri", short="u")]
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
    #[structopt(long="uri", short="u")]
    uri: String,
    /// Path to the template
    #[structopt(long = "path", short = "p")]
    path: String,
}

#[derive(Debug, StructOpt)]
enum Cli {
    #[structopt(name = "query")]
    Query(QueryArgs)
}

/*
target/debug/sqlc fetch --uri sqlite://:memory: --path /vol/projects/sql-composer/sql-composer/src/tests/values/double-include.tql --bind a:a_value;b:b_value;c:c_value;d:d_value
--mock-table table [a:a_value;b:b_value;c:c_value;d:d_value], [a:a_value;b:b_value;c:c_value;d:d_value]
--mock-path ./path.tmpl [a:a_value;b:b_value;c:c_value;d:d_value], [a:a_value;b:b_value;c:c_value;d:d_value]
*/
fn main() -> CliResult {
    let args = Cli::from_args();

    match args {
        Cli::Query(r) => {
            query(r)
        },
    }
}

fn setup(verbosity: Verbosity) -> CliResult {
    verbosity.setup_env_logger(&env!("CARGO_PKG_NAME")).expect("unable to setup evn_logger");

    Ok(())
}

fn parse(args: QueryArgs) -> CliResult {
    setup(args.verbosity)?;

    let comp = SqlComposition::from_str(&args.path);

    println!("{:?}", comp);

    Ok(())
}

fn query(args: QueryArgs) -> CliResult {
    setup(args.verbosity)?;

    println!("args.bind: {:?}", args.bind);
    println!("path: {}", args.path);
    let comp = SqlComposition::from_path_name(&args.path).unwrap();

    println!("comp: {:?}", comp);

    let uri = args.uri;

    let mut builder = ComposerBuilder::default();

    builder.uri(&uri);

    if let Some(b) = args.bind {
        println!("got bind args");
        builder.bind_named_set(&b).expect(&format!("unable to bind named set: {}", b));
    }

    let driver = builder.build().expect("unable to build composer");

    match driver {
        ComposerDriver::Rusqlite(c) =>{
            let rows = c.query(&comp.item).expect("could run query");


            for row in rows.rows() {
                println!("row: {:?}", row);
            }
        },
        _ => panic!("unsupported driver")
    };


    Ok(())
}
