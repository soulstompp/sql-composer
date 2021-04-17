# Contributing

Thanks for your interest in contributing to sql-composer! We very much look forward to
your suggestions, bug reports, and pull requests.

## Submitting bug reports

Have a look at our [issue tracker]. If you can't find an issue (open or closed)
describing your problem (or a very similar one) there, please open a new issue with
the following details:

- Which versions of Rust and sql-composer are you using?
- Which feature flags are you using?
- What are you trying to accomplish?
- What is the full error you are seeing?
- How can we reproduce this?
  - Please quote as much of your code as needed to reproduce (best link to a
    public repository or [Gist])
  - Please post as much of your database schema as is relevant to your error

[issue tracker]: https://github.com/soulstompp/sql-composer/issues
[Gist]: https://gist.github.com

Thank you! We'll try to respond as quickly as possible.


## Submitting feature requests

If you can't find an issue (open or closed) describing your idea on our [issue
tracker], open an issue. Adding answers to the following
questions in your description is +1:

- What do you want to do, and how do you expect sql-composer to support you with that?
- How might this be added to sql-composer?
- What are possible alternatives?
- Are there any disadvantages?

Thank you! We'll try to respond as quickly as possible.


## Contribute code to sql-composer

### Setting up sql-composer locally

1. Install Rust using [rustup], which allows you to easily switch between Rust
   versions. sql-composer currently supports Rust Stable, Nightly, Rust Beta.

2. Install the system libraries needed to interface with the database systems
   you wish to use.

   These are the same as when compiling sql-composer. It's generally a good idea
   to install _all_ drivers so you can run all tests locally.

3. Clone this repository and open it in your favorite editor.

4. Local override of database ports or passwords

   If you wish to change the local database ports, override PG_DATABASE_PORT
   and MYSQL_DATABASE_PORT via environment variables or edit the values in the
   `.env` file.

   Use MYSQL_DATABASE_PASS and PG_DATABASE_PASS to override the default
   passwords.  If the containers have already been run, changing the password
   will not affect the container.  Shut down the container and delete the 
   db/* files to clear the old password values.

   ```bash
   # delete database storage files to force re-initialization of database
   sudo rm -rf db/*
   ```
5. Use docker-compose to launch containers with databases for testing.

   `docker-compose up` will download and run containers for mysql and
   postgresql.  The internal database ports (3306 or 5432) will be
   forwarded to ports on the host as determined by MYSQL_DATABASE_PORT
   and PG_DATABASE_PORT environment variables.  Default port values are
   defined in the `.env` file in the root of the crate.

   The containers will be named `sql_composer_mysql` and `sql_composer_postgres`.
   Each will have an empty database named `sql_composer` owned by user `runner`

   The containers will run in the foreground until interrupted with `ctrl-c`.

   ```bash
   docker-compose up             # launch both postgresql and mysql
   docker-compose up mysql       # launch only mysql
   docker-compose up postgresql  # launch only postgresql
   ```

   db storage files will be cached in the `./db/` directory.  These files
   can be deleted to force the database to re-initialize on the next run of
   `docker-compose up`

6. Now, try running the test suite to confirm everything works for you locally
   by executing `cargo test`. (Initially, this will take a while to compile
   everything.)  Each database is hidden behind a feature flag.

   Run a test against all the databases with the `--all-features` flag

   Individual features are not supported at the top level workspace, they can
   only be enabled when building within the subprojects.
     See: https://github.com/rust-lang/cargo/issues/4942

   The features are listed in the `[features]` section of `Cargo.toml` of 
   the individual projects, as well as any `optional` dependencies.

   sql-composer supports 4 feature flags:
   * `mysql`
   * `rusqlite`
   * `postgres`
   * `composer-serde`

   Other crates reference these as default required features, referenced via sql-composer:
   `sql-composer/mysql`

   sql-composer-mysql:
   * `sql-composer/mysql` (default)
   * `composer-serde`

   sql-composer-postgres:
   * `sql-composer/postgres` (default)
   * `composer-serde`

   sql-composer-rusqlite:
   * `sql-composer/rusqlite` (default)
   * `composer-serde`

   sql-composer-direct:
   * `composer-serde`

   sql-composer-serde has no feature flags.

   sql-composer-cli defines dbd-* feature flags to enable a db type and the associated serde:
   * `dbd-mysql`
   * `dbd-rusqlite`
   * `dbd-postgres`


   ```bash
   cargo test                          # run non-db and sqlite tests only
   cargo test --all-features           # compile for all dbs and run all tests.

   # individual features are only supported in the sub-projects
   # sql-composer:
   (cd sql-composer && cargo test --features mysql)      # compile for mysql and run mysql tests.
   (cd sql-composer && cargo test --features postgres)   # compile for postgresql and run postgresql tests.
   (cd sql-composer && cargo test --features rusqlite)   # compile for rusqlite and run rusqlite tests.
   (cd sql-composer && cargo test --features composer-serde) # compile for serde and run serde tests.

   # multiple features are passed as one argument, separated by comma or spaces.  Use quotes if separating by spaces
   (cd sql-composer && cargo test --features "composer-serde mysql") # compile for serde and mysql
   (cd sql-composer && cargo test --features composer-serde,mysql)   # compile for serde and mysql

   # sql-composer-cli:
   (cd sql-composer-cli && cargo test --features dbd-mysql)      # compile for mysql and run mysql tests.
   (cd sql-composer-cli && cargo test --features dbd-postgres)   # compile for postgresql and run postgresql tests.
   (cd sql-composer-cli && cargo test --features dbd-rusqlite)   # compile for rusqlite and run rusqlite tests.

   # integration tests for sql-composer-cli against database instances
   docker-compose up                      # launch postgres and mysql databases configured by .env file
   make -C sql-composer-cli mysql         # integration test against mysql database
   make -C sql-composer-cli rusqlite      # integration test against sqlite database
   make -C sql-composer-cli postgres      # integration test against postgresql database
   make -C sql-composer-cli all           # integration tests against  sqli, mysql, and postgres database instances
   ```

[rustup]: https://rustup.rs/

### Coding Style

We follow the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md), enforced using [rustfmt](https://github.com/rust-lang/rustfmt).
To run rustfmt tests locally:

1. Use rustup to set rust toolchain to the version specified in the
   [rust-toolchain file](./rust-toolchain).

2. Install the rustfmt and clippy by running
   ```
   rustup component add rustfmt-preview
   rustup component add clippy-preview
   ```

3. Run clippy using cargo from the root of your sql-composer repo.
   ```
   cargo clippy
   ```
   Each PR needs to compile without warning.

4. Run rustfmt using cargo from the root of your sql-composer repo.

   To see changes that need to be made, run

   ```
   cargo fmt --all -- --check
   ```

   If all code is properly formatted (e.g. if you have not made any changes),
   this should run without error or output.
   If your code needs to be reformatted,
   you will see a diff between your code and properly formatted code.
   If you see code here that you didn't make any changes to
   then you are probably running the wrong version of rustfmt.
   Once you are ready to apply the formatting changes, run

   ```
   cargo fmt --all
   ```

   You won't see any output, but all your files will be corrected.

You can also use rustfmt to make corrections or highlight issues in your editor.
Check out [their README](https://github.com/rust-lang/rustfmt) for details.
