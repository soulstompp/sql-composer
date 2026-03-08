# Contributing

Thanks for your interest in contributing to sql-composer! We very much look forward to
your suggestions, bug reports, and pull requests.

## Submitting bug reports

Have a look at our [issue tracker]. If you can't find an issue (open or closed)
describing your problem (or a very similar one) there, please open a new issue with
the following details:

- Which versions of Rust and sql-composer are you using?
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
   versions. sql-composer currently supports Rust Stable.

2. Install the system libraries needed to interface with the database systems
   you wish to use (e.g. `libpq-dev` for PostgreSQL, `libmysqlclient-dev` for MySQL).

3. Clone this repository and open it in your favorite editor.

4. The project uses a Cargo workspace under `crates/`:

   ```
   crates/
     sql-composer/           # Core library
     sql-composer-rusqlite/  # rusqlite driver
     sql-composer-duckdb/    # DuckDB driver
     sql-composer-postgres/  # PostgreSQL driver (sync + async)
     sql-composer-mysql/     # MySQL driver (sync + async)
     sql-composer-sqlx/      # sqlx integration
     cargo-sqlc/             # CLI tool
   ```

5. **Database setup for integration tests** (optional)

   If you need to run tests that require a live database, use docker-compose to
   launch PostgreSQL and MySQL containers:

   ```bash
   docker-compose up             # launch both
   docker-compose up postgres    # launch only PostgreSQL
   docker-compose up mysql       # launch only MySQL
   ```

   Override ports or passwords via environment variables or the `.env` file.
   Database storage is cached in `./db/` — delete to reinitialize:

   ```bash
   sudo rm -rf db/*
   ```

6. Run the tests:

   ```bash
   # Run all workspace tests (core + rusqlite + duckdb + driver unit tests)
   cargo test --workspace

   # Run tests for a specific crate
   cargo test -p sql-composer
   cargo test -p sql-composer-rusqlite
   cargo test -p sql-composer-duckdb
   cargo test -p sql-composer-postgres
   cargo test -p sql-composer-mysql
   cargo test -p sql-composer-sqlx --features validate
   cargo test -p cargo-sqlc
   ```

[rustup]: https://rustup.rs/

### Coding Style

We follow the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md), enforced using [rustfmt](https://github.com/rust-lang/rustfmt).
To run rustfmt tests locally:

1. Install rustfmt and clippy:
   ```
   rustup component add rustfmt
   rustup component add clippy
   ```

2. Run clippy:
   ```
   cargo clippy --workspace -- -D warnings
   ```
   Each PR needs to compile without warnings.

3. Run rustfmt:

   To check formatting:
   ```
   cargo fmt --all -- --check
   ```

   To apply formatting:
   ```
   cargo fmt --all
   ```

You can also use rustfmt to make corrections or highlight issues in your editor.
Check out [their README](https://github.com/rust-lang/rustfmt) for details.
