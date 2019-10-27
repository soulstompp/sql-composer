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
4. Use docker-compose to launch containers with databases for testing.

   docker-compose will download and run containers for mysql and
   postgresql.  The internal database ports (3306 or 5432) will be
   forwarded to ephemeral ports on the host.

   The containers will be named `sql_composer_mysql` and `sql_composer_postgres`.
   Each will have an empty database named `sql_composer` owned by user `runner`

   The containers will run in the foreground until interrupted with `ctrl-c`.

   ```bash
   docker-compose up             # launch both postgresql and mysql
   docker-compose up mysql       # launch only mysql
   docker-compose up postgresql  # launch only postgresql
   ```

5. use `docker port` to find the local port mapped into the database containers.

   In another shell get the forwarded ports from the docker containers.

    ```bash
    docker port sql_composer_mysql 3306       # e.g. 0.0.0.0:32000
    docker port sql_composer_postgresql 5432  # e.g. 0.0.0.0:32001
    ```

   env.database.sh can be sourced to export environent variables
   `$MYSQL_DATABASE_URL` and `$PG_DATABASE_URL` into your shell.

   ```bash
   . ./env.database.sh
   ```

6. Create a `.env` file in this directory, and add the connection details for
   your databases.

7. Now, try running the test suite to confirm everything works for you locally
   by executing `cargo test`. (Initially, this will take a while to compile
   everything.)

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
