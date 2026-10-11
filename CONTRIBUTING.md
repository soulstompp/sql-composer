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

4. The project uses a Cargo workspace of the crates under `crates/` and the examples under
   `examples/`:

   ```
   crates/
     sql-composer/           # Core library
     sql-composer-rusqlite/  # rusqlite driver
     sql-composer-duckdb/    # DuckDB driver
     sql-composer-postgres/  # PostgreSQL driver (sync + async)
     sql-composer-mysql/     # MySQL driver (sync + async)
     sql-composer-sqlx/      # sqlx integration
     sql-composer-ffi/       # C ABI (sql_composer.h)
     sql-composer-py/        # Python module (sql_composer)
     cargo-sqlc/             # CLI tool
   examples/
     brick/                  # Runnable example on sqlc-brickgen's database (brick-example)
     sqlc-brickgen/          # Large catalogue generator for PostgreSQL (sqlc-brickgen)
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
   # Run all workspace tests (core + rusqlite + duckdb + driver unit tests + examples)
   cargo test --workspace

   # Run tests for a specific crate
   cargo test -p sql-composer
   cargo test -p sql-composer-rusqlite
   cargo test -p sql-composer-duckdb
   cargo test -p sql-composer-postgres
   cargo test -p sql-composer-mysql
   cargo test -p sql-composer-sqlx --features validate
   cargo test -p sql-composer-ffi
   cargo test -p sql-composer-py   # embeds Python, so it links to libpython
   cargo test -p cargo-sqlc

   # The catalogue generator's tests, which need no database
   cargo test -p sqlc-brickgen

   # Check that the brick example's committed .sql files are what its templates compose to
   cargo run -p cargo-sqlc -- sqlc compose --source examples/brick/sqlc --target examples/brick/.sql --verify
   ```

   After changing a template under `examples/brick/sqlc`, compose the example again (the same
   command without `--verify`, with `--skip-prepare`) and commit the `.sql` files with it.

[rustup]: https://rustup.rs/

### Adding an identity

An identity writes a template another way that returns the same rows, read from the template's
text alone and from nothing a catalog holds. Each is a row of the table below: the axis it varies,
its two spellings, where it holds, its trap (the near-identity, a spelling close to it that
returns other rows, and the rows that tell them apart), and its law, the test that runs both on
those rows. The first five are the axes of `cargo sqlc spellings` and `cargo sqlc spell`, in
`crates/sql-composer/src/spellings/`; the last is the respelling `cargo sqlc compose --respell`
writes, in `crates/sql-composer/src/respell.rs`.

| axis, and its key | one spelling, and the other | it holds where | its trap | its law |
|---|---|---|---|---|
| where a condition sits, `conditions` | in an inner join's `ON`, or in the `WHERE` | the join, and every join after it in its `FROM` item, is inner; every column is named with its relation; the condition holds no query | the condition moved into a `LEFT JOIN`'s `ON`: the outer rows come back | `conditions.rs`: `law_a_condition_in_an_inner_join_s_on_or_in_the_where_returns_the_same_rows` |
| negation, `negation` | `NOT` over an `AND` or an `OR`, or pushed onto each operand; `NOT NOT p` as `p` | always | `NOT (p AND q)` as `NOT p AND NOT q`; `NOT IN` as `NOT EXISTS`, over a subquery that holds a NULL | `negation.rs`: `law_each_spelling_of_a_not_returns_the_same_rows_on_every_pair_of_truths` |
| join order, `join_order` | a join list's relations in another order, each condition at the first `ON` where every relation it names is joined | every join inner or `CROSS`, none natural; every column named with its relation; no bare `*`; at most 8 relations; a `USING` column held once by the relations joined before it, read from their select lists | `SELECT *`: the columns come back in another order; an unqualified `USING` column, which reads the relation joined first: numeric's `1.0` against `1.00` | `join_order.rs`: `law_every_order_of_inner_joins_returns_the_same_rows` |
| how a relation read once is named, `naming` | inline, as a WITH query at the statement's head, or as a template of its own | it is read once, is not recursive, is not `MATERIALIZED` and writes nothing | a relation read twice that calls `random()`: as a WITH query it is computed once | `naming.rs`: `law_a_relation_read_once_returns_the_same_rows_under_each_name` |
| a semi-join's form, `semi_join` | `x IN (SELECT y …)`, or `EXISTS (SELECT … WHERE x = y)` | a conjunct of a `WHERE`, under no `NOT`, over a plain select | the same under a `NOT`, with a NULL in the subquery | `semi_join.rs`: `law_a_semi_join_in_either_form_returns_the_same_rows_under_no_not` |
| a union of a roster's arms, `--respell` | each arm reading the roster, or the roster read once and each arm's rows tagged with its number | every arm reads the same roster, joins it `ON true`, cuts it by a `WHERE` that reads the roster alone, and returns roster columns the same in every arm or plain columns of its own rows | the arms tagged by the roster's slug, which a case-insensitive collation can merge | `respell.rs`: `a_union_any_arm_of_which_breaks_a_condition_is_written_as_composed` |

To add an axis:

1. **Its module**, `crates/sql-composer/src/spellings/<axis>.rs`. It holds a constant for each
   pick, its value in `spellings.toml`, and `places`, which finds each place the axis applies in a
   template read for its spellings (`read::Read`). Each place lists its choices: the place as
   written first (`Choice::written`), then each spelling as edits of the template's own text, with
   the statement it must read back as. A choice that does not read back as that statement is never
   offered (`place::checked`), and every byte outside its edits stays as written. Where the text
   does not say whether a condition holds, the place stays as written.
2. **Its axis.** A variant of `Axis` in `spellings/mod.rs`, with its key, its picks, its words and
   its `places`, and its place in `Axis::ALL`, the order `spell` writes the axes in. A field of
   `Picks` in `spellings/picks.rs`, and its arm in `Picks::pick`. `cargo sqlc spellings`,
   `--axis` and `spell` read it from there.
3. **Its tests**, in the module: each spelling written, and written back by the other pick; what
   stays as written when each condition is broken; comments and directives kept where they stand.
4. **Its law**, a test named `law_…` and marked
   `#[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]`. It builds a `Trap`, mock
   tables holding the rows its near-identity fails on, and calls
   `laws::holds(written, spelled, near, &trap)`: the spelling returns the rows the template as
   written returns, as many times each and some rows at least, on tables of those rows and again on
   the same rows as mock tables, and the near-identity returns other rows. Seen to fail first, with
   the near-identity in the spelling's place.
5. **The brick templates.** The tests over the brick example still pass: each axis's places
   counted on `shared/scope_sets.sqlc`, the new axis among them
   (`each_place_is_counted_and_its_count_is_the_spellings_written`), every template read and
   written back byte for byte with nothing picked
   (`every_brick_template_reads_and_is_written_back_byte_for_byte`), every template written in
   every pick reads, composes and stays written
   (`every_template_written_in_every_pick_reads_composes_and_stays_written`, given the new pick),
   and every composed file keeps its WITH queries
   (`the_brick_templates_written_in_every_pick_keep_their_with_queries`). cargo-sqlc's
   `spellings_counts_each_place_before_it_writes_each_spelling` prints every axis's count line,
   the new one among them.
6. **The plan.** `cargo run -p brick-example -- fit`, on the database
   [the brick example](examples/brick/README.md) loads, checks that each spelling `spell` can
   write leaves every composed file that holds its template with the plan it has as written, plain
   and with `--with`, by `EXPLAIN (COSTS OFF)` alone. An axis whose spelling changes a plan is
   narrowed until none does.
7. **The pages.** Its row in the README's `spellings.toml` table, and its line in the CHANGELOG.

```bash
cargo test -p sql-composer --all-features spellings::
cargo test -p cargo-sqlc spell
SQLC_TEST_POSTGRES_URL=postgres://localhost/postgres \
  cargo test -p sql-composer --all-features -- --ignored law_
cargo run -p brick-example -- fit
```

A respelling of the composed file, as the last row is, goes in `respell.rs`, behind the feature
`respell`. Its tests break each of its conditions in turn and see the statement written as
composed, and see a file with nothing to respell written byte for byte.

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
   cargo clippy --workspace --all-targets -- -D warnings
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
