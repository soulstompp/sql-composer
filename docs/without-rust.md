# Composing without Rust

Your SQL stays SQL. Each query is a template, a file of plain SQL that other templates compose by
its path, and `cargo sqlc compose` writes every statement out composed, as a `.sql` file. `psql`
runs it as it stands, and a program in any language runs it with the names `manifest.json` gives
its placeholders. Nothing in your project is Rust: Rust's toolchain installs the one program, and
that is all of Rust the project needs.

## Install

Install Rust's toolchain with [rustup](https://rustup.rs), then the program:

```sh
cargo install cargo-sqlc
```

It goes into Cargo's `bin` directory, `~/.cargo/bin` unless you set it elsewhere, and Cargo runs it
as `cargo sqlc`. Composing reads `.sqlc` files, writes `.sql` files, and connects to no database.
`cargo uninstall cargo-sqlc` takes it away again.

## Templates in

A tree of templates is a directory of `.sqlc` files, `sqlc` by default, at any depth. The brick
example's [`reports/release_years.sqlc`](../examples/brick/sqlc/reports/release_years.sqlc) counts
the sets released in each year:

```sql
# Every year from the first release to the last, with the number of sets released in it: one row
# per year, keyed by year. generate_series writes the calendar and the sets are counted against it,
# so a year with no release is a year examined and found empty, with 0, and never a year left out.
SELECT y.year, count(s.set_num) AS sets
FROM (
    SELECT min(year) AS first_year, max(year) AS last_year
    FROM lego_sets
) span
CROSS JOIN generate_series(span.first_year, span.last_year) AS y(year)
LEFT JOIN lego_sets s ON s.year = y.year
GROUP BY y.year
ORDER BY y.year
```

The `#` lines are the template's own comments, and never reach the database. The
[README](../README.md) goes through the template language a directive at a time: a relation put
inside another, a value bound by name, a relation passed as an argument, relations combined as
sets.

## Composed files out

From the directory that holds `sqlc`:

```sh
cargo sqlc compose --source sqlc --target .sql --skip-prepare
```

`--skip-prepare` leaves out `cargo sqlx prepare`, which saves the query data a Rust program's SQLx
checks against. Without it, the command runs that step once the files are written, and exits 1 if
it fails, as it does where SQLx is not installed; the files are written all the same.

Every template gets a line, and the last line counts them. For the brick example:

```text
Composed 51 of 55 template(s) into .sql: 4 shape(s), 0 skipped
```

Each template's file keeps its path, so `sqlc/reports/release_years.sqlc` becomes
`.sql/reports/release_years.sql`, the same SQL with its `#` comments gone. A shape, a template with
a slot left for a caller to fill, gets no file of its own. The target is replaced whole on every
compose, so point `--target` at a directory that holds nothing else, and commit it beside the
templates.

## Running a file with psql

A file with no placeholder runs as it stands:

```sh
psql -d sqlc_brick -f .sql/reports/release_years.sql
```

A file with placeholders takes its values in the order its manifest lists the names (below).
[`sets/select_colored_parts.sql`](../examples/brick/.sql/sets/select_colored_parts.sql), a set's
lines in one colour, takes the colour's name as `$1` and the set as `$2`. psql 16 and later binds
them with `\bind`, written after the statement:

```sh
{ cat .sql/sets/select_colored_parts.sql; echo '\bind Black 10182-1 \g'; } | psql -d sqlc_brick
```

On any version, `PREPARE` and `EXECUTE` do the same:

```sh
{ echo 'PREPARE colored AS'; cat .sql/sets/select_colored_parts.sql; echo ';'
  echo "EXECUTE colored('Black', '10182-1');"; } | psql -d sqlc_brick
```

## The names, in the manifest

`manifest.json`, at the root of the target, records how the files were composed, then each file's
names. The brick example's begins:

```json
{
  "dialect": "postgres",
  "with": false,
  "views": null,
  "search_path": [],
  "respell": false,
  "files": {
```

and says of the colour listing:

```json
    "sets/select_colored_parts.sql": {
      "bind_params": [
        "color_name",
        "set_num"
      ],
      "statement_starts": [
        0
      ]
    },
```

A program in any language reads it with its own JSON reader. It finds a file's entry by the file's
path under the target, and gives its database driver each name's value in the order
`bind_params` lists them: here `$1` is `color_name` and `$2` is `set_num`. A name the template
binds twice is one placeholder on PostgreSQL, DuckDB and SQLite, and listed once. On MySQL and
MariaDB each `?` is a name of its own, and a name used twice is listed twice. A file of several
statements numbers each statement's placeholders from the first, and `statement_starts` says where
each statement's names begin in `bind_params`; the program sends the statements one at a time,
each ended where its database ends it.

## Keeping the files current

```sh
cargo sqlc compose --source sqlc --target .sql --verify
```

`--verify` composes in memory, with the flags the manifest records, and compares with the target:
it reports each file `CHANGED`, `MISSING` or `STALE`, writes nothing, and exits 1 on any. Run in CI,
it keeps the committed files what their templates compose to.

## Where it stops

Composing never checks the SQL. A mistake in it composes cleanly, and only the database refuses
it, so each file is run once by `psql` or by a test. The manifest holds names, never types or
values: the values are your program's.

To compose inside your program instead of ahead of time, Python has the module `sql_composer`,
distributed as `sql-composer`, and C has `sql-composer-ffi`. Each composes a whole tree as
`cargo sqlc compose` does and gives back the same files and the same `manifest.json`:
[sql-composer-py](../crates/sql-composer-py/README.md) and
[sql-composer-ffi](../crates/sql-composer-ffi/README.md) give the calls.
