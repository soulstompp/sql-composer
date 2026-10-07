# Changelog

## 0.0.5

### sql-composer

- **Named relations** — `Composer::name_relations` writes every relation a statement reads once, named after its file, at the head of the statement: one `WITH`, each relation after those it reads, the statement last. Every place that composes a relation says `SELECT * FROM <name>`. A statement's own `WITH` takes the names first, a `CREATE … AS` takes them at the head of its query, and each statement of a script gets its own. A template that binds a parameter, or is composed with its slots filled, is written in place. A reserved word, two files with one name, and a `:define` are refused.
- **`:union` wraps its sources** — Each source of a `:union`, and the whole, is a derived table of its own, as with `:intersect` and `:except`. A source that is itself a `:union` with another modifier no longer regroups with its neighbours, and a source ending in `ORDER BY` and `LIMIT` no longer ends the whole union. `:count` over several sources takes the same union. A column list on a `:union` is refused.
- **Mock tables stand for tables** — `Composer::add_mock_table` gives a table rows of your own: each composed statement that names the table gets `<name> AS (<rows>)` at the head of its `WITH`, and reads the rows in the table's place. A `MockTable` declares its columns in order with their types (`column`), takes rows of optional values (`row`), and writes each value cast to its column's type (`to_sql`). `MockColumn` is new; `add_row` and `mock_rows!` are gone, and `Composer::mock_tables` is a `BTreeMap`.
- **`driver::resolve_values_by_use`** — For positional placeholders: each use of a bind name takes the name's values again from the first.
- **A list inside a command's source, on MySQL** — `compose_with_values` gives a list bound inside a `:union`, `:intersect`, `:except` or `:count` source a `?` for each value. It wrote one `?`, which took only the list's first value.
- **A defined name, in the drivers** — `compose_with_values`, which every driver calls, says a name the template `:define`s everywhere underneath it, as `compose` does. It spliced the definition's body in again at each reference: the same rows, with each copy planned.
- **winnow 1** — The template parser is built on winnow 1.

### sql-composer-mysql

- **A bind name used twice** — Both drivers fill each use of a name with its values, where they stopped with `MissingBinding` at the second use.
- **mysql 28 and mysql_async 0.37** — The crate re-exports both, at these versions.

### sql-composer-rusqlite

- **rusqlite 0.40** — The crate re-exports it, at this version.

### sql-composer-sqlx

- **sqlx 0.9 and sqlparser 0.63** — `verify_postgres` connects with sqlx 0.9. Its PostgreSQL driver, 0.9.0, does not read the operating system's user: a URL that names no user connects as `anonymous` unless `PGUSER` is set, or the application builds `whoami` 2 with its default features.

### cargo-sqlc

- **`--with`** — `cargo sqlc compose` names every relation a statement reads once, at the head of the statement. It cannot be combined with `--views`.

### examples

- **The brick example** — `examples/lego` is `examples/brick` (crate `brick-example`, binary `brick`, database `sqlc_brick`, `SQLC_BRICK_DATABASE_URL`), and runs on the database sqlc-brickgen builds, which already holds the composite indexes for its joins; the `setup` subcommand and its downloader are gone.
- **The catalogue generator is `sqlc-brickgen`** — `examples/sqlc-brickgen/` builds the crate and binary `sqlc-brickgen`, formerly `lego-generator`, with its own version and its own changelog, [`examples/sqlc-brickgen/CHANGELOG.md`](examples/sqlc-brickgen/CHANGELOG.md).
- **More of PostgreSQL's SQL** — Templates on the builders, collections and purchases: window functions, `DISTINCT ON`, `LATERAL`, `ROLLUP`, `percentile_cont`, `generate_series`, an anti-join, division, `array_agg`, `jsonb` and `MERGE`.
- **Searches** — By the words of a part's name (`websearch_to_tsquery`, `ts_rank`, `ts_headline`), by a regular expression (`~*`, `regexp_match`), and by place (`earth_box` and `earth_distance` for a radius, `<->` for the nearest first), each served by an index the generator builds. Words and patterns fill the `@filter` slot; the builders near a city or in a postcode district fill a new `@area` slot. sqlc-brickgen 0.0.2 creates `cube`, `earthdistance` and `pg_trgm` in a schema of their own, `extensions`, and the example searches it after its own, never `public`. The example needs PostgreSQL 16 or later, where `earthdistance` works from a schema of its own.
- **City's and Technic's sets fold into one template** — `shared/scope_sets.sqlc` takes the scope as its `@scope` slot, and each scope returns its own id, from which the label is read.
- **Laws** — `laws` checks the templates' answers against second spellings in direct SQL, and runs two templates on mock tables. CI runs `cargo sqlc compose --verify` on the example, so its `.sql` files can no longer fall behind their templates.
- **TLS** — The example reaches a server that requires TLS (rustls); ask for it with the URL's `sslmode`.
- **sqlx 0.9** — The example runs on sqlx 0.9, and a URL that names no user still connects as the operating system's user.

### All crates

- Version bump to 0.0.5.

## 0.0.4

### sql-composer

- **`:intersect(sources...)` and `:except(first, rest...)`** — The other two set operations, with the `ALL` and `DISTINCT` modifiers `:union` takes. `:except(a, b, c)` is `a` less everything in `b` or `c`. Every source, and the whole, is wrapped as a derived table, so a source that is itself a union keeps its grouping and either can be a `:union` member. A `columns OF` list is refused.
- **`:define(path)`** — Composes a template's body even where that template is named. The name in front of `AS` stands for it everywhere under the template that defines it, so a statement can name its relations once in a `WITH` clause.
- **View registry** — `Composer::add_view` and `Composer::load_view_registry` make the composer write `SELECT * FROM <name>` wherever a registered template is composed, instead of inlining it. A registry is a template of `CREATE VIEW <name> AS :define(<path>)` statements. A registered template that binds a parameter or leaves a slot open is refused.

### cargo-sqlc

- **`--views <registry>`** — `cargo sqlc compose` composes with a view registry.

### examples

- **Lego example reworked** — A set's parts are listed version by version, and the summary and tracking tables keep versions apart. Theme scopes are a theme and every theme below it, chosen by id; colour and category filters are `(part_num, color_id)` patterns; bind values match parameters by name. New subcommands `shared-moulds` (`:intersect`), `city-only-moulds` (`:except`) and `laws`, which checks each promise against the same answer computed directly in SQL.
- **Lego catalogue generator** (`examples/lego-generator/`) — Builds a large LEGO catalogue from the real one the Lego example loads, with builders' collections and purchase logs, and loads it into Postgres in batches. `--size small|medium|huge` picks the scale, and the same `--sets`, `--seed` and wiring give the same rows.
- **Lego example's composed statements refreshed** — The committed `.sql` files had fallen behind their templates: several differed, and those of the newer templates were missing. They are composed again. A line with no category, part name or colour name now sorts after the named ones, and a set with no part count first within its year, on every database: each `ORDER BY` says so with an `IS NULL` term, since databases disagree on where a NULL sorts.
- **Composite indexes migration** — A migration gives the dump's tables the composite indexes a DBA adds for the example's joins, each led by the column the join into its table fixes and ending on the column the next join reads, or holding it in `INCLUDE`. `setup` and `migrate` run it, and it is safe to run again.

### All crates

- Version bump to 0.0.4.

## 0.0.3

### sql-composer

- **Fix: parser now handles comments before macros** — Templates with `#` comment lines immediately before `:compose()`, `:union()`, or `:count()` macros no longer produce empty output.
- **Fix: `:union()` no longer wraps members in parentheses** — Union output is now `QUERY UNION QUERY` rather than `(QUERY) UNION (QUERY)`, matching standard SQL semantics and avoiding unintended query planner behavior.
- **Fix: trailing whitespace trimmed in `:union()` output** — Composed union SQL no longer has blank lines between members and `UNION` keywords.

### examples

- **Lego database example** (`examples/lego/`) — A comprehensive runnable example using the Rebrickable Lego dataset on Postgres. Demonstrates every sql-composer feature: `:compose()`, `@slot` parameterization, `:bind()`, multi-value `:bind()` for `IN` clauses, `:union()`, `:count(DISTINCT)`, and `#` comments. Includes a clap CLI with subcommands, auto-download of the dataset, and committed `.sql` output for reference.

### All crates

- Version bump to 0.0.3.

## 0.0.2

### sql-composer

- **Parameterized `:compose()` with slot arguments** — Templates can now declare named slots (`@slot_name`) inside `:compose()` that callers fill with concrete file paths. This enables composable, parameterized templates: write a shared base query once and swap in different logic (e.g. filters, data sources) at the call site. Slots are explicitly scoped — child templates do not inherit parent slots.

### cargo-sqlc

- **Recursive directory scanning** — `cargo sqlc compose` now recursively walks all subdirectories under `--source`, composing every `.sqlc` file and mirroring the directory structure in `--target`.
- **Atomic output via temp directory** — Composition writes to a temporary directory first. The target is only replaced after all files compose successfully, preventing partial output on failure.
- **Clean target** — The entire target directory is wiped and recreated on each compose run, removing stale `.sql` files from deleted or reorganized sources.
- **`--verify` mode** — Composes all templates to memory and diffs against existing target files. Reports changed, missing, and stale files, then exits with code 1 on any mismatch. Designed for CI to ensure committed `.sql` files stay in sync with `.sqlc` sources.
- **Default target changed from `sql` to `.sql`** — The hidden directory name makes it obvious that the output is generated and should not be edited directly. Override with `--target` or `SQLC_TARGET_DIR`.

### All crates

- Version bump to 0.0.2.

## 0.0.1

- Initial release.
