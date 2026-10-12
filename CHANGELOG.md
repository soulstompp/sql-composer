# Changelog

## 0.1.0

### sql-composer

- **Named relations** — `Composer::name_relations` writes every relation a statement reads once, named after its file, at the head of the statement: one `WITH`, each relation after those it reads, the statement last. Every place that composes a relation says `SELECT * FROM <name>`. A statement's own `WITH` takes the names first, a `CREATE … AS` takes them at the head of its query, and each statement of a script gets its own. A template that binds a parameter, or is composed with its slots filled, is written in place. A reserved word, two files with one name, and a `:define` are refused.
- **`:union` wraps its sources** — Each source of a `:union`, and the whole, is a derived table of its own, as with `:intersect` and `:except`. A source that is itself a `:union` with another modifier no longer regroups with its neighbours, and a source ending in `ORDER BY` and `LIMIT` no longer ends the whole union. `:count` over several sources takes the same union. A column list on a `:union` is refused.
- **Mock tables stand for tables** — `Composer::add_mock_table` gives a table rows of your own: each composed statement that names the table gets `<name> AS (<rows>)` at the head of its `WITH`, and reads the rows in the table's place. A `MockTable` declares its columns in order with their types (`column`), takes rows of optional values (`row`), and writes each value cast to its column's type (`to_sql`). `MockColumn` is new; `add_row` and `mock_rows!` are gone, and `Composer::mock_tables` is a `BTreeMap`.
- **`driver::resolve_values_by_use`** — For positional placeholders: each use of a bind name takes the name's values again from the first.
- **A list inside a command's source, on MySQL** — `compose_with_values` gives a list bound inside a `:union`, `:intersect`, `:except` or `:count` source a `?` for each value. It wrote one `?`, which took only the list's first value.
- **A defined name, in the drivers** — `compose_with_values`, which every driver calls, says a name the template `:define`s everywhere underneath it, as `compose` does. It spliced the definition's body in again at each reference: the same rows, with each copy planned.
- **A distinct count of several sources, on SQLite** — `:count(DISTINCT col OF a, b)` unions its sources with a bare `UNION`, which removes duplicate rows already. It wrote `UNION DISTINCT`, which SQLite refuses.
- **`EXPECTING` and `NULL` are held** — `compose_with_values` refuses a binding given fewer values than its `EXPECTING` asks, or more (`Error::ValueCount`). `compose_with_values_and_nulls` takes a driver's test of which values are NULL, and refuses a NULL given to a `:bind` written without `NULL` (`Error::NullValue`). Both were only recorded on the `Binding`.
- **Each statement numbered from its first placeholder** — In a template of several statements, `compose` and `compose_with_values` number each statement's placeholders from the first, as `--with` did; they shared one numbering across the script. `ComposedSql::statement_starts` gives where each statement's names begin in `bind_params`, in both modes.
- **Each dialect's reserved words** — Naming relations refuses a file whose name its dialect reserves: PostgreSQL's reserved keywords and those it keeps for types and functions, MySQL's reserved keywords and `rollup`, and the keywords SQLite refuses as a name. Every dialect read PostgreSQL's reserved keywords alone, so `key.sqlc` composed on MySQL, and `left.sqlc` on PostgreSQL, to a statement the database refused. `Dialect::reserved_words` gives each list.
- **winnow 1** — The template parser is built on winnow 1.
- **Room for later variants** — `Error`, `Element`, `ComposeTarget`, `CommandKind`, `Dialect` and `TemplateSource` are `#[non_exhaustive]`: a `match` on one outside the crate takes a wildcard arm, so a variant added later breaks no caller.
- **MariaDB and DuckDB** — `Dialect::Mariadb` writes `?` and `Dialect::Duckdb` writes `$1`, each refusing the keywords its database refuses as a name, read from MariaDB 12.3 and DuckDB 1.5. A mock table's values double their backslashes on MariaDB, as on MySQL, whose strings read backslash escapes.
- **`Placeholders`** — `Dialect::placeholders` says how a dialect writes its placeholders: numbered after a sign, or positional. `Dialect::placeholder` and `Dialect::supports_numbered_placeholders` read it.
- **Statements end where the database ends them** — A template of several statements is split, and the tables a statement names are found for its mock tables, as the dialect's database reads its quotes and comments at its default settings. On PostgreSQL and DuckDB, `E'it\'s'` is one string. On MySQL and MariaDB, `"…"` is a string that reads backslash escapes, `` `…` `` is a name, `#` and `-- ` begin a comment, `--` with no space after it does not, and a `/* */` comment does not nest. On SQLite, `` `…` `` and `[…]` are names. MariaDB and SQLite read no dollar quotes. Every dialect was read as PostgreSQL, and `E'…'` as a plain string, so a `;` inside one of these could end a statement.
- **`#` and directives inside quotes and comments are SQL** — Inside a string, a quoted identifier, a `--` or `/* */` comment or a psql meta-command, a `#` is SQL, and so is a directive: `'#1'` stays `'#1'`, and `-- :bind(id)` stays a comment. The jsonb operators `#>`, `#>>` and `#-` are SQL wherever they stand, and `#--` begins a comment. A `#` began a comment wherever it stood, cutting a string or an operator short, and a directive inside a string or a comment was composed there.
- **A template two databases read differently is refused** — The template reader reads quotes and comments as each dialect's database does, and refuses a template two of them would read differently at a `#`, a directive, a `;` or its end, naming each reading (`Error::Parse`, by line and column, after the file's path): `E'it\'s' # note` is a string and a comment on PostgreSQL, and an unclosed string on SQLite.
- **A directive that does not parse is refused** — With its line and column. The template ended where the directive stood, and composed to a statement cut short, with no error.
- **`parser::template` is private** — `parse_template` and `parse_template_file` read a template.
- **`:dialect(…)`** — A template names the dialects it is written for at its head, `:dialect(postgres)` or `:dialect(postgres, duckdb)`, in `--dialect`'s words. Their databases alone read it, and composing it for another dialect is refused (`Error::NotWrittenFor`), whether it is the template composed or one it composes. A template that names none is written for every dialect. `Template::dialects` holds the names.
- **Dollar quotes in a template written for the databases that read them** — In a template written for PostgreSQL, DuckDB or MySQL, a dollar quote holds SQL as a string does, `#variable_conflict` and the jsonb operators with it, and a directive inside it composes. A `:bind` inside one is refused, written there (`Error::Parse`) or composed into it (`Error::BindInDollarQuote`): a function's body took its placeholder as the function's own first argument.
- **A missing slot names its template** — `Error::MissingSlot` holds `template`, the template the open slot stands in, and its message says it. It named the slot alone, which a caller composing two shapes could leave open in either.
- **`source::compose_all`** — Composes every `.sqlc` file under a directory, at any depth, as `cargo sqlc compose` does, with the flags a `source::Flags` holds: the dialect, `with`, `views` and `search_paths`. Each template takes one `source::Standing`: `Written`, with its `ComposedSql`; `Shape`, with the slots it leaves open; or `Skipped`, with the slot a template it composes leaves open and that template. Under `serde`, `source::Manifest` is what `cargo sqlc compose` writes to `manifest.json`, built from the standings with `new` and written and read with `to_json` and `from_json`.
- **Dialects by name** — `Dialect::ALL` lists every dialect, `Dialect::name` gives the name `--dialect` and `:dialect(…)` take, and `str::parse` reads it back. A name that is no dialect's is `Error::UnknownDialect`.
- **`ComposedSql` and `Placeholders` under `serde`** — The feature derives `Serialize` and `Deserialize` for both.
- **`Composer::tree`** — What composing a template writes, as a tree of `Held`: every template it holds, one for each path to it, how it is reached (`Reach`: a `:compose`, the slot it fills, a `:define`, a command's source, or under `name_relations` the head of the statement), and the name written wherever a name stands for its body. `Held::copies` counts how many times each template's body is written. `source::composer` is the composer `source::compose_all` composes with.
- **`respell`** — Under the feature `respell`, `respell::respell` respells a file composed for PostgreSQL: a union whose arms each read one roster, cut to their own rows of it and left joined `ON true` to what each reads, is written as one join, the roster read once and each arm's own rows tagged with its number. Every other byte is written as composed, and so is a statement sqlparser cannot read; `Respelled` says which, and which unions it respelled. `source::Flags::respell` and `source::respell_all` respell a directory's files, and `Error::RespellFor` refuses another dialect.
- **`spellings`** — Under the feature `spellings`, `spellings::lay_out` lays out a template's other spellings that return the same rows, on five axes (`spellings::Axis`): where a condition sits, negation, join order, how a relation read once is named, and a semi-join's form. Each place an axis applies is counted, and each spelling written with every other place as written (`Layout`). `spellings::spell` writes a template in the picks `spellings.toml` holds (`Picks`, refused by `Error::Picks` for a key or a pick no axis has), and `confirm` reads a join order picked on one template against outermost first. A template is read as sqlparser reads PostgreSQL's SQL, each directive standing in as SQL of its own kind, and nothing is read from a catalog; join orders are laid out and never written, a template another takes in (`Beside`) keeps its head as written, and a template or statement the parser cannot read stays as written.

### sql-composer-mysql

- **A bind name used twice** — Both drivers fill each use of a name with its values, where they stopped with `MissingBinding` at the second use.
- **A NULL where `:bind` does not say `NULL`** — Both drivers refuse `Value::NULL` given to such a binding.
- **mysql 28 and mysql_async 0.37** — The crate re-exports both, at these versions.

### sql-composer-rusqlite

- **A NULL where `:bind` does not say `NULL`** — The driver refuses a value that writes NULL given to such a binding.
- **rusqlite 0.40** — The crate re-exports it, at this version.

### sql-composer-duckdb

- **A NULL where `:bind` does not say `NULL`** — The driver refuses a value that writes NULL given to such a binding.
- **`Dialect::Duckdb`** — Compose for DuckDB with `Dialect::Duckdb`, as the driver's examples now do: it refuses DuckDB's reserved words as a relation's name, where `Dialect::Postgres` refused PostgreSQL's. The placeholders are the same, `$1`, `$2`.

### sql-composer-sqlx

- **`verify_mysql`** — Under the feature `mysql`, prepares each composed statement on a live MySQL, which checks its syntax and that its tables and columns exist, as `verify_postgres` does on PostgreSQL. The feature built sqlx with MySQL and added no function.
- **sqlx 0.9 and sqlparser 0.63** — `verify_postgres` connects with sqlx 0.9. Its PostgreSQL driver, 0.9.0, does not read the operating system's user: a URL that names no user connects as `anonymous` unless `PGUSER` is set, or the application builds `whoami` 2 with its default features.
- **MariaDB and DuckDB in `validate_syntax`** — MariaDB is read with sqlparser's MySQL dialect, and DuckDB with its DuckDB dialect.

### cargo-sqlc

- **`--with`** — `cargo sqlc compose` names every relation a statement reads once, at the head of the statement. It cannot be combined with `--views`.
- **`--search-path <dir>`** — `cargo sqlc compose` also looks for templates in this directory, after `--source`, so a template can compose a relation that lives in another tree. The relation is named after its path there, and nothing is composed for the templates there themselves. Repeat it to search more than one.
- **Only `.sqlc` files are composed** — `cargo sqlc compose` skips a file with no extension, as it skips one with another, and `--verify` reads only the target's `.sql` files. A stray `notes` was composed to `notes.sql`, and a target's `README` was reported stale.
- **`--dialect mariadb` and `--dialect duckdb`** — `cargo sqlc compose` composes for MariaDB and DuckDB, each with its own placeholders and reserved words.
- **Every template is reported** — `cargo sqlc compose` gives each template under `--source` a line: `Composed`; `Shape`, with the slots it leaves open; or `Skipped`, on standard error, with the slot a template it composes leaves open and that template. `--verify` reports the skipped ones too, and the exit code is unchanged. The last line counts each: `Composed 47 of 51 template(s) into .sql: 4 shape(s), 0 skipped`. A caller that left a slot open was skipped without a message, and a source holding only shapes was reported as holding no `.sqlc` file.
- **`manifest.json`** — `cargo sqlc compose` writes `manifest.json` at the root of `--target`: the flags it composed with (`dialect`, `with`, `views`, `search_path`), and for each composed file its bind names in placeholder order (`bind_params`) and where each statement's names begin (`statement_starts`), so a program in any language runs a composed file by its names. `--verify` compares the manifest as it compares the files, and composes with the flags it records: a flag left out is read from it, and one given that it records otherwise is refused, with both. `--verify` had to be given the flags the files were composed with.
- **`cargo sqlc tree`** — Prints each composed file's templates as `cargo tree` prints a crate's dependencies: every path to a template, the slot each filling stands in, the names `--with` and `--views` write, and a line for each template held more than once (`shared/theme_closure.sqlc: 2 copies`). `cargo sqlc tree <template>` prints one composed file's, and `--invert <template>` (`-i`) each composed file that holds the template, with its copies. It takes `compose`'s `--source`, `--dialect`, `--with`, `--views` and `--search-path`.
- **`--respell`** — `cargo sqlc compose --respell` writes that respelling for PostgreSQL, and is refused for every other dialect. Each file respelled gets a line, `Respelled …: 1 union(s), of 2 arms`, a statement that does not parse one on standard error, `Unparsed …`, and the last line ends with the unions respelled. The manifest records the flag, and `--verify` reads it.
- **`cargo sqlc spellings` and `cargo sqlc spell`** — `cargo sqlc spellings <template>` lays out a template's spellings, each axis counted place by place, then each spelling written out (`--axis <key>` for one axis). `cargo sqlc spell` writes every template under `--source` in the picks of its `spellings.toml`, as `cargo fmt` writes code, and `--check` reports each template not in them, writes nothing and exits 1. Neither writes a join order.

### sql-composer-ffi

- **A C ABI** — New in 0.1.0. A C program parses a template, composes it for any dialect with search paths, named relations, a view registry, mock tables and a count of values for each binding, and gets the SQL, its bind names in placeholder order and where each statement's names begin. It composes a whole directory as `cargo sqlc compose` does, `--respell` among its flags, with each template's standing and the text of `manifest.json`. Handles are opaque and freed by the library; a refusal is a status with sql-composer's own message. The header is `sql_composer.h`, written by cbindgen.
- **The brick example's checks, respelled** — The tests compose the brick templates through the ABI and through a C program, with `--respell` and without, and get the committed files byte for byte, but for `checks/all.sql` respelled, which reads the roster once, as `cargo sqlc compose --respell` writes it.

### sql-composer-py

- **A Python module** — New in 0.1.0: PyPI's `sql-composer`, imported as `sql_composer`, built by maturin. `parse`, `parse_file`, a `Composer` for a dialect named by its string, and `compose_all` for a directory, `respell` among its keywords, giving each template's standing and the text of `manifest.json`. A refusal raises `sql_composer.Error` with sql-composer's own message.
- **The brick example's checks, respelled** — The tests compose the brick templates through `compose_all`, with `respell=True` and without, and get the committed files byte for byte, but for `checks/all.sql` respelled, which reads the roster once, as `cargo sqlc compose --respell` writes it.

### examples

- **A walk** — The brick example's README walks the template language one template at a time: each step a question asked of the catalogue, the template that answers it, the statement it composes to and the subcommand that runs it, from a set's lines to the laws.
- **The brick example** — `examples/lego` is `examples/brick` (crate `brick-example`, binary `brick`, database `sqlc_brick`, `SQLC_BRICK_DATABASE_URL`), and runs on the database sqlc-brickgen builds, which already holds the composite indexes for its joins; the `setup` subcommand and its downloader are gone.
- **The catalogue generator is `sqlc-brickgen`** — `examples/sqlc-brickgen/` builds the crate and binary `sqlc-brickgen`, formerly `lego-generator`, with its own version and its own changelog, [`examples/sqlc-brickgen/CHANGELOG.md`](examples/sqlc-brickgen/CHANGELOG.md).
- **More of PostgreSQL's SQL** — Templates on the builders, collections and purchases: window functions, `DISTINCT ON`, `LATERAL`, `ROLLUP`, `percentile_cont`, `generate_series`, an anti-join, division, `array_agg`, `jsonb` and `MERGE`.
- **Searches** — By the words of a part's name (`websearch_to_tsquery`, `ts_rank`, `ts_headline`), by a regular expression (`~*`, `regexp_match`), and by place (`earth_box` and `earth_distance` for a radius, `<->` for the nearest first), each served by an index the generator builds. Words and patterns fill the `@filter` slot; the builders near a city or in a postcode district fill a new `@area` slot. sqlc-brickgen 0.0.2 creates `cube`, `earthdistance` and `pg_trgm` in a schema of their own, `extensions`, and the example searches it after its own, never `public`. The example needs PostgreSQL 16 or later, where `earthdistance` works from a schema of its own.
- **City's and Technic's sets fold into one template** — `shared/scope_sets.sqlc` takes the scope as its `@scope` slot, and each scope returns its own id, from which the label is read.
- **A roster of checks** — `checks/roster.sqlc` writes each check's rule once, as `VALUES`. `checks/lines_uncatalogued.sqlc` finds the inventory lines whose part the parts catalogue does not hold, and `checks/sets_without_lines.sqlc` the sets with no inventory line; each reads its own row of the roster and left joins `ON true` what it finds, so a check that finds nothing returns its rule alone. `checks/all.sqlc` is their `:union(ALL …)`, a union `--respell` rewrites. The `checks` subcommand prints each check's rows, and `all` runs it. `laws` holds each check's rows to a second spelling in direct SQL, the union to the checks' rows, the union respelled to its rows as composed, and, on mock rows, a check that finds nothing to its rule alone, composed and respelled.
- **Laws** — `laws` checks the templates' answers against second spellings in direct SQL, and runs three templates on mock tables. CI runs `cargo sqlc compose --verify` on the example, so its `.sql` files can no longer fall behind their templates.
- **`fit`** — Checks the composed files against the plans PostgreSQL makes for them, by `EXPLAIN (COSTS OFF)` alone, running nothing: each file scans each table as often as `cargo sqlc tree` says its templates read it, a `LEFT JOIN` the database removed named, and each spelling `cargo sqlc spell` can write leaves every file that holds its template with the plan it has as written, plain and with `--with`. It exits 1 if a law fails, or if a seed meant to break one does not.
- **TLS** — The example reaches a server that requires TLS (rustls); ask for it with the URL's `sslmode`.
- **One migration** — `migrate` creates the example's two tables with their version columns and keys in one migration, which is safe to run again.
- **sqlx 0.9** — The example runs on sqlx 0.9, and a URL that names no user still connects as the operating system's user.

### All crates

- Version bump to 0.1.0.
- **The Rust each crate needs** — Every crate states its `rust-version`, the oldest Rust its dependencies build on: 1.85 for sql-composer, cargo-sqlc and sql-composer-rusqlite, 1.85.1 for sql-composer-duckdb, 1.87 for sql-composer-postgres, 1.89 for sql-composer-mysql and 1.94 for sql-composer-sqlx. Each crate's page shows the README, and every crate is listed under its categories.
- **One version for the workspace** — The crates take their version, and the version each asks of sql-composer, from the workspace, so a release moves them in one place.

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
