# sql-composer

> **Também disponível em português europeu: [`README.pt.md`](README.pt.md).**

A join copied into a second query is two joins. They start out the same, and they drift apart.

sql-composer keeps one. Each query lives in its own file, in plain SQL, and other queries compose
it by its path. `cargo sqlc compose` writes every composed statement out as the plain SQL that
reaches the database, with no macros left in it and nothing assembled at run time. A DBA reads that
file, `EXPLAIN` runs it, and SQLx's `query_file!()` checks it when your code compiles.

Composition works because every query returns a relation, rows that share their columns, and a
relation can stand wherever a table stands. A query composed into another is read there as a table.
That is closure, and the whole template language rests on it: one relation put inside another, a
relation passed as an argument, relations combined as sets, and each relation named once.

The database still plans the composed statement. sql-composer only writes it.

## Install

```sh
cargo install cargo-sqlc
```

This installs one program, `cargo-sqlc`, into Cargo's `bin` directory (`~/.cargo/bin` unless you
set it elsewhere). Cargo runs it as `cargo sqlc`. Composing reads `.sqlc` files, writes `.sql`
files, and connects to no database.

After it composes, it runs `cargo sqlx prepare`, which saves the query data SQLx checks against
offline. That step is SQLx's own command, installed apart with `cargo install sqlx-cli`, and it
connects to the database in `DATABASE_URL`. `--skip-prepare` leaves it out.
`cargo uninstall cargo-sqlc` takes the program away again.

To compose from Rust instead, add the library:

```toml
[dependencies]
sql-composer = "0.0.5"
```

## A first composition

The templates on this page read the tables of the [brick example](examples/brick/): a catalogue of
building-brick sets, with their parts, colours and themes, and the builders who buy them, all built
by [sqlc-brickgen](examples/sqlc-brickgen/).

A set's lines are a join of five tables: every line of the set's inventories, with its part, the
part's category and its colour. Written once, it is a file of its own,
[`shared/set_part_details.sqlc`](examples/brick/sqlc/shared/set_part_details.sqlc):

```sql
# What parts are in a set: every line of every inventory the set has, one row per line, keyed by the
# line (inventory_id, part_num, color_id, is_spare). A set sold in more than one version has one
# inventory per version, and each line carries its version. Nothing here adds two versions
# together: a caller that totals a set totals it per version.
#
# A line is never dropped for what the catalogue lacks. A line whose part is not in the parts
# catalogue keeps its row, with part_status 'uncatalogued' and no part name or category.
SELECT
    ip.inventory_id,
    i.version,
    ip.part_num,
    ip.color_id,
    ip.is_spare,
    ip.quantity,
    CASE WHEN p.part_num IS NULL THEN 'uncatalogued' ELSE 'catalogued' END AS part_status,
    p.name AS part_name,
    pc.name AS category_name,
    c.name AS color_name,
    c.rgb AS color_rgb,
    c.is_trans
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
LEFT JOIN lego_parts p ON p.part_num = ip.part_num
LEFT JOIN lego_part_categories pc ON pc.id = p.part_cat_id
LEFT JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = :bind(set_num)
```

It is a complete query. It runs on its own, given a `set_num`, and its comment says what its rows
are and what keys them.
The `#` lines are the template's own comments, and they never reach the database.

The listing of a set's parts composes it,
[`sets/select_set_parts.sqlc`](examples/brick/sqlc/sets/select_set_parts.sqlc):

```sql
# A set's parts, one row per line, version by version, each line with its part, category and
# colour. The ORDER BY ends on the line key, so the order is total and a truncated listing is always
# the same rows. A line with no category, part name or colour name comes after the named ones; the
# ORDER BY says so, because databases disagree on where a NULL sorts.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    inventory_id,
    version,
    part_num,
    color_id,
    is_spare,
    part_status,
    part_name,
    category_name,
    color_name,
    color_rgb,
    quantity
FROM set_part_details
ORDER BY
    version,
    inventory_id,
    category_name IS NULL, category_name,
    part_name IS NULL, part_name,
    color_name IS NULL, color_name,
    part_num,
    color_id,
    is_spare
```

From the example's directory, `cargo sqlc compose --source sqlc --target .sql --skip-prepare`
writes [`.sql/sets/select_set_parts.sql`](examples/brick/.sql/sets/select_set_parts.sql). The join
stands where the `:compose` was, and a numbered placeholder where the `:bind` was:

```sql
WITH set_part_details AS (
    SELECT
    ip.inventory_id,
    i.version,
    ip.part_num,
    ip.color_id,
    ip.is_spare,
    ip.quantity,
    CASE WHEN p.part_num IS NULL THEN 'uncatalogued' ELSE 'catalogued' END AS part_status,
    p.name AS part_name,
    pc.name AS category_name,
    c.name AS color_name,
    c.rgb AS color_rgb,
    c.is_trans
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
LEFT JOIN lego_parts p ON p.part_num = ip.part_num
LEFT JOIN lego_part_categories pc ON pc.id = p.part_cat_id
LEFT JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = $1

)
SELECT
    inventory_id,
    version,
    part_num,
    color_id,
    is_spare,
    part_status,
    part_name,
    category_name,
    color_name,
    color_rgb,
    quantity
FROM set_part_details
ORDER BY
    version,
    inventory_id,
    category_name IS NULL, category_name,
    part_name IS NULL, part_name,
    color_name IS NULL, color_name,
    part_num,
    color_id,
    is_spare
```

The listing by colour further down composes the same file. So do the per-category totals an
`INSERT` and a `MERGE` write, and the spare counts an `UPDATE` keeps. A change to the join is made
once, and the next compose writes it into every one of them.

The composer never parses the SQL it splices. A mistake in the SQL composes cleanly, and only the
database refuses it. So the composed files are committed beside their templates.
`cargo sqlc compose --verify` checks that they are still what the templates compose to, and SQLx
or a test runs them.

## The template language

The directives are the moves of the relational algebra, written in SQL. Each section below says
what a template states, the copy it saves, how it shows in the composed statement, and where it
stops.

### `:compose`: one relation inside another

`:compose(path)` puts the relation in `path` where the directive stands: in a `FROM`, a `JOIN`, a
`WITH`, an `EXISTS`, an `INSERT … SELECT` or an `UPDATE … FROM`. The path is read under `--source`,
or under the search paths a `Composer` is given.

It saves the query pasted in by hand, the copy that drifts. In the composed statement the
relation's body stands where the directive was, with its `#` comments gone. Give each splice an
alias, as the example does, so that two composed relations with columns of the same name stay apart.

A template that composes itself, through any chain of files, is refused as a circular reference,
and so is a path that names no file.

### `:bind`: the values a statement takes

`:bind(name)` is a value the statement takes when it runs. A name can also take a list of values,
for an `IN`.

It saves the hand-numbered `$3` that has to move whenever a filter is added. A value is matched to
its placeholder by name, never by position.

The composer writes the dialect's own placeholder:

```sql
SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)
-- PostgreSQL: ... WHERE year = $2 AND theme_id = $1
-- SQLite:     ... WHERE year = ?2 AND theme_id = ?1
-- MySQL:      ... WHERE year = ? AND theme_id = ?
```

For PostgreSQL and SQLite, each name is numbered by its alphabetical place among the statement's
names, and a name used twice is one placeholder. For MySQL, each use is a `?` of its own, in the
order the statement uses them. The composed statement's `bind_params` lists the names in
placeholder order, and `query_file!()` takes its arguments in that same order.

A name given a list of values gets one placeholder per value. Composed with three values for
`color_ids` (`compose_with_values`),

```sql
SELECT name, rgb FROM lego_colors WHERE id IN (:bind(color_ids)) AND is_trans = :bind(is_trans)
```

becomes `... WHERE id IN ($1, $2, $3) AND is_trans = $4`, with `bind_params`
`["color_ids", "color_ids", "color_ids", "is_trans"]`.

`:bind(name EXPECTING 1..20)` and `:bind(name NULL)` record how many values a binding expects and
whether it takes `NULL`, on the parsed `Binding` (`min_values`, `max_values`, `nullable`), for the
calling code to read. Composing does not check them.

### Slots: a relation passed as an argument

A template can leave a relation open, as a slot, and let each caller fill it.
[`shared/filtered_set_parts.sqlc`](examples/brick/sqlc/shared/filtered_set_parts.sqlc) is a set's
lines, kept by whatever filter the caller passes as `@filter`:

```sql
# A set's lines that match a filter. The caller decides which filter to use (the @filter slot).
#
# A filter returns patterns with two columns, part_num and color_id, in which NULL means "any
# value". A line matches when some pattern agrees with it on every column the pattern names, and it
# comes back once however many patterns it matches.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
),
filter AS (
    :compose(@filter)
)
SELECT p.*
FROM set_part_details p
WHERE EXISTS (
    SELECT 1
    FROM filter f
    WHERE (f.part_num IS NULL OR f.part_num = p.part_num)
      AND (f.color_id IS NULL OR f.color_id = p.color_id)
)
```

A filter is a query of its own,
[`filters/by_color.sqlc`](examples/brick/sqlc/filters/by_color.sqlc):

```sql
# A colour, as a filter pattern: (any part, this colour). The colour is chosen by its name; a
# filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = :bind(color_name)
```

and [`sets/select_colored_parts.sqlc`](examples/brick/sqlc/sets/select_colored_parts.sqlc) fills
the slot with it:

```sql
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_color.sqlc)
```

A slot is filled only by the `:compose` that names it. A template composed inside another inherits
none of its caller's slots, so each level passes on what it needs. `@` means a slot only inside
`:compose(…)`; anywhere else, `@name` is SQL and passes through.

It saves the base query written once for each filter. A category filter fills the same slot with
`filters/by_category.sqlc`, and the base stays one file.

In the composed statement, the filter's body stands where `:compose(@filter)` was.

A template with a slot left open is a shape, not a query. The library refuses to compose it alone
(`MissingSlot`), and `cargo sqlc compose` writes no file for it. A caller that leaves a slot
unfilled is skipped in the same way, without a message. Only `--verify` notices, once the caller's
composed file has been committed: it reports that file `STALE`.

### Union, intersection and difference

`:union`, `:intersect` and `:except` combine whole queries, each one a file.
[`reports/city_only_moulds.sqlc`](examples/brick/sqlc/reports/city_only_moulds.sqlc) is the moulds
City sets use and no Technic set does:

```sql
# The moulds City sets use and no Technic set does: one row per part_num. The order of the sources
# matters: the first is the one the second is taken from.
:except(queries/city_moulds.sqlc, queries/technic_moulds.sqlc)
```

A difference's order matters: `:except(a, b, c)` is `a` less everything in `b` or `c`. A bare
`:union`, `:intersect` or `:except` removes duplicate rows. `ALL` keeps them (`:except(ALL a, b)`),
and `DISTINCT` says the default out loud.

It saves one query's body pasted beside another's.

In the composed statement, `:intersect` and `:except` wrap each source in a derived table of its
own, `SELECT * FROM (…) AS _except_1`, and wrap the whole, `(…) AS _except`. In SQL, `INTERSECT`
binds more tightly than `UNION` and `EXCEPT`, and those two group from left to right. Without its
wrapping, a source would regroup with its neighbours: `a EXCEPT b UNION c` means
`(a EXCEPT b) UNION c`, a plausible table with no error. The wrapping keeps each source whole, and
lets an `:intersect` or an `:except` be a `:union` source. `:union` writes its sources side by side,
with no wrapping.

So a `:union` source that is itself a `:union` with another modifier regroups. With `bc` composed as
`:union(ALL b, c)`, `:union(a, bc)` composes to `a UNION b UNION ALL c`. That is
`(a UNION b) UNION ALL c`, which keeps every row of `c`, duplicates and all. `:intersect` and
`:except` compare whole rows, so a column list is refused rather than ignored. SQLite takes one
modifier, `ALL`, and only after `UNION`.

### `:count`: a relation reduced to one figure

`:count(source)` counts a query's rows, and `:count(DISTINCT part_num OF source)` counts the
distinct values of its columns. Several sources are counted as their union.
[`reports/count_theme_parts.sqlc`](examples/brick/sqlc/reports/count_theme_parts.sqlc):

```sql
# How many distinct moulds (part numbers) the sets of a theme scope use, the theme and every theme
# below it: one row, one count.
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

It saves the source pasted by hand into a `SELECT COUNT(…)`.

In the composed statement, it is `SELECT COUNT(DISTINCT part_num) FROM (…) AS _count_sub`.

With several sources, the count's `DISTINCT` or `ALL` goes into their union too, as
`UNION DISTINCT` or `UNION ALL`, and SQLite refuses the first.

### Each relation named once

`cargo sqlc compose --with` writes each relation a statement reads once, named, at the head of the
statement.

It saves the copies. A relation composed by two templates is written twice into any statement that
composes both: a composed statement holds one copy of a relation for every path that leads to it,
and the database plans every copy.
[`reports/shared_moulds.sqlc`](examples/brick/sqlc/reports/shared_moulds.sqlc) reads the theme
closure through both of its sources, so its composed statement holds the closure twice.

With `--with`, every composed statement opens with one `WITH` that names each relation it reads,
each after the relations it reads in turn, and the statement comes last. A relation is named after
its path under `--source`, its directories joined by `_`: `shared/theme_closure.sqlc` is
`shared_theme_closure`, and every place that composes it reads `SELECT * FROM shared_theme_closure`.
The moulds Technic and City share then hold the closure once. Nothing is created on the database.

- A statement that opens with its own `WITH` or `WITH RECURSIVE` takes the names first, inside that
  clause, and a `CREATE … AS` takes them at the head of its query.
- A template of several statements gets a `WITH` for each, naming only what that statement reads.
  The statements end at each `;` outside quotes, comments, dollar quotes and psql meta-commands.
- A template that binds a parameter, itself or through a template it composes, is written where it
  is composed, as without `--with`. So is a template composed with its slots filled. What either
  reads is still named.
- A name that is a reserved word is refused, and so are two files that would take one name.

A statement can also choose its own names. `:define(path)` writes the body where `:compose` would
name it, and the name in front of its `AS` then stands for that template everywhere below, at any
depth:

```sql
WITH theme_closure AS (
    :define(shared/theme_closure.sqlc)
)
:compose(reports/count_theme_parts.sqlc)
```

The count's source composes `shared/theme_closure.sqlc` too, and reads it there as
`SELECT * FROM theme_closure`.

A template of `CREATE [OR REPLACE] VIEW <name> AS :define(<path>)` statements is a view registry.
Loaded with `cargo sqlc compose --views <registry>` or `Composer::load_view_registry()`, it makes
every composition say `SELECT * FROM <name>` for each template it registers. Composed as a template,
it gives the `CREATE VIEW` statements to run. A registered template that binds a parameter or leaves
a slot open is refused, since a view takes no argument. Where a statement defines a name for a
template that is also a view, the statement's name is the one used.

`--with` takes the place of both. It cannot be combined with `--views`, and a `:define` met under
`--with` is refused.

### Comments

A `#` begins a template comment anywhere on a line, and the comment runs to the end of the line.
Comments are where a template says what its rows are: their key, what they keep, and what they
leave out.

They save the copies a `--` comment makes. A `--` comment is SQL, so it passes into every statement
that composes the template. A `#` comment stays in the template.

They are stripped at composition and never reach the database. The newline goes with them, so the
next line joins the one the comment was on.

The composer looks for `#` without reading the SQL around it. So a `#` inside a string literal, a
quoted identifier, a `--` comment or an operator (`#>>`, `#-`) starts a comment too. The composed
statement comes out changed, and usually still runs. Spell an operator by its function
(`jsonb_extract_path_text` for `#>>`), and keep `#` out of strings and `--` lines.

## The walk

The [brick example](examples/brick/README.md) walks the relational algebra one template at a time,
on the tables above, with a subcommand for each step. It runs through selection, projection and
join, composition, a relation passed as an argument, the theme closure, and union, intersection and
difference. Then it goes on into more of PostgreSQL's SQL: window functions, `DISTINCT ON`,
`LATERAL`, `ROLLUP`, `percentile_cont`, `generate_series`, an anti-join, division, `array_agg`,
`jsonb` and `MERGE`.

It ends at `laws`: every promise the example makes, each checked against a second spelling of the
same answer in direct SQL. `laws` exits 1 if any of them fails.

## Reference

### `cargo sqlc compose`

```sh
cargo sqlc compose                                  # sqlc/ into .sql/, then cargo sqlx prepare
cargo sqlc compose --source assets/sqlc --target assets/.sql
cargo sqlc compose --dialect mysql                  # postgres (the default), mysql or sqlite
cargo sqlc compose --skip-prepare                   # leave out cargo sqlx prepare
cargo sqlc compose --verify                         # compare, write nothing
cargo sqlc compose --with                           # each relation named once per statement
cargo sqlc compose --views sqlc/views.sqlc          # a view registry
```

- **`--source`** is the directory of templates, `sqlc` by default, or `SQLC_SOURCE_DIR` when the
  flag is left out. Every `.sqlc` under it is composed, at any depth, and `:compose` paths are read
  under it.
- **`--target`** is where the composed files go, `.sql` by default, or `SQLC_TARGET_DIR`. Each
  template's file keeps its path: `sqlc/sets/select_set_parts.sqlc` becomes
  `.sql/sets/select_set_parts.sql`. Composition writes to a temporary directory first and puts it in
  the target's place only once every template has composed. The target is replaced whole, so a
  template removed or moved leaves no file behind, and a file written into the target by hand is
  gone after the next compose. Point `--target` at a directory that holds nothing else.
- **`--dialect`** chooses the placeholders: `postgres`, `mysql` or `sqlite`.
- **`--skip-prepare`** leaves out `cargo sqlx prepare`, which otherwise runs once the files are
  written. If it fails, the command exits 1, and the files are written all the same.
- **`--verify`** composes in memory and compares with the target. It reports each file `CHANGED`,
  `MISSING` or `STALE`, exits 1 on any, and writes nothing, as `cargo fmt --check` does. Run it with
  the same flags the files were composed with, since different flags compose different text.
- **`--with`** names each relation a statement reads once, at the head of the statement. It cannot
  be combined with `--views`.
- **`--views`** loads a view registry, and every template the registry names is read as its view.

### Directives

| directive | composes to |
|---|---|
| `:compose(path)` | the relation in `path`, where the directive stands |
| `:compose(path, @slot = other)` | the same, with `@slot` filled by `other`; several slots are separated by commas |
| `:compose(@slot)` | the relation a caller fills `@slot` with |
| `:define(path)` | the relation's body, and the name in front of its `AS` stands for it below |
| `:bind(name)` | the dialect's placeholder; one per value for a name bound to a list |
| `:bind(name EXPECTING n..m)`, `:bind(name NULL)` | the same, with the expectation recorded on the `Binding` |
| `:union(a, b, …)` | the sources joined by `UNION`, unwrapped; `ALL` or `DISTINCT` first, inside the parentheses |
| `:intersect(a, b, …)` | the sources joined by `INTERSECT`, each wrapped, the whole wrapped |
| `:except(a, b, …)` | `a` less every later source, each wrapped, the whole wrapped |
| `:count(a)`, `:count(DISTINCT col OF a)` | `SELECT COUNT(…) FROM (…) AS _count_sub` |
| `# …` | nothing: a template comment, to the end of the line |

A slot name is letters, digits, `-` and `_`.

### The library

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};

let input = "SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();

let composer = Composer::new(Dialect::Postgres);
let result = composer.compose(&template).unwrap();

assert_eq!(result.sql, "SELECT set_num, name, year FROM lego_sets WHERE year = $2 AND theme_id = $1");
assert_eq!(result.bind_params, vec!["theme_id", "year"]);
```

`parse_template_file` reads a template from a file. `Composer::add_search_path` adds a directory
`:compose` paths are read under. `compose_with_values` takes each binding's values and writes one
placeholder for each value. `load_view_registry` loads a view registry, and
`name_relations = true` composes as `--with` does.

| type | what it is |
|---|---|
| `Template` | a parsed template: its elements, and where it came from |
| `Element` | literal SQL, a `:bind`, a `:compose` or `:define`, or a command |
| `Binding` | a `:bind`: its name, the values it expects, and whether it takes `NULL` |
| `ComposeRef` | a `:compose()` or `:define()`, with its slot arguments |
| `ComposeTarget` | a path, a slot (`@name`) or a `:define` path |
| `SlotAssignment` | an `@name = path` inside `:compose()` |
| `Command` | a `:count()`, `:union()`, `:intersect()` or `:except()`, with its modifier and sources |
| `CommandKind` | `Count`, `Union`, `Intersect` or `Except` |
| `Composer` | composes templates into SQL for one dialect |
| `ComposedSql` | the composed SQL, and its bind names in placeholder order |
| `Dialect` | `Postgres`, `Mysql` or `Sqlite` |

The feature `serde` derives `Serialize` and `Deserialize` for the core types, and `std` (on by
default) builds with the standard library.

```toml
sql-composer = { version = "0.0.5", features = ["serde"] }
```

### Driver crates

Each driver wraps its database crate's connection in a type of its own, which implements
`ComposerConnection` (sync) or `ComposerConnectionAsync` (async). Its `compose` composes a template
and puts the values in placeholder order, in one step. The type dereferences to the connection it
wraps, so the database crate's own methods are still there. The same templates compose for every
dialect; only the placeholders change.

#### rusqlite

```toml
[dependencies]
sql-composer = "0.0.5"
sql-composer-rusqlite = "0.0.5"
```

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_rusqlite::SqliteConnection;

let conn = SqliteConnection::open_in_memory().unwrap();
conn.execute("CREATE TABLE lego_sets (set_num TEXT, name TEXT, year INTEGER, theme_id INTEGER, num_parts INTEGER)", []).unwrap();

let input = "SELECT set_num, name FROM lego_sets WHERE year = :bind(year)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
let composer = Composer::new(Dialect::Sqlite);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn rusqlite::types::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();

let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
let mut stmt = conn.prepare(&sql).unwrap();
let _rows = stmt.query(refs.as_slice()).unwrap();
```

#### DuckDB

```toml
[dependencies]
sql-composer = "0.0.5"
sql-composer-duckdb = "0.0.5"
```

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_duckdb::DuckDbConnection;

let conn = DuckDbConnection::open_in_memory().unwrap();
let input = "SELECT set_num, name FROM lego_sets WHERE year = :bind(year)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
// DuckDB reads PostgreSQL's $N placeholders
let composer = Composer::new(Dialect::Postgres);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn duckdb::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

#### PostgreSQL (sync and async)

```toml
[dependencies]
sql-composer = "0.0.5"
sql-composer-postgres = "0.0.5"  # sync and async, both on by default
# sql-composer-postgres = { version = "0.0.5", default-features = false, features = ["async"] }
```

The feature `sync` brings in the `postgres` crate and the sync type. `async` turns on the async
type, built on `tokio-postgres`, which the driver always depends on.

```rust,ignore
// Async (tokio-postgres)
use sql_composer_postgres::{PgClient, boxed_params};
use sql_composer::driver::ComposerConnectionAsync;

let (client, connection) = tokio_postgres::connect("host=localhost dbname=sqlc_brick", tokio_postgres::NoTls).await?;
tokio::spawn(connection);
let client = PgClient::from_client(client);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>]);
let (sql, params) = client.compose(&composer, &template, values).await?;
let refs = boxed_params(&params);
let rows = client.query(&sql as &str, &refs).await?;
```

```rust,ignore
// Sync (postgres)
use sql_composer_postgres::{PgConnection, boxed_params_sync};
use sql_composer::driver::ComposerConnection;

let client = postgres::Client::connect("host=localhost dbname=sqlc_brick", postgres::NoTls)?;
let conn = PgConnection::from_client(client);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn postgres::types::ToSql + Sync>]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

#### MySQL (sync and async)

```toml
[dependencies]
sql-composer = "0.0.5"
sql-composer-mysql = "0.0.5"  # sync and async, both on by default
# sql-composer-mysql = { version = "0.0.5", default-features = false, features = ["async"] }
```

The feature `sync` brings in the `mysql` crate and the sync type. `async` turns on the async type,
built on `mysql_async`, which the driver always depends on.

A name used twice is two placeholders on MySQL, and both types stop there with `MissingBinding`,
whatever values the name is given.

```rust,ignore
// Async (mysql_async)
use sql_composer_mysql::MysqlConn;
use sql_composer::driver::ComposerConnectionAsync;

let pool = mysql_async::Pool::new("mysql://root@localhost/sqlc_brick");
let conn = pool.get_conn().await?;
let conn = MysqlConn::from_conn(conn);

let values = bind_values!("year" => [mysql_async::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values).await?;
```

```rust,ignore
// Sync (mysql)
use sql_composer_mysql::MysqlConnection;
use sql_composer::driver::ComposerConnection;

let conn = mysql::Conn::new("mysql://root@localhost/sqlc_brick")?;
let conn = MysqlConnection::from_conn(conn);

let values = bind_values!("year" => [mysql::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

#### sqlx: checking composed statements

```toml
[dependencies]
sql-composer-sqlx = "0.0.5"                                        # checks against PostgreSQL (the default)
# sql-composer-sqlx = { version = "0.0.5", features = ["validate"] } # adds an offline syntax check
```

`verify_postgres`, under the feature `postgres` (on by default), prepares each statement on a live
PostgreSQL and deallocates it again. That checks its syntax, and that its tables and columns exist.
`validate_syntax`, under the feature `validate`, parses a statement offline with `sqlparser`, every
placeholder read as `1`. It checks syntax only. The feature `mysql` builds sqlx with MySQL, and no
function of this crate uses it.

```rust,ignore
use sql_composer_sqlx::verify_postgres;

// Check composed statements against a live database
let stmts = vec![&composed_sql];
verify_postgres("postgres://localhost/sqlc_brick", &stmts).await?;
```

```rust,ignore
use sql_composer_sqlx::validate_syntax;
use sql_composer::Dialect;

// Check syntax offline, with no database (the "validate" feature)
validate_syntax("SELECT set_num, name FROM lego_sets WHERE year = $1", Dialect::Postgres)?;
```

### The workspace

```
crates/
  sql-composer/           # the core: parser (winnow), types, composer
  sql-composer-rusqlite/  # rusqlite driver (ComposerConnection)
  sql-composer-duckdb/    # DuckDB driver (ComposerConnection)
  sql-composer-postgres/  # PostgreSQL driver (sync and async)
  sql-composer-mysql/     # MySQL driver (sync and async)
  sql-composer-sqlx/      # sqlx integration (verify, validate)
  cargo-sqlc/             # the cargo sqlc command
examples/
  brick/                  # every directive, and more of PostgreSQL's SQL, on sqlc-brickgen's database
  sqlc-brickgen/          # sqlc-brickgen: a large catalogue built from a real one, for PostgreSQL
```

The crates are at 0.0.5, and an 0.0 release may change the API.

## Licence

MIT, copyright Kenneth Allen Flegal: see [LICENSE](LICENSE). Each published crate carries the same
file.

The brick example's tables are built from a real catalogue of building-brick sets, which its README
credits. LEGO® is a trademark of the LEGO Group, which does not sponsor, authorise or endorse this
project.
