# sql-composer

> **Também disponível em português europeu: [`README.pt.md`][README.pt.md].**

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
sql-composer = "0.1.0"
```

## A first composition

The templates on this page read the tables of the [brick example][examples/brick/]: a catalogue of
building-brick sets, with their parts, colours and themes, and the builders who buy them, all built
by [sqlc-brickgen][examples/sqlc-brickgen/].

A set's lines are a join of five tables: every line of the set's inventories, with its part, the
part's category and its colour. Written once, it is a file of its own,
[`shared/set_part_details.sqlc`][examples/brick/sqlc/shared/set_part_details.sqlc]:

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
[`sets/select_set_parts.sqlc`][examples/brick/sqlc/sets/select_set_parts.sqlc]:

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
writes [`.sql/sets/select_set_parts.sql`][examples/brick/.sql/sets/select_set_parts.sql]. The join
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
placeholder order, and `query_file!()` takes its arguments in that same order. In a template of
several statements, each numbers its placeholders from the first, and `statement_starts` gives
where each statement's names begin in `bind_params`. `cargo sqlc compose` writes both lists for
each composed file into the target's `manifest.json` (*Reference*).

A name given a list of values gets one placeholder per value. Composed with three values for
`color_ids` (`compose_with_values`),

```sql
SELECT name, rgb FROM lego_colors WHERE id IN (:bind(color_ids)) AND is_trans = :bind(is_trans)
```

becomes `... WHERE id IN ($1, $2, $3) AND is_trans = $4`, with `bind_params`
`["color_ids", "color_ids", "color_ids", "is_trans"]`.

`:bind(name EXPECTING 1..20)` says how many values a binding takes, and `EXPECTING 2` says at least
two. `compose_with_values` refuses a count outside them (`ValueCount`). `:bind(name NULL)` says a
binding takes `NULL`, and the SQLite, DuckDB and MySQL drivers refuse a NULL given to one that does
not say so (`NullValue`). The PostgreSQL drivers' values show a NULL only once they are sent, so
there `NULL` is only recorded, on the parsed `Binding` (`nullable`).

### Slots: a relation passed as an argument

A template can leave a relation open, as a slot, and let each caller fill it.
[`shared/filtered_set_parts.sqlc`][examples/brick/sqlc/shared/filtered_set_parts.sqlc] is a set's
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
[`filters/by_color.sqlc`][examples/brick/sqlc/filters/by_color.sqlc]:

```sql
# A colour, as a filter pattern: (any part, this colour). The colour is chosen by its name; a
# filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = :bind(color_name)
```

and [`sets/select_colored_parts.sqlc`][examples/brick/sqlc/sets/select_colored_parts.sqlc] fills
the slot with it:

```sql
# A set's lines in the colour color_name, one row per line, keyed by the line: the set's lines with
# the colour's filter in the @filter slot.
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_color.sqlc)
```

A slot is filled only by the `:compose` that names it. A template composed inside another inherits
none of its caller's slots, so each level passes on what it needs. `@` means a slot only inside
`:compose(…)`; anywhere else, `@name` is SQL and passes through.

It saves the base query written once for each filter. The same slot takes a category
(`filters/by_category.sqlc`), the words of a search (`filters/by_words.sqlc`) and a regular
expression (`filters/by_pattern.sqlc`), and the base stays one file.

In the composed statement, the filter's body stands where `:compose(@filter)` was.

A template with a slot left open is a shape, not a query. The library refuses to compose it alone
(`MissingSlot`, naming the slot and the template it stands in), and `cargo sqlc compose` writes no
file for it. It says so, with the slots the shape leaves open:
`Shape sqlc/shared/filtered_set_parts.sqlc: @filter open`.

A caller that leaves a slot unfilled gets no file either, and nothing can compose it, since a slot
is filled only by the `:compose` that names it. `cargo sqlc compose` reports it skipped, on standard
error, with the slot and the template it stands in:
`Skipped sqlc/sets/select_colored_parts.sqlc: @filter of shared/filtered_set_parts.sqlc left open`.
`--verify` reports it the same way, beside the `STALE` its file gets once committed. Neither
changes the exit code.

### Union, intersection and difference

`:union`, `:intersect` and `:except` combine whole queries, each one a file.
[`reports/city_only_moulds.sqlc`][examples/brick/sqlc/reports/city_only_moulds.sqlc] is the moulds
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

In the composed statement, each source is wrapped in a derived table of its own,
`SELECT * FROM (…) AS _except_1`, and so is the whole, `(…) AS _except`. In SQL, `INTERSECT` binds
more tightly than `UNION` and `EXCEPT`, and those two group from left to right. Without its
wrapping, a source would regroup with its neighbours: `a EXCEPT b UNION c` means
`(a EXCEPT b) UNION c`, a plausible table with no error. The wrapping keeps each source whole,
another set operation or a query ending in `ORDER BY` and `LIMIT` among them, so any of the three
can be another's source.

A set operation compares whole rows, so a column list is refused rather than ignored. SQLite takes
one modifier, `ALL`, and only after `UNION`.

### `:count`: a relation reduced to one figure

`:count(source)` counts a query's rows, and `:count(DISTINCT part_num OF source)` counts the
distinct values of its columns. Several sources are counted as their union.
[`reports/count_theme_parts.sqlc`][examples/brick/sqlc/reports/count_theme_parts.sqlc]:

```sql
# How many distinct moulds (part numbers) the sets of a theme scope use, the theme and every theme
# below it: one row, one count.
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

It saves the source pasted by hand into a `SELECT COUNT(…)`.

In the composed statement, it is `SELECT COUNT(DISTINCT part_num) FROM (…) AS _count_sub`.

With several sources, the count's `ALL` goes into their union too, as `UNION ALL`. Otherwise the
union is a bare `UNION`, which removes duplicate rows and which SQLite takes as well.

### Each relation named once

`cargo sqlc compose --with` writes each relation a statement reads once, named, at the head of the
statement.

It saves the copies. A relation composed by two templates is written twice into any statement that
composes both: a composed statement holds one copy of a relation for every path that leads to it,
and the database plans every copy.
[`reports/shared_moulds.sqlc`][examples/brick/sqlc/reports/shared_moulds.sqlc] reads the theme
closure through both of its sources, so its composed statement holds the closure twice.
`cargo sqlc tree` draws each path (*Reference*).

With `--with`, every composed statement opens with one `WITH` that names each relation it reads,
each after the relations it reads in turn, and the statement comes last. A relation is named after
its path under `--source`, its directories joined by `_`: `shared/theme_closure.sqlc` is
`shared_theme_closure`, and every place that composes it reads `SELECT * FROM shared_theme_closure`.
The moulds Technic and City share then hold the closure once. Nothing is created on the database.

- A statement that opens with its own `WITH` or `WITH RECURSIVE` takes the names first, inside that
  clause, and a `CREATE … AS` takes them at the head of its query. Any other statement takes them
  in front of it, and MySQL takes no `WITH` before an `INSERT`.
- A template of several statements gets a `WITH` for each, naming only what that statement reads.
  The statements end at each `;` outside quotes, comments, dollar quotes and psql meta-commands,
  as the dialect's database reads them.
- A template that binds a parameter, itself or through a template it composes, is written where it
  is composed, as without `--with`. So is a template composed with its slots filled. What either
  reads is still named.
- A name the dialect reserves is refused (`key` on MySQL, `left` on PostgreSQL), and so are two
  files that would take one name.

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

Inside a string, a quoted identifier, a `--` or `/* */` comment or a psql meta-command, a `#` is
SQL, and so is a directive: `'#1'` stays `'#1'`, and `-- :bind(id)` reaches the database as the
comment it is. The jsonb operators `#>`, `#>>` and `#-` are SQL wherever they stand, and `#--`
begins a comment. PostgreSQL's operators `#` and `##` still begin one, so write the function behind
them, as `pg_operator.oprcode` names it: `int4xor(a, b)` for `a # b` between integers.

The composer reads those quotes and comments as each dialect's database reads them, and refuses a
template that two databases would read differently at a `#`, a directive, a `;` or its end, naming
both readings. `E'it\'s' # note` is a string and a comment on PostgreSQL. On SQLite, whose strings
read no backslash escapes, the `#` stands inside a string still open.

A template written for some dialects only says so at its head, `:dialect(postgres)` or
`:dialect(postgres, duckdb)`, and is then read by their databases alone and composed for no other.
One that says nothing is written for every dialect. In a template written for PostgreSQL, a dollar
quote (a function's body, a `DO` block) holds SQL as a string does, `#variable_conflict` and all,
except that a directive composes there. A `:bind` written there or composed into it is refused,
since no value reaches it.

## Mock tables: a template on rows of your own

A mock gives a table rows of your own. Every statement composed after it that names the table reads
the mock's rows instead, so a template can be checked on exactly the rows a case needs, the ones the
real data never holds among them:

```rust,ignore
use sql_composer::MockTable;

composer.add_mock_table(
    MockTable::new("lego_sets")
        .column("set_num", "varchar")
        .column("year", "integer")
        .row(&[Some("100-1"), Some("1990")])
        .row(&[Some("200-1"), Some("1992")]),
);
```

It saves the test tables created, filled and dropped around each case, and the copy of a query
rewritten to read them.

In the composed statement, the mock stands at the head of each statement that names the table, in
its `WITH`: `lego_sets AS (SELECT CAST('100-1' AS varchar) AS set_num, … UNION ALL …)`. A `WITH`
name stands for the table of the same name wherever the statement names it bare, so the composer
still parses none of the SQL. A statement with its own `WITH` takes the mock as its first entry, and
a mock with no rows keeps its columns and returns nothing. Each value is cast to its column's type,
written as the dialect spells it, and `None` is `NULL`.

A mock stops where the bare name does not reach. A schema-qualified name such as `lego.lego_sets`
and a view read the table itself, and so does a statement that names the table only in double
quotes. A statement that writes the table writes the table itself. MySQL takes no `WITH` before an
`INSERT`.

## The walk

The [brick example][examples/brick/README.md] walks the relational algebra one template at a time,
on the tables above, with a subcommand for each step. It runs through selection, projection and
join, composition, a relation passed as an argument, the theme closure, and union, intersection and
difference. Then it goes on into more of PostgreSQL's SQL: window functions, `DISTINCT ON`,
`LATERAL`, `ROLLUP`, `percentile_cont`, `generate_series`, an anti-join, division, `array_agg`,
`jsonb` and `MERGE`.

It searches with the indexes the generator builds: by the words of a name (`websearch_to_tsquery`,
ranked with `ts_rank` and marked with `ts_headline`), by a regular expression (`~*`, and
`regexp_match` to read a brick's size out of its name), and by place (`earth_box` and
`earth_distance` for a radius, `<->` for the nearest first). Each search fills a slot as any filter
does: words and patterns take `@filter`, and the builders near a city or in a postcode district take
`@area`.

It ends at `laws`: every promise the example makes, each checked against a second spelling of the
same answer in direct SQL, and two run on mock tables. `laws` exits 1 if any of them fails.

## Reference

### `cargo sqlc compose`

```sh
cargo sqlc compose                                  # sqlc/ into .sql/, then cargo sqlx prepare
cargo sqlc compose --source assets/sqlc --target assets/.sql
cargo sqlc compose --search-path ../shared/sqlc     # more templates, read after --source
cargo sqlc compose --dialect mysql                  # one of five, postgres by default
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
  gone after the next compose. Point `--target` at a directory that holds nothing else. Beside the
  files goes `manifest.json` (below).
- **`--search-path`** is one more directory `:compose` paths are read under, after `--source`, so a
  template can compose a relation another tree holds. Nothing is composed for the templates there
  themselves, and under `--with` a relation found there is named after its path there. Repeat it to
  search more than one.
- **`--dialect`** chooses the dialect: `postgres` (the default), `mysql`, `mariadb`, `sqlite` or
  `duckdb`. It decides the placeholders, and the names the dialect reserves.
- **`--skip-prepare`** leaves out `cargo sqlx prepare`, which otherwise runs once the files are
  written. If it fails, the command exits 1, and the files are written all the same.
- **`--verify`** composes in memory and compares with the target, its `manifest.json` included. It
  reports each file `CHANGED`, `MISSING` or `STALE`, exits 1 on any, and writes nothing, as
  `cargo fmt --check` does. It composes with the flags the manifest records: a flag left out is
  read from the manifest, and one given that the manifest records otherwise is refused, with both.
  A skipped template is reported as a compose reports it.
- **`--with`** names each relation a statement reads once, at the head of the statement. It cannot
  be combined with `--views`.
- **`--views`** loads a view registry, and every template the registry names is read as its view.
- **`--respell`** respells each composed statement for PostgreSQL where its own text proves the same
  rows (below). It is refused for every other dialect.

Every template under `--source` gets a line: `Composed`; `Shape`, with the slots it leaves open; or
`Skipped`, on standard error, with the slot left open and the template it stands in. The last line
counts them: `Composed 47 of 51 template(s) into .sql: 4 shape(s), 0 skipped`.

`manifest.json`, at the root of the target, records how its files were composed: the flags
(`dialect`, `with`, `views`, `search_path`), and for each composed file its bind names in
placeholder order (`bind_params`) and where each statement's names begin (`statement_starts`). The
brick example's says of one of its files:

```json
"sets/select_colored_parts.sql": {
  "bind_params": ["color_name", "set_num"],
  "statement_starts": [0]
}
```

So a program in any language runs a composed file by its names: `$1` is `color_name` and `$2` is
`set_num`. On MySQL and MariaDB each `?` takes the name in its place in `bind_params`, where a name
used twice is listed twice.

### `cargo sqlc tree`

```sh
cargo sqlc tree                                     # every composed file's templates
cargo sqlc tree reports/shared_moulds.sqlc          # one composed file's
cargo sqlc tree --invert shared/theme_closure.sqlc  # the files that hold one template
```

It prints each composed file's templates as `cargo tree` prints a crate's dependencies: every
template the file holds, once for every path that leads to it, with the slot each filling stands
in, and the name written wherever a name stands for a body. A template held more than once gets a
line with its copies:

```text
reports/shared_moulds.sqlc
├── queries/technic_moulds.sqlc
│   └── shared/scope_moulds.sqlc
│       └── scopes/technic.sqlc (@scope)
│           └── shared/theme_closure.sqlc
└── queries/city_moulds.sqlc
    └── shared/scope_moulds.sqlc
        └── scopes/city.sqlc (@scope)
            └── shared/theme_closure.sqlc
shared/scope_moulds.sqlc: 2 copies
shared/theme_closure.sqlc: 2 copies
```

With `--with` the closure is named once at the head of the statement,
`shared/theme_closure.sqlc (WITH shared_theme_closure)`, and each place that composed it reads the
name. Whether the database then reads a copy once or twice is its plan's to say, in `EXPLAIN`.
`--invert` (`-i`) prints, for one template, each composed file that holds it with its copies: every
statement a change to it reaches. `cargo sqlc tree` takes `--source`, `--dialect`, `--with`,
`--views` and `--search-path` as `compose` does.

### `--respell`

A union whose arms each read one roster, cut to their own rows of it and left joined `ON true` to
what each reads, reads the roster once for every arm. `--respell` writes it as one join: the roster
read once, each of its rows paired with the arms whose cut it passes, and every arm's own rows read
in one union, each tagged with its arm's number. That spelling alone is sql-composer's, because it
is proved from the composed text with no catalog, key or collation. The ones that rest on the
catalog are warren-pg-speller's, in PostgreSQL's planner. Every other byte of the file is written as
composed. The statements are read with sqlparser, and one it cannot read is written as composed,
with a line on standard error: `Unparsed <file>: statement(s) 4 of 52, written as
composed`. Each file respelled gets a line of its own, `Respelled <file>: 1 union(s),
of 32 arms`, and the manifest records the flag.

### Directives

| directive | composes to |
|---|---|
| `:compose(path)` | the relation in `path`, where the directive stands |
| `:compose(path, @slot = other)` | the same, with `@slot` filled by `other`; several slots are separated by commas |
| `:compose(@slot)` | the relation a caller fills `@slot` with |
| `:define(path)` | the relation's body, and the name in front of its `AS` stands for it below |
| `:bind(name)` | the dialect's placeholder; one per value for a name bound to a list |
| `:bind(name EXPECTING n..m)`, `:bind(name NULL)` | the same; a count outside `n..m` is refused, and, except on PostgreSQL, a NULL where `NULL` is not written |
| `:union(a, b, …)` | the sources joined by `UNION`, each wrapped, the whole wrapped; `ALL` or `DISTINCT` first, inside the parentheses |
| `:intersect(a, b, …)` | the sources joined by `INTERSECT`, each wrapped, the whole wrapped |
| `:except(a, b, …)` | `a` less every later source, each wrapped, the whole wrapped |
| `:count(a)`, `:count(DISTINCT col OF a)` | `SELECT COUNT(…) FROM (…) AS _count_sub` |
| `:dialect(postgres, duckdb)` | nothing: the dialects the template is written for, at its head; it is composed for no other |
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
placeholder for each value. `load_view_registry` loads a view registry, `add_mock_table` gives a
table rows of your own, and `name_relations = true` composes as `--with` does.
`source::compose_all` composes every template under a directory as `cargo sqlc compose` does, with
the flags a `source::Flags` holds, and says what it made of each: `Standing::Written`, with its
`ComposedSql`; `Standing::Shape`; or `Standing::Skipped`. `Dialect::ALL` lists the dialects, and
`"mysql".parse::<Dialect>()` reads one by the name `--dialect` takes.
`Composer::tree` gives what composing a template writes, as `cargo sqlc tree` prints it.

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
| `Held` | a template as a composed statement holds it: how it is reached, the name written for it, and what it holds |
| `Reach` | how a template is reached: `Root`, `Compose`, `Slot`, `Define`, `Source` or `Head` |
| `source::Flags` | the dialect, `--with`, `--views` and `--search-path` a directory is composed with |
| `source::Standing` | what composing made of one template: written, a shape, or skipped |
| `source::Manifest` | what `manifest.json` holds, under the feature `serde` |
| `respell::Respelled` | a file as respelled: its text, its statements, those not read and the unions respelled, under the feature `respell` |
| `Dialect` | `Postgres`, `Mysql`, `Mariadb`, `Sqlite` or `Duckdb` |
| `MockTable` | rows of your own for a table: its name, its columns in order, and its rows |
| `MockColumn` | a mock table's column: its name, and its type as the dialect spells it |

The feature `serde` derives `Serialize` and `Deserialize` for the core types, and `std` (on by
default) builds with the standard library.

```toml
sql-composer = { version = "0.1.0", features = ["serde"] }
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
sql-composer = "0.1.0"
sql-composer-rusqlite = "0.1.0"
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
sql-composer = "0.1.0"
sql-composer-duckdb = "0.1.0"
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
let composer = Composer::new(Dialect::Duckdb);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn duckdb::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

#### PostgreSQL (sync and async)

```toml
[dependencies]
sql-composer = "0.1.0"
sql-composer-postgres = "0.1.0"  # sync and async, both on by default
# sql-composer-postgres = { version = "0.1.0", default-features = false, features = ["async"] }
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
sql-composer = "0.1.0"
sql-composer-mysql = "0.1.0"  # sync and async, both on by default
# sql-composer-mysql = { version = "0.1.0", default-features = false, features = ["async"] }
```

The feature `sync` brings in the `mysql` crate and the sync type. `async` turns on the async type,
built on `mysql_async`, which the driver always depends on.

A name used twice is two placeholders on MySQL, and each takes the name's values again from the
first.

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
sql-composer-sqlx = "0.1.0"                                        # checks against PostgreSQL (the default)
# sql-composer-sqlx = { version = "0.1.0", features = ["validate"] } # adds an offline syntax check
```

`verify_postgres`, under the feature `postgres` (on by default), prepares each statement on a live
PostgreSQL and deallocates it again. That checks its syntax, and that its tables and columns exist.
`verify_mysql`, under the feature `mysql`, does the same on a live MySQL. `validate_syntax`, under
the feature `validate`, parses a statement offline with `sqlparser`, every placeholder read as `1`.
It checks syntax only.

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
  brick/                  # every directive but :define, and more of PostgreSQL's SQL, on sqlc-brickgen's database
  sqlc-brickgen/          # sqlc-brickgen: a large catalogue built from a real one, for PostgreSQL
```

## Licence

MIT, copyright Kenneth Allen Flegal: see [LICENSE][LICENSE]. Each published crate carries the same
file.

The brick example's tables are built from a real catalogue of building-brick sets, which its README
credits. LEGO® is a trademark of the LEGO Group, which does not sponsor, authorise or endorse this
project.

[README.pt.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/README.pt.md
[examples/brick/]: https://github.com/soulstompp/sql-composer/tree/HEAD/examples/brick
[examples/sqlc-brickgen/]: https://github.com/soulstompp/sql-composer/tree/HEAD/examples/sqlc-brickgen
[examples/brick/sqlc/shared/set_part_details.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/shared/set_part_details.sqlc
[examples/brick/sqlc/sets/select_set_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/sets/select_set_parts.sqlc
[examples/brick/.sql/sets/select_set_parts.sql]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/.sql/sets/select_set_parts.sql
[examples/brick/sqlc/shared/filtered_set_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/shared/filtered_set_parts.sqlc
[examples/brick/sqlc/filters/by_color.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/filters/by_color.sqlc
[examples/brick/sqlc/sets/select_colored_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/sets/select_colored_parts.sqlc
[examples/brick/sqlc/reports/city_only_moulds.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/city_only_moulds.sqlc
[examples/brick/sqlc/reports/count_theme_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/count_theme_parts.sqlc
[examples/brick/sqlc/reports/shared_moulds.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/shared_moulds.sqlc
[examples/brick/README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/README.md
[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/LICENSE
