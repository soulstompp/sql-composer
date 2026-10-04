# sql-composer

`sql-composer` helps to manage the complexity of growing SQL codebases by eliminating duplication while keeping you in real SQL. It is a clean macro-based template engine that composes complete, reusable SQL statements into larger queries. You write real SQL, extract the shared queries into their own files, and compose them into the queries with predictable named bindings that are portable across SQL dialects.

The ability to actively collaborate with your DBA team as the database fills up and query performance starts to matter by providing them SQL is worth the code maintenance costs, but is a major ask for the design team. SQLx makes this process dramatically more manageable with its `query_file!()` macro, which reads a query from a `.sql` file and checks it against the database at compile time. sql-composer tries to reintroduce some of the code reuse lost when SQL is just SQL.

In probably its most natural use case, sql-composer serves as a pre-compiler for the `.sql` files SQLx's `query_file!()` reads at compile time. You write `.sqlc` templates with `:compose()` references to shared logic, run `cargo sqlc compose` to generate the final `.sql` files, and `query_file!()` picks them up for compile-time verification.

The clean macros provide query reuse and basic bindings and feel and look a fair amount like a SQL function. The average SQL-savvy user should be able to quickly learn to read `.sqlc` templates and adapting to an existing set of templates should be fairly straightforward. This means DBAs should be able to follow along with templates but more importantly that OLTP queries can now be reused at least partially for ETL and OLAP query workloads.

The templates allow for `#` comments that are stripped during composition, so you can document your business rules, explain join ordering decisions, and leave notes for future developers without cluttering the SQL that hits your database. These comments can help teams collaborate and ensure a strong contextual understanding of queries for agents and search tools.

Reading and adapting to `.sqlc` templates is fairly straightforward, but designing them — particularly when starting from scratch — is a different skill. These templates will add cognitive load while designing your data paths in SQL, but they help distribute the ever-changing burden of query performance tuning across as many different specialists as possible. It will inevitably take all of you.

## Features

- **Clean macro syntax** embedded in SQL — no new language to learn
- **Dialect-aware placeholders** — Postgres (`$1`), MySQL (`?`), SQLite (`?1`)
- **Template composition** — include and reuse complete SQL statements via `:compose(path)`
- **Parameterized slots** — declare `@slot` placeholders in shared templates, filled by callers
- **Multi-value bindings** — expand `:bind(ids)` into `$1, $2, $3` for `IN` clauses
- **Recursive directory scanning** — compose entire directory trees of templates
- **`--verify` mode** — CI-friendly check that composed output matches committed `.sql` files
- **Atomic output** — composition writes to a temp directory, swaps on success, cleans stale files
- **Circular reference detection** — prevents infinite loops in template includes
- **`#` comments** — rich documentation stripped from output, zero cost in production
- **Driver crates** — thin wrappers for rusqlite, DuckDB, postgres, and MySQL

## Why sql-composer

**Eliminate SQL duplication without ever leaving SQL.** Shared business logic — what "active" means, how you resolve versions, how you calculate revenue — lives in one place and gets composed into every query that needs it. No ORM abstraction layer, no query builder DSL. Just SQL.

**Work directly with your DBA team.** The composed `.sql` files are the exact queries that hit your database. When your DBA has questions or suggestions, you're both looking at the same SQL. You can trace any statement back to the template that produced it.

**Reuse across OLTP and OLAP systems.** The same shared templates can be composed into transactional queries for Postgres (via SQLx) and analytical queries for DuckDB (via the duckdb driver) — without maintaining two divergent copies of your core business logic. Even across different database engines, the shared SQL stays in sync.

**Built for SQLx's `query_file!()`**. sql-composer is designed as a complement to SQLx. `cargo sqlc compose` generates the `.sql` files that `query_file!()` reads at compile time, then runs `cargo sqlx prepare` to save the query data SQLx checks them against when it builds offline. The macro takes its arguments in the order of the composed statement's placeholders, which for Postgres and SQLite is the alphabetical order of the bind names (see [How Bind Parameter Ordering Works](#how-bind-parameter-ordering-works)). You get DRY templates and compile-time verified queries.

**Rich documentation, zero production overhead.** Templates support `#` comments that are stripped during composition. Document your business rules, explain join ordering, leave notes for agents and future developers — none of it ends up in the SQL that hits your database.

**Clean macros.** sql-composer macros compose structured SQL building blocks — CTEs, queries, filters — not arbitrary string fragments. You can't splice in a bare WHERE clause or a partial JOIN. Templates are organized around composable units that each have a clear role in the final query, preventing the fragment spaghetti that makes other template systems unmaintainable.

### Caveats

* sql-composer requires structuring your SQL so that shared logic lives in composable units (CTEs, subqueries, filters) that get assembled into larger queries. This is a different way of organizing SQL than the more common approach of copy-pasting fragments. It may take some experimentation to find the right balance of template granularity and composition for your codebase. Also, as always, a particular RDBMS engine and the evolving shape of the data will ultimately decide which of this code reuse remains viable over time.

* This adds another layer of tooling and complexity to your SQL codebase. I am currently experimenting with a `sqlc_file!()` macro that could help somewhat mimic the amazing SQLx-style compile-time SQL errors that make life dramatically easier.

## Example: Lego Database

The examples below use the [Lego database](https://www.kaggle.com/datasets/rtatman/lego-database) ([SQL](https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql)), which has sets, parts, colors, themes, inventories, and the joins between them. It's a good fit because even simple questions ("what parts are in this set, with colors and categories?") require a 4-table join — exactly the kind of logic you'd want to write once and reuse.

LEGO® is a trademark of the LEGO Group, which does not sponsor, authorise or endorse this project.

### The problem: duplicated joins

Suppose you need to query the full part details for a set. The join chain is always the same:

```sql
-- This 4-table join appears in every query that needs part details for a set.
-- Without sql-composer, you copy-paste it into SELECT, INSERT, and UPDATE queries.
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_parts p ON p.part_num = ip.part_num
JOIN lego_part_categories pc ON pc.id = p.part_cat_id
JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = '75192-1'
```

Every query that needs part details — listing parts, building reports, updating counts — repeats this join chain. When the schema changes or the DBA adds an index hint, you update it in one place or forget and break something.

### The solution: shared template, composed three ways

Write the shared logic once as a CTE in its own template:

```
sqlc/
  shared/
    set_part_details.sqlc       # The canonical 4-table join, written once
  sets/
    select_set_parts.sqlc       # SELECT — list parts for a set
  reports/
    insert_set_summary.sqlc     # INSERT SELECT — populate a reporting table
  inventory/
    update_spare_counts.sqlc    # UPDATE — sync spare part counts
```

#### [`shared/set_part_details.sqlc`](examples/lego/sqlc/shared/set_part_details.sqlc)

```sql
# What parts are in a set: every line of every inventory the set has, one row per line, keyed by the
# line (inventory_id, part_num, color_id, is_spare). A set LEGO sold in more than one version has
# one inventory per version, and each line carries its version. Nothing here adds two versions
# together: a caller that totals a set totals it per version.
#
# A line is never dropped for what the catalogue lacks. A line whose part is not in the parts
# catalogue keeps its row, with part_status 'uncatalogued' and no part name or category.
#
# This query is the single source of truth for "what parts are in a set."
# If the DBA asks you to change join order or add a filter, change it here
# and every query that composes this template picks up the fix.
#
# Used by: sets/select_set_parts.sqlc
#          reports/insert_set_summary.sqlc
#          inventory/track_set_parts.sqlc
#          inventory/update_spare_counts.sqlc
#          shared/filtered_set_parts.sqlc
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

This is a complete, valid query. Callers compose it into a CTE — the `WITH ... AS (...)` wrapping lives in the calling template, not here. Every `#` comment is stripped from the output.

#### [`sets/select_set_parts.sqlc`](examples/lego/sqlc/sets/select_set_parts.sqlc) — SELECT

```sql
# List all parts for a set with full details, one row per line, version by version.
# Composes the shared part resolution query into a CTE. The ORDER BY ends on the line key, so the
# order is total and a truncated listing is always the same rows. A line with no category, part name
# or colour name comes after the named ones; the ORDER BY says so, because databases disagree on
# where a NULL sorts.
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

#### [`reports/insert_set_summary.sqlc`](examples/lego/sqlc/reports/insert_set_summary.sqlc) — INSERT SELECT

```sql
# Populate the per-category part count summary for a set: one row per (set, version, category). A
# set LEGO sold in several versions gets one summary per version, never a sum across them.
# Reuses the same part resolution as the detail query so counts
# are always consistent with what the detail view shows.
#
# A category with no spare lines gets a spare total of 0, and one with only spare lines gets a part
# total of 0. A line with no category (its part is not in the catalogue) is not summarised here; the
# parts listing shows it. Running it again replaces the version's summary rather than adding a
# second copy.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
INSERT INTO set_category_summary (set_num, version, category_name, total_parts, total_spare)
SELECT
    :bind(set_num),
    version,
    category_name,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_spare), 0),
    COALESCE(SUM(quantity) FILTER (WHERE is_spare), 0)
FROM set_part_details
WHERE category_name IS NOT NULL
GROUP BY version, category_name
ON CONFLICT (set_num, version, category_name) DO UPDATE
SET total_parts = EXCLUDED.total_parts,
    total_spare = EXCLUDED.total_spare
```

#### [`inventory/update_spare_counts.sqlc`](examples/lego/sqlc/inventory/update_spare_counts.sqlc) — UPDATE

```sql
# Sync the spare part counts in inventory_tracking from the canonical part resolution, version by
# version: a part's spare count is the sum of its spare lines across colours, in that version. A
# tracked part with no spare lines in its version gets 0.
# When the DBA changes the join logic in set_part_details, this UPDATE stays in sync.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
UPDATE inventory_tracking it
SET
    spare_count = COALESCE(spd.total_spare, 0),
    updated_at = NOW()
FROM (
    SELECT version, part_num, SUM(quantity) FILTER (WHERE is_spare) AS total_spare
    FROM set_part_details
    GROUP BY version, part_num
) spd
WHERE it.set_num = :bind(set_num)
  AND it.version = spd.version
  AND it.part_num = spd.part_num
```

All three queries share the same 4-table join. Change the join logic once in `shared/set_part_details.sqlc`, run `cargo sqlc compose`, and every composed `.sql` file is updated. The DBA can read any of the output files and see the full query — no templates, no macros, just SQL.

### Parameterized composition with slots

Sometimes a shared template needs to vary one piece of its logic depending on the caller. For example, the same base query might need different filters for different use cases. Instead of duplicating the entire template, use **slot arguments** — the shared template declares named slots with `@slot_name`, and the caller fills them with concrete file paths.

#### [`shared/filtered_set_parts.sqlc`](examples/lego/sqlc/shared/filtered_set_parts.sqlc) — base query with a `@filter` slot

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

#### [`filters/by_color.sqlc`](examples/lego/sqlc/filters/by_color.sqlc) — a standalone filter query

```sql
# A colour, as a filter pattern: (any part, this colour). The colour is chosen by its name; a
# filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = :bind(color_name)
```

#### [`filters/by_category.sqlc`](examples/lego/sqlc/filters/by_category.sqlc) — another standalone filter

```sql
# A part category, as filter patterns: (this part, any colour) for every part in the category. A
# category belongs to the part, so the patterns come from the parts catalogue and not from any
# inventory. A filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
JOIN lego_part_categories pc ON pc.id = p.part_cat_id
WHERE pc.name = :bind(category_name)
```

#### [`sets/select_colored_parts.sqlc`](examples/lego/sqlc/sets/select_colored_parts.sqlc) — fills `@filter` with the color filter

```sql
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_color.sqlc)
```

#### [`sets/select_category_parts.sqlc`](examples/lego/sqlc/sets/select_category_parts.sqlc) — fills `@filter` with the category filter

```sql
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_category.sqlc)
```

Both callers compose the same base query but plug in different filters. The base template is written once; the filter logic is swapped at composition time. Slots are explicitly scoped — a child template does NOT inherit slots from its parent, so there's no spooky action at a distance.

### Union, count, and multi-value bindings

The Lego example also demonstrates combining queries with `:union()`, counting with `:count()`, and multi-value bind parameters for `IN` clauses.

#### [`sets/select_sets_by_themes.sqlc`](examples/lego/sqlc/sets/select_sets_by_themes.sqlc) — multi-value `:bind()` for `IN`

```sql
# Find sets in any of the given theme scopes: each given theme and every theme below it. One row per
# set, however many of the given scopes contain it. The ORDER BY ends on set_num, so the order is
# total. Within a year, a set with no part count comes before the counted ones; the ORDER BY says
# so, because databases disagree on where a NULL sorts. A set with no year is never at or after
# min_year, so the year needs no such term. Demonstrates a multi-value bind for an IN clause.
SELECT
    s.set_num,
    s.name,
    s.year,
    t.name AS theme_name,
    s.num_parts
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE EXISTS (
    SELECT 1
    FROM (
        :compose(shared/theme_closure.sqlc)
    ) tc
    WHERE tc.theme_id = s.theme_id
      AND tc.ancestor_id IN (:bind(theme_ids EXPECTING 1..20))
)
  AND s.year >= :bind(min_year)
ORDER BY s.year DESC, s.num_parts IS NULL DESC, s.num_parts DESC, s.set_num
```

At runtime, call `compose_with_values()` with the actual value count for `theme_ids`, and the composer expands `:bind(theme_ids)` into the right number of placeholders: `$2, $3, $4` for 3 values.

#### [`reports/combined_theme_sets.sqlc`](examples/lego/sqlc/reports/combined_theme_sets.sqlc) — `:union()`

```sql
# Combine Technic and City sets into one result, each row labelled with its scope.
# Demonstrates :union() composing two standalone queries.
:union(queries/technic_sets.sqlc, queries/city_sets.sqlc)
```

Each union source ([`queries/technic_sets.sqlc`](examples/lego/sqlc/queries/technic_sets.sqlc), [`queries/city_sets.sqlc`](examples/lego/sqlc/queries/city_sets.sqlc)) is a complete, standalone query. Bind parameters from both sources are merged alphabetically: `$1`=city_theme_id, `$2`=min_year, `$3`=technic_theme_id.

#### [`reports/count_theme_parts.sqlc`](examples/lego/sqlc/reports/count_theme_parts.sqlc) — `:count(DISTINCT)`

```sql
# Count the distinct moulds (part numbers) across every set in a theme scope.
# Demonstrates :count(DISTINCT col OF source).
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

Wraps [`queries/theme_set_parts.sqlc`](examples/lego/sqlc/queries/theme_set_parts.sqlc) in `SELECT COUNT(DISTINCT part_num) FROM (...) AS _count_sub`.

### Running the example

The complete working example with all templates, a Rust binary, and setup instructions is in [`examples/lego/`](examples/lego/). See [`examples/lego/README.md`](examples/lego/README.md) for details.

## Quick Start

### CLI usage

Install `cargo-sqlc` as a cargo subcommand:

```sh
# From crates.io
cargo install cargo-sqlc

# From source
cargo install --path crates/cargo-sqlc
```

Cargo automatically discovers binaries named `cargo-<name>` on your `PATH` and makes them available as `cargo <name>`. After installing, `cargo sqlc` is ready to use.

Compose templates into `.sql` files:

```sh
# Compose all templates recursively from sqlc/ -> .sql/
cargo sqlc compose

# The output mirrors the source directory structure:
#   sqlc/sets/select_set_parts.sqlc -> .sql/sets/select_set_parts.sql
#   sqlc/shared/set_part_details.sqlc -> .sql/shared/set_part_details.sql

# With a specific dialect
cargo sqlc compose --dialect mysql

# Skip the sqlx prepare step
cargo sqlc compose --skip-prepare

# Override source/target directories
cargo sqlc compose --source assets/sqlc --target assets/.sql

# Verify that committed .sql files match templates (for CI)
cargo sqlc compose --verify

# Name the templates a view registry registers, instead of inlining them
cargo sqlc compose --views sqlc/views.sqlc
```

The compose step recursively walks all subdirectories under `--source`, composes every `.sqlc` file, and writes the output to the corresponding path under `--target`. The target directory is wiped and recreated on each run, so deleted or reorganized source files don't leave stale output behind. All composition happens to a temporary directory first — the target is only replaced after every file composes successfully.

After composing, `cargo sqlx prepare` runs automatically to keep the query cache up to date for compile-time checked queries. Set `DATABASE_URL` for this step, or use `--skip-prepare` to skip it.

#### `--verify` mode

```sh
cargo sqlc compose --verify
```

Composes all templates to memory and diffs against the existing target files. Reports changed, missing, and stale files, then exits with code 1 on any mismatch. Use this in CI to ensure committed `.sql` files stay in sync with `.sqlc` sources — analogous to `cargo fmt -- --check`.

#### `--views` registry

```sh
cargo sqlc compose --views sqlc/views.sqlc
```

A view registry is a template of `CREATE VIEW <name> AS :define(<path>)` statements:

```sql
# sqlc/views.sqlc
CREATE OR REPLACE VIEW theme_closure AS :define(shared/theme_closure.sqlc);
```

With `--views`, every composed statement says `SELECT * FROM <name>` wherever it composes `<path>`, at any depth, instead of inlining the template's body. Without it, everything inlines. A registered template that binds a parameter or leaves a slot open is refused, since a view takes no argument. The registry is read from the path as given, or else under `--source`. When it lies under `--source`, it composes like any other template, into the `CREATE VIEW` statements to run on the database: `:define` puts each view's body in place, and a view built on another names it.

#### Environment variables

Directories can be configured via environment variables:

```sh
export SQLC_SOURCE_DIR=src/queries
export SQLC_TARGET_DIR=generated/.sql

# Now just:
cargo sqlc compose
```

Priority: CLI arg > env var > default (`sqlc` / `.sql`).

### Library usage

Add `sql-composer` to your `Cargo.toml`:

```toml
[dependencies]
sql-composer = "0.0.4"
```

Parse a template and compose it into final SQL:

```rust,ignore
use sql_composer::parser::parse_template;
use sql_composer::composer::Composer;
use sql_composer::types::{Dialect, TemplateSource};

let input = "SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();

let composer = Composer::new(Dialect::Postgres);
let result = composer.compose(&template).unwrap();

// Bind params are alphabetically ordered for numbered dialects
assert_eq!(result.sql, "SELECT set_num, name, year FROM lego_sets WHERE year = $2 AND theme_id = $1");
assert_eq!(result.bind_params, vec!["theme_id", "year"]);
```

## Template Syntax

All macros use the syntax `:command()`. SQL outside of macros is passed through unchanged. Lines starting with `#` are template comments and are stripped during composition.

### `:bind(name)`

Creates one or more dialect-specific placeholders and adds the name to the bind parameter list.

```sql
SELECT set_num, name, year
FROM lego_sets
WHERE theme_id = :bind(theme_id) AND year >= :bind(min_year)
-- Postgres: ... WHERE theme_id = $2 AND year >= $1
-- MySQL:    ... WHERE theme_id = ? AND year >= ?
-- SQLite:   ... WHERE theme_id = ?2 AND year >= ?1
```

For multi-value bindings (e.g. `IN` clauses), pass multiple values for the same name at compose time:

```sql
SELECT name, rgb FROM lego_colors WHERE id IN (:bind(color_ids))
-- With 3 values -> Postgres: ... WHERE id IN ($1, $2, $3)
```

#### Validation options

`:bind()` supports optional validation constraints:

```sql
-- Require at least 1 value
:bind(color_ids EXPECTING 1)

-- Require between 1 and 10 values
:bind(color_ids EXPECTING 1..10)

-- Allow NULL values
:bind(parent_theme_id NULL)
```

### `:compose(path)` and `:compose(path, @slot = path)`

Include another complete SQL template, resolved from configured search paths.

```sql
# Compose the shared part details into a CTE, then query a specific category
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT part_name, color_name, quantity
FROM set_part_details
WHERE category_name = :bind(category_name)
ORDER BY quantity DESC
```

Compose targets can be SQL fragments (like a CTE without a `SELECT`) — they just need to produce valid SQL when composed into the calling template. Compose references are resolved against the search paths added via `Composer::add_search_path()`. Circular references are detected and produce an error.

#### Slot arguments

Templates can declare **slots** — named placeholders that the caller fills with concrete file paths:

```sql
# In the shared template: declare a slot
:compose(@filter)

# At the call site: fill the slot
:compose(shared/base.sqlc, @filter = filters/by_color.sqlc)

# Multiple slots
:compose(shared/report.sqlc, @source = data.sqlc, @filter = filters/active.sqlc)

# The target can itself be a slot
:compose(@slot)
:compose(@slot, @inner = detail.sqlc)
```

**Design rules:**
- `@` is only special inside `:compose()`. In regular SQL, `@anything` passes through unchanged.
- Slots are **explicitly scoped**. Child templates do NOT inherit parent slots. Pass them explicitly.
- Missing slots are a compile error.
- Slot names: alphanumeric, hyphens, underscores. No dots.

### `:define(path)`

`:define` composes a template's body where `:compose` would name it. The name in front of its `AS` stands for that template everywhere under the statement that defines it: every `:compose` of the same template below, at any depth, says `SELECT * FROM <name>` instead of repeating the body. A statement can so name its relations once, in a `WITH` clause:

```sql
# The theme closure's body appears once, here. The count's source composes
# shared/theme_closure.sqlc too, and reads it as SELECT * FROM theme_closure.
WITH theme_closure AS (
    :define(shared/theme_closure.sqlc)
)
:compose(reports/count_theme_parts.sqlc)
```

The name is the statement's own, and composing any other statement is unaffected. `CREATE [OR REPLACE] VIEW <name> AS :define(<path>)` names a template the same way, and a template of such statements is a view registry: loaded with `Composer::load_view_registry()` or `cargo sqlc compose --views`, it makes every composition say `SELECT * FROM <name>` for each template it registers. Where a statement defines a name for a template that is also a view, the statement's name is the one used. `:define` takes slot arguments as `:compose` does.

### `:union(sources...)` and `:count(sources...)`

Combine multiple template sources:

```sql
-- Union queries across different set types
:union(queries/technic_sets.sqlc, queries/city_sets.sqlc)

-- With DISTINCT or ALL modifiers
:union(DISTINCT queries/technic_sets.sqlc, queries/city_sets.sqlc)

-- Count rows from a template
:count(queries/star_wars_sets.sqlc)

-- Count specific columns
:count(set_num, name OF queries/star_wars_sets.sqlc)
```

### `:intersect(sources...)` and `:except(first, rest...)`

The other two set operations:

```sql
-- Rows every source holds
:intersect(queries/technic_sets.sqlc, queries/sets_since_2010.sqlc)

-- Rows of the first source that no later source holds: the first is the minuend,
-- so order matters, and :except(a, b, c) is a less everything in b or c
:except(queries/technic_sets.sqlc, queries/city_sets.sqlc)

-- With ALL (bag semantics) or DISTINCT modifiers, as for :union
:except(ALL queries/technic_sets.sqlc, queries/city_sets.sqlc)
```

Each source is wrapped in a derived table (`SELECT * FROM (...) AS _except_1`), and so is the whole
(`AS _except`). The composer does not parse the SQL it splices, and `INTERSECT` binds more tightly
than `UNION` and `EXCEPT`. A source that is itself a union would otherwise regroup:
`a EXCEPT b UNION c` means `(a EXCEPT b) UNION c`, which returns a plausible table and raises no
error. Closing the whole also lets an `:intersect` or `:except` be a `:union` member.

`:intersect` and `:except` take no `columns OF` list. They compare whole rows, so a list is refused
rather than ignored. SQLite accepts neither `ALL` nor `DISTINCT` after `INTERSECT` or `EXCEPT`.

## Driver Crates

Each driver crate wraps a database connection with a `ComposerConnection` (sync) or `ComposerConnectionAsync` (async) trait implementation that composes templates and resolves bind values in one step.

### rusqlite

```toml
[dependencies]
sql-composer = "0.0.4"
sql-composer-rusqlite = "0.0.4"
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

### DuckDB

```toml
[dependencies]
sql-composer = "0.0.4"
sql-composer-duckdb = "0.0.4"
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
// DuckDB uses Postgres-style $N placeholders
let composer = Composer::new(Dialect::Postgres);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn duckdb::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

### PostgreSQL (sync + async)

```toml
[dependencies]
sql-composer = "0.0.4"
sql-composer-postgres = "0.0.4"  # both sync and async enabled by default
# sql-composer-postgres = { version = "0.0.4", default-features = false, features = ["async"] }
```

**Features:** `sync` (enables `postgres` crate), `async` (enables `tokio-postgres`). Both enabled by default.

```rust,ignore
// Async (tokio-postgres)
use sql_composer_postgres::{PgClient, boxed_params};
use sql_composer::driver::ComposerConnectionAsync;

let (client, connection) = tokio_postgres::connect("host=localhost dbname=lego", tokio_postgres::NoTls).await?;
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

let client = postgres::Client::connect("host=localhost dbname=lego", postgres::NoTls)?;
let conn = PgConnection::from_client(client);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn postgres::types::ToSql + Sync>]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

### MySQL (sync + async)

```toml
[dependencies]
sql-composer = "0.0.4"
sql-composer-mysql = "0.0.4"  # both sync and async enabled by default
# sql-composer-mysql = { version = "0.0.4", default-features = false, features = ["async"] }
```

**Features:** `sync` (enables `mysql` crate), `async` (enables `mysql_async`). Both enabled by default.

```rust,ignore
// Async (mysql_async)
use sql_composer_mysql::MysqlConn;
use sql_composer::driver::ComposerConnectionAsync;

let pool = mysql_async::Pool::new("mysql://root@localhost/lego");
let conn = pool.get_conn().await?;
let conn = MysqlConn::from_conn(conn);

let values = bind_values!("year" => [mysql_async::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values).await?;
```

```rust,ignore
// Sync (mysql)
use sql_composer_mysql::MysqlConnection;
use sql_composer::driver::ComposerConnection;

let conn = mysql::Conn::new("mysql://root@localhost/lego")?;
let conn = MysqlConnection::from_conn(conn);

let values = bind_values!("year" => [mysql::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

### sqlx (verification + validation)

```toml
[dependencies]
sql-composer-sqlx = "0.0.4"                                        # postgres verification (default)
# sql-composer-sqlx = { version = "0.0.4", features = ["validate"] } # add offline syntax checking
# sql-composer-sqlx = { version = "0.0.4", features = ["mysql"] }    # mysql instead of postgres
```

**Features:** `postgres` (default, enables live verification against PostgreSQL), `mysql` (live verification against MySQL), `validate` (offline syntax checking via `sqlparser`).

```rust,ignore
use sql_composer_sqlx::verify_postgres;

// Verify composed queries against a live database
let stmts = vec![&composed_sql];
verify_postgres("postgres://localhost/lego", &stmts).await?;
```

```rust,ignore
use sql_composer_sqlx::validate_syntax;
use sql_composer::Dialect;

// Offline syntax validation (no database needed, requires "validate" feature)
validate_syntax("SELECT set_num, name FROM lego_sets WHERE year = $1", Dialect::Postgres)?;
```

## Workspace Structure

```
crates/
  sql-composer/           # Core: parser (winnow), types, composer
  sql-composer-rusqlite/  # rusqlite driver (ComposerConnection)
  sql-composer-duckdb/    # DuckDB driver (ComposerConnection)
  sql-composer-postgres/  # PostgreSQL driver (sync + async)
  sql-composer-mysql/     # MySQL driver (sync + async)
  sql-composer-sqlx/      # sqlx integration (verify, validate)
  cargo-sqlc/             # CLI pre-compiler
examples/
  lego/                   # Runnable example: every template feature on the Lego database
  lego-generator/         # sqlc-brickgen: a large catalogue built from the Lego database, for Postgres
```

## Core Library Features

The `sql-composer` core crate has the following optional features:

| Feature | Description |
|---------|-------------|
| `std` | Standard library support (enabled by default) |
| `serde` | Derive `Serialize`/`Deserialize` for core types (`Template`, `Element`, etc.) |

```toml
# With serde support
sql-composer = { version = "0.0.4", features = ["serde"] }
```

## Core Types

| Type | Description |
|------|-------------|
| `Template` | A parsed SQL template containing elements |
| `Element` | SQL literal, bind macro, compose reference, or command |
| `Binding` | A `:bind()` with name, optional value count constraints, and nullable flag |
| `ComposeRef` | A `:compose()` or `:define()` reference with optional slot arguments |
| `ComposeTarget` | Path, slot reference (`@name`) or `:define` path in a compose target |
| `SlotAssignment` | A `@name = path` slot binding in `:compose()` |
| `Command` | A `:count()`, `:union()`, `:intersect()` or `:except()` combinator |
| `Composer` | Transforms templates into final SQL with placeholders |
| `ComposedSql` | The result: final SQL string + ordered bind param names |
| `Dialect` | Target database: `Postgres`, `Mysql`, `Sqlite` |

## How Bind Parameter Ordering Works

Numbered dialects (Postgres, SQLite) use a two-pass approach:

1. **Collect** — scan all `:bind()` names into a deduplicated, alphabetically sorted set
2. **Assign** — give each unique name a 1-based index (`$1`, `$2`, ...)
3. **Emit** — replace each `:bind()` with its assigned placeholder

This means the same bind name always gets the same placeholder number, regardless of where it appears in the template. Alphabetical ordering provides deterministic, predictable parameter positions.

MySQL uses document-order positional `?` placeholders with no deduplication, matching its native parameter style.

## Project Status

This project is under active development. The core API (`Template`, `Composer`, `ComposedSql`) is stabilizing, but may still change before 0.1.0. Several crates in this workspace are being production tested to flush out remaining API issues.

## License

MIT, copyright Kenneth Allen Flegal: see [LICENSE](LICENSE). Each crate carries the same file.
