# Brick example

Every sql-composer feature, and more of PostgreSQL's SQL, on the database sqlc-brickgen builds.

## Setup

The example runs on PostgreSQL 15 or later (for `MERGE`). With the server running and `psql` on the
`PATH`, from the repository root:

```sh
createdb sqlc_brick
curl -sSfL https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql \
    | psql -X -q -v ON_ERROR_STOP=1 -o /dev/null -d sqlc_brick
cargo run --release -p sqlc-brickgen -- --database-url postgres:///sqlc_brick \
    --size small --traps none --indexes keys,composite,search
cargo run -p brick-example -- migrate
```

1. `createdb` makes the example's database.
2. `psql` loads Neon's sample dump into `public`: the real catalogue the generator reads. The load
   stops at the first failed statement.
3. The generator writes the catalogue and 20 thousand synthesized sets, with their builders,
   collections and purchases, into the schema `lego`, which it drops and recreates on every run.
   `--traps none` leaves out the traps it plants. `--indexes keys,composite,search` builds the keys,
   the composite indexes for the joins, and the search indexes: the parts' names by their words and
   by trigram, the sets' names by trigram, the builders' homes on the earth, and the postcodes by
   their prefix. Before it writes anything, it creates the extensions those indexes need in a schema
   of their own, `extensions`, apart from `public`: `cube` and `earthdistance` for distances on the
   earth, and `pg_trgm` for patterns. Only a superuser can create `earthdistance`; for any other
   role, the generator stops and says what a superuser has to run.
4. `migrate` adds the example's own tables (`set_category_summary`, `inventory_tracking`) to the
   same schema. Each migration is safe to run again, and runs again after the generator, whose run
   drops them with the schema.

The queries search the schema `lego`, then the schemas that hold the three extensions, and never
`public`, where the seed lies under the same table names. `--schema` names another schema than
`lego`. Extensions the database already held in `public` stay there, out of the queries' reach, and
the searches by place then stop at their first call to `ll_to_earth`.

### Connecting

The database is the one `--database-url` names, or `SQLC_BRICK_DATABASE_URL` when the flag is left
out. With neither, it is `postgres:///sqlc_brick`: the database `sqlc_brick` on the local server,
through its Unix socket, as the current user.

```sh
cargo run -p brick-example -- --database-url postgres://user@host/dbname migrate
SQLC_BRICK_DATABASE_URL=postgres://user@host/dbname cargo run -p brick-example -- all
```

The example speaks TLS (rustls), as a managed Postgres usually requires. Ask for it in the URL:
`?sslmode=require` encrypts without checking the server's certificate, and `?sslmode=verify-full`
also checks it, against Mozilla's root certificates, bundled, and any file `&sslrootcert=<file>`
names. With no `sslmode`, a connection tries TLS and falls back to plain when the server does not
offer it.

The subcommands read the templates from `--sqlc-dir`, `examples/brick/sqlc` by default. That path is
relative to the directory the command runs in, so run them from the repository root, or name the
directory.

### Data and licences

The example ships none of the catalogue: the `curl` above downloads it to your machine. It is Neon's
sample dump, `lego.sql` in
[neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs), a
repository under the MIT licence, whose README gives the data's source as Kaggle's
[LEGO Database](https://www.kaggle.com/datasets/rtatman/lego-database) (rtatman), under CC0,
public domain. The data comes originally from [Rebrickable](https://rebrickable.com). Rebrickable's
terms (effective 2026-07-09, as read on 2026-10-04) allow it to be used for any purpose, commercial
use included, and ask that it be credited as sourced from Rebrickable: credit Rebrickable wherever
you use it. The same terms forbid using any Rebrickable content to train AI models.

LEGO® is a trademark of the LEGO Group, which does not sponsor, authorise or endorse this project.

### Compose templates (optional)

Generate `.sql` files from `.sqlc` templates to see the composed output:

```sh
cargo sqlc compose --source examples/brick/sqlc --target examples/brick/.sql --skip-prepare
```

### Run examples

Each subcommand runs one template, for a feature of sql-composer or of PostgreSQL. Every query
returns a set of rows with their keys, never a bag: a line of an inventory is
`(inventory_id, part_num, color_id, is_spare)`, and a set is its `set_num`.

```sh
# :compose() + :bind() — list parts for a set, version by version
cargo run -p brick-example -- parts 10182-1
cargo run -p brick-example -- parts 75053-1      # a set sold in two versions

# :compose() in INSERT SELECT — populate the summary table, one row per (set, version, category)
cargo run -p brick-example -- summary 10182-1

# :compose() in INSERT … ON CONFLICT, then in UPDATE — track a set's parts and sync spare counts
cargo run -p brick-example -- spares 10182-1

# @slot composition — filter parts by color, or by category
cargo run -p brick-example -- by-color 10182-1 "Black"
cargo run -p brick-example -- by-category 10182-1 "Plates"

# Multi-value :bind() IN clause — find sets in theme scopes (each theme and every theme below it)
cargo run -p brick-example -- themes 2010 1 52 158

# :union() — combine Technic and City sets, each labelled with its scope
cargo run -p brick-example -- combined 2010

# :count(DISTINCT) — count distinct moulds in a theme scope (158 is Star Wars)
cargo run -p brick-example -- count 158

# :intersect() and :except() — moulds Technic and City share, and moulds only City uses
cargo run -p brick-example -- shared-moulds
cargo run -p brick-example -- city-only-moulds

# Window functions — a builder's purchases in order, numbered, with the pieces bought so far
cargo run -p brick-example -- history 3326

# DISTINCT ON — each builder's latest purchase
cargo run -p brick-example -- latest

# LATERAL — each root theme's three newest sets
cargo run -p brick-example -- newest

# ROLLUP — purchases per year and root theme, with each year's subtotal and the grand total
cargo run -p brick-example -- rollup 2016 2017

# percentile_cont — the median set size of each root theme
cargo run -p brick-example -- sizes

# generate_series — every year from the first release to the last, the years with none kept
cargo run -p brick-example -- years

# NOT EXISTS — the sets no builder ever bought
cargo run -p brick-example -- never-bought

# Division — the builders who own every set of a theme scope (463 is Spybiotics)
cargo run -p brick-example -- complete 463

# array_agg — a set's colours, version by version
cargo run -p brick-example -- colors 75053-1

# jsonb — a set as one JSON document
cargo run -p brick-example -- document 10182-1

# MERGE — keep a set's summary in step with its totals
cargo run -p brick-example -- merge 75053-1

# Full text in the @filter slot — a set's lines whose part names hold the words
cargo run -p brick-example -- by-words 10182-1 "window"

# Full text, ranked — the parts that best match a search, the matched words marked
cargo run -p brick-example -- search "technic beam"

# Regular expressions — a set's lines whose part names match, and bricks' sizes read from names
cargo run -p brick-example -- by-pattern 10182-1 '^(plate|tile) 1 x \d+$'
cargo run -p brick-example -- brick-sizes

# earthdistance in the @area slot — what the builders within 5 km of London (67) bought
cargo run -p brick-example -- bought-near 67 5

# A regular expression on postcodes in the @area slot — what London's SW district bought
cargo run -p brick-example -- bought-in '^SW'

# <-> in a LATERAL — the five builders nearest London, walked nearest first
cargo run -p brick-example -- nearest 67

# Check the example's laws (exits 1 if any fails)
cargo run -p brick-example -- laws

# Run all examples with default values, then the laws
cargo run -p brick-example -- all
```

### What the queries promise

- **A set's parts are listed version by version.** Some sets were sold in more than one version,
  each with its own inventory. The parts of such a set come back labelled with their version, and
  no total adds two versions together.
- **No line is dropped.** A line whose part is missing from the parts catalogue is kept, marked
  `uncatalogued`.
- **A theme scope is a theme and every theme below it,** chosen by id (`shared/theme_closure.sqlc`).
  Several themes can share a name, so a name never chooses a scope.
- **A filter is a set of `(part_num, color_id)` patterns** in which `NULL` means "any value". A
  category belongs to a part and a color belongs to a line, so each filter names only the column
  it is about, and each matching line comes back once.
- **Bind values are matched by name,** so a value can never land on the wrong parameter.

The `laws` subcommand checks each promise against the same answer computed directly in SQL, and
runs two templates on mock tables: rows of its own, which the catalogue never holds.

## Template Structure

```
sqlc/
  shared/
    set_part_details.sqlc          # Canonical part resolution: a set's lines, per version
    theme_closure.sqlc             # Every theme with every theme below it
    filtered_set_parts.sqlc        # @filter slot base query
    scope_moulds.sqlc              # @scope slot: the moulds a theme scope uses
    scope_sets.sqlc                # @scope slot: a theme scope's sets, labelled with the scope
    area_purchases.sqlc            # @area slot: what an area's builders bought, by root theme
    purchase_sets.sqlc             # Every purchase with its builder and set, through its collection row
    set_roots.sqlc                 # Every set with its root theme
    set_category_totals.sqlc       # A set's piece totals per version and category
  scopes/
    technic.sqlc                   # The Technic scope (theme ids, with the scope's own id)
    city.sqlc                      # The City scope (theme ids, with the scope's own id)
  areas/
    near_city.sqlc                 # earthdistance: the builders within a radius of a city
    postcodes.sqlc                 # ~: the builders whose postcode matches a pattern
  sets/
    select_set_parts.sqlc          # SELECT via :compose()
    select_colored_parts.sqlc      # @filter = by_color
    select_category_parts.sqlc     # @filter = by_category
    select_parts_by_words.sqlc     # @filter = by_words
    select_parts_by_pattern.sqlc   # @filter = by_pattern
    select_sets_by_themes.sqlc     # Multi-value :bind() IN clause
    never_bought.sqlc              # NOT EXISTS: the sets no builder bought
    set_colors.sqlc                # array_agg: a set's colours per version
    set_document.sqlc              # jsonb: a set as one document
  parts/
    search_parts.sqlc              # Full text, ranked: ts_rank and ts_headline
    brick_sizes.sqlc               # regexp_match: a brick's size read from its name
  filters/
    by_color.sqlc                  # Color filter patterns
    by_category.sqlc               # Category filter patterns
    by_words.sqlc                  # Full-text filter patterns: websearch_to_tsquery
    by_pattern.sqlc                # Regular-expression filter patterns: ~*
  reports/
    insert_set_summary.sqlc        # INSERT SELECT via :compose()
    merge_set_summary.sqlc         # MERGE via :compose()
    purchases_by_year_theme.sqlc   # ROLLUP: purchases per year and root theme
    theme_set_sizes.sqlc           # percentile_cont: the median set size per root theme
    release_years.sqlc             # generate_series: every year, the empty ones kept
    combined_theme_sets.sqlc       # :union() of two queries
    count_theme_parts.sqlc         # :count(DISTINCT)
    shared_moulds.sqlc             # :intersect() of two queries
    city_only_moulds.sqlc          # :except() of two queries
  builders/
    purchase_history.sqlc          # Window functions over a builder's purchases
    latest_purchases.sqlc          # DISTINCT ON: each builder's latest purchase
    complete_scope.sqlc            # Division: the builders who own a whole scope
    bought_near.sqlc               # @area = near_city
    bought_in_postcodes.sqlc       # @area = postcodes
    nearest.sqlc                   # <-> in a LATERAL: the builders nearest a city
  themes/
    newest_sets.sqlc               # LATERAL: each root theme's newest three
  inventory/
    track_set_parts.sqlc           # INSERT … ON CONFLICT via :compose()
    update_spare_counts.sqlc       # UPDATE via :compose()
  queries/
    technic_sets.sqlc              # @scope = technic (for the union)
    city_sets.sqlc                 # @scope = city (for the union)
    theme_set_parts.sqlc           # A theme scope's lines (for the count)
    technic_moulds.sqlc            # @scope = technic (for intersect and except)
    city_moulds.sqlc               # @scope = city (for intersect and except)
```

## Feature Coverage

| Feature | Template(s) |
|---------|------------|
| `:bind(name)` | Every template that takes a value |
| `:bind(name EXPECTING n..m)` | `select_sets_by_themes.sqlc` |
| `:compose(path)` | `select_set_parts`, `insert_set_summary`, `track_set_parts`, `update_spare_counts`, `theme_closure` users |
| `:compose(path, @slot = path)` | `select_colored_parts`, `select_category_parts`, `select_parts_by_words`, `select_parts_by_pattern`, `technic_moulds`, `city_moulds`, `technic_sets`, `city_sets`, `bought_near`, `bought_in_postcodes` |
| `:compose(@slot)` | `filtered_set_parts.sqlc`, `scope_moulds.sqlc`, `scope_sets.sqlc`, `area_purchases.sqlc` |
| `:union(src, src)` | `combined_theme_sets.sqlc` |
| `:count(DISTINCT col OF src)` | `count_theme_parts.sqlc` |
| `:intersect(src, src)` | `shared_moulds.sqlc` |
| `:except(src, src)` | `city_only_moulds.sqlc` |
| `#` comments | All `.sqlc` files |
| Multi-value bind (IN) | `select_sets_by_themes.sqlc` |
| Window functions | `purchase_history.sqlc` |
| `DISTINCT ON` | `latest_purchases.sqlc` |
| `LATERAL` | `newest_sets.sqlc` |
| `GROUPING SETS` (`ROLLUP`) | `purchases_by_year_theme.sqlc` |
| `percentile_cont … WITHIN GROUP` | `theme_set_sizes.sqlc` |
| `generate_series` | `release_years.sqlc` |
| Anti-join (`NOT EXISTS`) | `never_bought.sqlc` |
| Division (`NOT EXISTS … EXCEPT`) | `complete_scope.sqlc` |
| Arrays (`array_agg`) | `set_colors.sqlc` |
| `jsonb` | `set_document.sqlc` |
| `MERGE` | `merge_set_summary.sqlc` |
| Full text (`websearch_to_tsquery`, `ts_rank`, `ts_headline`) | `by_words.sqlc`, `search_parts.sqlc` |
| Regular expressions (`~*`, `~`, `regexp_match`) | `by_pattern.sqlc`, `brick_sizes.sqlc`, `postcodes.sqlc` |
| `earthdistance` (`earth_box`, `earth_distance`) | `near_city.sqlc` |
| Nearest first (`<->` in a `LATERAL`) | `nearest.sqlc` |
| Mock tables | two laws, on `complete_scope.sqlc` and `release_years.sqlc` |
