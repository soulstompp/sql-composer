# Lego Example

A full showcase of sql-composer features using the [Lego database](https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql).

## Setup

Make sure PostgreSQL is running, then:

```sh
cargo run -p lego-example -- setup
```

This single command will:
1. Download the [lego SQL dump](https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql) to `~/.cache/sql-composer/` (cached for future runs)
2. Create the `sqlc_lego` database via `createdb`
3. Load the lego data via `psql`
4. Run migrations to create the extra tables (`set_category_summary`, `inventory_tracking`)

`setup` drops and recreates the database the URL names, after disconnecting its sessions: point it
only at a database the example owns. The load stops at the first failed statement.

To use a different database URL:

```sh
cargo run -p lego-example -- --database-url postgres://user@host/dbname setup
```

### Compose templates (optional)

Generate `.sql` files from `.sqlc` templates to see the composed output:

```sh
cargo sqlc compose --source examples/lego/sqlc --target examples/lego/.sql --skip-prepare
```

### Run examples

Each subcommand demonstrates a different sql-composer feature. Every query returns a set of rows
with their keys, never a bag: a line of an inventory is `(inventory_id, part_num, color_id,
is_spare)`, and a set is its `set_num`.

```sh
# :compose() + :bind() — list parts for a set, version by version
cargo run -p lego-example -- parts 10182-1
cargo run -p lego-example -- parts 75053-1      # a set LEGO sold in two versions

# :compose() in INSERT SELECT — populate the summary table, one row per (set, version, category)
cargo run -p lego-example -- summary 10182-1

# :compose() in INSERT … ON CONFLICT, then in UPDATE — track a set's parts and sync spare counts
cargo run -p lego-example -- spares 10182-1

# @slot composition — filter parts by color, or by category
cargo run -p lego-example -- by-color 10182-1 "Black"
cargo run -p lego-example -- by-category 10182-1 "Plates"

# Multi-value :bind() IN clause — find sets in theme scopes (each theme and every theme below it)
cargo run -p lego-example -- themes 2010 1 52 158

# :union() — combine Technic and City sets, each labelled with its scope
cargo run -p lego-example -- combined 2010

# :count(DISTINCT) — count distinct moulds in a theme scope (158 is Star Wars)
cargo run -p lego-example -- count 158

# :intersect() and :except() — moulds Technic and City share, and moulds only City uses
cargo run -p lego-example -- shared-moulds
cargo run -p lego-example -- city-only-moulds

# Check the example's laws (exits 1 if any fails)
cargo run -p lego-example -- laws

# Run all examples with default values, then the laws
cargo run -p lego-example -- all
```

### What the queries promise

- **A set's parts are listed version by version.** LEGO sold some sets in more than one version,
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

The `laws` subcommand checks each promise against the same answer computed directly in SQL.

## Template Structure

```
sqlc/
  shared/
    set_part_details.sqlc          # Canonical part resolution: a set's lines, per version
    theme_closure.sqlc             # Every theme with every theme below it
    filtered_set_parts.sqlc        # @filter slot base query
    scope_moulds.sqlc              # @scope slot: the moulds a theme scope uses
  scopes/
    technic.sqlc                   # The Technic scope (theme ids)
    city.sqlc                      # The City scope (theme ids)
  sets/
    select_set_parts.sqlc          # SELECT via :compose()
    select_colored_parts.sqlc      # @filter = by_color
    select_category_parts.sqlc     # @filter = by_category
    select_sets_by_themes.sqlc     # Multi-value :bind() IN clause
  filters/
    by_color.sqlc                  # Color filter patterns
    by_category.sqlc               # Category filter patterns
  reports/
    insert_set_summary.sqlc        # INSERT SELECT via :compose()
    combined_theme_sets.sqlc       # :union() of two queries
    count_theme_parts.sqlc         # :count(DISTINCT)
    shared_moulds.sqlc             # :intersect() of two queries
    city_only_moulds.sqlc          # :except() of two queries
  inventory/
    track_set_parts.sqlc           # INSERT … ON CONFLICT via :compose()
    update_spare_counts.sqlc       # UPDATE via :compose()
  queries/
    technic_sets.sqlc              # Standalone (for union source)
    city_sets.sqlc                 # Standalone (for union source)
    theme_set_parts.sqlc           # Standalone (for count source)
    technic_moulds.sqlc            # @scope = technic (for intersect and except)
    city_moulds.sqlc               # @scope = city (for intersect and except)
```

## Feature Coverage

| Feature | Template(s) |
|---------|------------|
| `:bind(name)` | Every template that takes a value |
| `:bind(name EXPECTING n..m)` | `select_sets_by_themes.sqlc` |
| `:compose(path)` | `select_set_parts`, `insert_set_summary`, `track_set_parts`, `update_spare_counts`, `theme_closure` users |
| `:compose(path, @slot = path)` | `select_colored_parts`, `select_category_parts`, `technic_moulds`, `city_moulds` |
| `:compose(@slot)` | `filtered_set_parts.sqlc`, `scope_moulds.sqlc` |
| `:union(src, src)` | `combined_theme_sets.sqlc` |
| `:count(DISTINCT col OF src)` | `count_theme_parts.sqlc` |
| `:intersect(src, src)` | `shared_moulds.sqlc` |
| `:except(src, src)` | `city_only_moulds.sqlc` |
| `#` comments | All `.sqlc` files |
| Multi-value bind (IN) | `select_sets_by_themes.sqlc` |
