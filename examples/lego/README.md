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

Each subcommand demonstrates a different sql-composer feature:

```sh
# :compose() + :bind() — list parts for a set
cargo run -p lego-example -- parts 75192-1

# :compose() in INSERT SELECT — populate summary table
cargo run -p lego-example -- summary 75192-1

# :compose() in UPDATE — sync spare counts
cargo run -p lego-example -- spares 75192-1

# @slot composition — filter parts by color
cargo run -p lego-example -- by-color 75192-1 "Black"

# @slot composition — filter parts by category
cargo run -p lego-example -- by-category 75192-1 "Technic"

# Multi-value :bind() IN clause — find sets by theme IDs
cargo run -p lego-example -- themes 2010 1 22 158

# :union() — combine Technic and City sets
cargo run -p lego-example -- combined 2020

# :count(DISTINCT) — count distinct parts in a theme
cargo run -p lego-example -- count "Star Wars"

# Run all examples with default values
cargo run -p lego-example -- all
```

## Template Structure

```
sqlc/
  shared/
    set_part_details.sqlc          # Canonical 4-table CTE
    filtered_set_parts.sqlc        # @filter slot base query
  sets/
    select_set_parts.sqlc          # SELECT via :compose()
    select_colored_parts.sqlc      # @filter = by_color
    select_category_parts.sqlc     # @filter = by_category
    select_sets_by_themes.sqlc     # Multi-value :bind() IN clause
  filters/
    by_color.sqlc                  # Standalone color filter
    by_category.sqlc               # Standalone category filter
  reports/
    insert_set_summary.sqlc        # INSERT SELECT via :compose()
    combined_theme_sets.sqlc       # :union() of two queries
    count_theme_parts.sqlc         # :count(DISTINCT)
  inventory/
    update_spare_counts.sqlc       # UPDATE via :compose()
  queries/
    technic_sets.sqlc              # Standalone (for union source)
    city_sets.sqlc                 # Standalone (for union source)
    theme_set_parts.sqlc           # Standalone (for count source)
```

## Feature Coverage

| Feature | Template(s) |
|---------|------------|
| `:bind(name)` | All templates |
| `:bind(name EXPECTING n..m)` | `select_sets_by_themes.sqlc` |
| `:compose(path)` | `select_set_parts`, `insert_set_summary`, `update_spare_counts` |
| `:compose(path, @slot = path)` | `select_colored_parts`, `select_category_parts` |
| `:compose(@slot)` | `filtered_set_parts.sqlc` |
| `:union(src, src)` | `combined_theme_sets.sqlc` |
| `:count(DISTINCT col OF src)` | `count_theme_parts.sqlc` |
| `#` comments | All `.sqlc` files |
| Multi-value bind (IN) | `select_sets_by_themes.sqlc` |
