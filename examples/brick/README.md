# Lego Example

A full showcase of sql-composer features using the [Lego database](https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql).

## Setup

The example runs on the database sqlc-brickgen builds. With PostgreSQL running and `psql` on the
`PATH`, from the repository root:

```sh
createdb sqlc_brick
curl -sSfL https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql \
    | psql -X -q -v ON_ERROR_STOP=1 -o /dev/null -d sqlc_brick
cargo run --release -p sqlc-brickgen -- --database-url postgres:///sqlc_brick \
    --size small --traps none --indexes keys,composite
cargo run -p brick-example -- migrate
```

1. `createdb` makes the example's database.
2. `psql` loads Neon's sample dump into its `public` schema: the real catalogue the generator
   reads. The load stops at the first failed statement.
3. The generator writes the catalogue and 20 thousand synthesized sets, with their builders,
   collections and purchases, into the schema `lego`, which it drops and recreates on every run.
   `--traps none` leaves out the traps it plants, and `--indexes keys,composite` builds the keys and
   the composite indexes for the joins, without the search indexes and the extensions they need.
4. `migrate` adds the example's own tables (`set_category_summary`, `inventory_tracking`) to the
   same schema. Each migration is safe to run again, and runs again after the generator, whose run
   drops them with the schema.

The queries search the schema `lego` alone; `--schema` names another.

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

Each subcommand demonstrates a different sql-composer feature. Every query returns a set of rows
with their keys, never a bag: a line of an inventory is `(inventory_id, part_num, color_id,
is_spare)`, and a set is its `set_num`.

```sh
# :compose() + :bind() — list parts for a set, version by version
cargo run -p brick-example -- parts 10182-1
cargo run -p brick-example -- parts 75053-1      # a set LEGO sold in two versions

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

# Check the example's laws (exits 1 if any fails)
cargo run -p brick-example -- laws

# Run all examples with default values, then the laws
cargo run -p brick-example -- all
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
