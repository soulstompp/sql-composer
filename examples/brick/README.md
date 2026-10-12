# Brick example

This example walks sql-composer's template language one template at a time, on a catalogue of
building-brick sets. Each step asks the catalogue a question and answers it with a template. Its
subcommand composes the template, prints the statement it composes to, and runs it.

The walk starts at a set's lines and builds on them. They are listed, written into tables, and
narrowed by a filter passed in. Then the themes the sets belong to are gathered into scopes, the
scopes combined as sets and counted, and each relation named once. The walk goes on into more of
PostgreSQL's SQL, into searches that read the indexes the catalogue is built with, and into a
roster of checks on the catalogue itself. It ends at `laws`, which checks what the steps promise
against a second spelling of the same answer, and `fit`, which checks each composed statement
against the plan PostgreSQL makes for it.

The catalogue is the database sqlc-brickgen builds from a real one: sets, their inventories and
parts, colours and themes, and the builders who buy the sets.

## Setup

The example runs on PostgreSQL 16 or later, where `earthdistance` works from a schema of its own.
With the server running and `psql` on the `PATH`, from the repository root:

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
   stops at the first failed statement. The dump's source and licences are at the end of this
   page.
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
   same schema. It is safe to run again, and runs again after the generator, whose run drops them
   with the schema.

The queries search the schema `lego`, then the schemas that hold the three extensions, and never
`public`, where the seed lies under the same table names. `--schema` names another schema than
`lego`. Extensions the database already held in `public` stay there, out of the queries' reach, and
the searches by place then stop at their first call to `ll_to_earth`.

*Connecting*, at the end of this page, says how to reach a database other than `sqlc_brick`.

## The walk

Every command runs from the repository root. The templates are in `examples/brick/sqlc`, and the
files they compose to are committed beside them, in `examples/brick/.sql`, each at its template's
path. This command checks that every composed file is still what its template composes to, and
writes nothing:

```sh
cargo sqlc compose --source examples/brick/sqlc --target examples/brick/.sql --skip-prepare --verify
```

Without `--verify`, it writes them again.

A subcommand composes its template as it runs. It prints the composed statement and the names of
its bind parameters, binds its values by name, and prints the rows. A value given under a name the
statement does not bind is refused, so a value never lands on another parameter.

### 1. A set's lines

What parts are in a set? Every line of each of the set's inventories, with its part, the part's
category and its colour. [`shared/set_part_details.sqlc`][shared/set_part_details.sqlc] states it
once:

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

Its `WHERE` is a selection: it keeps one set's rows, by the value bound to `set_num`. Its `SELECT`
list is a projection, the columns kept. Its joins match each line with its inventory, its part, the
part's category and its colour, and the `LEFT JOIN`s keep a line the catalogue cannot name.

A listing composes it, [`sets/select_set_parts.sqlc`][sets/select_set_parts.sqlc]:

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

It composes to [`.sql/sets/select_set_parts.sql`][.sql/sets/select_set_parts.sql]. The join's body
stands in the `WITH` where `:compose` was, `$1` stands where `:bind(set_num)` was, and the `#`
lines of both templates are gone.

```sh
cargo run -p brick-example -- parts 10182-1
cargo run -p brick-example -- parts 75053-1      # a set sold in two versions
```

Each prints the set's lines version by version: each version's inventory, its lines and its
pieces, spares left out. Every line of the set's inventories comes back, and none twice. A set sold
in two versions is listed and totalled per version. For both sets, each version's pieces are the
figure the catalogue files for the set, `num_parts`.

The next step takes the same relation, unchanged, into statements that write.

### 2. The same relation in an INSERT, an UPDATE and a MERGE

How many pieces of each category does a set hold, version by version, kept in a table?
[`shared/set_category_totals.sqlc`][shared/set_category_totals.sqlc] reduces a set's lines to one
row per version and category:

```sql
# A set's piece totals per version and category: one row per (version, category_name), the pieces
# that are not spares and the spares apart. A category with no spare lines has a spare total of 0,
# and one with only spare lines a part total of 0. A line with no category (its part is not in the
# catalogue) is not totalled here; the parts listing shows it.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    version,
    category_name,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_spare), 0) AS total_parts,
    COALESCE(SUM(quantity) FILTER (WHERE is_spare), 0) AS total_spare
FROM set_part_details
WHERE category_name IS NOT NULL
GROUP BY version, category_name
```

[`reports/insert_set_summary.sqlc`][reports/insert_set_summary.sqlc] writes those rows into
`set_category_summary`, one of the tables `migrate` creates:

```sql
# Populate the per-category part summary for a set: one row per (set, version, category). A set
# sold in several versions gets one summary per version, never a sum across them. The totals are
# the shared ones, so they always agree with the parts listing. Running it again updates each
# category's row rather than adding a second; a category the set no longer has keeps its row.
INSERT INTO set_category_summary (set_num, version, category_name, total_parts, total_spare)
SELECT
    :bind(set_num),
    version,
    category_name,
    total_parts,
    total_spare
FROM (
    :compose(shared/set_category_totals.sqlc)
) t
ON CONFLICT (set_num, version, category_name) DO UPDATE
SET total_parts = EXCLUDED.total_parts,
    total_spare = EXCLUDED.total_spare
```

It composes to [`.sql/reports/insert_set_summary.sql`][.sql/reports/insert_set_summary.sql]. The
totals stand in its `FROM`, and the set's lines inside them, two levels down. `set_num` is bound
twice, as the value inserted and as the set the lines are selected by, and both are `$1`.

```sh
cargo run -p brick-example -- summary 10182-1
```

It prints how many rows it wrote, then each version's categories, parts and spares, read back from
the table. Run twice, it leaves one row per version and category, each the totals of the set's
lines.

[`inventory/update_spare_counts.sqlc`][inventory/update_spare_counts.sqlc] composes the set's lines
into an `UPDATE`:

```sql
# Sync the spare counts in inventory_tracking with the set's lines, version by version: a tracked
# part's spare count is the sum of its spare lines across colours in that version, and 0 where it
# has none. A tracked part the set's lines no longer hold keeps its count.
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

The rows it updates are put in first by
[`inventory/track_set_parts.sqlc`][inventory/track_set_parts.sqlc], an `INSERT … SELECT DISTINCT`
over the same lines that leaves a part already tracked as it is. `spares` runs both, in that order:

```sh
cargo run -p brick-example -- spares 10182-1
```

It prints how many parts it started tracking, then how many rows it updated.

[`reports/merge_set_summary.sqlc`][reports/merge_set_summary.sqlc] keeps the summary with `MERGE`,
over the same totals. It updates a row the totals still have, inserts a missing one, and deletes
nothing:

```sh
cargo run -p brick-example -- merge 75053-1
```

It prints how many rows it merged. A summary whose rows were changed or removed by hand comes back,
after the merge, to what the `INSERT` writes.

Four statements write from a set's lines, and none of their templates spells the join. The composer
writes these statements and runs none of them. `ON CONFLICT` needs the unique key each
table is created with, and the database checks that when the statement runs.

The next step keeps the set's lines and narrows them, by a filter each caller chooses.

### 3. A filter passed as a slot

Which of a set's lines are plates, and which are black? One base answers both, with the filter left
open as a slot. [`shared/filtered_set_parts.sqlc`][shared/filtered_set_parts.sqlc] is a set's
lines, kept by whatever relation a caller passes as `@filter`:

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

A filter is a relation of patterns. [`filters/by_category.sqlc`][filters/by_category.sqlc] gives a
pattern for each part in a category, with any colour:

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

[`sets/select_category_parts.sqlc`][sets/select_category_parts.sqlc] fills the slot with it:

```sql
# A set's lines whose part is in the category category_name, one row per line, keyed by the line:
# the set's lines with the category's filter in the @filter slot.
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_category.sqlc)
```

It composes to [`.sql/sets/select_category_parts.sql`][.sql/sets/select_category_parts.sql]. The
filter's body stands in the `WITH` where `:compose(@filter)` was. `category_name` is `$1` and
`set_num` is `$2`, in the order of their names. The base, with its slot open, is not a query, so
`.sql/shared` holds no file for it.

```sh
cargo run -p brick-example -- by-category 10182-1 "Plates"
cargo run -p brick-example -- by-color 10182-1 "Black"
```

`by-color` fills the same slot with [`filters/by_color.sqlc`][filters/by_color.sqlc], a colour with
any part, in [`sets/select_colored_parts.sqlc`][sets/select_colored_parts.sqlc]. Each prints the
set's lines that match, each line once. A category belongs to a part and a colour to a line, so
each filter names only the column it is about. The base is spelled once, whatever the filter.

The searches of steps 15 and 16 fill the same slot. The next step turns from a set's lines to the
themes the sets belong to.

### 4. The theme closure

Which themes lie under a theme? A theme has a parent, and its parent a parent of its own. A theme
scope is a theme and every theme below it, at any depth.
[`shared/theme_closure.sqlc`][shared/theme_closure.sqlc] pairs every theme with every theme at or
below it:

```sql
# Every theme paired with every theme at or below it: one row per (ancestor_id, theme_id), each
# theme its own ancestor at depth 0. A theme scope is a theme's whole subtree, so a scope joins this
# relation on theme_id and picks its ancestor_id. It never matches one theme id alone, and never a
# name, which several themes can share.
# The CYCLE clause guarantees the walk ends even if a parent_id ever pointed back into its own
# subtree; such a theme would be marked, and left out.
WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle
```

It binds nothing, so it composes to itself with its `#` lines gone,
[`.sql/shared/theme_closure.sql`][.sql/shared/theme_closure.sql], and the composed file runs alone:

```sh
PGOPTIONS='-c search_path=lego' psql -d sqlc_brick -f examples/brick/.sql/shared/theme_closure.sql
```

The rows of one `ancestor_id` are one scope: that theme, and every theme below it, each at its
depth.

The steps that follow compose it.

### 5. A list of ids

Which sets from 2010 on lie in Technic, City or Star Wars, or in any theme below them?
[`sets/select_sets_by_themes.sqlc`][sets/select_sets_by_themes.sqlc] takes the themes as one list,
bound to an `IN`:

```sql
# Find sets in any of the given theme scopes: each given theme and every theme below it. One row per
# set, however many of the given scopes contain it. The ORDER BY ends on set_num, so the order is
# total. Within a year, a set with no part count comes before the counted ones; the ORDER BY says
# so, because databases disagree on where a NULL sorts. A set with no year is never at or after
# min_year, so the year needs no such term. The given themes are one list, bound to an IN.
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

The `EXISTS` asks only whether a set's theme lies in one of the scopes, and adds none of the
closure's rows to the set's. `EXPECTING 1..20` says the list holds from one to twenty ids.

The committed file, [`.sql/sets/select_sets_by_themes.sql`][.sql/sets/select_sets_by_themes.sql],
has one placeholder for the list, `IN ($2)`. The subcommand composes the template with its values,
one placeholder for each id. For three ids, the `IN` is `IN ($2, $3, $4)`, and the bind parameters
are `min_year`, then `theme_ids` three times. Given twenty-one, composing refuses the list:
`binding 'theme_ids' was given 21 value(s), and expects 1..20`.

```sh
cargo run -p brick-example -- themes 2010 1 52 158
```

1 is Technic, 52 City and 158 Star Wars. It prints the sets, newest first, each with the name of
its own theme.

The next step takes one scope at a time, as a relation of its own, and combines scopes as sets.

### 6. Scopes, and their union

Which sets from 2010 on are Technic's, and which are City's, each labelled with its scope? A scope
is a relation of its own. [`scopes/technic.sqlc`][scopes/technic.sqlc] is Technic's: each theme in
it, with Technic's own id as `scope_id`.

```sql
# The Technic scope: the theme technic_theme_id and every theme below it, one row per theme_id, each
# with the scope's own theme as scope_id.
SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    :compose(shared/theme_closure.sqlc)
) tc
WHERE tc.ancestor_id = :bind(technic_theme_id)
```

[`scopes/city.sqlc`][scopes/city.sqlc] is City's, the same with `city_theme_id`. Each scope is
spelled once, and every question about it composes it.
[`shared/scope_sets.sqlc`][shared/scope_sets.sqlc] takes a scope in its `@scope` slot, and labels
each set with the name of the scope's own theme:

```sql
# The sets of a theme scope released in min_year or later: one row per set, keyed by set_num, each
# labelled with the name of the scope's own theme. The caller decides the scope (the @scope slot):
# one row per theme_id, with the scope's theme as scope_id. The label is read through scope_id, so
# it always names the scope that chose the rows.
SELECT s.set_num, s.name, s.year, s.num_parts, scope.name AS theme_group
FROM lego_sets s
JOIN (
    :compose(@scope)
) sc ON sc.theme_id = s.theme_id
JOIN lego_themes scope ON scope.id = sc.scope_id
WHERE s.year >= :bind(min_year)
```

[`queries/technic_sets.sqlc`][queries/technic_sets.sqlc] and
[`queries/city_sets.sqlc`][queries/city_sets.sqlc] fill the slot, one scope each.
[`reports/combined_theme_sets.sqlc`][reports/combined_theme_sets.sqlc] is their union:

```sql
# Technic's sets and City's, each released in min_year or later and labelled with its scope: one row
# per (theme_group, set_num). A set in both scopes comes back once under each label.
:union(queries/technic_sets.sqlc, queries/city_sets.sqlc)
```

It composes to [`.sql/reports/combined_theme_sets.sql`][.sql/reports/combined_theme_sets.sql].
Each source is wrapped in a derived table of its own, `_union_1` and `_union_2`, and so is the
whole, `_union`. Both sources read `min_year`, and it is one placeholder, `$2`. City's theme is `$1`
and Technic's `$3`. A bare `:union` writes `UNION`, which removes duplicate rows. The label is part
of the row, so a set in both scopes is two rows, and both stay.

```sh
cargo run -p brick-example -- combined 2010
```

It prints each set with its scope's name. Each scope's sets from that year on are there, each
under its scope's name, and no row comes twice.

The next step asks the same two scopes for their moulds, and takes their intersection and their
difference.

### 7. Intersection and difference

Which moulds do Technic and City sets share, and which does City use alone? A mould is a part
number. [`shared/scope_moulds.sqlc`][shared/scope_moulds.sqlc] is the moulds any set of a scope
uses, the scope in its `@scope` slot:

```sql
# The moulds (part numbers) used by any set in a theme scope: one row per part_num. The caller
# decides the scope (the @scope slot): one row per theme_id, with the scope's own theme as scope_id.
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        :compose(@scope)
    ) sc
)
```

[`queries/technic_moulds.sqlc`][queries/technic_moulds.sqlc] and
[`queries/city_moulds.sqlc`][queries/city_moulds.sqlc] fill it, one scope each.
[`reports/shared_moulds.sqlc`][reports/shared_moulds.sqlc] is their intersection:

```sql
# The moulds both Technic and City sets use: one row per part_num.
:intersect(queries/technic_moulds.sqlc, queries/city_moulds.sqlc)
```

and [`reports/city_only_moulds.sqlc`][reports/city_only_moulds.sqlc] their difference, City's less
Technic's:

```sql
# The moulds City sets use and no Technic set does: one row per part_num. The order of the sources
# matters: the first is the one the second is taken from.
:except(queries/city_moulds.sqlc, queries/technic_moulds.sqlc)
```

Both compose as the union does, each source wrapped and the whole wrapped:
[`.sql/reports/shared_moulds.sql`][.sql/reports/shared_moulds.sql] and
[`.sql/reports/city_only_moulds.sql`][.sql/reports/city_only_moulds.sql]. Each source composes its
scope, and each scope the theme closure, so each file holds the closure twice.

```sh
cargo run -p brick-example -- shared-moulds
cargo run -p brick-example -- city-only-moulds
```

Each prints how many moulds it found. The two split City's moulds: none is in both, and together
they are all of them.

The next step reduces a relation to one figure.

### 8. Count

How many distinct moulds do the sets of Star Wars use, the sets of every theme below it among them?
[`queries/theme_set_parts.sqlc`][queries/theme_set_parts.sqlc] is every line of every set in a
scope:

```sql
# Every line of every inventory of every set in a theme scope (the theme and every theme below it),
# one row per line, keyed by the line.
SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
JOIN (
    :compose(shared/theme_closure.sqlc)
) tc ON tc.theme_id = s.theme_id
WHERE tc.ancestor_id = :bind(theme_id)
```

[`reports/count_theme_parts.sqlc`][reports/count_theme_parts.sqlc] counts its distinct part
numbers:

```sql
# How many distinct moulds (part numbers) the sets of a theme scope use, the theme and every theme
# below it: one row, one count.
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

It composes to [`.sql/reports/count_theme_parts.sql`][.sql/reports/count_theme_parts.sql]:

```sql
SELECT COUNT(DISTINCT part_num) FROM (
SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
JOIN (
    WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle

) tc ON tc.theme_id = s.theme_id
WHERE tc.ancestor_id = $1

) AS _count_sub
```

```sh
cargo run -p brick-example -- count 158
```

It prints one figure. Its scope is every theme under Star Wars, at any depth, and no other.

Each statement above holds the closure once for every path that leads to it. The next step names
it once.

### 9. Every relation named once

Can the moulds Technic and City share be composed with the closure written once? `--with` names
each relation a statement reads once, at the head of the statement. Composed into a directory of
its own:

```sh
cargo sqlc compose --source examples/brick/sqlc --target target/brick-with --skip-prepare --with
```

`target/brick-with/reports/shared_moulds.sql` is:

```sql
WITH shared_theme_closure AS (
WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle
)
SELECT * FROM (
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    SELECT * FROM shared_theme_closure
) tc
WHERE tc.ancestor_id = $2

    ) sc
)
) AS _intersect_1
INTERSECT
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    SELECT * FROM shared_theme_closure
) tc
WHERE tc.ancestor_id = $1

    ) sc
)
) AS _intersect_2
) AS _intersect
```

The closure binds nothing, so it is named once, `shared_theme_closure`, and each scope reads it as
`SELECT * FROM shared_theme_closure`. The scopes bind their theme ids, so each is still written
where it is composed. Nothing is created on the database. It is step 7's statement, with each
relation it reads written once.

The subcommands compose without `--with`, so `shared-moulds` runs step 7's spelling. The steps from
here go further into PostgreSQL's SQL.

## Further into PostgreSQL's SQL

The templates from here compose as the steps above show, and each subcommand prints the composed
statement before its rows.

### 10. Purchases, read through their collection row

What did a builder buy, and when? A purchase names a row of its builder's collection, and the set
is that row's. [`shared/purchase_sets.sqlc`][shared/purchase_sets.sqlc] reads the set through the
row, once:

```sql
# Every purchase with its builder and the set it bought, one row per purchase, keyed by
# purchase_id. A purchase names a row of its builder's collection, and the set is that row's: the
# purchase never carries the set itself, so the set is read through the collection row.
SELECT
    p.purchase_id,
    p.builder_id,
    c.set_num,
    p.ordered_at,
    p.delivered_at
FROM lego_purchases p
JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no
```

[`builders/purchase_history.sqlc`][builders/purchase_history.sqlc] numbers one builder's purchases
in the order they were placed, with the pieces bought so far, through window functions:

```sql
# A builder's purchases in the order they were placed, each numbered, with the pieces bought so far.
# One row per purchase, keyed by purchase_id. The window's order ends on purchase_id, so it is
# total: two purchases placed at the same instant still take a place each, and the running total
# moves one purchase at a time.
WITH purchase_sets AS (
    :compose(shared/purchase_sets.sqlc)
)
SELECT
    row_number() OVER w AS nth,
    ps.purchase_id,
    ps.ordered_at,
    ps.set_num,
    s.name,
    s.num_parts,
    sum(s.num_parts) OVER w AS pieces_so_far
FROM purchase_sets ps
JOIN lego_sets s ON s.set_num = ps.set_num
WHERE ps.builder_id = :bind(builder_id)
WINDOW w AS (ORDER BY ps.ordered_at, ps.purchase_id)
ORDER BY nth
```

```sh
cargo run -p brick-example -- history 3326
```

It prints the builder's purchases in order, each numbered, with its pieces and the pieces so far.
The last running total is every piece the builder bought.

Two more questions compose the purchases.
[`builders/latest_purchases.sqlc`][builders/latest_purchases.sqlc] keeps each builder's latest
purchase with `DISTINCT ON`, which keeps the first row of each builder in its `ORDER BY`.
[`sets/never_bought.sqlc`][sets/never_bought.sqlc] keeps, with `NOT EXISTS`, the sets no purchase
reaches.

```sh
cargo run -p brick-example -- latest
cargo run -p brick-example -- never-bought
```

`latest` prints one row for each builder who bought anything. `never-bought` prints the sets never
bought: with the sets bought, they are every set, and no set is both.

The next step reads each set's root theme, through the closure.

### 11. Each set's root theme

Which root theme does a set belong to? A root theme has no parent.
[`shared/set_roots.sqlc`][shared/set_roots.sqlc] joins each set to the root above its theme:

```sql
# Every set with the root theme at the top of its theme's tree, one row per set, keyed by set_num.
# A root theme has no parent, and the theme closure pairs each theme with every theme above it, so
# each set meets exactly one root.
SELECT
    s.set_num,
    s.name,
    s.year,
    s.num_parts,
    r.id AS root_id,
    r.name AS root_name
FROM lego_sets s
JOIN (
    :compose(shared/theme_closure.sqlc)
) tc ON tc.theme_id = s.theme_id
JOIN lego_themes r ON r.id = tc.ancestor_id
WHERE r.parent_id IS NULL
```

Three questions read the root themes:

- [`reports/theme_set_sizes.sqlc`][reports/theme_set_sizes.sqlc]: the median set size of each root
  theme, with the smallest and the largest. `percentile_cont(0.5)` takes the middle size, or
  halfway between the two middle ones.
- [`reports/purchases_by_year_theme.sqlc`][reports/purchases_by_year_theme.sqlc]: the purchases per
  year and root theme, composing the purchases and the roots. `ROLLUP` adds each year's subtotal
  and the grand total, and `GROUPING` labels those rows, so a subtotal is never read as a theme
  with no name.
- [`themes/newest_sets.sqlc`][themes/newest_sets.sqlc]: each root theme's three newest sets, from
  anywhere below it, through the closure. `LATERAL` lets the subquery read the theme of the row it
  is joined to, so `LIMIT 3` keeps three for each theme.

```sh
cargo run -p brick-example -- sizes
cargo run -p brick-example -- rollup 2016 2017
cargo run -p brick-example -- newest
```

`sizes` prints each root theme's sets, how many of them have a size, and the smallest, median and
largest. `rollup` prints how many rows it found, then each year's subtotal and the grand total: a
year's themes add up to its subtotal, and the years to the total. `newest` prints up to three sets
for each root theme, newest first.

The next step counts the sets of every year, the years with none among them.

### 12. Every year, the empty ones kept

Which years saw no release? [`reports/release_years.sqlc`][reports/release_years.sqlc] writes the
calendar with `generate_series` and counts the sets against it:

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

```sh
cargo run -p brick-example -- years
```

It prints the span of years, and the years with no release. Every year from the first release to
the last is listed once, a year with no release with 0.

The next step asks which builders hold every set of a scope.

### 13. The builders who own a whole scope

Who owns every set of a theme scope? "Every" is a division: a builder is kept when the scope's sets,
less the sets in the builder's collection, leave nothing.
[`builders/complete_scope.sqlc`][builders/complete_scope.sqlc]:

```sql
# The builders who own every set of a theme scope (the theme and every theme below it): one row per
# builder, keyed by builder_id. A builder is kept when the scope's sets, less the sets in the
# builder's collection, leave nothing. A scope with no sets would keep every builder, so the scope
# must hold at least one set.
WITH scope_sets AS (
    SELECT s.set_num
    FROM lego_sets s
    JOIN (
        :compose(shared/theme_closure.sqlc)
    ) tc ON tc.theme_id = s.theme_id
    WHERE tc.ancestor_id = :bind(theme_id)
)
SELECT b.builder_id, b.name
FROM lego_builders b
WHERE EXISTS (SELECT 1 FROM scope_sets)
  AND NOT EXISTS (
    SELECT set_num FROM scope_sets
    EXCEPT
    SELECT c.set_num FROM lego_collection c WHERE c.builder_id = b.builder_id
)
ORDER BY b.builder_id
```

```sh
cargo run -p brick-example -- complete 463      # 463 is Spybiotics
```

It prints the builders who own every set of the scope. A scope with no sets keeps nobody, through
the template's first `EXISTS`, and the laws give the template such a scope on rows of their own.

The next step gathers a set's rows into a single value.

### 14. A set as arrays, and as one document

What colours does a set use, and what does a set look like as one value?
[`sets/set_colors.sqlc`][sets/set_colors.sqlc] gathers each version's colours into an array:

```sql
# A set's colours, version by version: one row per version, keyed by version, with every colour
# once, in name order. A line whose colour is missing from the catalogue has no colour name; it is
# counted in unnamed_lines and kept out of the array, where it would be a NULL.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    version,
    array_agg(DISTINCT color_name ORDER BY color_name)
        FILTER (WHERE color_name IS NOT NULL) AS colors,
    count(*) FILTER (WHERE color_name IS NULL) AS unnamed_lines
FROM set_part_details
GROUP BY version
ORDER BY version
```

[`sets/set_document.sqlc`][sets/set_document.sqlc] builds the set as one `jsonb` document: its
number, name, year and theme, and each version with its lines.

```sh
cargo run -p brick-example -- colors 75053-1
cargo run -p brick-example -- document 10182-1
```

`colors` prints each version's colours, and how many lines have no colour name. Each version holds
as many colours as its lines name. `document` prints the document, and every line of the set is in
it once.

The steps from here search.

## Searching with the generator's indexes

The searches read the search indexes the setup builds. Each search is a relation like any other,
and fills a slot as the filters of step 3 do.

### 15. By words

Which of a set's lines are windows? [`filters/by_words.sqlc`][filters/by_words.sqlc] is the parts
whose names match a search, as filter patterns:

```sql
# The parts whose names match a search, as filter patterns: (this part, any colour). The search is
# written as a web search engine takes one (websearch_to_tsquery): words that must all be there,
# "a phrase", or, and -a word to leave out, each word taken to its English stem. The parts' words
# index serves it. A filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE to_tsvector('english', p.name) @@ websearch_to_tsquery('english', :bind(words))
```

[`sets/select_parts_by_words.sqlc`][sets/select_parts_by_words.sqlc] fills step 3's `@filter` with
it. [`parts/search_parts.sqlc`][parts/search_parts.sqlc] searches every part's name, best match
first: `ts_rank` scores how well a name matches, and `ts_headline` marks the matched words.

```sh
cargo run -p brick-example -- by-words 10182-1 "window"
cargo run -p brick-example -- search "technic beam"
```

`by-words` prints the set's lines whose part names hold the words, each line once. `search` prints
the parts that hold every word, best first, each with its score and its name, the matched words
between asterisks.

### 16. By pattern

Which of a set's lines are plates or tiles one stud wide?
[`filters/by_pattern.sqlc`][filters/by_pattern.sqlc] is the parts whose names match a regular
expression, case aside:

```sql
# The parts whose names match a regular expression, as filter patterns: (this part, any colour). The
# match ignores case (~*), and the parts' trigram index serves it. A filter's contract is in
# shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE p.name ~* :bind(pattern)
```

[`sets/select_parts_by_pattern.sqlc`][sets/select_parts_by_pattern.sqlc] fills the same slot with
it. [`parts/brick_sizes.sqlc`][parts/brick_sizes.sqlc] reads a brick's size out of its name, as
`Brick 2 x 4`, with `regexp_match`.

```sh
cargo run -p brick-example -- by-pattern 10182-1 '^(plate|tile) 1 x \d+$'
cargo run -p brick-example -- brick-sizes
```

`by-pattern` prints the set's lines whose part names match, each line once. `brick-sizes` prints
each brick named by its size, with its width and its length.

### 17. By place

What did the builders near a city buy? [`shared/area_purchases.sqlc`][shared/area_purchases.sqlc]
is what the builders in an area bought, by root theme, the area in its `@area` slot:

```sql
# What the builders in an area bought, by the root theme of the set: one row per root theme, keyed
# by root_id, with the purchases and the builders who made them, most purchases first. The caller
# decides the area (the @area slot): builders, with one column, builder_id. A builder the area
# names twice still counts once, since the area only decides who is in it.
WITH purchase_sets AS (
    :compose(shared/purchase_sets.sqlc)
),
set_roots AS (
    :compose(shared/set_roots.sqlc)
),
area AS (
    :compose(@area)
)
SELECT
    sr.root_id,
    sr.root_name,
    count(*) AS purchases,
    count(DISTINCT ps.builder_id) AS builders
FROM purchase_sets ps
JOIN set_roots sr ON sr.set_num = ps.set_num
WHERE ps.builder_id IN (SELECT a.builder_id FROM area a)
GROUP BY sr.root_id, sr.root_name
ORDER BY purchases DESC, sr.root_id
```

[`areas/near_city.sqlc`][areas/near_city.sqlc] is the builders within a radius of a city:

```sql
# The builders whose homes lie within km kilometres of a city, on the earth's surface: one row per
# builder, keyed by builder_id, with the distance in metres. The city is chosen by its city_id.
# earth_box is the cube that holds the circle, and the builders' home index serves it;
# earth_distance then keeps the homes inside the circle itself, since the cube's corners lie
# outside it.
SELECT
    b.builder_id,
    earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
        AS metres
FROM lego_cities c
JOIN lego_builders b
  ON earth_box(ll_to_earth(c.latitude, c.longitude), :bind(km) * 1000)
     @> ll_to_earth(b.latitude, b.longitude)
WHERE c.city_id = :bind(city_id)
  AND earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
      <= :bind(km) * 1000
```

[`builders/bought_near.sqlc`][builders/bought_near.sqlc] fills `@area` with it.
[`areas/postcodes.sqlc`][areas/postcodes.sqlc] is the builders whose postcode matches a regular
expression, and [`builders/bought_in_postcodes.sqlc`][builders/bought_in_postcodes.sqlc] fills
`@area` with them. [`builders/nearest.sqlc`][builders/nearest.sqlc] is the builders nearest a city,
nearest first: inside a `LATERAL`, `<->` orders the homes by distance, the homes' index walks them
in that order, and only the nearest are read.

```sh
cargo run -p brick-example -- bought-near 67 5      # 67 is London; within 5 km
cargo run -p brick-example -- bought-in '^SW'       # London's SW district
cargo run -p brick-example -- nearest 67
```

`bought-near` and `bought-in` print, for each root theme, the purchases and the builders who made
them, most purchases first. The builders within the radius are the builders the distance on the
earth's surface puts there, and the purchases add up to every purchase they made of a set with a
theme. The builders of a postcode pattern anchored on a prefix are the builders whose postcode
begins with it. `nearest` prints the five builders nearest the city, with their distance in metres,
as near as any.

The last step checks the catalogue itself.

## Checking the catalogue

### 18. A roster of checks

Which lines name a part the parts catalogue does not hold, and which sets have no line at all? Each
question is a check, and each check's rule is written once, in a roster.
[`checks/roster.sqlc`][checks/roster.sqlc] lists the checks as `VALUES`, one row per check:

```sql
# The example's checks, one row per check, keyed by slug: the slug each check is cut to, and the
# rule it finds rows against. Each check reads its rule from here, so a rule is written once.
SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)
```

A check reads the roster, cut to its own row by its slug, and left joins `ON true` the rows it
finds. [`checks/lines_uncatalogued.sqlc`][checks/lines_uncatalogued.sqlc]:

```sql
# The inventory lines whose part is not in the parts catalogue, under the roster's row for this
# check: one row per line, with the set its inventory belongs to, and the line itself in detail.
# Where every line's part is in the catalogue, the roster's row comes back alone.
SELECT r.rule, p.set_num, p.detail
FROM (
    :compose(checks/roster.sqlc)
) r
LEFT JOIN (
    SELECT
        i.set_num,
        format('inventory %s, version %s: part %s in colour %s%s',
            ip.inventory_id, i.version, ip.part_num, ip.color_id,
            CASE WHEN ip.is_spare THEN ', a spare' ELSE '' END) AS detail
    FROM lego_inventory_parts ip
    JOIN lego_inventories i ON i.id = ip.inventory_id
    WHERE NOT EXISTS (SELECT 1 FROM lego_parts pt WHERE pt.part_num = ip.part_num)
) p ON true
WHERE r.slug = 'lines_uncatalogued'
```

Its rows are the lines step 1 marks `uncatalogued`. `ON true` joins the check's own row of the
roster to each line it finds, so each row carries the check's rule. Where the check finds nothing,
the `LEFT JOIN` keeps the roster's row alone, with no `set_num` and no `detail`. A check that finds
nothing still returns its rule, and is never missing from the rows.
[`checks/sets_without_lines.sqlc`][checks/sets_without_lines.sqlc] is the same shape over the sets:
a set whose inventories hold no line, and a set with no inventory.
[`checks/all.sqlc`][checks/all.sqlc] is their union:

```sql
# Every check's findings, one row each, and a row with no set_num for a check that finds nothing.
:union(ALL checks/lines_uncatalogued.sqlc, checks/sets_without_lines.sqlc)
```

It composes to [`.sql/checks/all.sql`][.sql/checks/all.sql]. Each check is wrapped as the union of
step 6 wraps its sources, and each holds the roster's body, so the statement reads the roster once
for each check. `ALL` writes `UNION ALL`, which keeps every row of every check and looks for no
duplicate to remove. `cargo sqlc compose --respell` writes the union to read the roster once, as
sql-composer's page shows in [*A union of arms that read one roster*][--respell].

```sh
cargo run -p brick-example -- checks
```

It prints each check's rule, how many rows it found, and the first ten, by set. A check that finds
nothing prints its rule alone. The lines whose part the catalogue does not hold are the real
catalogue's own, which the generator writes whatever `--traps` says. The union's rows are the two
checks' rows, each as often as its check returns it, and respelled it returns the same rows.

The walk ends at the laws.

## The laws

```sh
cargo run -p brick-example -- laws
```

`laws` checks the walk's promises against a second spelling of the same answer, written in
direct SQL in [`src/main.rs`][src/main.rs]. Each law prints `HOLDS` or `FAILS`, with the figures on
both sides, and `laws` exits 1 if any fails. A law that writes runs inside a transaction that is
rolled back, so checking leaves nothing behind. `cargo run -p brick-example -- all` runs every
step with the values above, then the laws.

| step | the promise | checked against |
|---|---|---|
| 1 | a set's parts are its lines, per version (10182-1, and 75053-1) | the set's lines counted in the tables; each version's pieces against `num_parts` |
| 2 | the summary of 75053-1, run twice, is one row per version and category | inside a transaction: the categories and pieces counted in the tables |
| 2 | `MERGE` of 75053-1 repairs its summary to what `INSERT` writes | inside a transaction: every total put wrong and one row removed, then `MERGE` twice |
| 3 | `by-color` and `by-category` keep the set's lines in that colour, or category | the set's lines chosen by a condition on the tables |
| 6 | Technic's and City's sets from 2000 are each scope's, labelled by it | a walk down the themes from the two scopes' own themes |
| 7 | shared and City-only moulds together are City's, none in both | City's moulds, `queries/city_moulds.sqlc` |
| 8 | a theme scope is the theme's whole subtree | the moulds of the sets found by a walk up, from each set's theme to its root |
| 10 | builder 3326's history is their purchases in order, with running totals | the purchases ordered by time, then by id; the pieces summed in the tables |
| 10 | each builder's latest purchase is the first of their purchases, latest first | `row_number()` over each builder's purchases, latest first |
| 10 | the sets never bought and the sets bought are every set, none in both | the sets bought, read through their collection rows, and every set |
| 11 | the median size of Star Wars is the middle of its sizes | its sizes listed by a walk up, sorted, and the middle one taken |
| 11 | purchases 2016 to 2017 add up by theme, by year and in total | each year's themes summed, the years summed, and the purchases of those years counted in the tables |
| 11 | each root theme's newest sets are its first three, newest first | a walk up, and `row_number()` over each root's sets, newest first |
| 12 | every year from first release to last is listed once, the empty ones with 0 | the first and last years, the sets with a year, and the years they have |
| 13 | the builders owning all of Spybiotics hold as many of its sets as it has | each builder's distinct sets of the scope, counted against the scope's |
| 14 | the colours of 75053-1 are the colours its lines name, per version | the distinct colour names counted in the tables |
| 14 | the document of 10182-1 holds each of its lines once | the lines in the document's arrays, against the set's lines in the tables |
| 15 | `by-words` keeps the set's lines whose part names hold the words ('window', 'door frame') | the parts whose names hold every word's stem |
| 15 | the search for 'technic beam' lists every part holding its words, best first | the same parts, ordered by `ts_rank`, then `part_num` |
| 16 | `by-pattern` keeps the set's lines whose part names match | `regexp_like`, case aside |
| 16 | the brick sizes are the bricks named by their width and length | each name cut at ` x ` with `split_part`, both parts all digits |
| 17 | the builders within 5 km of London are the haversine's | the haversine distance, every home measured |
| 17 | the builders in `^SW` are those whose postcode begins with it | `LIKE 'SW%'` |
| 17 | the 5 builders nearest London are as near as any | every home measured with `earth_distance`, the nearest five |
| 17 | what the builders near London bought adds up to their purchases | the purchases of the builders within the haversine distance |
| 18 | each check's rows are what it finds, under its rule from the roster | the lines a `LEFT JOIN` to the parts finds no part for, and every set less the sets with a line (`EXCEPT`), each with how many it examined |
| 18 | `checks/all.sqlc` is every check's rows, each as often | each check composed alone |
| 18 | `checks/all.sqlc` respelled is one union of an arm for each check, and returns its rows as composed | the rows as composed, each as often |
| 12 | on mock rows, 1991 between releases in 1990 and 1992 is listed with 0 | the rows `1990, 1`, `1991, 0`, `1992, 2` |
| 13 | on mock rows, the division keeps the owner of every set, and nobody for an empty scope | the one owner of both sets, and nobody |
| 18 | on mock rows, a check that finds nothing returns its roster row alone, composed and respelled | the rule alone and the two sets with no line; the respelling with its arms joined inner, which loses the rule |

The last three run their template on mock tables: rows of the example's own, in place of the
catalogue's. The calendar gets a year with no release between two that have one. The division gets
a theme with a theme below it, a builder who owns both its sets, one who owns one of them, and a
theme with no sets. The checks get a set with a line whose part is in the catalogue, a set whose
inventory holds no line, and a set with no inventory.

## The plans

```sh
cargo run -p brick-example -- fit
```

`fit` composes every template as `cargo sqlc compose` does, plain and with `--with`, and plans each
composed file with `EXPLAIN (COSTS OFF)`, as a generic plan when it binds a value. It runs none of
them, inside a read-only transaction it rolls back, and exits 1 if a law fails, or if a seed meant
to break one does not.

- **Each table, and each list of `VALUES`, is scanned as often as the file's text writes it.**
  `cargo sqlc tree` says how many copies of each template the file holds. Each copy reads the
  tables its template's own text names, and the lists of `VALUES` it writes, each list told apart
  by its columns. A plan that scans a table more or fewer times fails, unless the planner's reason
  is named: a `LEFT JOIN` it removed, because nothing above the join reads the table and the join
  matches at most one of its rows. Each file `--respell` rewrites is held to its plan by its own
  text: respelled, `checks/all.sql` scans the roster's list once and the list of its arms' numbers
  once, where as composed it scans the roster's list once for each check.
- **Each spelling `cargo sqlc spell` can write leaves the plan as it was.** Each is written in its
  template's place, and every composed file that holds the template, plain and with `--with`, is
  planned again. The plan is the one the file has as written, or differs only in how it is
  printed: the same conditions in another order, or a relation read by another name. Join orders
  are laid out and never written.
- **The laws fail where they must.** Each is checked on seeds, and fails on every one: a copy of a
  template the text holds and the tree does not count, a copy the tree counts and the text does
  not hold, the respelled text of `checks/all.sql` held to the plan of the file as composed, and
  joins written in other orders.

## Connecting

The database is the one `--database-url` names, or `SQLC_BRICK_DATABASE_URL` when the flag is left
out. With neither, it is `postgres:///sqlc_brick`: the database `sqlc_brick` on the local server,
through its Unix socket, as the current user.

```sh
cargo run -p brick-example -- --database-url postgres://user@host/dbname migrate
SQLC_BRICK_DATABASE_URL=postgres://user@host/dbname cargo run -p brick-example -- all
```

The example speaks TLS (rustls), as a managed PostgreSQL usually requires. Ask for it in the URL:
`?sslmode=require` encrypts without checking the server's certificate, and `?sslmode=verify-full`
also checks it, against Mozilla's root certificates, bundled, and any file `&sslrootcert=<file>`
names. With no `sslmode`, a connection tries TLS and falls back to plain when the server does not
offer it.

The subcommands read the templates from `--sqlc-dir`, `examples/brick/sqlc` by default. That path is
relative to the directory the command runs in, so a subcommand run elsewhere names the directory.

## The templates

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
  checks/
    roster.sqlc                    # VALUES: the checks, each with its slug and rule
    lines_uncatalogued.sqlc        # The lines whose part is not in the parts catalogue
    sets_without_lines.sqlc        # The sets with no inventory line
    all.sqlc                       # :union(ALL) of the checks, each reading the roster
```

## The data and its licences

The example ships none of the catalogue: the `curl` of the setup downloads it to your machine. It
is Neon's sample dump, `lego.sql` in
[neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs), a
repository under the MIT licence, whose README gives the data's source as Kaggle's
[LEGO Database](https://www.kaggle.com/datasets/rtatman/lego-database) (rtatman), under CC0,
public domain. The data comes originally from [Rebrickable](https://rebrickable.com). Rebrickable's
terms (effective 2026-07-09, as read on 2026-10-04) allow it to be used for any purpose, commercial
use included, and ask that it be credited as sourced from Rebrickable: credit Rebrickable wherever
you use it. The same terms forbid using any Rebrickable content to train AI models.

LEGO® is a trademark of the LEGO Group, which does not sponsor, authorise or endorse this project.

[shared/set_part_details.sqlc]: sqlc/shared/set_part_details.sqlc
[sets/select_set_parts.sqlc]: sqlc/sets/select_set_parts.sqlc
[.sql/sets/select_set_parts.sql]: .sql/sets/select_set_parts.sql
[shared/set_category_totals.sqlc]: sqlc/shared/set_category_totals.sqlc
[reports/insert_set_summary.sqlc]: sqlc/reports/insert_set_summary.sqlc
[.sql/reports/insert_set_summary.sql]: .sql/reports/insert_set_summary.sql
[inventory/update_spare_counts.sqlc]: sqlc/inventory/update_spare_counts.sqlc
[inventory/track_set_parts.sqlc]: sqlc/inventory/track_set_parts.sqlc
[reports/merge_set_summary.sqlc]: sqlc/reports/merge_set_summary.sqlc
[shared/filtered_set_parts.sqlc]: sqlc/shared/filtered_set_parts.sqlc
[filters/by_category.sqlc]: sqlc/filters/by_category.sqlc
[sets/select_category_parts.sqlc]: sqlc/sets/select_category_parts.sqlc
[.sql/sets/select_category_parts.sql]: .sql/sets/select_category_parts.sql
[filters/by_color.sqlc]: sqlc/filters/by_color.sqlc
[sets/select_colored_parts.sqlc]: sqlc/sets/select_colored_parts.sqlc
[shared/theme_closure.sqlc]: sqlc/shared/theme_closure.sqlc
[.sql/shared/theme_closure.sql]: .sql/shared/theme_closure.sql
[sets/select_sets_by_themes.sqlc]: sqlc/sets/select_sets_by_themes.sqlc
[.sql/sets/select_sets_by_themes.sql]: .sql/sets/select_sets_by_themes.sql
[scopes/technic.sqlc]: sqlc/scopes/technic.sqlc
[scopes/city.sqlc]: sqlc/scopes/city.sqlc
[shared/scope_sets.sqlc]: sqlc/shared/scope_sets.sqlc
[queries/technic_sets.sqlc]: sqlc/queries/technic_sets.sqlc
[queries/city_sets.sqlc]: sqlc/queries/city_sets.sqlc
[reports/combined_theme_sets.sqlc]: sqlc/reports/combined_theme_sets.sqlc
[.sql/reports/combined_theme_sets.sql]: .sql/reports/combined_theme_sets.sql
[shared/scope_moulds.sqlc]: sqlc/shared/scope_moulds.sqlc
[queries/technic_moulds.sqlc]: sqlc/queries/technic_moulds.sqlc
[queries/city_moulds.sqlc]: sqlc/queries/city_moulds.sqlc
[reports/shared_moulds.sqlc]: sqlc/reports/shared_moulds.sqlc
[reports/city_only_moulds.sqlc]: sqlc/reports/city_only_moulds.sqlc
[.sql/reports/shared_moulds.sql]: .sql/reports/shared_moulds.sql
[.sql/reports/city_only_moulds.sql]: .sql/reports/city_only_moulds.sql
[queries/theme_set_parts.sqlc]: sqlc/queries/theme_set_parts.sqlc
[reports/count_theme_parts.sqlc]: sqlc/reports/count_theme_parts.sqlc
[.sql/reports/count_theme_parts.sql]: .sql/reports/count_theme_parts.sql
[shared/purchase_sets.sqlc]: sqlc/shared/purchase_sets.sqlc
[builders/purchase_history.sqlc]: sqlc/builders/purchase_history.sqlc
[builders/latest_purchases.sqlc]: sqlc/builders/latest_purchases.sqlc
[sets/never_bought.sqlc]: sqlc/sets/never_bought.sqlc
[shared/set_roots.sqlc]: sqlc/shared/set_roots.sqlc
[reports/theme_set_sizes.sqlc]: sqlc/reports/theme_set_sizes.sqlc
[reports/purchases_by_year_theme.sqlc]: sqlc/reports/purchases_by_year_theme.sqlc
[themes/newest_sets.sqlc]: sqlc/themes/newest_sets.sqlc
[reports/release_years.sqlc]: sqlc/reports/release_years.sqlc
[builders/complete_scope.sqlc]: sqlc/builders/complete_scope.sqlc
[sets/set_colors.sqlc]: sqlc/sets/set_colors.sqlc
[sets/set_document.sqlc]: sqlc/sets/set_document.sqlc
[filters/by_words.sqlc]: sqlc/filters/by_words.sqlc
[sets/select_parts_by_words.sqlc]: sqlc/sets/select_parts_by_words.sqlc
[parts/search_parts.sqlc]: sqlc/parts/search_parts.sqlc
[filters/by_pattern.sqlc]: sqlc/filters/by_pattern.sqlc
[sets/select_parts_by_pattern.sqlc]: sqlc/sets/select_parts_by_pattern.sqlc
[parts/brick_sizes.sqlc]: sqlc/parts/brick_sizes.sqlc
[shared/area_purchases.sqlc]: sqlc/shared/area_purchases.sqlc
[areas/near_city.sqlc]: sqlc/areas/near_city.sqlc
[builders/bought_near.sqlc]: sqlc/builders/bought_near.sqlc
[areas/postcodes.sqlc]: sqlc/areas/postcodes.sqlc
[builders/bought_in_postcodes.sqlc]: sqlc/builders/bought_in_postcodes.sqlc
[builders/nearest.sqlc]: sqlc/builders/nearest.sqlc
[checks/roster.sqlc]: sqlc/checks/roster.sqlc
[checks/lines_uncatalogued.sqlc]: sqlc/checks/lines_uncatalogued.sqlc
[checks/sets_without_lines.sqlc]: sqlc/checks/sets_without_lines.sqlc
[checks/all.sqlc]: sqlc/checks/all.sqlc
[.sql/checks/all.sql]: .sql/checks/all.sql
[--respell]: ../../README.md#a-union-of-arms-that-read-one-roster---respell
[src/main.rs]: src/main.rs
