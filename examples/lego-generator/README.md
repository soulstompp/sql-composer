# LEGO catalogue generator

Builds a large LEGO catalogue from a real one and loads it into Postgres, together with builders,
their collections and their purchase logs.

The real catalogue is the eight `lego_*` tables (colours, themes, part categories, parts, sets,
inventories, inventory parts and inventory sets) in `--source-schema`. The generator reads them once,
writes them through unchanged but for the release years of the years with no release (see Years)
and a few root themes (see Traps), and adds synthesized sets modelled on them: each synthesized set
takes its root theme, year and contents from a real set, with its own set number and name. The
generated tables go to `--schema`, which is dropped and recreated on every run.

## Running

The real catalogue comes from the LEGO example's `setup`, which loads it into `public` of the
database it names. Name the same database in both commands. From the repository root:

```sh
cargo run -p lego-example -- --database-url postgres://localhost:5432/sqlc_lego setup
cargo run --release -p lego-generator -- --database-url postgres://localhost:5432/sqlc_lego --size small
```

`setup` needs `psql` on the `PATH`, and ends with "Setup complete!". It creates the database before
it loads the catalogue, so a `setup` that stops early leaves an empty database behind. The generator
checks for the eight tables before it reads anything, and names the ones it cannot find.
`--schema` may not name the `--source-schema`: the target schema is dropped and recreated.

`--size small|medium|huge|8m|80m|800m` names a number of synthesized sets: 20 thousand, 200
thousand, 2 million, and past huge the number in the name (8, 80 and 800 million). `--sets N` sets
it directly and wins.
The default is `huge`. Everything else scales with it: builders, collection rows and purchases
(`PURCHASES_PER_SET` per set, real and synthesized), and every trap planted at a rate. The cities,
postcodes, names, stores and calendar stay the same at every size, so a bigger size crowds them.
The same `--sets`, `--seed` and wiring flags give the same rows. Each key, statistics and vacuum
build may run an hour for every two million sets unless `--build-timeout` says otherwise.

The database URL can also come from `LEGO_GENERATOR_DATABASE_URL`, never from the general
`DATABASE_URL`: the target schema is dropped and recreated, so the database has to be named on
purpose.

Both programs speak TLS (rustls), as a managed Postgres usually requires. Ask for it in the URL:
`?sslmode=require` encrypts without checking the server's certificate, and `?sslmode=verify-full`
also checks it, against Mozilla's root certificates, bundled, and any file `&sslrootcert=<file>`
names. With no `sslmode`, a connection tries TLS and falls back to plain when the server does not
offer it.

## Loading

The load is hierarchical. A wave is `--chunk` root records (sets, or builders), written in one
transaction, parent table first:

1. the sets, then their inventories, then all of those inventories' lines and nested sets, then the
   wave's rows of `trap_manifest`;
2. the builders, then their collection rows, then those rows' purchases, then the manifest rows.

A wave that fails rolls back whole, so the tables hold exactly the waves that committed. `--jobs`
waves are written at once, each on its own session.

Each batch is sent with `COPY … FROM STDIN` in binary format by default (`--copy-format text` for the
text format, `--method unnest` for `INSERT … SELECT FROM UNNEST` batches). The primary keys are built
after the load (`--index-timing before` builds them first). So are the composite indexes a DBA gives
the tables for the joins between them, each led by the columns the join into its table fixes, with
the columns the queries read in `INCLUDE` (`COMPOSITE_INDEXES` in `src/load.rs`). Two of them are
expression indexes on the purchases, by month and then instant, one within each collection row and
one across all of them. They read the instant through `<schema>.clock(timestamptz)`, a function
declared `IMMUTABLE` that gives an instant as the wall clock of UTC, which makes an instant's month
indexable; the load creates it before the indexes, and a query that means to use them reads time
through the same function. Beside the composite indexes the load builds the unique keys the tables
declare besides their primary keys (`UNIQUE_KEYS` in `src/load.rs`): the postcodes' `code`, and the
natural keys of the inventory lines, `(inventory_id, part_num, color_id, is_spare)`, and of the
nested sets, `(inventory_id, set_num)`, which have no primary key. It also builds what a DBA adds
for the searches the other indexes do not serve (`SEARCHES`): a `text_pattern_ops` key for a
postcode by its prefix, a GiST on the builders' homes by distance (`ll_to_earth`), and GINs on part
names by their words and on part and set names by trigrams.

The generator writes its own lines and nested sets merged on those keys, and the real catalogue's
unchanged. So when the keys or partitions are asked for, it first checks that the real catalogue
repeats neither key, and stops before writing anything if it does.

`--indexes` says which of these are built: `keys` (the primary keys and the unique keys),
`composite` and `search`, as a list, or `none`. All three are built by default; the clock function
is created whatever the list. The search indexes need the extensions `cube`, `earthdistance` and
`pg_trgm`. When `search` is in the list, the generator creates each one in `public` before it writes
anything, unless the database has it already, in any schema; each search index names the schema its
extension is in. `earthdistance` is not a trusted extension, so only a superuser can create it: a
role that is not one needs a superuser to run `CREATE EXTENSION earthdistance CASCADE` in the
database first, or leaves `search` out of `--indexes`.

Autovacuum is off on the tables during the load and back on afterwards (`--autovacuum-during-load on`
leaves it on); then the tables are vacuumed and analysed. `--unlogged` creates unlogged tables for
scratch runs, and `--synchronous-commit` sets the loading sessions' commit mode (off by default).

## Years

No set is released in the years of `NO_RELEASE` (1994 to 1996 and 2001 to 2011). A real set the dump
dates in one of them is re-dated to the nearest release year, the later of two equally near, and
listed in `trap_manifest` under B9 with the year the dump gives it; the synthesized sets modelled on
it take the new year.

The years after the real catalogue (`MODELLED`, 2018 to 2026) are modelled on its last years
(`MODEL_ON`). Each root theme keeps its share of those years' sets, and a set of a modelled year
copies one of them. The releases per year keep growing at the real years' own rate (`MODEL_FIT`,
which leaves out the dump's partial last year), sped up `GROWTH_FACTOR` times, so the last years
hold far more sets than the first.

## The switchboard

A synthesized set lands in a socket: one root theme in one decade, from 1950 to 2030. A socket's
weight is its real sets plus the releases of its modelled years. The patch
`--patch <pattern>:<percent>,…` splits the synthesized sets into phases, each wired to a pattern:

- `natural`: sockets drawn with the real catalogue's own weights;
- `sorted`: one sweep through the sockets in order;
- `interleaved`: every wave steps across the whole socket order;
- `wavy`: a wave travelling over the sockets, forward then back (`--wavy-period`, `--wavy-amplitude`);
- `hotspot`: one socket takes a share of the sets (`--hotspot-share`, `--hotspot-socket`);
- `swing`: waves alternate between two groups of sockets (`--swing-a`, `--swing-b`, `--swing-period`);
- `paired`: packs written in pairs, their child sets chosen by release year so that the two packs'
  sets' years match two at a time and differ in a run of three, which a planner reading columns two
  at a time cannot tell apart. The years come from three pairs of patterns (`YEAR_PAIRS` in
  `src/paired.rs`), placed from a window of real release years that moves with the wave, and
  mirrored on its way back. Each pack is listed in `trap_manifest` under O5, its detail naming its
  pair, whether it is the first or second member, its offsets, its partner's offsets, the window and
  whether it is mirrored.

The relationships between sets (the cords) each have a dial for how often they cross from one socket
to another: nesting (`--cross-nesting`), versions (`--cross-versions`), twins (`--cross-twins`) and
collections (`--cross-collections`). The first three default to the rates measured on the real
catalogue. `--upto-phase N` writes only the phases up to the N-th, so every prefix of a run can be
loaded and checked on its own.

## Buying

Every set released by `TODAY` is on sale, and each has a demand: its popularity times how much
of its timeline has passed by today.
- Popularity is uneven: the most popular tenth of sets holds about half of it.
- The timeline is a sum of bursts from the set's release: the launch, which fades; comebacks and
  re-releases at random later dates; the spikes of its root theme, windows shared by every set of
  the theme; and a thin second-hand trickle. A set released last month has had little time to sell.

A collection row draws its set by demand, from the builder's home socket unless the collection cord
crosses, and each copy is bought on a day drawn from the set's timeline, in the store's opening
hours, at a second and a millisecond of its minute. The builder writes the instant down as the wall
clock of their home zone, to the millisecond, by that zone's offset history from the IANA time zone
database bundled into the binary, from 1950 on.

The row names the set it holds by its number, `set_num`, and the set's name is the sets' own. Where
the builder typed the set's number or name their own way (traps K1, K2, K3 and K5), the row also
keeps what they typed, in `typed_set_num` or `typed_name`; on every other row both are NULL. Each
copy the row holds is one purchase, which names the row by `(builder_id, row_no)`: the copies are
counted from the purchases, and a purchase reaches its set through its row.

## Where builders live

The builders live in real cities: `data/cities.tsv`, Natural Earth's 1:10m populated places (public
domain) in the eight home zones, at their real coordinates, with their populations. Its header says
how the places were picked.

- `lego_cities`: each city, its country, region, home zone, coordinates and population.
- `lego_postcodes`: a city's postcode districts, a grid over a square of its urban core's area, one
  district for about every 25,000 people, so the postcodes are the same at every size. Each code is
  in its country's format, and its leading characters follow the country's own scheme, so a code
  says where it is:
  - a US ZIP code's first digit, an Indian PIN code's, a Japanese and a Portuguese code's, by state,
    prefecture or district;
  - an Australian code by state, a Danish one by region;
  - a UK code's letters by its town's postcode area (`AB`, `IV`, `KW`, `ZE`, `BT`, …), London's by
    its compass point (`EC`, `WC`, `E`, `N`, `NW`, `W`, `SW`, `SE`).

  The rest of each code is numbered in order, and made up.
- `lego_streets`: a district's streets, as many as its builders need, each running north–south or
  east–west from one end to the other, under a made-up name in the country's way of naming streets.
- `lego_builders`: each builder's street and house number, odd on one side of the street and even
  on the other, and their home's point beside it.

A builder's zone is their city's, reached through their street and its postcode. Builders spread
over each zone's cities by population, and over each city to its edges. Every point a street or a
home holds is the centre of the thousandth-of-a-degree cell it falls in, so it names a block, not a
house, and no street name is real, so no row is anybody's address.

## Traps

Some rows are planted on purpose: key spellings, boundary years and absences, orders that depend on
the collation or on ties, and dates across clock changes. Every planted row is listed in
`trap_manifest`, with the phase, wave and socket it came from. The program prints each trap, its rate
and how many it planted when it starts.

`--traps` says which traps are planted: `all` (the default), `none`, or a list such as `K1,K3,D5`.
Only the traps planted at a rate and O5, which the `paired` phase writes, can be left out; naming
any other is refused. The others arise from the real catalogue, from the rows of other traps, or
from how every wave is written, and are there whatever `--traps` says: so are the purchases that
fall in a clock change or across a month by themselves. Without O5, the `paired` phase writes its
sets as `natural` does. D5's pre-orders name a set that is not out yet, which only B2 and B5 plant,
so D5 is refused without one of them.

Every reference in the generated tables names a row its table holds, traps included:

- a set of trap B3 with a theme the real catalogue's theme list does not hold carries a root theme
  the generated `lego_themes` adds, named by its id;
- an inventory of trap K8, filed under the number on the box, is filed under a bare set record of
  that number, which stands beside the set's `-1` record as trap K7's do;
- a synthesized set leaves out its model's lines whose part number the parts list does not hold.
  Those lines are the real catalogue's own, written unchanged and listed under trap B10.

## Output

Logs go to standard error: `RUST_LOG` filters them, and `--log-format json` writes JSON. Every
session names itself `lego-loader/<n>` in `application_name`. A progress line, with the COPY
statements then running, is logged every `--log-progress-every` seconds.

On standard output, the lines starting `SUMMARY` hold the run's figures: the resolved configuration,
rows, bytes, batches and time per table, sets and lines per phase and per socket, the trap counts,
the buying (the sets on sale, the share bought at least once, and the purchases per tenth of the
sets, most bought first), the WAL written and each table's final size. The same configuration is
written to the table `generator_run`, with the definitions of the clock function (`function_clock`)
and of each composite index, unique key and search index, keyed by its name (`composite_<name>`,
`unique_<name>`, `search_<name>`).

## Object-oriented tables

`--oo-schema <name>` also builds the generated sets, colours and builders as class hierarchies, with
PostgreSQL's table inheritance, in a schema of their own. That schema is dropped and recreated, and
may not name `--schema` or `--source-schema`. Each hierarchy's root table keeps the generated
table's name and columns. The rows are the generated rows, unchanged, and each is written to the
table of its most specific class.

`--classes` says which hierarchies are built: `none`, or a list of `sets`, `colors` and `builders`.
All three are built when it is left out. A list without `--oo-schema` is refused, since the classes
need a schema of their own, and so is `--classes none` with it, since that schema would keep the
classes of an earlier load: name the classes, or leave out `--oo-schema`. Nothing is dropped when
either is refused.

Every class's CHECK reads the row's own columns:

- **Sets, by the root theme of their theme:**
  - `licensed`, holding the licensed sets, with `star_wars` under it;
  - `in_house`, holding every other set, with:
    - `classic_play` over `town`, `space`, `castle` and `pirates`;
    - `technic`, with `technic_star_wars` under it;
    - `star_wars_elsewhere`.
  - `star_wars_all` is a second parent of the three Star Wars classes, so a Star Wars set is also a
    licensed, a Technic or an in-house set. A set with no theme is in `in_house`.
- **Colours, by hue:**
  - `neutral` holds the colours whose channels differ by less than 32, which have no hue to speak
    of;
  - `primary` (red, green, blue), `secondary` (yellow, cyan, magenta) and `tertiary` (orange,
    chartreuse, spring green, azure, violet, rose) hold the rest, a class per hue.
  - The hue is `<schema>.colour_wheel(rgb)`: the place on a twelve-hue wheel, counted from red, each
    place thirty degrees. A colour's class follows its `rgb`, not its name.
- **Builders, by the country of their home zone,** from the IANA database's `zone.tab`. A country
  with two home zones (the United States) has a class per zone under it. A builder's class is read
  off their own `street_id`: the streets of one zone, and of one country, are one run of ids.

A class's CHECK, which its children inherit, is named `<class>_check`. A class that has rows of its
own beside its children's leaves theirs out with a `NO INHERIT` CHECK named
`<class>_no_inherit_check`. As `--indexes` asks, each class has the generated table's primary key,
and each class with rows of its own every one of the generated table's composite indexes. Every
class is vacuumed and analysed. Read a hierarchy through its root, with the generated schema
behind it for the other tables:

```sql
SET search_path = lego_oo, lego;
SELECT count(*) FROM lego_sets WHERE theme_id = 158;                  -- the Star Wars classes only
SELECT count(*) FROM lego_colors WHERE lego_oo.colour_wheel(rgb) = 0;  -- the red class
```

A query that names a class's own condition, such as a theme, a street or a wheel place, reads only
the classes that can hold it. The planner leaves out the others, as long as the value is a constant
when the statement is planned.

After the build, a placement check reads every class's CHECKs back from the catalogue and checks
four things. The build fails if any does not hold:

- each hierarchy has exactly the generated table's rows;
- every row satisfying a class's CHECKs lies in that class or below it;
- every row of a class satisfies its CHECKs;
- a class meant to have no rows of its own has none.

The `SUMMARY oo` lines give the rows each class has of its own.

## Partitions by id

`--partitioning` also copies the generated tables, after the load, into tables partitioned by the
leading column of their natural key, the unique key over the table's own attributes. Each method
gets a schema of its own, named `<schema>_<method>` (`lego_inheritance`, `lego_range`,
`lego_hash`). Every run drops all three before the load, asked for or not, so no copy keeps an
earlier load's rows:

- `inheritance`: child tables below an empty parent, each holding one range of the column under a
  CHECK named `<child>_check`, as tables were partitioned before declarative partitioning. Nothing
  routes a row written later.
- `range`: `PARTITION BY RANGE`, with a `DEFAULT` partition for rows written later.
- `hash`: `PARTITION BY HASH`.

`--partitioning` takes `none` (the default) or a list of the three, so one load can build all of
them. `--partition-tables` names the tables to copy. Its default, `all`, is every generated table
but the run's own, `generator_run` and `trap_manifest`, which can still be named. `--partitions`
gives the number of partitions, 8 by default. Both are refused without `--partitioning`, and so
is any method's schema that would name `--source-schema` or `--oo-schema`. The partitions are named
`<table>_p0`, `<table>_p1`, …

A key is what a table declares, never what one load's rows happen to hold. A table's natural key
is its primary key: the catalogue's ids are the catalogue's own identifiers, and a generated id is
the generator's identity for what it generates. Only a table that declares two unique keys picks
one, and the tables with no primary key take the unique key they declare:

| table | natural key | partition column |
|---|---|---|
| `lego_inventory_parts` (no primary key) | `(inventory_id, part_num, color_id, is_spare)` | `inventory_id` |
| `lego_inventory_sets` (no primary key) | `(inventory_id, set_num)` | `inventory_id` |
| `lego_postcodes` | `(code)`, beside the primary key `postcode_id` | `code` |

The ranges are of equal width between the column's minimum and maximum, read after the load. A
table is left out when it has no natural key (`trap_manifest`), or when its key holds an expression
or a nullable column. Under the two range methods, so is a table whose key leads with text
(`lego_sets`, `lego_parts`, `lego_postcodes`, `generator_run`), since text has no equal-width
ranges; `hash` partitions those too. Read a method's schema with the generated schema behind it,
for the tables it leaves out:

```sql
SET search_path = lego_range, lego;
EXPLAIN SELECT * FROM lego_purchases WHERE purchase_id = 400000;  -- one partition
```

Each inheritance table is filled in one pass over the generated table, with no temporary files.
Its children start as the partitions of a declarative table, `<table>_load`, each already under its
CHECK, and that table routes every row to its child. Then each child is detached and made a child of
the parent with `INHERIT`, and `<table>_load` is dropped.

Each schema gets the indexes `--indexes` asks for. On a declarative table they are created on the
parent, which makes them on every partition. Under inheritance they are created on each child,
since an index covers one table, and not on the empty parent; the planner leaves out a child whose
CHECK a query's condition contradicts. A key that holds the partition column stays unique across
the whole table. A declarative table enforces it. Under inheritance, two rows equal on such a key
are equal on the partition column, so they fall in one child, and that child's unique index refuses
the second. A row written to the parent itself is checked by nothing. The natural key always holds
the partition column. Any other key cannot be unique across the table, so it is built as a plain
index: the postcodes' primary key, `postcode_id`, is one.

With `--unlogged`, the partitions and the inheritance tables are unlogged. A declarative parent
is not, since PostgreSQL refuses an unlogged partitioned table, and it holds no rows of its own.
PostgreSQL also refuses any storage parameter on a partitioned table, and autovacuum never
analyses one, so the build vacuums and analyses each table it makes.

After the build, the run checks that each row lies in one partition and each key is unique. It
counts each table's rows across its partitions against the generated table, and sees that an
inheritance parent holds none of its own. It reads every range back from the catalogue to see that
none overlaps another. And it sees that no value of a key that holds the partition column appears
twice. Where a valid unique index enforces the key on the copy, the index does that, and the rows
are not counted. Where nothing enforces it, as without `keys` in `--indexes`, they are counted.
The build fails if any of these does not hold. The `SUMMARY partition` lines give each partition's
rows, and the `SUMMARY size` lines each copied table's size with all its partitions.

`generator_run` records:
- the methods (`partitioning`), the tables (`partition_tables`) and the count (`partitions`);
- each table's natural key and column (`partition_key_<table>`, `partition_column_<table>`);
- for each method, its schema, the tables it partitions, and the ones it leaves out with the
  reason (`partition_<method>_schema`, `partition_<method>_tables`, `partition_<method>_left_out`);
- when the keys are built, every key that is no longer unique across its table
  (`unique_lost_<method>_<key>`).

## Tests

```sh
cargo test -p lego-generator
```
