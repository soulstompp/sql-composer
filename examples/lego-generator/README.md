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

`--size small|medium|huge` names a number of synthesized sets; `--sets N` sets it directly and wins.
The default is `huge`. Everything else scales with it: builders, collection rows and purchases
(`PURCHASES_PER_SET` per set, real and synthesized), and every trap planted at a rate. The same
`--sets`, `--seed` and wiring flags give the same rows.

The database URL can also come from `LEGO_GENERATOR_DATABASE_URL`, never from the general
`DATABASE_URL`: the target schema is dropped and recreated, so the database has to be named on
purpose.

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
after the load (`--index-timing before` builds them first). So are the strands: the composite
indexes a DBA gives the tables for the joins between them, each led by the column the join into its
table fixes (`STRANDS` in `src/load.rs`). Autovacuum is off on the tables during
the load and back on afterwards (`--autovacuum-during-load on` leaves it on); then the tables are
vacuumed and analysed. `--unlogged` creates unlogged tables for scratch runs, and
`--synchronous-commit` sets the loading sessions' commit mode (off by default).

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
- `zchord`: packs written in pairs, their child sets chosen by release year so that the years, read
  by their last digit round the decade, lie the same distances apart two at a time, but not three at
  a time. Each pack is listed in `trap_manifest` under O5.

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
hours. The builder writes the instant down as the wall clock of their home zone, by that zone's
offset history from the IANA time zone database bundled into the binary, from 1950 on.

The row names the set it holds by its number, `set_num`, and the set's name is the sets' own. Where
the builder typed the set's number or name their own way (traps K1, K2, K3 and K5), the row also
keeps what they typed, in `typed_set_num` or `typed_name`; on every other row both are NULL. Each
copy the row holds is one purchase, which names the row by `(builder_id, row_no)`: the copies are
counted from the purchases, and a purchase reaches its set through its row.

## Traps

Some rows are planted on purpose: key spellings, boundary years and absences, orders that depend on
the collation or on ties, and dates across clock changes. Every planted row is listed in
`trap_manifest`, with the phase, wave and socket it came from. The program prints each trap, its rate
and how many it planted when it starts.

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
the buying (the sets on sale, the share bought at least once, and the purchases per tenth of the sets,
most bought first), the WAL written and each table's final size. The same configuration is written to the table
`generator_run`.

## Object-oriented tables

`--oo-schema <name>` also builds the generated sets, colours and builders as class hierarchies, with
PostgreSQL's table inheritance, in a schema of their own. That schema is dropped and recreated, and
may not name `--schema` or `--source-schema`. Each hierarchy's root table keeps the generated
table's name and columns. The rows are the generated rows, unchanged, and each is written to the
table of its most specific class. Every class's CHECK reads the row's own columns:

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
  with two home zones (the United States) has a class per zone under it.

Each class has the generated table's primary key, and is vacuumed and analysed. Read a hierarchy
through its root, with the generated schema behind it for the other tables:

```sql
SET search_path = lego_oo, lego;
SELECT count(*) FROM lego_sets WHERE theme_id = 158;                  -- the Star Wars classes only
SELECT count(*) FROM lego_colors WHERE lego_oo.colour_wheel(rgb) = 0;  -- the red class
```

A query that names a class's own condition, such as a theme, a zone or a wheel place, reads only
the classes that can hold it. The planner leaves out the others, as long as the value is a constant
when the statement is planned.

After the build, a certificate reads every class's CHECKs back from the catalogue and checks four
things. The build fails if any does not hold:

- each hierarchy holds exactly the generated table's rows;
- every row satisfying a class's CHECKs lies in that class or below it;
- every row of a class satisfies its CHECKs;
- a class meant to hold no rows of its own holds none.

The `SUMMARY oo` lines give the rows each class holds of its own.

## Tests

```sh
cargo test -p lego-generator
```
