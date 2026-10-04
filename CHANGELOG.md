# Changelog

## Unreleased

### examples

- **Lego example's composed statements refreshed** — The committed `.sql` files had fallen behind their templates: several differed, and those of the newer templates were missing. They are composed again, and CI checks them with `cargo sqlc compose --verify`. A line with no category, part name or colour name now sorts after the named ones, and a set with no part count first within its year, on every database: each `ORDER BY` says so with an `IS NULL` term, since databases disagree on where a NULL sorts.
- **Composite indexes migration** — `migrations/20260930000000_composite_indexes.sql` gives the dump's tables the composite indexes a DBA adds for the example's joins, each led by the column the join into its table fixes and ending on the column the next join reads, or holding it in `INCLUDE`. `setup` and `migrate` run it, and it is safe to run again.
- **TLS** — The Lego example reaches a server that requires TLS (rustls); ask for it with the URL's `sslmode`.
- **Lego catalogue generator** — Its changes are in its own changelog, [`examples/lego-generator/CHANGELOG.md`](examples/lego-generator/CHANGELOG.md).

## 0.0.4

### sql-composer

- **`:intersect(sources...)` and `:except(first, rest...)`** — The other two set operations, with the `ALL` and `DISTINCT` modifiers `:union` takes. `:except(a, b, c)` is `a` less everything in `b` or `c`. Every source, and the whole, is wrapped as a derived table, so a source that is itself a union keeps its grouping and either can be a `:union` member. A `columns OF` list is refused.
- **`:define(path)`** — Composes a template's body even where that template is named. The name in front of `AS` stands for it everywhere under the template that defines it, so a statement can name its relations once in a `WITH` clause.
- **View registry** — `Composer::add_view` and `Composer::load_view_registry` make the composer write `SELECT * FROM <name>` wherever a registered template is composed, instead of inlining it. A registry is a template of `CREATE VIEW <name> AS :define(<path>)` statements. A registered template that binds a parameter or leaves a slot open is refused.

### cargo-sqlc

- **`--views <registry>`** — `cargo sqlc compose` composes with a view registry.

### examples

- **Lego example reworked** — A set's parts are listed version by version, and the summary and tracking tables keep versions apart. Theme scopes are a theme and every theme below it, chosen by id; colour and category filters are `(part_num, color_id)` patterns; bind values match parameters by name. New subcommands `shared-moulds` (`:intersect`), `city-only-moulds` (`:except`) and `laws`, which checks each promise against the same answer computed directly in SQL.
- **Lego catalogue generator** (`examples/lego-generator/`) — Builds a large LEGO catalogue from the real one the Lego example loads, with builders' collections and purchase logs, and loads it into Postgres in batches. `--size small|medium|huge` picks the scale, and the same `--sets`, `--seed` and wiring give the same rows.

### All crates

- Version bump to 0.0.4.

## 0.0.3

### sql-composer

- **Fix: parser now handles comments before macros** — Templates with `#` comment lines immediately before `:compose()`, `:union()`, or `:count()` macros no longer produce empty output.
- **Fix: `:union()` no longer wraps members in parentheses** — Union output is now `QUERY UNION QUERY` rather than `(QUERY) UNION (QUERY)`, matching standard SQL semantics and avoiding unintended query planner behavior.
- **Fix: trailing whitespace trimmed in `:union()` output** — Composed union SQL no longer has blank lines between members and `UNION` keywords.

### examples

- **Lego database example** (`examples/lego/`) — A comprehensive runnable example using the Rebrickable Lego dataset on Postgres. Demonstrates every sql-composer feature: `:compose()`, `@slot` parameterization, `:bind()`, multi-value `:bind()` for `IN` clauses, `:union()`, `:count(DISTINCT)`, and `#` comments. Includes a clap CLI with subcommands, auto-download of the dataset, and committed `.sql` output for reference.

### All crates

- Version bump to 0.0.3.

## 0.0.2

### sql-composer

- **Parameterized `:compose()` with slot arguments** — Templates can now declare named slots (`@slot_name`) inside `:compose()` that callers fill with concrete file paths. This enables composable, parameterized templates: write a shared base query once and swap in different logic (e.g. filters, data sources) at the call site. Slots are explicitly scoped — child templates do not inherit parent slots.

### cargo-sqlc

- **Recursive directory scanning** — `cargo sqlc compose` now recursively walks all subdirectories under `--source`, composing every `.sqlc` file and mirroring the directory structure in `--target`.
- **Atomic output via temp directory** — Composition writes to a temporary directory first. The target is only replaced after all files compose successfully, preventing partial output on failure.
- **Clean target** — The entire target directory is wiped and recreated on each compose run, removing stale `.sql` files from deleted or reorganized sources.
- **`--verify` mode** — Composes all templates to memory and diffs against existing target files. Reports changed, missing, and stale files, then exits with code 1 on any mismatch. Designed for CI to ensure committed `.sql` files stay in sync with `.sqlc` sources.
- **Default target changed from `sql` to `.sql`** — The hidden directory name makes it obvious that the output is generated and should not be edited directly. Override with `--target` or `SQLC_TARGET_DIR`.

### All crates

- Version bump to 0.0.2.

## 0.0.1

- Initial release.
