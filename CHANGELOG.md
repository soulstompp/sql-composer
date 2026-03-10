# Changelog

## 0.0.2

### sql-composer

- **Parameterized `:compose()` with slot arguments** — Templates can now declare named slots (`@slot_name`) inside `:compose()` that callers fill with concrete file paths. This enables composable, parameterized templates: write a shared base query once and swap in different logic (e.g. filters, data sources) at the call site. Slots are explicitly scoped — child templates do not inherit parent slots.
- **Fix: parser now handles comments before macros** — Templates with `#` comment lines immediately before `:compose()`, `:union()`, or `:count()` macros no longer produce empty output.
- **Fix: `:union()` no longer wraps members in parentheses** — Union output is now `QUERY UNION QUERY` rather than `(QUERY) UNION (QUERY)`, matching standard SQL semantics and avoiding unintended query planner behavior.
- **Fix: trailing whitespace trimmed in `:union()` output** — Composed union SQL no longer has blank lines between members and `UNION` keywords.

### examples

- **Lego database example** (`examples/lego/`) — A comprehensive runnable example using the Rebrickable Lego dataset on Postgres. Demonstrates every sql-composer feature: `:compose()`, `@slot` parameterization, `:bind()`, multi-value `:bind()` for `IN` clauses, `:union()`, `:count(DISTINCT)`, and `#` comments. Includes a clap CLI with subcommands, auto-download of the dataset, and committed `.sql` output for reference.

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
