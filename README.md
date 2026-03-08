# sql-composer

A SQL template engine that composes reusable SQL fragments with parameterized bindings.

sql-composer lets you write SQL templates with a simple macro syntax, then compose them into final SQL with dialect-specific placeholders and ordered bind parameters. Templates can include other templates, enabling reuse across queries.

## Features

- **Simple macro syntax** embedded in SQL — no new language to learn
- **Dialect-aware placeholders** — Postgres (`$1`), MySQL (`?`), SQLite (`?1`)
- **Template composition** — include and reuse SQL fragments via `:compose(path)`
- **Multi-value bindings** — expand `:bind(ids)` into `$1, $2, $3` for `IN` clauses
- **Circular reference detection** — prevents infinite loops in template includes
- **Driver crates** — thin wrappers for rusqlite, DuckDB, postgres, MySQL, and sqlx
- **CLI tool** — `cargo-sqlc` for pre-compiling templates

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
```

## Quick Start

```rust
use sql_composer::parser::parse_template;
use sql_composer::composer::Composer;
use sql_composer::types::{Dialect, TemplateSource};

let input = "SELECT * FROM users WHERE id = :bind(user_id) AND active = :bind(active);";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();

let composer = Composer::new(Dialect::Postgres);
let result = composer.compose(&template).unwrap();

// Bind params are alphabetically ordered for numbered dialects
assert_eq!(result.sql, "SELECT * FROM users WHERE id = $2 AND active = $1;");
assert_eq!(result.bind_params, vec!["active", "user_id"]);
```

## Macros

All macros use the syntax `:command()`. SQL outside of macros is passed through unchanged.

### `:bind(name)`

Creates one or more dialect-specific placeholders and adds the name to the bind parameter list.

```sql
SELECT * FROM users WHERE id = :bind(user_id) AND status = :bind(status)
-- Postgres: SELECT * FROM users WHERE id = $2 AND status = $1
-- MySQL:    SELECT * FROM users WHERE id = ? AND status = ?
-- SQLite:   SELECT * FROM users WHERE id = ?2 AND status = ?1
```

For multi-value bindings (e.g. `IN` clauses), pass multiple values for the same name:

```sql
SELECT * FROM users WHERE id IN (:bind(ids))
-- With 3 values → SELECT * FROM users WHERE id IN ($1, $2, $3)
```

### `:compose(path)`

Include another SQL template file, resolved from configured search paths.

```sql
SELECT * FROM users WHERE :compose(filters/active_users.sql)
```

### `:union(sources...)` and `:count(sources...)`

Combine multiple template sources with `UNION` or wrap them in a `COUNT` aggregate.

## Driver Crates

Each driver crate wraps a database connection with a `ComposerConnection` (sync) or `ComposerConnectionAsync` (async) trait implementation that composes templates and resolves bind values in one step.

### rusqlite / DuckDB

```rust
use sql_composer::bind_values;
use sql_composer_rusqlite::SqliteConnection;

let conn = SqliteConnection::open_in_memory().unwrap();
let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn rusqlite::types::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

### PostgreSQL / MySQL

These crates support both sync and async via feature flags (`sync`, `async`, both enabled by default).

### sqlx

The `sql-composer-sqlx` crate provides `verify_postgres()` for checking composed SQL against a live database, and `validate_syntax()` (feature `validate`) for offline syntax checking via sqlparser.

## Core Types

| Type | Description |
|------|-------------|
| `Template` | A parsed SQL template containing elements |
| `Element` | SQL literal, bind macro, compose reference, or command |
| `Composer` | Transforms templates into final SQL with placeholders |
| `ComposedSql` | The result: final SQL string + ordered bind param names |
| `Dialect` | Target database: `Postgres`, `Mysql`, `Sqlite` |

## Project Status

This project is under active development. The core API (`Template`, `Composer`, `ComposedSql`) is stabilizing, but may still change before 1.0.
