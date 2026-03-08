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
- **CLI tool** — `cargo sqlc compose` pre-compiles templates and runs `cargo sqlx prepare`

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

### Library usage

Add `sql-composer` to your `Cargo.toml`:

```toml
[dependencies]
sql-composer = "0.1"
```

Parse a template and compose it into final SQL:

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

### CLI usage

Install `cargo-sqlc` as a cargo subcommand:

```sh
# From crates.io (once published)
cargo install cargo-sqlc

# From source
cargo install --path crates/cargo-sqlc
```

Cargo automatically discovers binaries named `cargo-<name>` on your `PATH` and makes them available as `cargo <name>`. After installing, `cargo sqlc` is ready to use.

Create `.sqlc` template files in your source directory (default `sqlc/`):

```sql
-- sqlc/get_user.sqlc
SELECT id, name, email
FROM users
WHERE id = :bind(user_id) AND active = :bind(active)
```

Compose templates into final `.sql` files:

```sh
# Compose templates from sqlc/ -> sql/ and run cargo sqlx prepare
cargo sqlc compose

# With a specific dialect
cargo sqlc compose --dialect mysql

# Skip the sqlx prepare step
cargo sqlc compose --skip-prepare

# Override source/target directories
cargo sqlc compose --source src/queries --target generated/sql
```

The compose step reads all `.sqlc` files from the source directory, composes them into `.sql` files in the target directory, and then runs `cargo sqlx prepare` to keep the query cache up to date for compile-time checked queries. `DATABASE_URL` must be set for the `cargo sqlx prepare` step (or use `--skip-prepare` to skip it).

#### Environment variables

Directories can be configured via environment variables so you don't have to pass them every time:

```sh
export SQLC_SOURCE_DIR=src/queries
export SQLC_TARGET_DIR=generated/sql

# Now just:
cargo sqlc compose
```

Priority: CLI arg > env var > default (`sqlc`/`sql`).

## Template Syntax

All macros use the syntax `:command()`. SQL outside of macros is passed through unchanged. Lines starting with `#` are template comments and are stripped during parsing.

### `:bind(name)`

Creates one or more dialect-specific placeholders and adds the name to the bind parameter list.

```sql
SELECT * FROM users WHERE id = :bind(user_id) AND status = :bind(status)
-- Postgres: SELECT * FROM users WHERE id = $2 AND status = $1
-- MySQL:    SELECT * FROM users WHERE id = ? AND status = ?
-- SQLite:   SELECT * FROM users WHERE id = ?2 AND status = ?1
```

For multi-value bindings (e.g. `IN` clauses), pass multiple values for the same name at compose time:

```sql
SELECT * FROM users WHERE id IN (:bind(ids))
-- With 3 values → Postgres: SELECT * FROM users WHERE id IN ($1, $2, $3)
```

#### Validation options

`:bind()` supports optional validation constraints:

```sql
-- Require at least 1 value
:bind(ids EXPECTING 1)

-- Require between 1 and 10 values
:bind(ids EXPECTING 1..10)

-- Allow NULL values
:bind(optional_field NULL)
```

### `:compose(path)`

Include another SQL template file, resolved from configured search paths.

```sql
SELECT * FROM users WHERE :compose(filters/active_users.sqlc)
```

Compose references are resolved against the search paths added via `Composer::add_search_path()`. Circular references are detected and produce an error.

### `:union(sources...)` and `:count(sources...)`

Combine multiple template sources:

```sql
-- Union multiple queries
:union(queries/admins.sqlc, queries/moderators.sqlc)

-- With DISTINCT or ALL modifiers
:union(DISTINCT a.sqlc, b.sqlc)
:union(ALL a.sqlc, b.sqlc)

-- Count rows from a template
:count(queries/active_users.sqlc)

-- Count specific columns
:count(id, name OF queries/active_users.sqlc)
```

## Driver Crates

Each driver crate wraps a database connection with a `ComposerConnection` (sync) or `ComposerConnectionAsync` (async) trait implementation that composes templates and resolves bind values in one step.

### rusqlite

```toml
[dependencies]
sql-composer = "0.1"
sql-composer-rusqlite = "0.1"
```

```rust
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_rusqlite::SqliteConnection;

let conn = SqliteConnection::open_in_memory().unwrap();
conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", []).unwrap();

let input = "SELECT * FROM users WHERE id = :bind(user_id)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
let composer = Composer::new(Dialect::Sqlite);

let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn rusqlite::types::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();

let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
let mut stmt = conn.prepare(&sql).unwrap();
let _rows = stmt.query(refs.as_slice()).unwrap();
```

### DuckDB

```toml
[dependencies]
sql-composer = "0.1"
sql-composer-duckdb = "0.1"
```

```rust
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_duckdb::DuckDbConnection;

let conn = DuckDbConnection::open_in_memory().unwrap();
let input = "SELECT * FROM users WHERE id = :bind(user_id)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
// DuckDB uses Postgres-style $N placeholders
let composer = Composer::new(Dialect::Postgres);

let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn duckdb::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

### PostgreSQL (sync + async)

```toml
[dependencies]
sql-composer = "0.1"
sql-composer-postgres = "0.1"  # both sync and async enabled by default
# sql-composer-postgres = { version = "0.1", default-features = false, features = ["async"] }
```

**Features:** `sync` (enables `postgres` crate), `async` (enables `tokio-postgres`). Both enabled by default.

```rust
// Async (tokio-postgres)
use sql_composer_postgres::{PgClient, boxed_params};
use sql_composer::driver::ComposerConnectionAsync;

let (client, connection) = tokio_postgres::connect("host=localhost", tokio_postgres::NoTls).await?;
tokio::spawn(connection);
let client = PgClient::from_client(client);

let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>]);
let (sql, params) = client.compose(&composer, &template, values).await?;
let refs = boxed_params(&params);
let rows = client.query(&sql as &str, &refs).await?;
```

```rust
// Sync (postgres)
use sql_composer_postgres::{PgConnection, boxed_params_sync};
use sql_composer::driver::ComposerConnection;

let client = postgres::Client::connect("host=localhost", postgres::NoTls)?;
let conn = PgConnection::from_client(client);

let values = bind_values!("user_id" => [Box::new(1i32) as Box<dyn postgres::types::ToSql + Sync>]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

### MySQL (sync + async)

```toml
[dependencies]
sql-composer = "0.1"
sql-composer-mysql = "0.1"  # both sync and async enabled by default
# sql-composer-mysql = { version = "0.1", default-features = false, features = ["async"] }
```

**Features:** `sync` (enables `mysql` crate), `async` (enables `mysql_async`). Both enabled by default.

```rust
// Async (mysql_async)
use sql_composer_mysql::MysqlConn;
use sql_composer::driver::ComposerConnectionAsync;

let pool = mysql_async::Pool::new("mysql://root@localhost/test");
let conn = pool.get_conn().await?;
let conn = MysqlConn::from_conn(conn);

let values = bind_values!("user_id" => [mysql_async::Value::from(1i32)]);
let (sql, params) = conn.compose(&composer, &template, values).await?;
```

```rust
// Sync (mysql)
use sql_composer_mysql::MysqlConnection;
use sql_composer::driver::ComposerConnection;

let conn = mysql::Conn::new("mysql://root@localhost/test")?;
let conn = MysqlConnection::from_conn(conn);

let values = bind_values!("user_id" => [mysql::Value::from(1i32)]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

### sqlx (verification + validation)

```toml
[dependencies]
sql-composer-sqlx = "0.1"                                        # postgres verification (default)
# sql-composer-sqlx = { version = "0.1", features = ["validate"] } # add offline syntax checking
# sql-composer-sqlx = { version = "0.1", features = ["mysql"] }    # mysql instead of postgres
```

**Features:** `postgres` (default, enables live verification against PostgreSQL), `mysql` (live verification against MySQL), `validate` (offline syntax checking via `sqlparser`).

```rust
use sql_composer_sqlx::verify_postgres;

// Verify against a live database (checks tables, columns, syntax)
let stmts = vec![&composed_sql];
verify_postgres("postgres://localhost/mydb", &stmts).await?;
```

```rust
use sql_composer_sqlx::validate_syntax;
use sql_composer::Dialect;

// Offline syntax validation (no database needed, requires "validate" feature)
validate_syntax("SELECT * FROM users WHERE id = $1", Dialect::Postgres)?;
```

## Core Library Features

The `sql-composer` core crate has the following optional features:

| Feature | Description |
|---------|-------------|
| `std` | Standard library support (enabled by default) |
| `serde` | Derive `Serialize`/`Deserialize` for core types (`Template`, `Element`, etc.) |

```toml
# With serde support
sql-composer = { version = "0.1", features = ["serde"] }
```

## Core Types

| Type | Description |
|------|-------------|
| `Template` | A parsed SQL template containing elements |
| `Element` | SQL literal, bind macro, compose reference, or command |
| `Binding` | A `:bind()` with name, optional value count constraints, and nullable flag |
| `ComposeRef` | A `:compose()` reference to another template file |
| `Command` | A `:count()` or `:union()` combinator |
| `Composer` | Transforms templates into final SQL with placeholders |
| `ComposedSql` | The result: final SQL string + ordered bind param names |
| `Dialect` | Target database: `Postgres`, `Mysql`, `Sqlite` |

## How Bind Parameter Ordering Works

Numbered dialects (Postgres, SQLite) use a two-pass approach:

1. **Collect** — scan all `:bind()` names into a deduplicated, alphabetically sorted set
2. **Assign** — give each unique name a 1-based index (`$1`, `$2`, ...)
3. **Emit** — replace each `:bind()` with its assigned placeholder

This means the same bind name always gets the same placeholder number, regardless of where it appears in the template. Alphabetical ordering provides deterministic, predictable parameter positions.

MySQL uses document-order positional `?` placeholders with no deduplication, matching its native parameter style.

## Project Status

This project is under active development. The core API (`Template`, `Composer`, `ComposedSql`) is stabilizing, but may still change before 0.1.0. Several crates in this workspace will be production tested over the next few months at which point the project and so what API problems are lurking I plan to have flushed out quickly.

## License

MIT
