use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Parser, Subcommand};
use sql_composer::composer::{ComposedSql, Composer};
use sql_composer::parser::parse_template_file;
use sql_composer::types::Dialect;
use sqlx::postgres::{PgArguments, PgPoolOptions, PgRow};
use sqlx::query::Query;
use sqlx::{PgPool, Postgres, Row};

const LEGO_SQL_URL: &str =
    "https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql";

/// The set the examples use by default: Café Corner, a single-version set.
const DEFAULT_SET: &str = "10182-1";
/// A set LEGO sold in two versions, whose parts are listed version by version.
const VERSIONED_SET: &str = "75053-1";
/// Theme ids of the default scopes. A scope is the theme and every theme below it.
const TECHNIC_THEME_ID: i32 = 1;
const CITY_THEME_ID: i32 = 52;
const STAR_WARS_THEME_ID: i32 = 158;

#[derive(Parser)]
#[command(name = "lego", about = "sql-composer example using the Lego database")]
struct Cli {
    /// Postgres connection URL
    #[arg(
        long,
        env = "SQLC_LEGO_DATABASE_URL",
        default_value = "postgres:///sqlc_lego"
    )]
    database_url: String,

    /// Path to the sqlc template directory
    #[arg(long, default_value = "examples/lego/sqlc")]
    sqlc_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download lego data, create database, and run migrations
    Setup,
    /// Run the migrations for the example tables
    Migrate,
    /// List parts for a set, version by version (:compose + :bind)
    Parts { set_num: String },
    /// Insert set category summary, per version (:compose in INSERT)
    Summary { set_num: String },
    /// Track a set's parts and sync their spare counts (:compose in INSERT and UPDATE)
    Spares { set_num: String },
    /// Filter parts by color (@slot composition)
    ByColor { set_num: String, color: String },
    /// Filter parts by category (@slot composition)
    ByCategory { set_num: String, category: String },
    /// Find sets in theme scopes (multi-value :bind IN)
    Themes {
        min_year: i32,
        #[arg(required = true, num_args = 1..)]
        theme_ids: Vec<i32>,
    },
    /// Combine Technic and City sets (:union)
    Combined { min_year: i32 },
    /// Count distinct moulds in a theme scope (:count DISTINCT)
    Count { theme_id: i32 },
    /// Moulds that Technic and City sets share (:intersect)
    SharedMoulds,
    /// Moulds City sets use and Technic sets do not (:except)
    CityOnlyMoulds,
    /// Check the example's laws; exits 1 if any fails
    Laws,
    /// Run all examples with default values, then the laws
    All,
}

/// A bind value, matched to the composed statement's parameters by NAME.
#[derive(Clone)]
enum Value {
    Int(i32),
    Text(String),
}

fn int(name: &str, v: i32) -> (&str, Value) {
    (name, Value::Int(v))
}

fn text<'a>(name: &'a str, v: &str) -> (&'a str, Value) {
    (name, Value::Text(v.to_string()))
}

/// Compose a template and return the final SQL with bind param names.
fn compose(sqlc_dir: &Path, template: &str) -> ComposedSql {
    let mut composer = Composer::new(Dialect::Postgres);
    composer.add_search_path(sqlc_dir.to_path_buf());
    let tpl = parse_template_file(&sqlc_dir.join(template))
        .unwrap_or_else(|e| panic!("parse {template}: {e}"));
    composer
        .compose(&tpl)
        .unwrap_or_else(|e| panic!("compose {template}: {e}"))
}

/// Compose a template with value counts for multi-value bindings.
fn compose_multi<V>(
    sqlc_dir: &Path,
    template: &str,
    values: &BTreeMap<String, Vec<V>>,
) -> ComposedSql {
    let mut composer = Composer::new(Dialect::Postgres);
    composer.add_search_path(sqlc_dir.to_path_buf());
    let tpl = parse_template_file(&sqlc_dir.join(template))
        .unwrap_or_else(|e| panic!("parse {template}: {e}"));
    composer
        .compose_with_values(&tpl, values)
        .unwrap_or_else(|e| panic!("compose {template}: {e}"))
}

/// Bind `values` to a composed statement by parameter NAME, in the order the statement numbers them.
///
/// A name the statement repeats (a multi-value bind) takes the values given under that name, in
/// order. A value whose name the statement does not have is refused: a misspelt name would
/// otherwise leave a parameter unbound or bind the wrong one, silently.
fn bound<'q>(
    result: &'q ComposedSql,
    values: &[(&str, Value)],
) -> Query<'q, Postgres, PgArguments> {
    for (name, _) in values {
        assert!(
            result.bind_params.iter().any(|p| p == name),
            "value given for {name}, which the statement does not bind: {:?}",
            result.bind_params
        );
    }
    let mut taken: BTreeMap<&str, usize> = BTreeMap::new();
    let mut query = sqlx::query(&result.sql);
    for param in &result.bind_params {
        let nth = taken.entry(param.as_str()).or_insert(0);
        let value = values
            .iter()
            .filter(|(name, _)| name == param)
            .nth(*nth)
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("no value for bind param {param}"));
        *nth += 1;
        query = match value {
            Value::Int(v) => query.bind(*v),
            Value::Text(v) => query.bind(v.clone()),
        };
    }
    query
}

fn print_header(name: &str, result: &ComposedSql) {
    println!("\n── {name} ──");
    println!("SQL:\n{}\n", result.sql.trim());
    println!("Bind params: {:?}\n", result.bind_params);
}

/// An absent value printed as an absence, never as a number or an empty string.
fn shown<T: Display>(v: Option<T>) -> String {
    v.map_or_else(|| "—".to_string(), |v| v.to_string())
}

/// A line's identity: (inventory_id, part_num, color_id, is_spare).
type LineKey = (i32, String, i32, bool);

fn line_key(row: &PgRow) -> LineKey {
    (
        row.get("inventory_id"),
        row.get("part_num"),
        row.get("color_id"),
        row.get("is_spare"),
    )
}

/// Return the cache directory for sql-composer data files.
fn cache_dir() -> PathBuf {
    let base = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").expect("HOME not set");
            PathBuf::from(home).join(".cache")
        });
    base.join("sql-composer")
}

/// Download the lego SQL dump if not already cached. Returns the path.
async fn ensure_lego_sql() -> PathBuf {
    let dir = cache_dir();
    let path = dir.join("lego.sql");

    if path.exists() {
        println!("Using cached {}", path.display());
        return path;
    }

    println!("Downloading lego database from {LEGO_SQL_URL}...");
    std::fs::create_dir_all(&dir).expect("failed to create cache dir");

    let resp = reqwest::get(LEGO_SQL_URL)
        .await
        .expect("failed to download lego.sql");

    if !resp.status().is_success() {
        panic!("download failed: HTTP {}", resp.status());
    }

    let bytes = resp.bytes().await.expect("failed to read response body");
    std::fs::write(&path, &bytes).expect("failed to write lego.sql to cache");

    println!("Cached to {}", path.display());
    path
}

/// Extract the database name from a postgres:// URL, ignoring query params.
fn db_name_from_url(url: &str) -> &str {
    let without_query = url.split('?').next().unwrap_or(url);
    without_query.rsplit('/').next().unwrap_or("sqlc_lego")
}

/// Build a maintenance URL by replacing the database name with `postgres`.
fn maintenance_url(url: &str) -> String {
    let db_name = db_name_from_url(url);
    // Replace the last occurrence of /db_name with /postgres (before any query string)
    if let Some(pos) = url.rfind(&format!("/{db_name}")) {
        let mut result = url[..pos].to_string();
        result.push_str("/postgres");
        result.push_str(&url[pos + db_name.len() + 1..]);
        result
    } else {
        url.to_string()
    }
}

/// Quote a database name as an SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Commands::Setup = cli.command {
        cmd_setup(&cli.database_url).await;
        return;
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cli.database_url)
        .await
        .expect("failed to connect to database");

    let dir = &cli.sqlc_dir;
    match cli.command {
        Commands::Setup => unreachable!(),
        Commands::Migrate => cmd_migrate(&pool).await,
        Commands::Parts { set_num } => cmd_parts(&pool, dir, &set_num).await,
        Commands::Summary { set_num } => cmd_summary(&pool, dir, &set_num).await,
        Commands::Spares { set_num } => cmd_spares(&pool, dir, &set_num).await,
        Commands::ByColor { set_num, color } => cmd_by_color(&pool, dir, &set_num, &color).await,
        Commands::ByCategory { set_num, category } => {
            cmd_by_category(&pool, dir, &set_num, &category).await
        }
        Commands::Themes {
            min_year,
            theme_ids,
        } => cmd_themes(&pool, dir, min_year, &theme_ids).await,
        Commands::Combined { min_year } => cmd_combined(&pool, dir, min_year).await,
        Commands::Count { theme_id } => cmd_count(&pool, dir, theme_id).await,
        Commands::SharedMoulds => {
            cmd_moulds(
                &pool,
                dir,
                "reports/shared_moulds.sqlc",
                "shared by Technic and City",
            )
            .await;
        }
        Commands::CityOnlyMoulds => {
            cmd_moulds(
                &pool,
                dir,
                "reports/city_only_moulds.sqlc",
                "used by City and not Technic",
            )
            .await;
        }
        Commands::Laws => {
            if !cmd_laws(&pool, dir).await {
                std::process::exit(1);
            }
        }
        Commands::All => {
            if !cmd_all(&pool, dir).await {
                std::process::exit(1);
            }
        }
    }
}

// ── setup ───────────────────────────────────────────────────────────

async fn cmd_setup(database_url: &str) {
    // 1. Download lego.sql if not cached
    let sql_path = ensure_lego_sql().await;

    // 2. Drop and recreate the database so reruns work cleanly
    let db_name = db_name_from_url(database_url);
    let maint_url = maintenance_url(database_url);

    println!("Connecting to maintenance database...");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&maint_url)
        .await
        .expect("failed to connect to maintenance database — is PostgreSQL running?");

    println!("Dropping database {db_name} (if exists)...");
    // Force-disconnect other sessions before dropping
    let _ = sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(db_name)
    .execute(&admin_pool)
    .await;

    let ident = quote_ident(db_name);
    sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {ident}"))
        .execute(&admin_pool)
        .await
        .expect("failed to drop database");

    println!("Creating database {db_name}...");
    sqlx::raw_sql(&format!("CREATE DATABASE {ident}"))
        .execute(&admin_pool)
        .await
        .expect("failed to create database");

    admin_pool.close().await;

    // 3. Load the lego data via psql. ON_ERROR_STOP makes a failed statement fail the load: without
    //    it psql reports success whatever the script did.
    println!("Loading lego data into {db_name}...");
    let status = Command::new("psql")
        .arg("-X")
        .arg("-q")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg(database_url)
        .arg("-f")
        .arg(&sql_path)
        .stdout(std::process::Stdio::null())
        .status()
        .expect("failed to run psql — is PostgreSQL installed?");

    if !status.success() {
        eprintln!(
            "psql failed loading {} — see the error above",
            sql_path.display()
        );
        std::process::exit(1);
    }

    // 4. Run migrations
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("failed to connect to database after loading data");

    cmd_migrate(&pool).await;

    println!("\nSetup complete! Try:");
    println!("  cargo run -p lego-example -- all");
}

// ── migrate ─────────────────────────────────────────────────────────

async fn cmd_migrate(pool: &PgPool) {
    println!("Running migrations...");

    let migrations = [
        include_str!("../migrations/20240101000000_example_tables.sql"),
        include_str!("../migrations/20260926000000_versions.sql"),
        include_str!("../migrations/20260930000000_strands.sql"),
    ];
    for migration_sql in migrations {
        sqlx::raw_sql(migration_sql)
            .execute(pool)
            .await
            .expect("migration failed");
    }

    println!("Migrations complete.");
}

// ── :compose() + :bind() ────────────────────────────────────────────

async fn cmd_parts(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "sets/select_set_parts.sqlc");
    print_header("sets/select_set_parts", &result);

    let rows = bound(&result, &[text("set_num", set_num)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    // version -> (inventory_id, lines, pieces excluding spares)
    let mut versions: BTreeMap<i32, (i32, usize, i64)> = BTreeMap::new();
    for row in &rows {
        let v = versions
            .entry(row.get("version"))
            .or_insert((row.get("inventory_id"), 0, 0));
        v.1 += 1;
        if !row.get::<bool, _>("is_spare") {
            v.2 += i64::from(row.get::<i32, _>("quantity"));
        }
    }
    println!(
        "{} lines of set {set_num}, in {} version(s):",
        rows.len(),
        versions.len()
    );
    for (version, (inventory_id, lines, pieces)) in &versions {
        println!("  version {version} (inventory {inventory_id}): {lines} lines, {pieces} pieces");
    }
    for row in rows.iter().take(20) {
        println!(
            "  v{} {:<40} {:<20} {:<15} qty={}  spare={}{}",
            row.get::<i32, _>("version"),
            shown(row.get::<Option<String>, _>("part_name")),
            shown(row.get::<Option<String>, _>("category_name")),
            shown(row.get::<Option<String>, _>("color_name")),
            row.get::<i32, _>("quantity"),
            row.get::<bool, _>("is_spare"),
            if row.get::<String, _>("part_status") == "uncatalogued" {
                format!(
                    "  (part {} is not in the catalogue)",
                    row.get::<String, _>("part_num")
                )
            } else {
                String::new()
            },
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

// ── :compose() in INSERT SELECT ─────────────────────────────────────

async fn cmd_summary(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "reports/insert_set_summary.sqlc");
    print_header("reports/insert_set_summary", &result);

    // set_num is bound once and used twice: in the CTE WHERE and as the INSERT value
    let affected = bound(&result, &[text("set_num", set_num)])
        .execute(pool)
        .await
        .expect("query failed")
        .rows_affected();

    println!("Wrote {affected} category summary rows for set {set_num}:");
    let totals = sqlx::query(
        "SELECT version, count(*) AS categories, sum(total_parts) AS parts, \
         sum(total_spare) AS spares FROM set_category_summary WHERE set_num = $1 \
         GROUP BY version ORDER BY version",
    )
    .bind(set_num)
    .fetch_all(pool)
    .await
    .expect("query failed");
    for row in &totals {
        println!(
            "  version {}: {} categories, {} parts, {} spares",
            row.get::<i32, _>("version"),
            row.get::<i64, _>("categories"),
            row.get::<i64, _>("parts"),
            row.get::<i64, _>("spares"),
        );
    }
}

// ── :compose() in INSERT … ON CONFLICT, then in UPDATE ──────────────

async fn cmd_spares(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let track = compose(sqlc_dir, "inventory/track_set_parts.sqlc");
    print_header("inventory/track_set_parts", &track);
    let tracked = bound(&track, &[text("set_num", set_num)])
        .execute(pool)
        .await
        .expect("query failed")
        .rows_affected();
    println!("Started tracking {tracked} parts of set {set_num}.");

    let result = compose(sqlc_dir, "inventory/update_spare_counts.sqlc");
    print_header("inventory/update_spare_counts", &result);

    // set_num is bound once and used twice: in the CTE and in the UPDATE WHERE
    let affected = bound(&result, &[text("set_num", set_num)])
        .execute(pool)
        .await
        .expect("query failed")
        .rows_affected();

    println!("Updated {affected} inventory tracking rows for set {set_num}.");
}

// ── @slot composition ───────────────────────────────────────────────

async fn cmd_by_color(pool: &PgPool, sqlc_dir: &Path, set_num: &str, color: &str) {
    let result = compose(sqlc_dir, "sets/select_colored_parts.sqlc");
    print_header("sets/select_colored_parts", &result);

    let rows = bound(
        &result,
        &[text("color_name", color), text("set_num", set_num)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    println!("{} lines in color '{color}' in set {set_num}:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  v{} {:<40} {:<20} {:<15} qty={}",
            row.get::<i32, _>("version"),
            shown(row.get::<Option<String>, _>("part_name")),
            shown(row.get::<Option<String>, _>("category_name")),
            shown(row.get::<Option<String>, _>("color_name")),
            row.get::<i32, _>("quantity"),
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

async fn cmd_by_category(pool: &PgPool, sqlc_dir: &Path, set_num: &str, category: &str) {
    let result = compose(sqlc_dir, "sets/select_category_parts.sqlc");
    print_header("sets/select_category_parts", &result);

    let rows = bound(
        &result,
        &[text("category_name", category), text("set_num", set_num)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    println!(
        "{} lines in category '{category}' in set {set_num}:",
        rows.len()
    );
    for row in rows.iter().take(20) {
        println!(
            "  v{} {:<40} {:<15} qty={}",
            row.get::<i32, _>("version"),
            shown(row.get::<Option<String>, _>("part_name")),
            shown(row.get::<Option<String>, _>("color_name")),
            row.get::<i32, _>("quantity"),
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

// ── Multi-value :bind() IN clause ───────────────────────────────────

async fn cmd_themes(pool: &PgPool, sqlc_dir: &Path, min_year: i32, theme_ids: &[i32]) {
    // compose_with_values expands :bind(theme_ids EXPECTING 1..20)
    // into the right number of placeholders based on value count.
    let mut value_counts: BTreeMap<String, Vec<()>> = BTreeMap::new();
    value_counts.insert("min_year".into(), vec![()]);
    value_counts.insert("theme_ids".into(), vec![(); theme_ids.len()]);

    let result = compose_multi(sqlc_dir, "sets/select_sets_by_themes.sqlc", &value_counts);
    print_header("sets/select_sets_by_themes", &result);

    let mut values = vec![int("min_year", min_year)];
    values.extend(theme_ids.iter().map(|id| int("theme_ids", *id)));
    let rows = bound(&result, &values)
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} sets:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<15} {:<50} {:>4}  {:>5} parts  ({})",
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            shown(row.get::<Option<i32>, _>("year")),
            shown(row.get::<Option<i32>, _>("num_parts")),
            row.get::<String, _>("theme_name"),
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

// ── :union() ────────────────────────────────────────────────────────

async fn cmd_combined(pool: &PgPool, sqlc_dir: &Path, min_year: i32) {
    let result = compose(sqlc_dir, "reports/combined_theme_sets.sqlc");
    print_header("reports/combined_theme_sets", &result);

    let rows = bound(
        &result,
        &[
            int("city_theme_id", CITY_THEME_ID),
            int("min_year", min_year),
            int("technic_theme_id", TECHNIC_THEME_ID),
        ],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    println!("{} combined sets:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  [{:<8}] {:<15} {:<50} {:>4}  {:>5} parts",
            row.get::<String, _>("theme_group"),
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            shown(row.get::<Option<i32>, _>("year")),
            shown(row.get::<Option<i32>, _>("num_parts")),
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

// ── :count(DISTINCT) ────────────────────────────────────────────────

async fn cmd_count(pool: &PgPool, sqlc_dir: &Path, theme_id: i32) {
    let result = compose(sqlc_dir, "reports/count_theme_parts.sqlc");
    print_header("reports/count_theme_parts", &result);

    let row = bound(&result, &[int("theme_id", theme_id)])
        .fetch_one(pool)
        .await
        .expect("query failed");

    let count: i64 = row.get(0);
    println!("Distinct moulds in the scope of theme {theme_id}: {count}");
}

// ── :intersect() and :except() ──────────────────────────────────────

async fn cmd_moulds(pool: &PgPool, sqlc_dir: &Path, template: &str, what: &str) {
    let result = compose(sqlc_dir, template);
    print_header(template.trim_end_matches(".sqlc"), &result);

    let rows = bound(
        &result,
        &[
            int("city_theme_id", CITY_THEME_ID),
            int("technic_theme_id", TECHNIC_THEME_ID),
        ],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    println!("{} moulds {what}.", rows.len());
}

// ── laws ────────────────────────────────────────────────────────────
//
// Each law checks a composed template against the same answer computed directly in SQL, and
// prints HOLDS or FAILS with the figures on both sides.

fn verdict(law: &str, holds: bool, detail: &str) -> bool {
    println!(
        "LAW {law}: {} — {detail}",
        if holds { "HOLDS" } else { "FAILS" }
    );
    holds
}

/// The keys of a composed selection's rows, and whether any key came back twice.
async fn keys_of(
    pool: &PgPool,
    result: &ComposedSql,
    values: &[(&str, Value)],
) -> (BTreeSet<LineKey>, usize) {
    let rows = bound(result, values)
        .fetch_all(pool)
        .await
        .expect("query failed");
    let keys: BTreeSet<LineKey> = rows.iter().map(line_key).collect();
    (keys, rows.len())
}

/// A set's lines chosen by a predicate on the line, read straight from the tables.
async fn lines_where(
    pool: &PgPool,
    set_num: &str,
    predicate: &str,
    arg: &str,
) -> BTreeSet<LineKey> {
    sqlx::query(&format!(
        "SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare \
         FROM lego_inventory_parts ip JOIN lego_inventories i ON i.id = ip.inventory_id \
         WHERE i.set_num = $1 AND {predicate}"
    ))
    .bind(set_num)
    .bind(arg)
    .fetch_all(pool)
    .await
    .expect("query failed")
    .iter()
    .map(line_key)
    .collect()
}

async fn cmd_laws(pool: &PgPool, sqlc_dir: &Path) -> bool {
    println!("\n=== Laws ===");
    let mut all_hold = true;

    // 1. A set's parts are a set of lines, listed per version, and each version's pieces are the
    //    catalogue's own figure for the set.
    let parts = compose(sqlc_dir, "sets/select_set_parts.sqlc");
    for set_num in [DEFAULT_SET, VERSIONED_SET] {
        let rows = bound(&parts, &[text("set_num", set_num)])
            .fetch_all(pool)
            .await
            .expect("query failed");
        let keys: BTreeSet<LineKey> = rows.iter().map(line_key).collect();
        let mut pieces: BTreeMap<i32, i64> = BTreeMap::new();
        for row in rows.iter().filter(|r| !r.get::<bool, _>("is_spare")) {
            *pieces.entry(row.get("version")).or_default() +=
                i64::from(row.get::<i32, _>("quantity"));
        }
        let raw: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM lego_inventory_parts ip \
             JOIN lego_inventories i ON i.id = ip.inventory_id WHERE i.set_num = $1",
        )
        .bind(set_num)
        .fetch_one(pool)
        .await
        .expect("query failed");
        let filed: Option<i32> =
            sqlx::query_scalar("SELECT num_parts FROM lego_sets WHERE set_num = $1")
                .bind(set_num)
                .fetch_one(pool)
                .await
                .expect("query failed");
        let holds = rows.len() == keys.len()
            && rows.len() as i64 == raw
            && !pieces.is_empty()
            && pieces.values().all(|p| Some(*p) == filed.map(i64::from));
        all_hold &= verdict(
            &format!("parts of {set_num} are its lines, per version"),
            holds,
            &format!(
                "{} rows, {} distinct lines, {raw} lines in the tables; pieces per version {:?} against num_parts {}",
                rows.len(),
                keys.len(),
                pieces,
                shown(filed)
            ),
        );
    }

    // 2. The colour filter keeps exactly the set's lines in that colour, each once.
    let colored = compose(sqlc_dir, "sets/select_colored_parts.sqlc");
    let (keys, rows) = keys_of(
        pool,
        &colored,
        &[text("color_name", "Black"), text("set_num", DEFAULT_SET)],
    )
    .await;
    let truth = lines_where(
        pool,
        DEFAULT_SET,
        "ip.color_id IN (SELECT c.id FROM lego_colors c WHERE c.name = $2)",
        "Black",
    )
    .await;
    all_hold &= verdict(
        "by-color keeps the set's lines in that color",
        rows == keys.len() && keys == truth,
        &format!(
            "{rows} rows, {} distinct lines, {} in the tables",
            keys.len(),
            truth.len()
        ),
    );

    // 3. The category filter keeps exactly the set's lines whose part is in that category.
    let category = compose(sqlc_dir, "sets/select_category_parts.sqlc");
    let (keys, rows) = keys_of(
        pool,
        &category,
        &[
            text("category_name", "Plates"),
            text("set_num", DEFAULT_SET),
        ],
    )
    .await;
    let truth = lines_where(
        pool,
        DEFAULT_SET,
        "ip.part_num IN (SELECT p.part_num FROM lego_parts p \
         JOIN lego_part_categories pc ON pc.id = p.part_cat_id WHERE pc.name = $2)",
        "Plates",
    )
    .await;
    all_hold &= verdict(
        "by-category keeps the set's lines whose part is in that category",
        rows == keys.len() && keys == truth,
        &format!(
            "{rows} rows, {} distinct lines, {} in the tables",
            keys.len(),
            truth.len()
        ),
    );

    // 4. A theme scope is the theme and every theme below it: the composed count, against a walk
    //    up from each set's theme to its root.
    let count = compose(sqlc_dir, "reports/count_theme_parts.sqlc");
    let composed: i64 = bound(&count, &[int("theme_id", STAR_WARS_THEME_ID)])
        .fetch_one(pool)
        .await
        .expect("query failed")
        .get(0);
    let walked: i64 = sqlx::query_scalar(
        "WITH RECURSIVE up (set_num, theme_id) AS ( \
             SELECT s.set_num, s.theme_id FROM lego_sets s WHERE s.theme_id IS NOT NULL \
           UNION \
             SELECT u.set_num, t.parent_id FROM up u JOIN lego_themes t ON t.id = u.theme_id \
             WHERE t.parent_id IS NOT NULL) \
         SELECT count(DISTINCT ip.part_num) FROM lego_inventory_parts ip \
         JOIN lego_inventories i ON i.id = ip.inventory_id \
         WHERE i.set_num IN (SELECT set_num FROM up WHERE theme_id = $1)",
    )
    .bind(STAR_WARS_THEME_ID)
    .fetch_one(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        "a theme scope is the theme's whole subtree",
        composed == walked,
        &format!("composed {composed} moulds, walked up {walked}"),
    );

    // 5. The moulds City shares with Technic and the moulds only City uses: no mould is in both,
    //    and together they are all of City's moulds.
    let scopes = [
        int("city_theme_id", CITY_THEME_ID),
        int("technic_theme_id", TECHNIC_THEME_ID),
    ];
    let mut moulds = Vec::new();
    for template in [
        "reports/shared_moulds.sqlc",
        "reports/city_only_moulds.sqlc",
        "queries/city_moulds.sqlc",
    ] {
        let result = compose(sqlc_dir, template);
        let values: Vec<(&str, Value)> = scopes
            .iter()
            .filter(|(name, _)| result.bind_params.iter().any(|p| p == name))
            .map(|(name, v)| (*name, v.clone()))
            .collect();
        let set: BTreeSet<String> = bound(&result, &values)
            .fetch_all(pool)
            .await
            .expect("query failed")
            .iter()
            .map(|r| r.get::<String, _>("part_num"))
            .collect();
        moulds.push(set);
    }
    let (shared, city_only, city) = (&moulds[0], &moulds[1], &moulds[2]);
    let union: BTreeSet<String> = shared.union(city_only).cloned().collect();
    all_hold &= verdict(
        "shared and City-only moulds together are City's, none in both",
        shared.is_disjoint(city_only) && &union == city,
        &format!(
            "{} + {} = {} against {}",
            shared.len(),
            city_only.len(),
            union.len(),
            city.len()
        ),
    );

    // 6. The summary is idempotent and matches the parts listing, version by version. Run in a
    //    transaction that is rolled back, so checking leaves nothing behind.
    //    A statement under test that errors is a law that fails, not a crash.
    let summary = compose(sqlc_dir, "reports/insert_set_summary.sqlc");
    let law = format!("summary of {VERSIONED_SET}, run twice, is one row per version and category");
    let mut tx = pool.begin().await.expect("begin failed");
    let mut failure = None;
    for _ in 0..2 {
        if let Err(e) = bound(&summary, &[text("set_num", VERSIONED_SET)])
            .execute(&mut *tx)
            .await
        {
            failure = Some(e.to_string());
            break;
        }
    }
    if let Some(e) = failure {
        tx.rollback().await.expect("rollback failed");
        verdict(&law, false, &format!("the summary itself failed: {e}"));
        println!("=== Laws: SOME FAIL ===");
        return false;
    }
    let rows: Vec<(i32, i64, i64)> = sqlx::query_as(
        "SELECT version, count(*), sum(total_parts) FROM set_category_summary \
         WHERE set_num = $1 GROUP BY version ORDER BY version",
    )
    .bind(VERSIONED_SET)
    .fetch_all(&mut *tx)
    .await
    .expect("query failed");
    // The same figures straight from the tables: each version's categories and non-spare pieces,
    // over the lines whose part has a category (the lines the summary covers).
    let categories: Vec<(i32, i64, i64)> = sqlx::query_as(
        "SELECT i.version, count(DISTINCT p.part_cat_id), \
         coalesce(sum(ip.quantity) FILTER (WHERE NOT ip.is_spare), 0) \
         FROM lego_inventory_parts ip \
         JOIN lego_inventories i ON i.id = ip.inventory_id \
         JOIN lego_parts p ON p.part_num = ip.part_num \
         WHERE i.set_num = $1 GROUP BY i.version ORDER BY i.version",
    )
    .bind(VERSIONED_SET)
    .fetch_all(&mut *tx)
    .await
    .expect("query failed");
    tx.rollback().await.expect("rollback failed");
    let holds = rows == categories;
    all_hold &= verdict(
        &law,
        holds,
        &format!("(version, rows, parts) {rows:?} against the tables' (version, categories, pieces) {categories:?}"),
    );

    println!(
        "=== Laws: {} ===",
        if all_hold { "all hold" } else { "SOME FAIL" }
    );
    all_hold
}

// ── Run all examples ────────────────────────────────────────────────

async fn cmd_all(pool: &PgPool, sqlc_dir: &Path) -> bool {
    println!("=== Running all examples ===");

    cmd_parts(pool, sqlc_dir, DEFAULT_SET).await;
    cmd_parts(pool, sqlc_dir, VERSIONED_SET).await;
    cmd_summary(pool, sqlc_dir, DEFAULT_SET).await;
    cmd_spares(pool, sqlc_dir, DEFAULT_SET).await;
    cmd_by_color(pool, sqlc_dir, DEFAULT_SET, "Black").await;
    cmd_by_category(pool, sqlc_dir, DEFAULT_SET, "Plates").await;
    cmd_themes(
        pool,
        sqlc_dir,
        2010,
        &[TECHNIC_THEME_ID, CITY_THEME_ID, STAR_WARS_THEME_ID],
    )
    .await;
    cmd_combined(pool, sqlc_dir, 2010).await;
    cmd_count(pool, sqlc_dir, STAR_WARS_THEME_ID).await;
    cmd_moulds(
        pool,
        sqlc_dir,
        "reports/shared_moulds.sqlc",
        "shared by Technic and City",
    )
    .await;
    cmd_moulds(
        pool,
        sqlc_dir,
        "reports/city_only_moulds.sqlc",
        "used by City and not Technic",
    )
    .await;

    println!("\n=== All examples complete ===");
    cmd_laws(pool, sqlc_dir).await
}
