use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Parser, Subcommand};
use sql_composer::composer::{ComposedSql, Composer};
use sql_composer::parser::parse_template_file;
use sql_composer::types::Dialect;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const LEGO_SQL_URL: &str =
    "https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql";

#[derive(Parser)]
#[command(name = "lego", about = "sql-composer example using the Lego database")]
struct Cli {
    /// Postgres connection URL
    #[arg(long, env = "SQLC_LEGO_DATABASE_URL", default_value = "postgres:///sqlc_lego")]
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
    /// Run sqlx migrations for example tables
    Migrate,
    /// List parts for a set (:compose + :bind)
    Parts { set_num: String },
    /// Insert set category summary (:compose in INSERT)
    Summary { set_num: String },
    /// Update spare counts (:compose in UPDATE)
    Spares { set_num: String },
    /// Filter parts by color (@slot composition)
    ByColor { set_num: String, color: String },
    /// Filter parts by category (@slot composition)
    ByCategory { set_num: String, category: String },
    /// Find sets by theme IDs (multi-value :bind IN)
    Themes {
        min_year: i32,
        #[arg(required = true, num_args = 1..)]
        theme_ids: Vec<i32>,
    },
    /// Combine Technic and City sets (:union)
    Combined { min_year: i32 },
    /// Count distinct parts in a theme (:count DISTINCT)
    Count { theme_name: String },
    /// Run all examples with default values
    All,
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

fn print_header(name: &str, result: &ComposedSql) {
    println!("\n── {name} ──");
    println!("SQL:\n{}\n", result.sql.trim());
    println!("Bind params: {:?}\n", result.bind_params);
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

    match cli.command {
        Commands::Setup => unreachable!(),
        Commands::Migrate => cmd_migrate(&pool).await,
        Commands::Parts { set_num } => cmd_parts(&pool, &cli.sqlc_dir, &set_num).await,
        Commands::Summary { set_num } => cmd_summary(&pool, &cli.sqlc_dir, &set_num).await,
        Commands::Spares { set_num } => cmd_spares(&pool, &cli.sqlc_dir, &set_num).await,
        Commands::ByColor { set_num, color } => {
            cmd_by_color(&pool, &cli.sqlc_dir, &set_num, &color).await
        }
        Commands::ByCategory { set_num, category } => {
            cmd_by_category(&pool, &cli.sqlc_dir, &set_num, &category).await
        }
        Commands::Themes { min_year, theme_ids } => {
            cmd_themes(&pool, &cli.sqlc_dir, min_year, &theme_ids).await
        }
        Commands::Combined { min_year } => cmd_combined(&pool, &cli.sqlc_dir, min_year).await,
        Commands::Count { theme_name } => cmd_count(&pool, &cli.sqlc_dir, &theme_name).await,
        Commands::All => cmd_all(&pool, &cli.sqlc_dir).await,
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
    let _ = sqlx::raw_sql(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
    ))
    .execute(&admin_pool)
    .await;

    sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .execute(&admin_pool)
        .await
        .expect("failed to drop database");

    println!("Creating database {db_name}...");
    sqlx::raw_sql(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .expect("failed to create database");

    admin_pool.close().await;

    // 3. Load the lego data via psql
    println!("Loading lego data into {db_name}...");
    let status = Command::new("psql")
        .arg(database_url)
        .arg("-f")
        .arg(&sql_path)
        .stdout(std::process::Stdio::null())
        .status()
        .expect("failed to run psql — is PostgreSQL installed?");

    if !status.success() {
        eprintln!("psql failed — check the connection URL and that psql is installed");
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

    let migration_sql = include_str!("../migrations/20240101000000_example_tables.sql");
    sqlx::raw_sql(migration_sql)
        .execute(pool)
        .await
        .expect("migration failed");

    println!("Migrations complete.");
}

// ── :compose() + :bind() ────────────────────────────────────────────

async fn cmd_parts(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "sets/select_set_parts.sqlc");
    print_header("sets/select_set_parts", &result);

    // bind_params: ["set_num"]
    let rows = sqlx::query(&result.sql)
        .bind(set_num)
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} rows:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<40} {:<20} {:<15} qty={}  spare={}",
            row.get::<String, _>("part_name"),
            row.get::<String, _>("category_name"),
            row.get::<String, _>("color_name"),
            row.get::<i32, _>("quantity"),
            row.get::<bool, _>("is_spare"),
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

    // bind_params: ["set_num"] — used in both the CTE WHERE and the INSERT value
    let affected = sqlx::query(&result.sql)
        .bind(set_num)
        .execute(pool)
        .await
        .expect("query failed")
        .rows_affected();

    println!("Inserted {affected} category summary rows for set {set_num}.");
}

// ── :compose() in UPDATE ────────────────────────────────────────────

async fn cmd_spares(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "inventory/update_spare_counts.sqlc");
    print_header("inventory/update_spare_counts", &result);

    // bind_params: ["set_num"] — same param in CTE and UPDATE WHERE
    let affected = sqlx::query(&result.sql)
        .bind(set_num)
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

    // bind_params: ["color_name", "set_num"] — alphabetical
    let rows = sqlx::query(&result.sql)
        .bind(color)
        .bind(set_num)
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} parts matching color '{color}' in set {set_num}:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<40} {:<20} {:<15} qty={}",
            row.get::<String, _>("part_name"),
            row.get::<String, _>("category_name"),
            row.get::<String, _>("color_name"),
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

    // bind_params: ["category_name", "set_num"] — alphabetical
    let rows = sqlx::query(&result.sql)
        .bind(category)
        .bind(set_num)
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!(
        "{} parts in category '{category}' for set {set_num}:",
        rows.len()
    );
    for row in rows.iter().take(20) {
        println!(
            "  {:<40} {:<15} qty={}",
            row.get::<String, _>("part_name"),
            row.get::<String, _>("color_name"),
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

    // bind_params: ["min_year", "theme_ids", "theme_ids", ...]
    // Bind values in order, tracking position per param name.
    let mut query = sqlx::query(&result.sql);
    let mut theme_idx = 0;
    for name in &result.bind_params {
        match name.as_str() {
            "min_year" => query = query.bind(min_year),
            "theme_ids" => {
                query = query.bind(theme_ids[theme_idx]);
                theme_idx += 1;
            }
            _ => panic!("unexpected bind param: {name}"),
        }
    }

    let rows = query.fetch_all(pool).await.expect("query failed");

    println!("{} sets:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<15} {:<50} {:>4}  {:>5} parts  ({})",
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            row.get::<Option<i32>, _>("year").unwrap_or(0),
            row.get::<Option<i32>, _>("num_parts").unwrap_or(0),
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

    // bind_params: ["city_theme", "min_year", "technic_theme"] — alphabetical
    let rows = sqlx::query(&result.sql)
        .bind("City") // city_theme
        .bind(min_year) // min_year
        .bind("Technic") // technic_theme
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
            row.get::<Option<i32>, _>("year").unwrap_or(0),
            row.get::<Option<i32>, _>("num_parts").unwrap_or(0),
        );
    }
    if rows.len() > 20 {
        println!("  ... and {} more", rows.len() - 20);
    }
}

// ── :count(DISTINCT) ────────────────────────────────────────────────

async fn cmd_count(pool: &PgPool, sqlc_dir: &Path, theme_name: &str) {
    let result = compose(sqlc_dir, "reports/count_theme_parts.sqlc");
    print_header("reports/count_theme_parts", &result);

    // bind_params: ["theme_name"]
    let row = sqlx::query(&result.sql)
        .bind(theme_name)
        .fetch_one(pool)
        .await
        .expect("query failed");

    let count: i64 = row.get(0);
    println!("Distinct parts in theme '{theme_name}': {count}");
}

// ── Run all examples ────────────────────────────────────────────────

async fn cmd_all(pool: &PgPool, sqlc_dir: &Path) {
    println!("=== Running all examples ===");

    cmd_parts(pool, sqlc_dir, "75192-1").await;
    cmd_summary(pool, sqlc_dir, "75192-1").await;
    cmd_spares(pool, sqlc_dir, "75192-1").await;
    cmd_by_color(pool, sqlc_dir, "75192-1", "Black").await;
    cmd_by_category(pool, sqlc_dir, "75192-1", "Technic").await;
    cmd_themes(pool, sqlc_dir, 2010, &[1, 22, 158]).await;
    cmd_combined(pool, sqlc_dir, 2020).await;
    cmd_count(pool, sqlc_dir, "Star Wars").await;

    println!("\n=== All examples complete ===");
}
