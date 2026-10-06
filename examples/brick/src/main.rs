use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use sql_composer::composer::{ComposedSql, Composer};
use sql_composer::parser::parse_template_file;
use sql_composer::types::Dialect;
use sql_composer::MockTable;
use sqlx::postgres::{PgArguments, PgPoolOptions, PgRow};
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Postgres, Row};

/// The set the examples use by default: Café Corner, a single-version set.
const DEFAULT_SET: &str = "10182-1";
/// A set LEGO sold in two versions, whose parts are listed version by version.
const VERSIONED_SET: &str = "75053-1";
/// Theme ids of the default scopes. A scope is the theme and every theme below it.
const TECHNIC_THEME_ID: i32 = 1;
const CITY_THEME_ID: i32 = 52;
const STAR_WARS_THEME_ID: i32 = 158;
/// A small scope: two builders own every one of its sets.
const SPYBIOTICS_THEME_ID: i32 = 463;
/// The builder whose purchases the examples read by default.
const DEFAULT_BUILDER: i32 = 3326;
/// The years the purchase rollup covers by default.
const ROLLUP_YEARS: (i32, i32) = (2016, 2017);
/// The search the word filter runs on the default set by default.
const DEFAULT_WORDS: &str = "window";
/// The regular expression the pattern filter runs on the default set by default.
const DEFAULT_PATTERN: &str = r"^(plate|tile) 1 x \d+$";
/// The search the ranked part search runs by default.
const SEARCH_WORDS: &str = "technic beam";
/// The city the geo searches start from by default: London, and the radius round it in km.
const DEFAULT_CITY_ID: i32 = 67;
const DEFAULT_KM: i32 = 5;
/// The postcodes the postcode search reads by default: London's SW district, by its prefix.
const DEFAULT_POSTCODES: &str = "^SW";

/// Every set paired with its own theme and every theme above it, walked up from the set: the direct
/// spelling the laws check a theme scope against.
const WALK_UP: &str = "WITH RECURSIVE up (set_num, theme_id) AS ( \
         SELECT s.set_num, s.theme_id FROM lego_sets s WHERE s.theme_id IS NOT NULL \
       UNION \
         SELECT u.set_num, t.parent_id FROM up u JOIN lego_themes t ON t.id = u.theme_id \
         WHERE t.parent_id IS NOT NULL) ";

#[derive(Parser)]
#[command(name = "brick", about = "sql-composer example on a brick catalogue")]
struct Cli {
    /// Postgres connection URL
    #[arg(
        long,
        env = "SQLC_BRICK_DATABASE_URL",
        default_value = "postgres:///sqlc_brick"
    )]
    database_url: String,

    /// Schema holding sqlc-brickgen's tables, the only schema the queries search
    #[arg(long, default_value = "lego")]
    schema: String,

    /// Path to the sqlc template directory
    #[arg(long, default_value = "examples/brick/sqlc")]
    sqlc_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
    /// A builder's purchases in order, numbered, with the pieces so far (window functions)
    History { builder_id: i32 },
    /// Each builder's latest purchase (DISTINCT ON)
    Latest,
    /// Each root theme's three newest sets (LATERAL)
    Newest,
    /// Purchases per year and root theme, with subtotals (ROLLUP)
    Rollup { from_year: i32, to_year: i32 },
    /// The median set size of each root theme (percentile_cont)
    Sizes,
    /// Every year from the first release to the last, the empty years kept (generate_series)
    Years,
    /// The sets no builder ever bought (NOT EXISTS)
    NeverBought,
    /// The builders who own every set of a theme scope (division)
    Complete { theme_id: i32 },
    /// A set's colours, version by version, as arrays (array_agg)
    Colors { set_num: String },
    /// A set as one JSON document (jsonb)
    Document { set_num: String },
    /// Keep a set's summary in step with MERGE
    Merge { set_num: String },
    /// A set's lines whose part names match a search (full text, @slot composition)
    ByWords { set_num: String, words: String },
    /// A set's lines whose part names match a regular expression (~*, @slot composition)
    ByPattern { set_num: String, pattern: String },
    /// The parts whose names match a search, best match first (ts_rank, ts_headline)
    Search {
        words: String,
        #[arg(default_value_t = 10)]
        top: i32,
    },
    /// The bricks whose names give their size (regexp_match)
    BrickSizes,
    /// What the builders near a city bought, by root theme (earthdistance, @slot composition)
    BoughtNear { city_id: i32, km: i32 },
    /// What the builders whose postcode matches a pattern bought, by root theme (~, @slot)
    BoughtIn { pattern: String },
    /// The builders nearest a city, nearest first (<->, LATERAL)
    Nearest {
        city_id: i32,
        #[arg(default_value_t = 5)]
        top: i32,
    },
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

/// Compose a template with mock tables: each statement that names a mocked table reads the mock's
/// rows instead of the table's.
fn compose_mocked(sqlc_dir: &Path, template: &str, mocks: Vec<MockTable>) -> ComposedSql {
    let mut composer = Composer::new(Dialect::Postgres);
    composer.add_search_path(sqlc_dir.to_path_buf());
    for mock in mocks {
        composer.add_mock_table(mock);
    }
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
    let mut query = sqlx::query(AssertSqlSafe(result.sql.as_str()));
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

/// A composed statement read through an outer query, which names its rows `d`. The statement's
/// parameters are unchanged, so the same values bind to it.
fn read_through(result: &ComposedSql, select: &str, from_rest: &str) -> ComposedSql {
    ComposedSql {
        sql: format!("SELECT {select} FROM ({}) d {from_rest}", result.sql),
        bind_params: result.bind_params.clone(),
    }
}

/// An absent value printed as an absence, never as a number or an empty string.
fn shown<T: Display>(v: Option<T>) -> String {
    v.map_or_else(|| "—".to_string(), |v| v.to_string())
}

/// A line's identity: (inventory_id, part_num, color_id, is_spare).
type LineKey = (i32, String, i32, bool);

/// A set's summary rows: (version, category_name, total_parts, total_spare).
type Summary = Vec<(i32, String, i32, i32)>;

fn line_key(row: &PgRow) -> LineKey {
    (
        row.get("inventory_id"),
        row.get("part_num"),
        row.get("color_id"),
        row.get("is_spare"),
    )
}

/// Quote a name as an SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Every connection searches the generator's schema, then the schemas holding the extensions
    // the searches use, and never `public`. The database's `public` schema holds the catalogue the
    // generator read, under the same table names, so a search that reached it would read that
    // catalogue wherever the generator's table is missing.
    let mut search_path = vec![quote_ident(&cli.schema)];
    {
        let mut conn = PgConnection::connect(&cli.database_url)
            .await
            .expect("failed to connect to database");
        let schemas: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT n.nspname FROM pg_extension e \
             JOIN pg_namespace n ON n.oid = e.extnamespace \
             WHERE e.extname IN ('cube', 'earthdistance', 'pg_trgm') AND n.nspname <> 'public' \
             ORDER BY 1",
        )
        .fetch_all(&mut conn)
        .await
        .expect("query failed");
        search_path.extend(schemas.iter().map(|s| quote_ident(s)));
    }
    let search_path = search_path.join(", ");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path', $1, false)")
                    .bind(search_path)
                    .execute(conn)
                    .await
                    .map(|_| ())
            })
        })
        .connect(&cli.database_url)
        .await
        .expect("failed to connect to database");

    let schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(&cli.schema)
            .fetch_one(&pool)
            .await
            .expect("query failed");
    if !schema_exists {
        eprintln!(
            "schema {} is not in this database: build it with sqlc-brickgen, as the example's \
             README shows, or name the schema that holds it with --schema",
            cli.schema
        );
        std::process::exit(1);
    }

    let dir = &cli.sqlc_dir;
    match cli.command {
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
        Commands::History { builder_id } => cmd_history(&pool, dir, builder_id).await,
        Commands::Latest => cmd_latest(&pool, dir).await,
        Commands::Newest => cmd_newest(&pool, dir).await,
        Commands::Rollup { from_year, to_year } => cmd_rollup(&pool, dir, from_year, to_year).await,
        Commands::Sizes => cmd_sizes(&pool, dir).await,
        Commands::Years => cmd_years(&pool, dir).await,
        Commands::NeverBought => cmd_never_bought(&pool, dir).await,
        Commands::Complete { theme_id } => cmd_complete(&pool, dir, theme_id).await,
        Commands::Colors { set_num } => cmd_colors(&pool, dir, &set_num).await,
        Commands::Document { set_num } => cmd_document(&pool, dir, &set_num).await,
        Commands::Merge { set_num } => cmd_merge(&pool, dir, &set_num).await,
        Commands::ByWords { set_num, words } => cmd_by_words(&pool, dir, &set_num, &words).await,
        Commands::ByPattern { set_num, pattern } => {
            cmd_by_pattern(&pool, dir, &set_num, &pattern).await
        }
        Commands::Search { words, top } => cmd_search(&pool, dir, &words, top).await,
        Commands::BrickSizes => cmd_brick_sizes(&pool, dir).await,
        Commands::BoughtNear { city_id, km } => cmd_bought_near(&pool, dir, city_id, km).await,
        Commands::BoughtIn { pattern } => cmd_bought_in(&pool, dir, &pattern).await,
        Commands::Nearest { city_id, top } => cmd_nearest(&pool, dir, city_id, top).await,
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

// ── migrate ─────────────────────────────────────────────────────────

async fn cmd_migrate(pool: &PgPool) {
    println!("Running migrations...");

    let migrations = [
        include_str!("../migrations/20240101000000_example_tables.sql"),
        include_str!("../migrations/20260926000000_versions.sql"),
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

/// Say how many rows a listing of the first 20 left out.
fn more(rows: usize) {
    if rows > 20 {
        println!("  ... and {} more", rows - 20);
    }
}

// ── window functions ────────────────────────────────────────────────

async fn cmd_history(pool: &PgPool, sqlc_dir: &Path, builder_id: i32) {
    let result = compose(sqlc_dir, "builders/purchase_history.sqlc");
    print_header("builders/purchase_history", &result);

    let rows = bound(&result, &[int("builder_id", builder_id)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} purchases by builder {builder_id}:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:>3}. {:<15} {:<50} {:>5} pieces, {:>6} so far",
            row.get::<i64, _>("nth"),
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            shown(row.get::<Option<i32>, _>("num_parts")),
            shown(row.get::<Option<i64>, _>("pieces_so_far")),
        );
    }
    more(rows.len());
}

// ── DISTINCT ON ─────────────────────────────────────────────────────

async fn cmd_latest(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "builders/latest_purchases.sqlc");
    print_header("builders/latest_purchases", &result);

    let rows = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} builders, each with their latest purchase:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  builder {:>6} {:<20} purchase {:>9}  set {}",
            row.get::<i32, _>("builder_id"),
            row.get::<String, _>("builder_name"),
            row.get::<i64, _>("purchase_id"),
            row.get::<String, _>("set_num"),
        );
    }
    more(rows.len());
}

// ── LATERAL ─────────────────────────────────────────────────────────

async fn cmd_newest(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "themes/newest_sets.sqlc");
    print_header("themes/newest_sets", &result);

    let rows = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed");

    let roots: BTreeSet<i32> = rows.iter().map(|r| r.get("root_id")).collect();
    println!(
        "{} sets, the newest of {} root themes:",
        rows.len(),
        roots.len()
    );
    for row in rows.iter().take(20) {
        println!(
            "  {:<25} {:<15} {:<50} {:>4}",
            row.get::<String, _>("root_name"),
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            shown(row.get::<Option<i32>, _>("year")),
        );
    }
    more(rows.len());
}

// ── ROLLUP ──────────────────────────────────────────────────────────

async fn cmd_rollup(pool: &PgPool, sqlc_dir: &Path, from_year: i32, to_year: i32) {
    let result = compose(sqlc_dir, "reports/purchases_by_year_theme.sqlc");
    print_header("reports/purchases_by_year_theme", &result);

    let rows = bound(
        &result,
        &[int("from_year", from_year), int("to_year", to_year)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    let themes = rows
        .iter()
        .filter(|r| r.get::<String, _>("level") == "theme")
        .count();
    println!("{themes} (year, root theme) rows, with these subtotals:");
    for row in rows
        .iter()
        .filter(|r| r.get::<String, _>("level") != "theme")
    {
        println!(
            "  {:<5} {:>4}  {:>8} purchases",
            row.get::<String, _>("level"),
            shown(row.get::<Option<i32>, _>("year")),
            row.get::<i64, _>("purchases"),
        );
    }
}

// ── percentile_cont ─────────────────────────────────────────────────

async fn cmd_sizes(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "reports/theme_set_sizes.sqlc");
    print_header("reports/theme_set_sizes", &result);

    let rows = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} root themes:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<25} {:>5} sets ({:>5} sized): smallest {:>5}, median {:>7}, largest {:>5}",
            row.get::<String, _>("root_name"),
            row.get::<i64, _>("sets"),
            row.get::<i64, _>("sized"),
            shown(row.get::<Option<i32>, _>("smallest")),
            shown(row.get::<Option<f64>, _>("median")),
            shown(row.get::<Option<i32>, _>("largest")),
        );
    }
    more(rows.len());
}

// ── generate_series ─────────────────────────────────────────────────

async fn cmd_years(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "reports/release_years.sqlc");
    print_header("reports/release_years", &result);

    let rows: Vec<(i32, i64)> = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("year"), r.get("sets")))
        .collect();

    let empty: Vec<String> = rows
        .iter()
        .filter(|(_, sets)| *sets == 0)
        .map(|(year, _)| year.to_string())
        .collect();
    println!(
        "{} years, {} to {}; no release in {}: {}",
        rows.len(),
        shown(rows.first().map(|(y, _)| y)),
        shown(rows.last().map(|(y, _)| y)),
        empty.len(),
        empty.join(", ")
    );
}

// ── NOT EXISTS ──────────────────────────────────────────────────────

async fn cmd_never_bought(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "sets/never_bought.sqlc");
    print_header("sets/never_bought", &result);

    let rows = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} sets no builder ever bought:", rows.len());
    for row in rows.iter().take(20) {
        println!(
            "  {:<15} {:<60} {:>4}",
            row.get::<String, _>("set_num"),
            row.get::<String, _>("name"),
            shown(row.get::<Option<i32>, _>("year")),
        );
    }
    more(rows.len());
}

// ── division ────────────────────────────────────────────────────────

async fn cmd_complete(pool: &PgPool, sqlc_dir: &Path, theme_id: i32) {
    let result = compose(sqlc_dir, "builders/complete_scope.sqlc");
    print_header("builders/complete_scope", &result);

    let rows = bound(&result, &[int("theme_id", theme_id)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!(
        "{} builders own every set in the scope of theme {theme_id}:",
        rows.len()
    );
    for row in rows.iter().take(20) {
        println!(
            "  builder {:>6} {}",
            row.get::<i32, _>("builder_id"),
            row.get::<String, _>("name"),
        );
    }
    more(rows.len());
}

// ── arrays ──────────────────────────────────────────────────────────

async fn cmd_colors(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "sets/set_colors.sqlc");
    print_header("sets/set_colors", &result);

    let rows = bound(&result, &[text("set_num", set_num)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("Colours of set {set_num}, version by version:");
    for row in &rows {
        let colors: Vec<String> = row
            .get::<Option<Vec<String>>, _>("colors")
            .unwrap_or_default();
        println!(
            "  version {}: {} colours, {} lines with no colour name: {}",
            row.get::<i32, _>("version"),
            colors.len(),
            row.get::<i64, _>("unnamed_lines"),
            colors.join(", "),
        );
    }
}

// ── jsonb ───────────────────────────────────────────────────────────

async fn cmd_document(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "sets/set_document.sqlc");
    print_header("sets/set_document", &result);

    let pretty = read_through(&result, "jsonb_pretty(d.document) AS document", "");
    let rows = bound(&pretty, &[text("set_num", set_num)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    for row in &rows {
        let document: String = row.get("document");
        let lines: Vec<&str> = document.lines().collect();
        for line in lines.iter().take(30) {
            println!("  {line}");
        }
        if lines.len() > 30 {
            println!("  ... and {} more lines of the document", lines.len() - 30);
        }
    }
    if rows.is_empty() {
        println!("No set {set_num}.");
    }
}

// ── MERGE ───────────────────────────────────────────────────────────

async fn cmd_merge(pool: &PgPool, sqlc_dir: &Path, set_num: &str) {
    let result = compose(sqlc_dir, "reports/merge_set_summary.sqlc");
    print_header("reports/merge_set_summary", &result);

    let affected = bound(&result, &[text("set_num", set_num)])
        .execute(pool)
        .await
        .expect("query failed")
        .rows_affected();

    println!("Merged {affected} category summary rows for set {set_num}.");
}

// ── full text and regular expressions ──────────────────────────────

/// A set's lines as the filtered listings print them, the first twenty.
fn print_lines(rows: &[PgRow]) {
    for row in rows.iter().take(20) {
        println!(
            "  v{} {:<45} {:<20} {:<15} qty={}",
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

async fn cmd_by_words(pool: &PgPool, sqlc_dir: &Path, set_num: &str, words: &str) {
    let result = compose(sqlc_dir, "sets/select_parts_by_words.sqlc");
    print_header("sets/select_parts_by_words", &result);

    let rows = bound(&result, &[text("set_num", set_num), text("words", words)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!(
        "{} lines of set {set_num} whose part names match '{words}':",
        rows.len()
    );
    print_lines(&rows);
}

async fn cmd_by_pattern(pool: &PgPool, sqlc_dir: &Path, set_num: &str, pattern: &str) {
    let result = compose(sqlc_dir, "sets/select_parts_by_pattern.sqlc");
    print_header("sets/select_parts_by_pattern", &result);

    let rows = bound(
        &result,
        &[text("pattern", pattern), text("set_num", set_num)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");

    println!(
        "{} lines of set {set_num} whose part names match /{pattern}/:",
        rows.len()
    );
    print_lines(&rows);
}

async fn cmd_search(pool: &PgPool, sqlc_dir: &Path, words: &str, top: i32) {
    let result = compose(sqlc_dir, "parts/search_parts.sqlc");
    print_header("parts/search_parts", &result);

    let rows = bound(&result, &[int("top", top), text("words", words)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("The {} parts that best match '{words}':", rows.len());
    for row in &rows {
        println!(
            "  {:<10} {:.4}  {}",
            row.get::<String, _>("part_num"),
            row.get::<f32, _>("rank"),
            row.get::<String, _>("marked"),
        );
    }
}

async fn cmd_brick_sizes(pool: &PgPool, sqlc_dir: &Path) {
    let result = compose(sqlc_dir, "parts/brick_sizes.sqlc");
    print_header("parts/brick_sizes", &result);

    let rows = bound(&result, &[])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("{} bricks whose names give their size:", rows.len());
    for row in &rows {
        println!(
            "  {:<10} {:>2} x {:<2}  {}",
            row.get::<String, _>("part_num"),
            row.get::<i32, _>("width"),
            row.get::<i32, _>("length"),
            row.get::<String, _>("name"),
        );
    }
}

// ── places ──────────────────────────────────────────────────────────

/// What an area's builders bought, by root theme, as the area listings print it.
fn print_area_purchases(rows: &[PgRow]) {
    for row in rows.iter().take(10) {
        println!(
            "  {:<35} {:>6} purchases by {:>4} builders",
            row.get::<String, _>("root_name"),
            row.get::<i64, _>("purchases"),
            row.get::<i64, _>("builders"),
        );
    }
    if rows.len() > 10 {
        println!("  ... and {} more root themes", rows.len() - 10);
    }
}

async fn cmd_bought_near(pool: &PgPool, sqlc_dir: &Path, city_id: i32, km: i32) {
    let result = compose(sqlc_dir, "builders/bought_near.sqlc");
    print_header("builders/bought_near", &result);

    let rows = bound(&result, &[int("city_id", city_id), int("km", km)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("What the builders within {km} km of city {city_id} bought, by root theme:");
    print_area_purchases(&rows);
}

async fn cmd_bought_in(pool: &PgPool, sqlc_dir: &Path, pattern: &str) {
    let result = compose(sqlc_dir, "builders/bought_in_postcodes.sqlc");
    print_header("builders/bought_in_postcodes", &result);

    let rows = bound(&result, &[text("pattern", pattern)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("What the builders whose postcode matches /{pattern}/ bought, by root theme:");
    print_area_purchases(&rows);
}

async fn cmd_nearest(pool: &PgPool, sqlc_dir: &Path, city_id: i32, top: i32) {
    let result = compose(sqlc_dir, "builders/nearest.sqlc");
    print_header("builders/nearest", &result);

    let rows = bound(&result, &[int("city_id", city_id), int("top", top)])
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("The {} builders nearest city {city_id}:", rows.len());
    for row in &rows {
        println!(
            "  builder {:>6}  {:>8.0} m",
            row.get::<i32, _>("builder_id"),
            row.get::<f64, _>("metres"),
        );
    }
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
    sqlx::query(AssertSqlSafe(format!(
        "SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare \
         FROM lego_inventory_parts ip JOIN lego_inventories i ON i.id = ip.inventory_id \
         WHERE i.set_num = $1 AND {predicate}"
    )))
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
    let walked: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "{WALK_UP} SELECT count(DISTINCT ip.part_num) FROM lego_inventory_parts ip \
         JOIN lego_inventories i ON i.id = ip.inventory_id \
         WHERE i.set_num IN (SELECT set_num FROM up WHERE theme_id = $1)"
    )))
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

    // 7. A builder's purchase history lists the builder's purchases in the order they were placed,
    //    numbered from 1, and each running total is the pieces of that purchase and every one
    //    before it, ending on the builder's pieces counted straight from the tables.
    let history = compose(sqlc_dir, "builders/purchase_history.sqlc");
    let rows = bound(&history, &[int("builder_id", DEFAULT_BUILDER)])
        .fetch_all(pool)
        .await
        .expect("query failed");
    let nths: Vec<i64> = rows.iter().map(|r| r.get("nth")).collect();
    let listed: Vec<i64> = rows.iter().map(|r| r.get("purchase_id")).collect();
    let totals: Vec<Option<i64>> = rows.iter().map(|r| r.get("pieces_so_far")).collect();
    let summed: Vec<Option<i64>> = rows
        .iter()
        .scan(None, |so_far: &mut Option<i64>, r| {
            if let Some(n) = r.get::<Option<i32>, _>("num_parts") {
                *so_far = Some(so_far.unwrap_or(0) + i64::from(n));
            }
            Some(*so_far)
        })
        .collect();
    let placed: Vec<i64> = sqlx::query_scalar(
        "SELECT purchase_id FROM lego_purchases WHERE builder_id = $1 \
         ORDER BY ordered_at, purchase_id",
    )
    .bind(DEFAULT_BUILDER)
    .fetch_all(pool)
    .await
    .expect("query failed");
    let pieces: Option<i64> = sqlx::query_scalar(
        "SELECT sum(s.num_parts) FROM lego_purchases p \
         JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no \
         JOIN lego_sets s ON s.set_num = c.set_num WHERE p.builder_id = $1",
    )
    .bind(DEFAULT_BUILDER)
    .fetch_one(pool)
    .await
    .expect("query failed");
    let last = totals.last().copied().flatten();
    all_hold &= verdict(
        &format!("builder {DEFAULT_BUILDER}'s history is their purchases in order, with running totals"),
        !placed.is_empty()
            && nths == (1..=placed.len() as i64).collect::<Vec<_>>()
            && listed == placed
            && totals == summed
            && last == pieces,
        &format!(
            "{} rows in the order of {} placed, last running total {} against {} pieces in the tables",
            rows.len(),
            placed.len(),
            shown(last),
            shown(pieces)
        ),
    );

    // 8. DISTINCT ON keeps, for each builder, the purchase that comes first when the builder's
    //    purchases are numbered latest first.
    let latest = compose(sqlc_dir, "builders/latest_purchases.sqlc");
    let kept: BTreeSet<(i32, i64)> = bound(&latest, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("builder_id"), r.get("purchase_id")))
        .collect();
    let numbered: BTreeSet<(i32, i64)> = sqlx::query_as(
        "SELECT builder_id, purchase_id FROM ( \
           SELECT builder_id, purchase_id, row_number() OVER (PARTITION BY builder_id \
             ORDER BY ordered_at DESC, purchase_id DESC) AS nth \
           FROM lego_purchases) p \
         WHERE nth = 1",
    )
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect();
    all_hold &= verdict(
        "each builder's latest purchase is the first of their purchases, latest first",
        !kept.is_empty() && kept == numbered,
        &format!("{} kept, {} numbered first", kept.len(), numbered.len()),
    );

    // 9. LATERAL with LIMIT 3 keeps, for each root theme, the sets that come first three when the
    //    sets walked up to that root are numbered newest first.
    let newest = compose(sqlc_dir, "themes/newest_sets.sqlc");
    let kept: BTreeSet<(i32, String)> = bound(&newest, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("root_id"), r.get("set_num")))
        .collect();
    let numbered: BTreeSet<(i32, String)> = sqlx::query_as(AssertSqlSafe(format!(
        "{WALK_UP} SELECT theme_id, set_num FROM ( \
           SELECT u.theme_id, u.set_num, row_number() OVER (PARTITION BY u.theme_id \
             ORDER BY s.year DESC NULLS LAST, s.set_num DESC) AS nth \
           FROM up u JOIN lego_themes r ON r.id = u.theme_id \
           JOIN lego_sets s ON s.set_num = u.set_num \
           WHERE r.parent_id IS NULL) n \
         WHERE nth <= 3"
    )))
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect();
    all_hold &= verdict(
        "each root theme's newest sets are its first three, newest first",
        !kept.is_empty() && kept == numbered,
        &format!("{} kept, {} numbered", kept.len(), numbered.len()),
    );

    // 10. The ROLLUP adds up: each year's subtotal is its themes' sum, and the total is the
    //     years' sum and every purchase of those years in the tables.
    let (from_year, to_year) = ROLLUP_YEARS;
    let rollup = compose(sqlc_dir, "reports/purchases_by_year_theme.sqlc");
    let rows = bound(
        &rollup,
        &[int("from_year", from_year), int("to_year", to_year)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");
    let mut themes: BTreeMap<i32, i64> = BTreeMap::new();
    let mut years: BTreeMap<i32, i64> = BTreeMap::new();
    let mut total = None;
    for row in &rows {
        let purchases: i64 = row.get("purchases");
        match row.get::<String, _>("level").as_str() {
            "theme" => *themes.entry(row.get("year")).or_default() += purchases,
            "year" => {
                years.insert(row.get("year"), purchases);
            }
            _ => total = Some(purchases),
        }
    }
    let direct: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lego_purchases \
         WHERE extract(year FROM ordered_at AT TIME ZONE 'UTC') BETWEEN $1 AND $2",
    )
    .bind(from_year)
    .bind(to_year)
    .fetch_one(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        &format!("purchases {from_year} to {to_year} add up by theme, by year and in total"),
        !years.is_empty()
            && themes == years
            && total == Some(years.values().sum())
            && total == Some(direct),
        &format!(
            "years {years:?}, their themes {themes:?}, total {} against {direct} in the tables",
            shown(total)
        ),
    );

    // 11. A root theme's median size is the middle of its sizes listed straight from the tables,
    //     over a walk up from each set.
    let sizes = compose(sqlc_dir, "reports/theme_set_sizes.sqlc");
    let row = bound(&sizes, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .into_iter()
        .find(|r| r.get::<i32, _>("root_id") == STAR_WARS_THEME_ID);
    let (median, sized) = row.map_or((None, 0), |r| {
        (r.get::<Option<f64>, _>("median"), r.get::<i64, _>("sized"))
    });
    let mut listed: Vec<i32> = sqlx::query_scalar(AssertSqlSafe(format!(
        "{WALK_UP} SELECT s.num_parts FROM up u JOIN lego_sets s ON s.set_num = u.set_num \
         WHERE u.theme_id = $1 AND s.num_parts IS NOT NULL"
    )))
    .bind(STAR_WARS_THEME_ID)
    .fetch_all(pool)
    .await
    .expect("query failed");
    listed.sort_unstable();
    let n = listed.len();
    let middle = match n {
        0 => None,
        _ if n % 2 == 1 => Some(f64::from(listed[n / 2])),
        _ => Some((f64::from(listed[n / 2 - 1]) + f64::from(listed[n / 2])) / 2.0),
    };
    all_hold &= verdict(
        "the median size of Star Wars is the middle of its sizes",
        middle.is_some() && median == middle && sized == n as i64,
        &format!(
            "median {} of {sized} sized sets, against {} of {n} listed",
            shown(median),
            shown(middle)
        ),
    );

    // 12. The release years run from the first to the last, once each; their counts add up to
    //     every set with a year, and a year shows 0 exactly when no set has it.
    let release_years = compose(sqlc_dir, "reports/release_years.sqlc");
    let rows: Vec<(i32, i64)> = bound(&release_years, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("year"), r.get("sets")))
        .collect();
    let (first, last, dated): (Option<i32>, Option<i32>, i64) =
        sqlx::query_as("SELECT min(year), max(year), count(year) FROM lego_sets")
            .fetch_one(pool)
            .await
            .expect("query failed");
    let present: BTreeSet<i32> =
        sqlx::query_scalar("SELECT DISTINCT year FROM lego_sets WHERE year IS NOT NULL")
            .fetch_all(pool)
            .await
            .expect("query failed")
            .into_iter()
            .collect();
    let calendar: Vec<i32> = match (first, last) {
        (Some(first), Some(last)) => (first..=last).collect(),
        _ => Vec::new(),
    };
    let listed: Vec<i32> = rows.iter().map(|(year, _)| *year).collect();
    let empty: BTreeSet<i32> = rows
        .iter()
        .filter(|(_, sets)| *sets == 0)
        .map(|(year, _)| *year)
        .collect();
    let absent: BTreeSet<i32> = calendar
        .iter()
        .filter(|year| !present.contains(year))
        .copied()
        .collect();
    let counted: i64 = rows.iter().map(|(_, sets)| sets).sum();
    all_hold &= verdict(
        "every year from first release to last is listed once, the empty ones with 0",
        !rows.is_empty() && listed == calendar && counted == dated && empty == absent,
        &format!(
            "{} years listed against {}; {counted} sets against {dated}; empty {empty:?} against {absent:?}",
            listed.len(),
            calendar.len()
        ),
    );

    // 13. The sets never bought and the sets bought are every set, and no set is both.
    let never_bought = compose(sqlc_dir, "sets/never_bought.sqlc");
    let never: BTreeSet<String> = bound(&never_bought, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| r.get("set_num"))
        .collect();
    let bought: BTreeSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT c.set_num FROM lego_purchases p \
         JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no",
    )
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect();
    let every: BTreeSet<String> = sqlx::query_scalar("SELECT set_num FROM lego_sets")
        .fetch_all(pool)
        .await
        .expect("query failed")
        .into_iter()
        .collect();
    let union: BTreeSet<String> = never.union(&bought).cloned().collect();
    all_hold &= verdict(
        "the sets never bought and the sets bought are every set, none in both",
        !never.is_empty() && never.is_disjoint(&bought) && union == every,
        &format!(
            "{} + {} = {} against {}",
            never.len(),
            bought.len(),
            union.len(),
            every.len()
        ),
    );

    // 14. The builders the division keeps are the builders whose collection holds as many of the
    //     scope's sets as the scope has.
    let complete = compose(sqlc_dir, "builders/complete_scope.sqlc");
    let kept: BTreeSet<i32> = bound(&complete, &[int("theme_id", SPYBIOTICS_THEME_ID)])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| r.get("builder_id"))
        .collect();
    let counted: BTreeSet<i32> = sqlx::query_scalar(AssertSqlSafe(format!(
        "{WALK_UP}, scope AS (SELECT set_num FROM up WHERE theme_id = $1) \
         SELECT c.builder_id FROM lego_collection c JOIN scope ON scope.set_num = c.set_num \
         GROUP BY c.builder_id HAVING count(DISTINCT c.set_num) = (SELECT count(*) FROM scope)"
    )))
    .bind(SPYBIOTICS_THEME_ID)
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect();
    all_hold &= verdict(
        "the builders owning all of Spybiotics are those holding as many of its sets as it has",
        !kept.is_empty() && kept == counted,
        &format!("{kept:?} against {counted:?}"),
    );

    // 15. A set's colour arrays hold, version by version, as many colours as its lines name.
    let colors = compose(sqlc_dir, "sets/set_colors.sqlc");
    let arrays: Vec<(i32, i64)> = bound(&colors, &[text("set_num", VERSIONED_SET)])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| {
            let colors = r
                .get::<Option<Vec<String>>, _>("colors")
                .unwrap_or_default();
            (r.get("version"), colors.len() as i64)
        })
        .collect();
    let named: Vec<(i32, i64)> = sqlx::query_as(
        "SELECT i.version, count(DISTINCT c.name) FROM lego_inventory_parts ip \
         JOIN lego_inventories i ON i.id = ip.inventory_id \
         JOIN lego_colors c ON c.id = ip.color_id \
         WHERE i.set_num = $1 GROUP BY i.version ORDER BY i.version",
    )
    .bind(VERSIONED_SET)
    .fetch_all(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        &format!("the colours of {VERSIONED_SET} are the colours its lines name, per version"),
        !arrays.is_empty() && arrays == named,
        &format!("(version, colours) {arrays:?} against {named:?}"),
    );

    // 16. A set's document holds every line of the set once.
    let document = compose(sqlc_dir, "sets/set_document.sqlc");
    let counted = read_through(
        &document,
        "coalesce(sum(jsonb_array_length(v.version -> 'lines')), 0)::bigint AS lines",
        "CROSS JOIN LATERAL jsonb_array_elements(d.document -> 'versions') AS v(version)",
    );
    let in_document: i64 = bound(&counted, &[text("set_num", DEFAULT_SET)])
        .fetch_one(pool)
        .await
        .expect("query failed")
        .get("lines");
    let in_tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lego_inventory_parts ip \
         JOIN lego_inventories i ON i.id = ip.inventory_id WHERE i.set_num = $1",
    )
    .bind(DEFAULT_SET)
    .fetch_one(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        &format!("the document of {DEFAULT_SET} holds each of its lines once"),
        in_tables > 0 && in_document == in_tables,
        &format!("{in_document} lines in the document, {in_tables} in the tables"),
    );

    // 17. MERGE repairs a summary to what INSERT … ON CONFLICT writes: after INSERT, every total
    //     is put wrong and one row removed, and MERGE, run twice, brings back the same rows. Run
    //     in a transaction that is rolled back, so checking leaves nothing behind.
    let merge = compose(sqlc_dir, "reports/merge_set_summary.sqlc");
    let law = format!("MERGE of {VERSIONED_SET} repairs its summary to what INSERT writes");
    let read = "SELECT version, category_name, total_parts, total_spare \
                FROM set_category_summary WHERE set_num = $1 ORDER BY version, category_name";
    let values = [text("set_num", VERSIONED_SET)];
    let mut tx = pool.begin().await.expect("begin failed");
    let outcome: Result<(Summary, Summary), sqlx::Error> = async {
        sqlx::query("DELETE FROM set_category_summary WHERE set_num = $1")
            .bind(VERSIONED_SET)
            .execute(&mut *tx)
            .await?;
        bound(&summary, &values).execute(&mut *tx).await?;
        let inserted: Summary = sqlx::query_as(read)
            .bind(VERSIONED_SET)
            .fetch_all(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE set_category_summary SET total_parts = total_parts + 1, \
             total_spare = total_spare + 1 WHERE set_num = $1",
        )
        .bind(VERSIONED_SET)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM set_category_summary WHERE id = \
             (SELECT min(id) FROM set_category_summary WHERE set_num = $1)",
        )
        .bind(VERSIONED_SET)
        .execute(&mut *tx)
        .await?;
        for _ in 0..2 {
            bound(&merge, &values).execute(&mut *tx).await?;
        }
        let merged: Summary = sqlx::query_as(read)
            .bind(VERSIONED_SET)
            .fetch_all(&mut *tx)
            .await?;
        Ok((inserted, merged))
    }
    .await;
    tx.rollback().await.expect("rollback failed");
    all_hold &= match outcome {
        Ok((inserted, merged)) => verdict(
            &law,
            !inserted.is_empty() && merged == inserted,
            &format!(
                "{} rows inserted, {} after the merge",
                inserted.len(),
                merged.len()
            ),
        ),
        Err(e) => verdict(&law, false, &format!("a statement failed: {e}")),
    };

    // 18. The union of Technic's and City's sets is each scope's sets from the year on, labelled
    //     with the scope's own name, walked down the themes table from the two scope themes: a
    //     set in both scopes comes back once under each label, and no row twice.
    let combined = compose(sqlc_dir, "reports/combined_theme_sets.sqlc");
    let from_year = 2000;
    let rows = bound(
        &combined,
        &[
            int("city_theme_id", CITY_THEME_ID),
            int("min_year", from_year),
            int("technic_theme_id", TECHNIC_THEME_ID),
        ],
    )
    .fetch_all(pool)
    .await
    .expect("query failed");
    let composed: BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| (r.get("theme_group"), r.get("set_num")))
        .collect();
    let walked: BTreeSet<(String, String)> = sqlx::query(
        "WITH RECURSIVE down (scope_id, theme_id) AS ( \
             SELECT id, id FROM lego_themes WHERE id IN ($1, $2) \
           UNION ALL \
             SELECT d.scope_id, t.id FROM lego_themes t JOIN down d ON t.parent_id = d.theme_id \
         ) \
         SELECT scope.name, s.set_num FROM down d \
         JOIN lego_sets s ON s.theme_id = d.theme_id \
         JOIN lego_themes scope ON scope.id = d.scope_id \
         WHERE s.year >= $3",
    )
    .bind(TECHNIC_THEME_ID)
    .bind(CITY_THEME_ID)
    .bind(from_year)
    .fetch_all(pool)
    .await
    .expect("query failed")
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect();
    all_hold &= verdict(
        &format!("Technic's and City's sets from {from_year} are each scope's, labelled by it"),
        !composed.is_empty() && rows.len() == composed.len() && composed == walked,
        &format!(
            "{} rows, {} distinct, against {} walked",
            rows.len(),
            composed.len(),
            walked.len()
        ),
    );

    // 19. The word filter keeps exactly the set's lines whose part names hold every word of a
    //     plain search, stem for stem: the search's words all among the name's.
    let by_words = compose(sqlc_dir, "sets/select_parts_by_words.sqlc");
    for words in [DEFAULT_WORDS, "door frame"] {
        let (keys, rows) = keys_of(
            pool,
            &by_words,
            &[text("set_num", DEFAULT_SET), text("words", words)],
        )
        .await;
        let truth = lines_where(
            pool,
            DEFAULT_SET,
            "ip.part_num IN (SELECT p.part_num FROM lego_parts p \
             WHERE tsvector_to_array(to_tsvector('english', p.name)) \
             @> tsvector_to_array(to_tsvector('english', $2)))",
            words,
        )
        .await;
        all_hold &= verdict(
            &format!("by-words keeps the set's lines whose part names hold '{words}'"),
            !keys.is_empty() && rows == keys.len() && keys == truth,
            &format!(
                "{rows} rows, {} distinct lines, {} in the tables",
                keys.len(),
                truth.len()
            ),
        );
    }

    // 20. The pattern filter keeps exactly the set's lines whose part names match the regular
    //     expression, case aside.
    let by_pattern = compose(sqlc_dir, "sets/select_parts_by_pattern.sqlc");
    let (keys, rows) = keys_of(
        pool,
        &by_pattern,
        &[
            text("pattern", DEFAULT_PATTERN),
            text("set_num", DEFAULT_SET),
        ],
    )
    .await;
    let truth = lines_where(
        pool,
        DEFAULT_SET,
        "ip.part_num IN (SELECT p.part_num FROM lego_parts p WHERE regexp_like(p.name, $2, 'i'))",
        DEFAULT_PATTERN,
    )
    .await;
    all_hold &= verdict(
        &format!("by-pattern keeps the set's lines whose part names match /{DEFAULT_PATTERN}/"),
        !keys.is_empty() && rows == keys.len() && keys == truth,
        &format!(
            "{rows} rows, {} distinct lines, {} in the tables",
            keys.len(),
            truth.len()
        ),
    );

    // 21. The ranked search lists every part whose name holds the search's words, each once, best
    //     match first and, between equal matches, by part_num.
    let search = compose(sqlc_dir, "parts/search_parts.sqlc");
    let listed: Vec<String> = bound(&search, &[int("top", 100_000), text("words", SEARCH_WORDS)])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| r.get("part_num"))
        .collect();
    let truth: Vec<String> = sqlx::query_scalar(
        "SELECT part_num FROM lego_parts \
         WHERE tsvector_to_array(to_tsvector('english', name)) \
               @> tsvector_to_array(to_tsvector('english', $1)) \
         ORDER BY ts_rank(to_tsvector('english', name), plainto_tsquery('english', $1)) DESC, \
                  part_num",
    )
    .bind(SEARCH_WORDS)
    .fetch_all(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        &format!("the search for '{SEARCH_WORDS}' lists every part holding its words, best first"),
        !listed.is_empty() && listed == truth,
        &format!(
            "{} listed against {} in the tables",
            listed.len(),
            truth.len()
        ),
    );

    // 22. The brick sizes are the bricks named "Brick <width> x <length>", read the long way: the
    //     name cut at " x " into two parts, each all digits.
    let sizes = compose(sqlc_dir, "parts/brick_sizes.sqlc");
    let composed: Vec<(String, Option<i32>, Option<i32>)> = bound(&sizes, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("part_num"), r.get("width"), r.get("length")))
        .collect();
    let truth: Vec<(String, Option<i32>, Option<i32>)> = sqlx::query_as(
        "SELECT part_num, split_part(rest, ' x ', 1)::integer, split_part(rest, ' x ', 2)::integer \
         FROM (SELECT part_num, substr(name, 7) AS rest FROM lego_parts \
               WHERE left(name, 6) = 'Brick ') b \
         WHERE split_part(rest, ' x ', 3) = '' \
           AND split_part(rest, ' x ', 1) <> '' \
           AND translate(split_part(rest, ' x ', 1), '0123456789', '') = '' \
           AND split_part(rest, ' x ', 2) <> '' \
           AND translate(split_part(rest, ' x ', 2), '0123456789', '') = '' \
         ORDER BY 2, 3, 1",
    )
    .fetch_all(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        "the brick sizes are the bricks named by their width and length",
        !composed.is_empty() && composed == truth,
        &format!(
            "{} read by the pattern, {} cut apart",
            composed.len(),
            truth.len()
        ),
    );

    // 23. The builders near a city are the builders whose homes lie within the distance by the
    //     haversine formula, on a sphere the earth's size: the cube only lets the index pass over
    //     the homes far away.
    let near = compose(sqlc_dir, "areas/near_city.sqlc");
    let near_values = [int("city_id", DEFAULT_CITY_ID), int("km", DEFAULT_KM)];
    let composed: BTreeMap<i32, f64> = bound(&near, &near_values)
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("builder_id"), r.get("metres")))
        .collect();
    let haversine = "2 * earth() * asin(sqrt( \
                       sin(radians(b.latitude - c.latitude) / 2) ^ 2 \
                       + cos(radians(c.latitude)) * cos(radians(b.latitude)) \
                         * sin(radians(b.longitude - c.longitude) / 2) ^ 2))";
    let truth: BTreeMap<i32, f64> = sqlx::query(AssertSqlSafe(format!(
        "SELECT b.builder_id, {haversine} FROM lego_cities c, lego_builders b \
         WHERE c.city_id = $1 AND {haversine} <= $2 * 1000"
    )))
    .bind(DEFAULT_CITY_ID)
    .bind(DEFAULT_KM)
    .fetch_all(pool)
    .await
    .expect("query failed")
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect();
    let same_metres = composed
        .iter()
        .zip(&truth)
        .all(|((a, ma), (b, mb))| a == b && (ma - mb).abs() < 0.01);
    all_hold &= verdict(
        &format!(
            "the builders within {DEFAULT_KM} km of city {DEFAULT_CITY_ID} are the haversine's"
        ),
        !composed.is_empty() && composed.len() == truth.len() && same_metres,
        &format!(
            "{} by the index, {} by the formula",
            composed.len(),
            truth.len()
        ),
    );

    // 24. The builders whose postcode matches an anchored pattern are the builders whose postcode
    //     begins with its prefix, read with LIKE.
    let postcodes = compose(sqlc_dir, "areas/postcodes.sqlc");
    let rows = bound(&postcodes, &[text("pattern", DEFAULT_POSTCODES)])
        .fetch_all(pool)
        .await
        .expect("query failed");
    let composed: BTreeSet<i32> = rows.iter().map(|r| r.get("builder_id")).collect();
    let truth: BTreeSet<i32> = sqlx::query_scalar(
        "SELECT b.builder_id FROM lego_builders b \
         JOIN lego_streets st ON st.street_id = b.street_id \
         JOIN lego_postcodes pc ON pc.postcode_id = st.postcode_id \
         WHERE pc.code LIKE $1",
    )
    .bind(format!("{}%", DEFAULT_POSTCODES.trim_start_matches('^')))
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .collect();
    all_hold &= verdict(
        &format!("the builders in /{DEFAULT_POSTCODES}/ are those whose postcode begins with it"),
        !composed.is_empty() && rows.len() == composed.len() && composed == truth,
        &format!(
            "{} rows, {} builders, {} by LIKE",
            rows.len(),
            composed.len(),
            truth.len()
        ),
    );

    // 25. The builders nearest a city are as near as the nearest found by measuring every home:
    //     the same distances, nearest first, though builders who share a home may trade places.
    let nearest = compose(sqlc_dir, "builders/nearest.sqlc");
    let top = 5;
    let millimetres = |m: f64| (m * 1000.0).round() as i64;
    let composed: Vec<i64> = bound(
        &nearest,
        &[int("city_id", DEFAULT_CITY_ID), int("top", top)],
    )
    .fetch_all(pool)
    .await
    .expect("query failed")
    .iter()
    .map(|r| millimetres(r.get("metres")))
    .collect();
    let truth: Vec<i64> = sqlx::query_scalar::<_, f64>(
        "SELECT earth_distance(ll_to_earth(c.latitude, c.longitude), \
                               ll_to_earth(b.latitude, b.longitude)) AS metres \
         FROM lego_cities c, lego_builders b WHERE c.city_id = $1 ORDER BY metres LIMIT $2",
    )
    .bind(DEFAULT_CITY_ID)
    .bind(i64::from(top))
    .fetch_all(pool)
    .await
    .expect("query failed")
    .into_iter()
    .map(millimetres)
    .collect();
    all_hold &= verdict(
        &format!("the {top} builders nearest city {DEFAULT_CITY_ID} are as near as any"),
        composed.len() == top as usize && composed == truth,
        &format!("{composed:?} mm against {truth:?}"),
    );

    // 26. What the builders near a city bought adds up: each root theme once, and the purchases
    //     together are all the purchases those builders made of sets with a theme.
    let bought = compose(sqlc_dir, "builders/bought_near.sqlc");
    let rows = bound(&bought, &near_values)
        .fetch_all(pool)
        .await
        .expect("query failed");
    let roots: BTreeSet<i32> = rows.iter().map(|r| r.get("root_id")).collect();
    let purchases: i64 = rows.iter().map(|r| r.get::<i64, _>("purchases")).sum();
    let truth: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM lego_purchases p \
         JOIN lego_collection col ON col.builder_id = p.builder_id AND col.row_no = p.row_no \
         JOIN lego_sets s ON s.set_num = col.set_num \
         JOIN lego_builders b ON b.builder_id = p.builder_id \
         JOIN lego_cities c ON c.city_id = $1 \
         WHERE s.theme_id IN (SELECT id FROM lego_themes) AND {haversine} <= $2 * 1000"
    )))
    .bind(DEFAULT_CITY_ID)
    .bind(DEFAULT_KM)
    .fetch_one(pool)
    .await
    .expect("query failed");
    all_hold &= verdict(
        &format!("what the builders near city {DEFAULT_CITY_ID} bought adds up to their purchases"),
        purchases > 0 && roots.len() == rows.len() && purchases == truth,
        &format!(
            "{purchases} purchases over {} root themes, {truth} in the tables",
            rows.len()
        ),
    );

    // 27. On rows of the example's own, the division keeps a builder who owns every set of a
    //     scope, drops one missing a set, and keeps nobody for a scope with no sets, which the
    //     catalogue never has.
    let themes = MockTable::new("lego_themes")
        .column("id", "integer")
        .column("name", "varchar")
        .column("parent_id", "integer")
        .row(&[Some("1"), Some("Castle"), None])
        .row(&[Some("2"), Some("Knights"), Some("1")])
        .row(&[Some("3"), Some("Unreleased"), None]);
    let sets = MockTable::new("lego_sets")
        .column("set_num", "varchar")
        .column("theme_id", "integer")
        .row(&[Some("100-1"), Some("1")])
        .row(&[Some("200-1"), Some("2")]);
    let builders = MockTable::new("lego_builders")
        .column("builder_id", "integer")
        .column("name", "varchar")
        .row(&[Some("10"), Some("Owns both")])
        .row(&[Some("11"), Some("Owns one")])
        .row(&[Some("12"), Some("Owns none")]);
    let collection = MockTable::new("lego_collection")
        .column("builder_id", "integer")
        .column("row_no", "integer")
        .column("set_num", "varchar")
        .row(&[Some("10"), Some("1"), Some("100-1")])
        .row(&[Some("10"), Some("2"), Some("200-1")])
        .row(&[Some("11"), Some("1"), Some("100-1")]);
    let division = compose_mocked(
        sqlc_dir,
        "builders/complete_scope.sqlc",
        vec![themes, sets, builders, collection],
    );
    let mut kept = Vec::new();
    for theme_id in [1, 3] {
        let ids: Vec<i32> = bound(&division, &[int("theme_id", theme_id)])
            .fetch_all(pool)
            .await
            .expect("query failed")
            .iter()
            .map(|r| r.get("builder_id"))
            .collect();
        kept.push(ids);
    }
    all_hold &= verdict(
        "on mocked rows, the division keeps the owner of every set, and nobody for an empty scope",
        kept[0] == vec![10] && kept[1].is_empty(),
        &format!(
            "Castle keeps {:?}, the empty scope keeps {:?}",
            kept[0], kept[1]
        ),
    );

    // 28. On rows of the example's own, a year with no release between two that have one is
    //     listed with 0.
    let releases = MockTable::new("lego_sets")
        .column("set_num", "varchar")
        .column("year", "integer")
        .row(&[Some("100-1"), Some("1990")])
        .row(&[Some("200-1"), Some("1992")])
        .row(&[Some("300-1"), Some("1992")]);
    let years = compose_mocked(sqlc_dir, "reports/release_years.sqlc", vec![releases]);
    let listed: Vec<(i32, i64)> = bound(&years, &[])
        .fetch_all(pool)
        .await
        .expect("query failed")
        .iter()
        .map(|r| (r.get("year"), r.get("sets")))
        .collect();
    all_hold &= verdict(
        "on mocked rows, 1991 between releases in 1990 and 1992 is listed with 0",
        listed == vec![(1990, 1), (1991, 0), (1992, 2)],
        &format!("{listed:?}"),
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
    cmd_history(pool, sqlc_dir, DEFAULT_BUILDER).await;
    cmd_latest(pool, sqlc_dir).await;
    cmd_newest(pool, sqlc_dir).await;
    cmd_rollup(pool, sqlc_dir, ROLLUP_YEARS.0, ROLLUP_YEARS.1).await;
    cmd_sizes(pool, sqlc_dir).await;
    cmd_years(pool, sqlc_dir).await;
    cmd_never_bought(pool, sqlc_dir).await;
    cmd_complete(pool, sqlc_dir, SPYBIOTICS_THEME_ID).await;
    cmd_colors(pool, sqlc_dir, VERSIONED_SET).await;
    cmd_document(pool, sqlc_dir, DEFAULT_SET).await;
    cmd_merge(pool, sqlc_dir, VERSIONED_SET).await;
    cmd_by_words(pool, sqlc_dir, DEFAULT_SET, DEFAULT_WORDS).await;
    cmd_by_pattern(pool, sqlc_dir, DEFAULT_SET, DEFAULT_PATTERN).await;
    cmd_search(pool, sqlc_dir, SEARCH_WORDS, 10).await;
    cmd_brick_sizes(pool, sqlc_dir).await;
    cmd_bought_near(pool, sqlc_dir, DEFAULT_CITY_ID, DEFAULT_KM).await;
    cmd_bought_in(pool, sqlc_dir, DEFAULT_POSTCODES).await;
    cmd_nearest(pool, sqlc_dir, DEFAULT_CITY_ID, 5).await;

    println!("\n=== All examples complete ===");
    cmd_laws(pool, sqlc_dir).await
}
