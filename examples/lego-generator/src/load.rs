//! Writing to Postgres: the schema, one transaction per wave written parent first, the index and
//! statistics builds, and the counters the summary prints.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool, Postgres, Transaction};
use tracing::{debug, error, info, warn};

use crate::catalogue::{Catalogue, Theme};
use crate::encode::{Cell, Enc, Format};
use crate::places::{self, Places};
use crate::world::{BuilderWave, ManifestOut, SetWave};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Copy,
    Unnest,
}

impl Method {
    pub fn parse(s: &str) -> Result<Method, String> {
        match s {
            "copy" => Ok(Method::Copy),
            "unnest" => Ok(Method::Unnest),
            other => Err(format!("unknown method `{other}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexTiming {
    Before,
    After,
}

/// Which of the generator's indexes are built: the primary and unique keys, the composite indexes
/// (`COMPOSITE_INDEXES`), and the search indexes (`SEARCHES`) with the extensions they need.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Indexes {
    pub keys: bool,
    pub composite: bool,
    pub search: bool,
}

impl Indexes {
    #[cfg(test)]
    pub const ALL: Indexes = Indexes {
        keys: true,
        composite: true,
        search: true,
    };

    /// `none`, or a comma-separated list of `keys`, `composite` and `search`.
    pub fn parse(s: &str) -> Result<Indexes, String> {
        let mut out = Indexes {
            keys: false,
            composite: false,
            search: false,
        };
        if s.trim() == "none" {
            return Ok(out);
        }
        for item in s.split(',') {
            match item.trim() {
                "keys" => out.keys = true,
                "composite" => out.composite = true,
                "search" => out.search = true,
                other => {
                    return Err(format!(
                        "--indexes: want none, or a list of keys, composite and search, \
                         not `{other}`"
                    ))
                }
            }
        }
        Ok(out)
    }

    /// The list as `--indexes` takes it.
    pub fn name(&self) -> String {
        let names: Vec<&str> = [
            ("keys", self.keys),
            ("composite", self.composite),
            ("search", self.search),
        ]
        .iter()
        .filter(|(_, on)| *on)
        .map(|(name, _)| *name)
        .collect();
        if names.is_empty() {
            "none".into()
        } else {
            names.join(",")
        }
    }
}

/// The session and table settings a load runs under.
#[derive(Clone, Debug)]
pub struct Settings {
    pub schema: String,
    pub format: Format,
    pub method: Method,
    pub index_timing: IndexTiming,
    pub indexes: Indexes,
    pub unlogged: bool,
    pub synchronous_commit: bool,
    pub wave_timeout: String,
    pub build_timeout: String,
    pub maintenance_work_mem: String,
    pub parallel_maintenance_workers: u32,
    pub freeze_reference_tables: bool,
    pub analyze: bool,
    pub vacuum: bool,
    pub autovacuum_during_load: bool,
}

/// A table's columns and key, as the dump declares them.
pub struct TableDef {
    pub name: &'static str,
    pub columns: &'static str,
    pub typed: &'static str,
    pub key: Option<&'static str>,
}

pub const TABLES: [TableDef; 16] = [
    TableDef { name: "lego_colors", columns: "id, name, rgb, is_trans", typed: "id integer NOT NULL, name varchar(255) NOT NULL, rgb varchar(6) NOT NULL, is_trans character(1) NOT NULL", key: Some("id") },
    TableDef { name: "lego_themes", columns: "id, name, parent_id", typed: "id integer NOT NULL, name varchar(255) NOT NULL, parent_id integer", key: Some("id") },
    TableDef { name: "lego_part_categories", columns: "id, name", typed: "id integer NOT NULL, name varchar(255) NOT NULL", key: Some("id") },
    TableDef { name: "lego_parts", columns: "part_num, name, part_cat_id", typed: "part_num varchar(255) NOT NULL, name text NOT NULL, part_cat_id integer NOT NULL", key: Some("part_num") },
    TableDef { name: "lego_sets", columns: "set_num, name, year, theme_id, num_parts", typed: "set_num varchar(255) NOT NULL, name varchar(255) NOT NULL, year integer, theme_id integer, num_parts integer", key: Some("set_num") },
    TableDef { name: "lego_inventories", columns: "id, version, set_num", typed: "id integer NOT NULL, version integer NOT NULL, set_num varchar(255) NOT NULL", key: Some("id") },
    TableDef { name: "lego_inventory_parts", columns: "inventory_id, part_num, color_id, quantity, is_spare", typed: "inventory_id integer NOT NULL, part_num varchar(255) NOT NULL, color_id integer NOT NULL, quantity integer NOT NULL, is_spare boolean NOT NULL", key: None },
    TableDef { name: "lego_inventory_sets", columns: "inventory_id, set_num, quantity", typed: "inventory_id integer NOT NULL, set_num varchar(255) NOT NULL, quantity integer NOT NULL", key: None },
    TableDef { name: "lego_cities", columns: "city_id, name, country, region, zone, latitude, longitude, population", typed: "city_id integer NOT NULL, name varchar(255) NOT NULL, country character(2) NOT NULL, region varchar(255) NOT NULL, zone varchar(64) NOT NULL, latitude double precision NOT NULL, longitude double precision NOT NULL, population integer NOT NULL", key: Some("city_id") },
    TableDef { name: "lego_postcodes", columns: "postcode_id, city_id, code", typed: "postcode_id integer NOT NULL, city_id integer NOT NULL, code varchar(16) NOT NULL", key: Some("postcode_id") },
    TableDef { name: "lego_streets", columns: "street_id, postcode_id, name, from_latitude, from_longitude, to_latitude, to_longitude", typed: "street_id integer NOT NULL, postcode_id integer NOT NULL, name varchar(255) NOT NULL, from_latitude double precision NOT NULL, from_longitude double precision NOT NULL, to_latitude double precision NOT NULL, to_longitude double precision NOT NULL", key: Some("street_id") },
    TableDef { name: "lego_builders", columns: "builder_id, name, street_id, house_number, latitude, longitude", typed: "builder_id integer NOT NULL, name varchar(255) NOT NULL, street_id integer NOT NULL, house_number integer NOT NULL, latitude double precision NOT NULL, longitude double precision NOT NULL", key: Some("builder_id") },
    TableDef { name: "lego_collection", columns: "builder_id, row_no, set_num, typed_set_num, typed_name", typed: "builder_id integer NOT NULL, row_no integer NOT NULL, set_num varchar(255) NOT NULL, typed_set_num varchar(255), typed_name varchar(255)", key: Some("builder_id, row_no") },
    TableDef { name: "lego_purchases", columns: "purchase_id, builder_id, row_no, store, ordered_at, ordered_local, delivered_at", typed: "purchase_id bigint NOT NULL, builder_id integer NOT NULL, row_no integer NOT NULL, store varchar(64) NOT NULL, ordered_at timestamptz NOT NULL, ordered_local varchar(32) NOT NULL, delivered_at timestamptz", key: Some("purchase_id") },
    TableDef { name: "trap_manifest", columns: "trap, origin, tbl, row_key, phase, wave, socket, detail", typed: "trap varchar(8) NOT NULL, origin varchar(16) NOT NULL, tbl varchar(64) NOT NULL, row_key text NOT NULL, phase varchar(32) NOT NULL, wave bigint NOT NULL, socket varchar(32), detail text NOT NULL", key: None },
    TableDef { name: "generator_run", columns: "key, value", typed: "key text NOT NULL, value text NOT NULL", key: Some("key") },
];

/// The zone the schema's `clock` reads an instant in.
pub const CLOCK_ZONE: &str = "UTC";

/// `<schema>.clock(timestamptz)`: an instant as the wall clock of `CLOCK_ZONE`. It is declared
/// `IMMUTABLE`, so an index can hold an expression over it, such as a purchase's month; a query
/// reads time through the same function to use that index.
pub fn clock_function(schema: &str) -> String {
    format!(
        "CREATE FUNCTION {schema}.clock(timestamptz) RETURNS timestamp \
         LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT $1 AT TIME ZONE '{CLOCK_ZONE}' $$"
    )
}

/// One column of a composite index's key: a column of the table, or an expression over it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyPart {
    /// A column of the table.
    Column(&'static str),
    /// An expression: the name it gives the index's name, and its SQL, in which `{clock}` stands
    /// for the schema's `clock` function.
    Expression {
        label: &'static str,
        sql: &'static str,
    },
}

impl KeyPart {
    fn label(&self) -> &'static str {
        match self {
            KeyPart::Column(c) => c,
            KeyPart::Expression { label, .. } => label,
        }
    }

    /// The part as an index key holds it, the clock read from `schema`.
    pub fn key_sql(&self, schema: &str) -> String {
        match self {
            KeyPart::Column(c) => c.to_string(),
            KeyPart::Expression { sql, .. } => {
                format!("({})", sql.replace("{clock}", &format!("{schema}.clock")))
            }
        }
    }
}

/// A composite index built after the load: its table, its key, and its `INCLUDE` columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompositeIndex {
    pub table: &'static str,
    pub parts: &'static [KeyPart],
    pub include: &'static [&'static str],
}

impl CompositeIndex {
    /// The key and the `INCLUDE` columns, as `CREATE INDEX` writes them after the table's name.
    pub fn definition(&self, clock_schema: &str) -> String {
        let key: Vec<String> = self.parts.iter().map(|p| p.key_sql(clock_schema)).collect();
        match self.include {
            [] => format!("({})", key.join(", ")),
            include => format!("({}) INCLUDE ({})", key.join(", "), include.join(", ")),
        }
    }

    /// The index built as `name` on table `on`, which may be another table of the same columns,
    /// the clock read from `clock_schema`.
    pub fn create_sql(&self, name: &str, on: &str, clock_schema: &str) -> String {
        format!(
            "CREATE INDEX {name} ON {on} {}",
            self.definition(clock_schema)
        )
    }
}

/// A purchase's month, on UTC's wall clock.
const MONTH: KeyPart = KeyPart::Expression {
    label: "month",
    sql: "extract(month FROM {clock}(ordered_at))::smallint",
};
/// A purchase's instant, on UTC's wall clock.
const ORDERED_AT: KeyPart = KeyPart::Expression {
    label: "ordered_at",
    sql: "{clock}(ordered_at)",
};

/// The composite indexes built after the load, by table: each led by the columns the join into its
/// table fixes, ending on the column the next join reads or holding it in `INCLUDE`, and holding in
/// `INCLUDE` the columns the queries read from the table, so that a query answered through the
/// index need not read the table. The purchases have two expression indexes, by month and then
/// instant: one within each collection row, and one across all of them.
pub const COMPOSITE_INDEXES: &[CompositeIndex] = &[
    CompositeIndex {
        table: "lego_themes",
        parts: &[KeyPart::Column("parent_id"), KeyPart::Column("id")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_sets",
        parts: &[KeyPart::Column("theme_id"), KeyPart::Column("set_num")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_sets",
        parts: &[KeyPart::Column("theme_id"), KeyPart::Column("year")],
        include: &["set_num"],
    },
    CompositeIndex {
        table: "lego_inventories",
        parts: &[
            KeyPart::Column("set_num"),
            KeyPart::Column("version"),
            KeyPart::Column("id"),
        ],
        include: &[],
    },
    CompositeIndex {
        table: "lego_inventory_sets",
        parts: &[KeyPart::Column("inventory_id"), KeyPart::Column("set_num")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_inventory_parts",
        parts: &[
            KeyPart::Column("inventory_id"),
            KeyPart::Column("part_num"),
            KeyPart::Column("color_id"),
        ],
        include: &[],
    },
    CompositeIndex {
        table: "lego_parts",
        parts: &[KeyPart::Column("part_cat_id"), KeyPart::Column("part_num")],
        include: &["name"],
    },
    CompositeIndex {
        table: "lego_collection",
        parts: &[
            KeyPart::Column("set_num"),
            KeyPart::Column("builder_id"),
            KeyPart::Column("row_no"),
        ],
        include: &[],
    },
    CompositeIndex {
        table: "lego_collection",
        parts: &[KeyPart::Column("builder_id"), KeyPart::Column("row_no")],
        include: &["set_num"],
    },
    CompositeIndex {
        table: "lego_purchases",
        parts: &[
            KeyPart::Column("builder_id"),
            KeyPart::Column("row_no"),
            MONTH,
            ORDERED_AT,
        ],
        include: &["ordered_at", "purchase_id"],
    },
    CompositeIndex {
        table: "lego_purchases",
        parts: &[MONTH, ORDERED_AT],
        include: &["builder_id", "row_no", "ordered_at", "purchase_id"],
    },
    CompositeIndex {
        table: "lego_cities",
        parts: &[KeyPart::Column("zone"), KeyPart::Column("city_id")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_postcodes",
        parts: &[KeyPart::Column("city_id"), KeyPart::Column("postcode_id")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_streets",
        parts: &[KeyPart::Column("postcode_id"), KeyPart::Column("street_id")],
        include: &[],
    },
    CompositeIndex {
        table: "lego_builders",
        parts: &[KeyPart::Column("street_id"), KeyPart::Column("builder_id")],
        include: &[],
    },
];

/// A unique key besides the primary key, added as a constraint after the load: its table, its name
/// and its columns. The inventory lines and nested sets have no primary key; their natural keys are
/// declared here.
pub const UNIQUE_KEYS: &[(&str, &str, &str)] = &[
    ("lego_postcodes", "lego_postcodes_code_key", "code"),
    (
        "lego_inventory_parts",
        "lego_inventory_parts_natural_key",
        "inventory_id, part_num, color_id, is_spare",
    ),
    (
        "lego_inventory_sets",
        "lego_inventory_sets_natural_key",
        "inventory_id, set_num",
    ),
];

/// An index a DBA adds beside the composite indexes, for a search they do not serve: a code by its
/// prefix, a home by its distance, a name by its words or by a pattern. `sql` is what follows
/// `ON <table>`, with `{<extension>}` standing for the schema an extension is in. `extensions` are
/// created in `public` before it is built, unless the database has them already, in any schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Search {
    pub name: &'static str,
    pub table: &'static str,
    pub sql: &'static str,
    pub extensions: &'static [&'static str],
}

/// The schema each extension is in, as an identifier SQL can name it by, by extension name.
pub type ExtensionSchemas = BTreeMap<String, String>;

impl Search {
    /// What follows `ON <table>`, each extension named by the schema `schemas` gives it, or by
    /// `public`.
    pub fn sql_in(&self, schemas: &ExtensionSchemas) -> String {
        let mut sql = self.sql.to_string();
        for e in self.extensions {
            let schema = schemas.get(*e).map_or("public", String::as_str);
            sql = sql.replace(&format!("{{{e}}}"), schema);
        }
        sql
    }
}

pub const SEARCHES: &[Search] = &[
    Search {
        name: "lego_postcodes_code_pattern_idx",
        table: "lego_postcodes",
        sql: "(code text_pattern_ops)",
        extensions: &[],
    },
    Search {
        name: "lego_builders_home_earth_idx",
        table: "lego_builders",
        sql: "USING gist ({earthdistance}.ll_to_earth(latitude, longitude))",
        extensions: &["cube", "earthdistance"],
    },
    Search {
        name: "lego_parts_name_words_idx",
        table: "lego_parts",
        sql: "USING gin (to_tsvector('english', name))",
        extensions: &[],
    },
    Search {
        name: "lego_parts_name_trgm_idx",
        table: "lego_parts",
        sql: "USING gin (name {pg_trgm}.gin_trgm_ops)",
        extensions: &["pg_trgm"],
    },
    Search {
        name: "lego_sets_name_trgm_idx",
        table: "lego_sets",
        sql: "USING gin (name {pg_trgm}.gin_trgm_ops)",
        extensions: &["pg_trgm"],
    },
];

/// The longest name the server keeps, in bytes.
pub const NAME_BYTES: usize = 63;

/// The name of a composite index on `table` over `parts`: the table and the parts' names, or, where
/// that is longer than `NAME_BYTES`, its front and a digest of the whole.
pub fn index_name(table: &str, parts: &[KeyPart]) -> String {
    let labels: Vec<&str> = parts.iter().map(KeyPart::label).collect();
    let full = format!("{table}_{}_idx", labels.join("_"));
    if full.len() <= NAME_BYTES {
        return full;
    }
    let digest = full.bytes().fold(0x811c_9dc5_u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    });
    let mut cut = NAME_BYTES - 9;
    while !full.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}_{digest:08x}", &full[..cut])
}

/// The extensions the search indexes need, in the order they are created: none unless the search
/// indexes are built.
pub fn extensions_for(indexes: Indexes) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    if indexes.search {
        for e in SEARCHES.iter().flat_map(|x| x.extensions.iter().copied()) {
            if !out.contains(&e) {
                out.push(e);
            }
        }
    }
    out
}

/// What `generator_run` records of the clock function and of the indexes built: each one's
/// definition by its name, a search index's naming each extension by the schema it is in.
pub fn roster_rows(
    schema: &str,
    indexes: Indexes,
    extension_schemas: &ExtensionSchemas,
) -> Vec<(String, String)> {
    let mut rows = vec![("function_clock".to_string(), clock_function(schema))];
    if indexes.composite {
        for s in COMPOSITE_INDEXES {
            rows.push((
                format!("composite_{}", index_name(s.table, s.parts)),
                format!("{} {}", s.table, s.definition(schema)),
            ));
        }
    }
    if indexes.keys {
        for (table, name, columns) in UNIQUE_KEYS {
            rows.push((format!("unique_{name}"), format!("{table} ({columns})")));
        }
    }
    if indexes.search {
        for s in SEARCHES {
            rows.push((
                format!("search_{}", s.name),
                format!("{} {}", s.table, s.sql_in(extension_schemas)),
            ));
        }
    }
    rows
}

pub fn table(name: &str) -> &'static TableDef {
    TABLES
        .iter()
        .find(|t| t.name == name)
        .expect("declared table")
}

/// Per-table counters, summed over every batch written.
#[derive(Clone, Debug, Default)]
pub struct LevelStat {
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
    pub busy: Duration,
    pub largest_batch: u64,
}

#[derive(Default)]
pub struct Metrics {
    pub levels: Mutex<BTreeMap<&'static str, LevelStat>>,
    pub by_socket: Mutex<HashMap<String, (u64, u64)>>,
    pub by_phase: Mutex<BTreeMap<String, (u64, u64)>>,
    pub by_trap: Mutex<BTreeMap<String, u64>>,
    /// Purchases per set (see `World::on_sale`), over the builder waves written.
    pub bought: Mutex<Vec<u32>>,
    pub waves_done: AtomicU64,
    pub failed: AtomicBool,
}

impl Metrics {
    pub fn absorb_bought(&self, bought: &[(u64, u32)]) {
        let mut m = self.bought.lock().expect("metrics lock");
        for &(set, n) in bought {
            let i = set as usize;
            if m.len() <= i {
                m.resize(i + 1, 0);
            }
            m[i] += n;
        }
    }

    pub fn add(&self, table: &'static str, rows: u64, bytes: u64, busy: Duration) -> LevelStat {
        let mut m = self.levels.lock().expect("metrics lock");
        let s = m.entry(table).or_default();
        s.rows += rows;
        s.bytes += bytes;
        s.batches += 1;
        s.busy += busy;
        s.largest_batch = s.largest_batch.max(rows);
        s.clone()
    }

    pub fn absorb_counts(
        &self,
        sockets: &HashMap<String, (u64, u64)>,
        phases: &HashMap<String, (u64, u64)>,
        manifest: &[ManifestOut],
    ) {
        {
            let mut m = self.by_socket.lock().expect("metrics lock");
            for (k, v) in sockets {
                let e = m.entry(k.clone()).or_default();
                e.0 += v.0;
                e.1 += v.1;
            }
        }
        {
            let mut m = self.by_phase.lock().expect("metrics lock");
            for (k, v) in phases {
                let e = m.entry(k.clone()).or_default();
                e.0 += v.0;
                e.1 += v.1;
            }
        }
        let mut t = self.by_trap.lock().expect("metrics lock");
        for r in manifest {
            *t.entry(r.trap.to_string()).or_default() += 1;
        }
    }
}

/// A failed wave, with everything needed to find it again.
#[derive(Debug)]
pub struct LoadError {
    pub wave: String,
    pub table: &'static str,
    pub batch_rows: u64,
    pub sqlstate: Option<String>,
    pub message: String,
    pub rolled_back: bool,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "wave {} table {} batch {} rows: {} (SQLSTATE {}); rolled back: {}",
            self.wave,
            self.table,
            self.batch_rows,
            self.message,
            self.sqlstate.as_deref().unwrap_or("-"),
            self.rolled_back
        )
    }
}

fn sqlstate(e: &sqlx::Error) -> (Option<String>, String) {
    match e {
        sqlx::Error::Database(d) => (d.code().map(|c| c.to_string()), d.message().to_string()),
        other => (None, other.to_string()),
    }
}

/// A pool whose every session names itself `lego-loader/<n>` and carries the load's settings.
pub async fn pool(url: &str, size: u32, s: &Settings) -> Result<PgPool, sqlx::Error> {
    let counter = Arc::new(AtomicU64::new(0));
    let sync = if s.synchronous_commit { "on" } else { "off" };
    let timeout = s.wave_timeout.clone();
    let opts: PgConnectOptions = url
        .parse::<PgConnectOptions>()?
        .log_statements(log::LevelFilter::Debug);
    PgPoolOptions::new()
        .max_connections(size)
        .acquire_timeout(Duration::from_secs(600))
        .after_connect(move |conn, _meta| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let sync = sync.to_string();
            let timeout = timeout.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET application_name = 'lego-loader/{n}'"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::query(&format!("SET synchronous_commit = {sync}"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::query(&format!("SET statement_timeout = '{timeout}'"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET client_min_messages = warning")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(opts)
        .await
}

/// One session for the builds after the load, with the maintenance settings.
pub async fn build_session(url: &str, s: &Settings) -> Result<PgConnection, sqlx::Error> {
    let opts: PgConnectOptions = url
        .parse::<PgConnectOptions>()?
        .log_statements(log::LevelFilter::Debug);
    let mut c = PgConnection::connect_with(&opts).await?;
    for q in [
        "SET application_name = 'lego-loader/build'".to_string(),
        format!("SET statement_timeout = '{}'", s.build_timeout),
        format!("SET maintenance_work_mem = '{}'", s.maintenance_work_mem),
        format!(
            "SET max_parallel_maintenance_workers = {}",
            s.parallel_maintenance_workers
        ),
        "SET client_min_messages = warning".to_string(),
    ] {
        sqlx::query(&q).execute(&mut c).await?;
    }
    Ok(c)
}

/// The server facts a slow load is diagnosed against.
pub async fn server_settings(pool: &PgPool) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for g in [
        "server_version",
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "max_wal_size",
        "wal_level",
        "synchronous_commit",
        "max_parallel_maintenance_workers",
        "max_worker_processes",
        "checkpoint_timeout",
        "autovacuum",
        "TimeZone",
    ] {
        let v: Result<(String, String, Option<String>), _> = sqlx::query_as(
            "SELECT current_setting($1), source::text, boot_val FROM pg_settings WHERE name = $1",
        )
        .bind(g)
        .fetch_one(pool)
        .await;
        out.push((
            g.to_string(),
            match v {
                Ok((value, source, _)) if source == "default" || source == "configuration file" => {
                    value
                }
                Ok((value, source, _)) => format!("{value} (set by {source})"),
                Err(e) => format!("({e})"),
            },
        ));
    }
    let db: Result<(String, String), _> =
        sqlx::query_as("SELECT datcollate::text, pg_encoding_to_char(encoding)::text FROM pg_database WHERE datname = current_database()")
            .fetch_one(pool)
            .await;
    if let Ok((coll, enc)) = db {
        out.push(("datcollate".into(), coll));
        out.push(("encoding".into(), enc));
    }
    out
}

/// A table as `CREATE TABLE` makes it, with its primary key when the keys are built before the
/// load.
fn create_table(s: &Settings, t: &TableDef) -> String {
    let with_key = s.indexes.keys && s.index_timing == IndexTiming::Before;
    let key = match (with_key, t.key) {
        (true, Some(k)) => format!(", PRIMARY KEY ({k})"),
        _ => String::new(),
    };
    format!(
        "CREATE {}TABLE {}.{} ({}{key}){}",
        if s.unlogged { "UNLOGGED " } else { "" },
        s.schema,
        t.name,
        t.typed,
        if s.autovacuum_during_load {
            ""
        } else {
            " WITH (autovacuum_enabled = false)"
        }
    )
}

/// Drops and recreates the schema. The reference tables are created later, in the transaction that loads them.
pub async fn create_schema(pool: &PgPool, s: &Settings) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", s.schema))
        .execute(pool)
        .await?;
    sqlx::query(&format!("CREATE SCHEMA {}", s.schema))
        .execute(pool)
        .await?;
    for t in TABLES
        .iter()
        .filter(|t| !REFERENCE_TABLES.contains(&t.name))
    {
        sqlx::query(&create_table(s, t)).execute(pool).await?;
    }
    Ok(())
}

/// Creates in `public` the extensions the search indexes need, when they are built and the
/// database does not have them already in some schema, and returns the schema each one is in. Run
/// before the load, so that a role that may not create one stops before anything is written.
pub async fn create_extensions(pool: &PgPool, s: &Settings) -> Result<ExtensionSchemas, String> {
    let wanted = extensions_for(s.indexes);
    for e in &wanted {
        sqlx::query(&format!(
            "CREATE EXTENSION IF NOT EXISTS {e} WITH SCHEMA public"
        ))
        .execute(pool)
        .await
        .map_err(|err| {
            format!(
                "creating extension {e}: {err}. The search indexes need cube, earthdistance and \
                 pg_trgm, in any schema; earthdistance can be created only by a superuser, so \
                 have one run `CREATE EXTENSION earthdistance CASCADE` in this database, or \
                 leave search out of --indexes"
            )
        })?;
    }
    let names: Vec<String> = wanted.iter().map(|e| e.to_string()).collect();
    let schemas: ExtensionSchemas = sqlx::query_as::<_, (String, String)>(
        "SELECT e.extname::text, quote_ident(n.nspname)::text \
         FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = ANY($1)",
    )
    .bind(&names)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("reading the extensions' schemas: {err}"))?
    .into_iter()
    .collect();
    for (e, schema) in &schemas {
        info!(extension = %e, schema = %schema, "extension ready");
    }
    Ok(schemas)
}

async fn copy_batch(
    tx: &mut Transaction<'static, Postgres>,
    s: &Settings,
    t: &'static TableDef,
    enc: &Enc,
    freeze: bool,
) -> Result<u64, sqlx::Error> {
    let mut opts = Vec::new();
    if s.format == Format::Binary {
        opts.push("FORMAT binary");
    }
    if freeze {
        opts.push("FREEZE");
    }
    let with = if opts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", opts.join(", "))
    };
    let stmt = format!(
        "COPY {}.{} ({}) FROM STDIN{with}",
        s.schema, t.name, t.columns
    );
    let mut copy = tx.copy_in_raw(&stmt).await?;
    copy.send(enc.buf.as_slice()).await?;
    copy.finish().await
}

/// The reference tables, in the order they are loaded: the real catalogue's, then the places.
pub const REFERENCE_TABLES: [&str; 7] = [
    "lego_colors",
    "lego_themes",
    "lego_part_categories",
    "lego_parts",
    "lego_cities",
    "lego_postcodes",
    "lego_streets",
];

/// The rows of reference table `name`: the real catalogue's, and after its themes the root themes
/// the generator adds (`World::added_themes`); or the places' cities, postcodes and streets.
pub fn encode_reference(
    name: &str,
    cat: &Catalogue,
    added_themes: &[Theme],
    places: &Places,
    enc: &mut Enc,
) {
    match name {
        "lego_colors" => cat.colours.iter().for_each(|c| {
            enc.reference_row(&[
                Cell::Int(c.id),
                Cell::Text(&c.name),
                Cell::Text(&c.rgb),
                Cell::Text(&c.is_trans),
            ])
        }),
        "lego_themes" => cat.themes.iter().chain(added_themes).for_each(|th| {
            enc.reference_row(&[
                Cell::Int(th.id),
                Cell::Text(&th.name),
                Cell::OptInt(th.parent_id),
            ])
        }),
        "lego_part_categories" => cat
            .categories
            .iter()
            .for_each(|c| enc.reference_row(&[Cell::Int(c.id), Cell::Text(&c.name)])),
        "lego_parts" => cat.parts.iter().for_each(|p| {
            enc.reference_row(&[
                Cell::Text(&p.part_num),
                Cell::Text(&p.name),
                Cell::Int(p.part_cat_id),
            ])
        }),
        "lego_cities" => places.cities.iter().for_each(|c| {
            enc.reference_row(&[
                Cell::Int(c.id),
                Cell::Text(c.name),
                Cell::Text(c.country),
                Cell::Text(c.region),
                Cell::Text(crate::calendar::ZONE_NAMES[c.zone]),
                Cell::Float(places::degrees(c.latitude)),
                Cell::Float(places::degrees(c.longitude)),
                Cell::Int(c.population),
            ])
        }),
        "lego_postcodes" => places.postcodes.iter().for_each(|p| {
            enc.reference_row(&[
                Cell::Int(p.id),
                Cell::Int(places.cities[p.city].id),
                Cell::Text(&p.code),
            ])
        }),
        "lego_streets" => places.streets.iter().for_each(|s| {
            enc.reference_row(&[
                Cell::Int(s.id),
                Cell::Int(places.postcodes[s.postcode].id),
                Cell::Text(&s.name),
                Cell::Float(places::degrees(s.from.0)),
                Cell::Float(places::degrees(s.from.1)),
                Cell::Float(places::degrees(s.to.0)),
                Cell::Float(places::degrees(s.to.1)),
            ])
        }),
        other => unreachable!("{other} is not a reference table"),
    }
}

/// The reference tables, copied through unchanged but for the added themes, each created and
/// loaded in one transaction.
pub async fn load_reference_tables(
    pool: &PgPool,
    s: &Settings,
    cat: &Catalogue,
    added_themes: &[Theme],
    places: &Places,
    metrics: &Metrics,
) -> Result<(), LoadError> {
    let err = |t: &'static str, rows: u64, e: sqlx::Error, rb: bool| {
        let (st, msg) = sqlstate(&e);
        LoadError {
            wave: "reference_tables".into(),
            table: t,
            batch_rows: rows,
            sqlstate: st,
            message: msg,
            rolled_back: rb,
        }
    };
    for name in REFERENCE_TABLES {
        let t = table(name);
        let mut enc = Enc::new(s.format);
        encode_reference(name, cat, added_themes, places, &mut enc);
        enc.finish();
        let started = Instant::now();
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| err(t.name, enc.rows, e, false))?;
        let res = async {
            sqlx::query(&create_table(s, t)).execute(&mut *tx).await?;
            copy_batch(&mut tx, s, t, &enc, s.freeze_reference_tables).await
        }
        .await;
        match res {
            Ok(n) => {
                tx.commit()
                    .await
                    .map_err(|e| err(t.name, enc.rows, e, false))?;
                let st = metrics.add(t.name, n, enc.buf.len() as u64, started.elapsed());
                debug!(
                    table = t.name,
                    rows = n,
                    bytes = enc.buf.len(),
                    total_rows = st.rows,
                    "reference table written"
                );
            }
            Err(e) => {
                let rb = tx.rollback().await.is_ok();
                return Err(err(t.name, enc.rows, e, rb));
            }
        }
    }
    Ok(())
}

/// The batches of one wave, in parent-first order.
pub struct Batches<'a> {
    pub label: String,
    pub levels: Vec<(&'static str, Level<'a>)>,
}

pub enum Level<'a> {
    Sets(&'a SetWave),
    Inventories(&'a SetWave),
    Lines(&'a SetWave, &'a [String]),
    Nests(&'a SetWave),
    Builders(&'a BuilderWave),
    Collection(&'a BuilderWave),
    Purchases(&'a BuilderWave),
    Manifest(&'a [ManifestOut]),
}

impl Level<'_> {
    pub fn rows(&self) -> u64 {
        (match self {
            Level::Sets(w) => w.sets.len(),
            Level::Inventories(w) => w.inventories.len(),
            Level::Lines(w, _) => w.lines.len(),
            Level::Nests(w) => w.nests.len(),
            Level::Builders(w) => w.builders.len(),
            Level::Collection(w) => w.collection.len(),
            Level::Purchases(w) => w.purchases.len(),
            Level::Manifest(m) => m.len(),
        }) as u64
    }

    pub fn encode(&self, enc: &mut Enc) {
        match self {
            Level::Sets(w) => w.sets.iter().for_each(|r| enc.set(r)),
            Level::Inventories(w) => w.inventories.iter().for_each(|r| enc.inventory(r)),
            Level::Lines(w, pool) => w.lines.iter().for_each(|r| enc.line(r, pool)),
            Level::Nests(w) => w.nests.iter().for_each(|r| enc.nest(r)),
            Level::Builders(w) => w.builders.iter().for_each(|r| enc.builder(r)),
            Level::Collection(w) => w.collection.iter().for_each(|r| enc.collection(r)),
            Level::Purchases(w) => w.purchases.iter().for_each(|r| enc.purchase(r)),
            Level::Manifest(m) => m.iter().for_each(|r| enc.manifest(r)),
        }
    }
}

pub fn set_wave_batches<'a>(label: String, w: &'a SetWave, pool: &'a [String]) -> Batches<'a> {
    Batches {
        label,
        levels: vec![
            ("lego_sets", Level::Sets(w)),
            ("lego_inventories", Level::Inventories(w)),
            ("lego_inventory_parts", Level::Lines(w, pool)),
            ("lego_inventory_sets", Level::Nests(w)),
            ("trap_manifest", Level::Manifest(&w.manifest)),
        ],
    }
}

pub fn builder_wave_batches(label: String, w: &BuilderWave) -> Batches<'_> {
    Batches {
        label,
        levels: vec![
            ("lego_builders", Level::Builders(w)),
            ("lego_collection", Level::Collection(w)),
            ("lego_purchases", Level::Purchases(w)),
            ("trap_manifest", Level::Manifest(&w.manifest)),
        ],
    }
}

/// Writes one wave in one transaction, parent level first. A failure rolls the whole wave back.
pub async fn write_wave(
    pool: &PgPool,
    s: &Settings,
    b: &Batches<'_>,
    enc: &mut Enc,
    metrics: &Metrics,
) -> Result<(), LoadError> {
    let mk = |t: &'static str, rows: u64, e: sqlx::Error, rb: bool| {
        let (st, msg) = sqlstate(&e);
        LoadError {
            wave: b.label.clone(),
            table: t,
            batch_rows: rows,
            sqlstate: st,
            message: msg,
            rolled_back: rb,
        }
    };
    let mut tx = pool.begin().await.map_err(|e| mk("-", 0, e, false))?;
    for (name, level) in &b.levels {
        let rows = level.rows();
        if rows == 0 {
            continue;
        }
        let t = table(name);
        let started = Instant::now();
        let res = match s.method {
            Method::Copy => {
                enc.reset();
                level.encode(enc);
                enc.finish();
                copy_batch(&mut tx, s, t, enc, false)
                    .await
                    .map(|n| (n, enc.buf.len() as u64))
            }
            Method::Unnest => crate::unnest::insert(&mut tx, &s.schema, level).await,
        };
        match res {
            Ok((n, bytes)) if n == rows => {
                let elapsed = started.elapsed();
                let st = metrics.add(t.name, n, bytes, elapsed);
                debug!(
                    wave = %b.label,
                    table = t.name,
                    rows = n,
                    bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    rows_per_s = (n as f64 / elapsed.as_secs_f64().max(1e-9)) as u64,
                    total_rows = st.rows,
                    total_bytes = st.bytes,
                    "level written"
                );
            }
            Ok((n, _)) => {
                let rb = tx.rollback().await.is_ok();
                let e = mk(
                    t.name,
                    rows,
                    sqlx::Error::Protocol(format!("the server took {n} rows of {rows}")),
                    rb,
                );
                error!(%e, "wave failed");
                return Err(e);
            }
            Err(e) => {
                let rb = tx.rollback().await.is_ok();
                let e = mk(t.name, rows, e, rb);
                error!(wave = %e.wave, table = e.table, batch_rows = e.batch_rows, sqlstate = e.sqlstate.as_deref().unwrap_or("-"), message = %e.message, rolled_back = e.rolled_back, "wave failed");
                return Err(e);
            }
        }
    }
    tx.commit().await.map_err(|e| mk("commit", 0, e, false))?;
    Ok(())
}

/// Logs the running COPY statements of the loader's sessions, every `every`, until stopped.
pub async fn watch_progress(
    pool: PgPool,
    every: Duration,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    total_waves: u64,
) {
    let started = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(every).await;
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let rows: Result<Vec<(String, String, i64, i64)>, _> = sqlx::query_as(
            "SELECT a.application_name::text, p.relid::regclass::text, p.tuples_processed, p.bytes_processed \
             FROM pg_stat_progress_copy p JOIN pg_stat_activity a USING (pid) \
             WHERE a.application_name LIKE 'lego-loader/%' ORDER BY 1",
        )
        .fetch_all(&pool)
        .await;
        let done = metrics.waves_done.load(Ordering::SeqCst);
        let lines = metrics
            .levels
            .lock()
            .map(|m| m.get("lego_inventory_parts").map_or(0, |s| s.rows))
            .unwrap_or(0);
        let secs = started.elapsed().as_secs_f64();
        info!(
            waves_done = done,
            waves = total_waves,
            lines,
            lines_per_s = (lines as f64 / secs.max(1e-9)) as u64,
            elapsed_s = secs as u64,
            "progress"
        );
        match rows {
            Ok(rows) => {
                for (app, rel, tuples, bytes) in rows {
                    info!(session = %app, relation = %rel, tuples, bytes, "copy running");
                }
            }
            Err(e) => warn!(error = %e, "pg_stat_progress_copy unreadable"),
        }
    }
}

/// After the load: the primary keys (when built after), autovacuum back on, the clock function and
/// then the composite indexes, the unique keys and the search indexes, each as `--indexes` asks,
/// statistics, the visibility map. A search index names each extension by the schema
/// `extension_schemas` gives it.
pub async fn finish(
    conn: &mut PgConnection,
    s: &Settings,
    extension_schemas: &ExtensionSchemas,
) -> Result<Vec<(String, Duration)>, sqlx::Error> {
    let mut steps = Vec::new();
    let composite: &[CompositeIndex] = if s.indexes.composite {
        COMPOSITE_INDEXES
    } else {
        &[]
    };
    let unique: &[(&str, &str, &str)] = if s.indexes.keys { UNIQUE_KEYS } else { &[] };
    let searches: &[Search] = if s.indexes.search { SEARCHES } else { &[] };
    for t in &TABLES {
        let name = t.name;
        if s.indexes.keys && s.index_timing == IndexTiming::After {
            if let Some(k) = t.key {
                let started = Instant::now();
                sqlx::query(&format!(
                    "ALTER TABLE {}.{name} ADD PRIMARY KEY ({k})",
                    s.schema
                ))
                .execute(&mut *conn)
                .await?;
                steps.push((format!("primary key {name}"), started.elapsed()));
                info!(
                    table = name,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "primary key built"
                );
            }
        }
        sqlx::query(&format!(
            "ALTER TABLE {}.{name} RESET (autovacuum_enabled)",
            s.schema
        ))
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query(&clock_function(&s.schema))
        .execute(&mut *conn)
        .await?;
    for ix in composite {
        let started = Instant::now();
        let name = index_name(ix.table, ix.parts);
        let on = format!("{}.{}", s.schema, ix.table);
        sqlx::query(&ix.create_sql(&name, &on, &s.schema))
            .execute(&mut *conn)
            .await?;
        steps.push((format!("composite {name}"), started.elapsed()));
        info!(
            table = ix.table,
            index = %name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "composite index built"
        );
    }
    for (table, name, columns) in unique {
        let started = Instant::now();
        sqlx::query(&format!(
            "ALTER TABLE {}.{table} ADD CONSTRAINT {name} UNIQUE ({columns})",
            s.schema
        ))
        .execute(&mut *conn)
        .await?;
        steps.push((format!("unique {name}"), started.elapsed()));
        info!(
            table,
            index = name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "unique key built"
        );
    }
    for x in searches {
        let started = Instant::now();
        sqlx::query(&format!(
            "CREATE INDEX {} ON {}.{} {}",
            x.name,
            s.schema,
            x.table,
            x.sql_in(extension_schemas)
        ))
        .execute(&mut *conn)
        .await?;
        steps.push((format!("search {}", x.name), started.elapsed()));
        info!(
            table = x.table,
            index = x.name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "search index built"
        );
    }
    if s.vacuum {
        for t in &TABLES {
            let started = Instant::now();
            sqlx::query(&format!(
                "VACUUM (ANALYZE {}) {}.{}",
                s.analyze, s.schema, t.name
            ))
            .execute(&mut *conn)
            .await?;
            steps.push((format!("vacuum {}", t.name), started.elapsed()));
        }
    } else if s.analyze {
        for t in &TABLES {
            let started = Instant::now();
            sqlx::query(&format!("ANALYZE {}.{}", s.schema, t.name))
                .execute(&mut *conn)
                .await?;
            steps.push((format!("analyze {}", t.name), started.elapsed()));
        }
    }
    Ok(steps)
}

pub async fn wal_lsn(pool: &PgPool) -> Option<String> {
    sqlx::query_as::<_, (String,)>("SELECT pg_current_wal_lsn()::text")
        .fetch_one(pool)
        .await
        .ok()
        .map(|r| r.0)
}

pub async fn wal_bytes(pool: &PgPool, from: &str, to: &str) -> Option<i64> {
    sqlx::query_as::<_, (i64,)>("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint")
        .bind(to)
        .bind(from)
        .fetch_one(pool)
        .await
        .ok()
        .map(|r| r.0)
}

/// Each table of `schema` that is no other's partition or child, with the size on disk of it and of
/// every partition or child below it: a partitioned table has no files of its own, so
/// `pg_total_relation_size` counts nothing for it and nothing of its partitions.
pub async fn sizes(pool: &PgPool, schema: &str) -> Vec<(String, i64)> {
    let name = match schema.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => schema.to_ascii_lowercase(),
    };
    sqlx::query_as::<_, (String, i64)>(
        "WITH RECURSIVE tree (root, rel) AS ( \
           SELECT c.oid, c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
           WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') \
             AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid) \
           UNION \
           SELECT t.root, i.inhrelid FROM tree t JOIN pg_inherits i ON i.inhparent = t.rel) \
         SELECT r.relname::text, sum(pg_total_relation_size(t.rel))::bigint \
         FROM tree t JOIN pg_class r ON r.oid = t.root GROUP BY r.relname ORDER BY 1",
    )
    .bind(name)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

pub async fn write_run(
    pool: &PgPool,
    s: &Settings,
    pairs: &[(String, String)],
) -> Result<(), sqlx::Error> {
    let keys: Vec<String> = pairs.iter().map(|p| p.0.clone()).collect();
    let values: Vec<String> = pairs.iter().map(|p| p.1.clone()).collect();
    sqlx::query(&format!(
        "INSERT INTO {}.generator_run (key, value) SELECT * FROM UNNEST($1::text[], $2::text[])",
        s.schema
    ))
    .bind(keys)
    .bind(values)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(indexes: Indexes, index_timing: IndexTiming) -> Settings {
        Settings {
            schema: "lego".into(),
            format: Format::Binary,
            method: Method::Copy,
            index_timing,
            indexes,
            unlogged: false,
            synchronous_commit: false,
            wave_timeout: "300s".into(),
            build_timeout: "3600s".into(),
            maintenance_work_mem: "1GB".into(),
            parallel_maintenance_workers: 4,
            freeze_reference_tables: false,
            analyze: true,
            vacuum: true,
            autovacuum_during_load: false,
        }
    }

    #[test]
    fn indexes_are_none_or_a_list_and_any_other_word_is_refused() {
        assert_eq!(Indexes::parse("keys,composite,search"), Ok(Indexes::ALL));
        let two = Indexes {
            keys: true,
            composite: false,
            search: true,
        };
        assert_eq!(Indexes::parse(" search , keys "), Ok(two));
        let none = Indexes::parse("none").unwrap();
        assert!(!none.keys && !none.composite && !none.search);
        assert_eq!(none.name(), "none");
        assert_eq!(Indexes::parse("composite").unwrap().name(), "composite");
        assert_eq!(Indexes::ALL.name(), "keys,composite,search");
        for bad in ["all", "keys,btree", ""] {
            assert!(Indexes::parse(bad).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn the_extensions_are_created_only_for_the_search_indexes() {
        assert_eq!(
            extensions_for(Indexes::ALL),
            ["cube", "earthdistance", "pg_trgm"]
        );
        let no_search = Indexes {
            search: false,
            ..Indexes::ALL
        };
        assert!(extensions_for(no_search).is_empty());
    }

    #[test]
    fn a_search_index_names_each_extension_by_the_schema_it_is_in() {
        let sql = |name: &str, schemas: &ExtensionSchemas| {
            SEARCHES
                .iter()
                .find(|s| s.name == name)
                .unwrap()
                .sql_in(schemas)
        };
        let unknown = ExtensionSchemas::new();
        assert_eq!(
            sql("lego_builders_home_earth_idx", &unknown),
            "USING gist (public.ll_to_earth(latitude, longitude))"
        );
        assert_eq!(
            sql("lego_sets_name_trgm_idx", &unknown),
            "USING gin (name public.gin_trgm_ops)"
        );
        let elsewhere: ExtensionSchemas = [
            ("cube", "geo"),
            ("earthdistance", "geo"),
            ("pg_trgm", "\"Text Search\""),
        ]
        .into_iter()
        .map(|(e, s)| (e.to_string(), s.to_string()))
        .collect();
        assert_eq!(
            sql("lego_builders_home_earth_idx", &elsewhere),
            "USING gist (geo.ll_to_earth(latitude, longitude))"
        );
        assert_eq!(
            sql("lego_parts_name_trgm_idx", &elsewhere),
            "USING gin (name \"Text Search\".gin_trgm_ops)"
        );
        for s in SEARCHES {
            assert!(!s.sql_in(&elsewhere).contains('{'), "{}", s.name);
        }
    }

    #[test]
    fn generator_run_records_only_the_indexes_built() {
        let prefixes = |indexes: Indexes| -> Vec<String> {
            let mut p: Vec<String> = roster_rows("lego", indexes, &ExtensionSchemas::new())
                .into_iter()
                .map(|(k, _)| k.split('_').next().unwrap().to_string())
                .collect();
            p.dedup();
            p
        };
        assert_eq!(
            prefixes(Indexes::ALL),
            ["function", "composite", "unique", "search"]
        );
        assert_eq!(prefixes(Indexes::parse("none").unwrap()), ["function"]);
        assert_eq!(
            prefixes(Indexes::parse("keys").unwrap()),
            ["function", "unique"]
        );
    }

    #[test]
    fn a_table_is_created_with_its_primary_key_only_when_keys_are_built_before_the_load() {
        let t = table("lego_sets");
        let with_key =
            |indexes, timing| create_table(&settings(indexes, timing), t).contains("PRIMARY KEY");
        let no_keys = Indexes {
            keys: false,
            ..Indexes::ALL
        };
        assert!(with_key(Indexes::ALL, IndexTiming::Before));
        assert!(!with_key(Indexes::ALL, IndexTiming::After));
        assert!(!with_key(no_keys, IndexTiming::Before));
    }
}
