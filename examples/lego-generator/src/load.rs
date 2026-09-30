//! Writing to Postgres: the schema, one transaction per wave written parent first, the index and
//! statistics builds, and the counters the summary prints.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool, Postgres, Transaction};
use tracing::{debug, error, info, warn};

use crate::catalogue::Catalogue;
use crate::encode::{Cell, Enc, Format};
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

/// The session and table settings a load runs under.
#[derive(Clone, Debug)]
pub struct Settings {
    pub schema: String,
    pub format: Format,
    pub method: Method,
    pub index_timing: IndexTiming,
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

pub const TABLES: [TableDef; 13] = [
    TableDef { name: "lego_colors", columns: "id, name, rgb, is_trans", typed: "id integer NOT NULL, name varchar(255) NOT NULL, rgb varchar(6) NOT NULL, is_trans character(1) NOT NULL", key: Some("id") },
    TableDef { name: "lego_themes", columns: "id, name, parent_id", typed: "id integer NOT NULL, name varchar(255) NOT NULL, parent_id integer", key: Some("id") },
    TableDef { name: "lego_part_categories", columns: "id, name", typed: "id integer NOT NULL, name varchar(255) NOT NULL", key: Some("id") },
    TableDef { name: "lego_parts", columns: "part_num, name, part_cat_id", typed: "part_num varchar(255) NOT NULL, name text NOT NULL, part_cat_id integer NOT NULL", key: Some("part_num") },
    TableDef { name: "lego_sets", columns: "set_num, name, year, theme_id, num_parts", typed: "set_num varchar(255) NOT NULL, name varchar(255) NOT NULL, year integer, theme_id integer, num_parts integer", key: Some("set_num") },
    TableDef { name: "lego_inventories", columns: "id, version, set_num", typed: "id integer NOT NULL, version integer NOT NULL, set_num varchar(255) NOT NULL", key: Some("id") },
    TableDef { name: "lego_inventory_parts", columns: "inventory_id, part_num, color_id, quantity, is_spare", typed: "inventory_id integer NOT NULL, part_num varchar(255) NOT NULL, color_id integer NOT NULL, quantity integer NOT NULL, is_spare boolean NOT NULL", key: None },
    TableDef { name: "lego_inventory_sets", columns: "inventory_id, set_num, quantity", typed: "inventory_id integer NOT NULL, set_num varchar(255) NOT NULL, quantity integer NOT NULL", key: None },
    TableDef { name: "lego_builders", columns: "builder_id, name, home_zone", typed: "builder_id integer NOT NULL, name varchar(255) NOT NULL, home_zone varchar(64) NOT NULL", key: Some("builder_id") },
    TableDef { name: "lego_collection", columns: "builder_id, row_no, set_num, typed_set_num, typed_name", typed: "builder_id integer NOT NULL, row_no integer NOT NULL, set_num varchar(255) NOT NULL, typed_set_num varchar(255), typed_name varchar(255)", key: Some("builder_id, row_no") },
    TableDef { name: "lego_purchases", columns: "purchase_id, builder_id, row_no, store, ordered_at, ordered_local, delivered_at", typed: "purchase_id bigint NOT NULL, builder_id integer NOT NULL, row_no integer NOT NULL, store varchar(64) NOT NULL, ordered_at timestamptz NOT NULL, ordered_local varchar(32) NOT NULL, delivered_at timestamptz", key: Some("purchase_id") },
    TableDef { name: "trap_manifest", columns: "trap, origin, tbl, row_key, phase, wave, socket, detail", typed: "trap varchar(8) NOT NULL, origin varchar(16) NOT NULL, tbl varchar(64) NOT NULL, row_key text NOT NULL, phase varchar(32) NOT NULL, wave bigint NOT NULL, socket varchar(32), detail text NOT NULL", key: None },
    TableDef { name: "generator_run", columns: "key, value", typed: "key text NOT NULL, value text NOT NULL", key: Some("key") },
];

/// The composite indexes built after the load, by table: each led by the column the join into its
/// table fixes, and ending on the column the next join reads.
pub const STRANDS: [(&str, &str); 8] = [
    ("lego_themes", "parent_id, id"),
    ("lego_sets", "theme_id, set_num"),
    ("lego_inventories", "set_num, version, id"),
    ("lego_inventory_sets", "inventory_id, set_num"),
    ("lego_inventory_parts", "inventory_id, part_num, color_id"),
    ("lego_parts", "part_cat_id, part_num"),
    ("lego_purchases", "builder_id, row_no"),
    ("lego_collection", "set_num, builder_id, row_no"),
];

/// The name of a strand's index on `table` over `columns`.
pub fn strand_name(table: &str, columns: &str) -> String {
    format!("{table}_{}_idx", columns.replace(", ", "_"))
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

fn create_table(s: &Settings, t: &TableDef, with_key: bool) -> String {
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
    for t in &TABLES[4..] {
        sqlx::query(&create_table(s, t, s.index_timing == IndexTiming::Before))
            .execute(pool)
            .await?;
    }
    Ok(())
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

/// The reference tables, copied through unchanged, each created and loaded in one transaction.
pub async fn load_reference_tables(
    pool: &PgPool,
    s: &Settings,
    cat: &Catalogue,
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
    for name in [
        "lego_colors",
        "lego_themes",
        "lego_part_categories",
        "lego_parts",
    ] {
        let t = table(name);
        let mut enc = Enc::new(s.format);
        match name {
            "lego_colors" => cat.colours.iter().for_each(|c| {
                enc.reference_row(&[
                    Cell::Int(c.id),
                    Cell::Text(&c.name),
                    Cell::Text(&c.rgb),
                    Cell::Text(&c.is_trans),
                ])
            }),
            "lego_themes" => cat.themes.iter().for_each(|th| {
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
            _ => cat.parts.iter().for_each(|p| {
                enc.reference_row(&[
                    Cell::Text(&p.part_num),
                    Cell::Text(&p.name),
                    Cell::Int(p.part_cat_id),
                ])
            }),
        }
        enc.finish();
        let started = Instant::now();
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| err(t.name, enc.rows, e, false))?;
        let res = async {
            sqlx::query(&create_table(s, t, s.index_timing == IndexTiming::Before))
                .execute(&mut *tx)
                .await?;
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

/// After the load: keys (when built after), autovacuum back on, the strands, statistics, the
/// visibility map.
pub async fn finish(
    conn: &mut PgConnection,
    s: &Settings,
) -> Result<Vec<(String, Duration)>, sqlx::Error> {
    let mut steps = Vec::new();
    for t in &TABLES {
        let name = t.name;
        if s.index_timing == IndexTiming::After {
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
    for (table, columns) in STRANDS {
        let started = Instant::now();
        sqlx::query(&format!(
            "CREATE INDEX {} ON {}.{table} ({columns})",
            strand_name(table, columns),
            s.schema
        ))
        .execute(&mut *conn)
        .await?;
        steps.push((format!("strand {table}"), started.elapsed()));
        info!(
            table,
            columns,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "strand built"
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

pub async fn sizes(pool: &PgPool, schema: &str) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT c.relname::text, pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY 1",
    )
    .bind(schema)
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
