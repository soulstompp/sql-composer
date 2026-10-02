//! Generates a large LEGO catalogue from a real one, with builders' collections and purchase logs,
//! and loads it into Postgres in hierarchical waves: each wave's parents in one batch, then all of
//! their children in one batch per child table, and so on down.
//!
//! The real catalogue (the eight `lego_*` tables) is read from `--source-schema`. The generated
//! tables are written to `--schema`, which is dropped and recreated. Logs go to standard error;
//! the lines on standard output starting `SUMMARY` are the run's figures.

mod calendar;
mod catalogue;
mod chords;
mod demand;
mod encode;
mod load;
mod oo;
mod places;
mod rng;
mod switchboard;
mod text;
mod traps;
mod unnest;
mod world;

#[cfg(test)]
mod tests;

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use tracing::{error, info, info_span, warn, Instrument};
use tracing_subscriber::EnvFilter;

use crate::catalogue::{Catalogue, Real};
use crate::encode::{Enc, Format};
use crate::load::{IndexTiming, Method, Metrics, Settings};
use crate::switchboard::{Dials, Switchboard};
use crate::traps::DECLS;
use crate::world::{builders_for, Wiring, World, PURCHASES_PER_SET, ROWS_PER_BUILDER};

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Size {
    Small,
    Medium,
    Huge,
    #[value(name = "8m")]
    Sets8m,
    #[value(name = "80m")]
    Sets80m,
    #[value(name = "800m")]
    Sets800m,
}

impl Size {
    fn name(self) -> String {
        self.to_possible_value()
            .map_or_else(String::new, |v| v.get_name().to_string())
    }

    fn sets(self) -> u64 {
        match self {
            Size::Small => 20_000,
            Size::Medium => 200_000,
            Size::Huge => 2_000_000,
            Size::Sets8m => 8_000_000,
            Size::Sets80m => 80_000_000,
            Size::Sets800m => 800_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormat {
    Pretty,
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "lego",
    about = "Generate a large LEGO catalogue and load it into Postgres in hierarchical waves"
)]
struct Cli {
    /// Postgres connection URL. Its own variable, never the general `DATABASE_URL`, because the
    /// target schema is dropped and recreated.
    #[arg(long, env = "LEGO_GENERATOR_DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Schema holding the real `lego_*` tables to draw from.
    #[arg(long, default_value = "public")]
    source_schema: String,
    /// Schema the generated tables are written to; dropped and recreated.
    #[arg(long, default_value = "lego")]
    schema: String,
    /// A named size: small, medium, huge, or past huge by its number of sets: 8m, 80m, 800m.
    #[arg(long, value_enum, default_value = "huge")]
    size: Size,
    /// Synthesized sets; overrides `--size`.
    #[arg(long)]
    sets: Option<u64>,
    #[arg(long, default_value_t = 20_260_926)]
    seed: u64,
    /// Root records (sets; builders) per wave.
    #[arg(long, default_value_t = 1000)]
    chunk: u64,
    /// Waves written at once, each on its own session.
    #[arg(long, default_value_t = 4)]
    jobs: u32,
    /// The phases of the switchboard, as `<pattern>:<percent>,…`.
    #[arg(long, default_value = "natural:90,wavy:9,zchord:1")]
    patch: String,
    #[arg(long, default_value_t = 16)]
    wavy_period: u64,
    #[arg(long, default_value_t = 0.25)]
    wavy_amplitude: f64,
    #[arg(long, default_value_t = 0.9)]
    hotspot_share: f64,
    /// The hot socket as `r<root>/<decade>s`; the heaviest socket when absent.
    #[arg(long)]
    hotspot_socket: Option<String>,
    #[arg(long, default_value_t = 4)]
    swing_period: u64,
    /// First swing group: root theme ids and a half-open year range (the classic play themes of the classic era).
    #[arg(long, default_value = "50,126,186,147@1980-2000")]
    swing_a: String,
    /// Second swing group (the licensed themes of the modern era).
    #[arg(
        long,
        default_value = "158,482,246,561,264,269,272,570,579,577@2010-2030"
    )]
    swing_b: String,
    /// Nesting cords that cross to another root theme; the real catalogue's rate when absent.
    #[arg(long)]
    cross_nesting: Option<f64>,
    /// `-2` records in another socket than their `-1`; the real catalogue's rate when absent.
    #[arg(long)]
    cross_versions: Option<f64>,
    /// Bare records in another socket than their `-1`; the real catalogue's rate when absent.
    #[arg(long)]
    cross_twins: Option<f64>,
    /// Collection rows outside the builder's home socket.
    #[arg(long, default_value_t = 0.6)]
    cross_collections: f64,
    #[arg(long, default_value_t = 30_000)]
    lettered_ppm: u32,
    #[arg(long, default_value_t = 20_000)]
    rerelease_ppm: u32,
    #[arg(long, default_value_t = 700)]
    second_version_ppm: u32,
    #[arg(long, default_value_t = 150_000)]
    recolour_ppm: u32,
    /// COPY payload format.
    #[arg(long, default_value = "binary")]
    copy_format: String,
    /// `copy` or `unnest`.
    #[arg(long, default_value = "copy")]
    method: String,
    /// Build the primary keys `before` or `after` the load.
    #[arg(long, default_value = "after")]
    index_timing: String,
    /// Create the tables UNLOGGED (for scratch runs).
    #[arg(long)]
    unlogged: bool,
    /// `synchronous_commit` for the loading sessions.
    #[arg(long, default_value = "off")]
    synchronous_commit: String,
    /// `statement_timeout` for each wave's statements.
    #[arg(long, default_value = "300s")]
    wave_timeout: String,
    /// `statement_timeout` for the key, statistics and vacuum builds; an hour for every two million
    /// sets when absent.
    #[arg(long)]
    build_timeout: Option<String>,
    #[arg(long, default_value = "1GB")]
    maintenance_work_mem: String,
    #[arg(long, default_value_t = 4)]
    parallel_maintenance_workers: u32,
    /// COPY FREEZE for the reference tables (created in the loading transaction).
    #[arg(long)]
    freeze: bool,
    #[arg(long, default_value = "on")]
    analyze: String,
    #[arg(long, default_value = "on")]
    vacuum: String,
    /// Leave autovacuum on for the loaded tables while they load (off by default, and on again after).
    #[arg(long, default_value = "off")]
    autovacuum_during_load: String,
    /// Seconds between progress lines; 0 for none.
    #[arg(long, default_value_t = 10)]
    log_progress_every: u64,
    #[arg(long, value_enum, default_value = "pretty")]
    log_format: LogFormat,
    /// Generate and encode every wave, write nothing.
    #[arg(long)]
    dry_run: bool,
    /// Also build the object-oriented tables in this schema, which is dropped and recreated: the
    /// generated sets, colours and builders, each decomposed by kind with table inheritance.
    #[arg(long)]
    oo_schema: Option<String>,
    /// Write the synthesized sets of the patch's phases up to this one only (0: the real catalogue
    /// alone). The wiring is unchanged, so each prefix is exactly the start of the full run.
    #[arg(long)]
    upto_phase: Option<usize>,
}

/// Whether two schema names, as written on the command line, name the same schema: an unquoted
/// name folds to lower case, as Postgres folds it, and a quoted one keeps its case.
fn same_schema(a: &str, b: &str) -> bool {
    fn folded(s: &str) -> String {
        match s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            Some(quoted) => quoted.replace("\"\"", "\""),
            None => s.to_ascii_lowercase(),
        }
    }
    folded(a) == folded(b)
}

/// The build timeout for `sets` synthesized sets: an hour for every two million, at least an hour.
fn build_timeout_for(sets: u64) -> String {
    format!("{}s", 3600 * sets.div_ceil(2_000_000).max(1))
}

fn on(s: &str) -> Result<bool, String> {
    match s {
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        other => Err(format!("want on or off, not `{other}`")),
    }
}

fn init_logging(fmt: LogFormat) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn"));
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match fmt {
        LogFormat::Pretty => b.init(),
        LogFormat::Json => b.json().init(),
    }
}

fn summary(fields: &[&dyn std::fmt::Display]) {
    let parts: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
    println!("SUMMARY\t{}", parts.join("\t"));
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_format);
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "stopped");
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let wall = Instant::now();
    if same_schema(&cli.schema, &cli.source_schema) {
        return Err(format!(
            "--schema {} names the --source-schema: the target schema is dropped and recreated, \
             and the source schema holds the real catalogue",
            cli.schema
        ));
    }
    if let Some(oo) = &cli.oo_schema {
        if same_schema(oo, &cli.schema) || same_schema(oo, &cli.source_schema) {
            return Err(format!(
                "--oo-schema {oo} names the --schema or the --source-schema: the object-oriented \
                 schema is dropped and recreated"
            ));
        }
    }
    let sets = cli.sets.unwrap_or(cli.size.sets());
    let build_timeout = cli
        .build_timeout
        .clone()
        .unwrap_or_else(|| build_timeout_for(sets));
    let settings = Settings {
        schema: cli.schema.clone(),
        format: Format::parse(&cli.copy_format)?,
        method: Method::parse(&cli.method)?,
        index_timing: match cli.index_timing.as_str() {
            "before" => IndexTiming::Before,
            "after" => IndexTiming::After,
            other => {
                return Err(format!(
                    "--index-timing: want before or after, not `{other}`"
                ))
            }
        },
        unlogged: cli.unlogged,
        synchronous_commit: on(&cli.synchronous_commit)?,
        wave_timeout: cli.wave_timeout.clone(),
        build_timeout: build_timeout.clone(),
        maintenance_work_mem: cli.maintenance_work_mem.clone(),
        parallel_maintenance_workers: cli.parallel_maintenance_workers,
        freeze_reference_tables: cli.freeze,
        analyze: on(&cli.analyze)?,
        vacuum: on(&cli.vacuum)?,
        autovacuum_during_load: on(&cli.autovacuum_during_load)?,
    };
    let pool = load::pool(&cli.database_url, cli.jobs + 2, &settings)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let started = Instant::now();
    let cat = {
        let mut conn = pool.acquire().await.map_err(|e| format!("connect: {e}"))?;
        sqlx::query(&format!("SET statement_timeout = '{build_timeout}'"))
            .execute(&mut *conn)
            .await
            .map_err(|e| e.to_string())?;
        let absent = catalogue::missing(&mut conn, &cli.source_schema)
            .await
            .map_err(|e| format!("reading {}: {e}", cli.source_schema))?;
        if !absent.is_empty() {
            return Err(format!(
                "schema {} of this database does not hold the real catalogue (missing: {}). Load \
                 it with `cargo run -p lego-example -- --database-url <the same URL> setup`, which \
                 needs psql on the PATH, or name the schema that holds it with --source-schema",
                cli.source_schema,
                absent.join(", ")
            ));
        }
        let cat = Catalogue::read(&mut conn, &cli.source_schema)
            .await
            .map_err(|e| format!("reading {}: {e}", cli.source_schema))?;
        sqlx::query(&format!("SET statement_timeout = '{}'", cli.wave_timeout))
            .execute(&mut *conn)
            .await
            .map_err(|e| e.to_string())?;
        cat
    };
    let real = Real::new(cat);
    info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        sets = real.cat.sets.len(),
        lines = real.cat.lines.len(),
        "real catalogue read"
    );

    let builders = builders_for(sets + real.cat.sets.len() as u64);
    let wiring = Wiring {
        seed: cli.seed,
        sets,
        chunk: cli.chunk.max(1),
        builders,
        rows_per_builder: ROWS_PER_BUILDER,
        lettered_ppm: cli.lettered_ppm,
        rerelease_ppm: cli.rerelease_ppm,
        second_version_ppm: cli.second_version_ppm,
        recolour_ppm: cli.recolour_ppm,
        cross_nesting: cli
            .cross_nesting
            .unwrap_or(real.nesting_root_crossing.rate()),
        cross_versions: cli.cross_versions.unwrap_or(real.version_crossing.rate()),
        cross_twins: cli.cross_twins.unwrap_or(real.twin_crossing.rate()),
        cross_collections: cli.cross_collections,
    };
    let dials = Dials {
        wavy_period: cli.wavy_period,
        wavy_amplitude: cli.wavy_amplitude,
        hotspot_share: cli.hotspot_share,
        hotspot_socket: cli.hotspot_socket.clone(),
        swing_period: cli.swing_period,
        swing_a: cli.swing_a.clone(),
        swing_b: cli.swing_b.clone(),
    };
    let board = Switchboard::new(&real, &cli.patch, dials, sets, wiring.chunk)?;
    let world = Arc::new(World::new(real, wiring.clone(), board)?);

    // The banner: the resolved wiring and the server it runs against.
    let server = load::server_settings(&pool).await;
    let mut run_rows: Vec<(String, String)> = vec![
        ("size".into(), cli.size.name()),
        ("sets".into(), sets.to_string()),
        ("seed".into(), cli.seed.to_string()),
        ("chunk".into(), wiring.chunk.to_string()),
        ("jobs".into(), cli.jobs.to_string()),
        ("patch".into(), world.board.to_string()),
        ("wavy_period".into(), cli.wavy_period.to_string()),
        ("wavy_amplitude".into(), cli.wavy_amplitude.to_string()),
        (
            "hotspot".into(),
            world.real.sockets[usize::from(world.board.hot_socket())].label(),
        ),
        ("real_sets".into(), world.real.cat.sets.len().to_string()),
        ("real_lines".into(), world.real.cat.lines.len().to_string()),
        ("sockets".into(), world.real.sockets.len().to_string()),
        (
            "sockets_with_real_sets".into(),
            world
                .real
                .sockets
                .iter()
                .filter(|s| !s.templates.is_empty())
                .count()
                .to_string(),
        ),
        (
            "no_release_years".into(),
            catalogue::NO_RELEASE
                .iter()
                .map(|r| format!("{}-{}", r.start(), r.end()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "redated_real_sets".into(),
            world.real.redated.len().to_string(),
        ),
        (
            "growth_fitted".into(),
            format!(
                "{:.4} per year over {}-{}",
                world.real.fitted_growth.exp() - 1.0,
                catalogue::MODEL_FIT.start(),
                catalogue::MODEL_FIT.end()
            ),
        ),
        (
            "growth_modelled".into(),
            format!(
                "{:.4} per year, {} times the fitted rate",
                (catalogue::GROWTH_FACTOR * world.real.fitted_growth).exp() - 1.0,
                catalogue::GROWTH_FACTOR
            ),
        ),
        (
            "modelled_releases".into(),
            world
                .real
                .modelled
                .iter()
                .map(|(y, n)| format!("{y}:{n:.0}"))
                .collect::<Vec<_>>()
                .join(","),
        ),
        ("builders".into(), builders.to_string()),
        ("purchases_per_set".into(), PURCHASES_PER_SET.to_string()),
        (
            "collection_rows".into(),
            world.collection_rows().to_string(),
        ),
        ("rows_per_builder".into(), ROWS_PER_BUILDER.to_string()),
        (
            "cross_nesting".into(),
            format!("{:.4}", wiring.cross_nesting),
        ),
        (
            "cross_versions".into(),
            format!("{:.4}", wiring.cross_versions),
        ),
        ("cross_twins".into(), format!("{:.4}", wiring.cross_twins)),
        (
            "cross_collections".into(),
            format!("{:.4}", wiring.cross_collections),
        ),
        (
            "measured_nesting_root_crossing".into(),
            format!(
                "{}/{}",
                world.real.nesting_root_crossing.crossed, world.real.nesting_root_crossing.total
            ),
        ),
        (
            "measured_nesting_socket_crossing".into(),
            format!(
                "{}/{}",
                world.real.nesting_crossing.crossed, world.real.nesting_crossing.total
            ),
        ),
        (
            "measured_version_crossing".into(),
            format!(
                "{}/{}",
                world.real.version_crossing.crossed, world.real.version_crossing.total
            ),
        ),
        (
            "measured_twin_crossing".into(),
            format!(
                "{}/{}",
                world.real.twin_crossing.crossed, world.real.twin_crossing.total
            ),
        ),
        ("lettered_ppm".into(), cli.lettered_ppm.to_string()),
        ("rerelease_ppm".into(), cli.rerelease_ppm.to_string()),
        (
            "second_version_ppm".into(),
            cli.second_version_ppm.to_string(),
        ),
        ("recolour_ppm".into(), cli.recolour_ppm.to_string()),
        ("copy_format".into(), settings.format.name().into()),
        ("method".into(), cli.method.clone()),
        ("index_timing".into(), cli.index_timing.clone()),
        ("unlogged".into(), cli.unlogged.to_string()),
        ("synchronous_commit".into(), cli.synchronous_commit.clone()),
        ("freeze_reference_tables".into(), cli.freeze.to_string()),
        (
            "autovacuum_during_load".into(),
            cli.autovacuum_during_load.clone(),
        ),
        (
            "oo_schema".into(),
            cli.oo_schema.clone().unwrap_or_default(),
        ),
    ];
    for d in DECLS {
        let planted = world
            .planted
            .iter()
            .find(|p| p.0 == d.trap)
            .map_or(0, |p| p.1);
        run_rows.push((
            format!("trap_{}", d.trap),
            format!(
                "{:?} per_million={} floor={} planted={}: {}",
                d.population, d.per_million, d.floor, planted, d.title
            ),
        ));
    }
    run_rows.extend(load::roster_rows(&settings.schema));
    for (k, v) in &server {
        run_rows.push((format!("server_{k}"), v.clone()));
    }
    info!("resolved wiring and server:");
    for (k, v) in &run_rows {
        info!("  {k} = {v}");
        summary(&[&"config", k, v]);
    }
    if cli.freeze {
        warn!("COPY FREEZE applies to the reference tables only: every other table is written by many transactions, and FREEZE needs the table created or truncated in the loading one");
    }

    let metrics = Arc::new(Metrics::default());
    let real_waves = world.real_waves;
    let sets_written = match cli.upto_phase {
        None => sets,
        Some(0) => 0,
        Some(n) => world
            .board
            .phases
            .get(n - 1)
            .map(|p| p.end)
            .ok_or_else(|| {
                format!(
                    "--upto-phase {n}: the patch has {} phases",
                    world.board.phases.len()
                )
            })?,
    };
    summary(&[&"config", &"sets_written", &sets_written]);
    let synthetic_waves = sets_written.div_ceil(wiring.chunk);
    let builder_waves = builders.div_ceil(wiring.chunk);
    let total_waves = real_waves + synthetic_waves + builder_waves;
    let wal_before = if cli.dry_run {
        None
    } else {
        load::wal_lsn(&pool).await
    };

    if !cli.dry_run {
        let t = Instant::now();
        load::create_schema(&pool, &settings)
            .await
            .map_err(|e| format!("create schema: {e}"))?;
        load::load_reference_tables(
            &pool,
            &settings,
            &world.real.cat,
            &world.added_themes(),
            &world.places,
            &metrics,
        )
        .await
        .map_err(|e| e.to_string())?;
        load::write_run(&pool, &settings, &run_rows)
            .await
            .map_err(|e| format!("generator_run: {e}"))?;
        let plan = world.plan_manifest();
        let mut enc = Enc::new(settings.format);
        let b = load::Batches {
            label: "plan".into(),
            levels: vec![("trap_manifest", load::Level::Manifest(&plan))],
        };
        load::write_wave(&pool, &settings, &b, &mut enc, &metrics)
            .await
            .map_err(|e| e.to_string())?;
        metrics.absorb_counts(&Default::default(), &Default::default(), &plan);
        summary(&[
            &"step",
            &"schema_and_reference_tables",
            &format!("{:.3}", t.elapsed().as_secs_f64()),
        ]);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let watcher = if cli.log_progress_every > 0 && !cli.dry_run {
        Some(tokio::spawn(load::watch_progress(
            pool.clone(),
            Duration::from_secs(cli.log_progress_every),
            metrics.clone(),
            stop.clone(),
            total_waves,
        )))
    } else {
        None
    };

    let next = Arc::new(AtomicU64::new(0));
    let load_started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..cli.jobs.max(1) {
        let world = world.clone();
        let pool = pool.clone();
        let settings = settings.clone();
        let metrics = metrics.clone();
        let next = next.clone();
        let dry = cli.dry_run;
        workers.push(tokio::spawn(async move {
            let mut enc = Enc::new(settings.format);
            let chunk = world.wiring.chunk;
            loop {
                if metrics.failed.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let k = next.fetch_add(1, Ordering::SeqCst);
                if k >= total_waves {
                    return Ok(());
                }
                let res = if k < real_waves {
                    let w = world.clone();
                    let wave = tokio::task::spawn_blocking(move || w.real_wave(k)).await.expect("generation");
                    let label = format!("real/{k}");
                    let span = info_span!("wave", wave = %label, worker, origin = "real", phase = "0:real", sockets = wave.by_socket.len());
                    write_set_wave(&world, &pool, &settings, &mut enc, &metrics, label, wave, dry).instrument(span).await
                } else if k < real_waves + synthetic_waves {
                    let s = k - real_waves;
                    let (lo, hi) = (s * chunk, ((s + 1) * chunk).min(sets_written));
                    let w = world.clone();
                    let wave = tokio::task::spawn_blocking(move || w.set_wave(k, lo, hi)).await.expect("generation");
                    let phase = world.board.phase_of(lo).label();
                    let label = format!("sets/{s}");
                    let mix = socket_mix(&wave.by_socket);
                    let span = info_span!("wave", wave = %label, worker, origin = "synthetic", phase = %phase, sets = hi - lo, sockets = %mix);
                    write_set_wave(&world, &pool, &settings, &mut enc, &metrics, label, wave, dry).instrument(span).await
                } else {
                    let b = k - real_waves - synthetic_waves;
                    let (lo, hi) = (b * chunk, ((b + 1) * chunk).min(world.wiring.builders));
                    let w = world.clone();
                    let wave = tokio::task::spawn_blocking(move || w.builder_wave(k, lo, hi)).await.expect("generation");
                    let label = format!("builders/{b}");
                    let span = info_span!("wave", wave = %label, worker, origin = "builders", builders = hi - lo);
                    async {
                        let batches = load::builder_wave_batches(label.clone(), &wave);
                        let t = Instant::now();
                        let r = if dry { dry_encode(&batches, &mut enc, &metrics); Ok(()) } else { load::write_wave(&pool, &settings, &batches, &mut enc, &metrics).await };
                        if r.is_ok() {
                            metrics.absorb_counts(&Default::default(), &Default::default(), &wave.manifest);
                            metrics.absorb_bought(&wave.bought);
                            info!(builders = wave.builders.len(), collection = wave.collection.len(), purchases = wave.purchases.len(), manifest = wave.manifest.len(), elapsed_ms = t.elapsed().as_millis() as u64, "wave written");
                        }
                        r
                    }
                    .instrument(span)
                    .await
                };
                match res {
                    Ok(()) => {
                        metrics.waves_done.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        metrics.failed.store(true, Ordering::SeqCst);
                        return Err(e);
                    }
                }
            }
        }));
    }
    let mut failure = None;
    for w in workers {
        match w.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failure = Some(e.to_string()),
            Err(e) => failure = Some(format!("worker: {e}")),
        }
    }
    stop.store(true, Ordering::SeqCst);
    if let Some(w) = watcher {
        w.abort();
    }
    let load_secs = load_started.elapsed().as_secs_f64();
    summary(&[&"step", &"waves", &format!("{load_secs:.3}")]);
    if let Some(f) = failure {
        let done = metrics.waves_done.load(Ordering::SeqCst);
        return Err(format!("{f}; {done} of {total_waves} waves committed"));
    }

    let mut steps = Vec::new();
    let mut oo_rows = Vec::new();
    if !cli.dry_run {
        let mut conn = load::build_session(&cli.database_url, &settings)
            .await
            .map_err(|e| format!("build session: {e}"))?;
        steps = load::finish(&mut conn, &settings)
            .await
            .map_err(|e| format!("finish: {e}"))?;
        if let Some(oo) = &cli.oo_schema {
            let built = oo::build(&mut conn, &cli.schema, oo).await?;
            steps.extend(built.steps);
            oo_rows = built.rows;
        }
    }

    // The summary.
    for (step, d) in &steps {
        summary(&[&"step", step, &format!("{:.3}", d.as_secs_f64())]);
    }
    for (class, rows) in &oo_rows {
        summary(&[&"oo", class, rows]);
    }
    {
        let levels = metrics.levels.lock().expect("metrics lock");
        for (t, s) in levels.iter() {
            let secs = s.busy.as_secs_f64();
            summary(&[
                &"table",
                t,
                &s.rows,
                &s.bytes,
                &s.batches,
                &s.largest_batch,
                &format!("{secs:.3}"),
                &((s.rows as f64 / secs.max(1e-9)) as u64),
            ]);
        }
    }
    for (p, (s, l)) in metrics.by_phase.lock().expect("metrics lock").iter() {
        summary(&[&"phase", p, s, l]);
    }
    let mut sockets: Vec<(String, (u64, u64))> = metrics
        .by_socket
        .lock()
        .expect("metrics lock")
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    sockets.sort();
    for (label, (s, l)) in &sockets {
        summary(&[&"socket", label, s, l]);
    }
    for d in DECLS {
        let planted = world
            .planted
            .iter()
            .find(|p| p.0 == d.trap)
            .map_or(0, |p| p.1);
        let rows = metrics
            .by_trap
            .lock()
            .expect("metrics lock")
            .get(&d.trap.to_string())
            .copied()
            .unwrap_or(0);
        summary(&[&"trap", &d.trap, &planted, &rows]);
    }
    // Buying: how many of the sets on sale were bought at all, and how the purchases fall by tenths
    // of the sets, most bought first.
    {
        let bought = metrics.bought.lock().expect("metrics lock");
        let mut counts: Vec<u64> = world
            .buyable
            .iter()
            .map(|&s| u64::from(bought.get(s as usize).copied().unwrap_or(0)))
            .collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let on_sale = counts.len();
        let once = counts.iter().filter(|&&n| n > 0).count();
        let total: u64 = counts.iter().sum();
        summary(&[&"buying", &"sets_on_sale", &on_sale]);
        summary(&[
            &"buying",
            &"sets_bought",
            &once,
            &format!("{:.4}", once as f64 / on_sale.max(1) as f64),
        ]);
        summary(&[&"buying", &"purchases", &total]);
        for d in 0..10 {
            let slice = &counts[on_sale * d / 10..on_sale * (d + 1) / 10];
            let n: u64 = slice.iter().sum();
            summary(&[
                &"buying",
                &"tenth",
                &(d + 1),
                &n,
                &format!("{:.4}", n as f64 / total.max(1) as f64),
            ]);
        }
    }
    if !cli.dry_run {
        if let (Some(a), Some(b)) = (wal_before, load::wal_lsn(&pool).await) {
            if let Some(n) = load::wal_bytes(&pool, &a, &b).await {
                summary(&[&"wal_bytes", &n]);
            }
        }
        for (t, n) in load::sizes(&pool, &settings.schema).await {
            summary(&[&"size", &t, &n]);
        }
    }
    summary(&[&"waves", &total_waves]);
    summary(&[
        &"wall_seconds",
        &format!("{:.3}", wall.elapsed().as_secs_f64()),
    ]);
    Ok(())
}

fn socket_mix(by_socket: &std::collections::HashMap<String, (u64, u64)>) -> String {
    let mut v: Vec<(&String, &(u64, u64))> = by_socket.iter().collect();
    v.sort_by(|a, b| b.1 .0.cmp(&a.1 .0).then(a.0.cmp(b.0)));
    let top: Vec<String> = v
        .iter()
        .take(3)
        .map(|(k, c)| format!("{k}:{}", c.0))
        .collect();
    format!("{} sockets; {}", v.len(), top.join(" "))
}

fn dry_encode(b: &load::Batches<'_>, enc: &mut Enc, metrics: &Metrics) {
    for (name, level) in &b.levels {
        let t = Instant::now();
        let rows = level_rows_and_encode(level, enc);
        if rows > 0 {
            metrics.add(name, rows, enc.buf.len() as u64, t.elapsed());
        }
    }
}

fn level_rows_and_encode(level: &load::Level<'_>, enc: &mut Enc) -> u64 {
    enc.reset();
    let rows = level.rows();
    level.encode(enc);
    enc.finish();
    rows
}

#[allow(clippy::too_many_arguments)]
async fn write_set_wave(
    world: &World,
    pool: &sqlx::PgPool,
    settings: &Settings,
    enc: &mut Enc,
    metrics: &Metrics,
    label: String,
    wave: world::SetWave,
    dry: bool,
) -> Result<(), load::LoadError> {
    let t = Instant::now();
    let batches = load::set_wave_batches(label, &wave, &world.real.cat.part_pool);
    let r = if dry {
        dry_encode(&batches, enc, metrics);
        Ok(())
    } else {
        load::write_wave(pool, settings, &batches, enc, metrics).await
    };
    if r.is_ok() {
        metrics.absorb_counts(&wave.by_socket, &wave.by_phase, &wave.manifest);
        info!(
            sets = wave.sets.len(),
            inventories = wave.inventories.len(),
            lines = wave.lines.len(),
            nests = wave.nests.len(),
            manifest = wave.manifest.len(),
            elapsed_ms = t.elapsed().as_millis() as u64,
            "wave written"
        );
    }
    r
}
