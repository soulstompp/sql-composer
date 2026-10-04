//! Partitions by id (`--partitioning`): after the load, the generated tables are copied into a
//! schema per method, `<schema>_<method>`, each table split into `--partitions` parts by the
//! leading column of its natural key, the unique key over the table's own attributes:
//! - `inheritance`: child tables below an empty parent, each holding one range of the column under
//!   a CHECK, as tables were partitioned before declarative partitioning. The children are filled
//!   in one pass over the generated table: they start as the partitions of a declarative table,
//!   `<table>_load`, each already under its CHECK, which routes every row to its child; then each
//!   is detached and made a child of the parent with `INHERIT`, and `<table>_load` is dropped.
//!   Nothing routes a row written later.
//! - `range`: a declarative `PARTITION BY RANGE`, with a `DEFAULT` partition for rows written
//!   later.
//! - `hash`: a declarative `PARTITION BY HASH`.
//!
//! Every run drops every method's schema before the load, asked for or not, so that no copy keeps
//! an earlier load's rows. The ranges are of equal width between the column's minimum and maximum,
//! read after the load. A table is left unpartitioned when it has no natural key, or when its key
//! holds an expression or a nullable column; under the two range methods, also when its key leads
//! with a text column, which has no equal-width ranges. Read a schema with the generated schema
//! behind it on the `search_path`, for the tables it leaves out.
//!
//! Each schema gets the indexes `--indexes` asks for: on a declarative table's parent, from which
//! they reach every partition, and on each inheritance child, since an index covers one table. A
//! key that holds the partition column stays unique across the whole table: a declarative table
//! enforces it, and under inheritance two rows equal on it are equal on the partition column, so
//! they fall in one child, whose unique index refuses the second. The natural key always holds it.
//! Any other key is built as a plain index, and `generator_run` records it as no longer unique.
//!
//! After the build, a check counts each table's rows across its partitions against the generated
//! table, reads every range back from the catalogue to see that none overlaps another, and sees
//! that no value of a key that holds the partition column appears twice: where a valid unique
//! index enforces the key, that is its work, and elsewhere the rows are counted.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sqlx::PgConnection;
use tracing::info;

use crate::load::{
    index_name, ExtensionSchemas, Indexes, TableDef, COMPOSITE_INDEXES, SEARCHES, TABLES,
    UNIQUE_KEYS,
};
use crate::oo::{count, exec};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Method {
    Inheritance,
    Range,
    Hash,
}

impl Method {
    pub const ALL: [Method; 3] = [Method::Inheritance, Method::Range, Method::Hash];

    pub fn name(self) -> &'static str {
        match self {
            Method::Inheritance => "inheritance",
            Method::Range => "range",
            Method::Hash => "hash",
        }
    }

    fn declarative(self) -> bool {
        self != Method::Inheritance
    }
}

/// `none`, or a comma-separated list of `inheritance`, `range` and `hash`.
pub fn parse_methods(s: &str) -> Result<BTreeSet<Method>, String> {
    let mut out = BTreeSet::new();
    if s.trim() == "none" {
        return Ok(out);
    }
    for item in s.split(',') {
        let m = Method::ALL
            .into_iter()
            .find(|m| m.name() == item.trim())
            .ok_or_else(|| {
                format!(
                    "--partitioning: want none, or a list of inheritance, range and hash, \
                     not `{}`",
                    item.trim()
                )
            })?;
        out.insert(m);
    }
    Ok(out)
}

/// The list as `--partitioning` takes it.
pub fn methods_name(methods: &BTreeSet<Method>) -> String {
    if methods.is_empty() {
        return "none".into();
    }
    methods.iter().map(|m| m.name()).collect::<Vec<_>>().join(",")
}

/// The run's own tables, which `--partition-tables all` leaves out.
pub const RUN_TABLES: [&str; 2] = ["trap_manifest", "generator_run"];

/// `all`, every generated table but the run's own, or a comma-separated list of the generated
/// tables' names, in the order the load declares them.
pub fn parse_tables(s: &str) -> Result<Vec<&'static TableDef>, String> {
    if s.trim() == "all" {
        return Ok(TABLES
            .iter()
            .filter(|t| !RUN_TABLES.contains(&t.name))
            .collect());
    }
    let mut named = BTreeSet::new();
    for item in s.split(',') {
        let item = item.trim();
        if !TABLES.iter().any(|t| t.name == item) {
            return Err(format!(
                "--partition-tables: want all, or a list of the generated tables ({}), \
                 not `{item}`",
                TABLES.iter().map(|t| t.name).collect::<Vec<_>>().join(", ")
            ));
        }
        named.insert(item);
    }
    Ok(TABLES.iter().filter(|t| named.contains(t.name)).collect())
}

/// The list as `--partition-tables` takes it.
pub fn tables_name(tables: &[&TableDef]) -> String {
    let all = parse_tables("all").expect("all");
    if tables.iter().map(|t| t.name).eq(all.iter().map(|t| t.name)) {
        return "all".into();
    }
    tables.iter().map(|t| t.name).collect::<Vec<_>>().join(",")
}

/// The schema a method's copies go in: the generated schema's name with the method's after it.
pub fn method_schema(schema: &str, method: Method) -> String {
    match schema.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => format!("\"{quoted}_{}\"", method.name()),
        None => format!("{}_{}", schema.to_ascii_lowercase(), method.name()),
    }
}

/// The tables that declare more than one unique key, with the one that is their natural key, the
/// unique key over the table's own attributes; and the tables whose natural key is a unique key
/// they declare beside no primary key. Every other table's natural key is its primary key: the
/// catalogue's ids are the catalogue's own identifiers, and a generated id is the generator's
/// identity for what it generates. A key is what a table declares, never what one load's rows
/// happen to hold once.
pub const NATURAL_KEYS: &[(&str, &str)] = &[
    (
        "lego_inventory_parts",
        "inventory_id, part_num, color_id, is_spare",
    ),
    ("lego_inventory_sets", "inventory_id, set_num"),
    ("lego_postcodes", "code"),
];

/// A table's natural key: its entry in `NATURAL_KEYS`, else its primary key.
pub fn natural_key(t: &TableDef) -> Option<&'static str> {
    NATURAL_KEYS
        .iter()
        .find(|(name, _)| *name == t.name)
        .map(|(_, key)| *key)
        .or(t.key)
}

/// One of the unique keys a table declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    Primary(&'static str),
    Unique {
        name: &'static str,
        columns: &'static str,
    },
}

impl Key {
    pub fn columns(self) -> &'static str {
        match self {
            Key::Primary(c) => c,
            Key::Unique { columns, .. } => columns,
        }
    }

    /// The key's name on `table`: its constraint's.
    pub fn name(self, table: &str) -> String {
        match self {
            Key::Primary(_) => format!("{table}_pkey"),
            Key::Unique { name, .. } => name.to_string(),
        }
    }

    /// Whether the key stays unique across a table partitioned by `column`: it holds the column.
    pub fn holds(self, column: &str) -> bool {
        self.columns().split(',').any(|c| c.trim() == column)
    }
}

#[cfg(test)]
fn column_set(columns: &str) -> BTreeSet<&str> {
    columns.split(',').map(str::trim).collect()
}

/// The unique keys a table declares: its primary key, and the unique keys the load adds.
pub fn keys(t: &TableDef) -> Vec<Key> {
    let mut out: Vec<Key> = t.key.map(Key::Primary).into_iter().collect();
    for (table, name, columns) in UNIQUE_KEYS {
        if *table == t.name {
            out.push(Key::Unique { name, columns });
        }
    }
    out
}

/// The keys of `t` that are no longer unique across it when it is partitioned by `column`.
pub fn unique_lost(t: &TableDef, column: &str) -> Vec<Key> {
    keys(t).into_iter().filter(|k| !k.holds(column)).collect()
}

/// The partition column: the leading column of a table's natural key, and whether it holds text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub text: bool,
}

/// The columns of a `CREATE TABLE` column list, split at the commas outside parentheses.
fn column_defs(typed: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, ch) in typed.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(typed[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(typed[start..].trim());
    out
}

/// The column `t` is partitioned by under `method`, or why it is left unpartitioned.
pub fn partition_column(t: &TableDef, method: Method) -> Result<Column, String> {
    let key = natural_key(t).ok_or_else(|| "it has no natural key".to_string())?;
    let defs = column_defs(t.typed);
    let mut lead = None;
    for column in key.split(',').map(str::trim) {
        if !column
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(format!("its natural key holds an expression, `{column}`"));
        }
        let def = defs
            .iter()
            .find(|d| d.split_whitespace().next() == Some(column))
            .ok_or_else(|| format!("its natural key names {column}, which it does not declare"))?;
        if !def.contains("NOT NULL") {
            return Err(format!("its natural key holds {column}, which is nullable"));
        }
        lead.get_or_insert((column, *def));
    }
    let (lead, def) = lead.ok_or_else(|| "its natural key names no column".to_string())?;
    let ty = def[lead.len()..].trim_start().to_ascii_lowercase();
    let text = if ["smallint", "integer", "bigint"].iter().any(|i| ty.starts_with(i)) {
        false
    } else if ["varchar", "text", "character"].iter().any(|i| ty.starts_with(i)) {
        true
    } else {
        return Err(format!(
            "its natural key leads with {lead}, of a type not partitioned here"
        ));
    };
    if text && method != Method::Hash {
        return Err(format!(
            "its natural key leads with {lead}, text, which has no equal-width ranges"
        ));
    }
    Ok(Column { name: lead, text })
}

/// `n` half-open ranges `[lo, hi)` of equal width, the first starting at `min`, together holding
/// every value from `min` to `max`. The last ones hold nothing when there are fewer values than
/// ranges.
pub fn equal_ranges(min: i128, max: i128, n: u32) -> Vec<(i128, i128)> {
    let n = i128::from(n.max(1));
    let span = (max - min + 1).max(1);
    let width = (span + n - 1) / n;
    (0..n)
        .map(|k| (min + k * width, min + (k + 1) * width))
        .collect()
}

/// A partition's or child's name: the table's, then its place.
fn part_name(table: &str, k: usize) -> String {
    format!("{table}_p{k}")
}

/// The statements that make `table`'s copy in `schema` under `method`, partitions included, from
/// the generated table in `generated`: `ranges` for the range methods, `n` partitions for hash.
/// UNLOGGED applies to the tables that hold rows: a partitioned table holds none, and PostgreSQL
/// refuses an unlogged one.
#[allow(clippy::too_many_arguments)]
pub fn create_statements(
    method: Method,
    schema: &str,
    generated: &str,
    table: &str,
    column: &str,
    ranges: &[(i128, i128)],
    n: u32,
    unlogged: bool,
) -> Vec<String> {
    let un = if unlogged { "UNLOGGED " } else { "" };
    let mut out = Vec::new();
    match method {
        Method::Inheritance => {
            out.push(format!(
                "CREATE {un}TABLE {schema}.{table} (LIKE {generated}.{table})"
            ));
            out.push(format!(
                "CREATE TABLE {schema}.{table}_load (LIKE {generated}.{table}) \
                 PARTITION BY RANGE ({column})"
            ));
            for (k, (lo, hi)) in ranges.iter().enumerate() {
                let child = part_name(table, k);
                out.push(format!(
                    "CREATE {un}TABLE {schema}.{child} PARTITION OF {schema}.{table}_load \
                     (CONSTRAINT {child}_check CHECK ({column} >= {lo} AND {column} < {hi})) \
                     FOR VALUES FROM ({lo}) TO ({hi})"
                ));
            }
        }
        Method::Range => {
            out.push(format!(
                "CREATE TABLE {schema}.{table} (LIKE {generated}.{table}) \
                 PARTITION BY RANGE ({column})"
            ));
            for (k, (lo, hi)) in ranges.iter().enumerate() {
                out.push(format!(
                    "CREATE {un}TABLE {schema}.{} PARTITION OF {schema}.{table} \
                     FOR VALUES FROM ({lo}) TO ({hi})",
                    part_name(table, k)
                ));
            }
            out.push(format!(
                "CREATE {un}TABLE {schema}.{table}_default PARTITION OF {schema}.{table} DEFAULT"
            ));
        }
        Method::Hash => {
            out.push(format!(
                "CREATE TABLE {schema}.{table} (LIKE {generated}.{table}) \
                 PARTITION BY HASH ({column})"
            ));
            for k in 0..n {
                out.push(format!(
                    "CREATE {un}TABLE {schema}.{} PARTITION OF {schema}.{table} \
                     FOR VALUES WITH (MODULUS {n}, REMAINDER {k})",
                    part_name(table, k as usize)
                ));
            }
        }
    }
    out
}

/// The statements that fill `table`'s copy in one pass over the generated table: a declarative
/// table through its parent, which routes each row; the inheritance children through
/// `<table>_load`, from which they are then detached and put below their parent.
pub fn fill_statements(
    method: Method,
    schema: &str,
    generated: &str,
    table: &str,
    ranges: &[(i128, i128)],
) -> Vec<String> {
    match method {
        Method::Inheritance => {
            let load = format!("{schema}.{table}_load");
            let mut out = vec![format!(
                "INSERT INTO {load} SELECT * FROM {generated}.{table}"
            )];
            for k in 0..ranges.len() {
                let child = format!("{schema}.{}", part_name(table, k));
                out.push(format!("ALTER TABLE {load} DETACH PARTITION {child}"));
                out.push(format!("ALTER TABLE {child} INHERIT {schema}.{table}"));
            }
            out.push(format!("DROP TABLE {load}"));
            out
        }
        Method::Range | Method::Hash => vec![format!(
            "INSERT INTO {schema}.{table} SELECT * FROM {generated}.{table}"
        )],
    }
}

/// The statements that index one table of a method's schema: the `targets` are the declarative
/// parent, or every inheritance child. A target's index names start with the target's name. A key
/// that holds the partition column is built unique, and any other as a plain index.
pub fn index_statements(
    schema: &str,
    generated: &str,
    t: &TableDef,
    column: &str,
    targets: &[String],
    indexes: Indexes,
    extension_schemas: &ExtensionSchemas,
) -> Vec<String> {
    let mut out = Vec::new();
    let renamed = |name: &str, target: &str| name.replacen(t.name, target, 1);
    for target in targets {
        let on = format!("{schema}.{target}");
        if indexes.keys {
            for key in keys(t) {
                let columns = key.columns();
                out.push(match (key, key.holds(column)) {
                    (Key::Primary(_), true) => {
                        format!("ALTER TABLE {on} ADD PRIMARY KEY ({columns})")
                    }
                    (Key::Unique { name, .. }, true) => format!(
                        "ALTER TABLE {on} ADD CONSTRAINT {} UNIQUE ({columns})",
                        renamed(name, target)
                    ),
                    (Key::Unique { name, .. }, false) => format!(
                        "CREATE INDEX {} ON {on} ({columns})",
                        renamed(name, target)
                    ),
                    (_, false) => format!(
                        "CREATE INDEX {target}_{}_idx ON {on} ({columns})",
                        columns.replace(", ", "_")
                    ),
                });
            }
        }
        if indexes.composite {
            for ix in COMPOSITE_INDEXES.iter().filter(|ix| ix.table == t.name) {
                out.push(ix.create_sql(&index_name(target, ix.parts), &on, generated));
            }
        }
        if indexes.search {
            for s in SEARCHES.iter().filter(|s| s.table == t.name) {
                out.push(format!(
                    "CREATE INDEX {} ON {on} {}",
                    renamed(s.name, target),
                    s.sql_in(extension_schemas)
                ));
            }
        }
    }
    out
}

/// The tables a method partitions, each with its column.
pub type Partitioned = Vec<(&'static TableDef, Column)>;
/// The tables a method leaves out, each with why.
pub type LeftOut = Vec<(&'static str, String)>;

/// The tables `method` partitions, each with its column, and the ones it leaves out, each with why.
pub fn plan(method: Method, tables: &[&'static TableDef]) -> (Partitioned, LeftOut) {
    let (mut kept, mut left) = (Vec::new(), Vec::new());
    for t in tables {
        match partition_column(t, method) {
            Ok(c) => kept.push((*t, c)),
            Err(why) => left.push((t.name, why)),
        }
    }
    (kept, left)
}

/// What `generator_run` records of the partitioning: the methods, the tables, the count, each
/// table's natural key and column, each method's schema, its tables and the ones it leaves out,
/// and, when the keys are built, every key that is no longer unique across its table.
pub fn run_rows(
    schema: &str,
    methods: &BTreeSet<Method>,
    tables: &[&'static TableDef],
    n: u32,
    indexes: Indexes,
) -> Vec<(String, String)> {
    let mut rows = vec![("partitioning".to_string(), methods_name(methods))];
    if methods.is_empty() {
        return rows;
    }
    rows.push(("partition_tables".into(), tables_name(tables)));
    rows.push(("partitions".into(), n.to_string()));
    for t in tables {
        let key = natural_key(t).map_or_else(|| "none".to_string(), |k| format!("({k})"));
        rows.push((format!("partition_key_{}", t.name), key));
        let column = match partition_column(t, Method::Hash) {
            Ok(c) => c.name.to_string(),
            Err(why) => format!("none: {why}"),
        };
        rows.push((format!("partition_column_{}", t.name), column));
    }
    for &m in methods {
        let (kept, left) = plan(m, tables);
        rows.push((format!("partition_{}_schema", m.name()), method_schema(schema, m)));
        rows.push((
            format!("partition_{}_tables", m.name()),
            kept.iter().map(|(t, _)| t.name).collect::<Vec<_>>().join(","),
        ));
        rows.push((
            format!("partition_{}_left_out", m.name()),
            left.iter()
                .map(|(t, why)| format!("{t}: {why}"))
                .collect::<Vec<_>>()
                .join("; "),
        ));
        if indexes.keys {
            for (t, c) in &kept {
                for key in unique_lost(t, c.name) {
                    rows.push((
                        format!("unique_lost_{}_{}", m.name(), key.name(t.name)),
                        format!(
                            "{} ({}): a plain index, since it does not hold {}",
                            t.name,
                            key.columns(),
                            c.name
                        ),
                    ));
                }
            }
        }
    }
    rows
}

/// A range read back from a CHECK or a partition bound: its two integers, the lower first.
pub fn read_range(text: &str, column: &str) -> Result<(i128, i128), String> {
    let stripped = text.replace(column, " ");
    let mut numbers = Vec::new();
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let starts = bytes[i].is_ascii_digit()
            || (bytes[i] == b'-' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit));
        if starts && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')) {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            numbers.push(stripped[i..j].parse::<i128>().map_err(|e| e.to_string())?);
            i = j;
        } else {
            i += 1;
        }
    }
    match numbers[..] {
        [lo, hi] if lo < hi => Ok((lo, hi)),
        _ => Err(format!("cannot read a range from `{text}`")),
    }
}

/// The ranges that overlap another, as `[lo, hi)` pairs.
pub fn overlaps(ranges: &[(i128, i128)]) -> Vec<((i128, i128), (i128, i128))> {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable();
    sorted
        .windows(2)
        .filter(|w| w[1].0 < w[0].1)
        .map(|w| (w[0], w[1]))
        .collect()
}

/// What the build did: each step's time, and the rows each partition holds, by schema and table.
pub struct Built {
    pub steps: Vec<(String, Duration)>,
    pub rows: Vec<(String, String, String, i64)>,
}

/// Builds every method's schema from the generated tables in `generated`, then checks that each
/// row lies in one partition.
#[allow(clippy::too_many_arguments)]
pub async fn build(
    conn: &mut PgConnection,
    generated: &str,
    methods: &BTreeSet<Method>,
    tables: &[&'static TableDef],
    n: u32,
    unlogged: bool,
    indexes: Indexes,
    extension_schemas: &ExtensionSchemas,
) -> Result<Built, String> {
    let mut built = Built {
        steps: Vec::new(),
        rows: Vec::new(),
    };
    for &m in methods {
        let started = Instant::now();
        let schema = method_schema(generated, m);
        exec(conn, &format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await?;
        exec(conn, &format!("CREATE SCHEMA {schema}")).await?;
        let (kept, left) = plan(m, tables);
        for (t, why) in &left {
            info!(schema = %schema, table = t, reason = %why, "left unpartitioned");
        }
        for (t, c) in &kept {
            let ranges = if m == Method::Hash {
                Vec::new()
            } else {
                let (min, max) = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(&format!(
                    "SELECT min({col})::bigint, max({col})::bigint FROM {generated}.{table}",
                    col = c.name,
                    table = t.name
                ))
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| format!("reading {generated}.{}'s range: {e}", t.name))?;
                equal_ranges(
                    i128::from(min.unwrap_or(0)),
                    i128::from(max.unwrap_or(0)),
                    n,
                )
            };
            let make =
                create_statements(m, &schema, generated, t.name, c.name, &ranges, n, unlogged);
            for sql in make {
                exec(conn, &sql).await?;
            }
            for sql in fill_statements(m, &schema, generated, t.name, &ranges) {
                exec(conn, &sql).await?;
            }
            let targets: Vec<String> = if m.declarative() {
                vec![t.name.to_string()]
            } else {
                (0..ranges.len()).map(|k| part_name(t.name, k)).collect()
            };
            let index = index_statements(
                &schema,
                generated,
                t,
                c.name,
                &targets,
                indexes,
                extension_schemas,
            );
            for sql in index {
                exec(conn, &sql).await?;
            }
            exec(conn, &format!("VACUUM (ANALYZE) {schema}.{}", t.name)).await?;
            if !m.declarative() {
                for child in &targets {
                    exec(conn, &format!("VACUUM (ANALYZE) {schema}.{child}")).await?;
                }
            }
        }
        built
            .steps
            .push((format!("{schema} build"), started.elapsed()));
        let started = Instant::now();
        let rows =
            check_each_row_in_one_partition_and_each_key_unique(conn, generated, &schema, m, &kept)
                .await?;
        built.steps.push((
            format!("{schema} each row in one partition, each key unique"),
            started.elapsed(),
        ));
        info!(
            schema = %schema,
            tables = kept.len(),
            "partitioned copies built, each row in one partition, each key unique"
        );
        built.rows.extend(rows);
    }
    Ok(built)
}

/// Whether a valid unique index on each of `relations` enforces a key over `columns`: an index
/// without a predicate or an expression, whose key columns are all among them.
async fn enforced(
    conn: &mut PgConnection,
    relations: &[String],
    columns: &str,
) -> Result<bool, String> {
    let columns: Vec<String> = columns.split(',').map(|c| c.trim().to_string()).collect();
    let unenforced: i64 = sqlx::query_as::<_, (i64,)>(
        "SELECT count(*) FROM unnest($1::text[]::regclass[]) r(rel) \
         WHERE NOT EXISTS (SELECT 1 FROM pg_index x \
           WHERE x.indrelid = r.rel AND x.indisunique AND x.indisvalid \
             AND x.indpred IS NULL AND x.indexprs IS NULL \
             AND ARRAY(SELECT a.attname::text FROM pg_attribute a \
                       WHERE a.attrelid = x.indrelid \
                         AND a.attnum = ANY ((x.indkey::int2[])[0:x.indnkeyatts - 1])) \
                 <@ $2::text[])",
    )
    .bind(relations)
    .bind(&columns)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| format!("reading the unique indexes of {}: {e}", relations.join(", ")))?
    .0;
    Ok(!relations.is_empty() && unenforced == 0)
}

/// The check that each row of a generated table lies in exactly one partition of its copy, and that
/// each key holding the partition column is unique across the copy: the partitions' rows add up to
/// the generated table's, the parent holds none of its own, no partition's range, read back from
/// the catalogue, overlaps another's, and no value of such a key appears twice: a valid unique
/// index enforces that where it is built, and elsewhere the rows are counted. Returns the rows each
/// partition holds, or every way the copies fail.
async fn check_each_row_in_one_partition_and_each_key_unique(
    conn: &mut PgConnection,
    generated: &str,
    schema: &str,
    method: Method,
    kept: &[(&'static TableDef, Column)],
) -> Result<Vec<(String, String, String, i64)>, String> {
    let mut broken = Vec::new();
    let mut rows = Vec::new();
    for (t, c) in kept {
        let table = t.name;
        let parts = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(&format!(
            "SELECT p.relname::text, pg_get_expr(p.relpartbound, p.oid), \
             (SELECT string_agg(pg_get_constraintdef(k.oid), ' AND ') FROM pg_constraint k \
              WHERE k.conrelid = p.oid AND k.contype = 'c') \
             FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhrelid \
             WHERE i.inhparent = '{schema}.{table}'::regclass ORDER BY p.relname"
        ))
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| format!("reading {schema}.{table}'s partitions: {e}"))?;
        if parts.is_empty() {
            broken.push(format!("{schema}.{table}: no partitions"));
            continue;
        }
        let want = count(conn, &format!("SELECT count(*) FROM {generated}.{table}")).await?;
        let own = count(conn, &format!("SELECT count(*) FROM ONLY {schema}.{table}")).await?;
        if own != 0 {
            broken.push(format!(
                "{schema}.{table}: the parent holds {own} rows of its own"
            ));
        }
        let mut held = 0;
        let mut ranges = Vec::new();
        for (part, bound, check) in &parts {
            let n = count(conn, &format!("SELECT count(*) FROM ONLY {schema}.{part}")).await?;
            held += n;
            rows.push((schema.to_string(), table.to_string(), part.clone(), n));
            let range = match (method, bound, check) {
                (Method::Range, Some(b), _) if b != "DEFAULT" => Some(b),
                (Method::Inheritance, _, Some(k)) => Some(k),
                _ => None,
            };
            if let Some(text) = range {
                match read_range(text, c.name) {
                    Ok(r) => ranges.push(r),
                    Err(e) => broken.push(format!("{schema}.{part}: {e}")),
                }
            }
        }
        if held != want {
            broken.push(format!(
                "{schema}.{table}: its partitions hold {held} rows, the generated table {want}"
            ));
        }
        for (a, b) in overlaps(&ranges) {
            broken.push(format!(
                "{schema}.{table}: the range [{}, {}) overlaps [{}, {})",
                a.0, a.1, b.0, b.1
            ));
        }
        // Where a valid unique index enforces a key, on the declarative parent or on every
        // inheritance child, the rows cannot repeat it; elsewhere they are counted.
        let holders: Vec<String> = if method.declarative() {
            vec![format!("{schema}.{table}")]
        } else {
            parts.iter().map(|(p, _, _)| format!("{schema}.{p}")).collect()
        };
        for key in keys(t).into_iter().filter(|k| k.holds(c.name)) {
            let columns = key.columns();
            if enforced(conn, &holders, columns).await? {
                continue;
            }
            let twice = count(
                conn,
                &format!(
                    "SELECT count(*) FROM (SELECT 1 FROM {schema}.{table} \
                     GROUP BY {columns} HAVING count(*) > 1) d"
                ),
            )
            .await?;
            if twice != 0 {
                broken.push(format!(
                    "{schema}.{table}: ({columns}) is not unique; values held by more than \
                     one row: {twice}"
                ));
            }
        }
    }
    if !broken.is_empty() {
        return Err(format!(
            "the {} partitions failed the check that each row lies in one partition and each key \
             is unique: {}",
            method.name(),
            broken.join("; ")
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::table;

    #[test]
    fn methods_and_tables_are_named_lists_and_any_other_word_is_refused() {
        assert_eq!(
            parse_methods("inheritance,range,hash"),
            Ok(Method::ALL.into_iter().collect())
        );
        assert_eq!(parse_methods("none"), Ok(BTreeSet::new()));
        assert_eq!(
            methods_name(&parse_methods(" hash , range ").unwrap()),
            "range,hash"
        );
        for bad in ["all", "list", "range,btree", ""] {
            assert!(parse_methods(bad).is_err(), "{bad} was accepted");
        }
        let all = parse_tables("all").unwrap();
        assert_eq!(all.len(), TABLES.len() - RUN_TABLES.len());
        assert!(all.iter().all(|t| !RUN_TABLES.contains(&t.name)));
        assert_eq!(tables_name(&all), "all");
        let two = parse_tables("lego_purchases, lego_sets").unwrap();
        assert_eq!(tables_name(&two), "lego_sets,lego_purchases");
        assert_eq!(tables_name(&parse_tables("generator_run").unwrap()), "generator_run");
        for bad in ["purchases", "lego_purchases,lego_oo", ""] {
            assert!(parse_tables(bad).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn a_method_schema_takes_the_generated_schema_name_and_the_method() {
        assert_eq!(method_schema("lego", Method::Range), "lego_range");
        assert_eq!(method_schema("Lego", Method::Hash), "lego_hash");
        assert_eq!(
            method_schema("\"Lego\"", Method::Inheritance),
            "\"Lego_inheritance\""
        );
    }

    #[test]
    fn a_table_is_partitioned_by_the_leading_column_of_its_natural_key() {
        let column = |name: &str, m: Method| partition_column(table(name), m).map(|c| c.name);
        for m in Method::ALL {
            assert_eq!(column("lego_purchases", m), Ok("purchase_id"));
            assert_eq!(column("lego_collection", m), Ok("builder_id"));
            assert_eq!(column("lego_inventory_parts", m), Ok("inventory_id"));
            assert_eq!(column("lego_inventory_sets", m), Ok("inventory_id"));
            let e = column("trap_manifest", m).unwrap_err();
            assert!(e.contains("no natural key"), "{e}");
        }
        assert_eq!(column("lego_postcodes", Method::Hash), Ok("code"));
        assert_eq!(column("lego_sets", Method::Hash), Ok("set_num"));
        for m in [Method::Range, Method::Inheritance] {
            for name in ["lego_postcodes", "lego_sets", "lego_parts"] {
                let e = column(name, m).unwrap_err();
                assert!(e.contains("text"), "{name}: {e}");
            }
        }
        let nullable = TableDef {
            name: "t",
            columns: "a, b",
            typed: "a integer NOT NULL, b numeric(10, 2)",
            key: Some("a, b"),
        };
        assert!(partition_column(&nullable, Method::Hash)
            .unwrap_err()
            .contains("b, which is nullable"));
        let expression = TableDef {
            key: Some("(a + 1)"),
            ..nullable
        };
        assert!(partition_column(&expression, Method::Hash)
            .unwrap_err()
            .contains("expression"));
    }

    #[test]
    fn every_natural_key_is_a_key_its_table_declares() {
        for (table_name, natural) in NATURAL_KEYS {
            let t = table(table_name);
            assert!(
                keys(t)
                    .iter()
                    .any(|k| column_set(k.columns()) == column_set(natural)),
                "{table_name}: ({natural}) is not a key it declares"
            );
        }
        for t in TABLES.iter() {
            if let Some(natural) = natural_key(t) {
                assert!(
                    keys(t)
                        .iter()
                        .any(|k| column_set(k.columns()) == column_set(natural)),
                    "{}",
                    t.name
                );
            }
        }
        assert_eq!(
            natural_key(table("lego_inventories")),
            Some("id"),
            "a table declaring one key takes it"
        );
    }

    #[test]
    fn every_natural_key_stays_unique_under_every_method() {
        let mut partitioned = 0;
        for t in TABLES.iter() {
            for m in Method::ALL {
                let Ok(c) = partition_column(t, m) else {
                    continue;
                };
                partitioned += 1;
                let natural = natural_key(t).expect("a natural key");
                let kept: Vec<Key> = keys(t).into_iter().filter(|k| k.holds(c.name)).collect();
                assert!(
                    kept.iter()
                        .any(|k| column_set(k.columns()) == column_set(natural)),
                    "{} under {}: its natural key ({natural}) is not kept unique",
                    t.name,
                    m.name()
                );
                assert!(unique_lost(t, c.name)
                    .iter()
                    .all(|k| column_set(k.columns()) != column_set(natural)));
            }
        }
        assert!(partitioned > 0);
    }

    #[test]
    fn equal_ranges_hold_every_value_from_the_minimum_to_the_maximum_once() {
        for (min, max, n) in [(-1, 9999, 8), (1, 13_679, 8), (5, 7, 8), (3, 3, 4), (0, 99, 1)] {
            let ranges = equal_ranges(min, max, n);
            assert_eq!(ranges.len(), n as usize);
            assert_eq!(ranges[0].0, min);
            assert!(ranges.last().unwrap().1 > max);
            let width = ranges[0].1 - ranges[0].0;
            for w in ranges.windows(2) {
                assert_eq!(w[0].1, w[1].0, "{min}..{max} in {n}");
            }
            assert!(ranges.iter().all(|(lo, hi)| hi - lo == width && width >= 1));
            assert!(overlaps(&ranges).is_empty());
        }
    }

    #[test]
    fn each_method_makes_its_own_kind_of_partitions() {
        let ranges = equal_ranges(1, 80, 4);
        let make = |m: Method, unlogged: bool| {
            create_statements(m, "s", "g", "t", "id", &ranges, 4, unlogged)
        };
        let inh = make(Method::Inheritance, true);
        assert_eq!(inh[0], "CREATE UNLOGGED TABLE s.t (LIKE g.t)");
        assert_eq!(
            inh[1],
            "CREATE TABLE s.t_load (LIKE g.t) PARTITION BY RANGE (id)"
        );
        assert_eq!(
            inh[2],
            "CREATE UNLOGGED TABLE s.t_p0 PARTITION OF s.t_load \
             (CONSTRAINT t_p0_check CHECK (id >= 1 AND id < 21)) FOR VALUES FROM (1) TO (21)"
        );
        assert_eq!(inh.len(), 6);
        let range = make(Method::Range, true);
        assert_eq!(range[0], "CREATE TABLE s.t (LIKE g.t) PARTITION BY RANGE (id)");
        assert!(range[1].starts_with("CREATE UNLOGGED TABLE s.t_p0 PARTITION OF s.t"));
        assert!(range[1].ends_with("FOR VALUES FROM (1) TO (21)"));
        assert_eq!(
            range.last().unwrap(),
            "CREATE UNLOGGED TABLE s.t_default PARTITION OF s.t DEFAULT"
        );
        let hash = make(Method::Hash, false);
        assert_eq!(hash[0], "CREATE TABLE s.t (LIKE g.t) PARTITION BY HASH (id)");
        assert_eq!(hash.len(), 5);
        assert!(hash[4].ends_with("FOR VALUES WITH (MODULUS 4, REMAINDER 3)"));
        let fill = fill_statements(Method::Inheritance, "s", "g", "t", &ranges);
        assert_eq!(fill[0], "INSERT INTO s.t_load SELECT * FROM g.t");
        assert_eq!(fill.iter().filter(|s| s.starts_with("INSERT")).count(), 1);
        assert_eq!(fill[7], "ALTER TABLE s.t_load DETACH PARTITION s.t_p3");
        assert_eq!(fill[8], "ALTER TABLE s.t_p3 INHERIT s.t");
        assert_eq!(fill.last().unwrap(), "DROP TABLE s.t_load");
        assert_eq!(
            fill_statements(Method::Hash, "s", "g", "t", &[]),
            ["INSERT INTO s.t SELECT * FROM g.t"]
        );
    }

    #[test]
    fn a_key_without_the_partition_column_is_built_plain_and_recorded() {
        let postcodes = table("lego_postcodes");
        let lost: Vec<String> = unique_lost(postcodes, "code")
            .iter()
            .map(|k| k.name(postcodes.name))
            .collect();
        assert_eq!(lost, ["lego_postcodes_pkey"]);
        let sql = |t: &TableDef, column: &str, target: &str| {
            index_statements(
                "s",
                "g",
                t,
                column,
                &[target.to_string()],
                Indexes::ALL,
                &ExtensionSchemas::new(),
            )
        };
        let p = sql(postcodes, "code", "lego_postcodes_p0");
        let has = |v: &[String], s: &str| v.iter().any(|x| x == s);
        assert!(has(
            &p,
            "ALTER TABLE s.lego_postcodes_p0 ADD CONSTRAINT lego_postcodes_p0_code_key \
             UNIQUE (code)"
        ));
        assert!(has(
            &p,
            "CREATE INDEX lego_postcodes_p0_postcode_id_idx ON s.lego_postcodes_p0 (postcode_id)"
        ));
        assert!(!p.iter().any(|s| s.contains("PRIMARY KEY")));
        let lines = sql(table("lego_inventory_parts"), "inventory_id", "lego_inventory_parts");
        assert!(has(
            &lines,
            "ALTER TABLE s.lego_inventory_parts ADD CONSTRAINT lego_inventory_parts_natural_key \
             UNIQUE (inventory_id, part_num, color_id, is_spare)"
        ));
        let methods: BTreeSet<Method> = Method::ALL.into_iter().collect();
        let tables = parse_tables("all").unwrap();
        let rows = run_rows("lego", &methods, &tables, 8, Indexes::ALL);
        let recorded: Vec<&str> = rows
            .iter()
            .filter(|(k, _)| k.starts_with("unique_lost_"))
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(recorded, ["unique_lost_hash_lego_postcodes_pkey"]);
        let no_keys = Indexes {
            keys: false,
            ..Indexes::ALL
        };
        assert!(!run_rows("lego", &methods, &tables, 8, no_keys)
            .iter()
            .any(|(k, _)| k.starts_with("unique_lost_")));
    }

    #[test]
    fn a_range_is_read_back_from_the_catalogue_and_an_overlap_is_found() {
        assert_eq!(
            read_range("CHECK (((id >= '-1'::integer) AND (id < 1250)))", "id"),
            Ok((-1, 1250))
        );
        assert_eq!(
            read_range("FOR VALUES FROM ('3000000000') TO ('3000000100')", "purchase_id"),
            Ok((3_000_000_000, 3_000_000_100))
        );
        assert_eq!(
            read_range("CHECK (((p2_id >= 7) AND (p2_id < 9)))", "p2_id"),
            Ok((7, 9))
        );
        assert!(read_range("FOR VALUES FROM (5) TO (5)", "id").is_err());
        assert!(read_range("DEFAULT", "id").is_err());
        assert!(overlaps(&[(0, 10), (10, 20)]).is_empty());
        assert_eq!(overlaps(&[(10, 21), (0, 11)]), [((0, 11), (10, 21))]);
    }
}
