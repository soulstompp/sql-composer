//! The object-oriented tables (`--oo-schema`): the generated sets, colours and builders, each split
//! into a hierarchy of tables with PostgreSQL's table inheritance, in a schema of their own. The
//! other tables are not split: read the classes with the generated schema behind them on the
//! `search_path`.
//!
//! Every row is written to the table of its most specific class, unchanged. A class's CHECK reads
//! the row's own columns only:
//! - a set by its theme, following the theme tree to its root theme;
//! - a colour by the hue its `rgb` names on the twelve-hue colour wheel (primary, secondary and
//!   tertiary colours), or as neutral when it has too little colour for a hue;
//! - a builder by the country of their home zone, with a class per zone where a country has more
//!   than one.
//!
//! A CHECK the child tables inherit says what the class and all its children hold. A class that
//! also has rows of its own leaves out its children's rows with a `NO INHERIT` CHECK. A table with
//! two parents is a row of both, and a query through either parent reads it once.
//!
//! As `--indexes` asks, every class has the generated table's primary key, and every class with
//! rows of its own each of the generated table's composite indexes, reading time through the
//! generated schema's `clock`.
//!
//! After the build, a placement check reads every class's CHECKs back from the catalogue:
//! - each hierarchy has exactly the generated table's rows;
//! - every row satisfying a class's CHECKs lies in that class or below it;
//! - every row of a class satisfies its CHECKs;
//! - a class meant to have no rows of its own has none.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use sqlx::PgConnection;
use tracing::info;

use crate::calendar::{ZONE_COUNTRIES, ZONE_NAMES};
use crate::load::{index_name, CompositeIndex, Indexes, COMPOSITE_INDEXES};

/// The root themes of licensed sets, by name.
pub const LICENSED: [&str; 10] = [
    "Star Wars",
    "Super Heroes",
    "Harry Potter",
    "The Hobbit and Lord of the Rings",
    "Indiana Jones",
    "Cars",
    "SpongeBob SquarePants",
    "Teenage Mutant Ninja Turtles",
    "Disney Princess",
    "Minecraft",
];
/// The root themes of the classic play themes, by name, each a class of its own.
pub const CLASSIC_PLAY: [&str; 4] = ["Town", "Space", "Castle", "Pirates"];
const TECHNIC: &str = "Technic";
const STAR_WARS: &str = "Star Wars";

/// The wheel's twelve hues, from red, a place each thirty degrees.
pub const HUES: [&str; 12] = [
    "red",
    "orange",
    "yellow",
    "chartreuse",
    "green",
    "spring_green",
    "cyan",
    "azure",
    "blue",
    "violet",
    "magenta",
    "rose",
];
/// A colour whose strongest and weakest channels differ by less than this is neutral.
pub const NEUTRAL_BELOW: u8 = 32;

/// Which hierarchies are built: the sets', the colours' and the builders'.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Classes {
    pub sets: bool,
    pub colors: bool,
    pub builders: bool,
}

impl Classes {
    pub const ALL: Classes = Classes {
        sets: true,
        colors: true,
        builders: true,
    };
    pub const NONE: Classes = Classes {
        sets: false,
        colors: false,
        builders: false,
    };

    /// `none`, or a comma-separated list of `sets`, `colors` and `builders`.
    pub fn parse(s: &str) -> Result<Classes, String> {
        let mut out = Classes::NONE;
        if s.trim() == "none" {
            return Ok(out);
        }
        for item in s.split(',') {
            match item.trim() {
                "sets" => out.sets = true,
                "colors" => out.colors = true,
                "builders" => out.builders = true,
                other => {
                    return Err(format!(
                        "--classes: want none, or a list of sets, colors and builders, \
                         not `{other}`"
                    ))
                }
            }
        }
        Ok(out)
    }

    /// The hierarchies `--classes` and `--oo-schema` ask for: `--classes` when given, else all
    /// three with `--oo-schema` and none without. Refused when `--classes` names a hierarchy and
    /// no schema is given to build it in, and when it names none and a schema is given, which
    /// would keep an earlier load's rows there.
    pub fn resolve(classes: Option<&str>, oo_schema: Option<&str>) -> Result<Classes, String> {
        let resolved = match (classes, oo_schema) {
            (Some(c), _) => Classes::parse(c)?,
            (None, Some(_)) => Classes::ALL,
            (None, None) => Classes::NONE,
        };
        if resolved.any() && oo_schema.is_none() {
            return Err(format!(
                "--classes {} needs --oo-schema, the schema the classes are built in",
                resolved.name()
            ));
        }
        if let (false, Some(oo)) = (resolved.any(), oo_schema) {
            return Err(format!(
                "--classes none with --oo-schema {oo}: {oo} would keep the classes of an earlier \
                 load. Name the classes to build, or leave out --oo-schema"
            ));
        }
        Ok(resolved)
    }

    pub fn any(&self) -> bool {
        self.sets || self.colors || self.builders
    }

    /// The list as `--classes` takes it.
    pub fn name(&self) -> String {
        let names: Vec<&str> = [
            ("sets", self.sets),
            ("colors", self.colors),
            ("builders", self.builders),
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

/// What a class's CHECK tests.
#[derive(Clone, Debug)]
pub enum Test {
    /// The set's theme is one of these; a set with no theme is not.
    ThemeIn(BTreeSet<i32>),
    /// The set's theme is none of these; a set with no theme passes.
    ThemeNotIn(BTreeSet<i32>),
    /// Any other condition on the row's own columns.
    Sql(String),
}

impl Test {
    fn sql(&self) -> String {
        fn list(s: &BTreeSet<i32>) -> String {
            s.iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        }
        match self {
            Test::ThemeIn(s) if s.is_empty() => "false".into(),
            Test::ThemeIn(s) => format!("theme_id IN ({})", list(s)),
            Test::ThemeNotIn(s) if s.is_empty() => "true".into(),
            Test::ThemeNotIn(s) => format!("(theme_id IS NULL OR theme_id NOT IN ({}))", list(s)),
            Test::Sql(s) => s.clone(),
        }
    }

    /// Whether a set with this theme passes, where the test reads the theme.
    #[cfg(test)]
    fn holds_for(&self, theme: Option<i32>) -> bool {
        match (self, theme) {
            (Test::ThemeIn(_), None) => false,
            (Test::ThemeIn(s), Some(t)) => s.contains(&t),
            (Test::ThemeNotIn(_), None) => true,
            (Test::ThemeNotIn(s), Some(t)) => !s.contains(&t),
            (Test::Sql(s), _) => panic!("not a test of the theme: {s}"),
        }
    }
}

/// One class: its table, the tables it inherits from, the CHECK its children inherit, the
/// `NO INHERIT` CHECK on the rows of the table itself, and whether rows are written to it.
#[derive(Clone, Debug)]
pub struct Class {
    pub name: String,
    pub parents: Vec<String>,
    pub check: Option<Test>,
    pub no_inherit: Option<Test>,
    pub has_rows: bool,
}

fn class(
    name: &str,
    parents: &[&str],
    check: Option<Test>,
    no_inherit: Option<Test>,
    has_rows: bool,
) -> Class {
    Class {
        name: name.into(),
        parents: parents.iter().map(|p| p.to_string()).collect(),
        check,
        no_inherit,
        has_rows,
    }
}

/// A generated table decomposed into classes, the root first and every parent before its children.
#[derive(Clone, Debug)]
pub struct Hierarchy {
    /// The generated table, which also names the root class.
    pub table: &'static str,
    /// Its primary key, given to every class.
    pub key: &'static str,
    pub classes: Vec<Class>,
}

impl Hierarchy {
    fn get(&self, name: &str) -> &Class {
        self.classes
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no class {name}"))
    }

    /// The tests a row written to `name` itself must pass: its CHECKs and every ancestor's
    /// inherited one.
    pub fn tests(&self, name: &str) -> Vec<&Test> {
        let mut seen = BTreeSet::new();
        let mut todo = vec![name.to_string()];
        let mut out = Vec::new();
        let no_inherit = self.get(name).no_inherit.as_ref();
        while let Some(n) = todo.pop() {
            if !seen.insert(n.clone()) {
                continue;
            }
            let c = self.get(&n);
            out.extend(c.check.as_ref());
            todo.extend(c.parents.iter().cloned());
        }
        out.extend(no_inherit);
        out
    }

    /// The composite indexes class `c` has: every composite index of the generated table on a
    /// class with rows of its own, and none on a class without.
    pub fn composite_indexes(&self, c: &Class) -> Vec<&'static CompositeIndex> {
        if !c.has_rows {
            return Vec::new();
        }
        COMPOSITE_INDEXES
            .iter()
            .filter(|s| s.table == self.table)
            .collect()
    }

    fn predicate(&self, name: &str) -> String {
        let tests = self.tests(name);
        if tests.is_empty() {
            return "true".into();
        }
        tests
            .iter()
            .map(|t| format!("({})", t.sql()))
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

/// The sets' classes, from the theme tree `(id, name, parent_id)`. Refused when a named root theme
/// is missing.
pub fn sets(themes: &[(i32, String, Option<i32>)]) -> Result<Hierarchy, String> {
    let parent: BTreeMap<i32, Option<i32>> = themes.iter().map(|(i, _, p)| (*i, *p)).collect();
    let root_of = |mut t: i32| -> Option<i32> {
        for _ in 0..=themes.len() {
            match parent.get(&t)? {
                None => return Some(t),
                Some(p) => t = *p,
            }
        }
        None
    };
    let roots: BTreeMap<&str, i32> = themes
        .iter()
        .filter(|(_, _, p)| p.is_none())
        .map(|(i, n, _)| (n.as_str(), *i))
        .collect();
    let named: Vec<&str> = LICENSED
        .iter()
        .chain(CLASSIC_PLAY.iter())
        .chain([TECHNIC].iter())
        .copied()
        .collect();
    let missing: Vec<&str> = named
        .iter()
        .copied()
        .filter(|n| !roots.contains_key(n))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "the theme tree has no root theme named {}",
            missing.join(", ")
        ));
    }
    let under = |names: &[&str]| -> BTreeSet<i32> {
        let ids: BTreeSet<i32> = names.iter().map(|n| roots[n]).collect();
        themes
            .iter()
            .map(|(i, _, _)| *i)
            .filter(|&i| root_of(i).is_some_and(|r| ids.contains(&r)))
            .collect()
    };
    // every theme named Star Wars, and every theme below one
    let star_wars: BTreeSet<i32> = themes
        .iter()
        .map(|(i, _, _)| *i)
        .filter(|&i| {
            let mut t = Some(i);
            for _ in 0..=themes.len() {
                let Some(id) = t else { return false };
                if themes.iter().any(|(j, n, _)| *j == id && n == STAR_WARS) {
                    return true;
                }
                t = parent.get(&id).copied().flatten();
            }
            false
        })
        .collect();
    let licensed = under(&LICENSED);
    let star_wars_root = under(&[STAR_WARS]);
    let classic = under(&CLASSIC_PLAY);
    let technic = under(&[TECHNIC]);
    let technic_star_wars: BTreeSet<i32> = technic.intersection(&star_wars).copied().collect();
    let elsewhere: BTreeSet<i32> = star_wars
        .iter()
        .copied()
        .filter(|t| !licensed.contains(t) && !technic.contains(t) && !classic.contains(t))
        .collect();
    let in_house_not_own: BTreeSet<i32> = classic
        .iter()
        .chain(technic.iter())
        .chain(elsewhere.iter())
        .copied()
        .collect();

    let mut classes = vec![
        class("lego_sets", &[], None, None, false),
        class(
            "lego_sets_licensed",
            &["lego_sets"],
            Some(Test::ThemeIn(licensed.clone())),
            Some(Test::ThemeNotIn(star_wars_root.clone())),
            true,
        ),
        class(
            "lego_sets_star_wars_all",
            &["lego_sets"],
            Some(Test::ThemeIn(star_wars)),
            None,
            false,
        ),
        class(
            "lego_sets_star_wars",
            &["lego_sets_licensed", "lego_sets_star_wars_all"],
            Some(Test::ThemeIn(star_wars_root)),
            None,
            true,
        ),
        class(
            "lego_sets_in_house",
            &["lego_sets"],
            Some(Test::ThemeNotIn(licensed)),
            Some(Test::ThemeNotIn(in_house_not_own)),
            true,
        ),
        class(
            "lego_sets_classic_play",
            &["lego_sets_in_house"],
            Some(Test::ThemeIn(classic)),
            None,
            false,
        ),
    ];
    for name in CLASSIC_PLAY {
        classes.push(class(
            &format!("lego_sets_{}", name.to_ascii_lowercase()),
            &["lego_sets_classic_play"],
            Some(Test::ThemeIn(under(&[name]))),
            None,
            true,
        ));
    }
    classes.extend([
        class(
            "lego_sets_technic",
            &["lego_sets_in_house"],
            Some(Test::ThemeIn(technic)),
            Some(Test::ThemeNotIn(technic_star_wars.clone())),
            true,
        ),
        class(
            "lego_sets_technic_star_wars",
            &["lego_sets_technic", "lego_sets_star_wars_all"],
            Some(Test::ThemeIn(technic_star_wars)),
            None,
            true,
        ),
        class(
            "lego_sets_star_wars_elsewhere",
            &["lego_sets_in_house", "lego_sets_star_wars_all"],
            Some(Test::ThemeIn(elsewhere)),
            None,
            true,
        ),
    ]);
    Ok(Hierarchy {
        table: "lego_sets",
        key: "set_num",
        classes,
    })
}

/// The colours' classes: neutral, and the primary, secondary and tertiary hues of the wheel, each
/// read by `<schema>.colour_wheel(rgb)`.
pub fn colours(schema: &str) -> Hierarchy {
    let wheel = format!("{schema}.colour_wheel(rgb)");
    let mut classes = vec![
        class("lego_colors", &[], None, None, false),
        class(
            "lego_colors_neutral",
            &["lego_colors"],
            Some(Test::Sql(format!("{wheel} IS NULL"))),
            None,
            true,
        ),
    ];
    // the places as a list, which the planner can compare with a query's own places
    for (group, places) in [
        ("primary", vec![0, 4, 8]),
        ("secondary", vec![2, 6, 10]),
        ("tertiary", vec![1, 3, 5, 7, 9, 11]),
    ] {
        let parent = format!("lego_colors_{group}");
        let list = places
            .iter()
            .map(|p: &usize| p.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        classes.push(class(
            &parent,
            &["lego_colors"],
            Some(Test::Sql(format!("{wheel} IN ({list})"))),
            None,
            false,
        ));
        for place in places {
            classes.push(class(
                &format!("lego_colors_{}", HUES[place]),
                &[&parent],
                Some(Test::Sql(format!("{wheel} = {place}"))),
                None,
                true,
            ));
        }
    }
    Hierarchy {
        table: "lego_colors",
        key: "id",
        classes,
    }
}

/// The builders' classes: a class per country of the home zones, and a class per zone under a
/// country that holds more than one. A builder's class is read off their street: `streets` gives
/// each zone's lowest and highest street id, and the streets of a zone, and of a country, are one
/// run of ids.
pub fn builders(streets: &BTreeMap<String, (i32, i32)>) -> Hierarchy {
    let mut by_country: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for zone in ZONE_NAMES {
        let country = ZONE_COUNTRIES
            .iter()
            .find(|(z, _)| *z == zone)
            .map(|(_, c)| *c)
            .unwrap_or_else(|| panic!("{zone} has no country"));
        by_country.entry(country).or_default().push(zone);
    }
    let run = |zones: &[&str]| {
        let ids: Vec<(i32, i32)> = zones
            .iter()
            .map(|z| {
                *streets
                    .get(*z)
                    .unwrap_or_else(|| panic!("{z} has no streets"))
            })
            .collect();
        let lo = ids.iter().map(|r| r.0).min().expect("a zone");
        let hi = ids.iter().map(|r| r.1).max().expect("a zone");
        format!("street_id BETWEEN {lo} AND {hi}")
    };
    let mut classes = vec![class("lego_builders", &[], None, None, false)];
    for (country, zones) in &by_country {
        let name = format!("lego_builders_{}", country.to_ascii_lowercase());
        let one = zones.len() == 1;
        classes.push(class(
            &name,
            &["lego_builders"],
            Some(Test::Sql(run(zones))),
            None,
            one,
        ));
        if !one {
            for zone in zones {
                let city = zone.rsplit('/').next().unwrap_or(zone).to_ascii_lowercase();
                classes.push(class(
                    &format!("{name}_{city}"),
                    &[&name],
                    Some(Test::Sql(run(&[zone]))),
                    None,
                    true,
                ));
            }
        }
    }
    Hierarchy {
        table: "lego_builders",
        key: "builder_id",
        classes,
    }
}

/// The wheel place of an `rgb` colour: the hue, rounded to the nearest thirty degrees, counted from
/// red; NULL for a neutral colour or a value that is not six hexadecimal digits.
fn colour_wheel(schema: &str) -> String {
    format!(
        "CREATE FUNCTION {schema}.colour_wheel(rgb text) RETURNS integer
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $wheel$
DECLARE
    r integer; g integer; b integer; hi integer; lo integer; hue numeric;
BEGIN
    IF rgb !~ '^[0-9A-Fa-f]{{6}}$' THEN
        RETURN NULL;
    END IF;
    r := ('x' || substr(rgb, 1, 2))::bit(8)::integer;
    g := ('x' || substr(rgb, 3, 2))::bit(8)::integer;
    b := ('x' || substr(rgb, 5, 2))::bit(8)::integer;
    hi := greatest(r, g, b);
    lo := least(r, g, b);
    IF hi - lo < {NEUTRAL_BELOW} THEN
        RETURN NULL;
    END IF;
    IF hi = r THEN
        hue := mod(60.0 * (g - b) / (hi - lo) + 360, 360);
    ELSIF hi = g THEN
        hue := 60.0 * (b - r) / (hi - lo) + 120;
    ELSE
        hue := 60.0 * (r - g) / (hi - lo) + 240;
    END IF;
    RETURN mod(round(hue / 30)::integer, 12);
END
$wheel$"
    )
}

/// What the build did: each step's time, and the rows each class has of its own.
pub struct Built {
    pub steps: Vec<(String, Duration)>,
    pub rows: Vec<(String, i64)>,
}

pub(crate) async fn exec(conn: &mut PgConnection, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(&mut *conn)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}: {sql}"))
}

pub(crate) async fn count(conn: &mut PgConnection, sql: &str) -> Result<i64, String> {
    sqlx::query_as::<_, (i64,)>(sql)
        .fetch_one(&mut *conn)
        .await
        .map(|r| r.0)
        .map_err(|e| format!("{e}: {sql}"))
}

/// Builds the hierarchies `classes` names in `oo` from the generated tables in `generated`, each
/// class with the primary key and the composite indexes `indexes` asks for, then runs the placement
/// check on them.
pub async fn build(
    conn: &mut PgConnection,
    generated: &str,
    oo: &str,
    classes: Classes,
    indexes: Indexes,
) -> Result<Built, String> {
    let mut steps = Vec::new();
    let started = Instant::now();
    exec(conn, &format!("DROP SCHEMA IF EXISTS {oo} CASCADE")).await?;
    exec(conn, &format!("CREATE SCHEMA {oo}")).await?;
    let mut hierarchies = Vec::new();
    if classes.sets {
        let themes = sqlx::query_as::<_, (i32, String, Option<i32>)>(&format!(
            "SELECT id, name, parent_id FROM {generated}.lego_themes"
        ))
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| format!("reading the themes: {e}"))?;
        hierarchies.push(sets(&themes)?);
    }
    if classes.colors {
        exec(conn, &colour_wheel(oo)).await?;
        hierarchies.push(colours(oo));
    }
    if classes.builders {
        let streets: BTreeMap<String, (i32, i32)> =
            sqlx::query_as::<_, (String, i32, i32)>(&format!(
                "SELECT c.zone, min(s.street_id), max(s.street_id) \
                 FROM {generated}.lego_streets s \
                 JOIN {generated}.lego_postcodes p ON p.postcode_id = s.postcode_id \
                 JOIN {generated}.lego_cities c ON c.city_id = p.city_id \
                 GROUP BY c.zone"
            ))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| format!("reading the streets: {e}"))?
            .into_iter()
            .map(|(zone, lo, hi)| (zone, (lo, hi)))
            .collect();
        hierarchies.push(builders(&streets));
    }

    for h in &hierarchies {
        for c in &h.classes {
            let create = if c.parents.is_empty() {
                format!(
                    "CREATE TABLE {oo}.{} (LIKE {generated}.{})",
                    c.name, h.table
                )
            } else {
                let mut checks = Vec::new();
                if let Some(t) = &c.check {
                    checks.push(format!("CONSTRAINT {}_check CHECK ({})", c.name, t.sql()));
                }
                if let Some(t) = &c.no_inherit {
                    checks.push(format!(
                        "CONSTRAINT {}_no_inherit_check CHECK ({}) NO INHERIT",
                        c.name,
                        t.sql()
                    ));
                }
                let parents = c
                    .parents
                    .iter()
                    .map(|p| format!("{oo}.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "CREATE TABLE {oo}.{} ({}) INHERITS ({parents})",
                    c.name,
                    checks.join(", ")
                )
            };
            exec(conn, &create).await?;
        }
        for c in h.classes.iter().filter(|c| c.has_rows) {
            exec(
                conn,
                &format!(
                    "INSERT INTO {oo}.{} SELECT * FROM {generated}.{} WHERE ({}) IS TRUE",
                    c.name,
                    h.table,
                    h.predicate(&c.name)
                ),
            )
            .await?;
        }
        for c in &h.classes {
            if indexes.keys {
                exec(
                    conn,
                    &format!("ALTER TABLE {oo}.{} ADD PRIMARY KEY ({})", c.name, h.key),
                )
                .await?;
            }
            for ix in h
                .composite_indexes(c)
                .into_iter()
                .filter(|_| indexes.composite)
            {
                let on = format!("{oo}.{}", c.name);
                exec(
                    conn,
                    &ix.create_sql(&index_name(&c.name, ix.parts), &on, generated),
                )
                .await?;
            }
            exec(conn, &format!("VACUUM (ANALYZE) {oo}.{}", c.name)).await?;
        }
    }
    steps.push(("oo build".to_string(), started.elapsed()));

    let started = Instant::now();
    let rows = check_placement(conn, generated, oo, &hierarchies).await?;
    steps.push(("oo placement check".to_string(), started.elapsed()));
    info!(
        schema = oo,
        classes = rows.len(),
        "object-oriented tables built, every row in its place"
    );
    Ok(Built { steps, rows })
}

/// The placement check: each hierarchy has exactly the generated table's rows, and each row lies
/// in the class its CHECKs, read back from the catalogue, name. Returns the rows each class has of
/// its own, or every way the classes fail.
async fn check_placement(
    conn: &mut PgConnection,
    generated: &str,
    oo: &str,
    hierarchies: &[Hierarchy],
) -> Result<Vec<(String, i64)>, String> {
    let mut broken = Vec::new();
    let mut rows = Vec::new();
    for h in hierarchies {
        let table = h.table;
        let extra = count(
            conn,
            &format!(
                "SELECT count(*) FROM (TABLE {oo}.{table} EXCEPT ALL TABLE {generated}.{table}) d"
            ),
        )
        .await?;
        let lost = count(
            conn,
            &format!(
                "SELECT count(*) FROM (TABLE {generated}.{table} EXCEPT ALL TABLE {oo}.{table}) d"
            ),
        )
        .await?;
        if extra != 0 || lost != 0 {
            broken.push(format!(
                "{oo}.{table}: {extra} rows the generated table does not hold, {lost} it lacks"
            ));
        }
        for c in &h.classes {
            let name = &c.name;
            // the class's CHECKs as the catalogue holds them, its own and its inherited ones
            let from_catalogue = sqlx::query_as::<_, (Option<String>,)>(&format!(
                "SELECT string_agg('(' || pg_get_expr(conbin, conrelid) || ') IS TRUE', ' AND ' ORDER BY conname)
                 FROM pg_constraint WHERE conrelid = '{oo}.{name}'::regclass AND contype = 'c'"
            ))
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| format!("reading {oo}.{name}'s checks: {e}"))?
            .0
            .unwrap_or_else(|| "true".into());
            let outside = count(
                conn,
                &format!(
                    "SELECT count(*) FROM (SELECT * FROM {oo}.{table} WHERE {from_catalogue} \
                     EXCEPT ALL SELECT * FROM {oo}.{name}) d"
                ),
            )
            .await?;
            if outside != 0 {
                broken.push(format!(
                    "{oo}.{name}: {outside} rows satisfy its checks and lie outside it"
                ));
            }
            let failing = count(
                conn,
                &format!("SELECT count(*) FROM ONLY {oo}.{name} WHERE NOT ({from_catalogue})"),
            )
            .await?;
            if failing != 0 {
                broken.push(format!(
                    "{oo}.{name}: {failing} of its rows do not satisfy its checks"
                ));
            }
            let only = count(conn, &format!("SELECT count(*) FROM ONLY {oo}.{name}")).await?;
            if !c.has_rows && only != 0 {
                broken.push(format!(
                    "{oo}.{name}: has {only} rows of its own, and is meant to have none"
                ));
            }
            rows.push((name.clone(), only));
        }
    }
    if !broken.is_empty() {
        return Err(format!(
            "the object-oriented tables failed the placement check: {}",
            broken.join("; ")
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every named root theme, Star Wars themes under Technic and under another in-house root, and
    /// a theme below a classic play root.
    fn themes() -> Vec<(i32, String, Option<i32>)> {
        let mut t: Vec<(i32, String, Option<i32>)> = Vec::new();
        let mut id = 1;
        for name in LICENSED
            .iter()
            .chain(CLASSIC_PLAY.iter())
            .chain([TECHNIC, "Seasonal", "Classic"].iter())
        {
            t.push((id, name.to_string(), None));
            id += 1;
        }
        let root = |t: &[(i32, String, Option<i32>)], n: &str| {
            t.iter().find(|(_, m, p)| m == n && p.is_none()).unwrap().0
        };
        let sw = root(&t, "Star Wars");
        let technic = root(&t, TECHNIC);
        let seasonal = root(&t, "Seasonal");
        let town = root(&t, "Town");
        for (name, parent) in [
            ("Episode IV-VI", sw),
            ("Star Wars", technic),
            ("Star Wars", seasonal),
            ("City", town),
            ("Bionicle", technic),
        ] {
            t.push((id, name.into(), Some(parent)));
            id += 1;
        }
        let technic_sw = t
            .iter()
            .find(|(_, n, p)| n == "Star Wars" && *p == Some(technic))
            .unwrap()
            .0;
        t.push((id, "Snowspeeder".into(), Some(technic_sw)));
        t
    }

    #[test]
    fn every_set_lands_in_exactly_one_class_with_rows() {
        let t = themes();
        let h = sets(&t).unwrap();
        let unknown = t.iter().map(|(i, _, _)| *i).max().unwrap() + 100;
        let cases: Vec<Option<i32>> = t
            .iter()
            .map(|(i, _, _)| Some(*i))
            .chain([None, Some(unknown)])
            .collect();
        for theme in cases {
            let homes: Vec<&str> = h
                .classes
                .iter()
                .filter(|c| c.has_rows)
                .filter(|c| h.tests(&c.name).iter().all(|test| test.holds_for(theme)))
                .map(|c| c.name.as_str())
                .collect();
            assert_eq!(homes.len(), 1, "theme {theme:?} lands in {homes:?}");
        }
    }

    #[test]
    fn a_set_lands_in_the_class_its_theme_names() {
        let t = themes();
        let h = sets(&t).unwrap();
        let id = |name: &str, parent: Option<&str>| {
            let p = parent.map(|p| t.iter().find(|(_, n, q)| n == p && q.is_none()).unwrap().0);
            t.iter().find(|(_, n, q)| n == name && *q == p).unwrap().0
        };
        let home = |theme: Option<i32>| {
            h.classes
                .iter()
                .filter(|c| c.has_rows && h.tests(&c.name).iter().all(|test| test.holds_for(theme)))
                .map(|c| c.name.clone())
                .next()
                .unwrap()
        };
        assert_eq!(
            home(Some(id("Episode IV-VI", Some("Star Wars")))),
            "lego_sets_star_wars"
        );
        assert_eq!(home(Some(id("Harry Potter", None))), "lego_sets_licensed");
        assert_eq!(home(Some(id("City", Some("Town")))), "lego_sets_town");
        assert_eq!(
            home(Some(id("Star Wars", Some(TECHNIC)))),
            "lego_sets_technic_star_wars"
        );
        assert_eq!(
            home(Some(id("Bionicle", Some(TECHNIC)))),
            "lego_sets_technic"
        );
        assert_eq!(
            home(Some(id("Star Wars", Some("Seasonal")))),
            "lego_sets_star_wars_elsewhere"
        );
        assert_eq!(home(Some(id("Classic", None))), "lego_sets_in_house");
        assert_eq!(home(None), "lego_sets_in_house");
    }

    #[test]
    fn a_theme_tree_without_a_named_root_is_refused() {
        let t: Vec<_> = themes()
            .into_iter()
            .filter(|(_, n, _)| n != "Pirates")
            .collect();
        let e = sets(&t).unwrap_err();
        assert!(e.contains("Pirates"), "{e}");
    }

    /// Each zone's lowest and highest street id, from the places a small world draws.
    fn streets() -> BTreeMap<String, (i32, i32)> {
        let places = crate::places::Places::new(7, 4000);
        ZONE_NAMES
            .iter()
            .enumerate()
            .map(|(z, name)| (name.to_string(), places.street_ids(&[z])))
            .collect()
    }

    #[test]
    fn every_home_zone_has_a_country_and_a_country_of_two_zones_has_a_class_per_zone() {
        let runs = streets();
        let h = builders(&runs);
        let range = |t: &Test| -> (i32, i32) {
            let Test::Sql(s) = t else { panic!("{t:?}") };
            let n: Vec<i32> = s.split(' ').filter_map(|w| w.parse().ok()).collect();
            (n[0], n[1])
        };
        for (zone, (lo, hi)) in &runs {
            let homes: Vec<&str> = h
                .classes
                .iter()
                .filter(|c| c.has_rows)
                .filter(|c| {
                    h.tests(&c.name).iter().all(|t| {
                        let (a, b) = range(t);
                        a <= *lo && *hi <= b
                    })
                })
                .map(|c| c.name.as_str())
                .collect();
            assert_eq!(homes.len(), 1, "{zone} lands in {homes:?}");
        }
        assert!(h
            .classes
            .iter()
            .any(|c| c.name == "lego_builders_us_new_york" && c.has_rows));
        assert!(h
            .classes
            .iter()
            .any(|c| c.name == "lego_builders_us" && !c.has_rows));
    }

    #[test]
    fn the_colour_classes_cover_the_wheel_once() {
        let h = colours("oo");
        let leaves: Vec<&Class> = h.classes.iter().filter(|c| c.has_rows).collect();
        assert_eq!(leaves.len(), 13);
        for (place, hue) in HUES.iter().enumerate() {
            let named: Vec<&str> = leaves
                .iter()
                .filter(|c| matches!(&c.check, Some(Test::Sql(s)) if s.ends_with(&format!("= {place}"))))
                .map(|c| c.name.as_str())
                .collect();
            assert_eq!(named, vec![format!("lego_colors_{hue}").as_str()]);
        }
    }

    /// A class with rows of its own has every composite index of its table, and a class without has
    /// none. The index names are unique within each schema, and fit the server's limit.
    #[test]
    fn a_class_with_rows_has_every_composite_index_of_its_table() {
        use crate::load::NAME_BYTES;
        let generated: BTreeSet<String> = COMPOSITE_INDEXES
            .iter()
            .map(|s| index_name(s.table, s.parts))
            .collect();
        assert_eq!(generated.len(), COMPOSITE_INDEXES.len());
        assert!(generated.iter().all(|n| n.len() <= NAME_BYTES));
        let mut names = BTreeSet::new();
        let mut built = 0;
        for h in [
            sets(&themes()).unwrap(),
            colours("oo"),
            builders(&streets()),
        ] {
            let of_table: Vec<&CompositeIndex> = COMPOSITE_INDEXES
                .iter()
                .filter(|s| s.table == h.table)
                .collect();
            for c in &h.classes {
                let want = if c.has_rows {
                    of_table.clone()
                } else {
                    Vec::new()
                };
                assert_eq!(h.composite_indexes(c), want, "{}", c.name);
                for s in want {
                    let n = index_name(&c.name, s.parts);
                    assert!(n.len() <= NAME_BYTES, "{n}");
                    assert!(names.insert(n.clone()), "{n} twice");
                    built += 1;
                }
            }
        }
        // The sets' table has more than one composite index, so a class with only one shows here.
        assert!(
            COMPOSITE_INDEXES
                .iter()
                .filter(|s| s.table == "lego_sets")
                .count()
                > 1
        );
        assert!(built > 0);
    }

    #[test]
    fn classes_are_none_or_a_list_and_any_other_word_is_refused() {
        assert_eq!(Classes::parse("sets,colors,builders"), Ok(Classes::ALL));
        assert_eq!(Classes::parse("none"), Ok(Classes::NONE));
        assert_eq!(
            Classes::parse(" builders , sets ").unwrap().name(),
            "sets,builders"
        );
        for bad in ["all", "colours", "sets,themes", ""] {
            assert!(Classes::parse(bad).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn the_classes_follow_the_schema_and_a_mismatch_either_way_is_refused() {
        assert_eq!(Classes::resolve(None, Some("oo")), Ok(Classes::ALL));
        assert_eq!(Classes::resolve(None, None), Ok(Classes::NONE));
        assert_eq!(Classes::resolve(Some("none"), None), Ok(Classes::NONE));
        let e = Classes::resolve(Some("none"), Some("oo")).unwrap_err();
        assert!(e.contains("Name the classes"), "{e}");
        assert_eq!(
            Classes::resolve(Some("colors"), Some("oo")).unwrap().name(),
            "colors"
        );
        let e = Classes::resolve(Some("sets"), None).unwrap_err();
        assert!(e.contains("--oo-schema"), "{e}");
    }
}
