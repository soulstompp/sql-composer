//! Where the builders live: real cities, and the postcodes, streets and homes drawn in them.
//!
//! The cities are `data/cities.tsv`: Natural Earth's populated places (public domain) in the home
//! zones, at their real coordinates. A city is a square of its urban core's area, centred on its
//! coordinates and cut into a grid of postcode districts, one for about every `DISTRICT_PEOPLE`
//! people, so the postcodes are the same whatever the number of builders. A postcode's leading
//! characters follow its country's own scheme, so a code says where it is; the rest is numbered in
//! order. A district's streets run north–south or east–west inside it, under made-up names, as
//! many as its builders need, and a builder's home stands beside their street, odd numbers on one
//! side and even numbers on the other.
//!
//! Every point a street or a home holds is the centre of the thousandth-of-a-degree cell it falls
//! in, so it names a block and never one house, and no street name is real, so no row is anybody's
//! address. Points are held in microdegrees.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::OnceLock;

use crate::calendar::ZONE_NAMES;
use crate::demand::Alias;
use crate::rng::{Purpose, Rng};

const CITIES: &str = include_str!("../data/cities.tsv");

/// The people a postcode district holds, about.
const DISTRICT_PEOPLE: f64 = 25_000.0;
/// The builders a street is drawn for, on average.
const HOMES_PER_STREET: f64 = 10.0;
/// The most streets one district holds.
const MAX_STREETS: i64 = 50;
/// The narrowest district, in kilometres.
const DISTRICT_MIN_KM: f64 = 0.5;
/// People per square kilometre, for a city whose urban core has no area in the source.
const DENSITY: f64 = 2500.0;
/// The narrowest and widest city, in kilometres.
const CITY_KM: (f64, f64) = (1.0, 80.0);
const KM_PER_DEGREE: f64 = 111.32;
/// How far a home stands back from its street, in kilometres.
const SETBACK_KM: (f64, f64) = (0.015, 0.035);

/// A real city, as the source gives it.
#[derive(Clone, Debug)]
pub struct City {
    pub id: i32,
    pub country: &'static str,
    /// The home zone, by its index in `ZONE_NAMES`.
    pub zone: usize,
    pub name: &'static str,
    pub region: &'static str,
    pub latitude: i32,
    pub longitude: i32,
    pub population: i32,
    side_km: f64,
}

/// A postcode district of a city.
#[derive(Clone, Debug)]
pub struct Postcode {
    pub id: i32,
    /// The city, by its index in `Places::cities`.
    pub city: usize,
    pub code: String,
}

/// A street of a district: from one end to the other, north–south or east–west.
#[derive(Clone, Debug)]
pub struct Street {
    pub id: i32,
    /// The district, by its index in `Places::postcodes`.
    pub postcode: usize,
    pub name: String,
    pub from: (i32, i32),
    pub to: (i32, i32),
    pub north_south: bool,
    /// The house numbers run from 1 to this.
    pub houses: i32,
}

/// Where one builder lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Home {
    pub street_id: i32,
    pub house_number: i32,
    pub latitude: i32,
    pub longitude: i32,
}

#[derive(Clone, Debug)]
pub struct Places {
    pub cities: Vec<City>,
    pub postcodes: Vec<Postcode>,
    pub streets: Vec<Street>,
    /// Per home zone, its cities and an alias table over their populations.
    by_zone: Vec<(Vec<usize>, Alias)>,
    /// Per city, its streets' indices.
    streets_of: Vec<Range<usize>>,
}

/// The cities, parsed once from `data/cities.tsv`, with their ids in file order.
pub fn cities() -> &'static [City] {
    static ALL: OnceLock<Vec<City>> = OnceLock::new();
    ALL.get_or_init(|| {
        CITIES
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with("country\t"))
            .enumerate()
            .map(|(i, line)| {
                let f: Vec<&'static str> = line.split('\t').collect();
                assert_eq!(f.len(), 8, "cities.tsv line {}: {line}", i + 1);
                let zone = ZONE_NAMES
                    .iter()
                    .position(|z| *z == f[1])
                    .unwrap_or_else(|| panic!("{}: {} is not a home zone", f[2], f[1]));
                let number = |s: &str| -> f64 {
                    s.parse()
                        .unwrap_or_else(|_| panic!("{}: `{s}` is not a number", f[2]))
                };
                let population = number(f[6]);
                let area = number(f[7]);
                let side_km = if area > 0.0 {
                    area.sqrt()
                } else {
                    (population / DENSITY).sqrt()
                }
                .clamp(CITY_KM.0, CITY_KM.1);
                City {
                    id: i as i32 + 1,
                    country: f[0],
                    zone,
                    name: f[2],
                    region: f[3],
                    latitude: micro(number(f[4])),
                    longitude: micro(number(f[5])),
                    population: population as i32,
                    side_km,
                }
            })
            .collect()
    })
}

fn micro(degrees: f64) -> i32 {
    (degrees * 1e6).round() as i32
}

/// The centre of the thousandth-of-a-degree cell a point in microdegrees falls in.
pub fn cell_centre(micro: i32) -> i32 {
    micro.div_euclid(1000) * 1000 + 500
}

/// Degrees from microdegrees.
pub fn degrees(micro: i32) -> f64 {
    f64::from(micro) / 1e6
}

impl Places {
    /// The postcodes and streets of every city: the postcodes by each city's population, and the
    /// streets for `builders` builders spread over the home zones evenly and over each zone's
    /// cities by population. The same seed and count give the same places.
    pub fn new(seed: u64, builders: u64) -> Places {
        let cities = cities().to_vec();
        let zones = ZONE_NAMES.len();
        let mut zone_population = vec![0.0f64; zones];
        for c in &cities {
            zone_population[c.zone] += f64::from(c.population);
        }
        let per_zone = builders as f64 / zones as f64;
        let mut postcodes = Vec::new();
        let mut streets = Vec::new();
        let mut streets_of = Vec::with_capacity(cities.len());
        let mut codes = Codes::default();
        for (ci, c) in cities.iter().enumerate() {
            let mut r = Rng::stream(seed, Purpose::Places, ci as u64);
            let expected = per_zone * f64::from(c.population) / zone_population[c.zone];
            let widest = ((c.side_km / DISTRICT_MIN_KM).floor() as i64).max(1);
            let districts = (f64::from(c.population) / DISTRICT_PEOPLE).round().max(1.0);
            let k = (districts.sqrt().ceil() as i64).clamp(1, widest);
            let per_district = expected / (k * k) as f64;
            let per_district_streets =
                ((per_district / HOMES_PER_STREET).ceil() as i64).clamp(1, MAX_STREETS);
            let houses = ((per_district / per_district_streets as f64 * 4.0).ceil() as i32).max(20);
            let houses = houses + houses % 2;
            let half_lat = c.side_km / 2.0 / KM_PER_DEGREE;
            let half_lon = half_lat / degrees(c.latitude).to_radians().cos();
            let (d_lat, d_lon) = (2.0 * half_lat / k as f64, 2.0 * half_lon / k as f64);
            let first_street = streets.len();
            for i in 0..k {
                for j in 0..k {
                    let south = degrees(c.latitude) - half_lat + i as f64 * d_lat;
                    let west = degrees(c.longitude) - half_lon + j as f64 * d_lon;
                    let postcode = postcodes.len();
                    postcodes.push(Postcode {
                        id: postcode as i32 + 1,
                        city: ci,
                        code: codes.code(ci, c, (i * k + j) as u32, (i, j, k), &mut r),
                    });
                    for _ in 0..per_district_streets {
                        let north_south = r.below(2) == 0;
                        let across = 0.05 + 0.9 * r.unit();
                        let start = 0.3 * r.unit();
                        let end = (start + 0.4 + 0.6 * r.unit()).min(1.0);
                        let (from, to) = if north_south {
                            let lon = west + across * d_lon;
                            ((south + start * d_lat, lon), (south + end * d_lat, lon))
                        } else {
                            let lat = south + across * d_lat;
                            ((lat, west + start * d_lon), (lat, west + end * d_lon))
                        };
                        let snap = |(lat, lon): (f64, f64)| {
                            (cell_centre(micro(lat)), cell_centre(micro(lon)))
                        };
                        streets.push(Street {
                            id: streets.len() as i32 + 1,
                            postcode,
                            name: street_name(c.country, &mut r),
                            from: snap(from),
                            to: snap(to),
                            north_south,
                            houses,
                        });
                    }
                }
            }
            streets_of.push(first_street..streets.len());
        }
        let by_zone = (0..zones)
            .map(|z| {
                let ids: Vec<usize> = (0..cities.len()).filter(|&i| cities[i].zone == z).collect();
                let weights: Vec<f64> = ids
                    .iter()
                    .map(|&i| f64::from(cities[i].population))
                    .collect();
                let alias = Alias::new(&weights);
                (ids, alias)
            })
            .collect();
        Places {
            cities,
            postcodes,
            streets,
            by_zone,
            streets_of,
        }
    }

    /// A home in `zone`: a city drawn by population, a street of it, a house number on it, and the
    /// home's point beside the street.
    pub fn home(&self, zone: usize, r: &mut Rng) -> Home {
        let (ids, alias) = &self.by_zone[zone];
        let city = ids[alias.draw(r)];
        let range = &self.streets_of[city];
        let s = &self.streets[range.start + r.below(range.len() as u64) as usize];
        let n = 1 + r.below(s.houses as u64) as i32;
        let slots = s.houses / 2;
        let along = (f64::from((n + 1) / 2) - 0.5) / f64::from(slots);
        let lat = degrees(s.from.0) + along * degrees(s.to.0 - s.from.0);
        let lon = degrees(s.from.1) + along * degrees(s.to.1 - s.from.1);
        let back = SETBACK_KM.0 + (SETBACK_KM.1 - SETBACK_KM.0) * r.unit();
        let side = if n % 2 == 1 { -1.0 } else { 1.0 };
        let (lat, lon) = if s.north_south {
            (
                lat,
                lon + side * back / (KM_PER_DEGREE * lat.to_radians().cos()),
            )
        } else {
            (lat + side * back / KM_PER_DEGREE, lon)
        };
        Home {
            street_id: s.id,
            house_number: n,
            latitude: cell_centre(micro(lat)),
            longitude: cell_centre(micro(lon)),
        }
    }

    /// The city a street lies in, by its index in `cities`.
    #[cfg(test)]
    pub fn city_of_street(&self, street_id: i32) -> usize {
        self.postcodes[self.streets[street_id as usize - 1].postcode].city
    }

    /// The street ids of the cities in the home zones `zones`, lowest and highest. The cities are
    /// in country and zone order, so one country's or one zone's streets are one run of ids.
    #[cfg(test)]
    pub fn street_ids(&self, zones: &[usize]) -> (i32, i32) {
        let mut ids = self
            .cities
            .iter()
            .enumerate()
            .filter(|(_, c)| zones.contains(&c.zone))
            .flat_map(|(i, _)| self.streets_of[i].clone())
            .map(|s| self.streets[s].id);
        let first = ids.next().expect("a zone with streets");
        let last = ids.next_back().unwrap_or(first);
        (first, last)
    }
}

/// Each country's postcodes. A code's leading characters follow the country's own scheme, so a
/// code says where it is: a ZIP's, a PIN's, a Japanese and a Portuguese code's first digit by
/// state, prefecture or district; an Australian one's by state; a Danish one's by region; and a UK
/// code's letters by its post town's area, with London's by its compass point. The rest is
/// numbered in order, and made up.
#[derive(Default)]
struct Codes {
    /// Per country and leading key, the next number free under it.
    next: HashMap<(&'static str, &'static str), u32>,
    /// Per city, the first number it was given.
    first: HashMap<usize, u32>,
}

impl Codes {
    /// The code of district `d` of city `ci`, which lies at `(i, j)` of the city's `k` × `k` grid.
    fn code(&mut self, ci: usize, c: &City, d: u32, at: (i64, i64, i64), r: &mut Rng) -> String {
        match c.country {
            "US" => {
                let lead = us_lead(c.region);
                let sc = self.block(ci, ("US", lead), d, 100, 100);
                format!("{lead}{sc:02}{:02}", d % 100)
            }
            "IN" => {
                let lead = in_lead(c.region);
                let sc = self.block(ci, ("IN", lead), d, 1000, 100);
                format!("{lead}{sc:02}{:03}", d % 1000)
            }
            "JP" => {
                let lead = jp_lead(c.region);
                let sc = self.block(ci, ("JP", lead), d, 100, 100);
                format!("{lead}{sc:02}-{:02}{:02}", d % 100, r.below(100))
            }
            "PT" => {
                let lead = pt_lead(c.region);
                let n = self.take(("PT", lead), 1000);
                format!("{lead}{n:03}-{:03}", r.below(1000))
            }
            "DK" => {
                let lead = dk_lead(c);
                let n = self.take(("DK", lead), 1000);
                format!("{lead}{n:03}")
            }
            "AU" => match c.region {
                "Australian Capital Territory" => {
                    format!("{}", 2600 + self.take(("AU", "ACT"), 20))
                }
                "New South Wales" => format!("{}", 2000 + self.take(("AU", "NSW"), 600)),
                other => panic!("{}: no postcodes for {other}", c.name),
            },
            "GB" => {
                let (area, from) = gb_area(c, at);
                let next = self.next.entry(("GB", area)).or_insert(from);
                *next = (*next).max(from);
                let district = *next;
                *next += 1;
                let unit = |r: &mut Rng| char::from(INWARD[r.below(INWARD.len() as u64) as usize]);
                format!("{area}{district} {}{}{}", r.below(10), unit(r), unit(r))
            }
            other => panic!("{}: no postcodes for {other}", c.name),
        }
    }

    /// The number of district `d` in the run of numbers its city holds under `key`, `per` districts
    /// to a number, at most `cap` numbers under one key.
    fn block(
        &mut self,
        ci: usize,
        key: (&'static str, &'static str),
        d: u32,
        per: u32,
        cap: u32,
    ) -> u32 {
        let first = match self.first.get(&ci) {
            Some(&f) => f,
            None => {
                let f = *self.next.get(&key).unwrap_or(&0);
                self.first.insert(ci, f);
                f
            }
        };
        let n = first + d / per;
        let next = self.next.entry(key).or_insert(0);
        *next = (*next).max(n + 1);
        assert!(n < cap, "{key:?}: more than {cap} numbers");
        n
    }

    /// The next number free under `key`, below `cap`.
    fn take(&mut self, key: (&'static str, &'static str), cap: u32) -> u32 {
        let next = self.next.entry(key).or_insert(0);
        let n = *next;
        *next += 1;
        assert!(n < cap, "{key:?}: more than {cap} numbers");
        n
    }
}

/// The letters a UK inward code uses: every letter but C, I, K, M, O and V.
const INWARD: &[u8] = b"ABDEFGHJLNPQRSTUWXYZ";

/// A ZIP code's first digit, by state.
fn us_lead(state: &str) -> &'static str {
    match state {
        "Connecticut" | "Massachusetts" | "Maine" | "New Hampshire" | "New Jersey"
        | "Rhode Island" | "Vermont" => "0",
        "Delaware" | "New York" | "Pennsylvania" => "1",
        "District of Columbia"
        | "Maryland"
        | "North Carolina"
        | "South Carolina"
        | "Virginia"
        | "West Virginia" => "2",
        "Alabama" | "Florida" | "Georgia" | "Mississippi" | "Tennessee" => "3",
        "Indiana" | "Kentucky" | "Michigan" | "Ohio" => "4",
        "Arizona" | "Idaho" | "Nevada" | "Utah" => "8",
        "California" | "Oregon" | "Washington" => "9",
        other => panic!("no ZIP code for {other}"),
    }
}

/// A PIN code's first digit, by state.
fn in_lead(state: &str) -> &'static str {
    match state {
        "Chandigarh" | "Delhi" | "Haryana" | "Himachal Pradesh" | "Jammu and Kashmir"
        | "Ladakh" | "Punjab" => "1",
        "Uttar Pradesh" | "Uttaranchal" => "2",
        "Dadra and Nagar Haveli" | "Rajasthan" => "3",
        "Chhattisgarh" | "Goa" | "Madhya Pradesh" | "Maharashtra" => "4",
        "Andhra Pradesh" | "Karnataka" | "Telangana" => "5",
        "Kerala" | "Lakshadweep" | "Puducherry" | "Tamil Nadu" => "6",
        "Andaman and Nicobar"
        | "Arunachal Pradesh"
        | "Assam"
        | "Manipur"
        | "Meghalaya"
        | "Mizoram"
        | "Nagaland"
        | "Orissa"
        | "Sikkim"
        | "Tripura"
        | "West Bengal" => "7",
        "Bihar" | "Jharkhand" => "8",
        other => panic!("no PIN code for {other}"),
    }
}

/// A Japanese postcode's first digit, by prefecture.
fn jp_lead(prefecture: &str) -> &'static str {
    match prefecture {
        "Akita" | "Aomori" | "Hokkaido" | "Iwate" => "0",
        "Tokyo" => "1",
        "Chiba" | "Kanagawa" => "2",
        "Gunma" | "Ibaraki" | "Nagano" | "Saitama" | "Tochigi" => "3",
        "Aichi" | "Shizuoka" | "Yamanashi" => "4",
        "Gifu" | "Mie" | "Osaka" | "Shiga" => "5",
        "Hyogo" | "Kyoto" | "Nara" | "Shimane" | "Tottori" | "Wakayama" => "6",
        "Ehime" | "Hiroshima" | "Kagawa" | "Kochi" | "Okayama" | "Tokushima" | "Yamaguchi" => "7",
        "Fukuoka" | "Kagoshima" | "Kumamoto" | "Miyazaki" | "Nagasaki" | "Oita" | "Saga" => "8",
        "Fukui" | "Fukushima" | "Ishikawa" | "Miyagi" | "Niigata" | "Okinawa" | "Toyama"
        | "Yamagata" => "9",
        other => panic!("no postcode for {other}"),
    }
}

/// A Portuguese postcode's first digit, by district.
fn pt_lead(district: &str) -> &'static str {
    match district {
        "Lisboa" => "1",
        "Leiria" | "Santarém" | "Setúbal" => "2",
        "Aveiro" | "Coimbra" | "Viseu" => "3",
        "Braga" | "Porto" | "Viana do Castelo" => "4",
        "Bragança" | "Vila Real" => "5",
        "Castelo Branco" | "Guarda" => "6",
        "Beja" | "Évora" | "Portalegre" => "7",
        "Faro" => "8",
        other => panic!("no postcode for {other}"),
    }
}

/// A Danish postcode's first digit: Copenhagen's, the rest of the capital region's, Zealand's,
/// Funen's, south Jutland's, mid Jutland's west and east, and north Jutland's.
fn dk_lead(c: &City) -> &'static str {
    let lon = degrees(c.longitude);
    match c.region {
        "Hovedstaden" if c.name == "København" => "1",
        "Hovedstaden" => "3",
        "Sjaælland" | "Sjælland" => "4",
        "Syddanmark" if lon >= 9.7 => "5",
        "Syddanmark" => "6",
        "Midtjylland" if lon >= 10.0 => "8",
        "Midtjylland" => "7",
        "Nordjylland" => "9",
        other => panic!("{}: no postcode for {other}", c.name),
    }
}

/// The UK postcode areas of the towns, and the district each town's numbers start at.
const GB_AREAS: &[(&str, &str, u32)] = &[
    ("Birmingham", "B", 1),
    ("Manchester", "M", 1),
    ("Leeds", "LS", 1),
    ("Sheffield", "S", 1),
    ("Glasgow", "G", 1),
    ("Newcastle", "NE", 1),
    ("Nottingham", "NG", 1),
    ("Liverpool", "L", 1),
    ("Southend-on-Sea", "SS", 0),
    ("Bristol", "BS", 1),
    ("Edinburgh", "EH", 1),
    ("Brighton", "BN", 1),
    ("Bradford", "BD", 1),
    ("Leicester", "LE", 1),
    ("Sunderland", "SR", 1),
    ("Belfast", "BT", 1),
    ("Portsmouth", "PO", 1),
    ("Bournemouth", "BH", 1),
    ("Middlesbrough", "TS", 1),
    ("Stoke", "ST", 1),
    ("Coventry", "CV", 1),
    ("Southampton", "SO", 14),
    ("Reading", "RG", 1),
    ("Kingston upon Hull", "HU", 1),
    ("Swansea", "SA", 1),
    ("Blackpool", "FY", 1),
    ("Plymouth", "PL", 1),
    ("Luton", "LU", 1),
    ("Oxford", "OX", 1),
    ("Norwich", "NR", 1),
    ("Aberdeen", "AB", 10),
    ("York", "YO", 1),
    ("Dundee", "DD", 1),
    ("Ipswich", "IP", 1),
    ("Peterborough", "PE", 1),
    ("Cambridge", "CB", 1),
    ("Exeter", "EX", 1),
    ("Bath", "BA", 1),
    ("Chester", "CH", 1),
    ("Londonderry/Derry", "BT", 47),
    ("Greenock", "PA", 15),
    ("Carlisle", "CA", 1),
    ("Scarborough", "YO", 11),
    ("Ayr", "KA", 6),
    ("Inverness", "IV", 1),
    ("Perth", "PH", 1),
    ("Dover", "CT", 16),
    ("Dumfries", "DG", 1),
    ("Omagh", "BT", 78),
    ("Penzance", "TR", 18),
    ("Lisburn", "BT", 27),
    ("Fort William", "PH", 33),
    ("Kirkwall", "KW", 15),
    ("Wick", "KW", 1),
    ("Lerwick", "ZE", 1),
];

/// A UK district's postcode area, and the district number its town starts at. London's areas are
/// its compass points from the centre of the city's grid, with the City's two at the centre.
fn gb_area(c: &City, (i, j, k): (i64, i64, i64)) -> (&'static str, u32) {
    if c.name == "London" {
        let half = k as f64 / 2.0;
        let (north, east) = (i as f64 + 0.5 - half, j as f64 + 0.5 - half);
        if north.abs().max(east.abs()) <= (k as f64 / 8.0).max(0.5) {
            return (if east >= 0.0 { "EC" } else { "WC" }, 1);
        }
        let angle = north.atan2(east).to_degrees().rem_euclid(360.0);
        let area = match angle {
            a if a < 67.5 => "E",
            a if a < 112.5 => "N",
            a if a < 157.5 => "NW",
            a if a < 202.5 => "W",
            a if a < 270.0 => "SW",
            a if a < 337.5 => "SE",
            _ => "E",
        };
        return (area, 1);
    }
    GB_AREAS
        .iter()
        .find(|(town, _, _)| *town == c.name)
        .map(|(_, area, from)| (*area, *from))
        .unwrap_or_else(|| panic!("no postcode area for {}", c.name))
}

const WORDS: &[&str] = &[
    "Brick",
    "Stud",
    "Plate",
    "Tile",
    "Slope",
    "Axle",
    "Gear",
    "Hinge",
    "Baseplate",
    "Beam",
    "Bush",
    "Clip",
    "Bracket",
    "Wedge",
    "Arch",
    "Panel",
    "Grille",
    "Cone",
    "Dish",
    "Jumper",
    "Ingot",
    "Corner",
    "Lattice",
    "Pinion",
    "Bollard",
    "Shutter",
    "Turntable",
    "Crank",
    "Pulley",
    "Spindle",
    "Rafter",
    "Gable",
    "Keystone",
    "Mortar",
    "Cobble",
    "Brickyard",
    "Roundstud",
    "Flatplate",
];
const MODIFIERS: &[&str] = &[
    "Old", "Upper", "Lower", "Little", "Great", "North", "South", "East", "West", "Red", "Yellow",
    "Blue", "Green",
];
const US_TYPES: &[&str] = &[
    "Street",
    "Avenue",
    "Road",
    "Lane",
    "Drive",
    "Court",
    "Place",
    "Way",
    "Boulevard",
    "Terrace",
];
const GB_TYPES: &[&str] = &[
    "Street", "Road", "Lane", "Close", "Row", "Crescent", "Terrace", "Mews", "Gardens", "Walk",
];
const AU_TYPES: &[&str] = &[
    "Street",
    "Road",
    "Avenue",
    "Parade",
    "Crescent",
    "Close",
    "Lane",
    "Place",
    "Grove",
    "Esplanade",
];
const IN_TYPES: &[&str] = &[
    "Road",
    "Marg",
    "Lane",
    "Street",
    "Path",
    "Cross Road",
    "Main Road",
    "Nagar Road",
];
const DK_WORDS: &[&str] = &[
    "Klods", "Plade", "Tap", "Aksel", "Tandhjul", "Hængsel", "Bue", "Kegle", "Flise", "Bjælke",
    "Kile", "Hjørne", "Rist", "Skive", "Vindue", "Dør", "Tårn", "Bro", "Mursten", "Gavl",
];
const DK_TYPES: &[&str] = &["vej", "gade", "allé", "stræde", "vænget", "toften"];
const PT_WORDS: &[(&str, &str)] = &[
    ("do", "Tijolo"),
    ("da", "Placa"),
    ("do", "Eixo"),
    ("da", "Engrenagem"),
    ("da", "Dobradiça"),
    ("do", "Arco"),
    ("do", "Cone"),
    ("da", "Telha"),
    ("do", "Pino"),
    ("da", "Viga"),
    ("do", "Canto"),
    ("da", "Grelha"),
    ("da", "Janela"),
    ("da", "Torre"),
    ("da", "Ponte"),
    ("do", "Bloco"),
    ("das", "Peças"),
    ("dos", "Blocos"),
    ("do", "Encaixe"),
    ("da", "Roldana"),
];
const PT_TYPES: &[&str] = &["Rua", "Avenida", "Travessa", "Largo", "Calçada", "Beco"];
const JP_WORDS: &[&str] = &[
    "Renga", "Ita", "Jiku", "Haguruma", "Tsugai", "Kado", "Mado", "Hashira", "Yane", "Kawara",
    "Tsumiki", "Kumiki", "Kaidan", "Hashi", "Tobira", "Kugi", "Kuruma", "Takara", "Shiro", "Tō",
];
const JP_TYPES: &[&str] = &["dōri", "chō", "zaka", "yokochō", "suji"];

/// A made-up street name in the way the country names its streets.
fn street_name(country: &str, r: &mut Rng) -> String {
    let english = |types: &[&str], r: &mut Rng| {
        let word = *r.pick(WORDS);
        let kind = *r.pick(types);
        if r.below(5) == 0 {
            format!("{} {word} {kind}", r.pick(MODIFIERS))
        } else {
            format!("{word} {kind}")
        }
    };
    match country {
        "US" => english(US_TYPES, r),
        "GB" => english(GB_TYPES, r),
        "AU" => english(AU_TYPES, r),
        "IN" => english(IN_TYPES, r),
        "DK" => format!("{}{}", r.pick(DK_WORDS), r.pick(DK_TYPES)),
        "PT" => {
            let kind = *r.pick(PT_TYPES);
            let (of, word) = *r.pick(PT_WORDS);
            format!("{kind} {of} {word}")
        }
        "JP" => format!("{}-{}", r.pick(JP_WORDS), r.pick(JP_TYPES)),
        other => panic!("no street names for {other}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// `code` in the shape of `pattern`, where `9` is any digit and every other character itself.
    fn shaped(code: &str, pattern: &str) -> bool {
        code.len() == pattern.len()
            && code.chars().zip(pattern.chars()).all(|(c, p)| match p {
                '9' => c.is_ascii_digit(),
                _ => c == p,
            })
    }

    #[test]
    fn every_postcode_has_its_countrys_shape_and_says_where_it_is() {
        let places = Places::new(7, 4000);
        for p in &places.postcodes {
            let c = &places.cities[p.city];
            let code = p.code.as_str();
            let ok = match c.country {
                "US" => shaped(code, "99999") && code.starts_with(us_lead(c.region)),
                "IN" => shaped(code, "999999") && code.starts_with(in_lead(c.region)),
                "JP" => shaped(code, "999-9999") && code.starts_with(jp_lead(c.region)),
                "PT" => shaped(code, "9999-999") && code.starts_with(pt_lead(c.region)),
                "DK" => shaped(code, "9999") && code.starts_with(dk_lead(c)),
                "AU" => shaped(code, "9999") && code.starts_with('2'),
                "GB" => {
                    let (outward, inward) = code.split_once(' ').expect("two parts");
                    let letters = outward.chars().take_while(char::is_ascii_uppercase).count();
                    let digits = &outward[letters..];
                    (1..=2).contains(&letters)
                        && !digits.is_empty()
                        && digits.chars().all(|ch| ch.is_ascii_digit())
                        && inward.len() == 3
                        && inward.as_bytes()[0].is_ascii_digit()
                        && inward.bytes().skip(1).all(|b| INWARD.contains(&b))
                }
                _ => false,
            };
            assert!(ok, "{} {}: {code}", c.country, c.name);
        }
        let codes = |town: &str| -> Vec<String> {
            places
                .postcodes
                .iter()
                .filter(|p| {
                    let c = &places.cities[p.city];
                    c.country == "GB" && c.name == town
                })
                .map(|p| p.code.clone())
                .collect()
        };
        assert!(codes("Lerwick").iter().all(|c| c.starts_with("ZE")));
        assert!(codes("Kirkwall").iter().all(|c| c.starts_with("KW15 ")));
        assert!(codes("Fort William").iter().all(|c| c.starts_with("PH33 ")));
        assert!(codes("Aberdeen").iter().all(|c| {
            let n: u32 = c[2..c.find(' ').unwrap()].parse().unwrap();
            c.starts_with("AB") && n >= 10
        }));
        let london: HashSet<String> = codes("London")
            .iter()
            .map(|c| c.chars().take_while(char::is_ascii_uppercase).collect())
            .collect();
        let compass: HashSet<String> = ["E", "EC", "N", "NW", "SE", "SW", "W", "WC"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(london, compass);
    }

    #[test]
    fn every_home_zone_has_its_capital_and_every_city_is_in_its_zones_country() {
        let all = cities();
        for (z, name) in ZONE_NAMES.iter().enumerate() {
            assert!(all.iter().any(|c| c.zone == z), "{name}");
        }
        for name in [
            "Sydney",
            "København",
            "London",
            "Tokyo",
            "Lisbon",
            "New York",
            "Los Angeles",
            "Mumbai",
        ] {
            assert!(all.iter().any(|c| c.name == name), "{name}");
        }
        for c in all {
            let (_, country) = crate::calendar::ZONE_COUNTRIES
                .iter()
                .find(|(z, _)| *z == ZONE_NAMES[c.zone])
                .unwrap();
            assert_eq!(c.country, *country, "{c:?}");
        }
    }

    #[test]
    fn every_point_is_a_cell_centre_and_every_home_lies_in_its_city() {
        let places = Places::new(7, 4000);
        for s in &places.streets {
            for (lat, lon) in [s.from, s.to] {
                assert_eq!(lat.rem_euclid(1000), 500, "{s:?}");
                assert_eq!(lon.rem_euclid(1000), 500, "{s:?}");
            }
        }
        let mut r = Rng::new(3);
        for z in 0..ZONE_NAMES.len() {
            for _ in 0..500 {
                let h = places.home(z, &mut r);
                assert_eq!(h.latitude.rem_euclid(1000), 500, "{h:?}");
                assert_eq!(h.longitude.rem_euclid(1000), 500, "{h:?}");
                let c = &places.cities[places.city_of_street(h.street_id)];
                assert_eq!(c.zone, z);
                let half = c.side_km / 2.0 + 0.2;
                let dlat = degrees(h.latitude - c.latitude).abs() * KM_PER_DEGREE;
                let dlon = degrees(h.longitude - c.longitude).abs()
                    * KM_PER_DEGREE
                    * degrees(c.latitude).to_radians().cos();
                assert!(dlat <= half && dlon <= half, "{h:?} is outside {}", c.name);
            }
        }
    }

    #[test]
    fn postcodes_are_unique_in_each_country_and_streets_of_a_zone_are_one_run() {
        let places = Places::new(7, 40_000);
        let mut seen = HashSet::new();
        for p in &places.postcodes {
            let country = places.cities[p.city].country;
            assert!(
                seen.insert((country, p.code.clone())),
                "{country} {}",
                p.code
            );
        }
        for (z, name) in ZONE_NAMES.iter().enumerate() {
            let (lo, hi) = places.street_ids(&[z]);
            let inside = places
                .streets
                .iter()
                .filter(|s| (lo..=hi).contains(&s.id))
                .all(|s| places.cities[places.city_of_street(s.id)].zone == z);
            assert!(inside, "{name}");
        }
    }

    #[test]
    fn the_same_seed_draws_the_same_places() {
        let a = Places::new(11, 4000);
        let b = Places::new(11, 4000);
        assert_eq!(a.streets.len(), b.streets.len());
        assert!(a
            .streets
            .iter()
            .zip(&b.streets)
            .all(|(x, y)| x.name == y.name && x.from == y.from));
        let c = Places::new(12, 4000);
        assert!(a
            .streets
            .iter()
            .zip(&c.streets)
            .any(|(x, y)| x.name != y.name || x.from != y.from));
    }
}
