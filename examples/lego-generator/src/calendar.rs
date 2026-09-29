//! Civil dates, the builders' home time zones, and ISO 8601 week dates.
//!
//! A purchase is an instant (seconds since 1970-01-01 UTC). A builder writes it down as the wall
//! clock of their home zone. The zones' offsets are the IANA time zone database's, bundled into the
//! binary, so every machine reads the same history; purchase dates are drawn from 1950 onward.

use std::sync::OnceLock;

use jiff::tz::{AmbiguousOffset, Offset, TimeZone, TimeZoneDatabase};
use jiff::Timestamp;

/// Days since 1970-01-01 of a proleptic Gregorian date.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of a day number.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => panic!("month {m}"),
    }
}

/// ISO weekday of a day number: Monday is 1, Sunday is 7.
pub fn iso_weekday(days: i64) -> u32 {
    ((days + 3).rem_euclid(7) + 1) as u32
}

/// The ISO 8601 week date of a day: the week-year is the calendar year of the week's Thursday.
pub fn iso_week(days: i64) -> (i64, u32) {
    let thursday = days - i64::from(iso_weekday(days)) + 4;
    let (ty, _, _) = civil_from_days(thursday);
    let week = (thursday - days_from_civil(ty, 1, 1)) / 7 + 1;
    (ty, week as u32)
}

/// The builders' home zones, by IANA name.
pub const ZONE_NAMES: [&str; 8] = [
    "Europe/Lisbon",
    "Europe/Copenhagen",
    "America/New_York",
    "Australia/Sydney",
    "Europe/London",
    "Asia/Tokyo",
    "America/Los_Angeles",
    "Asia/Kolkata",
];

/// Each home zone's country, by its ISO 3166 code, as the IANA database's `zone.tab` gives it.
pub const ZONE_COUNTRIES: [(&str, &str); 8] = [
    ("Europe/Lisbon", "PT"),
    ("Europe/Copenhagen", "DK"),
    ("America/New_York", "US"),
    ("Australia/Sydney", "AU"),
    ("Europe/London", "GB"),
    ("Asia/Tokyo", "JP"),
    ("America/Los_Angeles", "US"),
    ("Asia/Kolkata", "IN"),
];

/// A builder's home zone: its IANA name and its offset history, from the bundled database.
#[derive(Clone, Debug)]
pub struct Zone {
    pub name: &'static str,
    tz: TimeZone,
}

/// The home zones, in `ZONE_NAMES` order.
pub fn zones() -> &'static [Zone] {
    static ZONES: OnceLock<Vec<Zone>> = OnceLock::new();
    ZONES.get_or_init(|| {
        let db = TimeZoneDatabase::bundled();
        ZONE_NAMES
            .iter()
            .map(|&name| Zone {
                name,
                tz: db
                    .get(name)
                    .unwrap_or_else(|e| panic!("{name}: not in the bundled database: {e}")),
            })
            .collect()
    })
}

/// What a wall-clock reading names in a zone.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LocalTime {
    /// One instant.
    Unique(i64),
    /// Two instants, the earlier first: the clock went back and read this time twice.
    Twice(i64, i64),
    /// No instant: the clock went forward over it.
    Gap,
}

/// A change of a zone's offset at instant `at` (UTC seconds), in minutes east of UTC.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Shift {
    pub at: i64,
    pub before_min: i32,
    pub after_min: i32,
}

impl Shift {
    /// The clock went back, so the readings of the `len_secs` before `at` are read again after it.
    pub fn is_fold(&self) -> bool {
        self.after_min < self.before_min
    }

    /// How far the clock moved, in seconds.
    pub fn len_secs(&self) -> i64 {
        i64::from((self.after_min - self.before_min).abs()) * 60
    }
}

fn instant(t: i64) -> Timestamp {
    Timestamp::from_second(t).expect("an instant in jiff's range")
}

impl Zone {
    /// Offset east of UTC in minutes at an instant.
    pub fn offset_min_at(&self, t: i64) -> i32 {
        self.tz.to_offset(instant(t)).seconds() / 60
    }

    /// The offset changes with `from <= at < to`, in order. A transition that changes only the
    /// zone's abbreviation or its summer-time flag is not a shift.
    pub fn shifts(&self, from: i64, to: i64) -> Vec<Shift> {
        let mut before = self.offset_min_at(from - 1);
        let mut out = Vec::new();
        for tr in self.tz.following(instant(from - 1)) {
            let at = tr.timestamp().as_second();
            if at >= to {
                break;
            }
            let after = tr.offset().seconds() / 60;
            if after != before {
                out.push(Shift {
                    at,
                    before_min: before,
                    after_min: after,
                });
            }
            before = after;
        }
        out
    }

    /// The instants a wall-clock reading (seconds since 1970-01-01, read as if it were UTC) names.
    pub fn instants_of(&self, local: i64) -> LocalTime {
        let dt = Offset::UTC.to_datetime(instant(local));
        match self.tz.to_ambiguous_timestamp(dt).offset() {
            AmbiguousOffset::Unambiguous { offset } => {
                LocalTime::Unique(local - i64::from(offset.seconds()))
            }
            AmbiguousOffset::Fold { before, after } => LocalTime::Twice(
                local - i64::from(before.seconds()),
                local - i64::from(after.seconds()),
            ),
            AmbiguousOffset::Gap { .. } => LocalTime::Gap,
        }
    }

    /// The one instant a wall-clock reading is taken to name: the earlier of two, and inside a gap
    /// the reading taken in the offset before the clock went forward.
    pub fn instant_of(&self, local: i64) -> i64 {
        let dt = Offset::UTC.to_datetime(instant(local));
        let offset = match self.tz.to_ambiguous_timestamp(dt).offset() {
            AmbiguousOffset::Unambiguous { offset } => offset,
            AmbiguousOffset::Fold { before, .. } | AmbiguousOffset::Gap { before, .. } => before,
        };
        local - i64::from(offset.seconds())
    }
}

/// Seconds since 1970-01-01 of a wall-clock reading taken as if it were UTC.
pub fn local_secs(days: i64, hour: u32, minute: u32) -> i64 {
    days * 86_400 + i64::from(hour) * 3600 + i64::from(minute) * 60
}

/// `YYYY-MM-DD HH:MM:SS+HH:MM`: the instant `t` as the wall clock at `offset_min`, with the offset.
pub fn render_with_offset(t: i64, offset_min: i32) -> String {
    let local = t + i64::from(offset_min) * 60;
    let (y, m, d) = civil_from_days(local.div_euclid(86_400));
    let s = local.rem_euclid(86_400);
    let sign = if offset_min < 0 { '-' } else { '+' };
    let o = offset_min.unsigned_abs();
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}{sign}{:02}:{:02}",
        s / 3600,
        (s % 3600) / 60,
        s % 60,
        o / 60,
        o % 60
    )
}

/// `YYYY-MM-DD HH:MM` of a wall-clock reading.
pub fn render_local_minute(local: i64) -> String {
    let (y, m, d) = civil_from_days(local.div_euclid(86_400));
    let s = local.rem_euclid(86_400);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        s / 3600,
        (s % 3600) / 60
    )
}

/// Microseconds since 2000-01-01 00:00 UTC: the binary form of a Postgres timestamptz.
pub fn pg_micros(t: i64) -> i64 {
    (t - 946_684_800) * 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zone(name: &str) -> &'static Zone {
        zones().iter().find(|z| z.name == name).expect(name)
    }

    /// An hour's start in UTC.
    fn utc(y: i64, m: u32, d: u32, h: u32) -> i64 {
        local_secs(days_from_civil(y, m, d), h, 0)
    }

    #[test]
    fn civil_round_trip() {
        for z in -800_000..800_000i64 {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(iso_weekday(0), 4, "1970-01-01 was a Thursday");
    }

    #[test]
    fn iso_weeks_at_year_ends() {
        assert_eq!(iso_week(days_from_civil(2024, 12, 30)), (2025, 1));
        assert_eq!(iso_week(days_from_civil(2021, 1, 1)), (2020, 53));
        assert_eq!(iso_week(days_from_civil(2021, 1, 4)), (2021, 1));
        assert_eq!(iso_week(days_from_civil(2008, 12, 29)), (2009, 1));
    }

    /// Transitions of the bundled history that no fixed yearly rule gives: summer time in Tokyo
    /// only through 1951, British Standard Time from 1968 to 1971, Lisbon on Central European Time
    /// from 1966 to 1976, New York's summer time from January 1974, and none in Copenhagen before
    /// 1980, in Sydney before 1971 or in Kolkata at all.
    #[test]
    fn the_bundled_history_has_the_transitions_no_yearly_rule_gives() {
        let s = |at, before_min, after_min| Shift {
            at,
            before_min,
            after_min,
        };
        let all = |name| zone(name).shifts(utc(1950, 1, 1, 0), utc(2027, 1, 1, 0));
        assert_eq!(
            all("Asia/Tokyo"),
            [
                s(utc(1950, 5, 6, 15), 540, 600),
                s(utc(1950, 9, 9, 15), 600, 540),
                s(utc(1951, 5, 5, 15), 540, 600),
                s(utc(1951, 9, 8, 15), 600, 540),
            ]
        );
        let london = zone("Europe/London");
        assert_eq!(
            london.shifts(utc(1968, 1, 1, 0), utc(1972, 1, 1, 0)),
            [
                s(utc(1968, 2, 18, 2), 0, 60),
                s(utc(1971, 10, 31, 2), 60, 0)
            ]
        );
        let lisbon = zone("Europe/Lisbon");
        assert_eq!(
            lisbon.shifts(utc(1966, 1, 1, 0), utc(1977, 1, 1, 0)),
            [s(utc(1966, 4, 3, 2), 0, 60), s(utc(1976, 9, 26, 0), 60, 0)]
        );
        assert_eq!(
            zone("America/New_York").shifts(utc(1974, 1, 1, 0), utc(1975, 1, 1, 0)),
            [
                s(utc(1974, 1, 6, 7), -300, -240),
                s(utc(1974, 10, 27, 6), -240, -300)
            ]
        );
        assert_eq!(
            all("Europe/Copenhagen").first(),
            Some(&s(utc(1980, 4, 6, 1), 60, 120))
        );
        assert_eq!(
            all("Australia/Sydney").first(),
            Some(&s(utc(1971, 10, 30, 16), 600, 660))
        );
        assert_eq!(all("Asia/Kolkata"), []);
        assert_eq!(zone("Asia/Kolkata").offset_min_at(utc(1950, 1, 1, 0)), 330);
    }

    /// At every shift from 1950, the readings the clock skipped name no instant, the readings it
    /// read twice name both, the readings either side name one, and each instant reads back as the
    /// reading that named it.
    #[test]
    fn every_reading_around_every_shift_names_its_instants() {
        let mut shifts = 0;
        for z in zones() {
            for sh in z.shifts(utc(1950, 1, 1, 0), utc(2027, 1, 1, 0)) {
                shifts += 1;
                let len = sh.len_secs();
                let first = sh.at + i64::from(sh.before_min.min(sh.after_min)) * 60;
                let last = sh.at + i64::from(sh.before_min.max(sh.after_min)) * 60;
                assert_eq!(last - first, len);
                for local in (first - 3600..last + 3600).step_by(600) {
                    let named = z.instants_of(local);
                    let within = (first..last).contains(&local);
                    match named {
                        LocalTime::Gap => assert!(within && !sh.is_fold(), "{} {sh:?}", z.name),
                        LocalTime::Twice(a, b) => {
                            assert!(within && sh.is_fold(), "{} {sh:?}", z.name);
                            assert_eq!(b - a, len);
                        }
                        LocalTime::Unique(_) => assert!(!within, "{} {sh:?}", z.name),
                    }
                    if let LocalTime::Unique(t) | LocalTime::Twice(t, _) = named {
                        assert_eq!(t + i64::from(z.offset_min_at(t)) * 60, local);
                        assert_eq!(z.instant_of(local), t);
                    } else {
                        let t = z.instant_of(local);
                        assert_eq!(t, local - i64::from(sh.before_min) * 60);
                    }
                }
            }
        }
        assert!(shifts > 500, "{shifts} shifts");
    }

    #[test]
    fn lisbon_fall_back_reads_twice_and_spring_forward_has_a_gap() {
        let lisbon = zone("Europe/Lisbon");
        let fall = local_secs(days_from_civil(2025, 10, 26), 1, 30);
        match lisbon.instants_of(fall) {
            LocalTime::Twice(a, b) => {
                assert_eq!(b - a, 3600);
                assert_eq!(
                    render_with_offset(a, lisbon.offset_min_at(a)),
                    "2025-10-26 01:30:00+01:00"
                );
                assert_eq!(
                    render_with_offset(b, lisbon.offset_min_at(b)),
                    "2025-10-26 01:30:00+00:00"
                );
            }
            other => panic!("{other:?}"),
        }
        let spring = local_secs(days_from_civil(2026, 3, 29), 1, 30);
        assert_eq!(lisbon.instants_of(spring), LocalTime::Gap);
        let noon = local_secs(days_from_civil(2026, 7, 1), 12, 0);
        assert_eq!(lisbon.instants_of(noon), LocalTime::Unique(noon - 3600));
    }

    #[test]
    fn sydney_summer_spans_the_new_year() {
        let sydney = zone("Australia/Sydney");
        let new_year = local_secs(days_from_civil(2026, 1, 1), 0, 0);
        assert_eq!(sydney.offset_min_at(new_year - 11 * 3600), 660);
        let july = local_secs(days_from_civil(2026, 7, 1), 0, 0);
        assert_eq!(sydney.offset_min_at(july), 600);
    }
}
