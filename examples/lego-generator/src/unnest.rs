//! The other way to write a batch: one `INSERT … SELECT FROM UNNEST(…)` per level, every column
//! bound as an array.

use sqlx::{Postgres, Transaction};

use crate::calendar;
use crate::load::{table, Level};
use crate::world::Stamp;

fn stamp_text(s: &Stamp) -> String {
    match s {
        Stamp::At { t, offset_min } => calendar::render_with_offset(*t, *offset_min),
        Stamp::Infinity => "infinity".into(),
    }
}

/// Inserts one level; returns the rows the server reported and the bytes of the bound values.
pub async fn insert(
    tx: &mut Transaction<'static, Postgres>,
    schema: &str,
    level: &Level<'_>,
) -> Result<(u64, u64), sqlx::Error> {
    let q = |name: &str, types: &str| {
        let t = table(name);
        let params: Vec<String> = types
            .split(',')
            .enumerate()
            .map(|(k, ty)| format!("${}::{}", k + 1, ty.trim()))
            .collect();
        format!(
            "INSERT INTO {schema}.{} ({}) SELECT * FROM UNNEST({})",
            t.name,
            t.columns,
            params.join(", ")
        )
    };
    let (sql, bytes, res) = match level {
        Level::Sets(w) => {
            let a: Vec<&str> = w.sets.iter().map(|r| r.set_num.as_str()).collect();
            let b: Vec<&str> = w.sets.iter().map(|r| r.name.as_str()).collect();
            let c: Vec<Option<i32>> = w.sets.iter().map(|r| r.year).collect();
            let d: Vec<Option<i32>> = w.sets.iter().map(|r| r.theme_id).collect();
            let e: Vec<Option<i32>> = w.sets.iter().map(|r| r.num_parts).collect();
            let bytes = a
                .iter()
                .chain(b.iter())
                .map(|s| s.len() as u64)
                .sum::<u64>()
                + 12 * w.sets.len() as u64;
            let sql = q("lego_sets", "text[], text[], int4[], int4[], int4[]");
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .bind(d)
                .bind(e)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Inventories(w) => {
            let a: Vec<i32> = w.inventories.iter().map(|r| r.id).collect();
            let b: Vec<i32> = w.inventories.iter().map(|r| r.version).collect();
            let c: Vec<&str> = w.inventories.iter().map(|r| r.set_num.as_str()).collect();
            let bytes = c.iter().map(|s| s.len() as u64).sum::<u64>() + 8 * a.len() as u64;
            let sql = q("lego_inventories", "int4[], int4[], text[]");
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Lines(w, pool) => {
            let a: Vec<i32> = w.lines.iter().map(|r| r.inventory_id).collect();
            let b: Vec<&str> = w
                .lines
                .iter()
                .map(|r| pool[r.part as usize].as_str())
                .collect();
            let c: Vec<i32> = w.lines.iter().map(|r| r.color_id).collect();
            let d: Vec<i32> = w.lines.iter().map(|r| r.quantity).collect();
            let e: Vec<bool> = w.lines.iter().map(|r| r.is_spare).collect();
            let bytes = b.iter().map(|s| s.len() as u64).sum::<u64>() + 13 * a.len() as u64;
            let sql = q(
                "lego_inventory_parts",
                "int4[], text[], int4[], int4[], bool[]",
            );
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .bind(d)
                .bind(e)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Nests(w) => {
            let a: Vec<i32> = w.nests.iter().map(|r| r.inventory_id).collect();
            let b: Vec<&str> = w.nests.iter().map(|r| r.set_num.as_str()).collect();
            let c: Vec<i32> = w.nests.iter().map(|r| r.quantity).collect();
            let bytes = b.iter().map(|s| s.len() as u64).sum::<u64>() + 8 * a.len() as u64;
            let sql = q("lego_inventory_sets", "int4[], text[], int4[]");
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Builders(w) => {
            let a: Vec<i32> = w.builders.iter().map(|r| r.builder_id).collect();
            let b: Vec<&str> = w.builders.iter().map(|r| r.name.as_str()).collect();
            let c: Vec<&str> = w.builders.iter().map(|r| r.home_zone).collect();
            let bytes = b
                .iter()
                .chain(c.iter())
                .map(|s| s.len() as u64)
                .sum::<u64>()
                + 4 * a.len() as u64;
            let sql = q("lego_builders", "int4[], text[], text[]");
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Collection(w) => {
            let a: Vec<i32> = w.collection.iter().map(|r| r.builder_id).collect();
            let b: Vec<i32> = w.collection.iter().map(|r| r.row_no).collect();
            let c: Vec<&str> = w.collection.iter().map(|r| r.set_num.as_str()).collect();
            let d: Vec<Option<&str>> = w
                .collection
                .iter()
                .map(|r| r.typed_set_num.as_deref())
                .collect();
            let e: Vec<Option<&str>> = w
                .collection
                .iter()
                .map(|r| r.typed_name.as_deref())
                .collect();
            let f: Vec<i32> = w.collection.iter().map(|r| r.quantity).collect();
            let bytes = c
                .iter()
                .chain(d.iter().chain(e.iter()).flatten())
                .map(|s| s.len() as u64)
                .sum::<u64>()
                + 12 * a.len() as u64;
            let sql = q(
                "lego_collection",
                "int4[], int4[], text[], text[], text[], int4[]",
            );
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .bind(d)
                .bind(e)
                .bind(f)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Purchases(w) => {
            let a: Vec<i64> = w.purchases.iter().map(|r| r.purchase_id).collect();
            let b: Vec<i32> = w.purchases.iter().map(|r| r.builder_id).collect();
            let c: Vec<i32> = w.purchases.iter().map(|r| r.row_no).collect();
            let d: Vec<&str> = w.purchases.iter().map(|r| r.set_num.as_str()).collect();
            let e: Vec<&str> = w.purchases.iter().map(|r| r.store).collect();
            let f: Vec<String> = w
                .purchases
                .iter()
                .map(|r| stamp_text(&r.ordered_at))
                .collect();
            let g: Vec<&str> = w
                .purchases
                .iter()
                .map(|r| r.ordered_local.as_str())
                .collect();
            let h: Vec<String> = w
                .purchases
                .iter()
                .map(|r| stamp_text(&r.delivered_at))
                .collect();
            let bytes = d
                .iter()
                .chain(e.iter())
                .chain(g.iter())
                .map(|s| s.len() as u64)
                .sum::<u64>()
                + 32 * a.len() as u64;
            let sql = q(
                "lego_purchases",
                "int8[], int4[], int4[], text[], text[], timestamptz[], text[], timestamptz[]",
            );
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .bind(d)
                .bind(e)
                .bind(f)
                .bind(g)
                .bind(h)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
        Level::Manifest(m) => {
            let a: Vec<String> = m.iter().map(|r| r.trap.to_string()).collect();
            let b: Vec<&str> = m.iter().map(|r| r.origin.name()).collect();
            let c: Vec<&str> = m.iter().map(|r| r.table).collect();
            let d: Vec<&str> = m.iter().map(|r| r.row_key.as_str()).collect();
            let e: Vec<&str> = m.iter().map(|r| r.phase.as_str()).collect();
            let f: Vec<i64> = m.iter().map(|r| r.wave).collect();
            let g: Vec<Option<&str>> = m.iter().map(|r| r.socket.as_deref()).collect();
            let h: Vec<&str> = m.iter().map(|r| r.detail.as_str()).collect();
            let bytes = d
                .iter()
                .chain(h.iter())
                .map(|s| s.len() as u64)
                .sum::<u64>()
                + 40 * a.len() as u64;
            let sql = q(
                "trap_manifest",
                "text[], text[], text[], text[], text[], int8[], text[], text[]",
            );
            let r = sqlx::query(&sql)
                .bind(a)
                .bind(b)
                .bind(c)
                .bind(d)
                .bind(e)
                .bind(f)
                .bind(g)
                .bind(h)
                .execute(&mut **tx)
                .await;
            (sql, bytes, r)
        }
    };
    let _ = sql;
    res.map(|r| (r.rows_affected(), bytes))
}
