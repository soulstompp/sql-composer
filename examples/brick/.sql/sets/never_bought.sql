WITH purchase_sets AS (
    SELECT
    p.purchase_id,
    p.builder_id,
    c.set_num,
    p.ordered_at,
    p.delivered_at
FROM lego_purchases p
JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no

)
SELECT s.set_num, s.name, s.year, s.theme_id
FROM lego_sets s
WHERE NOT EXISTS (
    SELECT 1
    FROM purchase_sets ps
    WHERE ps.set_num = s.set_num
)
ORDER BY s.set_num
