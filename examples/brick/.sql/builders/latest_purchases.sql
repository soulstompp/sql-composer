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
SELECT DISTINCT ON (ps.builder_id)
    ps.builder_id,
    b.name AS builder_name,
    ps.purchase_id,
    ps.ordered_at,
    ps.set_num
FROM purchase_sets ps
JOIN lego_builders b ON b.builder_id = ps.builder_id
ORDER BY ps.builder_id, ps.ordered_at DESC, ps.purchase_id DESC
