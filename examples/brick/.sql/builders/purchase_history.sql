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
SELECT
    row_number() OVER w AS nth,
    ps.purchase_id,
    ps.ordered_at,
    ps.set_num,
    s.name,
    s.num_parts,
    sum(s.num_parts) OVER w AS pieces_so_far
FROM purchase_sets ps
JOIN lego_sets s ON s.set_num = ps.set_num
WHERE ps.builder_id = $1
WINDOW w AS (ORDER BY ps.ordered_at, ps.purchase_id)
ORDER BY nth
