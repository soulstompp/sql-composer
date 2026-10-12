WITH purchase_sets AS (
    SELECT
    p.purchase_id,
    p.builder_id,
    c.set_num,
    p.ordered_at,
    p.delivered_at
FROM lego_purchases p
JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no

),
set_roots AS (
    SELECT
    s.set_num,
    s.name,
    s.year,
    s.num_parts,
    r.id AS root_id,
    r.name AS root_name
FROM lego_sets s
JOIN (
    WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle

) tc ON tc.theme_id = s.theme_id
JOIN lego_themes r ON r.id = tc.ancestor_id
WHERE r.parent_id IS NULL

),
dated AS (
    SELECT
        extract(year FROM ps.ordered_at AT TIME ZONE 'UTC')::integer AS year,
        sr.root_id,
        sr.root_name
    FROM purchase_sets ps
    JOIN set_roots sr ON sr.set_num = ps.set_num
)
SELECT
    CASE
        WHEN GROUPING(year) = 1 THEN 'total'
        WHEN GROUPING(root_id) = 1 THEN 'year'
        ELSE 'theme'
    END AS level,
    year,
    root_id,
    root_name,
    count(*) AS purchases
FROM dated
WHERE year BETWEEN $1 AND $2
GROUP BY ROLLUP (year, (root_id, root_name))
ORDER BY year NULLS LAST, root_id NULLS LAST
