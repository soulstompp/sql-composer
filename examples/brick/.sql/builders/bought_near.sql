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
area AS (
    SELECT
    b.builder_id,
    earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
        AS metres
FROM lego_cities c
JOIN lego_builders b
  ON earth_box(ll_to_earth(c.latitude, c.longitude), $2 * 1000)
     @> ll_to_earth(b.latitude, b.longitude)
WHERE c.city_id = $1
  AND earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
      <= $2 * 1000

)
SELECT
    sr.root_id,
    sr.root_name,
    count(*) AS purchases,
    count(DISTINCT ps.builder_id) AS builders
FROM purchase_sets ps
JOIN set_roots sr ON sr.set_num = ps.set_num
WHERE ps.builder_id IN (SELECT a.builder_id FROM area a)
GROUP BY sr.root_id, sr.root_name
ORDER BY purchases DESC, sr.root_id

