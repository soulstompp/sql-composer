SELECT * FROM (
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.theme_id
FROM (
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

) tc
WHERE tc.ancestor_id = $2

    ) sc
)
) AS _intersect_1
INTERSECT
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.theme_id
FROM (
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

) tc
WHERE tc.ancestor_id = $1

    ) sc
)
) AS _intersect_2
) AS _intersect
