SELECT COUNT(DISTINCT part_num) FROM (
SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
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
WHERE tc.ancestor_id = $1

) AS _count_sub
