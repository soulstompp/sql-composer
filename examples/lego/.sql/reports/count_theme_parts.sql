SELECT COUNT(DISTINCT part_num) FROM (
SELECT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
JOIN lego_themes t ON t.id = s.theme_id
WHERE t.name = $1

) AS _count_sub
