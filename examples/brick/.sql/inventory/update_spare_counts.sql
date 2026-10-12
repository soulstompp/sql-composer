WITH set_part_details AS (
    SELECT
    ip.inventory_id,
    i.version,
    ip.part_num,
    ip.color_id,
    ip.is_spare,
    ip.quantity,
    CASE WHEN p.part_num IS NULL THEN 'uncatalogued' ELSE 'catalogued' END AS part_status,
    p.name AS part_name,
    pc.name AS category_name,
    c.name AS color_name,
    c.rgb AS color_rgb,
    c.is_trans
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
LEFT JOIN lego_parts p ON p.part_num = ip.part_num
LEFT JOIN lego_part_categories pc ON pc.id = p.part_cat_id
LEFT JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = $1

)
UPDATE inventory_tracking it
SET
    spare_count = COALESCE(spd.total_spare, 0),
    updated_at = NOW()
FROM (
    SELECT version, part_num, SUM(quantity) FILTER (WHERE is_spare) AS total_spare
    FROM set_part_details
    GROUP BY version, part_num
) spd
WHERE it.set_num = $1
  AND it.version = spd.version
  AND it.part_num = spd.part_num
