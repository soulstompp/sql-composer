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
WHERE i.set_num = $2

),
filter AS (
    SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = $1

)
SELECT p.*
FROM set_part_details p
WHERE EXISTS (
    SELECT 1
    FROM filter f
    WHERE (f.part_num IS NULL OR f.part_num = p.part_num)
      AND (f.color_id IS NULL OR f.color_id = p.color_id)
)

