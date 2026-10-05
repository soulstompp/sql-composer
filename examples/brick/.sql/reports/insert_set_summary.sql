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
INSERT INTO set_category_summary (set_num, version, category_name, total_parts, total_spare)
SELECT
    $1,
    version,
    category_name,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_spare), 0),
    COALESCE(SUM(quantity) FILTER (WHERE is_spare), 0)
FROM set_part_details
WHERE category_name IS NOT NULL
GROUP BY version, category_name
ON CONFLICT (set_num, version, category_name) DO UPDATE
SET total_parts = EXCLUDED.total_parts,
    total_spare = EXCLUDED.total_spare
