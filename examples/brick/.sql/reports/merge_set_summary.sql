MERGE INTO set_category_summary sc
USING (
    SELECT $1::varchar AS set_num, t.*
    FROM (
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
SELECT
    version,
    category_name,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_spare), 0) AS total_parts,
    COALESCE(SUM(quantity) FILTER (WHERE is_spare), 0) AS total_spare
FROM set_part_details
WHERE category_name IS NOT NULL
GROUP BY version, category_name

    ) t
) src
ON sc.set_num = src.set_num
   AND sc.version = src.version
   AND sc.category_name = src.category_name
WHEN MATCHED THEN
    UPDATE SET total_parts = src.total_parts, total_spare = src.total_spare
WHEN NOT MATCHED THEN
    INSERT (set_num, version, category_name, total_parts, total_spare)
    VALUES (src.set_num, src.version, src.category_name, src.total_parts, src.total_spare)
