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

),
versions AS (
    SELECT
        version,
        inventory_id,
        jsonb_agg(
            jsonb_build_object(
                'part_num', part_num,
                'color_id', color_id,
                'is_spare', is_spare,
                'quantity', quantity,
                'part_name', part_name,
                'color_name', color_name
            )
            ORDER BY part_num, color_id, is_spare
        ) AS lines
    FROM set_part_details
    GROUP BY version, inventory_id
)
SELECT
    s.set_num,
    jsonb_build_object(
        'set_num', s.set_num,
        'name', s.name,
        'year', s.year,
        'theme', t.name,
        'versions', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'version', v.version,
                    'inventory_id', v.inventory_id,
                    'lines', v.lines
                )
                ORDER BY v.version
            )
            FROM versions v
        )
    ) AS document
FROM lego_sets s
LEFT JOIN lego_themes t ON t.id = s.theme_id
WHERE s.set_num = $1
