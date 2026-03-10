WITH set_part_details AS (
    SELECT
        ip.part_num,
        p.name AS part_name,
        pc.name AS category_name,
        c.name AS color_name,
        c.rgb AS color_rgb,
        c.is_trans,
        ip.quantity,
        ip.is_spare
    FROM lego_inventory_parts ip
    JOIN lego_inventories i ON i.id = ip.inventory_id
    JOIN lego_parts p ON p.part_num = ip.part_num
    JOIN lego_part_categories pc ON pc.id = p.part_cat_id
    JOIN lego_colors c ON c.id = ip.color_id
    WHERE i.set_num = $1
)

UPDATE inventory_tracking it
SET
    spare_count = spd.total_spare,
    updated_at = NOW()
FROM (
    SELECT part_num, SUM(quantity) AS total_spare
    FROM set_part_details
    WHERE is_spare
    GROUP BY part_num
) spd
WHERE it.part_num = spd.part_num
  AND it.set_num = $1
