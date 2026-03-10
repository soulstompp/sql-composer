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
SELECT
    part_name,
    category_name,
    color_name,
    color_rgb,
    quantity,
    is_spare
FROM set_part_details
ORDER BY category_name, part_name, color_name
