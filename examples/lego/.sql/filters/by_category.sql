SELECT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_parts p ON p.part_num = ip.part_num
JOIN lego_part_categories pc ON pc.id = p.part_cat_id
WHERE pc.name = $1
