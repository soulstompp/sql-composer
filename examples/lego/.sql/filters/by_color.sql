SELECT part_num
FROM lego_inventory_parts ip
JOIN lego_colors c ON c.id = ip.color_id
WHERE c.name = $1
