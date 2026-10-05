SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
JOIN lego_part_categories pc ON pc.id = p.part_cat_id
WHERE pc.name = $1
