SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE p.name ~* $1
