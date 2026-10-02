SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = $1
