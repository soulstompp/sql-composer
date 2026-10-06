SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE to_tsvector('english', p.name) @@ websearch_to_tsquery('english', $1)
