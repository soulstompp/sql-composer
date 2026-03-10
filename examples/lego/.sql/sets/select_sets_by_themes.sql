SELECT
    s.set_num,
    s.name,
    s.year,
    t.name AS theme_name,
    s.num_parts
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE s.theme_id IN ($2)
  AND s.year >= $1
ORDER BY s.year DESC, s.num_parts DESC
