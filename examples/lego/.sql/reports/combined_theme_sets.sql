SELECT s.set_num, s.name, s.year, s.num_parts, 'Technic' AS theme_group
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE t.name = $3
  AND s.year >= $2
UNION
SELECT s.set_num, s.name, s.year, s.num_parts, 'City' AS theme_group
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE t.name = $1
  AND s.year >= $2

