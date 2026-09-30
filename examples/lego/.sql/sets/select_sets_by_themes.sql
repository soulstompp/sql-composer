SELECT
    s.set_num,
    s.name,
    s.year,
    t.name AS theme_name,
    s.num_parts
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE EXISTS (
    SELECT 1
    FROM (
        WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle

    ) tc
    WHERE tc.theme_id = s.theme_id
      AND tc.ancestor_id IN ($2)
)
  AND s.year >= $1
ORDER BY s.year DESC, s.num_parts DESC, s.set_num
