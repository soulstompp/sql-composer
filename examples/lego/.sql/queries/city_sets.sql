SELECT s.set_num, s.name, s.year, s.num_parts, scope.name AS theme_group
FROM lego_sets s
JOIN lego_themes scope ON scope.id = $1
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.theme_id
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
WHERE tc.ancestor_id = $1

    ) sc
)
  AND s.year >= $2
