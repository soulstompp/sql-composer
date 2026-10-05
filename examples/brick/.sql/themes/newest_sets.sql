SELECT
    r.id AS root_id,
    r.name AS root_name,
    n.set_num,
    n.name,
    n.year
FROM lego_themes r
CROSS JOIN LATERAL (
    SELECT s.set_num, s.name, s.year
    FROM lego_sets s
    JOIN (
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

    ) tc ON tc.theme_id = s.theme_id
    WHERE tc.ancestor_id = r.id
    ORDER BY s.year DESC NULLS LAST, s.set_num DESC
    LIMIT 3
) n
WHERE r.parent_id IS NULL
ORDER BY r.id, n.year DESC NULLS LAST, n.set_num DESC
