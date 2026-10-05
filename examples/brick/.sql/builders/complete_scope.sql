WITH scope_sets AS (
    SELECT s.set_num
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
    WHERE tc.ancestor_id = $1
)
SELECT b.builder_id, b.name
FROM lego_builders b
WHERE EXISTS (SELECT 1 FROM scope_sets)
  AND NOT EXISTS (
    SELECT set_num FROM scope_sets
    EXCEPT
    SELECT c.set_num FROM lego_collection c WHERE c.builder_id = b.builder_id
)
ORDER BY b.builder_id
