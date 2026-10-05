WITH set_roots AS (
    SELECT
    s.set_num,
    s.name,
    s.year,
    s.num_parts,
    r.id AS root_id,
    r.name AS root_name
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
JOIN lego_themes r ON r.id = tc.ancestor_id
WHERE r.parent_id IS NULL

)
SELECT
    root_id,
    root_name,
    count(*) AS sets,
    count(num_parts) AS sized,
    min(num_parts) AS smallest,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY num_parts) AS median,
    max(num_parts) AS largest
FROM set_roots
GROUP BY root_id, root_name
ORDER BY root_id
