SELECT * FROM (
SELECT * FROM (
SELECT r.rule, p.set_num, p.detail
FROM (
    SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)

) r
LEFT JOIN (
    SELECT
        i.set_num,
        format('inventory %s, version %s: part %s in colour %s%s',
            ip.inventory_id, i.version, ip.part_num, ip.color_id,
            CASE WHEN ip.is_spare THEN ', a spare' ELSE '' END) AS detail
    FROM lego_inventory_parts ip
    JOIN lego_inventories i ON i.id = ip.inventory_id
    WHERE NOT EXISTS (SELECT 1 FROM lego_parts pt WHERE pt.part_num = ip.part_num)
) p ON true
WHERE r.slug = 'lines_uncatalogued'
) AS _union_1
UNION ALL
SELECT * FROM (
SELECT r.rule, p.set_num, p.detail
FROM (
    SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)

) r
LEFT JOIN (
    SELECT s.set_num, s.name AS detail
    FROM lego_sets s
    WHERE NOT EXISTS (
        SELECT 1
        FROM lego_inventories i
        WHERE i.set_num = s.set_num
          AND EXISTS (SELECT 1 FROM lego_inventory_parts ip WHERE ip.inventory_id = i.id)
    )
) p ON true
WHERE r.slug = 'sets_without_lines'
) AS _union_2
) AS _union
