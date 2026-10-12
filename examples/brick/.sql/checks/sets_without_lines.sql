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
