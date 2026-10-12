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
