SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)
