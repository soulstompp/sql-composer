SELECT n.builder_id, n.metres
FROM lego_cities c
CROSS JOIN LATERAL (
    SELECT
        b.builder_id,
        earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
            AS metres
    FROM lego_builders b
    ORDER BY ll_to_earth(b.latitude, b.longitude) <-> ll_to_earth(c.latitude, c.longitude)
    LIMIT $2
) n
WHERE c.city_id = $1
ORDER BY n.metres, n.builder_id
