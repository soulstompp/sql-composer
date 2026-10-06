SELECT
    b.builder_id,
    earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
        AS metres
FROM lego_cities c
JOIN lego_builders b
  ON earth_box(ll_to_earth(c.latitude, c.longitude), $2 * 1000)
     @> ll_to_earth(b.latitude, b.longitude)
WHERE c.city_id = $1
  AND earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
      <= $2 * 1000
