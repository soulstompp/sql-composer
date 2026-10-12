SELECT b.builder_id
FROM lego_builders b
JOIN lego_streets st ON st.street_id = b.street_id
JOIN lego_postcodes pc ON pc.postcode_id = st.postcode_id
WHERE pc.code ~ $1
