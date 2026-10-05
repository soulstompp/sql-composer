SELECT y.year, count(s.set_num) AS sets
FROM (
    SELECT min(year) AS first_year, max(year) AS last_year
    FROM lego_sets
) span
CROSS JOIN generate_series(span.first_year, span.last_year) AS y(year)
LEFT JOIN lego_sets s ON s.year = y.year
GROUP BY y.year
ORDER BY y.year
