SELECT
    sized.part_num,
    sized.name,
    sized.size[1]::integer AS width,
    sized.size[2]::integer AS length
FROM (
    SELECT p.part_num, p.name, regexp_match(p.name, '^Brick (\d+) x (\d+)$') AS size
    FROM lego_parts p
    WHERE p.name ~ '^Brick (\d+) x (\d+)$'
) sized
ORDER BY width, length, sized.part_num
