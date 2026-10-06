SELECT
    p.part_num,
    p.name,
    ts_rank(to_tsvector('english', p.name), websearch_to_tsquery('english', $2)) AS rank,
    ts_headline(
        'english',
        p.name,
        websearch_to_tsquery('english', $2),
        'StartSel=*, StopSel=*, HighlightAll=true'
    ) AS marked
FROM lego_parts p
WHERE to_tsvector('english', p.name) @@ websearch_to_tsquery('english', $2)
ORDER BY rank DESC, p.part_num
LIMIT $1
