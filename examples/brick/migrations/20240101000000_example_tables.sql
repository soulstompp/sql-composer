-- The example's own tables, beside the catalogue sqlc-brickgen writes. A set sold in several
-- versions has one inventory per version, and both tables keep the versions apart: one summary row
-- per (set, version, category) and one tracking row per (set, version, part).

CREATE TABLE IF NOT EXISTS set_category_summary (
    id SERIAL PRIMARY KEY,
    set_num VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    total_parts INTEGER NOT NULL DEFAULT 0,
    total_spare INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (set_num, version, category_name)
);

CREATE TABLE IF NOT EXISTS inventory_tracking (
    id SERIAL PRIMARY KEY,
    set_num VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    part_num VARCHAR(255) NOT NULL,
    spare_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (set_num, version, part_num)
);
