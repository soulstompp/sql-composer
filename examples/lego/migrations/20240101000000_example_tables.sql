-- Extra tables for the lego example.
-- Base Lego data comes from the neon sample dump, not this migration.

CREATE TABLE IF NOT EXISTS set_category_summary (
    id SERIAL PRIMARY KEY,
    set_num VARCHAR(255) NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    total_parts INTEGER NOT NULL DEFAULT 0,
    total_spare INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_tracking (
    id SERIAL PRIMARY KEY,
    set_num VARCHAR(255) NOT NULL,
    part_num VARCHAR(255) NOT NULL,
    spare_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (set_num, part_num)
);
