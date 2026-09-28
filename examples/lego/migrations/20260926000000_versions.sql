-- A set LEGO sold in several versions has one inventory per version, and the example's tables keep
-- the versions apart: one summary row per (set, version, category) and one tracking row per
-- (set, version, part). Rows written before this migration added the versions of a set together,
-- so they are removed rather than labelled with a version they never had; `summary` and `spares`
-- rebuild them. Safe to run again: a table that already has its version column is left alone.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'set_category_summary'
          AND column_name = 'version'
    ) THEN
        TRUNCATE set_category_summary;
        ALTER TABLE set_category_summary ADD COLUMN version INTEGER NOT NULL;
        ALTER TABLE set_category_summary
            ADD CONSTRAINT set_category_summary_set_num_version_category_name_key
            UNIQUE (set_num, version, category_name);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'inventory_tracking'
          AND column_name = 'version'
    ) THEN
        TRUNCATE inventory_tracking;
        ALTER TABLE inventory_tracking DROP CONSTRAINT IF EXISTS inventory_tracking_set_num_part_num_key;
        ALTER TABLE inventory_tracking ADD COLUMN version INTEGER NOT NULL;
        ALTER TABLE inventory_tracking
            ADD CONSTRAINT inventory_tracking_set_num_version_part_num_key
            UNIQUE (set_num, version, part_num);
    END IF;
END $$;
