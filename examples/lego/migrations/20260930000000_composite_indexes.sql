-- The composite indexes a DBA gives the dump's tables for the joins the example's queries make.
-- Each is led by the column the join into its table fixes, and ends on the column the next join
-- reads or holds it in INCLUDE. The primary keys stay as they are. Safe to run again.

CREATE INDEX IF NOT EXISTS lego_themes_parent_id_id_idx ON lego_themes (parent_id, id);
CREATE INDEX IF NOT EXISTS lego_sets_theme_id_set_num_idx ON lego_sets (theme_id, set_num);
CREATE INDEX IF NOT EXISTS lego_sets_theme_id_year_idx ON lego_sets (theme_id, year) INCLUDE (set_num);
CREATE INDEX IF NOT EXISTS lego_inventories_set_num_version_id_idx
    ON lego_inventories (set_num, version, id);
CREATE INDEX IF NOT EXISTS lego_inventory_sets_inventory_id_set_num_idx
    ON lego_inventory_sets (inventory_id, set_num);
CREATE INDEX IF NOT EXISTS lego_inventory_parts_inventory_id_part_num_color_id_idx
    ON lego_inventory_parts (inventory_id, part_num, color_id);
CREATE INDEX IF NOT EXISTS lego_parts_part_cat_id_part_num_idx ON lego_parts (part_cat_id, part_num) INCLUDE (name);
