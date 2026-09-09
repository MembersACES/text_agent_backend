-- Aliases so a sequence template can be linked to product pages without renaming
-- its call key. JSON array of sequence_type strings, e.g.
-- ["solar_panel_cleaning_followup_v1"].
--
-- Safe to run more than once. The app also adds the column on first write if
-- missing, so behaviour is unchanged until this runs.

ALTER TABLE autonomous_sequence_templates
    ADD COLUMN IF NOT EXISTS linked_flow_keys TEXT;
