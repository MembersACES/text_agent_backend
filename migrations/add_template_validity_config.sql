-- Offer validity configuration per sequence template.
--
-- Before this, validity was a hardcoded "anchor + 7 days" in five code paths. That
-- meant the system presented clients with a deadline no retailer had set, and a
-- sequence restart silently moved a deadline the client had already been given.
--
-- validity_mode:
--   'none'          - never mention validity
--   'retailer_date' - use only a date a human supplied; never invent one
--   'fixed_days'    - anchor + validity_days (previous behaviour, N was 7)
--
-- Safe to run more than once. The application defaults to ('fixed_days', 7) when
-- these columns are absent, so behaviour is unchanged until this migration runs.

ALTER TABLE autonomous_sequence_templates
    ADD COLUMN IF NOT EXISTS validity_mode VARCHAR(20) NOT NULL DEFAULT 'fixed_days';

ALTER TABLE autonomous_sequence_templates
    ADD COLUMN IF NOT EXISTS validity_days INTEGER NOT NULL DEFAULT 7;

UPDATE autonomous_sequence_templates
   SET validity_mode = 'fixed_days'
 WHERE validity_mode IS NULL OR TRIM(validity_mode) = '';

UPDATE autonomous_sequence_templates
   SET validity_days = 7
 WHERE validity_days IS NULL OR validity_days < 1;
