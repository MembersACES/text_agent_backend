-- Optional: add flag when retell_agent_id was auto-copied from another sequence type.
-- Run once on PostgreSQL (e.g. Cloud SQL). App works without this column; GET/PATCH ignore it if missing.

ALTER TABLE public.autonomous_sequence_type
  ADD COLUMN IF NOT EXISTS retell_agent_copied smallint NOT NULL DEFAULT 0;

COMMENT ON COLUMN public.autonomous_sequence_type.retell_agent_copied IS
  '1 = retell_agent_id was seeded by copying another type; clear by changing the ID or PATCH retell_agent_reviewed.';
