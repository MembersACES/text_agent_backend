-- Per-lane figures_mode on sequence templates.
--   comparison — current EmailAgent behaviour (proposal / savings sentence)
--   none       — no figures; do not refer to a proposal
--
-- gci_outbound_v1 is seeded to none only when the column is still empty, so a
-- later operator change is not overwritten on restart.

ALTER TABLE autonomous_sequence_templates
    ADD COLUMN IF NOT EXISTS figures_mode VARCHAR(32);

UPDATE autonomous_sequence_templates
   SET figures_mode = 'none'
 WHERE sequence_type = 'gci_outbound_v1'
   AND (figures_mode IS NULL OR TRIM(figures_mode) = '');

UPDATE autonomous_sequence_templates
   SET figures_mode = 'comparison'
 WHERE figures_mode IS NULL OR TRIM(figures_mode) = '';
