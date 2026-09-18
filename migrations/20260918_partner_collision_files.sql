-- Collision file refs for staff adjudication; audit detail for those Drive ids.

ALTER TABLE partner_lead_collisions ADD COLUMN IF NOT EXISTS files_json TEXT;
ALTER TABLE partner_audit_events ADD COLUMN IF NOT EXISTS detail_json TEXT;
