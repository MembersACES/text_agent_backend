-- Signed-via-ACES contract fields on CRM clients (sheet sync; stage unchanged by sync).
-- Run once on PostgreSQL (e.g. Cloud SQL). SQLite dev picks these up via database.py ensure_columns.

ALTER TABLE public.clients
  ADD COLUMN IF NOT EXISTS has_signed_contract smallint NOT NULL DEFAULT 0;

ALTER TABLE public.clients
  ADD COLUMN IF NOT EXISTS signed_contract_utilities text;

ALTER TABLE public.clients
  ADD COLUMN IF NOT EXISTS signed_contract_checked_at timestamp without time zone;

COMMENT ON COLUMN public.clients.has_signed_contract IS
  '1 when FILE_IDS sheet shows Signed via ACES for any utility; updated by admin sync only.';

COMMENT ON COLUMN public.clients.signed_contract_utilities IS
  'JSON array of utility labels with Signed via ACES status from sheet sync.';

COMMENT ON COLUMN public.clients.signed_contract_checked_at IS
  'UTC timestamp of last signed-contract sync for this client.';
