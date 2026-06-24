-- C&I electricity time-of-use (TOU) rates on offers: current (invoiced/contracted) + new (offered),
-- per peak / shoulder / off-peak, in c/kWh. shoulder is null for peak/off-peak-only sites; all null
-- for non-electricity offers (gas keeps using the blended contracted_rate / offer_rate).
-- Run once on PostgreSQL (e.g. Cloud SQL). SQLite/dev picks these up via database.py init_db().

ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS current_peak_rate double precision;
ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS current_shoulder_rate double precision;
ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS current_offpeak_rate double precision;
ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS new_peak_rate double precision;
ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS new_shoulder_rate double precision;
ALTER TABLE public.offers ADD COLUMN IF NOT EXISTS new_offpeak_rate double precision;

COMMENT ON COLUMN public.offers.current_peak_rate IS 'C&I electricity current peak rate (c/kWh).';
COMMENT ON COLUMN public.offers.current_shoulder_rate IS 'C&I electricity current shoulder rate (c/kWh); null if not applicable.';
COMMENT ON COLUMN public.offers.current_offpeak_rate IS 'C&I electricity current off-peak rate (c/kWh).';
COMMENT ON COLUMN public.offers.new_peak_rate IS 'C&I electricity offered/new peak rate (c/kWh).';
COMMENT ON COLUMN public.offers.new_shoulder_rate IS 'C&I electricity offered/new shoulder rate (c/kWh); null if not applicable.';
COMMENT ON COLUMN public.offers.new_offpeak_rate IS 'C&I electricity offered/new off-peak rate (c/kWh).';
