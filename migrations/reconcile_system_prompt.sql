-- Reconcile autonomous_sequence_type prompt columns onto `system_prompt`.
-- The CRM Email tab writes `system_prompt`. Older worker models expected
-- `email_system_prompt` / `sms_system_prompt`, which do not exist in the
-- sqlite schema and must not remain the read path.
--
-- Safe to run more than once. Does not drop the legacy columns if they exist;
-- the worker no longer reads them.

ALTER TABLE autonomous_sequence_type
    ADD COLUMN IF NOT EXISTS system_prompt TEXT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'autonomous_sequence_type'
           AND column_name = 'email_system_prompt'
    ) THEN
        UPDATE autonomous_sequence_type
           SET system_prompt = email_system_prompt
         WHERE (system_prompt IS NULL OR TRIM(system_prompt) = '')
           AND email_system_prompt IS NOT NULL
           AND TRIM(email_system_prompt) <> '';
    END IF;
END $$;
