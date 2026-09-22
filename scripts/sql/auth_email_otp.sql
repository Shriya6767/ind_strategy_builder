-- ============================================================================
-- Upgrade for installs that ran an earlier auth_migration.sql (phone-based
-- signup). Idempotent. Fresh installs only need auth_migration.sql.
-- ============================================================================
ALTER TABLE app_user ADD COLUMN IF NOT EXISTS is_email_verified BOOLEAN NOT NULL DEFAULT FALSE;

-- Google accounts were marked phone-verified as a stand-in; they are email-verified.
UPDATE app_user SET is_email_verified = TRUE
WHERE google_sub IS NOT NULL
   OR (EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'app_user' AND column_name = 'is_phone_verified'));

-- Phone numbers are no longer collected.
ALTER TABLE app_user DROP COLUMN IF EXISTS phone;
ALTER TABLE app_user DROP COLUMN IF EXISTS is_phone_verified;
ALTER TABLE app_user ALTER COLUMN email SET NOT NULL;

-- otp_verification.phone held the number the code went to; it now holds
-- the email address, so name it for what it is.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'otp_verification' AND column_name = 'phone') THEN
        ALTER TABLE otp_verification RENAME COLUMN phone TO target;
    END IF;
END $$;
ALTER TABLE otp_verification ALTER COLUMN target TYPE VARCHAR(255);
DROP INDEX IF EXISTS idx_otp_phone_purpose;
CREATE INDEX IF NOT EXISTS idx_otp_target_purpose ON otp_verification (target, purpose, created_at DESC);
