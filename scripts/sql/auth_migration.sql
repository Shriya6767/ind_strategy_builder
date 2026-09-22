-- ============================================================================
-- Authentication + per-user ownership.  Idempotent: safe to run twice.
-- Run on the host:  psql -h <host> -U <user> -d <db> -f scripts/sql/auth_migration.sql
-- ============================================================================

-- One row per account. A user signs up with email+password (verified by a
-- code sent to the email) or with Google (then password is NULL and
-- google_sub is set).
CREATE TABLE IF NOT EXISTS app_user (
    user_id            BIGSERIAL PRIMARY KEY,
    email              VARCHAR(255) UNIQUE NOT NULL,
    name               VARCHAR(100) NOT NULL,
    password_hash      VARCHAR(100),                     -- bcrypt hash, never the password
    google_sub         VARCHAR(64)  UNIQUE,              -- Google's permanent id for the account
    is_email_verified  BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    last_login_at      TIMESTAMP
);

-- One-time codes for signup verification and password reset. `target` is
-- the email the code was sent to. The code itself is never stored -- only
-- its keyed hash -- so a DB leak does not leak live codes. Rows expire
-- after a few minutes and are single-use.
CREATE TABLE IF NOT EXISTS otp_verification (
    otp_id      BIGSERIAL PRIMARY KEY,
    target      VARCHAR(255) NOT NULL,
    purpose     VARCHAR(20)  NOT NULL,                   -- 'signup' | 'reset_password'
    otp_hash    VARCHAR(128) NOT NULL,
    expires_at  TIMESTAMP    NOT NULL,
    attempts    INTEGER      NOT NULL DEFAULT 0,          -- wrong guesses so far
    consumed    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_otp_target_purpose ON otp_verification (target, purpose, created_at DESC);

-- Ownership: every strategy and portfolio belongs to a user.
ALTER TABLE strategy  ADD COLUMN IF NOT EXISTS user_id BIGINT REFERENCES app_user(user_id);
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS user_id BIGINT REFERENCES app_user(user_id);
CREATE INDEX IF NOT EXISTS idx_strategy_user  ON strategy  (user_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_user ON portfolio (user_id);

-- Rows created before authentication existed have user_id NULL and are
-- invisible to everyone. After creating your own account, adopt them:
--   UPDATE strategy  SET user_id = <your user_id> WHERE user_id IS NULL;
--   UPDATE portfolio SET user_id = <your user_id> WHERE user_id IS NULL;
