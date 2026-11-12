-- Leg-level premium-diff entry filter (straddle on today's first candle vs yesterday's close).
-- Idempotent: safe to run again.
ALTER TABLE leg_details ADD COLUMN IF NOT EXISTS is_premium_diff       BOOLEAN     DEFAULT FALSE;
ALTER TABLE leg_details ADD COLUMN IF NOT EXISTS premium_diff_mode     VARCHAR(10);
ALTER TABLE leg_details ADD COLUMN IF NOT EXISTS premium_diff_operator VARCHAR(2);
