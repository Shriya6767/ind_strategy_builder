-- Portfolio-level overall stop-loss / target (squares off EVERY strategy in
-- the portfolio the minute the day's combined MTM hits the level).
-- Idempotent: safe to run again.
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS is_portfolio_sl                BOOLEAN     DEFAULT FALSE;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_sl_type              VARCHAR(20);
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_sl_mtm_value         NUMERIC;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_sl_percent_value     NUMERIC;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS is_portfolio_target            BOOLEAN     DEFAULT FALSE;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_target_type          VARCHAR(20);
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_target_mtm_value     NUMERIC;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS portfolio_target_percent_value NUMERIC;
