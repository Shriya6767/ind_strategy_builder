-- Portfolio-level backtest settings (index, quantity multiplier, period selection, slippage).
-- Idempotent: safe to run again.
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS index_name       VARCHAR(30) DEFAULT 'sensex';
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS qty_multiplier   INTEGER     DEFAULT 1;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS period_selection JSONB;
ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS slippage         NUMERIC     DEFAULT 0;
