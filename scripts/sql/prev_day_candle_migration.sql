-- Leg-level previous-day candle entry filter: the leg enters only when yesterday's Sensex
-- candle (close vs open) has the chosen colour -- green (close >= open) or red (close <= open).
-- Idempotent: safe to run again.
ALTER TABLE leg_details ADD COLUMN IF NOT EXISTS is_prev_day_candle BOOLEAN    DEFAULT FALSE;
ALTER TABLE leg_details ADD COLUMN IF NOT EXISTS prev_day_candle    VARCHAR(5);
