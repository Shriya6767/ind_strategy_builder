-- Strategy-level "Stop Monitoring After" (AlgoTest): from stop_monitoring_time to the end of
-- each day the strategy is not monitored (no SL/target/trailing/overall check, no entry or
-- re-entry, pending orders cancelled); open legs still exit at exit_time.
-- Idempotent: safe to run again.
ALTER TABLE strategy ADD COLUMN IF NOT EXISTS is_stop_monitoring   BOOLEAN DEFAULT FALSE;
ALTER TABLE strategy ADD COLUMN IF NOT EXISTS stop_monitoring_time TIME;
