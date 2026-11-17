-- 2026-11-17: execution settings lost monitoring_frequency_sec, order_timeout_sec,
-- exit_fallback_market, max_daily_loss, paper_slippage_pct, auto_restart, and the
-- margin toggle was renamed squareoff_on_entry_error -> squareoff_on_margin_error.
-- ExecutionSettings.from_dict rejects unknown keys, so rows saved before that date
-- must be rewritten once. Safe to re-run.

UPDATE live_execution_setting
SET settings = (settings::jsonb
                - 'monitoring_frequency_sec' - 'order_timeout_sec' - 'exit_fallback_market'
                - 'max_daily_loss' - 'paper_slippage_pct' - 'auto_restart')
WHERE settings::jsonb ?| array['monitoring_frequency_sec', 'order_timeout_sec', 'exit_fallback_market',
                               'max_daily_loss', 'paper_slippage_pct', 'auto_restart'];

UPDATE live_execution_setting
SET settings = (settings::jsonb - 'squareoff_on_entry_error')
               || jsonb_build_object('squareoff_on_margin_error', settings::jsonb -> 'squareoff_on_entry_error')
WHERE settings::jsonb ? 'squareoff_on_entry_error';

UPDATE live_deployment
SET settings = (settings::jsonb
                - 'monitoring_frequency_sec' - 'order_timeout_sec' - 'exit_fallback_market'
                - 'max_daily_loss' - 'paper_slippage_pct' - 'auto_restart')
WHERE settings::jsonb ?| array['monitoring_frequency_sec', 'order_timeout_sec', 'exit_fallback_market',
                               'max_daily_loss', 'paper_slippage_pct', 'auto_restart'];

UPDATE live_deployment
SET settings = (settings::jsonb - 'squareoff_on_entry_error')
               || jsonb_build_object('squareoff_on_margin_error', settings::jsonb -> 'squareoff_on_entry_error')
WHERE settings::jsonb ? 'squareoff_on_entry_error';
