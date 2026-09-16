from src.core.config import Database
from src.core.logger import get_logger
from src.services.save_strategy import insert_legs

logger = get_logger(__name__)


class UpdateStrategyService:
    @staticmethod
    def update_strategy(request):
        conn = None
        cursor = None

        try:
            strategy = request["strategy"]
            strategy_id = strategy.get("strategy_id")
            if strategy_id is None:
                return {"status": False, "message": "strategy_id is required to update a strategy.", "not_found": False}

            conn = Database.get_connection()
            cursor = conn.cursor()

            version = strategy.get("version") or None
            if not version:
                cursor.execute(
                    "SELECT MAX(version) FROM strategy WHERE strategy_id = %s;",
                    (strategy_id,)
                )
                version = cursor.fetchone()[0]
                if version is None:
                    return {
                        "status": False,
                        "message": f"Strategy not found for strategy_id={strategy_id}.",
                        "not_found": True,
                    }

            # Lock the row for the duration of the transaction so two
            # concurrent updates of the same version cannot interleave.
            cursor.execute(
                "SELECT id FROM strategy WHERE strategy_id = %s AND version = %s FOR UPDATE;",
                (strategy_id, version)
            )
            if cursor.fetchone() is None:
                return {
                    "status": False,
                    "message": f"Strategy not found for strategy_id={strategy_id}, version={version}.",
                    "not_found": True,
                }

            cursor.execute(
                """
                UPDATE strategy SET
                    strategy_name = %s,
                    symbol = %s,
                    start_date = %s,
                    end_date = %s,
                    dte_filter = %s,
                    underlying_type = %s,
                    is_squareoff = %s,
                    is_trail_sl_break_even = %s,
                    trail_sl_break_even_type = %s,
                    strategy_type = %s,
                    entry_time = %s,
                    entry_delay = %s,
                    exit_time = %s,
                    exit_delay = %s,
                    is_delay_restart = %s,
                    delay_restart_time = %s,
                    positional_expire_on = %s,
                    positional_entry_day = %s,
                    positional_exit_day = %s,
                    is_strategy_sl = %s,
                    strategy_sl_type = %s,
                    strategy_sl_value = %s,
                    is_strategy_target = %s,
                    strategy_target_type = %s,
                    strategy_target_value = %s,
                    is_overall_reentry_sl = %s,
                    overall_reentry_sl_type = %s,
                    overall_reentry_sl_value = %s,
                    is_overall_reentry_target = %s,
                    overall_reentry_target_type = %s,
                    overall_reentry_target_value = %s,
                    is_overall_trail_sl = %s,
                    overall_trail_sl_type = %s,
                    overall_instrument_move = %s,
                    overall_stoploss_move = %s,
                    leg_count = %s
                WHERE strategy_id = %s AND version = %s;
                """,
                (
                    strategy["strategy_name"],
                    strategy["symbol"],
                    strategy["start_date"],
                    strategy["end_date"],
                    strategy.get("dte_filter"),
                    strategy.get("underlying_type"),
                    strategy.get("is_squareoff"),
                    strategy.get("is_trail_sl_break_even"),
                    strategy.get("trail_sl_break_even_type"),
                    strategy["strategy_type"],
                    strategy["entry_time"],
                    strategy.get("entry_delay"),
                    strategy["exit_time"],
                    strategy.get("exit_delay"),
                    strategy.get("is_delay_restart", False),
                    strategy.get("delay_restart_time"),
                    strategy.get("positional_expire_on"),
                    strategy.get("positional_entry_day"),
                    strategy.get("positional_exit_day"),
                    strategy.get("is_strategy_sl"),
                    strategy.get("strategy_sl_type"),
                    strategy.get("strategy_sl_value"),
                    strategy.get("is_strategy_target"),
                    strategy.get("strategy_target_type"),
                    strategy.get("strategy_target_value"),
                    strategy.get("is_overall_reentry_sl"),
                    strategy.get("overall_reentry_sl_type"),
                    strategy.get("overall_reentry_sl_value"),
                    strategy.get("is_overall_reentry_target"),
                    strategy.get("overall_reentry_target_type"),
                    strategy.get("overall_reentry_target_value"),
                    strategy.get("is_overall_trail_sl"),
                    strategy.get("overall_trail_sl_type"),
                    strategy.get("overall_instrument_move"),
                    strategy.get("overall_stoploss_move"),
                    strategy.get("leg_count"),
                    strategy_id,
                    version,
                )
            )

            cursor.execute(
                "DELETE FROM leg_details WHERE strategy_id = %s AND version = %s;",
                (strategy_id, version)
            )
            legs_deleted = cursor.rowcount

            legs_saved = insert_legs(cursor, strategy_id, version, request)

            conn.commit()

            logger.info(
                f"Strategy updated in place. strategy_id={strategy_id}, version={version}, "
                f"legs {legs_deleted} -> {legs_saved}"
            )
            return {
                "status": True,
                "strategy_id": strategy_id,
                "version": version,
                "legs_saved": legs_saved,
            }

        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"Error while updating strategy: {str(e)}")
            return {"status": False, "message": str(e), "not_found": False}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
