from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)


LEG_INSERT_QUERY = """
    INSERT INTO leg_details (
        strategy_id,
        parent_leg_id,
        lot_size,
        position_type,
        option_type,
        expiry_type,
        strike_criteria,
        atm_strike,
        strike_sign,
        premium_value,
        lower_range,
        upper_range,
        multiplier_percentage,
        is_target,
        target_type,
        target_value,
        is_stoploss,
        stoploss_type,
        stoploss_value,
        is_trail_sl,
        trail_sl_type,
        instrument_moves,
        stoploss_moves,
        is_reentry_sl,
        reentry_sl_type,
        reentry_sl_value,
        is_reentry_target,
        reentry_target_type,
        reentry_target_value,
        is_simple_momentum,
        momentum_type,
        momentum_value,
        is_range_breakout,
        range_breakout_type,
        range_end_day,
        range_end_time,
        range_on,
        version,
        is_lazy_leg,
        is_sequential,
        leg_name,
        is_selected
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s
    )
    RETURNING leg_id;
"""


def insert_legs(cursor, strategy_id, version, request) -> int:
    inserted = 0

    def insert_leg(leg, parent_leg_id=None, is_lazy_leg=False, is_sequential=False,
                   leg_name=None, is_selected=True):
        nonlocal inserted
        cursor.execute(
            LEG_INSERT_QUERY,
            (
                strategy_id,
                parent_leg_id,
                leg.get("lot_size"),
                leg.get("position_type"),
                leg.get("option_type"),
                leg.get("expiry_type"),
                leg.get("strike_criteria"),
                leg.get("atm_strike"),
                leg.get("strike_sign"),
                leg.get("premium_value"),
                leg.get("lower_range"),
                leg.get("upper_range"),
                leg.get("multiplier_percentage"),

                leg.get("is_target"),
                leg.get("target_type"),
                leg.get("target_value"),

                leg.get("is_stoploss"),
                leg.get("stoploss_type"),
                leg.get("stoploss_value"),

                leg.get("is_trail_sl"),
                leg.get("trail_sl_type"),
                leg.get("instrument_moves"),
                leg.get("stoploss_moves"),

                leg.get("is_reentry_sl"),
                leg.get("reentry_sl_type"),
                leg.get("reentry_sl_value"),

                leg.get("is_reentry_target"),
                leg.get("reentry_target_type"),
                leg.get("reentry_target_value"),

                leg.get("is_simple_momentum"),
                leg.get("momentum_type"),
                leg.get("momentum_value"),

                leg.get("is_range_breakout"),
                leg.get("range_breakout_type"),
                str(leg.get("range_end_day")) if leg.get("range_end_day") is not None else None,
                leg.get("range_end_time"),
                leg.get("range_on"),
                version,
                is_lazy_leg,
                is_sequential,
                leg_name,
                is_selected
            )
        )
        inserted += 1
        inserted_leg_id = cursor.fetchone()[0]

        nested_lazy_leg = leg.get("lazy_leg")
        sequential_leg = leg.get("sequential_leg")

        if nested_lazy_leg is not None:
            insert_leg(
                leg=nested_lazy_leg,
                parent_leg_id=inserted_leg_id,
                is_lazy_leg=True,
                is_sequential=False,
                leg_name=nested_lazy_leg.get("leg_name"),
                is_selected=True
            )
        if sequential_leg is not None:
            insert_leg(
                leg=sequential_leg,
                parent_leg_id=inserted_leg_id,
                is_lazy_leg=False,
                is_sequential=True,
                leg_name=sequential_leg.get("leg_name"),
                is_selected=True
            )

    for leg in request["legs"]:
        insert_leg(leg=leg, parent_leg_id=None, is_lazy_leg=False,
                   is_sequential=False, leg_name=None, is_selected=True)

    for unselected_leg in request.get("unselected_legs") or []:
        insert_leg(leg=unselected_leg, parent_leg_id=None, is_lazy_leg=False,
                   is_sequential=False, leg_name=unselected_leg.get("leg_name"),
                   is_selected=False)

    return inserted


class SaveStrategyService:
    @staticmethod
    def save_strategy(request, user_id: int):
        conn = None
        cursor = None

        try:
            conn = Database.get_connection()
            cursor = conn.cursor()

            strategy = request["strategy"]
            legs = request["legs"]

            strategy_id = strategy.get("strategy_id")

            if strategy_id is None:
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(strategy_id), 0) + 1
                    FROM strategy;
                    """
                )
                strategy_id = cursor.fetchone()[0]
                version = 1

            else:
                # "Save As New" on an existing id: only its owner may add a
                # version to it.
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(version), 0)
                    FROM strategy
                    WHERE strategy_id = %s AND user_id = %s;
                    """,
                    (strategy_id, user_id)
                )
                latest = cursor.fetchone()[0]
                if not latest:
                    return {"status": False, "message": f"Strategy {strategy_id} not found.", "not_found": True}
                version = latest + 1

            strategy_query = """
                INSERT INTO strategy (
                    user_id,
                    strategy_id,
                    strategy_name,
                    symbol,
                    start_date,
                    end_date,
                    underlying_type,
                    is_squareoff,
                    is_trail_sl_break_even,
                    trail_sl_break_even_type,
                    strategy_type,
                    entry_time,
                    entry_delay,
                    exit_time,
                    exit_delay,
                    is_delay_restart,
                    delay_restart_time,
                    positional_expire_on,
                    positional_entry_day,
                    positional_exit_day,
                    is_strategy_sl,
                    strategy_sl_type,
                    strategy_sl_value,
                    is_strategy_target,
                    strategy_target_type,
                    strategy_target_value,
                    is_overall_reentry_sl,
                    overall_reentry_sl_type,
                    overall_reentry_sl_value,
                    is_overall_reentry_target,
                    overall_reentry_target_type,
                    overall_reentry_target_value,
                    is_overall_trail_sl,
                    overall_trail_sl_type,
                    overall_instrument_move,
                    overall_stoploss_move,
                    leg_count,
                    version
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id;
            """

            cursor.execute(
                strategy_query,
                (
                    user_id,
                    strategy_id,
                    strategy["strategy_name"],
                    strategy["symbol"],
                    strategy["start_date"],
                    strategy["end_date"],
                    strategy["underlying_type"],
                    strategy["is_squareoff"],
                    strategy["is_trail_sl_break_even"],
                    strategy["trail_sl_break_even_type"],
                    strategy["strategy_type"],
                    strategy["entry_time"],
                    strategy["entry_delay"],
                    strategy["exit_time"],
                    strategy["exit_delay"],
                    strategy.get("is_delay_restart", False),
                    strategy.get("delay_restart_time"),
                    strategy.get("positional_expire_on"),
                    strategy.get("positional_entry_day"),
                    strategy.get("positional_exit_day"),
                    strategy["is_strategy_sl"],
                    strategy["strategy_sl_type"],
                    strategy["strategy_sl_value"],
                    strategy["is_strategy_target"],
                    strategy["strategy_target_type"],
                    strategy["strategy_target_value"],
                    strategy["is_overall_reentry_sl"],
                    strategy["overall_reentry_sl_type"],
                    strategy["overall_reentry_sl_value"],
                    strategy["is_overall_reentry_target"],
                    strategy["overall_reentry_target_type"],
                    strategy["overall_reentry_target_value"],
                    strategy["is_overall_trail_sl"],
                    strategy["overall_trail_sl_type"],
                    strategy["overall_instrument_move"],
                    strategy["overall_stoploss_move"],
                    strategy["leg_count"],
                    version
                )
            )

            cursor.fetchone()

            insert_legs(cursor, strategy_id, version, request)

            conn.commit()

            logger.info(
                f"Strategy saved successfully. "
                f"strategy_id={strategy_id}, "
                f"version={version}"
            )
            return {
                "status": True,
                "strategy_id": strategy_id,
                "version": version
            }

        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(
                f"Error while saving strategy: {str(e)}"
            )
            return {
                "status": False,
                "message": str(e)
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()