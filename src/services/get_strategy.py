from psycopg2.extras import RealDictCursor
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)

class GetStrategyService:

    _INTERNAL_LEG_FIELDS = ("leg_id", "parent_leg_id", "is_lazy_leg", "is_sequential", "is_selected")

    @staticmethod
    def get_strategy(strategy_id, strategy_name, version, user_id=None):
        """
        Fetch complete strategy data from database by ID and name.
        Args:
            strategy_id: Unique identifier for the strategy
            strategy_name: Strategy name (used for verification)
            version: Strategy version
            user_id: owner; when given, another user's strategy is "not found"
        Returns:
            {"success": True, "data": {...}} or {"success": False, "error": "..."}
        """
        conn = None
        cursor = None
        try:
            conn = Database.get_connection()
            # RealDictCursor returns rows as dicts keyed by column name,
            # so we never depend on column ORDER matching an index number.
            cursor = conn.cursor(cursor_factory=RealDictCursor)
         
            strategy_query = """
            SELECT
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
            FROM strategy
            WHERE strategy_id = %s AND strategy_name = %s AND version = %s
            """
            params = [strategy_id, strategy_name, version]
            if user_id is not None:
                strategy_query += " AND user_id = %s"
                params.append(user_id)

            cursor.execute(strategy_query, params)
            row = cursor.fetchone()

            if not row:
                logger.warning(f"[GET-STRATEGY] Strategy not found: {strategy_id} - {strategy_name}")
                return {
                    "success": False,
                    "error": "Strategy not found"
                }
           
            strategy_data = GetStrategyService._normalize_strategy(row)

            legs_query = """
            SELECT
                leg_id,
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
                is_lazy_leg,
                is_sequential,
                leg_name,
                is_selected
            FROM leg_details
            WHERE strategy_id = %s AND version = %s
            ORDER BY leg_id
            """

            cursor.execute(legs_query, (strategy_id, version))
            leg_rows = cursor.fetchall()
           
            legs_data = [GetStrategyService._normalize_leg(r) for r in leg_rows]

            selected_legs, unselected_legs = GetStrategyService._reconstruct_legs(legs_data)

            logger.info(
                f"[GET-STRATEGY] Successfully loaded strategy with "
                f"{len(selected_legs)} selected leg(s), {len(unselected_legs)} unselected leg(s)"
            )

            return {
                "success": True,
                "data": {
                    "strategy_id": strategy_id,
                    "strategy_name": strategy_name,
                    "strategy": strategy_data,
                    "legs": selected_legs,
                    "unselected_legs": unselected_legs
                }
            }

        except Exception as e:
            logger.error(f"[GET-STRATEGY] ERROR: {str(e)}")
            return {
                "success": False,
                "error": f"Internal server error: {str(e)}"
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    @staticmethod
    def _reconstruct_legs(legs_data: list) -> tuple:
        """Rebuilds the exact nested shape save-strategy accepts on the way
        in -- each top-level leg optionally carrying its own "lazy_leg"
        and/or "sequential_leg" key -- from the flat leg_details rows.
        parent_leg_id says WHICH parent a child row belongs to;
        is_lazy_leg/is_sequential says WHICH slot on that parent it fills.
        """
        children_by_parent = {}
        for leg in legs_data:
            parent_id = leg.get("parent_leg_id")
            if parent_id is not None:
                children_by_parent.setdefault(parent_id, []).append(leg)

        def attach(leg: dict) -> dict:
            for child in children_by_parent.get(leg["leg_id"], []):
                attach(child)  # recurse first, in case this child has its own children
                if child.get("is_lazy_leg"):
                    leg["lazy_leg"] = child
                elif child.get("is_sequential"):
                    leg["sequential_leg"] = child
            return leg

        top_level_legs = [leg for leg in legs_data if leg.get("parent_leg_id") is None]
        for leg in top_level_legs:
            attach(leg)

        selected_legs = [
            GetStrategyService._strip_internal_fields(leg)
            for leg in top_level_legs if leg.get("is_selected", True)
        ]
        unselected_legs = [
            GetStrategyService._strip_internal_fields(leg)
            for leg in top_level_legs if not leg.get("is_selected", True)
        ]
        return selected_legs, unselected_legs


    @staticmethod
    def _strip_internal_fields(leg: dict) -> dict:
        """Removes DB/reconstruction-only bookkeeping fields so what comes
        back out matches exactly what save-strategy originally accepted --
        those fields never existed in the original request; they're purely
        internal to how leg_details stores nested lazy/sequential legs.
        Recurses into lazy_leg/sequential_leg so they're cleaned too."""
        cleaned = {k: v for k, v in leg.items() if k not in GetStrategyService._INTERNAL_LEG_FIELDS}
        if "lazy_leg" in leg:
            cleaned["lazy_leg"] = GetStrategyService._strip_internal_fields(leg["lazy_leg"])
        if "sequential_leg" in leg:
            cleaned["sequential_leg"] = GetStrategyService._strip_internal_fields(leg["sequential_leg"])
        return cleaned


    @staticmethod
    def _v(row, key, cast=None, default=None):
        """
        Safely fetch + cast a value, only falling back to `default`
        when the value is actually NULL (None) — not when it's a
        legitimate falsy value like 0, 0.0, or False.
        """
        val = row.get(key)
        if val is None:
            return default
        return cast(val) if cast else val

    @staticmethod
    def _normalize_strategy(row):
        v = GetStrategyService._v
        return {
            "symbol": v(row, "symbol"),
            "start_date": v(row, "start_date", str),
            "end_date": v(row, "end_date", str),
            "underlying_type": v(row, "underlying_type", None, "option"),
            "is_squareoff": v(row, "is_squareoff", bool, False),
            "is_trail_sl_break_even": v(row, "is_trail_sl_break_even", bool, False),
            "trail_sl_break_even_type": v(row, "trail_sl_break_even_type", str, "POINTS"),
            "strategy_type": v(row, "strategy_type", str, "INTRADAY"),
            "entry_time": v(row, "entry_time", str, "09:30:00"),
            "entry_delay": v(row, "entry_delay", int, 0),
            "exit_time": v(row, "exit_time", str, "15:30:00"),
            "exit_delay": v(row, "exit_delay", int, 0),
            "is_delay_restart": v(row, "is_delay_restart", bool, False),
            "delay_restart_time": v(row, "delay_restart_time", str),

            "positional_expire_on": v(row, "positional_expire_on", str),
            "positional_entry_day": v(row, "positional_entry_day", int),
            "positional_exit_day": v(row, "positional_exit_day", int),
            "is_strategy_sl": v(row, "is_strategy_sl", bool, False),
            "strategy_sl_type": v(row, "strategy_sl_type", str, "PERCENT"),
            "strategy_sl_value": v(row, "strategy_sl_value", float, 0.0),

            "is_strategy_target": v(row, "is_strategy_target", bool, False),
            "strategy_target_type": v(row, "strategy_target_type", str, "POINTS"),
            "strategy_target_value": v(row, "strategy_target_value", float, 0.0),

            "is_overall_reentry_sl": v(row, "is_overall_reentry_sl", bool, False),
            "overall_reentry_sl_type": v(row, "overall_reentry_sl_type", str, "RE_ASAP"),
            "overall_reentry_sl_value": v(row, "overall_reentry_sl_value", int, 0),

            "is_overall_reentry_target": v(row, "is_overall_reentry_target", bool, False),
            "overall_reentry_target_type": v(row, "overall_reentry_target_type", str, "RE_ASAP"),
            "overall_reentry_target_value": v(row, "overall_reentry_target_value", int, 0),

            "is_overall_trail_sl": v(row, "is_overall_trail_sl", bool, False),
            "overall_trail_sl_type": v(row, "overall_trail_sl_type", str, "POINTS"),
            "overall_instrument_move": v(row, "overall_instrument_move", int, 0),
            "overall_stoploss_move": v(row, "overall_stoploss_move", int, 0),

            "leg_count": v(row, "leg_count", int, 0),
            "version": v(row, "version", int, 0)
        }

    @staticmethod
    def _normalize_expiry_type(raw):
        """Legacy SPX-era rows stored '0dte'/'1dte'; the Sensex engine takes
        weekly / next weekly / monthly / next monthly. Old Ndte values (and
        NULL) map to 'weekly' -- the nearest-expiry equivalent -- so
        strategies saved before the migration still run."""
        if not raw:
            return "weekly"
        if str(raw).strip().lower().endswith("dte"):
            return "weekly"
        return raw

    @staticmethod
    def _normalize_leg(row):
        v = GetStrategyService._v
        return {
            "leg_id": v(row, "leg_id"),
            "parent_leg_id": v(row, "parent_leg_id"),
            "lot_size": v(row, "lot_size", int, 0),
            "position_type": v(row, "position_type", str, "BUY"),
            "option_type": v(row, "option_type", str, "call"),
            "expiry_type": GetStrategyService._normalize_expiry_type(v(row, "expiry_type", str)),
            "strike_criteria": v(row, "strike_criteria", str, "strike_type"),
             "atm_strike": v(row, "atm_strike", str, "0"),
            "strike_sign": v(row, "strike_sign", str, ""),
            "premium_value": v(row, "premium_value", int, 0),
            "lower_range": v(row, "lower_range", int, 0),
            "upper_range": v(row, "upper_range", int, 0),
            "multiplier_percentage": v(row, "multiplier_percentage", int, 0),

            "is_target": v(row, "is_target", bool, False),
            "target_type": v(row, "target_type", str, "POINTS"),
            "target_value": v(row, "target_value", float, 0.0),

            "is_stoploss": v(row, "is_stoploss", bool, False),
            "stoploss_type": v(row, "stoploss_type", str, "POINTS"),
            "stoploss_value": v(row, "stoploss_value", float, 0.0),

            "is_trail_sl": v(row, "is_trail_sl", bool, False),
            "trail_sl_type": v(row, "trail_sl_type", str, "POINTS"),
            "instrument_moves": v(row, "instrument_moves", int, 0),
            "stoploss_moves": v(row, "stoploss_moves", int, 0),

            "is_reentry_sl": v(row, "is_reentry_sl", bool, False),
            "reentry_sl_type": v(row, "reentry_sl_type", str, "RE_ASAP"),
            "reentry_sl_value": v(row, "reentry_sl_value", int, 0),

            "is_reentry_target": v(row, "is_reentry_target", bool, False),
            "reentry_target_type": v(row, "reentry_target_type", str, "RE_ASAP"),
            "reentry_target_value": v(row, "reentry_target_value", int, 0),

            "is_simple_momentum": v(row, "is_simple_momentum", bool, False),
            "momentum_type": v(row, "momentum_type", str, "PERCENT_DOWN"),
            "momentum_value": v(row, "momentum_value", float, 0.0),

            "is_range_breakout": v(row, "is_range_breakout", bool, False),
            "range_breakout_type": v(row, "range_breakout_type"),
            "range_end_day": v(row, "range_end_day", str),
            "range_end_time": v(row, "range_end_time", str),
            "range_on": v(row, "range_on"),
            "is_lazy_leg": v(row, "is_lazy_leg", bool, False),
            "is_sequential": v(row, "is_sequential", bool, False),
            "leg_name": v(row, "leg_name", str, ""),
            "is_selected": v(row, "is_selected", bool, True)
        }