from psycopg2.extras import Json
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)

VALID_WEEKDAYS = ("M", "T", "W", "Th", "F", "Sa", "Su")


class SavePortfolioService:
    @staticmethod
    def save_portfolio(request: dict, user_id: int) -> dict:
        conn = None
        try:
            portfolio_id = request.get("portfolio_id")
            portfolio_name = request["portfolio_name"]
            strategies = request["strategies"]

            if not strategies:
                return {"status": False, "message": "At least one strategy is required."}

            # Portfolio-wide defaults (top-level request fields).
            index_name = str(request.get("index") or "sensex").strip().lower()
            qty_multiplier = SavePortfolioService._int(request.get("qty_multiplier"), 1, "qty_multiplier")
            period_selection = SavePortfolioService._normalize_period_selection(request.get("period_selection"), "portfolio")
            slippage = SavePortfolioService._float(request.get("slippage"), 0.0, "slippage")

            normalized_strategies = SavePortfolioService._normalize_strategies(strategies)

            conn = Database.get_connection()
            cursor = conn.cursor()

            # Every strategy in the portfolio must be the caller's own.
            wanted = sorted({int(s["strategy_id"]) for s in normalized_strategies})
            cursor.execute(
                "SELECT DISTINCT strategy_id FROM strategy WHERE user_id = %s AND strategy_id = ANY(%s);",
                (user_id, wanted),
            )
            owned = {row[0] for row in cursor.fetchall()}
            foreign = [sid for sid in wanted if sid not in owned]
            if foreign:
                return {"status": False, "message": f"Strategy id(s) not found: {foreign}."}

            if portfolio_id is None:
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(portfolio_id), 0) + 1
                    FROM portfolio;
                    """
                )
                portfolio_id = cursor.fetchone()[0]

                cursor.execute(
                    """
                    INSERT INTO portfolio (portfolio_id, portfolio_name, strategies, user_id,
                                           index_name, qty_multiplier, period_selection, slippage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (portfolio_id, portfolio_name, Json(normalized_strategies), user_id,
                     index_name, qty_multiplier, Json(period_selection) if period_selection is not None else None, slippage),
                )
                message = f"Portfolio created successfully."
            else:
                cursor.execute(
                    """
                    UPDATE portfolio
                    SET portfolio_name = %s,
                        strategies = %s,
                        index_name = %s,
                        qty_multiplier = %s,
                        period_selection = %s,
                        slippage = %s,
                        updated_at = NOW()
                    WHERE portfolio_id = %s AND user_id = %s
                    RETURNING id;
                    """,
                    (portfolio_name, Json(normalized_strategies), index_name, qty_multiplier,
                     Json(period_selection) if period_selection is not None else None, slippage,
                     portfolio_id, user_id),
                )
                message = f"Portfolio updated."

            row = cursor.fetchone()
            if row is None:
                conn.rollback()
                return {"status": False, "message": f"Portfolio {portfolio_id} not found to update."}

            conn.commit()
            logger.info(f"Portfolio saved successfully. portfolio_id={portfolio_id}")
            return {
                "status": True,
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio_name,
                "message": message
            }
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"Failed to save portfolio: {e}")
            return {"status": False, "message": str(e)}
        finally:
            if conn:
                cursor.close()
                conn.close()


    @staticmethod
    def _normalize_strategies(strategies: list) -> list:
        normalized = []
        for s in strategies:
            if s.get("strategy_id") is None:
                raise ValueError("Each strategy in a portfolio must include strategy_id.")
            if not s.get("strategy_name"):
                raise ValueError(f"strategy_id {s['strategy_id']} must include strategy_name.")
            if not s.get("version"):
                raise ValueError(f"strategy_id {s['strategy_id']} must include version.")

            label = f"strategy_id {s['strategy_id']}"
            normalized.append({
                "strategy_id": s["strategy_id"],
                "strategy_name": s["strategy_name"],
                "version": s["version"],
                # per-strategy backtest overrides, stored exactly as the UI sends them
                "qty_multiplier": SavePortfolioService._int(s.get("qty_multiplier"), 1, f"{label} qty_multiplier"),
                "period_selection": SavePortfolioService._normalize_period_selection(s.get("period_selection"), label),
                "slippage": SavePortfolioService._float(s.get("slippage"), 0.0, f"{label} slippage"),
            })
        return normalized


    @staticmethod
    def _normalize_period_selection(raw, label: str):
        """{"mode": "weekdays", "weekdays_selected": [...]} or
        {"mode": "dte", "dte_selected": [...]}; None when absent."""
        if raw in (None, "", {}):
            return None
        if not isinstance(raw, dict):
            raise ValueError(f"{label}: period_selection must be an object.")
        mode = str(raw.get("mode") or "").strip().lower()
        if mode == "weekdays":
            days = raw.get("weekdays_selected") or []
            bad = [d for d in days if d not in VALID_WEEKDAYS]
            if bad:
                raise ValueError(f"{label}: invalid weekdays {bad} (valid: {list(VALID_WEEKDAYS)}).")
            return {"mode": "weekdays", "weekdays_selected": list(days)}
        if mode == "dte":
            try:
                dtes = [int(d) for d in (raw.get("dte_selected") or [])]
            except (TypeError, ValueError):
                raise ValueError(f"{label}: dte_selected must be a list of integers.")
            return {"mode": "dte", "dte_selected": dtes}
        raise ValueError(f"{label}: period_selection.mode must be 'weekdays' or 'dte'.")


    @staticmethod
    def _int(value, default: int, label: str) -> int:
        if value in (None, ""):
            return default
        try:
            out = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{label} must be an integer.")
        if out < 1:
            raise ValueError(f"{label} must be at least 1.")
        return out


    @staticmethod
    def _float(value, default: float, label: str) -> float:
        if value in (None, ""):
            return default
        try:
            out = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{label} must be a number.")
        if out < 0:
            raise ValueError(f"{label} cannot be negative.")
        return out