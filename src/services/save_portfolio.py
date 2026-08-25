from psycopg2.extras import Json
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)


class SavePortfolioService:
    @staticmethod
    def save_portfolio(request: dict) -> dict:
        conn = None
        try:
            portfolio_id = request.get("portfolio_id")
            portfolio_name = request["portfolio_name"]
            strategies = request["strategies"]

            if not strategies:
                return {"status": False, "message": "At least one strategy is required."}

            normalized_strategies = SavePortfolioService._normalize_strategies(strategies)

            conn = Database.get_connection()
            cursor = conn.cursor()

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
                    INSERT INTO portfolio (portfolio_id, portfolio_name, strategies)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    """,
                    (portfolio_id, portfolio_name, Json(normalized_strategies)),
                )
                message = f"Portfolio created successfully."
            else:
                cursor.execute(
                    """
                    UPDATE portfolio
                    SET portfolio_name = %s,
                        strategies = %s,
                        updated_at = NOW()
                    WHERE portfolio_id = %s
                    RETURNING id;
                    """,
                    (portfolio_name, Json(normalized_strategies), portfolio_id),
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

            normalized.append({
                "strategy_id": s["strategy_id"],
                "strategy_name": s["strategy_name"],
                "version": s["version"],
            })
        return normalized