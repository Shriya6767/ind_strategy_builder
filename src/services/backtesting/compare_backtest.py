from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)

class CompareBacktestService:
    def compare_backtests(self, request: dict) -> list[dict]:
        conn = None

        try:
            strategy_id = request.get("strategy_id")
            strategy_name = request.get("strategy_name")

            conn = Database.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    strategy_id,
                    strategy_name,
                    version,
                    backtest_start_date,
                    backtest_end_date,
                    overall_mtm,
                    avg_mtm,
                    max_drawdown,
                    risk_reward_ratio,
                    win_percentage,
                    created_at
                FROM version_result
                WHERE strategy_id = %s
                  AND strategy_name = %s
                ORDER BY version DESC;
                """,
                (strategy_id, strategy_name),
            )

            rows = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]

            result = [
                dict(zip(columns, row))
                for row in rows
            ]

            return result

        except Exception as e:
            logger.exception(f"Error comparing backtests: {e}")
            raise
        finally:
            if conn:
                conn.close()