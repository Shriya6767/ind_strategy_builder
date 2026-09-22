from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)

class CompareBacktestService:
    def compare_backtests(self, request: dict, user_id: int) -> list[dict]:
        conn = None

        try:
            strategy_id = request.get("strategy_id")
            strategy_name = request.get("strategy_name")

            conn = Database.get_connection()
            cursor = conn.cursor()

            # version_result has no owner column; ownership comes from the
            # strategy it belongs to.
            cursor.execute(
                """
                SELECT
                    vr.strategy_id,
                    vr.strategy_name,
                    vr.version,
                    vr.backtest_start_date,
                    vr.backtest_end_date,
                    vr.overall_mtm,
                    vr.avg_mtm,
                    vr.max_drawdown,
                    vr.risk_reward_ratio,
                    vr.win_percentage,
                    vr.created_at
                FROM version_result vr
                WHERE vr.strategy_id = %s
                  AND vr.strategy_name = %s
                  AND EXISTS (
                        SELECT 1 FROM strategy s
                        WHERE s.strategy_id = vr.strategy_id AND s.user_id = %s
                  )
                ORDER BY vr.version DESC;
                """,
                (strategy_id, strategy_name, user_id),
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