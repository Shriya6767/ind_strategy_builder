from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)

class ResultStore:
    """Caches the RAW (pre-slippage) trade_results from a backtest run so
    /apply-slippage can recompute against it repeatedly without re-running
    the simulation. In-memory dict for now, matching DataStore's pattern --
    swap for Redis/a DB table if backtests need to survive a restart or be
    shared across multiple app instances."""

    _results: dict[str, list] = {}

    @classmethod
    def save_result(cls, trade_results: list, strategy_id: str) -> None:
        cls._results[strategy_id] = trade_results

    @classmethod
    def get_result(cls, strategy_id: str) -> list:
        if strategy_id not in cls._results:
            raise KeyError(strategy_id)
        return cls._results[strategy_id]

    @classmethod
    def clear(cls, strategy_id: str | None = None) -> None:
        if strategy_id is None:
            cls._results.clear()
        else:
            cls._results.pop(strategy_id, None)
            

class VersionedResultStore:
    @classmethod
    def save_result(cls, summary_report_result: list, strategy_id: int, strategy_name: str, start_date: str, end_date: str, version: int) -> None:
        conn = None

        try:
            conn = Database.get_connection()
            cursor = conn.cursor()

            tradewise_summary = next(
                (
                    item["summaryReport"]
                    for item in summary_report_result
                    if item["reportType"] == "Tradewise"
                ),
                None,
            )

            if tradewise_summary is None:
                raise ValueError("Tradewise summary not found.")

            cursor.execute(
                """
                INSERT INTO version_result
                (
                    strategy_id,
                    strategy_name,
                    version,
                    backtest_start_date,
                    backtest_end_date,
                    overall_mtm,
                    avg_mtm,
                    max_drawdown,
                    risk_reward_ratio,
                    win_percentage
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (strategy_id, version)
                DO NOTHING
                """,
                (
                    strategy_id,
                    strategy_name,
                    version,
                    start_date,
                    end_date,
                    float(tradewise_summary["OverallProfit"]),
                    float(tradewise_summary["AvgProfitPerTrade"]),
                    float(tradewise_summary["Max_Drawdown"]),
                    float(tradewise_summary["ReturnPerMaxDD"]),
                    float(tradewise_summary["WinPer"]),
                ),
            )
            conn.commit()
            logger.info(f"Versioned result saved for strategy_id: {strategy_id}, version: {version}")

        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()