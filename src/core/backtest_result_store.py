from src.core.modules import OrderedDict, threading
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)


class ResultStore:
    """Caches the RAW (pre-slippage) trade_results of a user's latest run
    of each strategy so /apply-slippage can recompute without re-running
    the simulation.

    Keyed by (user_id, strategy_id): unsaved strategies all arrive with the
    frontend's placeholder id (e.g. -999), so keying by strategy_id alone
    would let one user's slippage recalculation read another user's
    results. Bounded LRU so 60 users cannot grow it without limit -- the
    oldest entries fall out and their /apply-slippage answers "re-run".
    In-memory, per process; swap for Redis if the API ever runs as
    several processes."""

    MAX_ENTRIES = 300
    _results: "OrderedDict[tuple, list]" = OrderedDict()
    _lock = threading.Lock()

    @classmethod
    def save_result(cls, trade_results: list, user_id: int, strategy_id) -> None:
        key = (user_id, str(strategy_id))
        with cls._lock:
            cls._results.pop(key, None)
            cls._results[key] = trade_results
            while len(cls._results) > cls.MAX_ENTRIES:
                cls._results.popitem(last=False)

    @classmethod
    def get_result(cls, user_id: int, strategy_id) -> list:
        key = (user_id, str(strategy_id))
        with cls._lock:
            if key not in cls._results:
                raise KeyError(key)
            cls._results.move_to_end(key)
            return cls._results[key]

    @classmethod
    def clear(cls, user_id: int | None = None, strategy_id=None) -> None:
        with cls._lock:
            if user_id is None:
                cls._results.clear()
            else:
                cls._results.pop((user_id, str(strategy_id)), None)


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
                DO UPDATE SET
                    strategy_name       = EXCLUDED.strategy_name,
                    backtest_start_date = EXCLUDED.backtest_start_date,
                    backtest_end_date   = EXCLUDED.backtest_end_date,
                    overall_mtm         = EXCLUDED.overall_mtm,
                    avg_mtm             = EXCLUDED.avg_mtm,
                    max_drawdown        = EXCLUDED.max_drawdown,
                    risk_reward_ratio   = EXCLUDED.risk_reward_ratio,
                    win_percentage      = EXCLUDED.win_percentage
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