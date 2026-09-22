from src.core.data_store import DataStore
from src.core.config import Database
from src.core.backtest_result_store import ResultStore, VersionedResultStore
from src.core.executor import run_backtest_isolated
from src.services.backtesting.slippage_service import SlippageService


class DataRangeError(ValueError):
    """The strategy's dates fall outside the resident market data."""


class NotOwnerError(PermissionError):
    """The strategy_id/version being persisted is not the caller's."""


class BacktestService:

    def run_engine(self, request: dict, user_id: int) -> dict:
        strategy = request["strategy"]
        strategy_id = strategy["strategy_id"]
        strategy_name = strategy["strategy_name"]
        start_date = strategy["start_date"]
        end_date = strategy["end_date"]
        version = strategy["version"]
        if strategy_id is None or strategy_id == "":
            raise ValueError("strategy_id can not be empty or null.")

        lo, hi = DataStore.bounds(start_date, end_date)
        if lo >= hi:
            avail = DataStore.loaded_range
            raise DataRangeError(
                f"No market data between {start_date} and {end_date}"
                + (f" (available: {avail[0]} to {avail[1]})." if avail else ".")
            )

        # Persisting version metrics writes to a saved strategy's row:
        # only its owner may do that.
        if version != 0 and not self._owns(user_id, strategy_id, version):
            raise NotOwnerError(f"Strategy {strategy_id} v{version} not found.")

        engine_output = run_backtest_isolated(request, lo, hi)

        trade_results = engine_output["trade_results"]
        summary_report_result = engine_output["summary_report_result"]
        ResultStore.save_result(trade_results, user_id, strategy_id)
        if version != 0:
            VersionedResultStore.save_result(summary_report_result, strategy_id, strategy_name, start_date, end_date, version)

        return {
            "strategy_id": strategy_id,
            **engine_output,
        }

    def apply_slippage(self, user_id: int, strategy_id, slippage_percent: float) -> dict:
        trade_results = ResultStore.get_result(user_id, strategy_id)
        return {
            "strategy_id": strategy_id,
            **SlippageService.apply(trade_results, slippage_percent),
        }

    @staticmethod
    def _owns(user_id: int, strategy_id, version) -> bool:
        conn = Database.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM strategy WHERE strategy_id = %s AND version = %s AND user_id = %s LIMIT 1;",
                (strategy_id, version, user_id),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()