from src.core.data_store import DataStore
from src.core.backtest_result_store import ResultStore, VersionedResultStore
from src.services.backtesting.backtest_engine import BacktestEngine
from src.services.backtesting.slippage_service import SlippageService


class BacktestService:

    def run_engine(self, request: dict) -> dict:
        strategy_id = request["strategy"]["strategy_id"]
        strategy_name = request["strategy"]["strategy_name"]
        start_date = request["strategy"]["start_date"]
        end_date = request["strategy"]["end_date"]
        version = request["strategy"]["version"]
        if strategy_id is None or strategy_id == "":
            raise ValueError("strategy_id can not be empty or null.")

        df = DataStore.get_df()
        engine = BacktestEngine(df, request)
        engine_output = engine.run()

        trade_results = engine_output["trade_results"]
        summary_report_result = engine_output["summary_report_result"]
        ResultStore.save_result(trade_results, strategy_id)
        if version != 0:
            VersionedResultStore.save_result(summary_report_result, strategy_id, strategy_name, start_date, end_date, version)
        
        return {
            "strategy_id": strategy_id,
            **engine_output,
        }

    def apply_slippage(self, strategy_id: str, slippage_percent: float) -> dict:
        trade_results = ResultStore.get_result(strategy_id)
        return {
            "strategy_id": strategy_id,
            **SlippageService.apply(trade_results, slippage_percent),
        }