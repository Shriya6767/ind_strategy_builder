import copy
import multiprocessing as mp
import platform
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.core.data_store import DataStore
from src.core.logger import get_logger
from src.services.backtesting.backtest_engine import BacktestEngine
from src.services.backtesting.load_data import DataLoader
from src.services.backtesting.report_builder import BacktestReportBuilder
from src.services.backtesting.portfolio_report_builder import PortfolioReportBuilder
from src.services.backtesting.slippage_service import SlippageService
from src.services.get_strategy import GetStrategyService

logger = get_logger(__name__)

_FORK_CTX = mp.get_context("fork") if platform.system() != "Windows" else None

_WEEKDAY_CODE_TO_INDEX = {"M": 0, "T": 1, "W": 2, "Th": 3, "F": 4, "Sa": 5, "Su": 6}

def _run_single_strategy(df, strategy_request: dict) -> dict:
    strategy_id = strategy_request["strategy"]["strategy_id"]
    try:
        engine = BacktestEngine(df, strategy_request)
        output = engine.run()
        return {"strategy_id": strategy_id, "ok": True, "output": output}
    except Exception as exc:
        logger.exception(f"Strategy {strategy_id} failed in portfolio run: {exc}")
        return {"strategy_id": strategy_id, "ok": False, "error": str(exc)}


class PortfolioBacktestService:
    def run_portfolio(self, request: dict) -> dict:
        aggregate_at_eod = request.get("aggregate_at_eod", True)
        portfolio_id = request.get("portfolio_id")
        start_date = request.get("start_date")
        end_date = request.get("end_date")

        if portfolio_id is None:
            raise ValueError("portfolio_id is required.")
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required.")

        overrides = request.get("strategy_overrides")
        self._validate(overrides)

        strategies = self._expand_strategies(overrides, start_date, end_date)

        df_by_dte_type = self._load_dataframes(strategies, start_date, end_date)

        results_by_id = self._run_strategies_in_parallel(strategies, df_by_dte_type)
        failures = {sid: r["error"] for sid, r in results_by_id.items() if not r["ok"]}
        if failures:
            raise RuntimeError(f"Portfolio backtest failed for strategies: {failures}")

        strategy_payloads, trade_results_by_id = self._persist_and_collect(strategies, overrides, results_by_id)
        merged_trade_results = PortfolioReportBuilder.merge_trade_results(trade_results_by_id)
        aggregate_report = BacktestReportBuilder(merged_trade_results).build()
        return {
            "portfolio_id": portfolio_id,
            "aggregate": aggregate_report,
            "strategies": strategy_payloads,
        }


    @staticmethod
    def _validate(overrides: list) -> None:
        if not overrides:
            raise ValueError("strategy_overrides is required to run a portfolio backtest.")

        for s in overrides:
            if not s.get("strategy_id"):
                raise ValueError("Each entry in strategy_overrides must include strategy_id.")
            if not s.get("strategy_name"):
                raise ValueError(f"strategy_id {s['strategy_id']}: strategy_name is required in strategy_overrides.")
            if not s.get("version"):
                raise ValueError(f"strategy_id {s['strategy_id']}: version is required in strategy_overrides.")

            period_selection = s.get("period_selection")
            mode = (period_selection or {}).get("mode")
            if mode not in (None, "dte", "weekdays"):
                raise NotImplementedError(
                    f"strategy_id {s['strategy_id']}: period_selection mode {mode!r} isn't implemented yet "
                    f"(budget_days needs a budget-day calendar data source that doesn't exist yet -- "
                    f"refusing to run rather than silently ignoring it)."
                )
            if mode == "dte" and not period_selection.get("dte_selected") and period_selection.get("dte_selected") != 0:
                raise ValueError(f"strategy_id {s['strategy_id']}: dte_selected is required for mode='dte'.")
            if mode == "weekdays" and not period_selection.get("weekdays_selected"):
                raise ValueError(f"strategy_id {s['strategy_id']}: weekdays_selected is required for mode='weekdays'.")


    @staticmethod
    def _resolve_dte_type(strategy_request: dict) -> str:
        dte_filter = strategy_request["strategy"].get("dte_filter", 0)
        dte_type = f"{dte_filter}dte"
        if dte_type not in ("0dte", "1dte"):
            raise ValueError(
                f"strategy_id {strategy_request['strategy']['strategy_id']}: "
                f"dte_filter={dte_filter!r} doesn't map to a supported dataset (0 or 1)."
            )
        return dte_type


    @classmethod
    def _load_dataframes(cls, strategies: list, start_date: str, end_date: str) -> dict:
        dte_types_needed = {cls._resolve_dte_type(s) for s in strategies}

        df_by_dte_type = {}
        for dte_type in dte_types_needed:
            loader = DataLoader(dte_type=dte_type)
            loader.load(start_date, end_date)  # side effect: DataStore.set_df(df)
            df_by_dte_type[dte_type] = DataStore.get_df()
        return df_by_dte_type


    @staticmethod
    def _expand_strategies(overrides: list, start_date: str, end_date: str) -> list:
        expanded = []
        for s in overrides:
            result = GetStrategyService.get_strategy(s["strategy_id"], s["strategy_name"], s["version"])
            if not result.get("success"):
                raise ValueError(
                    f"Could not load strategy {s['strategy_id']} (v{s['version']}): {result.get('error')}"
                )

            data = result["data"]
            strategy_request = {
                "strategy": {
                    "strategy_id": data["strategy_id"],
                    "strategy_name": data["strategy_name"],
                    **data["strategy"],
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "legs": copy.deepcopy(data["legs"]),
            }

            qty_multiplier = s.get("qty_multiplier", 1)
            if qty_multiplier and qty_multiplier != 1:
                for leg in strategy_request["legs"]:
                    leg["lot_size"] = leg.get("lot_size", 1) * qty_multiplier

            expanded.append(strategy_request)
        return expanded


    def _run_strategies_in_parallel(self, strategies: list[dict], df_by_dte_type: dict) -> dict:
        max_workers = min(len(strategies), mp.cpu_count())
        results_by_id = {}
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=_FORK_CTX) as pool:
            futures = {}
            for s in strategies:
                sid = s["strategy"]["strategy_id"]
                df = df_by_dte_type[self._resolve_dte_type(s)]
                futures[pool.submit(_run_single_strategy, df, s)] = sid
            for future in as_completed(futures):
                result = future.result()
                results_by_id[result["strategy_id"]] = result
        return results_by_id


    @staticmethod
    def _apply_weekday_filter(output: dict, period_selection: dict) -> dict:
        selected = period_selection.get("weekdays_selected") or []
        try:
            weekday_indices = {_WEEKDAY_CODE_TO_INDEX[code] for code in selected}
        except KeyError as e:
            raise ValueError(f"Unknown weekday code in period_selection: {e}")

        filtered_trade_results = [
            day for day in output["trade_results"]
            if day["trade_date"].weekday() in weekday_indices
        ]
        report = BacktestReportBuilder(filtered_trade_results).build()
        return {**output, "trade_results": filtered_trade_results, **report}


    @staticmethod
    def _apply_dte_filter(output: dict, period_selection: dict) -> dict:
        selected = period_selection.get("dte_selected")
        selected_dtes = set(selected) if isinstance(selected, list) else {selected}
        
        def _day_dte(day_result):
            for leg in day_result["legs"]:
                if leg.get("dte") is not None:
                    return leg["dte"]
            return None

        filtered_trade_results = [
            day for day in output["trade_results"]
            if _day_dte(day) in selected_dtes
        ]
        report = BacktestReportBuilder(filtered_trade_results).build()
        return {**output, "trade_results": filtered_trade_results, **report}


    def _persist_and_collect(self, strategies: list[dict], overrides: list[dict], results_by_id: dict) -> tuple[list[dict], dict]:
        overrides_by_id = {s["strategy_id"]: s for s in overrides}

        strategy_payloads = []
        trade_results_by_id = {}

        for s in strategies:
            meta = s["strategy"]
            sid = meta["strategy_id"]
            override = overrides_by_id[sid]
            output = results_by_id[sid]["output"]

            period_selection = override.get("period_selection")
            if period_selection:
                mode = period_selection.get("mode")
                if mode == "weekdays":
                    output = self._apply_weekday_filter(output, period_selection)

            slippage_percent = override.get("slippage_percent", 0)
            if slippage_percent:
                slipped = SlippageService.apply(output["trade_results"], slippage_percent)
                output = {**output, **slipped} 

            trade_results_by_id[sid] = output["trade_results"]
            strategy_payloads.append({
                "strategy_id": sid,
                "strategy_name": meta["strategy_name"],
                **output,
            })

        return strategy_payloads, trade_results_by_id