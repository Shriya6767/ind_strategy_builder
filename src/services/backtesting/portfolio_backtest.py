import copy
import gc
import multiprocessing as mp
import os
import platform
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.core.modules import pd
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

_SHARED_DF = None

_WEEKDAY_CODE_TO_INDEX = {"M": 0, "T": 1, "W": 2, "Th": 3, "F": 4, "Sa": 5, "Su": 6}


def _available_memory_bytes():
    """Free RAM, or None when it can't be determined.

    /proc/meminfo's MemAvailable is the kernel's own estimate of what a new
    workload can claim without swapping, which is exactly the question here,
    and it needs no third-party package on the Ubuntu host. psutil is used
    if it happens to be installed (e.g. on a dev machine); everything else
    falls through to None and the worker count stays CPU-bound.
    """
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        import psutil
        return psutil.virtual_memory().available
    except Exception:
        return None


def _run_single_strategy(strategy_request: dict, df=None) -> dict:
    """Runs one strategy in a worker process. `df` is None on the fork path,
    where the frame is inherited as _SHARED_DF rather than pickled in."""
    strategy_id = strategy_request["strategy"]["strategy_id"]
    try:
        engine = BacktestEngine(_SHARED_DF if df is None else df, strategy_request)
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

        df = self._prepare_shared_frame(self._load_dataframe(start_date, end_date))

        overrides_by_id = {s["strategy_id"]: s for s in overrides}
        payloads_by_id, reduced_by_id, failures = self._run_and_reduce(strategies, overrides_by_id, df)
        if failures:
            raise RuntimeError(f"Portfolio backtest failed for strategies: {failures}")

        merged_trade_results = PortfolioReportBuilder.merge_trade_results(reduced_by_id)
        aggregate_report = BacktestReportBuilder(merged_trade_results).build()
        # Response order follows the request's strategy_overrides order.
        strategy_payloads = [payloads_by_id[s["strategy"]["strategy_id"]] for s in strategies]
        return {
            "portfolio_id": portfolio_id,
            "aggregate": aggregate_report,
            "trade_results": merged_trade_results,
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
    def _load_dataframe(start_date: str, end_date: str):
        """One load serves every strategy: each Sensex daily file carries
        every live expiry, and legs pick their contract per expiry_type
        (weekly / next weekly / monthly / next monthly) inside the engine.
        A frame already loaded for EXACTLY this range (by a previous
        portfolio run or /load-data) is reused as-is."""
        
        start = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        end = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        if DataStore.covers(start, end):
            logger.info(f"Portfolio: reusing the loaded {start}..{end} frame (no reload).")
            return DataStore.get_df()
        DataStore.clear_df()
        gc.collect()
        DataLoader().load(start_date, end_date)  # side effect: DataStore.set_df(df, start, end)
        return DataStore.get_df()


    @staticmethod
    def _prepare_shared_frame(df):
        """Derives the engine's day columns ONCE in the parent, before the
        pool forks (see BacktestEngine.derive_day_columns): each worker's
        prepare_dataframe finds them and skips, and because every column is
        a refcount-free numeric/categorical dtype the workers read the
        inherited pages without ever privatizing them."""
        return BacktestEngine.derive_day_columns(df)


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


    def _run_and_reduce(self, strategies: list[dict], overrides_by_id: dict, df) -> tuple[dict, dict, dict]:
        """Runs each strategy in its own process against the same market data
        and post-processes every result AS IT COMPLETES: weekday filter +
        slippage, then keep only (a) the summary blocks for the response and
        (b) a pnl-only skeleton of its trade_results for the aggregate merge.
        The full leg-level output -- hundreds of MB per strategy on a
        multi-year window -- is dropped inside the loop, so parent memory
        stays flat at roughly ONE strategy's output no matter how many
        strategies the portfolio holds.

        The data is NOT passed to submit() on Linux. Arguments to submit are
        pickled and piped to the worker, so handing over the DataFrame would
        cost one serialized copy in the parent plus one private copy per
        worker -- for a 3-year load that is ~4 GB each, and it is what made a
        2-strategy portfolio peak around 33 GB. Publishing it as a module
        global before the pool forks lets every worker read the parent's
        pages copy-on-write instead: the engine only ever adds columns to a
        shallow copy, so almost nothing is actually duplicated.

        Windows has no fork and must still pickle the frame, so it keeps the
        argument path.
        """
        global _SHARED_DF

        forking = _FORK_CTX is not None
        max_workers = self._worker_budget(len(strategies), df, shared=forking)
        names_by_id = {s["strategy"]["strategy_id"]: s["strategy"]["strategy_name"] for s in strategies}

        payloads_by_id, reduced_by_id, failures = {}, {}, {}
        if forking:
            _SHARED_DF = df
        try:
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=_FORK_CTX) as pool:
                futures = {}
                for s in strategies:
                    sid = s["strategy"]["strategy_id"]
                    futures[pool.submit(_run_single_strategy, s,
                                        None if forking else df)] = sid
                for future in as_completed(list(futures)):
                    # pop so the future (and the full output it holds) can be
                    # garbage-collected as soon as this iteration ends
                    sid = futures.pop(future)
                    result = future.result()
                    if not result["ok"]:
                        failures[sid] = result["error"]
                        continue
                    output = result["output"]
                    override = overrides_by_id[sid]

                    period_selection = override.get("period_selection")
                    if period_selection and period_selection.get("mode") == "weekdays":
                        output = self._apply_weekday_filter(output, period_selection)

                    slippage_percent = override.get("slippage_percent", 0)
                    if slippage_percent:
                        slipped = SlippageService.apply(output["trade_results"], slippage_percent)
                        output = {**output, **slipped}

                    reduced_by_id[sid] = self._reduce_for_merge(output["trade_results"])
                    payloads_by_id[sid] = {
                        "strategy_id": sid,
                        "strategy_name": names_by_id[sid],
                        "summary_report_result": output["summary_report_result"],
                        "monthly_state_result": output.get("monthly_state_result"),
                    }
        finally:
            _SHARED_DF = None
        return payloads_by_id, reduced_by_id, failures


    @staticmethod
    def _reduce_for_merge(trade_results: list) -> list:
        """Keeps only EXECUTED legs (pnl is not None) and the day's
        overall-exit marker. The executed leg dicts are kept by REFERENCE
        (no copy) with all their fields: the merge tags them strategy_id
        and they become the response's combined trade report, and the
        aggregate report is rebuilt from the same rows. What's dropped is
        the bulk a combined report never shows -- non-executed leg blocks
        (RANGE_BREAKOUT_NOT_TRIGGERED, NO_CHAIN_DATA, ...), empty days, and
        the per-strategy duplicate copy of the full output."""
        reduced = []
        for day in trade_results:
            legs = [leg for leg in day["legs"] if leg.get("pnl") is not None]
            if not legs:
                continue
            slim = {"trade_date": day["trade_date"], "legs": legs}
            if day.get("overall_exits"):
                slim["overall_exits"] = True
            reduced.append(slim)
        return reduced


    @staticmethod
    def _worker_budget(strategy_count: int, df, shared: bool) -> int:
        """How many strategies to run at once.

        Bounded by memory as well as CPU: each worker needs room for the
        columns the engine derives (and, without fork, its own copy of the
        frame), so a wide portfolio on a small box should run slower rather
        than exhaust RAM and freeze. Falls back to the CPU count when the
        available memory can't be read.
        """
        # PORTFOLIO_MAX_WORKERS (.env) caps the parallel workers below the
        # machine's core count so the OS, this API process and PostgreSQL
        # keep cores of their own while a portfolio runs (e.g. 16 on the
        # 20-core host). Unset/0 = every core.
        core_cap = int(os.environ.get("PORTFOLIO_MAX_WORKERS", 0) or 0) or mp.cpu_count()
        workers = max(1, min(strategy_count, core_cap))
        available = _available_memory_bytes()
        if available is None:
            return workers

        frame = int(df.memory_usage(deep=False).sum())
        # The frame holds only numeric/categorical columns (see
        # BacktestEngine.derive_day_columns), so forked workers read the
        # inherited pages without dirtying them -- no refcounts to write. A
        # worker's own footprint is its day-frame slices, caches and results
        # (~20 bytes/row of cushion); without fork add the whole pickled
        # frame as before.
        per_worker = 20 * len(df) + (0 if shared else frame)
        affordable = max(1, int(available * 0.7 // max(per_worker, 1)))
        if affordable < workers:
            logger.warning(
                f"Portfolio: limiting to {affordable} parallel worker(s) instead of "
                f"{workers} -- {available / 1024**3:.1f} GB available, each worker "
                f"needs about {per_worker / 1024**3:.1f} GB."
            )
        return min(workers, affordable)


    @staticmethod
    def _apply_weekday_filter(output: dict, period_selection: dict) -> dict:
        selected = period_selection.get("weekdays_selected") or []
        try:
            weekday_indices = {_WEEKDAY_CODE_TO_INDEX[code] for code in selected}
        except KeyError as e:
            raise ValueError(f"Unknown weekday code in period_selection: {e}")

        filtered_trade_results = [
            day for day in output["trade_results"]
            if BacktestReportBuilder._as_date(day["trade_date"]).weekday() in weekday_indices
        ]
        report = BacktestReportBuilder(filtered_trade_results).build()
        return {**output, "trade_results": filtered_trade_results, **report}


    @staticmethod
    def _apply_dte_filter(output: dict, period_selection: dict) -> dict:
        """Keeps only trading days whose traded contract had the selected
        days-to-expiry at entry (0 = entered on expiry day, 1 = the day
        before, ...). Sensex results carry no dte column -- it's derived per
        day from the first executed leg: expiration_date - entry date."""
        selected = period_selection.get("dte_selected")
        selected_dtes = {int(v) for v in (selected if isinstance(selected, list) else [selected])}

        as_date = BacktestReportBuilder._as_date

        def _day_dte(day_result):
            for leg in day_result["legs"]:
                if leg.get("entry_price") is None or leg.get("expiration_date") is None:
                    continue
                entry_date = as_date(leg.get("entry_datetime") or day_result["trade_date"])
                return (as_date(leg["expiration_date"]) - entry_date).days
            return None

        filtered_trade_results = [
            day for day in output["trade_results"]
            if _day_dte(day) in selected_dtes
        ]
        report = BacktestReportBuilder(filtered_trade_results).build()
        return {**output, "trade_results": filtered_trade_results, **report}


