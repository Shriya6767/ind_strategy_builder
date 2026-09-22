"""Runs single-strategy backtests OUTSIDE the API process.

Why: a backtest is seconds of pure CPU. Run inside the API process it
either blocks the event loop (every other user's request waits) or, in a
thread, fights Python's GIL with other backtests. A small pool of forked
worker processes fixes both: each backtest gets its own CPU, and because
the workers are forked AFTER the resident market frame is loaded they
share it copy-on-write -- no pickling, no duplicate memory.

Windows has no fork; there the backtest simply runs in the calling
thread (the route is a sync `def`, so FastAPI already keeps it off the
event loop). That is fine for the dev laptop.
"""
import multiprocessing as mp
import platform
from concurrent.futures import ProcessPoolExecutor
from src.core.modules import threading
from src.core.config import BACKTEST_WORKERS
from src.core.data_store import DataStore
from src.core.logger import get_logger

logger = get_logger(__name__)

_FORK_CTX = mp.get_context("fork") if platform.system() != "Windows" else None
_pool: ProcessPoolExecutor | None = None
_pool_lock = threading.Lock()


def _pool_size() -> int:
    return BACKTEST_WORKERS if BACKTEST_WORKERS > 0 else max(2, mp.cpu_count() // 2)


def _run_in_worker(request: dict, lo: int, hi: int) -> dict:
    """Worker body: slice the inherited resident frame, run the engine."""
    from src.services.backtesting.backtest_engine import BacktestEngine
    df = DataStore.get_df().iloc[lo:hi]
    return BacktestEngine(df, request).run()


def _get_pool() -> ProcessPoolExecutor:
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ProcessPoolExecutor(max_workers=_pool_size(), mp_context=_FORK_CTX)
            logger.info(f"Backtest worker pool started ({_pool_size()} forked workers).")
        return _pool


def reset_pool() -> None:
    """Call after the resident frame is reloaded: existing workers still
    hold the old frame, so they are retired and re-forked on next use."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.shutdown(wait=False, cancel_futures=False)
            _pool = None


def run_backtest_isolated(request: dict, lo: int, hi: int) -> dict:
    if _FORK_CTX is None:
        return _run_in_worker(request, lo, hi)
    return _get_pool().submit(_run_in_worker, request, lo, hi).result()
