"""One `DeploymentRunner` = one activated strategy: the live counterpart of
`BacktestEngine` for a single trade (intraday / sequential day, a BTST
today+tomorrow pair, or one positional expiry cycle).

Feature parity with the backtest engine, same rules, live prices:
  * strike selection (points / closest premium / premium range / ATM %),
    expiry labels (weekly / next weekly / monthly / next monthly), positional
    cycles (entry/exit N calendar weekdays before expiry, all legs on the
    cycle's expiry);
  * conditional entries: simple momentum (instrument or underlying, points
    or percent, up/down) and range breakout (instrument or underlying,
    High/Low, range window entry_time..range_end_time; BTST "tomorrow",
    positional N-days-before-expiry windows);
  * observation -> sequential legs (the parent is watched, never traded;
    its momentum / range trigger enters the sequential_leg, resolved fresh
    at that moment, with its own optional momentum / range);
  * per-leg SL / target (POINTS, PERCENT, UNDERLYING_*), trailing SL;
  * per-leg re-entries after SL / target: RE_ASAP(_REVERSE) = fresh strike
    now, RE_COST(_REVERSE) = same contract when price returns to cost,
    RE_MOMENTUM(_REVERSE) = fresh strike + the leg's momentum rule,
    LAZY_LEG = the lazy_leg definition (own side/type/strike/re-entries);
  * overall SL / target / trailing on combined MTM, overall re-entry
    RE_ASAP / RE_MOMENTUM (+REVERSE, flipping the side held at the breach);
  * BTST / positional carry days: monitoring resumes at delay_restart_time
    when is_delay_restart is on; square-off at exit_time on exit_date.

Price-driven work happens inside feed callbacks on the event loop; order
placement runs in tasks so a slow broker never blocks a tick. Persistence
goes through the engine's ordered `DbWriter`.
"""
from src.core.modules import asyncio, time, math, datetime, date, timedelta, Optional, Any
from src.core import config
from src.core.constant import QUANTITY, COST_BUFFER_PCT
from src.core.logger import get_logger
from src.live import deployment_store as store
from src.live.execution import (
    ExecutionSettings, LegExecution, limit_price, order_limit_price, mpp_price, mpp_pct, round_tick, stop_limit_prices,
)
from src.live.strike_resolver import map_option_type, normalize_expiry_type, candidate_strikes, select_strike
from src.live.xts_client import SEG_BSEFO, SEG_BSECM, XTSError
from src.live.feed import Key, Candle
from src.live.brokers import Broker, OrderUpdate
from src.live.timeutil import (
    now_ist, today_ist, parse_hms, hms, at, secs_now, MARKET_CLOSE_SECS, MARKET_OPEN_SECS, to_naive_ist,
    weekdays_before_expiry, is_trading_day, next_trading_day, IST,
)

logger = get_logger(__name__)

LAST_EXIT_SECS = MARKET_CLOSE_SECS - 30           # never plan an exit after 15:29:30
OVERNIGHT_PAUSE_SECS = 15 * 3600 + 45 * 60        # BTST / positional holds pause at 15:45
OVERNIGHT_REASON = "market closed -- restart before 09:15"
TERMINAL_STATUSES = ("squared_off", "completed", "error", "cancelled", "manual")
PREMIUM_TYPES = ("POINTS", "PERCENT")
UNDERLYING_TYPES = ("UNDERLYING_POINTS", "UNDERLYING_PERCENT")
OPEN_STATUSES = ("open", "exiting")
PENDING_STATUSES = ("pending", "entering", "waiting")
MULTIDAY_TYPES = ("btst", "positional")


def _prepare_meta(raw: dict, leg_number: int) -> dict:
    """Engine-ready copy of a leg definition (mapped option type, canonical
    expiry label). Nested lazy / sequential definitions stay raw and are
    prepared when they come into play."""
    meta = dict(raw)
    meta["leg_number"] = leg_number
    meta["_option_type"] = map_option_type(raw.get("option_type"))
    meta["_expiry_type"] = normalize_expiry_type(raw.get("expiry_type"))
    return meta


class LegState:
    __slots__ = (
        "leg_number", "meta", "attempt", "contract", "key", "side", "lots", "qty", "status", "kind",
        "entry_order_id", "entry_price", "entry_ts", "spot_at_entry", "sl", "tgt", "base_sl", "best_move",
        "trail_step", "trail_gain", "exit_order_id", "exit_price", "exit_ts", "exit_reason", "pnl",
        "live_leg_id", "error", "reentry_sl_left", "reentry_tgt_left", "entry_payload", "last_check",
        "option_type", "entry_mode", "wait", "watch_key", "trigger", "trigger_up", "range_hi", "range_lo",
        "range_end_dt", "range_high_side", "wait_deadline", "seq_meta", "trade_id",
        "exit_fails", "next_exit_try", "wait_saved_at",
        "ref_price", "sl_order_id", "sl_payload", "sl_busy", "next_trail_at",
    )

    def __init__(self, leg_number: int, meta: dict, attempt: int, side: str, entry_mode: str = "ENTRY",
                 kind: str = "trade"):
        self.leg_number, self.meta, self.attempt, self.side = leg_number, meta, attempt, side
        self.kind = kind                          # trade | observation
        self.entry_mode = entry_mode
        self.contract = None
        self.key: Key | None = None
        self.option_type = meta.get("_option_type") or "CE"
        self.lots = int(meta.get("lot_size") or 1)
        self.qty = 0
        self.status = "pending"                   # pending | waiting | entering | open | exiting | closed | error | skipped | done
        self.entry_order_id = self.exit_order_id = None
        self.entry_price = self.exit_price = None
        self.entry_ts = self.exit_ts = None
        self.spot_at_entry = None
        self.sl = self.tgt = self.base_sl = None
        self.best_move = 0.0
        self.trail_step = self.trail_gain = None
        self.exit_reason = None
        self.pnl = 0.0
        self.live_leg_id = None
        self.error = None
        self.reentry_sl_left = int(meta.get("reentry_sl_value") or 0) if meta.get("is_reentry_sl") else 0
        self.reentry_tgt_left = int(meta.get("reentry_target_value") or 0) if meta.get("is_reentry_target") else 0
        self.entry_payload = None
        self.last_check = 0.0
        # conditional-entry state
        self.wait: str | None = None              # momentum | range | cost
        self.watch_key: Key | None = None
        self.trigger: float | None = None
        self.trigger_up: bool = True
        self.range_hi = self.range_lo = None
        self.range_end_dt = None
        self.range_high_side = True
        self.wait_deadline = None
        self.seq_meta: dict | None = None         # observation legs: the sequential_leg to enter on trigger
        self.trade_id = 0                         # overall "trade" index (bumps on overall re-entry)
        self.exit_fails = 0                       # failed exit attempts (rejected / not filled)
        self.next_exit_try = 0.0                  # epoch before which SL/target must not re-fire an exit
        self.wait_saved_at = 0.0                  # last time the pending wait (range hi/lo) was written to the DB
        self.ref_price = None                     # intended entry price (tgt_sl_ref_price = TRIGGER)
        self.sl_order_id = None                   # the stop-loss resting at the broker as an SL-L order
        self.sl_payload = None
        self.sl_busy = False                      # an SL-L modify is in flight
        self.next_trail_at = 0.0                  # epoch of the next trailing-stop application

    @property
    def direction(self) -> int:
        return 1 if self.side == "BUY" else -1

    @property
    def spot_bullish(self) -> bool:
        return (self.option_type == "CE") == (self.side == "BUY")

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES

    def unrealised(self, ltp: float | None) -> float:
        if not self.is_open or self.entry_price is None or ltp is None:
            return 0.0
        return (ltp - self.entry_price) * self.direction * self.qty

    def tag(self, kind: str) -> str:
        return f"L{self.leg_number}A{self.attempt}{kind}"


class DeploymentRunner:
    # No fresh entry (re-entry / overall re-entry / conditional fill) inside this window before exit_time.
    REENTRY_CUTOFF_SECS = 60
    # Warn when a leg's LTP is older than this; repeat at most every STALE_WARN_EVERY_SECS per leg.
    STALE_LTP_SECS = 5
    STALE_WARN_EVERY_SECS = 30
    # Live mode: compare our open legs with the broker's positions this often (and on restore).
    RECONCILE_EVERY_SECS = 60
    RECONCILE_GRACE_SECS = 20     # a leg filled less than this long ago is not judged against the position book

    def __init__(self, engine, dep: dict, feed, broker: Broker, restore: bool = False):
        self.engine = engine
        self.feed = feed
        self.store = feed.store
        self.broker = broker
        self.master = engine.master
        self.db = engine.db
        self.dep = dep
        self.id: int = dep["deployment_id"]
        self.user_id: int = dep["user_id"]
        self.restore = restore
        snap = dep["strategy_snapshot"]
        self.strategy: dict = snap["strategy"]
        self.leg_defs: list[dict] = [_prepare_meta(l, i) for i, l in enumerate(snap["legs"], start=1)]
        self.settings = ExecutionSettings.from_dict(dep["settings"])
        self.mode = dep["mode"]
        self.venue = "broker" if self.mode == "live" else "paper broker"
        self.trade_date: date = _as_date(dep["trade_date"])
        self.exit_date: date = _as_date(dep["exit_date"])
        self.strategy_type = str(self.strategy.get("strategy_type", "intraday")).lower()
        self.is_positional = self.strategy_type.startswith("positional")
        self.is_btst = self.strategy_type == "btst"
        self.is_multiday = self.is_btst or self.is_positional
        self.entry_secs = parse_hms(self.strategy.get("entry_time", "09:30:00")) \
            + int(self.strategy.get("entry_delay") or 0) * 60
        # Strategy execution time: advance the entry by up to 59 s (AlgoTest rule).
        # "Delay entry by" is applied per leg when its order is placed.
        self._exec_time_note = None
        if self.settings.strategy_execution_time:
            try:
                exec_secs = parse_hms(self.settings.strategy_execution_time)
            except (ValueError, IndexError):
                exec_secs = None
            if exec_secs is not None and self.entry_secs - 59 <= exec_secs <= self.entry_secs:
                self.entry_secs = exec_secs
            else:
                self._exec_time_note = (f"strategy_execution_time {self.settings.strategy_execution_time} ignored: it must be "
                                        f"within 59 s before the entry time {hms(self.entry_secs)}")
        self._leg_settings: dict[int, ExecutionSettings] = {}
        self.exit_secs = min(parse_hms(self.strategy.get("exit_time", "15:15:00"))
                             + int(self.strategy.get("exit_delay") or 0) * 60, LAST_EXIT_SECS)
        self.entry_dt = at(self.trade_date, self.entry_secs)
        self.exit_dt = at(self.exit_date, self.exit_secs)
        self.cycle_expiry: date | None = None    # positional: the cycle's settlement expiry
        self.delay_restart_secs = None
        if self.is_multiday and self.strategy.get("is_delay_restart") and self.strategy.get("delay_restart_time"):
            self.delay_restart_secs = parse_hms(self.strategy["delay_restart_time"])

        self.status: str = dep["status"]
        self.status_reason: str | None = dep.get("status_reason")
        self.paused = self.status == "paused"
        self.overnight_paused = self.paused and (self.status_reason or "").startswith(OVERNIGHT_REASON)
        self.last_unpaused: datetime | None = None
        self.legs: list[LegState] = []
        self._by_key: dict[Key, list[LegState]] = {}      # open legs by contract key
        self._waiting: list[LegState] = []                # legs with a pending conditional entry
        self._listening: set[Key] = set()
        self._subscribed: set[Key] = set()
        self.realised = float(dep.get("realised_pnl") or 0)
        self._trade_id = 0
        self._task: asyncio.Task | None = None
        self._done = asyncio.Event()
        self._squaring = False
        self._entering = False
        self._armed = False
        self._pending_tasks: set[asyncio.Task] = set()
        self._overall_reentry_left = 0
        self._overall_reverse = False
        self._overall_mode = "RE_ASAP"
        self._overall = None
        self.last_error: str | None = None
        self._last_reconcile = time.time()        # first periodic position check one full interval after start
        self.updated = time.time()

    def start(self):
        self._task = asyncio.get_running_loop().create_task(self.run(), name=f"deployment-{self.id}")
        return self._task

    async def pause(self):
        if self.status not in ("running", "scheduled"):
            return
        self.paused = True
        self._set_status("paused", "paused by user")
        self._event("Paused: no new entries or exits until resumed")

    async def resume(self, exit_date: date | None = None):
        if self.status != "paused":
            return
        if self.overnight_paused and not (8 * 3600 <= secs_now() < MARKET_CLOSE_SECS):
            raise ValueError("Restart is available between 08:00 and 15:30 -- the strategy stays paused overnight")
        if exit_date is not None and exit_date != self.exit_date:
            if exit_date < today_ist() or (self.cycle_expiry and exit_date > self.cycle_expiry):
                raise ValueError(f"exit_date must be between today and the contract expiry ({self.cycle_expiry})")
            self.exit_date, self.exit_dt = exit_date, at(exit_date, self.exit_secs)
            self.db.submit(store.update_deployment_dates, self.id, self.trade_date, exit_date)
            self._event(f"Exit date changed to {exit_date}")
        overnight, self.paused, self.overnight_paused = self.overnight_paused, False, False
        self.last_unpaused = now_ist()
        if overnight and secs_now() < MARKET_OPEN_SECS:
            self._set_status("scheduled", "restarted -- monitoring starts at 09:15")   # AlgoTest: Scheduled until the open
        else:
            self._set_status("running", "restarted" if overnight else None)
        self._event("Restarted" if overnight else "Resumed")
        if overnight:
            self._spawn(self._rearm_broker_stops())

    async def _rearm_broker_stops(self):
        """After an overnight restart the day-1 SL-L orders are gone (DAY validity):
        put them back once the market is open and flip Scheduled -> Running."""
        await asyncio.sleep(max(0.0, MARKET_OPEN_SECS + 5 - secs_now()))
        if not self.active or self.paused:
            return
        if self.status == "scheduled":
            self._set_status("running", None)
        for leg in self.legs:
            if self.active and not self.paused and self._broker_sl_ok(leg):
                await self._place_sl_order(leg)

    async def _pause_overnight(self):
        """15:45 on a carry day: monitoring stops, resting stops are
        cancelled, the position stays at the broker until the user restarts."""
        for leg in self.legs:
            if leg.status == "open" and leg.sl_order_id:
                try:
                    await self.broker.cancel(leg.sl_order_id, "")
                except XTSError:
                    pass
                leg.sl_order_id = leg.sl_payload = None
                self._persist_leg(leg)
        self.paused = self.overnight_paused = True
        self._set_status("paused", f"{OVERNIGHT_REASON} on {next_trading_day(today_ist())}")
        self._event("Strategy has been paused -- market closed; restart it before the open to continue monitoring", "warn")

    async def cancel_deployment(self):
        """AlgoTest 'Cancel Deployment' on a scheduled / paused strategy: no further
        orders; an open position stays at the broker for the user."""
        await self.switch_to_manual("cancelled", status="cancelled")

    async def squareoff(self, reason: str = "manual"):
        await self._squareoff_all(reason)
        if self.status not in TERMINAL_STATUSES:
            self._finish("squared_off", reason)

    async def switch_to_manual(self, reason: str = "manual", status: str = "manual"):
        """AlgoTest 'Switch to Manual': disconnect the strategy. Open positions stay at the
        broker for the user to manage; our resting / pending orders are cancelled and no
        further orders are generated."""
        if self.status in TERMINAL_STATUSES:
            return
        self._squaring = True
        for leg in self.legs:
            if leg.status in ("open", "exiting"):
                filled = None
                for oid in (leg.sl_order_id, leg.exit_order_id if leg.status == "exiting" else None):
                    if not oid:
                        continue
                    try:
                        await self.broker.cancel(oid, "")
                    except XTSError as e:
                        await self.broker.refresh_order(oid)
                        latest = self.broker.tracker.latest(oid)
                        if latest is not None and latest.status == "Filled":
                            filled = latest
                        else:
                            self._event(f"Leg {leg.leg_number}: could not cancel order {oid} ({e}) -- cancel it at the broker", "warn")
                if filled is not None:
                    leg.sl_order_id = None
                    self._close_leg(leg, filled.avg_price, "stoploss")
                    continue
                leg.sl_order_id = leg.sl_payload = None
                leg.exit_price, leg.exit_ts, leg.exit_reason = self.store.ltp(leg.key), now_ist(), "manual"
                leg.pnl = round(leg.unrealised(leg.exit_price), 2)
                leg.status = "manual"
                self._persist_leg(leg)
                self._event(f"Leg {leg.leg_number}: {leg.side} {leg.qty} {getattr(leg.contract, 'symbol', '')} handed over to manual at {leg.exit_price}")
            elif leg.status == "waiting":
                leg.status, leg.error = "skipped", "switched to manual before the entry condition was met"
                self._persist_wait(leg, "cancelled")
                self._persist_leg(leg)
            elif leg.status == "entering" and leg.entry_order_id:
                try:
                    await self.broker.cancel(leg.entry_order_id, leg.entry_payload.get("orderUniqueIdentifier", ""))
                except XTSError:
                    pass
        self._waiting.clear()
        self._finish(status, reason)

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    @property
    def active(self) -> bool:
        return self.status not in TERMINAL_STATUSES

    def unrealised(self) -> float:
        return sum(leg.unrealised(self.store.ltp(leg.key)) for leg in self.legs if leg.key and leg.is_open)

    def snapshot(self) -> dict:
        legs = []
        for leg in self.legs:
            if leg.kind == "observation" and leg.status == "done":
                continue
            ltp = self.store.ltp(leg.key) if leg.key else None
            legs.append({
                "leg_number": leg.leg_number, "attempt": leg.attempt, "status": leg.status,
                "symbol": getattr(leg.contract, "symbol", None), "side": leg.side, "qty": leg.qty,
                "entry_price": leg.entry_price, "entry_date": _day(leg.entry_ts), "entry_time": _clock(leg.entry_ts),
                "initial_sl": leg.base_sl, "stoploss": leg.sl, "target": leg.tgt, "ltp": ltp,
                "exit_price": leg.exit_price, "exit_date": _day(leg.exit_ts), "exit_time": _clock(leg.exit_ts),
                "exit_reason": leg.exit_reason,
                "pnl": round(leg.pnl if leg.status in ("closed", "manual") else leg.unrealised(ltp), 2),
                "wait": leg.wait, "trigger": leg.trigger, "error": leg.error,
            })
        unreal = self.unrealised()
        return {
            "deployment_id": self.id, "user_id": self.user_id, "strategy_id": self.dep["strategy_id"],
            "strategy_name": self.dep.get("strategy_name"), "version": self.dep["version"], "mode": self.mode,
            "strategy_type": self.strategy_type, "status": self.status, "status_reason": self.status_reason,
            "trade_date": str(self.trade_date), "exit_date": str(self.exit_date),
            "cycle_expiry": str(self.cycle_expiry) if self.cycle_expiry else None,
            "entry_time": hms(self.entry_secs), "exit_time": hms(self.exit_secs),
            "realised_pnl": round(self.realised, 2), "unrealised_pnl": round(unreal, 2),
            "mtm": round(self.realised + unreal, 2), "legs": legs, "updated": self.updated,
            "last_unpaused": self.last_unpaused.strftime("%Y-%m-%d %H:%M:%S") if self.last_unpaused else None,
            # overall risk as currently armed (overall_sl is the TRAILED stop, as a positive MTM distance)
            "overall_sl": (self._overall or {}).get("sl_now", (self._overall or {}).get("sl")),
            "overall_target": (self._overall or {}).get("tgt"),
            "overall_best_mtm": (self._overall or {}).get("best"),
        }

    async def run(self):
        try:
            if self.restore:
                await self._restore()
            else:
                if self._exec_time_note:
                    self._event(self._exec_time_note, "warn")
                if not self._plan_dates():
                    return
                if not self._dte_allowed():
                    return
                if not await self._wait_for_entry():
                    return
                await self._enter_all(reverse=False, mode="ENTRY")
            if self.active:
                await self._monitor_until_exit()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"[RUN {self.id}] failed: {e}")
            self._event(f"Runner failed: {e}", "error")
            try:
                await self._squareoff_all("runner_error")
            except Exception as e2:
                logger.error(f"[RUN {self.id}] square-off after failure also failed: {e2}")
            self._finish("error", str(e)[:300])
        finally:
            self._disarm()
            for t in list(self._pending_tasks):
                t.cancel()
            await self._release_subscriptions()
            self._done.set()

    def _plan_dates(self) -> bool:
        """Positional: pick the expiry cycle (the first whose exit day is not
        past) and derive entry/exit days the engine's way; rewrite the
        deployment row so restarts and the UI see the real dates."""
        if not self.is_positional:
            return True
        expire_on = str(self.strategy.get("positional_expire_on") or "weekly").lower()
        entry_day = int(self.strategy.get("positional_entry_day") or 0)
        exit_day = int(self.strategy.get("positional_exit_day") or 0)
        if expire_on == "monthly":
            monthly = {}
            for e in self.master.expiries:
                monthly[(e.year, e.month)] = e
            candidates = [e for _, e in sorted(monthly.items())]
        else:
            candidates = list(self.master.expiries)
        today = today_ist()
        for expiry in candidates:
            entry_date = weekdays_before_expiry(expiry, entry_day)
            exit_date = weekdays_before_expiry(expiry, exit_day)
            if exit_date < today or entry_date > exit_date:
                continue
            if exit_date == today and secs_now() >= self.exit_secs:
                continue
            self.cycle_expiry = expiry
            self.trade_date, self.exit_date = entry_date, exit_date
            self.entry_dt, self.exit_dt = at(entry_date, self.entry_secs), at(exit_date, self.exit_secs)
            self.db.submit(store.update_deployment_dates, self.id, entry_date, exit_date)
            self._event(f"Positional cycle: expiry {expiry}, entry {entry_date} {hms(self.entry_secs)}, "
                        f"exit {exit_date} {hms(self.exit_secs)} (T-{entry_day} / T-{exit_day})")
            return True
        self._finish("error", "no positional expiry cycle ahead in the contract master")
        return False

    def _dte_allowed(self) -> bool:
        """Execution Days in DTE mode: run only when today's trading days to
        the weekly expiry (0 = expiry day) is one of `execution_dte`."""
        if self.settings.execution_days_mode != "dte":
            return True
        expiry = self.cycle_expiry or next((e for e in self.master.expiries if e >= self.trade_date), None)
        if expiry is None:
            self._finish("error", "no expiry listed to compute days-to-expiry")
            return False
        dte, d = 0, self.trade_date
        while d < expiry:
            d += timedelta(days=1)
            if is_trading_day(d):
                dte += 1
        if dte in self.settings.execution_dte:
            self._event(f"DTE {dte} (expiry {expiry}) is an execution day")
            return True
        self._finish("cancelled", f"today is DTE {dte} (expiry {expiry}); strategy runs on DTE {list(self.settings.execution_dte)}")
        return False

    async def _wait_for_entry(self) -> bool:
        now = now_ist()
        if now >= self.exit_dt:
            self._finish("completed", "activated after exit time -- nothing to do")
            return False
        if now < self.entry_dt:
            self._set_status("scheduled", f"entry at {self.entry_dt:%Y-%m-%d %H:%M:%S}")
            while True:
                remaining = (self.entry_dt - now_ist()).total_seconds()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, 30.0))
        else:
            self._event(f"Activated after entry time {hms(self.entry_secs)} -- entering now", "warn")
        return True

    def _expiry_for(self, meta: dict) -> date | None:
        if self.is_positional:
            return self.cycle_expiry
        labels = self.master.resolve_expiries(self.trade_date, exclude_same_day=self.is_btst)
        return labels.get(meta["_expiry_type"])

    async def _spot(self) -> float | None:
        key = self.engine.index_key
        if key is None:
            return None
        if key not in self._subscribed:
            await self.feed.subscribe([key])
            self._subscribed.add(key)
        for _ in range(20):
            ltp = self.store.ltp(key)
            if ltp:
                return ltp
            await asyncio.sleep(0.1)
        return None

    async def _resolve_contract(self, leg: LegState):
        """Pick the leg's contract NOW (spot, strike criteria, live quotes),
        subscribe it and size the order. Raises ValueError when impossible."""
        meta = leg.meta
        leg.option_type = meta["_option_type"]
        expiry = self._expiry_for(meta)
        if expiry is None:
            raise ValueError(f"no {meta['_expiry_type']} expiry listed")
        strikes = self.master.strikes(expiry, leg.option_type)
        if strikes.size == 0:
            raise ValueError(f"no {leg.option_type} strikes for expiry {expiry}")
        step = self.master.ladder_step(expiry, leg.option_type)
        spot = await self._spot()
        if spot is None:
            raise ValueError("SENSEX index LTP unavailable (set LIVE_SENSEX_INDEX_ID / check the feed)")
        cands = candidate_strikes(meta, leg.option_type, spot, strikes, step)
        keys, contracts = [], {}
        for s in cands:
            c = self.master.contract(expiry, s, leg.option_type)
            if c is not None:
                contracts[s] = c
                keys.append((c.segment, c.instrument_id))
        if not keys:
            raise ValueError("candidate strikes are not in the contract master")
        # premium-based criteria scan a window through REST quotes (no
        # subscription slots); only the chosen contract gets subscribed.
        if len(keys) > 1:
            await self.feed.snapshot(keys)
        else:
            await self.feed.subscribe(keys)
        quotes = {s: self.store.ltp((c.segment, c.instrument_id)) for s, c in contracts.items()}
        strike, money = select_strike(meta, leg.option_type, spot, strikes, step, quotes)
        contract = contracts.get(strike) or self.master.contract(expiry, strike, leg.option_type)
        if contract is None:
            raise ValueError(f"strike {strike} not in master")
        leg.contract = contract
        leg.key = (contract.segment, contract.instrument_id)
        if len(keys) > 1:
            await self.feed.subscribe([leg.key])
        self._subscribed.add(leg.key)
        lot_units = contract.lot_size or self.master.lot_size or QUANTITY
        leg.qty = leg.lots * self.settings.qty_multiplier * (1 if config.LIVE_ORDER_QTY_IN_LOTS else lot_units)
        if contract.freeze_qty and not config.LIVE_ORDER_QTY_IN_LOTS and leg.qty > contract.freeze_qty:
            raise ValueError(f"quantity {leg.qty} exceeds the exchange freeze quantity {contract.freeze_qty} -- reduce lots")
        what = "observes" if leg.kind == "observation" else leg.side
        self._event(f"Leg {leg.leg_number}: {what} {leg.lots}x {contract.symbol} ({money}, spot {spot:.0f}, LTP {quotes.get(strike)})")
        return spot

    def _next_attempt(self, leg_number: int) -> int:
        return 1 + sum(1 for l in self.legs if l.leg_number == leg_number and l.kind == "trade")

    async def _enter_all(self, reverse: bool, mode: str, held_sides: dict | None = None):
        """Start every leg definition: plain legs enter now, conditional legs
        arm their wait, observation legs arm their trigger. `mode` ENTRY for
        the initial entry, else OVERALL_<re-entry mode>."""
        self._entering = True
        try:
            self.paused = False
            self._set_status("running", None)
            self._trade_id += 1
            self._overall = None
            immediate: list[LegState] = []
            for meta in self.leg_defs:
                seq_cfg = meta.get("sequential_leg")
                if seq_cfg and mode == "ENTRY":
                    # observation leg: watched, never traded
                    leg = LegState(meta["leg_number"], meta, 0, "OBS", entry_mode="OBSERVATION", kind="observation")
                    leg.seq_meta = _prepare_meta(seq_cfg, meta["leg_number"])
                    leg.trade_id = self._trade_id
                    self.legs.append(leg)
                    try:
                        await self._resolve_contract(leg)
                        if not self._arm_condition(leg, meta):
                            raise ValueError("observation leg needs simple momentum or range breakout as trigger")
                    except Exception as e:
                        leg.status, leg.error = "error", str(e)
                        self._event(f"Leg {leg.leg_number}: observation cannot start -- {e}", "error")
                    continue
                base = _prepare_meta(seq_cfg, meta["leg_number"]) if seq_cfg else meta   
                side = str(base.get("position_type", "BUY")).upper()
                if held_sides and base["leg_number"] in held_sides:
                    side = held_sides[base["leg_number"]]
                if reverse:
                    side = _flip(side)
                leg = LegState(base["leg_number"], base, self._next_attempt(base["leg_number"]), side, entry_mode=mode)
                leg.trade_id = self._trade_id
                self.legs.append(leg)
                try:
                    await self._resolve_contract(leg)
                except Exception as e:
                    leg.status, leg.error = "error", str(e)
                    self._event(f"Leg {leg.leg_number}: cannot resolve contract -- {e}", "error")
                    continue
                use_momentum = base.get("is_simple_momentum") and (mode == "ENTRY" or mode.startswith("OVERALL_RE_MOMENTUM"))
                if use_momentum and self._arm_condition(leg, base, momentum_only=True):
                    continue
                if mode == "ENTRY" and base.get("is_range_breakout") and self._arm_condition(leg, base):
                    continue
                immediate.append(leg)
            self._arm()
            await self._place_entries(immediate)
            # legs whose condition could not even be armed count as failed entries
            rejected = [l for l in self.legs if l.trade_id == self._trade_id and l.kind == "trade"
                        and l.status == "error" and l not in immediate]
            await self._after_entries(immediate + rejected)
        finally:
            self._entering = False

    async def _after_entries(self, batch: list[LegState]):
        failed = [l for l in batch if l.status == "error"]
        filled = [l for l in batch if l.is_open]
        if failed and self.settings.squareoff_on_entry_error and (filled or self._any_open()):
            self._event(f"{len(failed)} leg(s) failed to enter -- squaring off the filled leg(s)", "error")
            await self._squareoff_all("entry_error")
            self._finish("error", f"leg(s) {', '.join(str(l.leg_number) for l in failed)} failed to enter")
            return
        if failed and not filled and not self._any_open() and not self._waiting:
            self._finish("error", f"no leg could enter: {failed[0].error}")
            return
        if failed:
            self._set_status("running", f"{len(failed)} leg(s) failed to enter")
        self._recompute_overall()

    def _any_open(self) -> bool:
        return any(l.is_open for l in self.legs)

    async def _place_entries(self, legs: list[LegState]):
        if not legs:
            return
        # every leg is sent at the same moment, no buy-before-sell ordering
        await asyncio.gather(*(self._place_entry(l) for l in legs))

    async def _place_entry(self, leg: LegState):
        ls = self._ls(leg.leg_number)
        if ls.delay_entry_sec and leg.entry_mode == "ENTRY":
            await asyncio.sleep(ls.delay_entry_sec)       # "Delay entry by", per leg
        if self._squaring or now_ist() >= self.exit_dt:
            leg.status, leg.error = "skipped", "exit time reached before entry"
            return
        ltp = self.store.ltp(leg.key)
        if not ltp:
            await self.feed.snapshot([leg.key])
            ltp = self.store.ltp(leg.key)
        if not ltp:
            leg.status, leg.error = "error", "no LTP for the contract"
            self._event(f"Leg {leg.leg_number}: no LTP for {leg.contract.symbol}", "error")
            return
        self._warn_if_stale(leg, "entry")
        # the price the strategy MEANT to enter at: the momentum / cost level when the
        # wait was on this contract's premium, else the LTP now (tgt_sl_ref_price = TRIGGER)
        leg.ref_price = leg.trigger if (leg.trigger is not None and leg.watch_key == leg.key) else ltp
        order_type = ls.entry_order_type
        _, bid, ask = self.store.quote(leg.key)
        price = order_limit_price(ltp, leg.side, order_type, ls.entry_limit_buffer, ls.buffer_kind("entry"),
                                  leg.contract.tick_size, bid, ask)
        tag = f"D{self.id}{leg.tag('E')}"[:20]
        payload = self.broker.build_order(
            segment="BSEFO", instrument_id=leg.contract.instrument_id, side=leg.side, quantity=leg.qty,
            order_type="LIMIT", product=ls.product, limit_price=price, tag=tag)
        leg.entry_payload = payload
        leg.status = "entering"
        leg.live_leg_id = await asyncio.to_thread(store.insert_leg, self._leg_row(leg))
        self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=None,
                       unique_tag=tag, action="place", side=leg.side, order_type=order_type, quantity=leg.qty,
                       price=price or ltp, status="sent", payload=payload)
        try:
            app_id = await self.broker.place(payload)
        except XTSError as e:
            leg.status, leg.error = "error", f"entry rejected: {e}"
            self._event(f"Leg {leg.leg_number}: entry order rejected by broker -- {e}", "error")
            self._persist_leg(leg)
            return
        leg.entry_order_id = app_id
        self._persist_leg(leg)
        upd = await self._await_fill(leg, app_id, payload, ls.entry_convert_to_market_sec,
                                     ls.entry_limit_buffer, ls.buffer_kind("entry"), order_type)
        if upd is None or upd.status != "Filled" or upd.filled_qty <= 0:
            reason = (upd.reason if upd else "") or (f"not filled within {self.settings.order_timeout_sec}s" if upd is None
                                                    else upd.status.lower())
            leg.status, leg.error = "error", f"entry {reason}"
            self._event(f"Leg {leg.leg_number}: entry failed -- {reason}", "error")
            self._persist_leg(leg)
            return
        if upd.filled_qty < leg.qty:
            self._event(f"Leg {leg.leg_number}: partially filled {upd.filled_qty}/{leg.qty}; running with the filled quantity", "warn")
            leg.qty = upd.filled_qty
        self._on_entry_filled(leg, upd.avg_price or ltp)

    CHASE_STEP_SECS = 2      # how often an unfilled limit is re-priced while chasing

    def _mpp(self, leg: LegState, side: str, fallback: float) -> float:
        ltp, bid, ask = self.store.quote(leg.key)
        return mpp_price(ltp or fallback, side, bid, ask, leg.contract.tick_size)

    async def _modify_to_mpp(self, leg: LegState, app_id: str, payload: dict) -> None:
        price = self._mpp(leg, payload["orderSide"], float(payload.get("limitPrice") or 0))
        await self.broker.modify(app_id, payload, order_type="LIMIT", limit_price=price)

    async def _chase(self, leg: LegState, app_id: str, payload: dict, convert_sec: int,
                     buffer: float, buffer_type: str, order_type: str = "LIMIT") -> OrderUpdate | None:
        """AlgoTest's "convert to market after": keep re-pricing the open limit
        to LTP +/- buffer every CHASE_STEP_SECS; once `convert_sec` has passed
        without a fill, re-price it as an MPP order."""
        deadline = time.monotonic() + convert_sec
        side = payload["orderSide"]
        while True:
            upd = await self.broker.wait_terminal(app_id, min(self.CHASE_STEP_SECS, max(0.2, deadline - time.monotonic())))
            if upd is not None:
                return upd
            try:
                if time.monotonic() >= deadline:
                    self._event(f"Leg {leg.leg_number}: order {app_id} not filled in {convert_sec}s -- converting to MPP", "warn")
                    await self._modify_to_mpp(leg, app_id, payload)
                    return await self.broker.wait_terminal(app_id, 15.0)
                ltp = self.store.ltp(leg.key)
                _, bid, ask = self.store.quote(leg.key)
                new = order_limit_price(ltp, side, order_type, buffer, buffer_type, leg.contract.tick_size, bid, ask) if ltp else None
                if new and new != payload.get("limitPrice"):
                    await self.broker.modify(app_id, payload, order_type="LIMIT", limit_price=new)
            except XTSError as e:
                await self.broker.refresh_order(app_id)         # it may have filled between the wait and the modify
                latest = self.broker.tracker.latest(app_id)
                if latest is not None and latest.terminal:
                    return latest
                self._event(f"Leg {leg.leg_number}: re-pricing order {app_id} failed -- {e}", "warn")
                if time.monotonic() >= deadline:
                    return await self.broker.wait_terminal(app_id, 5.0)

    async def _await_fill(self, leg: LegState, app_id: str, payload: dict, convert_sec: int = 0,
                          buffer: float = 0.0, buffer_type: str = "POINTS", order_type: str = "LIMIT") -> OrderUpdate | None:
        if convert_sec > 0:
            upd = await self._chase(leg, app_id, payload, convert_sec, buffer, buffer_type, order_type)
        else:
            upd = await self.broker.wait_terminal(app_id, self.settings.order_timeout_sec)
        if upd is None:
            self._event(f"Leg {leg.leg_number}: order {app_id} open after {self.settings.order_timeout_sec}s -- cancelling", "warn")
            try:
                await self.broker.cancel(app_id, payload.get("orderUniqueIdentifier", ""))
            except XTSError as e:
                logger.warning(f"[RUN {self.id}] cancel {app_id} failed: {e}")
            upd = await self.broker.wait_terminal(app_id, 5.0)
            if upd is None:
                latest = self.broker.tracker.latest(app_id)
                if latest is not None and latest.filled_qty > 0:
                    upd = OrderUpdate(app_id, "Filled", latest.filled_qty, latest.avg_price, "partial after cancel", latest.tag)
        if upd is not None:
            self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=app_id,
                           unique_tag=payload.get("orderUniqueIdentifier", ""), action="update", side=payload.get("orderSide"),
                           order_type=payload.get("orderType"), quantity=payload.get("orderQuantity"),
                           price=payload.get("limitPrice"), status=upd.status, filled_qty=upd.filled_qty,
                           avg_price=upd.avg_price, reason=upd.reason, payload=upd.raw)
            if upd.status == "Cancelled" and upd.filled_qty > 0:
                upd = OrderUpdate(app_id, "Filled", upd.filled_qty, upd.avg_price, "partial then cancelled", upd.tag, upd.raw)
        return upd

    def _on_entry_filled(self, leg: LegState, price: float):
        leg.entry_price = round(price, 2)
        leg.entry_ts = now_ist()
        leg.spot_at_entry = self.store.ltp(self.engine.index_key) if self.engine.index_key else None
        leg.status = "open"
        leg.sl = leg.base_sl = self._level(leg, "stoploss")
        leg.tgt = self._level(leg, "target")
        self._init_trailing(leg)
        self._by_key.setdefault(leg.key, []).append(leg)
        self._event(f"Leg {leg.leg_number}: {leg.side} {leg.qty} {leg.contract.symbol} filled @ {leg.entry_price}"
                    + (f", SL {leg.sl}" if leg.sl else "") + (f", TGT {leg.tgt}" if leg.tgt else ""))
        self._persist_leg(leg)
        self._listen(leg.key)
        self._recompute_overall()
        if self._broker_sl_ok(leg):
            self._spawn(self._place_sl_order(leg))      # rest the stop-loss at the broker as an SL-L order

    def _wait_deadline(self, kind: str, tomorrow: bool = False) -> datetime:
        """Momentum and at-cost waits fill on the entry day only for BTST
        (the engine scans day 1); everything else may fill until just before
        the exit."""
        if self.is_btst and not tomorrow:          # momentum / cost / same-day range: day 1 only
            return at(self.trade_date, LAST_EXIT_SECS)
        return self.exit_dt - timedelta(seconds=self.REENTRY_CUTOFF_SECS)

    def _arm_condition(self, leg: LegState, meta: dict, momentum_only: bool = False) -> bool:
        """Turn the leg into a waiting one per its momentum / range settings.
        Returns False when the leg has no condition (enter immediately)."""
        if meta.get("is_simple_momentum"):
            mtype = str(meta.get("momentum_type") or "").upper()
            value = float(meta.get("momentum_value") or 0)
            underlying = mtype.startswith("UNDERLYING")
            watch = self.engine.index_key if underlying else leg.key
            base = self.store.ltp(watch)
            if base is None or not mtype.endswith(("_UP", "_DOWN")):
                leg.status, leg.error = "error", f"momentum base price unavailable ({mtype})"
                self._event(f"Leg {leg.leg_number}: {leg.error}", "error")
                return True
            up = mtype.endswith("_UP")
            if "PERCENT" in mtype:
                trigger = base * (1 + (value if up else -value) / 100)
            else:
                trigger = base + (value if up else -value)
            leg.wait, leg.watch_key, leg.trigger, leg.trigger_up = "momentum", watch, round(trigger, 2), up
            leg.wait_deadline = self._wait_deadline("momentum")
            self._start_wait(leg, f"momentum {mtype} {value}: waits for {'>=' if up else '<='} {leg.trigger} (base {base})")
            return True
        if momentum_only:
            return False
        if meta.get("is_range_breakout"):
            underlying = str(meta.get("range_breakout_type") or "instrument").lower() == "underlying"
            leg.watch_key = self.engine.index_key if underlying else leg.key
            leg.range_high_side = str(meta.get("range_on") or "high").lower() == "high"
            end_secs = parse_hms(meta.get("range_end_time"))
            end_day = meta.get("range_end_day")
            tomorrow = False
            if self.is_positional and self.cycle_expiry and str(end_day or "").strip().lstrip("-").isdigit():
                end_date = weekdays_before_expiry(self.cycle_expiry, int(end_day))
            elif self.is_btst and str(end_day or "today").strip().lower() == "tomorrow":
                end_date, tomorrow = self.exit_date, True
            else:
                end_date = self.trade_date
            leg.range_end_dt = at(end_date, end_secs)
            if leg.range_end_dt <= now_ist():
                leg.status, leg.error = "error", (f"range window already over: range_end_time {hms(end_secs)} on {end_date} "
                                                  f"is before the entry at {now_ist():%H:%M:%S}")
                self._event(f"Leg {leg.leg_number}: {leg.error} -- range_end_time must be after the moment the leg starts", "error")
                return True
            if leg.range_end_dt >= self.exit_dt:
                leg.status, leg.error = "error", "range window ends after exit_time -- no room to enter"
                self._event(f"Leg {leg.leg_number}: {leg.error}", "error")
                return True
            leg.wait = "range"
            leg.range_hi = leg.range_lo = None
            leg.wait_deadline = self._wait_deadline("range", tomorrow)
            self._start_wait(leg, f"range breakout on {'spot' if underlying else 'premium'} "
                                  f"{'High' if leg.range_high_side else 'Low'}, window until {leg.range_end_dt:%Y-%m-%d %H:%M}")
            return True
        return False

    def _arm_cost_wait(self, leg: LegState, cost: float, exited_above: bool):
        """RE_COST: same contract, enter when the price comes back to cost."""
        leg.wait, leg.watch_key, leg.trigger = "cost", leg.key, round(cost, 2)
        leg.trigger_up = not exited_above          # exited above cost -> wait for price <= cost
        leg.wait_deadline = self._wait_deadline("cost")
        self._start_wait(leg, f"RE_COST waits for price to return to {leg.trigger}")

    def _start_wait(self, leg: LegState, what: str):
        leg.status = "waiting"
        self._waiting.append(leg)
        self._listen(leg.watch_key)
        self._event(f"Leg {leg.leg_number}: {what}")
        self._persist_wait(leg, "waiting")

    WAIT_SAVE_EVERY_SECS = 5      # range high/low progress is written at most this often per leg

    def _persist_wait(self, leg: LegState, status: str):
        """Write the pending wait so a restart can re-arm it exactly (contract,
        trigger, range so far, deadline, and the leg definition itself)."""
        c = leg.contract
        row = {
            "deployment_id": self.id, "leg_number": leg.leg_number, "attempt": leg.attempt, "kind": leg.wait or "",
            "leg_kind": leg.kind, "entry_mode": leg.entry_mode, "side": leg.side,
            "instrument_id": getattr(c, "instrument_id", None),
            "watch_segment": leg.watch_key[0] if leg.watch_key else None,
            "watch_instrument_id": leg.watch_key[1] if leg.watch_key else None,
            "quantity": leg.qty, "lots": leg.lots, "trigger_price": leg.trigger, "trigger_up": leg.trigger_up,
            "range_hi": leg.range_hi, "range_lo": leg.range_lo, "range_end_at": to_naive_ist(leg.range_end_dt),
            "range_high_side": leg.range_high_side, "deadline": to_naive_ist(leg.wait_deadline),
            "trade_id": leg.trade_id, "reentry_sl_left": leg.reentry_sl_left, "reentry_tgt_left": leg.reentry_tgt_left,
            "meta": {"leg": leg.meta, "seq_meta": leg.seq_meta}, "status": status,
        }
        leg.wait_saved_at = time.time()
        self.db.submit(store.upsert_wait, row)

    def _check_waits(self, key: Key, price: float, ts: float):
        if not self._waiting or self._squaring or self.paused or not self._gate_open():
            return
        now = now_ist()
        for leg in list(self._waiting):
            if leg.watch_key != key or leg.status != "waiting":
                continue
            if leg.wait_deadline and now >= leg.wait_deadline:
                self._waiting.remove(leg)
                leg.status, leg.error = "skipped", "condition not met before the deadline"
                self._event(f"Leg {leg.leg_number}: {leg.wait} condition never met -- skipped", "warn")
                self._persist_wait(leg, "skipped")
                continue
            hit = False
            if leg.wait == "momentum":
                hit = price >= leg.trigger if leg.trigger_up else price <= leg.trigger
            elif leg.wait == "cost":
                hit = price >= leg.trigger if leg.trigger_up else price <= leg.trigger
            elif leg.wait == "range":
                if now <= leg.range_end_dt:
                    leg.range_hi = price if leg.range_hi is None else max(leg.range_hi, price)
                    leg.range_lo = price if leg.range_lo is None else min(leg.range_lo, price)
                    if ts - leg.wait_saved_at >= self.WAIT_SAVE_EVERY_SECS:
                        self._persist_wait(leg, "waiting")
                    continue
                if leg.range_hi is None:
                    continue
                hit = price >= leg.range_hi if leg.range_high_side else price <= leg.range_lo
            if hit:
                self._waiting.remove(leg)
                self._persist_wait(leg, "done")
                leg.wait = None
                self._event(f"Leg {leg.leg_number}: condition met at {price}")
                if leg.kind == "observation":
                    leg.status = "done"
                    self._spawn(self._enter_sequential(leg))
                else:
                    leg.status = "pending"
                    self._spawn(self._enter_conditional(leg))

    async def _enter_conditional(self, leg: LegState):
        await self._place_entry(leg)
        await self._after_entries([leg])

    async def _enter_sequential(self, obs: LegState):
        """The observation triggered: resolve the sequential leg fresh, then
        enter it (or arm its own momentum / range)."""
        meta = obs.seq_meta
        leg = LegState(meta["leg_number"], meta, self._next_attempt(meta["leg_number"]),
                       str(meta.get("position_type", "BUY")).upper(), entry_mode="SEQUENTIAL")
        leg.trade_id = self._trade_id
        self.legs.append(leg)
        try:
            await self._resolve_contract(leg)
        except Exception as e:
            leg.status, leg.error = "error", str(e)
            self._event(f"Leg {leg.leg_number}: sequential leg cannot resolve -- {e}", "error")
            await self._after_entries([leg])
            return
        if self._arm_condition(leg, meta):
            return
        await self._enter_conditional(leg)

    # ------------------------------------------------------------------ levels
    def _level(self, leg: LegState, kind: str) -> float | None:
        m = leg.meta
        if not m.get(f"is_{kind}"):
            return None
        ltype = str(m.get(f"{kind}_type") or "POINTS").upper()
        value = float(m.get(f"{kind}_value") or 0)
        if kind == "target":
            direction, spot_dir = leg.direction, (1 if leg.spot_bullish else -1)
        else:
            direction, spot_dir = -leg.direction, (-1 if leg.spot_bullish else 1)
        tick = leg.contract.tick_size if leg.contract else 0.05
        # Tgt/SL ref price: TRADED = the fill, TRIGGER = the price the strategy meant to enter at
        base = leg.entry_price
        if self._ls(leg.leg_number).tgt_sl_ref_price == "TRIGGER" and leg.ref_price:
            base = leg.ref_price
        if ltype == "POINTS":
            return round_tick(base + direction * value, tick)
        if ltype == "PERCENT":
            return round_tick(base * (1 + direction * value / 100), tick)
        if leg.spot_at_entry is None:
            self._event(f"Leg {leg.leg_number}: {kind} type {ltype} needs the index LTP -- ignored", "warn")
            return None
        if ltype == "UNDERLYING_POINTS":
            return round(leg.spot_at_entry + spot_dir * value, 2)
        if ltype == "UNDERLYING_PERCENT":
            return round(leg.spot_at_entry * (1 + spot_dir * value / 100), 2)
        self._event(f"Leg {leg.leg_number}: unsupported {kind}_type {ltype}", "warn")
        return None

    def _init_trailing(self, leg: LegState):
        m = leg.meta
        if not m.get("is_trail_sl") or leg.sl is None or str(m.get("stoploss_type", "")).upper() in UNDERLYING_TYPES:
            return
        ttype = str(m.get("trail_sl_type") or "POINTS").upper()
        moves, gain = m.get("instrument_moves"), m.get("stoploss_moves")
        if not moves or gain is None:
            return
        if ttype == "POINTS":
            leg.trail_step, leg.trail_gain = float(moves), float(gain)
        elif ttype == "PERCENT":
            leg.trail_step = round(leg.entry_price * float(moves) / 100, 2)
            leg.trail_gain = round(leg.sl * float(gain) / 100, 2)
        if leg.trail_step is not None and leg.trail_step <= 0:
            leg.trail_step = None

    def _recompute_overall(self):
        """Overall SL/target thresholds on the legs of the CURRENT trade that
        have filled; PERCENT types scale with the entry value, which grows as
        conditional legs join (the engine tracks entry_value bar by bar)."""
        s = self.strategy
        has_sl, has_tgt = bool(s.get("is_strategy_sl")), bool(s.get("is_strategy_target"))
        if not has_sl and not has_tgt:
            self._overall = None
            return
        legs = [l for l in self.legs if l.trade_id == self._trade_id and l.entry_price and l.kind == "trade"
                and l.status in ("open", "exiting", "closed")]
        entry_value = sum(abs(l.entry_price * l.qty) for l in legs)
        if entry_value <= 0:
            return

        def threshold(ttype, value):
            if value is None:
                return None
            ttype = str(ttype or "POINTS").upper()
            if ttype in ("PERCENT", "TOTAL_PREMIUM_PERCENT"):
                return round(entry_value * abs(float(value)) / 100, 2)
            return abs(float(value))

        prev = self._overall or {}
        ov = {
            "sl": threshold(s.get("strategy_sl_type"), s.get("strategy_sl_value")) if has_sl else None,
            "tgt": threshold(s.get("strategy_target_type"), s.get("strategy_target_value")) if has_tgt else None,
            "best": prev.get("best", 0.0), "trail_step": None, "trail_gain": None, "entry_value": entry_value,
        }
        if s.get("is_overall_trail_sl") and ov["sl"]:
            ttype = str(s.get("overall_trail_sl_type") or "POINTS").upper()
            moves, gain = s.get("overall_instrument_move"), s.get("overall_stoploss_move")
            if moves and gain is not None:
                if ttype in ("POINTS", "MTM"):
                    ov["trail_step"], ov["trail_gain"] = float(moves), float(gain)
                elif ttype == "PERCENT":
                    ov["trail_step"] = round(ov["sl"] * float(moves) / 100, 2)
                    ov["trail_gain"] = round(ov["sl"] * float(gain) / 100, 2)
        changed = prev.get("sl") != ov["sl"] or prev.get("tgt") != ov["tgt"]
        self._overall = ov
        if self._trade_id == 1 and not prev and self._overall_reentry_left == 0:
            if s.get("is_overall_reentry_sl") and s.get("overall_reentry_sl_value"):
                self._overall_reentry_left = int(s["overall_reentry_sl_value"])
                self._overall_mode = str(s.get("overall_reentry_sl_type") or "RE_ASAP").upper()
            elif s.get("is_overall_reentry_target") and s.get("overall_reentry_target_value"):
                self._overall_reentry_left = int(s["overall_reentry_target_value"])
                self._overall_mode = str(s.get("overall_reentry_target_type") or "RE_ASAP").upper()
            self._overall_reverse = self._overall_mode.endswith("REVERSE")
        if changed:
            parts = []
            if ov["sl"]:
                parts.append(f"overall SL {ov['sl']:.0f}")
            if ov["tgt"]:
                parts.append(f"overall target {ov['tgt']:.0f}")
            if parts:
                self._event("Armed " + ", ".join(parts) + f" (entry value {entry_value:.0f})")

    def _arm(self):
        self._armed = True
        if self.engine.index_key:
            self._listen(self.engine.index_key)

    def _listen(self, key: Key | None):
        if key is None or key in self._listening:
            return
        self._listening.add(key)
        self.store.on_tick(key, self._on_tick)
        if self.settings.trade_monitoring == "CANDLE_CLOSE":
            self.store.on_candle(key, self._on_candle)

    def _disarm(self):
        for key in list(self._listening):
            self.store.off_tick(key, self._on_tick)
            self.store.off_candle(key, self._on_candle)
        self._listening.clear()
        self._armed = False

    def _gate_open(self) -> bool:
        """Carry days of a BTST / positional hold: monitoring resumes at
        delay_restart_time when is_delay_restart is on (else at the open)."""
        if not self.is_multiday:
            return True
        today = today_ist()
        if today <= self.trade_date:
            return True
        s = secs_now()
        if s < MARKET_OPEN_SECS:
            return False
        if self.delay_restart_secs is not None and s < self.delay_restart_secs:
            return False
        return True

    def _on_tick(self, key: Key, price: float, ts: float):
        if self._squaring:
            return
        self._check_waits(key, price, ts)
        if self.paused or not self._gate_open():
            return
        freq = self.settings.monitoring_frequency_sec
        if self.settings.trade_monitoring == "LTP":
            for leg in self._by_key.get(key, ()):
                if freq and ts - leg.last_check < freq:
                    continue
                leg.last_check = ts
                self._check_leg(leg, price)
        if key == self.engine.index_key:
            for legs in self._by_key.values():
                for leg in legs:
                    if leg.status == "open" and (str(leg.meta.get("stoploss_type", "")).upper() in UNDERLYING_TYPES
                                                 or str(leg.meta.get("target_type", "")).upper() in UNDERLYING_TYPES):
                        self._check_leg(leg, self.store.ltp(leg.key) or leg.entry_price, price)
        # candle-close monitoring judges the overall SL/target at the candle close too
        # (a loss that recovers inside the minute does not exit); the daily-loss kill
        # switch stays tick-driven in both modes
        self._check_overall(only_kill=self.settings.trade_monitoring != "LTP")

    def _on_candle(self, key: Key, candle: Candle):
        if self.paused or self._squaring or not self._gate_open():
            return
        for leg in self._by_key.get(key, ()):
            self._check_leg(leg, candle.close)
        self._check_overall()

    def _check_leg(self, leg: LegState, price: float, spot: float | None = None):
        if leg.status != "open" or price is None:
            return
        if leg.next_exit_try and time.time() < leg.next_exit_try:
            return                                 # a failed exit is cooling down -- no order storm
        m = leg.meta
        if leg.trail_step:
            # Monitoring CONTINUOUS: trail on the best price seen since the last
            # application. DELAYED: only the LTP at the end of each interval counts
            # (a spike that fades inside the interval is ignored). Either way the
            # stop is applied -- and the SL-L order modified -- once per trail_frequency_sec.
            ls = self._ls(leg.leg_number)
            now_t = time.time()
            favorable = (price - leg.entry_price) * leg.direction
            if ls.trail_monitoring == "CONTINUOUS" and favorable > leg.best_move:
                leg.best_move = favorable
            if now_t >= leg.next_trail_at:
                leg.next_trail_at = now_t + ls.trail_frequency_sec
                if ls.trail_monitoring == "DELAYED" and favorable > leg.best_move:
                    leg.best_move = favorable
                steps = math.floor(leg.best_move / leg.trail_step)
                new_sl = leg.base_sl + leg.direction * steps * leg.trail_gain
                if (leg.direction > 0 and new_sl > leg.sl) or (leg.direction < 0 and new_sl < leg.sl):
                    leg.sl = round_tick(new_sl, leg.contract.tick_size)
                    self.db.submit(store.update_leg, leg.live_leg_id, stoploss_price=leg.sl)
                    if leg.sl_order_id:
                        self._spawn(self._modify_sl_order(leg))
        sl_type = str(m.get("stoploss_type") or "POINTS").upper()
        tgt_type = str(m.get("target_type") or "POINTS").upper()
        if spot is None and (sl_type in UNDERLYING_TYPES or tgt_type in UNDERLYING_TYPES):
            spot = self.store.ltp(self.engine.index_key) if self.engine.index_key else None
        hit = None
        if leg.sl is not None and leg.sl_order_id:
            # the stop rests at the broker as an SL-L order: the exchange triggers it.
            # Only step in if the price has run THROUGH its limit (triggered but unfilled).
            lim = float(leg.sl_payload.get("limitPrice") or 0)
            if lim and ((leg.direction > 0 and price < lim) or (leg.direction < 0 and price > lim)):
                hit = "stoploss"
        elif leg.sl is not None:
            if sl_type in PREMIUM_TYPES:
                if (leg.direction > 0 and price <= leg.sl) or (leg.direction < 0 and price >= leg.sl):
                    hit = "stoploss"
            elif spot is not None:
                if (leg.spot_bullish and spot <= leg.sl) or (not leg.spot_bullish and spot >= leg.sl):
                    hit = "stoploss"
        if hit is None and leg.tgt is not None:
            if tgt_type in PREMIUM_TYPES:
                if (leg.direction > 0 and price >= leg.tgt) or (leg.direction < 0 and price <= leg.tgt):
                    hit = "target"
            elif spot is not None:
                if (leg.spot_bullish and spot >= leg.tgt) or (not leg.spot_bullish and spot <= leg.tgt):
                    hit = "target"
        if hit:
            leg.status = "exiting"
            self._event(f"Leg {leg.leg_number}: {hit} hit at {price} (level {leg.sl if hit == 'stoploss' else leg.tgt})")
            self._spawn(self._exit_leg(leg, hit))

    def _trade_realised(self) -> float:
        return sum(l.pnl for l in self.legs if l.trade_id == self._trade_id and l.status == "closed")

    def _check_overall(self, only_kill: bool = False):
        if self._squaring:
            return
        unreal = self.unrealised()
        if self.settings.max_daily_loss and (self.realised + unreal) <= -self.settings.max_daily_loss:
            self._event(f"Max daily loss {self.settings.max_daily_loss:.0f} breached (MTM {self.realised + unreal:.0f}) -- squaring off", "error")
            self._squaring = True          # claim the breach NOW: the next tick must not fire it again
            self._spawn(self._finish_after_squareoff("max_daily_loss"))
            return
        ov = self._overall
        if not ov or only_kill:
            return
        mtm = self._trade_realised() + unreal
        sl = ov["sl"]
        if sl is not None and ov["trail_step"]:
            if mtm > ov["best"]:
                ov["best"] = mtm
            steps = math.floor(max(ov["best"], 0) / ov["trail_step"])
            # engine rule: every `trail_step` of best profit tightens the stop by
            # `trail_gain`, never past break-even (threshold clipped at 0)
            sl = max(ov["sl"] - steps * ov["trail_gain"], 0.0)
            if sl != ov.get("sl_now", ov["sl"]):
                self._event(f"Overall SL trailed: best MTM {ov['best']:.0f} -> {steps} step(s), "
                            f"stop now at MTM {-sl:.0f} (was {-ov.get('sl_now', ov['sl']):.0f})")
            ov["sl_now"] = sl
        if sl is not None and mtm <= -sl:
            self._event(f"Overall stop-loss hit: MTM {mtm:.0f} (threshold {-sl:.0f})", "warn")
            self._squaring = True          # claim the breach NOW: the next tick must not fire it again
            self._spawn(self._overall_hit("overall_stoploss"))
            return
        if ov["tgt"] is not None and mtm >= ov["tgt"]:
            self._event(f"Overall target hit: MTM {mtm:.0f} (threshold {ov['tgt']:.0f})")
            self._squaring = True          # claim the breach NOW: the next tick must not fire it again
            self._spawn(self._overall_hit("overall_target"))

    async def _overall_hit(self, reason: str):
        held = {l.leg_number: l.side for l in self.legs if l.trade_id == self._trade_id and l.kind == "trade" and l.side in ("BUY", "SELL")}
        await self._squareoff_all(reason)
        if not self.active:                # a manual square-off finished the deployment meanwhile
            return
        if self._overall_reentry_left > 0 and now_ist() < self.exit_dt - timedelta(seconds=self.REENTRY_CUTOFF_SECS):
            self._overall_reentry_left -= 1
            self._event(f"Overall re-entry {self._overall_mode} ({self._overall_reentry_left} left)")
            self._squaring = False
            await self._enter_all(reverse=self._overall_reverse, mode=f"OVERALL_{self._overall_mode}", held_sides=held)
        else:
            self._finish("squared_off", reason)

    async def _finish_after_squareoff(self, reason: str):
        await self._squareoff_all(reason)
        self._finish("squared_off", reason)

    async def _exit_leg(self, leg: LegState, reason: str):
        if leg.status not in ("open", "exiting"):
            return
        leg.status = "exiting"
        side = _flip(leg.side)
        ltp = self.store.ltp(leg.key) or leg.entry_price
        self._warn_if_stale(leg, "exit")
        ls = self._ls(leg.leg_number)
        order_type = ls.exit_order_type
        _, bid, ask = self.store.quote(leg.key)
        price = order_limit_price(ltp, side, order_type, ls.exit_limit_buffer, ls.buffer_kind("exit"),
                                  leg.contract.tick_size, bid, ask)
        tag = f"D{self.id}{leg.tag('X')}"[:20]
        app_id = payload = None
        # A stop-loss already resting at the broker is REUSED: the SL-L order is
        # modified into this exit (target, exit time, square-off, overall rule, or a
        # stop the market ran through) instead of sending a second order.
        sl_oid, sl_payload = leg.sl_order_id, leg.sl_payload
        if sl_oid:
            leg.sl_order_id = None                       # take the order over from its watcher
            latest = self.broker.tracker.latest(sl_oid)
            if latest is not None and latest.status == "Filled":
                self._close_leg(leg, latest.avg_price or ltp, "stoploss")
                return
            try:
                await self.broker.modify(sl_oid, sl_payload, order_type="LIMIT", limit_price=price)
                app_id, payload = sl_oid, sl_payload
                self._event(f"Leg {leg.leg_number}: SL-L order {sl_oid} at the {self.venue} converted to the {reason} exit (limit {price})")
            except XTSError as e:
                await self.broker.refresh_order(sl_oid)
                latest = self.broker.tracker.latest(sl_oid)
                if latest is not None and latest.status == "Filled":      # the stop filled first
                    self._close_leg(leg, latest.avg_price or ltp, "stoploss")
                    return
                self._event(f"Leg {leg.leg_number}: could not convert SL-L order {sl_oid} ({e}) -- cancelling it and sending a fresh exit", "warn")
                try:
                    await self.broker.cancel(sl_oid, sl_payload.get("orderUniqueIdentifier", ""))
                except XTSError as e2:
                    self._event(f"Leg {leg.leg_number}: SL-L order {sl_oid} could not be cancelled ({e2}) -- check the broker terminal", "error")
        if app_id is None:
            payload = self.broker.build_order(
                segment="BSEFO", instrument_id=leg.contract.instrument_id, side=side, quantity=leg.qty,
                order_type="LIMIT", product=ls.product, limit_price=price, tag=tag)
            self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=None,
                           unique_tag=tag, action="place", side=side, order_type=order_type, quantity=leg.qty,
                           price=price, status="sent", reason=reason, payload=payload)
            try:
                app_id = await self.broker.place(payload)
            except XTSError as e:
                leg.status, leg.error = "open", f"exit rejected: {e}"
                self._event(f"Leg {leg.leg_number}: EXIT ORDER REJECTED -- {e}. Position still open!", "error")
                self._exit_failed(leg)
                self._persist_leg(leg)
                return
        leg.exit_order_id = app_id
        if ls.exit_convert_to_market_sec > 0:
            upd = await self._chase(leg, app_id, payload, ls.exit_convert_to_market_sec,
                                    ls.exit_limit_buffer, ls.buffer_kind("exit"), order_type)
        else:
            upd = await self.broker.wait_terminal(app_id, self.settings.order_timeout_sec)
        if (upd is None or upd.status != "Filled") and self.settings.exit_fallback_market:
            self._event(f"Leg {leg.leg_number}: exit limit not filled -- re-pricing as MPP", "warn")
            try:
                if upd is None:
                    await self._modify_to_mpp(leg, app_id, payload)
                    upd = await self.broker.wait_terminal(app_id, 15.0)
                if upd is None or upd.status != "Filled":
                    mkt = dict(payload, orderType="LIMIT",
                               limitPrice=self._mpp(leg, side, ltp),
                               orderUniqueIdentifier=(tag + "M")[:20])
                    if upd is None:
                        try:
                            await self.broker.cancel(app_id, tag)
                        except XTSError:
                            pass
                    app_id = await self.broker.place(mkt)
                    leg.exit_order_id = app_id
                    upd = await self.broker.wait_terminal(app_id, 15.0)
            except XTSError as e:
                self._event(f"Leg {leg.leg_number}: market exit failed -- {e}", "error")
        if upd is not None:
            self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=app_id,
                           unique_tag=tag, action="update", side=side, order_type=order_type, quantity=leg.qty,
                           price=price, status=upd.status, filled_qty=upd.filled_qty, avg_price=upd.avg_price,
                           reason=upd.reason, payload=upd.raw)
        if upd is None or upd.status != "Filled":
            leg.status, leg.error = "open", "exit not filled"
            self._event(f"Leg {leg.leg_number}: EXIT NOT FILLED -- position still open, square off manually", "error")
            self._exit_failed(leg)
            self._persist_leg(leg)
            return
        self._close_leg(leg, upd.avg_price or ltp, reason)

    def _close_leg(self, leg: LegState, price: float, reason: str):
        """Book a filled exit exactly once, whichever path saw it first (the
        exit order, or the SL-L order filling at the broker)."""
        if leg.status == "closed":
            return
        leg.exit_price = round(price, 2)
        leg.exit_ts = now_ist()
        leg.exit_reason = reason
        leg.pnl = round((leg.exit_price - leg.entry_price) * leg.direction * leg.qty, 2)
        leg.status = "closed"
        leg.sl_order_id = None
        self.realised += leg.pnl
        self._event(f"Leg {leg.leg_number}: exit {_flip(leg.side)} @ {leg.exit_price} ({reason}) PnL {leg.pnl:+.2f}")
        self._persist_leg(leg)
        self.db.submit(store.set_realised_pnl, self.id, self.realised)
        if leg.key in self._by_key and leg in self._by_key[leg.key]:
            self._by_key[leg.key].remove(leg)
        if reason in ("stoploss", "target") and not self._squaring:
            self._spawn(self._maybe_reenter(leg, reason))

    # ------------------------------------------------------------------ stop-loss resting at the broker (SL-L)
    def _ls(self, leg_number: int) -> LegExecution:
        """The leg-level execution settings this leg runs with."""
        ls = self._leg_settings.get(leg_number)
        if ls is None:
            ls = self._leg_settings[leg_number] = self.settings.for_leg(leg_number)
        return ls

    def _broker_sl_ok(self, leg: LegState) -> bool:
        """AlgoTest sends the stop as an SL-L order in advance. We do the same
        when the stop is a premium level, exits are LIMIT type, monitoring is
        on LTP (a resting order cannot wait for a candle close) and the broker
        lists StopLimit; otherwise the stop stays in software."""
        ls = self._ls(leg.leg_number)
        return (leg.sl is not None and leg.status == "open" and not leg.sl_order_id
                and str(leg.meta.get("stoploss_type") or "POINTS").upper() in PREMIUM_TYPES
                and ls.sl_order_at_broker
                and self.settings.trade_monitoring == "LTP"
                and getattr(self.broker, "supports_stop_limit", None) is not False)

    def _sl_prices(self, leg: LegState) -> tuple[float, float]:
        ls = self._ls(leg.leg_number)
        if ls.exit_order_type == "MPP":
            limit_buffer, buffer_type = mpp_pct(leg.sl), "PERCENT"
        else:
            limit_buffer, buffer_type = ls.exit_limit_buffer, ls.buffer_kind("exit")
        return stop_limit_prices(leg.sl, _flip(leg.side), ls.exit_trigger_buffer, limit_buffer,
                                 buffer_type, leg.contract.tick_size)

    async def _place_sl_order(self, leg: LegState):
        ls = self._ls(leg.leg_number)
        side = _flip(leg.side)
        trigger, limit = self._sl_prices(leg)
        tag = f"D{self.id}{leg.tag('S')}"[:20]
        payload = self.broker.build_order(
            segment="BSEFO", instrument_id=leg.contract.instrument_id, side=side, quantity=leg.qty,
            order_type="STOPLIMIT", product=ls.product, limit_price=limit, stop_price=trigger, tag=tag)
        self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=None,
                       unique_tag=tag, action="place", side=side, order_type="STOPLIMIT", quantity=leg.qty,
                       price=limit, status="sent", reason=f"stoploss trigger {trigger}", payload=payload)
        try:
            app_id = await self.broker.place(payload)
        except XTSError as e:
            self._event(f"Leg {leg.leg_number}: SL-L order rejected by broker ({e}) -- the stop-loss stays in software", "warn")
            return
        if leg.status != "open":                     # the leg closed while the order was being placed
            try:
                await self.broker.cancel(app_id, tag)
            except XTSError:
                pass
            return
        leg.sl_order_id, leg.sl_payload = app_id, payload
        self._event(f"Leg {leg.leg_number}: stop-loss resting at the {self.venue} as SL-L {side} -- trigger {trigger}, limit {limit}")
        self._persist_leg(leg)
        self._spawn(self._watch_sl_order(leg, app_id))

    SL_REFRESH_EVERY_SECS = 30       # safety-net poll of a resting SL-L order when the order socket is quiet

    async def _watch_sl_order(self, leg: LegState, app_id: str):
        quiet = 0.0
        while leg.sl_order_id == app_id and leg.status in ("open", "exiting"):
            upd = await self.broker.tracker.wait_terminal(app_id, 5.0)
            if upd is None:
                quiet += 5.0
                if quiet >= self.SL_REFRESH_EVERY_SECS:
                    quiet = 0.0
                    await self.broker.refresh_order(app_id)
                continue
            if leg.sl_order_id != app_id:            # _exit_leg took the order over
                return
            self.db.submit(store.log_order, deployment_id=self.id, live_leg_id=leg.live_leg_id, app_order_id=app_id,
                           unique_tag=(leg.sl_payload or {}).get("orderUniqueIdentifier", ""), action="update",
                           side=_flip(leg.side), order_type="STOPLIMIT", quantity=leg.qty, status=upd.status,
                           filled_qty=upd.filled_qty, avg_price=upd.avg_price, reason=upd.reason, payload=upd.raw)
            if upd.status == "Filled":
                self._event(f"Leg {leg.leg_number}: stop-loss order filled at the {self.venue} @ {upd.avg_price}")
                self._close_leg(leg, upd.avg_price or leg.sl, "stoploss")
            else:
                leg.sl_order_id = leg.sl_payload = None
                self._event(f"Leg {leg.leg_number}: SL-L order {upd.status.lower()} by the broker ({upd.reason or 'no reason'}) "
                            f"-- the stop-loss is back in software", "warn")
                self._persist_leg(leg)
            return

    async def _modify_sl_order(self, leg: LegState):
        """Trailing: move the resting SL-L order to the new stop."""
        if leg.sl_busy or not leg.sl_order_id:
            return
        leg.sl_busy = True
        try:
            trigger, limit = self._sl_prices(leg)
            await self.broker.modify(leg.sl_order_id, leg.sl_payload, order_type="STOPLIMIT",
                                     limit_price=limit, stop_price=trigger)
            self._event(f"Leg {leg.leg_number}: SL-L order trailed -- stop {leg.sl}, trigger {trigger}, limit {limit}")
        except XTSError as e:
            self._event(f"Leg {leg.leg_number}: could not trail the SL-L order ({e})", "warn")
        finally:
            leg.sl_busy = False

    async def _squareoff_all(self, reason: str):
        self._squaring = True
        exits = []
        for leg in self.legs:
            if leg.status == "open":
                leg.status = "exiting"
                exits.append(self._exit_leg(leg, reason))
            elif leg.status == "waiting":
                leg.status, leg.error = "skipped", f"square-off ({reason}) before the entry condition was met"
                self._persist_wait(leg, "cancelled")
                if leg in self._waiting:
                    self._waiting.remove(leg)
            elif leg.status == "entering" and leg.entry_order_id:
                try:
                    await self.broker.cancel(leg.entry_order_id, leg.entry_payload.get("orderUniqueIdentifier", ""))
                except XTSError:
                    pass
        if exits:
            await asyncio.gather(*exits, return_exceptions=True)
        stuck = [l.leg_number for l in self.legs if l.status in ("open", "exiting")]
        if stuck:
            self._event(f"Legs still open after square-off: {stuck}", "error")

    async def _maybe_reenter(self, leg: LegState, reason: str):
        m = leg.meta
        mode = str(m.get("reentry_sl_type" if reason == "stoploss" else "reentry_target_type") or "").upper()
        left = leg.reentry_sl_left if reason == "stoploss" else leg.reentry_tgt_left
        if left <= 0 or not mode or self.paused or self._squaring \
                or now_ist() >= self.exit_dt - timedelta(seconds=self.REENTRY_CUTOFF_SECS):
            return
        sl_left = leg.reentry_sl_left - (1 if reason == "stoploss" else 0)
        tgt_left = leg.reentry_tgt_left - (1 if reason == "target" else 0)

        if mode == "LAZY_LEG":
            cfg = m.get("lazy_leg")
            if not cfg:
                self._event(f"Leg {leg.leg_number}: LAZY_LEG re-entry configured but no lazy_leg definition", "warn")
                return
            lazy = _prepare_meta(cfg, leg.leg_number)
            new = LegState(leg.leg_number, lazy, leg.attempt + 1, str(lazy.get("position_type", "BUY")).upper(), entry_mode="LAZY_LEG")
            # the lazy leg carries its OWN re-entry budget (engine: current_leg_meta = lazy_leg_meta)
        else:
            side = _flip(leg.side) if mode.endswith("REVERSE") else leg.side
            new = LegState(leg.leg_number, m, leg.attempt + 1, side, entry_mode=mode)
            new.reentry_sl_left, new.reentry_tgt_left = sl_left, tgt_left
        new.trade_id = self._trade_id
        self.legs.append(new)
        self._event(f"Leg {leg.leg_number}: re-entry #{new.attempt} ({mode}, SL {new.reentry_sl_left} / TGT {new.reentry_tgt_left} left)")

        if mode.startswith("RE_COST"):
            new.option_type, new.contract, new.key, new.qty = leg.option_type, leg.contract, leg.key, leg.qty
            self._arm_cost_wait(new, leg.entry_price, exited_above=leg.exit_price > leg.entry_price)
            return
        try:
            await self._resolve_contract(new)          # RE_ASAP / RE_MOMENTUM / LAZY_LEG: fresh strike now
        except Exception as e:
            new.status, new.error = "error", str(e)
            self._event(f"Leg {leg.leg_number}: re-entry cannot resolve contract -- {e}", "error")
            return
        if mode.startswith("RE_MOMENTUM") or mode == "LAZY_LEG":
            if self._arm_condition(new, new.meta, momentum_only=True):
                return
        await self._enter_conditional(new)

    async def _monitor_until_exit(self):
        while self.active:
            now = now_ist()
            if now >= self.exit_dt:
                if self.overnight_paused:
                    self._event(f"Exit time {hms(self.exit_secs)} on {self.exit_date} reached while paused -- strategy was not restarted", "error")
                    await self.switch_to_manual("not restarted before exit time -- position still open at the broker", status="error")
                    break
                self._event(f"Exit time {hms(self.exit_secs)} on {self.exit_date} reached -- squaring off")
                await self._squareoff_all("exit_time")
                self._finish("squared_off", "exit_time")
                break
            if self.is_multiday and not self.paused and now.date() < self.exit_date and secs_now() >= OVERNIGHT_PAUSE_SECS:
                await self._pause_overnight()
            if self.mode == "live" and time.time() - self._last_reconcile >= self.RECONCILE_EVERY_SECS \
                    and MARKET_OPEN_SECS <= secs_now() <= MARKET_CLOSE_SECS:
                self._last_reconcile = time.time()
                await self._reconcile_with_broker("periodic")
            for leg in self.legs:                      # once a second: open legs priced on old ticks
                if leg.status == "open":
                    self._warn_if_stale(leg, "monitoring")
            pending = any(l.status in PENDING_STATUSES or l.is_open for l in self.legs)
            if not pending and not self._entering and self.legs and self._overall_reentry_left == 0:
                self._finish("completed", "all legs closed")
                break
            await asyncio.sleep(min(1.0, max(0.05, (self.exit_dt - now).total_seconds())))

    async def _restore(self):
        rows = await asyncio.to_thread(store.list_legs, self.id)
        pending = await asyncio.to_thread(store.list_waits, self.id)
        if self.is_positional and self.status == "scheduled":
            if not self._plan_dates():
                return
        if not rows and not pending and self.status in ("scheduled", "running", "paused"):
            self._event("Worker restarted before entry -- resuming schedule")
            if await self._wait_for_entry():
                await self._enter_all(reverse=False, mode="ENTRY")
            return
        self._event("Worker restarted -- restoring open legs and pending conditional entries from the database", "warn")
        if self.is_positional and self.cycle_expiry is None:
            for r in rows:
                if r.get("expiry"):
                    self.cycle_expiry = _as_date(r["expiry"])
                    break
        book = {}
        try:
            for row in await self.broker.order_book():
                book[str(row.get("AppOrderID"))] = row
        except Exception as e:
            self._event(f"Order book unavailable during restore: {e}", "warn")
        self._trade_id = 1
        for r in rows:
            meta = self.leg_defs[r["leg_number"] - 1] if 0 < r["leg_number"] <= len(self.leg_defs) else {}
            if meta.get("sequential_leg"):
                meta = _prepare_meta(meta["sequential_leg"], r["leg_number"])
            leg = LegState(r["leg_number"], meta, r["attempt"], r["side"], entry_mode=r.get("entry_mode") or "ENTRY")
            leg.trade_id = self._trade_id
            leg.live_leg_id = r["live_leg_id"]
            leg.option_type = r.get("option_type") or leg.option_type
            leg.contract = self.master.by_id.get(int(r["instrument_id"])) if r.get("instrument_id") else None
            leg.key = (SEG_BSEFO, int(r["instrument_id"])) if r.get("instrument_id") else None
            leg.qty, leg.lots = int(r["quantity"]), int(r["lots"])
            leg.status = r["status"]
            leg.entry_order_id, leg.exit_order_id = r.get("entry_order_id"), r.get("exit_order_id")
            leg.entry_price = float(r["entry_price"]) if r.get("entry_price") is not None else None
            leg.spot_at_entry = float(r["underlying_at_entry"]) if r.get("underlying_at_entry") is not None else None
            leg.sl = leg.base_sl = float(r["stoploss_price"]) if r.get("stoploss_price") is not None else None
            leg.tgt = float(r["target_price"]) if r.get("target_price") is not None else None
            leg.exit_price = float(r["exit_price"]) if r.get("exit_price") is not None else None
            leg.pnl = float(r.get("pnl") or 0)
            leg.exit_reason = r.get("exit_reason")
            leg.ref_price = float(r["ref_price"]) if r.get("ref_price") is not None else None
            leg.sl_order_id = r.get("sl_order_id")
            leg.entry_ts = r["entry_time"].replace(tzinfo=IST) if r.get("entry_time") else None
            leg.exit_ts = r["exit_time"].replace(tzinfo=IST) if r.get("exit_time") else None
            self.legs.append(leg)
            if leg.status in ("entering", "exiting"):
                oid = leg.entry_order_id if leg.status == "entering" else leg.exit_order_id
                row = book.get(str(oid)) if oid else None
                upd = OrderUpdate.from_xts(row) if row else None
                if leg.status == "entering":
                    if upd and upd.status == "Filled" and leg.contract:
                        leg.qty = upd.filled_qty or leg.qty
                        await self.feed.subscribe([leg.key]); self._subscribed.add(leg.key)
                        self._on_entry_filled(leg, upd.avg_price)
                    else:
                        if upd and not upd.terminal:
                            try:
                                await self.broker.cancel(oid, "")
                            except XTSError:
                                pass
                        leg.status, leg.error = "error", "entry unresolved across restart"
                        self._persist_leg(leg)
                else:
                    if upd and upd.status == "Filled":
                        leg.exit_price = upd.avg_price
                        leg.pnl = round((leg.exit_price - leg.entry_price) * leg.direction * leg.qty, 2)
                        leg.status, leg.exit_reason = "closed", "restored"
                        self.realised += leg.pnl
                        self._persist_leg(leg)
                    else:
                        leg.status = "open"
                        self._event(f"Leg {leg.leg_number}: exit unresolved across restart -- treating as open", "warn")
            if leg.status == "open" and leg.key and leg.contract:
                await self.feed.subscribe([leg.key]); self._subscribed.add(leg.key)
                self._by_key.setdefault(leg.key, []).append(leg)
                self._init_trailing(leg)
                if leg.sl_order_id:                  # the stop was resting at the broker when the worker stopped
                    sl_row = book.get(str(leg.sl_order_id))
                    sl_upd = OrderUpdate.from_xts(sl_row) if sl_row else None
                    if sl_upd and sl_upd.status == "Filled":
                        self._event(f"Leg {leg.leg_number}: stop-loss order filled at the {self.venue} while the worker was down")
                        self._close_leg(leg, sl_upd.avg_price or leg.sl, "stoploss")
                        continue
                    if sl_upd and not sl_upd.terminal:
                        trigger, limit = self._sl_prices(leg)
                        leg.sl_payload = self.broker.build_order(
                            segment="BSEFO", instrument_id=leg.contract.instrument_id, side=_flip(leg.side),
                            quantity=leg.qty, order_type="STOPLIMIT", product=self._ls(leg.leg_number).product,
                            limit_price=float(sl_row.get("OrderPrice") or limit),
                            stop_price=float(sl_row.get("OrderStopPrice") or trigger),
                            tag=str(sl_row.get("OrderUniqueIdentifier") or ""))
                        self.broker.tracker.handle(sl_upd)
                        self._spawn(self._watch_sl_order(leg, leg.sl_order_id))
                        self._event(f"Leg {leg.leg_number}: SL-L order {leg.sl_order_id} is still resting at the {self.venue} -- watching it")
                    else:
                        leg.sl_order_id = None       # gone (cancelled / not in today's book)
                if not self.paused and secs_now() >= MARKET_OPEN_SECS and self._broker_sl_ok(leg):
                    self._spawn(self._place_sl_order(leg))   # no resting stop: put one back
        if self.engine.index_key:
            await self._spot()
        await self._reconcile_with_broker("restore")   # the broker's book is the truth after a restart
        await self._restore_waits()
        if self.status == "paused":
            self.paused = True
        elif self.is_multiday and secs_now() < MARKET_OPEN_SECS:
            self._set_status("scheduled", "restored after restart -- monitoring starts at 09:15")
            self._spawn(self._rearm_broker_stops())
        else:
            self._set_status("running", "restored after restart")
        self._arm()
        for key in list(self._by_key):
            self._listen(key)
        self._recompute_overall()

    async def _restore_waits(self):
        """Re-arm every pending conditional entry exactly as it was persisted:
        same contract, same trigger / range so far, same deadline."""
        rows = await asyncio.to_thread(store.list_waits, self.id)
        for r in rows:
            meta = (r.get("meta") or {}).get("leg") or {}
            if not meta:
                continue
            leg = LegState(r["leg_number"], meta, r["attempt"], r.get("side") or "OBS",
                           entry_mode=r.get("entry_mode") or "ENTRY", kind=r.get("leg_kind") or "trade")
            leg.seq_meta = (r.get("meta") or {}).get("seq_meta")
            leg.trade_id = int(r.get("trade_id") or 1)
            leg.reentry_sl_left, leg.reentry_tgt_left = int(r.get("reentry_sl_left") or 0), int(r.get("reentry_tgt_left") or 0)
            leg.option_type = meta.get("_option_type") or leg.option_type
            contract = self.master.by_id.get(int(r["instrument_id"])) if r.get("instrument_id") else None
            if contract is None:
                self._event(f"Leg {leg.leg_number}: pending {r['kind']} entry NOT restored -- contract "
                            f"{r.get('instrument_id')} is not in today's master", "warn")
                self.db.submit(store.update_wait, self.id, r["leg_number"], r["attempt"], status="skipped")
                continue
            leg.contract, leg.key = contract, (contract.segment, contract.instrument_id)
            leg.qty, leg.lots = int(r.get("quantity") or 0), int(r.get("lots") or leg.lots)
            await self.feed.subscribe([leg.key]); self._subscribed.add(leg.key)
            leg.wait = r["kind"]
            leg.watch_key = ((int(r["watch_segment"]), int(r["watch_instrument_id"]))
                             if r.get("watch_instrument_id") else leg.key)
            if leg.watch_key not in (leg.key, self.engine.index_key):
                await self.feed.subscribe([leg.watch_key]); self._subscribed.add(leg.watch_key)
            leg.trigger = float(r["trigger_price"]) if r.get("trigger_price") is not None else None
            leg.trigger_up = bool(r["trigger_up"]) if r.get("trigger_up") is not None else True
            leg.range_hi = float(r["range_hi"]) if r.get("range_hi") is not None else None
            leg.range_lo = float(r["range_lo"]) if r.get("range_lo") is not None else None
            leg.range_high_side = bool(r["range_high_side"]) if r.get("range_high_side") is not None else True
            leg.range_end_dt = r["range_end_at"].replace(tzinfo=IST) if r.get("range_end_at") else None
            leg.wait_deadline = r["deadline"].replace(tzinfo=IST) if r.get("deadline") else None
            self.legs.append(leg)
            if leg.wait_deadline and now_ist() >= leg.wait_deadline:
                leg.status, leg.error = "skipped", "deadline passed while the worker was down"
                self._event(f"Leg {leg.leg_number}: pending {leg.wait} entry expired while the worker was down -- skipped", "warn")
                self._persist_wait(leg, "skipped")
                continue
            leg.status = "waiting"
            self._waiting.append(leg)
            self._listen(leg.watch_key)
            detail = (f"trigger {leg.trigger}" if leg.wait in ("momentum", "cost")
                      else f"range {leg.range_lo}..{leg.range_hi}, window until {leg.range_end_dt:%Y-%m-%d %H:%M}")
            self._event(f"Leg {leg.leg_number}: restored pending {leg.wait} entry on {contract.symbol} ({detail})")

    async def _reconcile_with_broker(self, where: str):
        """The broker's position book is the truth. For every leg we believe
        is open, check the broker still holds a position in that contract on
        our side (same product). None -> the leg was closed elsewhere (dealer
        terminal, broker RMS, expiry): mark it closed at the current LTP and
        say so loudly; smaller -> warn. Paper mode has no broker book."""
        if self.mode != "live":
            return
        open_legs = [l for l in self.legs if l.status == "open" and l.key and l.contract]
        if not open_legs:
            return
        try:
            positions = await self.broker.positions()
        except Exception as e:
            self._event(f"Broker position check ({where}) failed: {e}", "warn")
            return
        products = {self._ls(l.leg_number).product for l in open_legs}     # legs may override the product
        net: dict[int, float] = {}
        for p in positions or []:
            try:
                # Symphony's docs say ExchangeInstrumentID; brokers also send ExchangeInstrumentId
                iid = int(p.get("ExchangeInstrumentID") or p.get("ExchangeInstrumentId"))
            except (TypeError, ValueError):
                continue
            if str(p.get("ProductType") or "").upper() not in products:
                continue
            buy, sell = _num(p.get("OpenBuyQuantity")), _num(p.get("OpenSellQuantity"))
            net[iid] = net.get(iid, 0.0) + ((buy - sell) if (buy or sell) else _num(p.get("Quantity")))
        now = now_ist()
        for leg in open_legs:
            # a fill needs a moment to reach the position book: never judge a fresh leg
            if leg.entry_ts and (now - leg.entry_ts).total_seconds() < self.RECONCILE_GRACE_SECS:
                continue
            iid = leg.contract.instrument_id
            if iid not in net:
                # ABSENCE IS NOT PROOF. The book does not list this contract at all (a query
                # or naming problem, or an expired/settled row): say so, never close on it.
                self._event(f"Leg {leg.leg_number}: {leg.contract.symbol} is not listed in the broker's position book "
                            f"({where}) -- cannot confirm the position; leg left open, check the broker terminal", "warn")
                continue
            have = net[iid]
            if have * leg.direction > 0 and abs(have) >= leg.qty:
                continue                                    # broker agrees
            if have * leg.direction > 0:
                self._event(f"Leg {leg.leg_number}: broker holds {abs(have):.0f} of our {leg.qty} units in "
                            f"{leg.contract.symbol} ({where}) -- partial position, check the broker terminal", "warn")
                continue
            # the book LISTS the contract with a flat or opposite net: it really was closed elsewhere
            ltp = self.store.ltp(leg.key) or leg.entry_price
            leg.exit_price, leg.exit_ts, leg.exit_reason = round(ltp, 2), now_ist(), "closed_at_broker"
            leg.pnl = round((leg.exit_price - leg.entry_price) * leg.direction * leg.qty, 2)
            leg.status = "closed"
            self.realised += leg.pnl
            if leg.key in self._by_key and leg in self._by_key[leg.key]:
                self._by_key[leg.key].remove(leg)
            self._event(f"Leg {leg.leg_number}: broker shows NO {leg.side} position in {leg.contract.symbol} ({where}) -- "
                        f"marked closed at LTP {leg.exit_price} (PnL approx {leg.pnl:+.2f}); verify in the broker terminal", "error")
            self._persist_leg(leg)
            self.db.submit(store.set_realised_pnl, self.id, self.realised)

    def _exit_failed(self, leg: LegState):
        """A rejected / unfilled exit leaves the leg open with its stop still
        breached, so the very next tick would send another order. Back off
        5 s, 10 s, 20 s ... capped at 60 s between automatic retries; a manual
        square-off is never blocked by this."""
        leg.exit_fails += 1
        delay = min(5 * 2 ** (leg.exit_fails - 1), 60)
        leg.next_exit_try = time.time() + delay
        self._event(f"Leg {leg.leg_number}: exit attempt {leg.exit_fails} failed -- next automatic retry in {delay}s", "warn")

    def _warn_if_stale(self, leg: LegState, where: str):
        """Warn when the leg's LTP has not updated for STALE_LTP_SECS. One
        warning per leg per STALE_WARN_EVERY_SECS, so a dead feed does not
        flood the log. The index age is printed too: a quiet option with a
        ticking index is just an illiquid strike, both quiet = feed problem."""
        if leg.key is None or not (MARKET_OPEN_SECS <= secs_now() <= MARKET_CLOSE_SECS):
            return
        age = self.store.age(leg.key)
        if age <= self.STALE_LTP_SECS:
            return
        warned = self.__dict__.setdefault("_stale_warned", {})
        now = time.time()
        if now - warned.get(leg.key, 0) < self.STALE_WARN_EVERY_SECS:
            return
        warned[leg.key] = now
        index_age = self.store.age(self.engine.index_key) if self.engine.index_key else float("inf")
        symbol = getattr(leg.contract, "symbol", leg.key)
        self._event(f"Leg {leg.leg_number}: LTP {self.store.ltp(leg.key)} of {symbol} is {age:.0f}s old and not updated "
                    f"({where}); SENSEX tick is {index_age:.0f}s old", "warn")

    def _spawn(self, coro):
        t = asyncio.get_running_loop().create_task(coro)
        self._pending_tasks.add(t)
        t.add_done_callback(self._pending_tasks.discard)
        return t

    def _set_status(self, status: str, reason: str | None):
        self.status, self.status_reason = status, reason
        self.updated = time.time()
        self.db.submit(store.update_deployment_status, self.id, status, reason)
        self.engine.notify(self)

    def _finish(self, status: str, reason: str | None):
        self._disarm()
        self._set_status(status, reason)
        self.db.submit(store.set_realised_pnl, self.id, self.realised)
        self._event(f"Deployment {status} ({reason}); realised PnL {self.realised:+.2f}")

    def _event(self, message: str, level: str = "info"):
        log = {"info": logger.info, "warn": logger.warning, "error": logger.error}.get(level, logger.info)
        log(f"[RUN {self.id}] {message}")
        if level == "error":
            self.last_error = message
        self.updated = time.time()
        self.db.submit(store.log_event, self.id, message, level)
        self.engine.notify(self)

    def _leg_row(self, leg: LegState) -> dict:
        c = leg.contract
        return {
            "deployment_id": self.id, "leg_number": leg.leg_number, "attempt": leg.attempt,
            "exchange_segment": "BSEFO", "instrument_id": getattr(c, "instrument_id", None),
            "symbol": getattr(c, "symbol", None), "expiry": getattr(c, "expiry", None),
            "strike": getattr(c, "strike", None), "option_type": leg.option_type, "side": leg.side,
            "quantity": leg.qty, "lots": leg.lots, "status": leg.status, "entry_mode": leg.entry_mode,
        }

    def _persist_leg(self, leg: LegState):
        if leg.live_leg_id is None:
            self.db.submit(_insert_leg_and_ignore, self._leg_row(leg))
            return
        self.db.submit(store.update_leg, leg.live_leg_id, status=leg.status, entry_order_id=leg.entry_order_id,
                       entry_price=leg.entry_price, entry_time=to_naive_ist(leg.entry_ts),
                       underlying_at_entry=leg.spot_at_entry, stoploss_price=leg.sl, target_price=leg.tgt,
                       exit_order_id=leg.exit_order_id, exit_price=leg.exit_price, exit_time=to_naive_ist(leg.exit_ts),
                       exit_reason=leg.exit_reason, pnl=leg.pnl if leg.status in ("closed", "manual") else None,
                       error=leg.error, quantity=leg.qty, sl_order_id=leg.sl_order_id, ref_price=leg.ref_price)

    async def _release_subscriptions(self):
        keys = [k for k in self._subscribed if k != self.engine.index_key]
        if keys:
            try:
                await self.feed.unsubscribe(keys)
            except Exception:
                pass
        self._subscribed.clear()


def _insert_leg_and_ignore(row: dict):
    store.insert_leg(row)


def _num(v) -> float:
    try:
        return float(v) if v not in (None, "", "NaN") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _flip(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY"


def _day(ts: datetime | None) -> str | None:
    return ts.strftime("%Y-%m-%d") if ts else None


def _clock(ts: datetime | None) -> str | None:
    return ts.strftime("%H:%M:%S") if ts else None


def _as_date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])
