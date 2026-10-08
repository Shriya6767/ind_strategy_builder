"""API-side live trading service: execution settings, activation (creates
the deployment row and forwards the command to the worker), commands, and
read models. Shared with the worker's auto-activation job.
"""
from src.core.modules import RealDictCursor, httpx, orjson, date, datetime
from src.core import config
from src.core.config import Database
from src.core.logger import get_logger
from src.core.constant import REENTRY_MODES
from src.services.get_strategy import GetStrategyService
from src.live import deployment_store as store
from src.live.broker_store import BrokerAccountService
from src.live.execution import ExecutionSettings, STRATEGY_KEYS
from src.live.timeutil import today_ist, next_trading_day, is_trading_day, weekday_code, now_ist, parse_hms, hms, secs_now

ENTRY_TIME_RANGE = (parse_hms("09:15:15"), parse_hms("15:29:00"))
EXIT_TIME_RANGE = (parse_hms("09:15:05"), parse_hms("15:29:30"))


def _secs(strategy: dict, key: str) -> int:
    return parse_hms(strategy.get(f"{key}_time") or ("09:30:00" if key == "entry" else "15:15:00")) \
        + int(strategy.get(f"{key}_delay") or 0) * 60

logger = get_logger(__name__)

# "sequential" is timed like intraday (same-day entry/exit); its legs carry
# observation -> sequential_leg hand-offs, exactly as in the backtest engine.
SUPPORTED_STRATEGY_TYPES = ("intraday", "sequential", "btst", "positional")
SUPPORTED_REENTRY = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_COST", "RE_COST_REVERSE",
                     "RE_MOMENTUM", "RE_MOMENTUM_REVERSE", "LAZY_LEG")
SUPPORTED_OVERALL_REENTRY = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_MOMENTUM", "RE_MOMENTUM_REVERSE")
MOMENTUM_TYPES = ("POINTS_UP", "POINTS_DOWN", "PERCENT_UP", "PERCENT_DOWN",
                  "UNDERLYING_POINTS_UP", "UNDERLYING_POINTS_DOWN", "UNDERLYING_PERCENT_UP", "UNDERLYING_PERCENT_DOWN")


class LiveValidationError(ValueError):
    pass


class LiveTradeService:

    @staticmethod
    def save_execution_settings(user_id: int, body: dict) -> dict:
        strategy_id = int(body.get("strategy_id") or 0)
        if not strategy_id:
            raise LiveValidationError("strategy_id is required")
        LiveTradeService._strategy_head(strategy_id, user_id)
        settings = ExecutionSettings.from_dict(body.get("settings") or body)
        broker_account_id = body.get("broker_account_id")
        if broker_account_id:
            if not BrokerAccountService.get_public(int(broker_account_id), user_id):
                raise LiveValidationError("Broker account not found")
            broker_account_id = int(broker_account_id)
        elif settings.mode == "live":
            raise LiveValidationError("A broker account is required for live mode")
        else:
            broker_account_id = None
        version = int(body.get("version") or 0)
        auto = bool(body.get("auto_activate", False))
        store.upsert_execution_setting(user_id, strategy_id, version, broker_account_id, settings.to_dict(), auto)
        return {"strategy_id": strategy_id, "version": version, "broker_account_id": broker_account_id,
                "settings": settings.to_dict(), "auto_activate": auto, "status": "ready"}


    @staticmethod
    def overview(user_id: int) -> dict:
        today = today_ist()
        settings = [{
            "strategy_id": r["strategy_id"], "version": r["version"], "broker_account_id": r["broker_account_id"],
            "settings": r["settings"], "auto_activate": r["auto_activate"], "updated_at": str(r["updated_at"]),
        } for r in store.list_execution_settings(user_id)]
        deployments = [_row_public(r) for r in store.list_deployments(user_id, today, with_active=True)]
        return {"trade_date": str(today), "execution_settings": settings, "deployments": deployments}


    @staticmethod
    def _strategy_head(strategy_id: int, user_id: int, version: int = 0) -> dict:
        """(strategy_name, version) of the caller's strategy: the given
        version, or the latest one when version is 0/None."""
        conn = Database.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            if version:
                cur.execute("""SELECT strategy_name, version FROM strategy
                               WHERE strategy_id = %s AND user_id = %s AND version = %s""",
                            (strategy_id, user_id, version))
            else:
                cur.execute("""SELECT strategy_name, version FROM strategy WHERE strategy_id = %s AND user_id = %s
                               ORDER BY version DESC LIMIT 1""", (strategy_id, user_id))
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        if not row:
            raise LiveValidationError("Strategy not found")
        return row


    @staticmethod
    def validate_strategy(strategy: dict, legs: list[dict], settings: ExecutionSettings | None = None) -> None:
        stype = str(strategy.get("strategy_type", "intraday")).lower()
        if stype not in SUPPORTED_STRATEGY_TYPES:
            raise LiveValidationError(f"strategy_type '{stype}' is not supported live yet (intraday and btst are)")
        if str(strategy.get("underlying_type", "cash")).lower() != "cash":
            raise LiveValidationError("Only cash strategies can be traded live")
        if not legs:
            raise LiveValidationError("Strategy has no selected legs")
        if stype == "intraday" and parse_hms(strategy.get("exit_time", "15:15:00")) <= parse_hms(strategy.get("entry_time", "09:30:00")):
            raise LiveValidationError("exit_time must be after entry_time")
        if settings is not None and settings.strategy_execution_time:
            entry_secs = parse_hms(strategy.get("entry_time", "09:30:00")) + int(strategy.get("entry_delay") or 0) * 60
            try:
                exec_secs = parse_hms(settings.strategy_execution_time)
            except (ValueError, IndexError):
                raise LiveValidationError("strategy_execution_time must be HH:MM:SS")
            if not entry_secs - 59 <= exec_secs <= entry_secs:
                raise LiveValidationError(
                    f"strategy_execution_time {settings.strategy_execution_time} must be within 59 seconds BEFORE "
                    f"the strategy entry time {strategy.get('entry_time')}")
        if stype in ("btst", "positional") and settings is not None and \
                any(settings.for_leg(i).product != "NRML" for i in range(1, len(legs) + 1)):
            raise LiveValidationError(f"{stype} holds overnight: product must be NRML (the broker force-closes MIS positions before the close)")
        if stype == "positional":
            expire_on = str(strategy.get("positional_expire_on") or "weekly").lower()
            if expire_on not in ("weekly", "monthly"):
                raise LiveValidationError("positional_expire_on must be weekly or monthly")
            entry_day = int(strategy.get("positional_entry_day") or 0)
            exit_day = int(strategy.get("positional_exit_day") or 0)
            if entry_day < exit_day:
                raise LiveValidationError("positional_entry_day must be >= positional_exit_day (days before expiry)")
            if entry_day == exit_day and parse_hms(strategy.get("exit_time", "15:15:00")) <= parse_hms(strategy.get("entry_time", "09:30:00")):
                raise LiveValidationError("positional entry and exit fall on the same day: exit_time must be after entry_time")

        def check_leg(leg: dict, label: str, nested: bool = False):
            if int(leg.get("lot_size") or 0) <= 0:
                raise LiveValidationError(f"{label}: lot_size must be at least 1")
            if leg.get("is_simple_momentum"):
                mtype = str(leg.get("momentum_type") or "").upper()
                if mtype not in MOMENTUM_TYPES or leg.get("momentum_value") is None:
                    raise LiveValidationError(f"{label}: momentum_type must be one of {MOMENTUM_TYPES} with a momentum_value")
            if leg.get("is_range_breakout"):
                if str(leg.get("range_on") or "").lower() not in ("high", "low"):
                    raise LiveValidationError(f"{label}: range_on must be High or Low")
                if not leg.get("range_end_time"):
                    raise LiveValidationError(f"{label}: range_end_time is required for range breakout")
                if str(leg.get("range_breakout_type") or "instrument").lower() not in ("instrument", "underlying"):
                    raise LiveValidationError(f"{label}: range_breakout_type must be instrument or underlying")
                if stype in ("intraday", "sequential") and \
                        parse_hms(leg["range_end_time"]) <= parse_hms(strategy.get("entry_time", "09:30:00")):
                    raise LiveValidationError(
                        f"{label}: range_end_time {leg['range_end_time']} must be after entry_time "
                        f"{strategy.get('entry_time')} -- the range is built between the two")
            for flag, key in (("is_reentry_sl", "reentry_sl_type"), ("is_reentry_target", "reentry_target_type")):
                if leg.get(flag):
                    mode = str(leg.get(key) or "").upper()
                    if mode not in SUPPORTED_REENTRY:
                        raise LiveValidationError(f"{label}: re-entry mode {mode} is not supported (use {', '.join(SUPPORTED_REENTRY)})")
                    if mode == "LAZY_LEG" and not leg.get("lazy_leg"):
                        raise LiveValidationError(f"{label}: LAZY_LEG re-entry needs a lazy_leg definition")
            if leg.get("lazy_leg"):
                check_leg(leg["lazy_leg"], f"{label} lazy leg", nested=True)
            seq = leg.get("sequential_leg")
            if seq:
                if nested:
                    raise LiveValidationError(f"{label}: a nested leg cannot carry its own sequential_leg")
                if not (leg.get("is_simple_momentum") or leg.get("is_range_breakout")):
                    raise LiveValidationError(f"{label}: an observation leg with a sequential_leg needs simple momentum or range breakout as its trigger")
                check_leg(seq, f"{label} sequential leg", nested=True)

        for i, leg in enumerate(legs, start=1):
            check_leg(leg, f"Leg {i}")
        for flag, key in (("is_overall_reentry_sl", "overall_reentry_sl_type"), ("is_overall_reentry_target", "overall_reentry_target_type")):
            if strategy.get(flag):
                mode = str(strategy.get(key) or "").upper()
                if mode not in SUPPORTED_OVERALL_REENTRY:
                    raise LiveValidationError(f"Overall re-entry mode {mode} is not supported")


    @staticmethod
    def activation_timings(user_id: int, strategy_id: int, version: int = 0) -> dict:
        """Defaults for the 'Edit Entry/Exit Timings' box of the activation dialog."""
        head = LiveTradeService._strategy_head(strategy_id, user_id, version)
        loaded = GetStrategyService.get_strategy(strategy_id, head["strategy_name"], head["version"], user_id)
        if not loaded.get("success"):
            raise LiveValidationError(loaded.get("error") or "Strategy could not be loaded")
        strategy = loaded["data"]["strategy"]
        stype = str(strategy.get("strategy_type", "intraday")).lower()
        today = today_ist()
        out = {"strategy_id": strategy_id, "version": head["version"], "strategy_type": stype,
               "entry_date": str(today), "entry_time": hms(_secs(strategy, "entry")),
               "exit_date": str(today), "exit_time": hms(_secs(strategy, "exit")), "expiry": None,
               "editable": {"entry_date": False, "entry_time": False, "exit_date": False, "exit_time": False}}
        if stype == "btst":
            out["exit_date"] = str(next_trading_day(today))
            out["editable"].update(entry_time=True, exit_date=True, exit_time=True)
        elif stype.startswith("positional"):
            fwd = LiveTradeService.forward("/internal/positional-cycle",
                                           body={"strategy": strategy, "exit_secs": _secs(strategy, "exit")})
            if not fwd.get("forwarded"):
                raise RuntimeError(fwd.get("error") or "worker unreachable")
            out.update(fwd["data"])
            out["editable"] = dict.fromkeys(out["editable"], True)
        return out

    @staticmethod
    def create_deployment(user_id: int, body: dict) -> dict:
        """Validates and inserts a 'scheduled' deployment for today. Does NOT
        start it -- call `forward(id, 'activate')` (API) or engine.activate (worker)."""
        strategy_id = int(body.get("strategy_id") or 0)
        if not strategy_id:
            raise LiveValidationError("strategy_id is required")
        saved = store.get_execution_setting(user_id, strategy_id)
        version = int(body.get("version") or (saved or {}).get("version") or 0)
        head = LiveTradeService._strategy_head(strategy_id, user_id, version)
        version = head["version"]
        raw_settings = {k: v for k, v in ((saved or {}).get("settings") or {}).items() if k in STRATEGY_KEYS}
        raw_settings.update(body.get("settings") or {})
        settings = ExecutionSettings.from_dict(raw_settings)
        broker_account_id = body.get("broker_account_id") or (saved or {}).get("broker_account_id")
        broker_account_id = int(broker_account_id) if broker_account_id else None

        today = today_ist()
        if store.has_active_deployment(user_id, strategy_id):
            raise LiveValidationError("This strategy is already deployed")

        if settings.mode == "live":
            if not broker_account_id:
                raise LiveValidationError("Live mode needs a broker account")
            sess = BrokerAccountService.get_session(broker_account_id)
            if not sess or sess["user_id"] != user_id:
                raise LiveValidationError("Broker is not logged in -- log in on the Broker Setup page first")
        elif broker_account_id and not BrokerAccountService.get_public(broker_account_id, user_id):
            raise LiveValidationError("Broker account not found")

        loaded = GetStrategyService.get_strategy(strategy_id, head["strategy_name"], version, user_id)
        if not loaded.get("success"):
            raise LiveValidationError(loaded.get("error") or "Strategy could not be loaded")
        strategy, legs = dict(loaded["data"]["strategy"]), loaded["data"]["legs"]
        stype = str(strategy.get("strategy_type", "intraday")).lower()
        multiday = stype == "btst" or stype.startswith("positional")
        # Activation dialog: intraday fixed; BTST entry time / exit date / exit time; positional all four.
        for key, rng in (("entry_time", ENTRY_TIME_RANGE), ("exit_time", EXIT_TIME_RANGE)):
            if body.get(key) and multiday:
                s = parse_hms(body[key])
                if not (rng[0] <= s <= rng[1]):
                    raise LiveValidationError(f"{key} must be between {hms(rng[0])} and {hms(rng[1])}")
                strategy[key], strategy[key.replace("_time", "_delay")] = hms(s), 0
        LiveTradeService.validate_strategy(strategy, legs, settings)
        entry_secs, exit_secs = _secs(strategy, "entry"), _secs(strategy, "exit")
        entry_date, exit_date = today, (next_trading_day(today) if stype == "btst" else today)
        if body.get("entry_date") and stype.startswith("positional"):
            entry_date = date.fromisoformat(str(body["entry_date"]))
            strategy["entry_date_override"] = str(entry_date)
        if body.get("exit_date") and multiday:
            exit_date = date.fromisoformat(str(body["exit_date"]))
            if stype.startswith("positional"):
                strategy["exit_date_override"] = str(exit_date)
        if entry_date < today or not is_trading_day(entry_date):
            raise LiveValidationError("entry_date must be a trading day today or later")
        if exit_date < entry_date or not is_trading_day(exit_date):
            raise LiveValidationError("exit_date must be a trading day on or after the entry date")
        if exit_date == entry_date and exit_secs <= entry_secs:
            raise LiveValidationError("Exit time must be after the entry time")
        if exit_date == today and secs_now() >= exit_secs:
            raise LiveValidationError(f"Exit time {strategy.get('exit_time')} has already passed today")
        # DTE mode is judged by the worker (it needs the expiry calendar); weekdays here
        if settings.execution_days_mode == "weekdays" and weekday_code(entry_date) not in settings.execution_days:
            raise LiveValidationError(f"{weekday_code(entry_date)} is not in the strategy's execution days {list(settings.execution_days)}")

        dep_id = store.create_deployment(
            user_id=user_id, strategy_id=strategy_id, strategy_name=head["strategy_name"], version=version,
            broker_account_id=broker_account_id, mode=settings.mode, trade_date=entry_date, exit_date=exit_date,
            settings=settings.to_dict(), snapshot={"strategy": strategy, "legs": legs})
        store.log_event(dep_id, f"Activated by user ({settings.mode} mode, version {version})")
        logger.info(f"[LIVE] deployment {dep_id} created strategy={strategy_id} v{version} user={user_id} mode={settings.mode}")
        return store.get_deployment(dep_id, user_id)


    @staticmethod
    def _headers() -> dict:
        return {"X-Internal-Token": config.LIVE_INTERNAL_TOKEN} if config.LIVE_INTERNAL_TOKEN else {}


    @staticmethod
    def forward(path: str, method: str = "POST", timeout: float = 8.0, body: dict | None = None) -> dict:
        url = f"{config.LIVE_WORKER_URL}{path}"
        try:
            with httpx.Client(timeout=timeout) as http:
                r = http.request(method, url, headers=LiveTradeService._headers(), json=body)
            body = orjson.loads(r.content) if r.content else {}
            if r.status_code >= 400:
                return {"forwarded": False, "error": body.get("detail") or body.get("message") or f"HTTP {r.status_code}"}
            return {"forwarded": True, **(body if isinstance(body, dict) else {"data": body})}
        except httpx.HTTPError as e:
            logger.error(f"[LIVE] worker unreachable ({url}): {e}")
            return {"forwarded": False, "error": "Live worker is not running; the deployment stays scheduled and starts when the worker comes up."}


    @staticmethod
    def activate(user_id: int, body: dict) -> dict:
        dep = LiveTradeService.create_deployment(user_id, body)
        fwd = LiveTradeService.forward(f"/internal/deployments/{dep['deployment_id']}/activate")
        out = _row_public(dep)
        out["worker"] = fwd
        return out


    @staticmethod
    def command(user_id: int, deployment_id: int, cmd: str, body: dict | None = None) -> dict:
        if cmd not in ("pause", "resume", "squareoff", "manual", "cancel", "activate"):
            raise LiveValidationError("Unknown command")
        dep = store.get_deployment(deployment_id, user_id)
        if not dep:
            raise LookupError("Deployment not found")
        if dep["status"] in ("squared_off", "completed", "error", "cancelled", "manual") and cmd != "activate":
            raise LiveValidationError(f"Deployment is already {dep['status']}")
        if cmd == "cancel" and dep["status"] not in ("scheduled", "paused"):
            raise LiveValidationError("Cancel Deployment is only available while scheduled or paused -- use Square Off or Switch to Manual")
        fwd = LiveTradeService.forward(f"/internal/deployments/{deployment_id}/{cmd}", body=body or None)
        if not fwd.get("forwarded"):
            if cmd in ("squareoff", "manual", "cancel"):
                raise RuntimeError(fwd.get("error") or "worker unreachable -- manage the position at the broker terminal")
            store.log_event(deployment_id, f"{cmd}: {fwd.get('error')}", "warn")
        return {"deployment_id": deployment_id, "command": cmd, "worker": fwd}

    @staticmethod
    def squareoff_all(user_id: int) -> dict:
        return LiveTradeService.forward(f"/internal/users/{user_id}/squareoff-all")

    @staticmethod
    def manual_all(user_id: int) -> dict:
        return LiveTradeService.forward(f"/internal/users/{user_id}/manual-all")

    @staticmethod
    def cancel_all(user_id: int) -> list[dict]:
        fwd = LiveTradeService.forward(f"/internal/users/{user_id}/cancel-all")
        if not fwd.get("forwarded"):
            raise RuntimeError(fwd.get("error") or "worker unreachable")
        return fwd.get("data")

    @staticmethod
    def restart_all(user_id: int, body: dict) -> list[dict]:
        items = (body or {}).get("restarts") or []
        if not isinstance(items, list) or not items:
            raise LiveValidationError("restarts must be a non-empty list of {deployment_id, restart_at}")
        fwd = LiveTradeService.forward(f"/internal/users/{user_id}/restart-all", body={"restarts": items})
        if not fwd.get("forwarded"):
            raise RuntimeError(fwd.get("error") or "worker unreachable")
        return fwd.get("data")

    @staticmethod
    def live_snapshots(user_id: int) -> dict:
        return LiveTradeService.forward(f"/internal/users/{user_id}/snapshots", method="GET", timeout=4.0)

    @staticmethod
    def list(user_id: int, trade_date: str | None, include_archived: bool) -> list[dict]:
        d = date.fromisoformat(trade_date) if trade_date else None
        return [_row_public(r) for r in store.list_deployments(user_id, d, include_archived)]

    @staticmethod
    def detail(user_id: int, deployment_id: int) -> dict:
        dep = store.get_deployment(deployment_id, user_id)
        if not dep:
            raise LookupError("Deployment not found")
        out = _row_public(dep)
        out["settings"] = dep["settings"]
        out["legs"] = [_leg_public(r) for r in store.list_legs(deployment_id)]
        out["events"] = [_jsonable(r) for r in store.list_events(deployment_id)]
        return out

    @staticmethod
    def archive(user_id: int, deployment_id: int, archived: bool = True) -> bool:
        return store.archive_deployment(deployment_id, user_id, archived)


def _jsonable(row: dict) -> dict:
    return orjson.loads(orjson.dumps(row, default=str))


def _num(v):
    return float(v) if v is not None else None


def _leg_public(r: dict) -> dict:
    entry_ts, exit_ts = r.get("entry_time"), r.get("exit_time")
    return {
        "leg_number": r["leg_number"], "attempt": r["attempt"], "status": r["status"],
        "symbol": r.get("symbol"), "side": r["side"], "qty": r["quantity"],
        "entry_price": _num(r.get("entry_price")),
        "entry_date": entry_ts.strftime("%Y-%m-%d") if entry_ts else None,
        "entry_time": entry_ts.strftime("%H:%M:%S") if entry_ts else None,
        "stoploss": _num(r.get("stoploss_price")), "target": _num(r.get("target_price")),
        "exit_price": _num(r.get("exit_price")),
        "exit_date": exit_ts.strftime("%Y-%m-%d") if exit_ts else None,
        "exit_time": exit_ts.strftime("%H:%M:%S") if exit_ts else None,
        "exit_reason": r.get("exit_reason"), "pnl": _num(r.get("pnl")), "error": r.get("error"),
    }


def _row_public(r: dict) -> dict:
    return {
        "deployment_id": r["deployment_id"], "strategy_id": r["strategy_id"], "strategy_name": r.get("strategy_name"),
        "version": r["version"], "broker_account_id": r.get("broker_account_id"), "mode": r["mode"],
        "trade_date": str(r["trade_date"]), "exit_date": str(r["exit_date"]), "status": r["status"],
        "status_reason": r.get("status_reason"), "realised_pnl": float(r.get("realised_pnl") or 0),
        "is_archived": r.get("is_archived", False), "created_at": str(r["created_at"]), "updated_at": str(r["updated_at"]),
    }
