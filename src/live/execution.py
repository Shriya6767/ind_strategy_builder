"""Execution settings (the per-strategy "Select execution" dialog) and the
price arithmetic shared by every broker adapter."""
from src.core.modules import dataclasses, math
from src.core import config

TICK = 0.05
VALID_DAYS = ("M", "T", "W", "Th", "F", "Sa", "Su")
STRATEGY_KEYS = frozenset({
    "mode", "qty_multiplier", "trade_monitoring", "monitoring_frequency_sec", "strategy_execution_time",
    "order_timeout_sec", "exit_fallback_market", "execution_days_mode", "execution_days", "execution_dte",
    "squareoff_on_entry_error", "max_daily_loss", "paper_slippage_pct", "legs",
})
LEG_KEYS = frozenset({
    "product", "tgt_sl_ref_price", "delay_entry_sec", "entry_order_type", "exit_order_type",
    "entry_buffer_type", "exit_buffer_type", "entry_trigger_buffer", "exit_trigger_buffer",
    "entry_limit_buffer", "exit_limit_buffer", "sl_order_at_broker", "trail_monitoring", "trail_frequency_sec",
    "entry_convert_to_market_sec", "exit_convert_to_market_sec",
})


def _clean(raw: dict | None) -> dict:
    return {k: v for k, v in (raw or {}).items() if v is not None and v != ""}


def _enum(raw: dict, key: str, default: str, allowed: tuple, upper: bool = True) -> str:
    v = str(raw.get(key, default))
    v = v.upper() if upper else v.lower()
    if v not in allowed:
        raise ValueError(f"{key} must be one of {list(allowed)}")
    return v


@dataclasses.dataclass(slots=True)
class LegExecution:
    product: str = "NRML"                    # NRML | MIS
    tgt_sl_ref_price: str = "TRADED"         # TRADED | TRIGGER
    delay_entry_sec: int = 0
    entry_order_type: str = "LIMIT"          # LIMIT | MPP
    exit_order_type: str = "LIMIT"
    entry_buffer_type: str = "POINTS"        # POINTS | PERCENT
    exit_buffer_type: str = "POINTS"
    entry_trigger_buffer: float = 0.0
    entry_limit_buffer: float = 3.0
    exit_trigger_buffer: float = 0.0
    exit_limit_buffer: float = 3.0
    sl_order_at_broker: bool = True
    trail_monitoring: str = "CONTINUOUS"     # CONTINUOUS | DELAYED
    trail_frequency_sec: int = 5
    entry_convert_to_market_sec: int = 0     # 0 = off; 1-20
    exit_convert_to_market_sec: int = 0      # 0 = off; 1-40

    @classmethod
    def from_dict(cls, raw: dict | None, label: str = "leg") -> "LegExecution":
        raw = _clean(raw)
        bad = [k for k in raw if k not in LEG_KEYS]
        if bad:
            where = "settings" if any(k in STRATEGY_KEYS for k in bad) else "nowhere"
            raise ValueError(f"{label}: {bad} not allowed here (strategy-level keys belong in settings)" if where == "settings"
                             else f"{label}: unknown keys {bad}")
        l = cls()
        l.product = _enum(raw, "product", l.product, ("NRML", "MIS"))
        l.tgt_sl_ref_price = _enum(raw, "tgt_sl_ref_price", l.tgt_sl_ref_price, ("TRADED", "TRIGGER"))
        l.delay_entry_sec = max(0, int(raw.get("delay_entry_sec", 0) or 0))
        l.entry_order_type = _enum(raw, "entry_order_type", l.entry_order_type, ("LIMIT", "MPP"))
        l.exit_order_type = _enum(raw, "exit_order_type", l.exit_order_type, ("LIMIT", "MPP"))
        for name in ("entry_buffer_type", "exit_buffer_type"):
            v = str(raw.get(name, "POINTS")).upper()
            v = {"%": "PERCENT", "PTS": "POINTS"}.get(v, v)
            if v not in ("POINTS", "PERCENT"):
                raise ValueError(f"{label}: {name} must be POINTS or PERCENT")
            setattr(l, name, v)
        l.entry_trigger_buffer = abs(float(raw.get("entry_trigger_buffer", 0) or 0))
        l.exit_trigger_buffer = abs(float(raw.get("exit_trigger_buffer", 0) or 0))
        l.entry_limit_buffer = float(raw.get("entry_limit_buffer", l.entry_limit_buffer) or 0)
        l.exit_limit_buffer = float(raw.get("exit_limit_buffer", l.exit_limit_buffer) or 0)
        l.sl_order_at_broker = bool(raw.get("sl_order_at_broker", l.sl_order_at_broker))
        l.trail_monitoring = _enum(raw, "trail_monitoring", l.trail_monitoring, ("CONTINUOUS", "DELAYED"))
        l.trail_frequency_sec = max(0, int(raw.get("trail_frequency_sec", l.trail_frequency_sec) or 0))
        l.entry_convert_to_market_sec = min(20, max(0, int(raw.get("entry_convert_to_market_sec", 0) or 0)))
        l.exit_convert_to_market_sec = min(40, max(0, int(raw.get("exit_convert_to_market_sec", 0) or 0)))
        return l

    def buffer_kind(self, which: str) -> str:
        return self.entry_buffer_type if which == "entry" else self.exit_buffer_type

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


@dataclasses.dataclass(slots=True)
class ExecutionSettings:
    mode: str = "paper"                      # paper | live
    qty_multiplier: int = 1
    trade_monitoring: str = "LTP"            # LTP | CANDLE_CLOSE
    monitoring_frequency_sec: int = 0        # LTP mode: 0 = every tick, N = at most one check per N s
    strategy_execution_time: str | None = None
    order_timeout_sec: int = config.LIVE_ORDER_TIMEOUT_SECONDS
    exit_fallback_market: bool = True
    execution_days_mode: str = "weekdays"    # weekdays | dte
    execution_days: tuple = ("M", "T", "W", "Th", "F")
    execution_dte: tuple = ()
    squareoff_on_entry_error: bool = True
    max_daily_loss: float | None = None
    paper_slippage_pct: float = 0.0
    legs: dict = dataclasses.field(default_factory=dict)   # {"1": LegExecution, ...}

    @classmethod
    def from_dict(cls, raw: dict | None) -> "ExecutionSettings":
        raw = _clean(raw)
        bad = [k for k in raw if k not in STRATEGY_KEYS]
        if bad:
            leg_keys = [k for k in bad if k in LEG_KEYS]
            if leg_keys:
                raise ValueError(f"{leg_keys} are leg-level settings: put them under settings.legs[\"<leg number>\"]")
            raise ValueError(f"unknown settings {bad}")
        s = cls()
        s.mode = _enum(raw, "mode", s.mode, ("paper", "live"), upper=False)
        s.qty_multiplier = max(1, int(raw.get("qty_multiplier", s.qty_multiplier) or 1))
        tm = str(raw.get("trade_monitoring", s.trade_monitoring)).upper().replace(" ", "_")
        tm = {"ON_LTP": "LTP", "ON_CANDLE_CLOSE": "CANDLE_CLOSE", "CANDLE": "CANDLE_CLOSE"}.get(tm, tm)
        if tm not in ("LTP", "CANDLE_CLOSE"):
            raise ValueError("trade_monitoring must be LTP or CANDLE_CLOSE")
        s.trade_monitoring = tm
        s.monitoring_frequency_sec = max(0, int(raw.get("monitoring_frequency_sec", 0) or 0))
        set_time = raw.get("strategy_execution_time")
        s.strategy_execution_time = str(set_time).strip() if set_time else None
        s.order_timeout_sec = max(5, int(raw.get("order_timeout_sec", s.order_timeout_sec) or s.order_timeout_sec))
        s.exit_fallback_market = bool(raw.get("exit_fallback_market", s.exit_fallback_market))
        s.execution_days_mode = _enum(raw, "execution_days_mode", s.execution_days_mode, ("weekdays", "dte"), upper=False)
        days = raw.get("execution_days")
        if days:
            days = tuple(str(d) for d in days)
            bad = [d for d in days if d not in VALID_DAYS]
            if bad:
                raise ValueError(f"execution_days must use {VALID_DAYS}, got {bad}")
            s.execution_days = days
        try:
            s.execution_dte = tuple(int(d) for d in (raw.get("execution_dte") or ()))
        except (TypeError, ValueError):
            raise ValueError("execution_dte must be a list of integers")
        if s.execution_days_mode == "dte" and not s.execution_dte:
            raise ValueError("execution_days_mode 'dte' needs execution_dte, e.g. [0, 1]")
        s.squareoff_on_entry_error = bool(raw.get("squareoff_on_entry_error", s.squareoff_on_entry_error))
        mdl = raw.get("max_daily_loss")
        s.max_daily_loss = abs(float(mdl)) if mdl not in (None, "", 0, "0") else None
        s.paper_slippage_pct = float(raw.get("paper_slippage_pct", 0) or 0)
        legs = raw.get("legs") or {}
        if not isinstance(legs, dict):
            raise ValueError("legs must be an object keyed by leg number")
        s.legs = {}
        for key, block in legs.items():
            if not isinstance(block, dict):
                raise ValueError(f"legs[{key}] must be an object")
            s.legs[str(int(key))] = LegExecution.from_dict(block, f"legs[{key}]")
        return s

    def for_leg(self, leg_number: int) -> LegExecution:
        return self.legs.get(str(leg_number)) or LegExecution()

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["execution_days"] = list(self.execution_days)
        d["execution_dte"] = list(self.execution_dte)
        d["legs"] = {k: v.to_dict() for k, v in self.legs.items()}
        return d



def round_tick(price: float, tick: float = TICK) -> float:
    return round(math.floor(price / tick + 0.5) * tick, 2)


def limit_price(ltp: float, side: str, buffer: float, buffer_type: str, tick: float = TICK) -> float:
    """Marketable limit: a BUY a little above LTP, a SELL a little below, so
    it fills immediately at the touch but never worse than LTP +/- buffer."""
    pts = ltp * buffer / 100.0 if buffer_type == "PERCENT" else buffer
    raw = ltp + pts if side == "BUY" else ltp - pts
    return max(round_tick(raw, tick), tick)


MPP_SLABS = ((10, 5.0), (100, 3.0), (500, 2.0), (float("inf"), 1.0))   # option premium upper bound -> protection %


def mpp_pct(price: float) -> float:
    for upper, pct in MPP_SLABS:
        if price < upper:
            return pct
    return MPP_SLABS[-1][1]


def mpp_price(ltp: float, side: str, bid: float | None = None, ask: float | None = None, tick: float = TICK) -> float:
    """AlgoTest's Market Price Protection: BUY limit = bid + slab %, SELL limit = ask - slab %
    (the LTP stands in when the feed has no bid / ask)."""
    base = (bid if side == "BUY" else ask) or ltp
    pct = mpp_pct(base)
    raw = base * (1 + pct / 100.0) if side == "BUY" else base * (1 - pct / 100.0)
    return max(round_tick(raw, tick), tick)


def order_limit_price(ltp: float, side: str, order_type: str, buffer: float, buffer_type: str,
                      tick: float = TICK, bid: float | None = None, ask: float | None = None) -> float:
    """Every order goes to the broker as LIMIT: the user's buffer for LIMIT, MPP for MPP."""
    if order_type == "MPP":
        return mpp_price(ltp, side, bid, ask, tick)
    return limit_price(ltp, side, buffer, buffer_type, tick)


def stop_limit_prices(level: float, side: str, trigger_buffer: float, limit_buffer: float,
                      buffer_type: str, tick: float = TICK) -> tuple[float, float]:
    """(trigger, limit) of an SL-L order around `level`, AlgoTest's rule:
    BUY  -> trigger = level - trigger buffer, limit = level + limit buffer
    SELL -> trigger = level + trigger buffer, limit = level - limit buffer
    The order arms slightly before the level and may fill up to the limit."""
    pct = buffer_type == "PERCENT"
    trig = level * trigger_buffer / 100.0 if pct else trigger_buffer
    lim = level * limit_buffer / 100.0 if pct else limit_buffer
    if side == "BUY":
        trigger, limit = level - trig, level + lim
    else:
        trigger, limit = level + trig, level - lim
    return max(round_tick(trigger, tick), tick), max(round_tick(limit, tick), tick)


def apply_slippage(ltp: float, side: str, pct: float, tick: float = TICK) -> float:
    if not pct:
        return round_tick(ltp, tick)
    raw = ltp * (1 + pct / 100.0) if side == "BUY" else ltp * (1 - pct / 100.0)
    return max(round_tick(raw, tick), tick)