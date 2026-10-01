"""Clock helpers. Everything the live engine schedules is IST wall-clock,
expressed -- like the backtest engine -- as seconds since midnight."""
from src.core.modules import datetime, date, timedelta, ZoneInfo, time
from src.core import config

IST = ZoneInfo("Asia/Kolkata")

MARKET_OPEN_SECS = 9 * 3600 + 15 * 60      # 09:15:00
MARKET_CLOSE_SECS = 15 * 3600 + 30 * 60    # 15:30:00

_HOLIDAYS: set[date] = set()
for _raw in config.LIVE_HOLIDAYS:
    try:
        _HOLIDAYS.add(date.fromisoformat(_raw))
    except ValueError:
        pass


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def secs_now() -> float:
    n = now_ist()
    return n.hour * 3600 + n.minute * 60 + n.second + n.microsecond / 1e6


def parse_hms(raw) -> int:
    """'09:35:00' -> 34500. Accepts HH:MM too."""
    parts = str(raw).strip().split(":")
    h, m = int(parts[0]), int(parts[1])
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def hms(secs) -> str:
    secs = int(secs)
    return f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d}"


def at(day: date, secs: int) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(seconds=int(secs))


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in _HOLIDAYS


def next_trading_day(day: date) -> date:
    nxt = day + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def weekdays_before_expiry(expiry: date, k: int) -> date:
    """The trading day 'k days before expiry' counted in CALENDAR weekdays
    (Mon-Fri), the backtest engine's rule: a holiday inside the window is
    held through, not counted around; if the computed day itself is a
    holiday the next trading day (up to expiry) is used."""
    d = expiry
    steps = k
    while steps > 0:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            steps -= 1
    while d <= expiry and not is_trading_day(d):
        d += timedelta(days=1)
    return d


def weekday_code(day: date) -> str:
    return ("M", "T", "W", "Th", "F", "Sa", "Su")[day.weekday()]


def minute_label(ts: float) -> int:
    """Completion label (seconds since midnight, IST) of the 1-minute candle
    that contains epoch `ts`: a tick at 09:15:37 belongs to the candle
    labelled 09:16:00 -- the same convention as the backtest frame."""
    local = datetime.fromtimestamp(ts, IST)
    return (local.hour * 3600 + local.minute * 60) + 60


def sleep_until_secs(target_secs: float) -> float:
    """Seconds to sleep from now until `target_secs` today (0 if passed)."""
    return max(0.0, target_secs - secs_now())


def to_naive_ist(dt: datetime | None):
    """DB columns are naive TIMESTAMPs in IST."""
    if dt is None:
        return None
    return dt.astimezone(IST).replace(tzinfo=None)


def epoch_now() -> float:
    return time.time()
