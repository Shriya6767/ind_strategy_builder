"""Strike selection on live quotes, mirroring `BacktestEngine.select_strike`
so a strategy trades the same contract live that it traded in the backtest:

* based on points  -> ATM = spot rounded to the ladder step; ITM/OTM-n walk
                      n steps inward/outward (CE inward = lower strike)
* closest premium  -> the strike whose LTP is nearest premium_value
* premium range    -> a strike whose LTP lies in [lower, upper]; SELL takes
                      the richest, BUY the cheapest; none -> nearest to the band
* atm percentage   -> ATM +/- pct% of ATM, snapped to the nearest listed strike
"""
from src.core.modules import np

VALID_EXPIRY_TYPES = ("weekly", "next_weekly", "monthly", "next_monthly")


def map_option_type(raw) -> str:
    v = str(raw or "").strip().lower()
    if v in ("call", "ce", "c"):
        return "CE"
    if v in ("put", "pe", "p"):
        return "PE"
    raise ValueError(f"Unknown option_type {raw!r} (expected call/put).")


def normalize_expiry_type(raw) -> str:
    v = str(raw or "weekly").strip().lower().replace(" ", "_")
    if v.endswith("dte"):
        v = "weekly"
    if v not in VALID_EXPIRY_TYPES:
        raise ValueError(f"Unknown expiry_type {raw!r} (expected one of {VALID_EXPIRY_TYPES}).")
    return v


def ladder_atm(spot: float, step: int) -> int:
    return int(round(spot / step) * step)


def _nearest_listed(strikes: np.ndarray, target: float) -> int:
    idx = int(np.abs(strikes - target).argmin())
    return int(strikes[idx])


def _points_target(leg: dict, option_type: str, spot: float, step: int) -> tuple[int, str]:
    atm = ladder_atm(spot, step)
    raw = leg.get("atm_strike", 0)
    if raw in (0, "0", "ATM", "atm", None, ""):
        return atm, "ATM"
    label, _, offset_str = str(raw).partition("-")
    label = label.strip().upper()
    offset = int(offset_str) if offset_str.strip().isdigit() else 1
    if label not in ("ITM", "OTM"):
        raise ValueError(f"unrecognized atm_strike '{raw}'")
    inward = -1 if option_type == "CE" else 1
    direction = inward if label == "ITM" else -inward
    return atm + direction * offset * step, label


def candidate_strikes(leg: dict, option_type: str, spot: float, strikes: np.ndarray, step: int) -> list[int]:
    """Strikes whose quotes are needed before `select_strike` can decide."""
    if strikes.size == 0:
        return []
    criteria = str(leg.get("strike_criteria") or "based on points").lower()
    if criteria == "based on points":
        target, _ = _points_target(leg, option_type, spot, step)
        return [int(target) if target in strikes else _nearest_listed(strikes, target)]
    if criteria == "atm percentage":
        pct = float(leg.get("multiplier_percentage") or 0)
        sign = 1 if str(leg.get("strike_sign", "+")).strip() == "+" else -1
        atm = ladder_atm(spot, step)
        return [_nearest_listed(strikes, atm + sign * atm * pct / 100.0)]
    # premium criteria: the whole listed chain is quoted (REST, batches of 50), as the backtest scans it
    return [int(s) for s in strikes]


def select_strike(leg: dict, option_type: str, spot: float, strikes: np.ndarray, step: int,
                  quotes: dict[int, float]) -> tuple[int, str]:
    """-> (strike, moneyness label). `quotes` = strike -> LTP for the
    candidates. Raises ValueError when nothing qualifies."""
    if strikes.size == 0:
        raise ValueError("no strikes listed for this expiry/type")
    criteria = str(leg.get("strike_criteria") or "based on points").lower()
    atm = ladder_atm(spot, step)

    if criteria == "based on points":
        target, label = _points_target(leg, option_type, spot, step)
        strike = int(target) if target in strikes else _nearest_listed(strikes, target)
        return strike, label

    if criteria == "atm percentage":
        pct = float(leg.get("multiplier_percentage") or 0)
        sign = leg.get("strike_sign")
        if sign not in ("+", "-"):
            raise ValueError("atm percentage needs strike_sign '+' or '-'")
        direction = 1 if sign == "+" else -1
        strike = _nearest_listed(strikes, atm + direction * atm * pct / 100.0)
        return strike, _label_for(strike, atm, option_type)

    priced = {int(k): float(v) for k, v in quotes.items() if v and v > 0}
    if not priced:
        raise ValueError("no quotes available for premium-based strike selection")

    if criteria == "closest premium":
        want = leg.get("premium_value")
        if want is None:
            raise ValueError("closest premium needs premium_value")
        strike = min(priced, key=lambda s: (abs(priced[s] - float(want)), abs(s - atm)))
        return strike, _label_for(strike, atm, option_type)

    if criteria == "premium range":
        lower, upper = leg.get("lower_range"), leg.get("upper_range")
        if lower is None or upper is None:
            raise ValueError("premium range needs lower_range and upper_range")
        lower, upper = sorted((float(lower), float(upper)))
        in_range = {s: p for s, p in priced.items() if lower <= p <= upper}
        if not in_range:
            strike = min(priced, key=lambda s: (abs(priced[s] - min(max(priced[s], lower), upper)), abs(s - atm)))
        elif str(leg.get("position_type", "BUY")).upper() == "SELL":
            strike = max(in_range, key=in_range.get)
        else:
            strike = min(in_range, key=in_range.get)
        return strike, _label_for(strike, atm, option_type)

    raise ValueError(f"unsupported strike_criteria '{criteria}'")


def _label_for(strike: int, atm: int, option_type: str) -> str:
    if strike == atm:
        return "ATM"
    itm = strike < atm if option_type == "CE" else strike > atm
    return "ITM" if itm else "OTM"
