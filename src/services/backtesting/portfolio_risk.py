"""Portfolio-level overall stop-loss / target.

A portfolio runs its strategies independently (each with its own legs,
stops, targets and re-entries). This adds one rule ABOVE them: at every
candle close the CUMULATIVE P&L of the portfolio's current cycle -- every
leg already closed in the cycle at its realised pnl, every running leg at
its mark -- is judged against the portfolio stop and target. The first time
either is hit the whole portfolio is flat: every leg still open in any
strategy is squared off at that candle's close, and nothing that would have
entered later that day (delayed fills, re-entries) is allowed in. There is
no portfolio-level re-entry.

A CYCLE is the span from the first entry while the portfolio is flat until
it is flat again (every leg has exited -- the strategies' exit times) or a
breach squares it off. It runs across days: a BTST or positional hold keeps
the cycle open overnight, so on day 2 the legs closed on day 1 (a straddle's
stopped-out CE, yesterday's intraday legs) still count, until the carried
leg exits. Once the portfolio is flat the next cycle starts from zero.

Per leg the accounting mirrors the engine's strategy-level overall SL
(BacktestEngine._find_overall_breach): contribution from the bar AFTER
entry at the candle close, priced x quantity (quantity_multiplier =
QUANTITY x lot_size), frozen at the realised pnl after exit. MTM thresholds
are absolute money; PERCENT is a share of the cycle's combined entry
premium (premium x quantity of every leg entered in the cycle so far).
Nothing can breach while the portfolio is flat.

The pass runs in the parent AFTER the workers finish and only rewrites
executed legs (exit fields + pnl), so the per-strategy simulations are
untouched; it prices legs from the same resident frame the workers used.
"""
from src.core.modules import pd, np
from src.core.constant import QUANTITY
from src.core.logger import get_logger

logger = get_logger(__name__)

RISK_TYPES = ("MTM", "PERCENT")
RISK_FIELDS = (
    "is_portfolio_sl", "portfolio_sl_type", "portfolio_sl_mtm_value", "portfolio_sl_percent_value",
    "is_portfolio_target", "portfolio_target_type", "portfolio_target_mtm_value", "portfolio_target_percent_value",
)
_DT_FMT = "%Y-%m-%d %H:%M:%S"


def _num(value, label: str):
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a number.")
    if out < 0:
        raise ValueError(f"{label} cannot be negative.")
    return out


def normalize_portfolio_risk(request: dict) -> dict:
    """The eight portfolio SL/target fields in canonical form, validated.
    Shared by save-portfolio (persist) and run-portfolio-backtest (apply)
    so both accept exactly the same shape. A disabled side keeps whatever
    type/values were sent (the UI round-trips them) but is never applied."""
    out = {}
    for side in ("sl", "target"):
        enabled = bool(request.get(f"is_portfolio_{side}"))
        rtype = request.get(f"portfolio_{side}_type")
        rtype = str(rtype).strip().upper() if rtype not in (None, "") else None
        mtm = _num(request.get(f"portfolio_{side}_mtm_value"), f"portfolio_{side}_mtm_value")
        pct = _num(request.get(f"portfolio_{side}_percent_value"), f"portfolio_{side}_percent_value")
        if enabled:
            if rtype not in RISK_TYPES:
                raise ValueError(f"portfolio_{side}_type must be one of {list(RISK_TYPES)} when is_portfolio_{side} is true.")
            if rtype == "MTM" and not mtm:
                raise ValueError(f"portfolio_{side}_mtm_value must be greater than 0 for type MTM.")
            if rtype == "PERCENT" and not pct:
                raise ValueError(f"portfolio_{side}_percent_value must be greater than 0 for type PERCENT.")
        out[f"is_portfolio_{side}"] = enabled
        out[f"portfolio_{side}_type"] = rtype
        out[f"portfolio_{side}_mtm_value"] = mtm
        out[f"portfolio_{side}_percent_value"] = pct
    return out


class PortfolioRisk:
    def __init__(self, settings: dict):
        self.has_sl = settings["is_portfolio_sl"]
        self.has_target = settings["is_portfolio_target"]
        self.sl_type = settings["portfolio_sl_type"]
        self.target_type = settings["portfolio_target_type"]
        self.sl_value = settings["portfolio_sl_mtm_value"] if self.sl_type == "MTM" else settings["portfolio_sl_percent_value"]
        self.target_value = (settings["portfolio_target_mtm_value"] if self.target_type == "MTM"
                             else settings["portfolio_target_percent_value"])
        self._tz = None                      # frame's datetime tz, set in apply()

    @property
    def enabled(self) -> bool:
        return self.has_sl or self.has_target

    def apply(self, reduced_by_id: dict, df: pd.DataFrame) -> list[dict]:
        """Mutates the executed legs in `reduced_by_id` (strategy_id -> day
        blocks) in place and returns one record per portfolio exit:
        {trade_date, exit_datetime, exit_reason, combined_pnl, legs_squared_off,
         legs_cancelled}. `df` is the portfolio's window of the resident
        frame (time-sorted, derived columns present)."""
        if not self.enabled:
            return []

        legs_by_day = self._legs_by_day(reduced_by_id)
        if not legs_by_day:
            return []

        # The frame's datetime_utc is tz-aware (the engine labels IST
        # wall-clock as UTC); sanitized leg timestamps are naive strings.
        # Every comparison below re-attaches the frame's tz via _ts.
        times = df["datetime_utc"]
        self._tz = times.dt.tz
        exits = []
        cancelled_ids = set()        # legs removed on an earlier day never count again
        carry_pnl = carry_capital = 0.0   # the open cycle's realised pnl / closed premium carried overnight
        for day in sorted(legs_by_day):
            entries = [e for e in legs_by_day[day] if id(e[2]) not in cancelled_ids]
            d0 = self._ts(day)
            d1 = d0 + pd.Timedelta(days=1)
            lo, hi = int(times.searchsorted(d0)), int(times.searchsorted(d1))
            # legs open at some point TODAY: a leg the portfolio already closed
            # on an earlier day (its original exit day may still be today) is
            # not in today's position -- its pnl travels in the carry instead.
            legs = [e[2] for e in entries
                    if not e[2].get("exit_datetime") or self._ts(e[2]["exit_datetime"]) >= d0]
            if not legs:
                carry_pnl = carry_capital = 0.0          # nothing in position: flat
                continue
            if lo >= hi:
                continue          # no session (weekend/holiday inside a hold): the carry waits for the next bars
            day_rows = df.iloc[lo:hi]
            series = self._day_series(day_rows, legs, carry_pnl, carry_capital)
            if series is None:
                continue                                  # no prices today for these contracts
            index, mtm, capital, last_flat, pivot = series

            breach = self._breach(mtm, capital)
            if breach is not None:
                idx, reason = breach
                breach_dt = pd.Timestamp(index[idx])
                prices = pivot.loc[breach_dt]
                spot_rows = day_rows.loc[day_rows["datetime_utc"] == breach_dt, "underlying_price"]
                spot = round(float(spot_rows.iloc[0]), 2) if not spot_rows.empty else None
                squared, cancelled_now = self._square_off(entries, day, breach_dt, reason, prices, spot)
                cancelled_ids.update(cancelled_now)
                exits.append({
                    "trade_date": day,
                    "exit_datetime": breach_dt.strftime(_DT_FMT),
                    "exit_reason": reason,
                    "combined_pnl": round(float(mtm[idx]), 2),
                    "legs_squared_off": squared,
                    "legs_cancelled": len(cancelled_now),
                })
                logger.info(f"Portfolio {reason} on {day} at {breach_dt.strftime('%H:%M')}: "
                            f"MTM {mtm[idx]:,.0f}, {squared} leg(s) squared off, {len(cancelled_now)} cancelled")
                carry_pnl = carry_capital = 0.0          # squared off: flat, next cycle starts at zero
                continue

            carry_pnl, carry_capital = self._carry_forward(legs, d1, last_flat.iloc[-1], carry_pnl, carry_capital)

        # drop day blocks emptied by cancellations
        for sid, blocks in reduced_by_id.items():
            reduced_by_id[sid] = [b for b in blocks if b["legs"]]
        return exits

    @staticmethod
    def _legs_by_day(reduced_by_id: dict) -> dict:
        """day -> [(strategy_id, day_block, leg)] for every executed leg, on
        its entry day and, when different (BTST / positional), on its exit
        day and every day in between."""
        by_day = {}
        for sid, blocks in reduced_by_id.items():
            for block in blocks:
                for leg in block["legs"]:
                    if leg.get("entry_price") is None or not leg.get("entry_datetime"):
                        continue
                    first = pd.Timestamp(str(leg["entry_datetime"])[:10])
                    last = pd.Timestamp(str(leg["exit_datetime"])[:10]) if leg.get("exit_datetime") else first
                    for day in pd.date_range(first, last, freq="D"):
                        by_day.setdefault(day.strftime("%Y-%m-%d"), []).append((sid, block, leg))
        return by_day

    def _ts(self, value) -> pd.Timestamp:
        """A leg's sanitized (naive, IST wall-clock) timestamp as a Timestamp
        comparable with the frame's tz-aware datetime_utc."""
        ts = pd.Timestamp(value)
        if self._tz is not None and ts.tzinfo is None:
            ts = ts.tz_localize(self._tz)
        return ts

    def _day_series(self, day_rows: pd.DataFrame, legs: list, carry_pnl: float, carry_capital: float):
        """Per candle of the day: cumulative cycle MTM, cycle capital, the
        last flat candle so far (NaT until the first), and the price pivot.

        One vectorised pass. A candle is FLAT when no leg is in position
        (entry <= t <= exit, the exit candle still counting so the cycle's
        final pnl is judged on it). Everything that belongs to a cycle ended
        by a flat candle -- the overnight carry and the legs that exited
        before it -- drops out of the MTM from that candle on.
        """
        tickers = {leg["ticker"] for leg in legs}
        sub = day_rows.loc[day_rows["ticker"].isin(tickers)]
        if sub.empty:
            return None
        # observed=True: ticker is categorical -- without it the pivot would
        # grow a column for every ticker in the whole loaded range.
        pivot = (sub.pivot_table(index="datetime_utc", columns="ticker", values="close",
                                 aggfunc="last", observed=True)
                    .sort_index().ffill())
        index = pivot.index
        n = len(index)
        far = index[-1] + pd.Timedelta(days=1)

        parts = []                                   # (contribution, entry_dt, exit_dt, premium)
        open_count = np.zeros(n, dtype=int)
        for leg in legs:
            ticker = leg["ticker"]
            if ticker not in pivot.columns:
                continue
            entry_dt = self._ts(leg["entry_datetime"])
            exit_dt = self._ts(leg["exit_datetime"]) if leg.get("exit_datetime") else far
            direction = 1 if leg["position"] == "BUY" else -1
            qty = float(leg.get("quantity_multiplier") or QUANTITY)
            entry_price = float(leg["entry_price"])

            price = pivot[ticker]
            if leg.get("exit_price") is not None:
                # Realised from the exit candle ON: a leg filled at its stop or
                # target inside that candle is worth its fill, not the candle's
                # close (an ATM call once showed +2,248 at 09:21 on a trade that
                # had closed at +200 one minute after entry).
                price = price.where(index < exit_dt, float(leg["exit_price"]))
            contribution = (direction * (price - entry_price) * qty).where(index > entry_dt, 0.0)
            parts.append((contribution.to_numpy(), entry_dt, exit_dt, entry_price * qty))
            open_count += np.asarray((index >= entry_dt) & (index <= exit_dt), dtype=int)

        flat = open_count == 0
        last_flat = pd.Series(index, index=index).where(flat).ffill()
        carry_active = last_flat.isna().to_numpy()          # before the first flat candle today

        mtm = np.where(carry_active, carry_pnl, 0.0)
        capital = np.where(carry_active, carry_capital, 0.0)
        for contribution, entry_dt, exit_dt, premium in parts:
            alive = ~(last_flat >= exit_dt).to_numpy()      # NaT compares False: still in the cycle
            mtm = mtm + contribution * alive
            capital = capital + premium * (np.asarray(index >= entry_dt) & alive)
        return index, mtm, capital, last_flat, pivot

    def _breach(self, mtm: np.ndarray, capital: np.ndarray):
        valid = ~np.isnan(mtm)
        sl = self._threshold(self.sl_type, self.sl_value, capital) if self.has_sl else None
        target = self._threshold(self.target_type, self.target_value, capital) if self.has_target else None
        sl_hit = (mtm <= -sl) if sl is not None else np.zeros(len(mtm), dtype=bool)
        target_hit = (mtm >= target) if target is not None else np.zeros(len(mtm), dtype=bool)
        hit = (sl_hit | target_hit) & (capital > 0) & valid
        if not hit.any():
            return None
        idx = int(hit.argmax())
        return idx, ("PORTFOLIO_STOPLOSS" if sl_hit[idx] else "PORTFOLIO_TARGET")

    @staticmethod
    def _threshold(rtype: str, value: float, capital: np.ndarray):
        if rtype == "PERCENT":
            return np.abs(capital) * abs(value) / 100.0
        return np.full(len(capital), abs(value), dtype=float)

    def _carry_forward(self, legs: list, d1, last_flat_end, carry_pnl: float, carry_capital: float):
        """What tomorrow inherits when no breach fired today: nothing if the
        portfolio is flat at the close; otherwise the open cycle's realised
        pnl and the premium of its CLOSED legs (an open leg's premium is
        counted tomorrow from its own entry). The cycle is everything since
        the day's last flat candle, plus today's carry when there was none."""
        held = [leg for leg in legs
                if not leg.get("exit_datetime") or self._ts(leg["exit_datetime"]) >= d1]
        if not held:
            return 0.0, 0.0
        no_flat_today = pd.isna(last_flat_end)
        pnl = carry_pnl if no_flat_today else 0.0
        capital = carry_capital if no_flat_today else 0.0
        for leg in legs:
            if not leg.get("exit_datetime") or leg.get("exit_price") is None:
                continue
            exit_dt = self._ts(leg["exit_datetime"])
            if exit_dt >= d1 or (not no_flat_today and exit_dt <= last_flat_end):
                continue                                  # still open, or a previous cycle's leg
            direction = 1 if leg["position"] == "BUY" else -1
            qty = float(leg.get("quantity_multiplier") or QUANTITY)
            pnl += (float(leg["exit_price"]) - float(leg["entry_price"])) * qty * direction
            capital += float(leg["entry_price"]) * qty
        return pnl, capital

    def _square_off(self, entries: list, day: str, breach_dt, reason: str, prices, spot) -> tuple[int, set]:
        """Closes every leg open at the breach and removes every leg that
        would only have entered after it. Returns (legs squared off, ids of
        the legs removed) -- the caller keeps those ids out of later days."""
        squared, cancelled = 0, set()
        for sid, block, leg in entries:
            entry_dt = self._ts(leg["entry_datetime"])
            exit_dt = self._ts(leg["exit_datetime"]) if leg.get("exit_datetime") else None
            if entry_dt > breach_dt and str(leg["entry_datetime"])[:10] == day:
                # would only have entered after the portfolio went flat
                block["legs"] = [l for l in block["legs"] if l is not leg]
                cancelled.add(id(leg))
                continue
            if exit_dt is not None and exit_dt <= breach_dt:
                continue                                           # already closed, realised pnl stands
            ticker = leg["ticker"]
            if ticker not in prices.index or pd.isna(prices[ticker]):
                continue
            exit_price = round(float(prices[ticker]), 2)
            direction = 1 if leg["position"] == "BUY" else -1
            qty = float(leg.get("quantity_multiplier") or QUANTITY)
            leg["exit_datetime"] = breach_dt.strftime(_DT_FMT)
            leg["exit_price"] = exit_price
            leg["underlying_exit_price"] = spot
            leg["exit_reason"] = reason
            leg["status"] = "EXIT_DONE"
            leg["pnl"] = round((exit_price - float(leg["entry_price"])) * qty * direction, 2)
            if "is_held_overnight" in leg:
                leg["is_held_overnight"] = str(leg["entry_datetime"])[:10] != day
            squared += 1
        return squared, cancelled