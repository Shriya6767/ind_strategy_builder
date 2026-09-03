from src.core.modules import pd, np, datetime, date, timedelta, dt_time
from src.core.constant import REENTRY_MODES, OVERALL_REENTRY_MODES, QUANTITY, COST_BUFFER_PCT
from src.core.logger import get_logger
from src.services.backtesting.report_builder import BacktestReportBuilder
from enum import Enum

logger = get_logger(__name__)

def _sanitize(obj):
    """Recursively replaces NaN/NaT/np.nan floats with None and formats
    every datetime as a plain string so the result is always
    JSON-serializable, with no ISO 'T' or timezone suffix in the response:
      datetimes  -> 'YYYY-MM-DD HH:MM:SS'  (IST wall-clock)
      dates and midnight-only datetimes (expiration_date) -> 'YYYY-MM-DD'
    """
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, float) and np.isnan(obj):
        return None
    if isinstance(obj, np.floating):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (pd.Timestamp, datetime)):
        if pd.isna(obj):
            return None
        if obj.tzinfo is not None:
            # tz-aware timestamps hold IST wall-clock -- drop the tz, keep the clock
            obj = obj.tz_localize(None) if isinstance(obj, pd.Timestamp) else obj.replace(tzinfo=None)
        if obj.hour == 0 and obj.minute == 0 and obj.second == 0:
            return obj.strftime("%Y-%m-%d")
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, date):
        return obj.isoformat()
    return obj
    

class BacktestEngine:
    # Sensex session is 09:15-15:30 IST. Bars are labeled by their
    # COMPLETION minute (the 09:15:00-09:15:59 candle is stamped 09:16:00),
    # so labels run 09:16:00-15:30:00. BTST Day-1 monitoring runs until the
    # close; Day-2 carryover handling starts from the first completed bar.
    _DEFAULT_DAY1_MARKET_CLOSE = "15:30:00"
    _DEFAULT_DAY2_MARKET_OPEN = "09:16:00"

    # How old a last traded price may be and still be matched by the
    # premium-based strike criteria. See _recently_priced.
    PREMIUM_MAX_PRICE_AGE_MINUTES = 15

    # Sensex options quote in 5-paisa ticks.
    TICK_SIZE = 0.05

    # Strike ladder interval used when the chain is too thin to infer one.
    _FALLBACK_STRIKE_STEP = 100

    def __init__(self, df: pd.DataFrame, request: dict):
        self.df = df.copy()
        self.strategy = request["strategy"]
        self.legs = request["legs"]
        self.trade_results = []

        self.strategy_type = self.strategy.get("strategy_type", "intraday").lower()

        if self.strategy_type == "btst":
            self.held_from_previous_day = {}  # Positions held overnight

        self.entry_time = self._parse_entry_time()
        self.exit_time = self._parse_exit_time()

        # BTST exits on Day 2, so its exit_time may be earlier in the clock
        # than entry_time. For INTRADAY that's a contradiction -- fail loudly
        # instead of silently producing zero trades.
        if self.strategy_type != "btst" and self.exit_time <= self.entry_time:
            raise ValueError(
                f"INTRADAY strategy: exit_time ({self.exit_time}) must be after "
                f"entry_time ({self.entry_time})."
            )

        if self.strategy_type == "btst":
            self.day1_market_close = self._parse_day1_market_close()
            self.day2_market_open = self._parse_day2_market_open()

        # Entry-day scans (momentum / range-breakout fills, cost re-entries)
        # must run to the Day-1 session close for BTST: there exit_time is
        # the DAY-2 cutoff and can be earlier in the clock than entry_time,
        # which would make a `trade_time <= exit_time` scan permanently empty.
        self.entry_day_cutoff = self.day1_market_close if self.strategy_type == "btst" else self.exit_time

        # Precompute leg metadata
        self.legs_meta = self._prepare_legs_meta()
        self._lot_size_by_leg = {
            leg["leg_number"]: self._effective_leg_meta(leg).get("lot_size", 1) for leg in self.legs_meta
        }

        # Runtime caches
        self._range_cache = {}
        self._ticker_series_cache = {}
        self._underlying_series_cache = None
        self._snapshot_cache = {}  # snapshot minute -> as-of chain, per trading day
        self._trail_skip_warned = set()  # leg numbers already warned about ignored trailing
        self._next_trade_date = {}       # trading date -> the next one
        self._reentry_frame_cache = {}   # date -> day rows + next day up to exit_time
        self._day_expiry_map = {}  # expiry_type label -> expiry date, rebuilt per trading day
        self._snapshot_day_df = None     # frame the current chain snapshot was built from


    def _parse_entry_time(self) -> dt_time:
        """Strategy-level entry time + entry_delay, computed once.
        All legs enter simultaneously at this time."""
        entry_dt = datetime.strptime(self.strategy["entry_time"], "%H:%M:%S")
        entry_dt += timedelta(minutes=self.strategy.get("entry_delay", 0) or 0)
        return entry_dt.time()


    def _parse_exit_time(self) -> dt_time:
        """Strategy-level squareoff/exit time + exit_delay, computed once.
        For BTST, this is the Day-2 cutoff. For INTRADAY, same-day exit cutoff."""
        exit_dt = datetime.strptime(self.strategy["exit_time"], "%H:%M:%S")
        exit_dt += timedelta(minutes=self.strategy.get("exit_delay", 0) or 0)
        return exit_dt.time()


    def _parse_day1_market_close(self) -> dt_time:
        raw = self._DEFAULT_DAY1_MARKET_CLOSE
        parsed = datetime.strptime(raw, "%H:%M:%S").time()
        return parsed
    
    
    def _parse_day2_market_open(self) -> dt_time:
        raw = self._DEFAULT_DAY2_MARKET_OPEN
        parsed = datetime.strptime(raw, "%H:%M:%S").time()
        return parsed


    def _prepare_legs_meta(self) -> list[dict]:
        """Attach precomputed, engine-ready fields (mapped option_type,
        normalized expiry_type) to each leg so execute_leg doesn't re-derive
        them every trading day. The expiry_type label is resolved to a
        concrete expiry date per trading day in _build_day_expiry_map."""
        legs_meta = []
        for index, leg in enumerate(self.legs, start=1):
            meta = dict(leg)
            meta["leg_number"] = index
            meta["expiry_type"] = self._normalize_expiry_type(leg)
            meta["option_type"] = self._map_option_type(leg)
            if leg.get("is_range_breakout"):
                meta["_range_end_parsed"] = (datetime.strptime(leg.get("range_end_time"), "%H:%M:%S")).time()

            sequential_leg_config = leg.get("sequential_leg")
            if sequential_leg_config:
                meta["_sequential_leg_meta"] = self._prepare_nested_leg_meta(meta, sequential_leg_config)

            legs_meta.append(meta)
        return legs_meta


    def _prepare_nested_leg_meta(self, parent_leg_meta: dict, leg_config: dict) -> dict:
        meta = dict(leg_config)
        meta["leg_number"] = parent_leg_meta["leg_number"]
        meta["expiry_type"] = self._normalize_expiry_type(leg_config)
        meta["option_type"] = self._map_option_type(leg_config)
        if leg_config.get("is_range_breakout"):
            meta["_range_end_parsed"] = datetime.strptime(leg_config.get("range_end_time"), "%H:%M:%S").time()
        return meta


    def _effective_leg_meta(self, leg_meta: dict) -> dict:
        return leg_meta.get("_sequential_leg_meta") or leg_meta


    VALID_EXPIRY_TYPES = ("weekly", "next_weekly", "monthly", "next_monthly")

    @staticmethod
    def _map_option_type(leg: dict) -> str:
        raw = str(leg.get("option_type", "")).strip().lower()
        if raw in ("call", "ce", "c"):
            return "CE"
        if raw in ("put", "pe", "p"):
            return "PE"
        raise ValueError(f"Unknown option_type {leg.get('option_type')!r} (expected call/put).")


    def _normalize_expiry_type(self, leg: dict) -> str:
        """'weekly' / 'next weekly' / 'monthly' / 'next monthly' (case and
        space insensitive) -> canonical label used by _build_day_expiry_map."""
        raw = str(leg.get("expiry_type", "weekly")).strip().lower().replace(" ", "_")
        if raw not in self.VALID_EXPIRY_TYPES:
            raise ValueError(
                f"Unknown expiry_type {leg.get('expiry_type')!r} "
                f"(expected one of: {', '.join(self.VALID_EXPIRY_TYPES)})."
            )
        return raw


    def _build_expiry_calendar(self):
        """Month -> last known expiry of that month, built ONCE from every
        expiry observed across the whole loaded date range. A single day's
        chain can't identify the monthly contract (early in a month the
        monthly isn't listed yet, and far-out quarterlies would be mistaken
        for it), but across the range the true last expiry of each month
        shows up. Near the end of the range later months may be incomplete
        -- load a range extending past the expiries you trade."""
        self._monthly_expiry_by_month = {}
        for expiry in sorted(pd.to_datetime(pd.unique(self.df["expiration_date"]))):
            self._monthly_expiry_by_month[(expiry.year, expiry.month)] = expiry


    def _build_day_expiry_map(self, trade_date, day_df: pd.DataFrame) -> dict:
        """Resolves the expiry_type labels to concrete expiry dates for ONE
        trading day:
          weekly       -> nearest expiry in that day's chain (today itself
                          on expiry day)
          next_weekly  -> second-nearest expiry in that day's chain
          monthly      -> last expiry of the current month (rolls to the
                          next month once it has passed), from the
                          range-wide calendar
          next_monthly -> last expiry of the following month
        BTST holds overnight, so same-day expiries are excluded there.
        A label missing from the map -- or a resolved contract not listed in
        that day's chain -- makes the leg report NO_CHAIN_DATA."""
        min_date = pd.Timestamp(trade_date)
        if self.strategy_type == "btst":
            min_date += pd.Timedelta(days=1)

        mapping = {}

        day_expiries = sorted(
            expiry for expiry in pd.to_datetime(pd.unique(day_df["expiration_date"]))
            if expiry >= min_date
        )
        if day_expiries:
            mapping["weekly"] = day_expiries[0]
            if len(day_expiries) > 1:
                mapping["next_weekly"] = day_expiries[1]

        monthlies = [
            last_expiry
            for _, last_expiry in sorted(self._monthly_expiry_by_month.items())
            if last_expiry >= min_date
        ]
        if monthlies:
            mapping["monthly"] = monthlies[0]
            if len(monthlies) > 1:
                mapping["next_monthly"] = monthlies[1]
        return mapping


    def _resolve_leg_expiry(self, leg_meta: dict):
        """The concrete expiry date this leg trades on the current day, or
        None when the label can't be resolved that day (e.g. 'next monthly'
        past the end of the loaded range, or nothing left to trade after
        BTST excludes same-day expiries)."""
        return self._day_expiry_map.get(leg_meta["expiry_type"])


    def _filter_leg_chain(self, chain_snapshot: pd.DataFrame, leg_meta: dict) -> pd.DataFrame:
        """All strikes of the leg's option type on the leg's resolved expiry
        for the current day. Empty result -> caller treats it as no chain
        (either the expiry label didn't resolve, or nothing on that expiry
        has traded at all yet today -- the snapshot carries every contract's
        last trade forward, so a merely quiet contract is still present)."""
        expiry = self._resolve_leg_expiry(leg_meta)
        if expiry is None:
            return chain_snapshot.iloc[0:0]
        return chain_snapshot[
            (chain_snapshot["option_type"] == leg_meta["option_type"])
            & (chain_snapshot["expiration_date"] == expiry)
        ]


    def _get_leg_meta_by_number(self, leg_number: int) -> dict:
        """BTST helper: retrieve leg metadata by leg_number."""
        for meta in self.legs_meta:
            if meta["leg_number"] == leg_number:
                return self._effective_leg_meta(meta)
        return None 


    def run(self):
        self.prepare_dataframe()
        self._build_expiry_calendar()
        self.process_days()
        self._dedupe_carryover_legs()
        if self.strategy_type == "btst":
            self._flush_unclosed_final_holds()
            self._regroup_btst_by_entry_date()
        logger.info("Backtest Completed.")
        return _sanitize({
            "trade_results": self.trade_results,
            **BacktestReportBuilder(
                self.trade_results,
                period=(self.strategy.get("start_date"), self.strategy.get("end_date")),
            ).build(),
        })
 

    def prepare_dataframe(self):
        self.df["datetime_utc"] = pd.to_datetime(self.df["datetime_utc"], utc=True)
        # self.df.sort_values("datetime_utc", inplace=True)
        self.df["trade_date"] = self.df["datetime_utc"].dt.date
        self.df["trade_time"] = self.df["datetime_utc"].dt.time


    def process_days(self):
        grouped = self.df.groupby("trade_date", sort=False)
        self._last_trade_date = self.df["trade_date"].max()
        # date -> the trading day after it, so a pending BTST re-entry can be
        # searched into the next session (see _reentry_search_frame).
        ordered_dates = sorted(self.df["trade_date"].unique())
        self._next_trade_date = dict(zip(ordered_dates, ordered_dates[1:]))
        logger.info(f"Total Trading Days: {len(grouped)}")
        for trade_date, day_df in grouped:
            self._day_expiry_map = self._build_day_expiry_map(trade_date, day_df)
            self._snapshot_cache = {}
            self._reentry_frame_cache = {}
            if self.strategy_type == "btst":
                self.process_day_btst(trade_date, day_df)
            else:
                self.process_day_intraday(trade_date, day_df)


    def _dedupe_carryover_legs(self):
        if self.strategy_type != "btst":
            return

        for day_block in self.trade_results:
            block_date = day_block["trade_date"]
            kept_legs = []
            for leg in day_block["legs"]:
                exit_dt = leg.get("exit_datetime")
                if exit_dt is not None:
                    exit_date = exit_dt.date() if hasattr(exit_dt, "date") else exit_dt
                    if exit_date != block_date:
                        # This leg's true close is recorded in a LATER
                        # block -- drop this earlier, now-stale appearance.
                        continue
                kept_legs.append(leg)
            day_block["legs"] = kept_legs


    def _regroup_btst_by_entry_date(self):
        """BTST day blocks come out of the day loop keyed by the day a
        position CLOSES (Day 2). The API/report convention is the opposite:
        a trade belongs to the day it was ENTERED. Re-bucket every leg by
        its entry date; overall_exits stay on the day they actually fired.
        Also strips the internal _trade_date bookkeeping key."""
        legs_by_date = {}
        exits_by_date = {}
        for day_block in self.trade_results:
            for leg in day_block["legs"]:
                leg.pop("_trade_date", None)
                # A re-entry that filled on Day 2 carries the date of the trade
                # it continues; everything else is bucketed by its own entry.
                chain_date = leg.pop("_chain_date", None)
                entry_dt = leg.get("entry_datetime")
                bucket = chain_date or (
                    entry_dt.date() if entry_dt is not None else day_block["trade_date"]
                )
                legs_by_date.setdefault(bucket, []).append(leg)
            for overall_exit in day_block.get("overall_exits", []):
                exit_dt = overall_exit.get("exit_datetime")
                bucket = exit_dt.date() if exit_dt is not None else day_block["trade_date"]
                exits_by_date.setdefault(bucket, []).append(overall_exit)

        rebuilt = []
        for trade_date in sorted(set(legs_by_date) | set(exits_by_date)):
            legs = legs_by_date.get(trade_date, [])
            block = {
                "trade_date": trade_date,
                "legs": legs
            }
            if trade_date in exits_by_date:
                block["overall_exits"] = exits_by_date[trade_date]
            rebuilt.append(block)
        self.trade_results = rebuilt


    def process_day_intraday(self, trade_date, day_df):
        entry_snapshot = self.find_entry_snapshot(day_df)
        if entry_snapshot is None or entry_snapshot.empty:
            logger.warning(f"No Entry Snapshot Found: {trade_date}")
            return
        self.execute_strategy(trade_date, day_df, entry_snapshot)


    def process_day_btst(self, trade_date, day_df):
        closed_today = []
        tickers_held_today = set()

        # ========== PHASE 1: Close any positions held overnight from yesterday ==========
        if self.held_from_previous_day:            
            for leg_key, leg_result in list(self.held_from_previous_day.items()):
                leg_meta = self._get_leg_meta_by_number(leg_result["leg"])
                if leg_meta:
                    # Use Phase-2 logic to close: Day-2 entry_time through exit_time
                    closed_leg = self._close_held_overnight_leg(leg_result, leg_meta, day_df)
                    
                    if closed_leg.get("status") == "EXIT_DONE":
                        lot_size = leg_meta.get("lot_size", 1)
                        direction = 1 if closed_leg["position"] == "BUY" else -1
                        closed_leg["quantity_multiplier"] = QUANTITY * lot_size
                        closed_leg["pnl"] = round(
                            (closed_leg["exit_price"] - closed_leg["entry_price"]) * QUANTITY * lot_size * direction, 2
                        )
                        closed_leg["is_reentry"] = False
                        closed_leg["reentry_mode"] = None
                        closed_leg["is_held_overnight"] = True
                        
                        del self.held_from_previous_day[leg_key]

                        # Closing the carryover ENDS that trade -- no leg-level
                        # re-entry chain follows it. The Day-2 close is the
                        # strategy's own square-off, and Phase 2 opens a fresh
                        # position minutes later; re-entering in between would
                        # re-open the very position just squared off and then
                        # double up on it. AlgoTest reports only the carryover
                        # and then the day's new entry.
                        closed_today.append(closed_leg)
                    else:
                        del self.held_from_previous_day[leg_key]

        # BTST needs a Day 2 to close on: no fresh position is opened on the
        # LAST day of the backtest window (AlgoTest convention) -- today only
        # closes carryovers.
        if trade_date == self._last_trade_date:
            if closed_today:
                self.trade_results.append({
                    "trade_date": trade_date,
                    "legs": closed_today
                })
            return

        # ========== PHASE 2: Find today's entry snapshot and enter new positions ==========
        entry_snapshot = self.find_entry_snapshot(day_df)
        if entry_snapshot is None or entry_snapshot.empty:
            logger.warning(f"[{trade_date}] No entry snapshot at/after entry_time")
            if closed_today:
                self.trade_results.append({
                    "trade_date": trade_date,
                    "legs": closed_today
                })
            return

        self.execute_strategy_btst(
            trade_date, day_df, entry_snapshot,
            skip_tickers=tickers_held_today,
            prior_legs=closed_today,
        )


    def find_entry_snapshot(self, day_df: pd.DataFrame):
        """Returns every option-chain row (all strikes/types) at the first
        trade_time >= entry_time. This is the full chain snapshot every
        leg picks its strike from, so all legs enter at the same instant.

        The upper bound keeps entries out of special sessions beyond the
        strategy's window (e.g. the evening Muhurat session on 2024-11-01,
        bars 18:01-19:00): INTRADAY entries must leave room to exit the
        same day (<= exit_time); BTST exits on Day 2, so its exit_time can
        be earlier in the clock than entry_time -- Day-1 entries are
        bounded by the session close instead."""
        upper_bound = self.day1_market_close if self.strategy_type == "btst" else self.exit_time
        candidates = day_df.loc[
            (day_df["trade_time"] >= self.entry_time) & (day_df["trade_time"] <= upper_bound),
            "datetime_utc",
        ]
        if candidates.empty:
            return None
        return self._asof_chain_snapshot(day_df, candidates.min())


    def execute_strategy(self, trade_date, day_df: pd.DataFrame, entry_snapshot: pd.DataFrame):
        """INTRADAY: Standard entry and exit logic."""
        self._range_cache = {}
        self._ticker_series_cache = {}
        self._underlying_series_cache = None

        result = {
            "trade_date": trade_date,
            "legs": []
        }

        for leg_meta in self.legs_meta:
            leg_result = self.execute_leg(leg_meta, entry_snapshot, day_df)
            if leg_result.get("status") == "ENTRY_DONE":
                effective_leg_meta = leg_result.pop("_effective_leg_meta", None) or leg_meta
                lot_size = effective_leg_meta.get("lot_size", 1)

                leg_result = self.evaluate_leg_exit(effective_leg_meta, leg_result, day_df)
                leg_result["quantity_multiplier"] = QUANTITY * lot_size
                if leg_result.get("status") == "EXIT_DONE":
                    leg_result["pnl"] = round((leg_result["exit_price"] - leg_result["entry_price"]) * QUANTITY * lot_size * (1 if effective_leg_meta["position_type"] == "BUY" else -1), 2)
                else:
                    # e.g. OPEN_NO_EXIT_SIGNAL -- no exit bar exists, so no pnl
                    leg_result["pnl"] = None
                leg_result["is_reentry"] = False
                leg_result["reentry_mode"] = None
                result["legs"].append(leg_result)

                # ---- Re-entry: after the FIRST entry/exit (untouched)
                result["legs"].extend(
                    self._run_reentry_loop(effective_leg_meta, leg_result, day_df, self.evaluate_leg_exit)
                )
            else:
                result["legs"].append(leg_result)

        # ---- Overall SL/Target/Re-entry: cycles across all legs after all
        result = self._apply_overall_risk_management(day_df, result)

        self.trade_results.append(result)


    def execute_strategy_btst(self, trade_date, day_df: pd.DataFrame, entry_snapshot: pd.DataFrame,
                               skip_tickers=None, prior_legs=None):
        self._range_cache = {}
        self._ticker_series_cache = {}
        self._underlying_series_cache = None
        
        skip_tickers = skip_tickers or set()
        prior_legs = prior_legs or []

        result = {
            "trade_date": trade_date,
            "legs": list(prior_legs)
        }

        for leg_meta in self.legs_meta:
            leg_result = self.execute_leg(leg_meta, entry_snapshot, day_df)
            if leg_result.get("status") == "ENTRY_DONE" and leg_result.get("ticker") in skip_tickers:
                logger.debug(
                    f"Leg {leg_meta['leg_number']} ({leg_result['ticker']}): skipping duplicate "
                    f"entry, already held/reopened today"
                )
                continue

            if leg_result.get("status") == "ENTRY_DONE":
                effective_leg_meta = leg_result.pop("_effective_leg_meta", None) or leg_meta
                lot_size = effective_leg_meta.get("lot_size", 1)

                # Day-1 monitoring (entry-day SL/TGT, or held overnight)
                leg_result = self._evaluate_leg_exit_btst_phase1(effective_leg_meta, leg_result, day_df)

                if leg_result.get("status") == "EXIT_DONE":
                    leg_result["quantity_multiplier"] = QUANTITY * lot_size
                    direction = 1 if effective_leg_meta["position_type"] == "BUY" else -1
                    leg_result["pnl"] = round((leg_result["exit_price"] - leg_result["entry_price"]) * QUANTITY * lot_size * direction, 2)
                    leg_result["is_reentry"] = False
                    leg_result["reentry_mode"] = None
                    leg_result["is_held_overnight"] = False
                    result["legs"].append(leg_result)

                    # Per-leg re-entry for Day-1 exits (SL/TGT only)
                    reentry_legs = self._run_reentry_loop(
                        effective_leg_meta, leg_result, day_df, self._evaluate_leg_exit_btst_phase1
                    )

                    for rl in reentry_legs:
                        rl["is_held_overnight"] = (rl.get("status") == "HELD_OVERNIGHT")
                        if rl.get("status") == "HELD_OVERNIGHT":
                            rl["_trade_date"] = rl["entry_datetime"].date()
                            rl_key = f"leg_{rl['leg']}_{rl['entry_datetime'].isoformat()}"
                            self.held_from_previous_day[rl_key] = rl
                        result["legs"].append(rl)

                elif leg_result.get("status") == "HELD_OVERNIGHT":
                    leg_result["is_reentry"] = False
                    leg_result["reentry_mode"] = None
                    leg_result["is_held_overnight"] = True
                    key = f"leg_{leg_result['leg']}_{leg_result['entry_datetime'].isoformat()}"
                    self.held_from_previous_day[key] = leg_result
                    result["legs"].append(leg_result)

            else:
                leg_result["is_reentry"] = False
                leg_result["reentry_mode"] = None
                result["legs"].append(leg_result)

        result = self._apply_overall_risk_management(day_df, result)

        if result["legs"]:
            self.trade_results.append(result)

   
    def execute_leg(self, leg_meta, entry_snapshot: pd.DataFrame, day_df: pd.DataFrame):
        """Execute entry logic for a single leg. Shared by INTRADAY and BTST."""
        
        sequential_leg_meta = leg_meta.get("_sequential_leg_meta")
        if sequential_leg_meta is not None:
            return self._execute_observation_leg(leg_meta, sequential_leg_meta, entry_snapshot, day_df)

        expiry = self._resolve_leg_expiry(leg_meta)
        if expiry is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: no '{leg_meta['expiry_type']}' expiry "
                f"available this trading day (resolvable: {list(self._day_expiry_map) or 'none'})"
            )
            return {
                "leg": leg_meta["leg_number"],
                "position": leg_meta["position_type"],
                "option": leg_meta["option_type"],
                "expiry_type": leg_meta["expiry_type"],
                "status": "NO_EXPIRY_AVAILABLE",
            }

        leg_chain = self._filter_leg_chain(entry_snapshot, leg_meta)
        if leg_chain.empty:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: expiry {expiry.date()} resolved but no chain "
                f"rows at the entry snapshot (option_type={leg_meta['option_type']}, "
                f"expiry_type={leg_meta['expiry_type']})"
            )
            return {
                "leg": leg_meta["leg_number"],
                "position": leg_meta["position_type"],
                "option": leg_meta["option_type"],
                "expiry_type": leg_meta["expiry_type"],
                "expiration_date": expiry,
                "status": "NO_CHAIN_DATA",
            }

        strike_row = self.select_strike(leg_meta, leg_chain)
        if strike_row is None:
            return {
                "leg": leg_meta["leg_number"],
                "position": leg_meta["position_type"],
                "option": leg_meta["option_type"],
                "status": "STRIKE_NOT_FOUND",
            }

        if leg_meta.get("is_simple_momentum"):
            fill_row = self._resolve_momentum_fill(leg_meta, day_df, strike_row)
            if fill_row is None:
                return {
                    "leg": leg_meta["leg_number"],
                    "position": leg_meta["position_type"],
                    "option": leg_meta["option_type"],
                    "ticker": strike_row["ticker"],
                    "strike": strike_row["strike"],
                    "moneyness": strike_row["moneyness"],
                    "status": "MOMENTUM_NOT_TRIGGERED",
                }
        elif leg_meta.get("is_range_breakout"):
            fill_row = self._resolve_range_breakout_fill(leg_meta, day_df, strike_row)
            if fill_row is None:
                return {
                    "leg": leg_meta["leg_number"],
                    "position": leg_meta["position_type"],
                    "option": leg_meta["option_type"],
                    "ticker": strike_row["ticker"],
                    "strike": strike_row["strike"],
                    "moneyness": strike_row["moneyness"],
                    "status": "RANGE_BREAKOUT_NOT_TRIGGERED",
                }
        else:
            fill_row = strike_row

        entry_result = self._build_entry_result(
            leg_meta, leg_meta["position_type"], strike_row, fill_row, is_reentry=False
        )
        entry_result["status"] = "ENTRY_DONE"
        return entry_result


    @staticmethod
    def _underlying_at(row):
        """Spot price on a bar row, for reporting entry/exit context."""
        value = row.get("underlying_price")
        return round(float(value), 2) if value is not None else None


    def _build_entry_result(self, leg_meta, position_type, strike_row, fill_row, is_reentry=False, reentry_mode=None):
        return {
            "leg": leg_meta["leg_number"],
            "ticker": strike_row["ticker"],
            "position": position_type,
            "option": leg_meta["option_type"],
            "moneyness": strike_row["moneyness"],
            "distance_from_underlying": (
                round(float(strike_row["distance_from_underlying"]), 2)
                if strike_row.get("distance_from_underlying") is not None else None
            ),
            "entry_datetime": fill_row["datetime_utc"],
            "entry_price": round(float(fill_row["close"]), 2),
            "underlying_entry_price": round(float(fill_row["underlying_price"]), 2) if "underlying_price" in fill_row else None,
            "entry_reason": "REENTRY" if is_reentry else "ENTRY",
            "strike": strike_row["strike"],
            "expiry_type": leg_meta["expiry_type"],
            "expiration_date": strike_row.get("expiration_date"),
        }


    # Observation Leg -> Sequential Leg (trigger hand-off)

    def _execute_observation_leg(self, observation_meta, sequential_meta, entry_snapshot: pd.DataFrame, day_df: pd.DataFrame):
        obs_expiry = self._resolve_leg_expiry(observation_meta)
        if obs_expiry is None:
            logger.warning(
                f"Leg {observation_meta['leg_number']}: no '{observation_meta['expiry_type']}' "
                f"expiry available for the observation leg this trading day "
                f"(resolvable: {list(self._day_expiry_map) or 'none'})"
            )
            return {
                "leg": observation_meta["leg_number"],
                "position": observation_meta["position_type"],
                "option": observation_meta["option_type"],
                "expiry_type": observation_meta["expiry_type"],
                "status": "NO_EXPIRY_AVAILABLE",
            }

        obs_chain = self._filter_leg_chain(entry_snapshot, observation_meta)
        if obs_chain.empty:
            logger.warning(
                f"Leg {observation_meta['leg_number']}: no chain rows for observation leg "
                f"(option_type={observation_meta['option_type']}, expiry_type={observation_meta['expiry_type']})"
            )
            return {
                "leg": observation_meta["leg_number"],
                "position": observation_meta["position_type"],
                "option": observation_meta["option_type"],
                "expiry_type": observation_meta["expiry_type"],
                "expiration_date": obs_expiry,
                "status": "NO_CHAIN_DATA",
            }

        obs_strike_row = self.select_strike(observation_meta, obs_chain)
        if obs_strike_row is None:
            return {
                "leg": observation_meta["leg_number"],
                "position": observation_meta["position_type"],
                "option": observation_meta["option_type"],
                "status": "STRIKE_NOT_FOUND",
            }

        trigger_row = self._resolve_observation_trigger(observation_meta, day_df, obs_strike_row)
        if trigger_row is None:
            return {
                "leg": observation_meta["leg_number"],
                "position": observation_meta["position_type"],
                "option": observation_meta["option_type"],
                "ticker": obs_strike_row["ticker"],
                "strike": obs_strike_row["strike"],
                "moneyness": obs_strike_row["moneyness"],
                "status": "OBSERVATION_NOT_TRIGGERED",
            }

        entry_result = self._enter_sequential_leg(sequential_meta, day_df, trigger_row["datetime_utc"])
        if entry_result is None:
            return {
                "leg": sequential_meta["leg_number"],
                "position": sequential_meta["position_type"],
                "option": sequential_meta["option_type"],
                "status": "SEQUENTIAL_STRIKE_NOT_FOUND",
            }

        entry_result["status"] = "ENTRY_DONE"
        entry_result["_effective_leg_meta"] = sequential_meta
        entry_result["observation_ticker"] = obs_strike_row["ticker"]
        entry_result["observation_strike"] = obs_strike_row["strike"]
        entry_result["observation_trigger_datetime"] = trigger_row["datetime_utc"]
        return entry_result


    def _resolve_observation_trigger(self, observation_meta, day_df: pd.DataFrame, obs_strike_row):
        if observation_meta.get("is_simple_momentum"):
            return self._resolve_momentum_fill(observation_meta, day_df, obs_strike_row)
        if observation_meta.get("is_range_breakout"):
            return self._resolve_range_breakout_fill(observation_meta, day_df, obs_strike_row)

        logger.warning(
            f"Leg {observation_meta['leg_number']}: sequential_leg is configured but the "
            f"observation leg has neither is_simple_momentum nor is_range_breakout enabled"
        )
        return None


    def _enter_sequential_leg(self, sequential_meta, day_df: pd.DataFrame, trigger_datetime):
        chain_snapshot = self._get_chain_snapshot_at(day_df, trigger_datetime)
        if chain_snapshot is None:
            return None

        leg_chain = self._filter_leg_chain(chain_snapshot, sequential_meta)
        if leg_chain.empty:
            logger.warning(
                f"Leg {sequential_meta['leg_number']}: no chain rows for sequential_leg "
                f"(option_type={sequential_meta['option_type']}, expiry_type={sequential_meta['expiry_type']})"
            )
            return None

        strike_row = self.select_strike(sequential_meta, leg_chain)
        if strike_row is None:
            return None

        if sequential_meta.get("is_simple_momentum"):
            fill_row = self._resolve_momentum_fill(sequential_meta, day_df, strike_row)
            if fill_row is None:
                return None
        elif sequential_meta.get("is_range_breakout"):
            fill_row = self._resolve_range_breakout_fill(sequential_meta, day_df, strike_row)
            if fill_row is None:
                return None
        else:
            fill_row = strike_row

        return self._build_entry_result(
            sequential_meta, sequential_meta["position_type"], strike_row, fill_row, is_reentry=False
        )


    def evaluate_leg_exit(self, leg_meta, leg_result, day_df):
        """Route to INTRADAY or BTST phase1 evaluator based on strategy type."""
        if self.strategy_type == "btst":
            return self._evaluate_leg_exit_btst_phase1(leg_meta, leg_result, day_df)
        else:
            return self._evaluate_leg_exit_intraday(leg_meta, leg_result, day_df)


    def _evaluate_leg_exit_intraday(self, leg_meta, leg_result, day_df):
        entry_datetime = leg_result["entry_datetime"]
        exit_cutoff = self.exit_time

        subset = day_df.loc[
            (day_df["ticker"] == leg_result["ticker"])
            & (day_df["datetime_utc"] > entry_datetime)
            & (day_df["trade_time"] <= exit_cutoff)
        ].sort_values("datetime_utc")

        if not subset.empty:
            hit = self._check_sl_target_hit(leg_meta, leg_result, subset)
            if hit is not None:
                exit_row, exit_reason, exit_fill_price = hit
                leg_result["exit_datetime"] = exit_row["datetime_utc"]
                leg_result["exit_price"] = exit_fill_price
                leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
                leg_result["exit_reason"] = exit_reason
                leg_result["status"] = "EXIT_DONE"
                return leg_result

        # Force close at exit_time -- or, if there was no data at all in the
        exit_row = subset.iloc[-1] if not subset.empty else self._find_exit_time_candle(day_df, leg_result["ticker"])
        if exit_row is None:
            leg_result["exit_datetime"] = None
            leg_result["exit_price"] = None
            leg_result["underlying_exit_price"] = None
            leg_result["exit_reason"] = "NO_EXIT_DATA"
            leg_result["status"] = "OPEN_NO_EXIT_SIGNAL"
            return leg_result

        leg_result["exit_datetime"] = exit_row["datetime_utc"]
        leg_result["exit_price"] = exit_row["close"]
        leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
        leg_result["exit_reason"] = "TIME_EXIT"
        leg_result["status"] = "EXIT_DONE"
        return leg_result


    def _find_exit_time_candle(self, day_df: pd.DataFrame, ticker: str):
        candles = day_df.loc[
            (day_df["ticker"] == ticker) & (day_df["trade_time"] <= self.exit_time)
        ].sort_values("datetime_utc")
        return candles.iloc[-1] if not candles.empty else None


    def _evaluate_leg_exit_btst_phase1(self, leg_meta, leg_result, day_df):
        """BTST Phase 1 (Day-1): Monitor SL/TGT until day1_market_close.
        If neither hit, mark as HELD_OVERNIGHT for next day's processing.
        """
        entry_datetime = leg_result["entry_datetime"]

        subset = day_df.loc[
            (day_df["ticker"] == leg_result["ticker"])
            & (day_df["datetime_utc"] >= entry_datetime)
            & (day_df["trade_time"] <= self.day1_market_close)
        ].sort_values("datetime_utc")

        if subset.empty:
            leg_result["status"] = "HELD_OVERNIGHT"
            leg_result["exit_datetime"] = None
            leg_result["exit_price"] = None
            leg_result["underlying_exit_price"] = None
            leg_result["_trade_date"] = entry_datetime.date()
            return leg_result

        hit = self._check_sl_target_hit(leg_meta, leg_result, subset)
        if hit is not None:
            # Individual leg SL/TGT hit before day1_market_close - exit today
            exit_row, exit_reason, exit_fill_price = hit
            leg_result["exit_datetime"] = exit_row["datetime_utc"]
            leg_result["exit_price"] = exit_fill_price
            leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
            leg_result["exit_reason"] = exit_reason
            leg_result["status"] = "EXIT_DONE"
            return leg_result

        leg_result["status"] = "HELD_OVERNIGHT"
        leg_result["exit_datetime"] = None
        leg_result["exit_price"] = None
        leg_result["underlying_exit_price"] = None
        leg_result["_trade_date"] = entry_datetime.date()
        return leg_result


    def _evaluate_leg_exit_btst_phase2(self, leg_meta, leg_result, day_df):
        ticker = leg_result["ticker"]

        # A true carryover entered YESTERDAY, so it is live from the Day-2
        # open. A leg that RE-ENTERED today is only live from its own entry --
        # scanning it from the Day-2 open let it "exit" on a bar before it
        # existed, producing exit_datetime earlier than entry_datetime.
        start_mask = day_df["trade_time"] >= self.day2_market_open
        entry_dt = leg_result.get("entry_datetime")
        if entry_dt is not None:
            today = day_df["trade_date"].iloc[0] if not day_df.empty else None
            if today is not None and entry_dt.date() == today:
                start_mask &= day_df["datetime_utc"] >= entry_dt

        full_day_series = day_df.loc[
            (day_df["ticker"] == ticker) & start_mask
        ].sort_values("datetime_utc")

        if full_day_series.empty:
            logger.warning(
                f"[DEBUG] No rows at all for ticker={ticker} on this trade_date. "
                f"Sample tickers present today: {day_df['ticker'].unique()[:5]}"
            )

        leg_series = full_day_series.loc[full_day_series["trade_time"] <= self.exit_time]

        used_series = leg_series if not leg_series.empty else full_day_series

        if used_series.empty:
            leg_result["exit_datetime"] = None
            leg_result["exit_price"] = None
            leg_result["underlying_exit_price"] = None
            leg_result["exit_reason"] = "No Day-2 data for this ticker"
            leg_result["status"] = "OPEN_NO_EXIT_SIGNAL"
            return leg_result

        hit = self._check_sl_target_hit(leg_meta, leg_result, used_series)
        if hit is not None:
            exit_row, exit_reason, exit_fill_price = hit
            leg_result["exit_datetime"] = exit_row["datetime_utc"]
            leg_result["exit_price"] = exit_fill_price
            leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
            leg_result["exit_reason"] = exit_reason
            leg_result["status"] = "EXIT_DONE"
            return leg_result

        exit_row = leg_series.iloc[-1] if not leg_series.empty else full_day_series.iloc[0]

        # Use exit_row's price, but timestamp = exactly exit_time
        if not leg_series.empty:
           exit_datetime = pd.Timestamp(f"{exit_row['trade_date']} {self.exit_time}", tz='UTC')
        else:
           exit_datetime = exit_row["datetime_utc"]

        leg_result["exit_datetime"] = exit_datetime
        leg_result["exit_price"] = round(float(exit_row["close"]), 2)
        leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
        leg_result["exit_reason"] = "BTST_EXIT"
        leg_result["status"] = "EXIT_DONE"
        return leg_result


    def _is_day1_market_closed(self, current_time: dt_time) -> bool:
        """Helper: check if current_time is past day1_market_close."""
        return current_time > self.day1_market_close


    def _close_held_overnight_leg(self, leg_result, leg_meta, day_df):
        """BTST helper: close a position held overnight using Phase-2 logic."""
        return self._evaluate_leg_exit_btst_phase2(leg_meta, leg_result, day_df)


    def _flush_unclosed_final_holds(self):
        """BTST-only: legs still HELD_OVERNIGHT with no Day-2 data left to
        close them (i.e. entered on the LAST trade_date in the dataframe,
        with nothing after it to attempt a Phase-2 close against) are
        REMOVED from trade_results entirely, instead of shown as UNCLOSED.
        """
        if not self.held_from_previous_day:
            logger.info("No unclosed overnight positions at end of backtest")
            return

        unresolved_leg_ids = {
            id(leg_result)
            for leg_result in self.held_from_previous_day.values()
            if leg_result.get("status") == "HELD_OVERNIGHT"
        }

        if not unresolved_leg_ids:
            return

        logger.warning(
            f"End of backtest: {len(unresolved_leg_ids)} overnight positions "
            f"had no Day-2 data -- removing them from the response entirely"
        )

        for day_block in self.trade_results:
            day_block["legs"] = [
                leg for leg in day_block["legs"] if id(leg) not in unresolved_leg_ids
            ]

        self.trade_results = [
            day_block for day_block in self.trade_results if day_block["legs"]
        ]
   

    def _reentry_search_frame(self, day_df: pd.DataFrame) -> pd.DataFrame:
        """Entry-day rows plus the NEXT trading day's rows up to the strategy
        exit_time.

        A pending re-entry order (return-to-cost, momentum level) that never
        fills before the close is still live overnight -- AlgoTest fills it
        the next morning. Bounded by exit_time rather than the market close,
        because a BTST position must be square by the Day-2 cutoff.

        INTRADAY is unaffected: there is no Day 2 to spill into."""
        if self.strategy_type != "btst" or day_df.empty:
            return day_df
        today = day_df["trade_date"].iloc[0]
        cached = self._reentry_frame_cache.get(today)
        if cached is not None:
            return cached
        next_date = self._next_trade_date.get(today)
        frame = day_df
        if next_date is not None:
            tail = self.df.loc[
                (self.df["trade_date"] == next_date)
                & (self.df["trade_time"] <= self.exit_time)
            ]
            if not tail.empty:
                frame = pd.concat([day_df, tail])
        self._reentry_frame_cache[today] = frame
        return frame


    def _run_reentry_loop(self, leg_meta, first_result, day_df, evaluator):
        lot_size = leg_meta.get("lot_size", 1)
        remaining_sl_reentries = int(leg_meta.get("reentry_sl_value")) if leg_meta.get("is_reentry_sl") else 0
        remaining_target_reentries = int(leg_meta.get("reentry_target_value")) if leg_meta.get("is_reentry_target") else 0

        new_legs = []
        prev_result = first_result
        current_leg_meta = leg_meta
        safety_cap = 50  # defensive upper bound
        while safety_cap > 0 and prev_result.get("status") == "EXIT_DONE" and prev_result.get("exit_reason") in ("STOPLOSS_HIT", "TARGET_HIT"):
            safety_cap -= 1

            if prev_result["exit_reason"] == "STOPLOSS_HIT":
                if remaining_sl_reentries <= 0:
                    break
                remaining_sl_reentries -= 1
                mode = current_leg_meta.get("reentry_sl_type", "RE_ASAP")
            else:
                if remaining_target_reentries <= 0:
                    break
                remaining_target_reentries -= 1
                mode = current_leg_meta.get("reentry_target_type", "RE_ASAP")

            if mode not in REENTRY_MODES:
                logger.warning(f"Leg {leg_meta['leg_number']}: unsupported reentry mode '{mode}'")
                break

            reentry_result = self._resolve_reentry(
                current_leg_meta, mode, prev_result, self._reentry_search_frame(day_df)
            )
            if reentry_result is None:
                break

            lazy_leg_meta = reentry_result.pop("_lazy_leg_meta", None)
            if lazy_leg_meta is not None:
                effective_leg_meta = lazy_leg_meta
            elif mode.endswith("_REVERSE"):
                effective_leg_meta = dict(current_leg_meta)
                effective_leg_meta["position_type"] = reentry_result["position"]
            else:
                effective_leg_meta = current_leg_meta

            # A re-entry that only filled on Day 2 belongs to Day 2: the
            # evaluator here holds Day-1 data and knows nothing about it. Hand
            # it to the carryover machinery, which closes it at the Day-2
            # exit_time, and end the chain -- there is no room for another
            # re-entry before the square-off.
            if (self.strategy_type == "btst"
                    and reentry_result["entry_datetime"].date() > day_df["trade_date"].iloc[0]):
                reentry_result.update({
                    "status": "HELD_OVERNIGHT",
                    "exit_datetime": None, "exit_price": None,
                    "underlying_exit_price": None, "exit_reason": None, "pnl": None,
                    "quantity_multiplier": QUANTITY * lot_size,
                    "_trade_date": reentry_result["entry_datetime"].date(),
                    # It fills on Day 2 but belongs to the trade that opened on
                    # Day 1 -- report it under that day, as AlgoTest does.
                    "_chain_date": day_df["trade_date"].iloc[0],
                    "is_reentry": True, "reentry_mode": mode,
                })
                new_legs.append(reentry_result)
                break

            reentry_result = evaluator(effective_leg_meta, reentry_result, day_df)
            if reentry_result.get("status") == "EXIT_DONE":
                direction = 1 if reentry_result["position"] == "BUY" else -1
                reentry_result["quantity_multiplier"] = QUANTITY * lot_size
                reentry_result["pnl"] = round(
                    (reentry_result["exit_price"] - reentry_result["entry_price"]) * QUANTITY * lot_size * direction, 2
                )
            elif reentry_result.get("status") == "HELD_OVERNIGHT":
                # FIX: mark trade_date so process_day_btst Phase-1 can find/close
                # this leg the next day, same as a normal HELD_OVERNIGHT leg.
                reentry_result["_trade_date"] = reentry_result["entry_datetime"].date()

            reentry_result["is_reentry"] = True
            reentry_result["reentry_mode"] = mode
            new_legs.append(reentry_result)
            prev_result = reentry_result

            if lazy_leg_meta is not None:
                current_leg_meta = lazy_leg_meta
                remaining_sl_reentries = int(current_leg_meta.get("reentry_sl_value")) if current_leg_meta.get("is_reentry_sl") else 0
                remaining_target_reentries = int(current_leg_meta.get("reentry_target_value")) if current_leg_meta.get("is_reentry_target") else 0

        return new_legs


    @staticmethod
    def _is_spot_bullish(leg_meta: dict, position_type: str) -> bool:
        """True when the leg gains as the UNDERLYING rises: long calls and
        short puts. UNDERLYING_* SL/target levels must sit on the losing/
        winning side of this spot exposure -- which depends on option type
        AND position, not position alone (a SELL PE loses when spot FALLS,
        so its underlying stoploss is BELOW entry spot)."""
        return (leg_meta.get("option_type") == "CE") == (position_type == "BUY")


    def _calc_target_price(self, leg_meta: dict, entry_price: float, underlying_entry_price: float, position_type: str):
        if not leg_meta.get("is_target"):
            return None
        target_type = leg_meta.get("target_type")
        target_value = leg_meta.get("target_value")
        # Premium: BUY profits as premium rises, SELL as it falls.
        direction = 1 if position_type == "BUY" else -1
        # Spot: favorable direction follows the leg's spot exposure.
        spot_direction = 1 if self._is_spot_bullish(leg_meta, position_type) else -1

        if target_type == "POINTS":
            return self._round_to_tick(entry_price + direction * target_value)
        elif target_type == "PERCENT":
            return self._round_to_tick(entry_price * (1 + direction * target_value / 100))
        elif target_type == "UNDERLYING_POINTS":
            return round(underlying_entry_price + spot_direction * target_value, 2)
        elif target_type == "UNDERLYING_PERCENT":
            return round(underlying_entry_price * (1 + spot_direction * target_value / 100), 2)

        logger.warning(f"Leg {leg_meta['leg_number']}: unsupported target_type '{target_type}'")
        return None


    def _round_to_tick(self, price: float) -> float:
        """Snap a PREMIUM level to a tradeable tick. A PERCENT target of
        482.30 x 1.95 = 940.485 is not a price anyone can be filled at;
        AlgoTest uses 940.50. Only premium levels get this -- UNDERLYING_*
        levels are index values and are left alone."""
        return round(round(price / self.TICK_SIZE) * self.TICK_SIZE, 2)


    def _calc_stoploss_price(self, leg_meta: dict, entry_price: float, underlying_entry_price: float, position_type: str):
        if not leg_meta.get("is_stoploss"):
            return None
        stoploss_type = leg_meta.get("stoploss_type")
        stoploss_value = leg_meta.get("stoploss_value")
        # Stoploss direction is the mirror image of target direction.
        direction = -1 if position_type == "BUY" else 1
        spot_direction = -1 if self._is_spot_bullish(leg_meta, position_type) else 1

        if stoploss_type == "POINTS":
            return self._round_to_tick(entry_price + direction * stoploss_value)
        elif stoploss_type == "PERCENT":
            return self._round_to_tick(entry_price * (1 + direction * stoploss_value / 100))
        elif stoploss_type == "UNDERLYING_POINTS":
            return round(underlying_entry_price + spot_direction * stoploss_value, 2)
        elif stoploss_type == "UNDERLYING_PERCENT":
            return round(underlying_entry_price * (1 + spot_direction * stoploss_value / 100), 2)

        logger.warning(f"Leg {leg_meta['leg_number']}: unsupported stoploss_type '{stoploss_type}'")
        return None


    def _calc_trailing_stoploss_series(self, leg_meta: dict, entry_price: float, stoploss_price, position_type: str, close: np.ndarray):
        if not leg_meta.get("is_trail_sl") or stoploss_price is None:
            return stoploss_price

        stoploss_type = leg_meta.get("stoploss_type")
        if stoploss_type in ("UNDERLYING_POINTS", "UNDERLYING_PERCENT"):
            leg_number = leg_meta["leg_number"]
            if leg_number not in self._trail_skip_warned:
                self._trail_skip_warned.add(leg_number)
                logger.warning(
                    f"Leg {leg_number}: is_trail_sl=True is ignored because "
                    f"stoploss_type={stoploss_type} -- trailing steps are defined "
                    f"on the instrument premium, not on the underlying."
                )
            return stoploss_price

        trail_sl_type = leg_meta.get("trail_sl_type")
        instrument_moves = leg_meta.get("instrument_moves")
        stoploss_moves = leg_meta.get("stoploss_moves")

        if trail_sl_type is None or not instrument_moves or stoploss_moves is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: is_trail_sl=True but trail_sl_type/"
                f"instrument_moves/stoploss_moves missing -- skipping trailing"
            )
            return stoploss_price

        if trail_sl_type == "POINTS":
            step_move = instrument_moves
            step_gain = stoploss_moves
        elif trail_sl_type == "PERCENT":
            step_move = round(entry_price * instrument_moves / 100, 2)
            step_gain = round(stoploss_price * stoploss_moves / 100, 2)
        else:
            logger.warning(f"Leg {leg_meta['leg_number']}: unsupported trail_sl_type '{trail_sl_type}'")
            return stoploss_price

        if not step_move or step_move <= 0:
            return stoploss_price

        if position_type == "BUY":
            best_so_far = np.maximum.accumulate(close)
            favorable_move = best_so_far - entry_price
            direction = 1
        else:
            best_so_far = np.minimum.accumulate(close)
            favorable_move = entry_price - best_so_far
            direction = -1

        favorable_move = np.clip(favorable_move, a_min=0, a_max=None)
        steps = np.floor(favorable_move / step_move)

        return stoploss_price + direction * steps * step_gain


    @staticmethod
    def _target_hit_mask(high: np.ndarray, low: np.ndarray, underlying_high: np.ndarray, underlying_low: np.ndarray, target_price: float, target_type: str, position_type: str, spot_bullish: bool) -> np.ndarray:
        if target_type in ("POINTS", "PERCENT"):
            return high >= target_price if position_type == "BUY" else low <= target_price
        elif target_type in ("UNDERLYING_POINTS", "UNDERLYING_PERCENT"):
            return underlying_high >= target_price if spot_bullish else underlying_low <= target_price


    @staticmethod
    def _stoploss_hit_mask(high: np.ndarray, low: np.ndarray, underlying_high: np.ndarray, underlying_low: np.ndarray, stoploss_price: float, stoploss_type: str, position_type: str, spot_bullish: bool) -> np.ndarray:
        if stoploss_type in ("POINTS", "PERCENT"):
            return low <= stoploss_price if position_type == "BUY" else high >= stoploss_price
        elif stoploss_type in ("UNDERLYING_POINTS", "UNDERLYING_PERCENT"):
            return underlying_low <= stoploss_price if spot_bullish else underlying_high >= stoploss_price


    @staticmethod
    def _underlying_exit_fill(level, underlying_open, underlying_high, underlying_low,
                              exit_idx, candle_open, close_fill, breach_above):
        """Fill price for an exit triggered by the UNDERLYING (the
        UNDERLYING_POINTS / UNDERLYING_PERCENT stop and target types).

        Two cases, and they fill at different prices:

          * the spot crossed the level DURING the bar. The option price at the
            crossing instant is unknowable inside a 1-minute bar, so use the
            bar close.
          * the spot had ALREADY passed the level when the bar opened. Nothing
            could have filled at the level; the first tradeable price is the
            bar's open. The common case is a BTST position gapping through its
            stop overnight and exiting on the next session's first bar.

        Which case applies is decided by the spot's OPEN, not by whether the
        whole bar sits beyond the level. The whole-bar test is only an
        approximation of this and a single tick defeats it: on 2026-01-27 the
        spot opened at 81,252.20, already past an 81,248.59 target, then
        wicked to 81,248.51 -- 0.08 below -- so the bar was no longer wholly
        beyond and the exit mis-filled at the close.

        Measured against AlgoTest over 2024-2026: of the underlying stops the
        whole-bar test still got wrong, 36 of 39 are fixed by reading the
        open, and the underlying_open fallback below keeps the old behaviour
        on data built before that column existed.
        """
        if underlying_open is None:
            gapped = (underlying_low[exit_idx] >= level if breach_above
                      else underlying_high[exit_idx] <= level)
        else:
            gapped = (underlying_open[exit_idx] >= level if breach_above
                      else underlying_open[exit_idx] <= level)
        return candle_open if gapped else close_fill


    def _check_sl_target_hit(self, leg_meta, leg_result, leg_series, start_idx=0):
        entry_price = leg_result["entry_price"]
        underlying_entry_price = leg_result.get("underlying_entry_price")
        position_type = leg_meta["position_type"]

        target_price = self._calc_target_price(leg_meta, entry_price, underlying_entry_price, position_type)
        stoploss_price = self._calc_stoploss_price(leg_meta, entry_price, underlying_entry_price, position_type)
        leg_result["target_price"] = target_price
        leg_result["stoploss_price"] = stoploss_price

        series = leg_series.iloc[start_idx:]
        if series.empty:
            return None

        close = series["close"].to_numpy()
        high = series["high"].to_numpy()
        low = series["low"].to_numpy()
        bar_open = series["open"].to_numpy()

        if "underlying_high" in series.columns:
            underlying_high = series["underlying_high"].to_numpy()
            underlying_low = series["underlying_low"].to_numpy()
        else:
            underlying_high = underlying_low = series["underlying_price"].to_numpy()

        # Present only on data built with the spot's open (see
        # _underlying_exit_fill); None falls back to the whole-bar test.
        underlying_open = (
            series["underlying_open"].to_numpy()
            if "underlying_open" in series.columns else None
        )

        trailing_stoploss_price = self._calc_trailing_stoploss_series(
            leg_meta, entry_price, stoploss_price, position_type, close
        )

        spot_bullish = self._is_spot_bullish(leg_meta, position_type)
        target_hit_mask = (
            self._target_hit_mask(high, low, underlying_high, underlying_low, target_price, leg_meta.get("target_type"), position_type, spot_bullish)
            if target_price is not None
            else np.zeros(len(close), dtype=bool)
        )
        stoploss_hit_mask = (
            self._stoploss_hit_mask(high, low, underlying_high, underlying_low, trailing_stoploss_price, leg_meta.get("stoploss_type"), position_type, spot_bullish)
            if stoploss_price is not None
            else np.zeros(len(close), dtype=bool)
        )
        hit_mask = target_hit_mask | stoploss_hit_mask

        if not hit_mask.any():
            return None

        exit_idx = int(hit_mask.argmax())
        exit_row = series.iloc[exit_idx]

        candle_open = float(bar_open[exit_idx])
        target_gapped_open = (
            target_hit_mask[exit_idx]
            and leg_meta.get("target_type") in ("POINTS", "PERCENT")
            and (candle_open >= target_price if position_type == "BUY" else candle_open <= target_price)
        )
        underlying_fill = float(close[exit_idx])

        if target_hit_mask[exit_idx] and (target_gapped_open or not stoploss_hit_mask[exit_idx]):
            exit_reason = "TARGET_HIT"
            if leg_meta.get("target_type") in ("POINTS", "PERCENT"):
                exit_fill_price = candle_open if target_gapped_open else float(target_price)
            else:
                exit_fill_price = self._underlying_exit_fill(
                    target_price, underlying_open, underlying_high, underlying_low,
                    exit_idx, candle_open, underlying_fill, breach_above=spot_bullish
                )
        else:
            exit_reason = "STOPLOSS_HIT"
            level = float(
                trailing_stoploss_price[exit_idx]
                if isinstance(trailing_stoploss_price, np.ndarray)
                else trailing_stoploss_price
            )
            if leg_meta.get("stoploss_type") in ("POINTS", "PERCENT"):
                gapped = candle_open <= level if position_type == "BUY" else candle_open >= level
                exit_fill_price = candle_open if gapped else level
            else:
                exit_fill_price = self._underlying_exit_fill(
                    level, underlying_open, underlying_high, underlying_low,
                    exit_idx, candle_open, underlying_fill, breach_above=not spot_bullish
                )

        return exit_row, exit_reason, round(exit_fill_price, 2)

   
    def select_strike(self, leg_meta: dict, leg_chain: pd.DataFrame):
        """Dispatches to the strike-selection function matching
        strike_criteria."""
        strike_criteria = leg_meta.get("strike_criteria")

        if strike_criteria == "based on points":
            return self._select_strike_based_on_points(leg_meta, leg_chain)
        elif strike_criteria == "closest premium":
            return self._select_strike_closest_premium(leg_meta, leg_chain)
        elif strike_criteria == "premium range":
            return self._select_strike_premium_range(leg_meta, leg_chain)
        elif strike_criteria == "atm percentage":
            return self._select_strike_percentage_of_atm(leg_meta, leg_chain)

        logger.warning(f"Unsupported strike_criteria: {strike_criteria}")
        return None


    @staticmethod
    def _strike_ladder_step(strikes: np.ndarray) -> int:
        """The listed strike interval implied by `strikes`: the GCD of the
        gaps between them.

        GCD rather than the smallest or the most common gap, because strikes
        that have not traded leave holes in the observed ladder -- a run of
        gaps like 100/200/500 still implies a 100-point ladder. Measured
        across the whole data tree this returns 100 for the near expiries and
        500 for the far-dated ones (which genuinely list only 500s), and
        never a spurious sub-100 value.
        """
        if strikes.size < 2:
            return BacktestEngine._FALLBACK_STRIKE_STEP
        gaps = np.diff(strikes)
        gaps = gaps[gaps > 0]
        if gaps.size == 0:
            return BacktestEngine._FALLBACK_STRIKE_STEP
        return int(np.gcd.reduce(gaps)) or BacktestEngine._FALLBACK_STRIKE_STEP


    def _select_strike_based_on_points(self, leg_meta: dict, leg_chain: pd.DataFrame):
        """
        atm_strike values:
          0 / "0" / "ATM"      -> the strike nearest the underlying on the
                                  listed ladder: round(spot / step) * step
          "ITM-1", "ITM-2", ...-> n ladder steps in the money  (ATM -+ n*step)
          "OTM-1", "OTM-2", ...-> n ladder steps out of the money

        Steps run along the LISTED ladder, not along the strikes that happen
        to have printed. Counting printed rows instead made both ATM and the
        ITM-n/OTM-n offsets drift whenever a strike was quiet: on 2025-04-07
        spot was 72,678 (true ATM 72700, which first printed at 09:17), so
        the 09:16 entry took 72500 as ATM and, walking the printed ITM rows
        72400/72200/72100/72000/71500, landed on 71500 for ITM-5 -- 1,200
        points in instead of 500.

        In the money is DOWN the ladder for a call and UP for a put, since a
        call gains intrinsic value as the strike falls and a put as it rises.
        """
        atm_strike = leg_meta.get("atm_strike", 0)

        if leg_chain.empty:
            logger.warning(f"Leg {leg_meta['leg_number']}: empty chain snapshot")
            return None

        strikes = np.unique(leg_chain["strike"].to_numpy().astype(np.int64))
        step = self._strike_ladder_step(strikes)
        spot = float(leg_chain["underlying_price"].iloc[0])
        atm = int(round(spot / step) * step)

        if atm_strike in (0, "0", "ATM"):
            target_strike, moneyness_type = atm, "ATM"
        else:
            moneyness_type, _, offset_str = str(atm_strike).partition("-")
            moneyness_type = moneyness_type.strip().upper()
            offset = int(offset_str) if offset_str.strip().isdigit() else 1

            if moneyness_type not in ("ITM", "OTM"):
                logger.warning(f"Leg {leg_meta['leg_number']}: unrecognized atm_strike '{atm_strike}'")
                return None

            inward = -1 if leg_meta["option_type"] == "CE" else 1
            direction = inward if moneyness_type == "ITM" else -inward
            target_strike = atm + direction * offset * step

        row = leg_chain.loc[leg_chain["strike"] == target_strike]
        if not row.empty:
            selected = row.iloc[0].copy()
        else:
            selected = self._first_print_row(leg_chain, target_strike)
            if selected is None:
                logger.warning(
                    f"Leg {leg_meta['leg_number']}: {atm_strike} resolves to strike "
                    f"{target_strike} (ATM {atm}, ladder step {step}) which never "
                    f"trades today -- no bar to enter on"
                )
                return None

        # The snapshot labels moneyness by strike-vs-spot, so the ladder ATM
        # reads as ITM/OTM whenever spot sits off the strike. Report what was
        # asked for instead, and leave the row's own price/distance columns
        # untouched.
        selected["moneyness"] = moneyness_type
        return selected


    def _first_print_row(self, leg_chain: pd.DataFrame, target_strike: int):
        """A chain row for a ladder strike that is listed but has not printed
        yet at the entry minute.

        The as-of snapshot can only carry a contract forward once it has
        traded, so a strike that is silent at the entry minute has no row at
        all, and the leg used to be dropped as STRIKE_NOT_FOUND. AlgoTest,
        working from a quote feed, still enters it -- and on 41 of the 52 such
        cases in 2024 its entry price is exactly this contract's FIRST print
        of the day. So fall back to that first bar, filled at its OPEN (the
        first traded price) and stamped at the entry minute.

        This reads a price from later in the session: it stands in for the
        quote a live feed would have shown at the entry minute. Only the
        ATM/ITM/OTM ladder uses it. The premium-based criteria must not --
        they match ON the price, so a not-yet-happened number would let them
        pick the strike itself with hindsight.
        """
        day_df = self._snapshot_day_df
        if day_df is None or leg_chain.empty:
            return None

        reference = leg_chain.iloc[0]
        snapshot_time = reference["datetime_utc"]
        bars = day_df.loc[
            (day_df["strike"] == target_strike)
            & (day_df["option_type"] == reference["option_type"])
            & (day_df["expiration_date"] == reference["expiration_date"])
            & (day_df["datetime_utc"] > snapshot_time)
        ]
        if bars.empty:
            return None

        spot = float(reference["underlying_price"])
        row = bars.loc[bars["datetime_utc"].idxmin()].copy()
        row["close"] = float(row["open"])
        row["datetime_utc"] = snapshot_time
        row["trade_time"] = reference["trade_time"]
        row["underlying_price"] = spot
        row["distance_from_underlying"] = abs(float(target_strike) - spot)
        return row


    def _recently_priced(self, leg_chain: pd.DataFrame, leg_meta: dict) -> pd.DataFrame:
        """Narrows a chain to contracts that printed recently.

        Only for the premium-based criteria. ATM/ITM/OTM selection keys off
        the strike ladder, so it just needs a contract to exist; matching on
        `close` instead means an option that last traded hours ago can win
        on a premium nobody could have got. Falls back to the full chain
        when nothing is recent -- a stale match still beats dropping the leg,
        and the warning says which happened.
        """
        if "price_age_minutes" not in leg_chain.columns or leg_chain.empty:
            return leg_chain
        fresh = leg_chain.loc[leg_chain["price_age_minutes"] <= self.PREMIUM_MAX_PRICE_AGE_MINUTES]
        if fresh.empty:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: no contract on this expiry traded within "
                f"{self.PREMIUM_MAX_PRICE_AGE_MINUTES}min of the entry -- matching premium "
                f"against last traded prices up to "
                f"{leg_chain['price_age_minutes'].min():.0f}min old"
            )
            return leg_chain
        return fresh


    def _select_strike_closest_premium(self, leg_meta: dict, leg_chain: pd.DataFrame):
        premium_value = leg_meta.get("premium_value")

        if premium_value is None:
            logger.warning(f"Leg {leg_meta['leg_number']}: strike_criteria='Closest Premium' requires premium_value")
            return None

        if leg_chain.empty:
            return None

        leg_chain = self._recently_priced(leg_chain, leg_meta)
        premium_distance = (leg_chain["close"] - premium_value).abs()
        closest_index = premium_distance.idxmin()
        return leg_chain.loc[closest_index]


    def _select_strike_premium_range(self, leg_meta: dict, leg_chain: pd.DataFrame):
        """Selects a strike whose premium (close price) falls within
        [lower_range, upper_range]."""

        lower = leg_meta.get("lower_range")
        upper = leg_meta.get("upper_range")

        if lower is None or upper is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: strike_criteria='Premium Range' "
                f"requires lower_range and upper_range"
            )
            return None

        if lower > upper:
            lower, upper = upper, lower

        if leg_chain.empty:
            return None

        leg_chain = self._recently_priced(leg_chain, leg_meta)
        in_range = leg_chain.loc[(leg_chain["close"] >= lower) & (leg_chain["close"] <= upper)]

        if in_range.empty:
            # No strike's premium falls inside the range -- fall back to
            # whichever strike's premium is nearest to the range bounds,
            # rather than dropping the leg's entry entirely.
            clipped = leg_chain["close"].clip(lower, upper)
            distance = (leg_chain["close"] - clipped).abs()
            closest_index = distance.idxmin()
            logger.warning(
                f"Leg {leg_meta['leg_number']}: no strike premium within range "
                f"[{lower}, {upper}] -- falling back to nearest available premium"
            )
            return leg_chain.loc[closest_index]

        if len(in_range) == 1:
            return in_range.iloc[0]

        if leg_meta["position_type"] == "SELL":
            best_index = in_range["close"].idxmax()
        else:
            best_index = in_range["close"].idxmin()

        return in_range.loc[best_index]


    def _select_strike_percentage_of_atm(self, leg_meta: dict, leg_chain: pd.DataFrame):
        """Calculation: Selected Strike = ATM Strike +/- (percentage% of ATM Strike)
        If exact strike not found in chain, selects closest available strike."""

        operator = leg_meta.get("strike_sign")
        percentage_value = leg_meta.get("multiplier_percentage")

        if operator is None or percentage_value is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: strike_criteria='ATM Percentage' "
                f"requires strike_sign (+/-) and multiplier_percentage"
            )
            return None

        if operator not in ("+", "-"):
            logger.warning(
                f"Leg {leg_meta['leg_number']}: invalid strike_sign '{operator}' "
                f"(valid: '+' or '-')"
            )
            return None

        percentage_value = float(percentage_value)

        atm_rows = leg_chain.loc[leg_chain["moneyness"] == "ATM"]
        if atm_rows.empty:
            logger.warning(f"Leg {leg_meta['leg_number']}: no ATM strike found in chain snapshot")
            return None

        atm_strike = float(atm_rows.iloc[0]["strike"])
        percentage_offset = atm_strike * percentage_value / 100.0
        direction = 1 if operator == "+" else -1
        target_strike = round(atm_strike + direction * percentage_offset, 2)

        strikes = leg_chain["strike"] if leg_chain["strike"].dtype == float else leg_chain["strike"].astype(float)
        strike_distance = (strikes - target_strike).abs()
        closest_index = strike_distance.idxmin()

        return leg_chain.loc[closest_index]

    # ======================================================================
    # Momentum & Range Breakout
    # ======================================================================

    def _resolve_momentum_fill(self, leg_meta, day_df, strike_row):
        """Strike is already fixed (strike_row, picked at entry_time). Scans
        that SAME ticker's candles forward from entry_time, looking for the
        first candle where the reference price crosses the momentum trigger
        level. Returns that candle as the actual fill, or None if the
        trigger is never reached before exit_time."""
        momentum_type = leg_meta.get("momentum_type")
        momentum_value = leg_meta.get("momentum_value")

        if momentum_type is None or momentum_value is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: is_simple_momentum=True but "
                f"momentum_type/momentum_value missing"
            )
            return None

        is_underlying = str(momentum_type).startswith("UNDERLYING")
        # strike_row comes from _asof_chain_snapshot, so its `close` is the
        # contract's last traded price (the only price knowable at this
        # minute) while its datetime_utc is the snapshot minute itself. The
        # scan below relies on that stamping to never look at a bar from
        # before the strike was selected -- see _asof_chain_snapshot.
        base_price = strike_row["underlying_price"] if is_underlying else strike_row["close"]
        trigger_price = self._calc_momentum_trigger(leg_meta, momentum_type, momentum_value, base_price)
        if trigger_price is None:
            return strike_row  # unsupported momentum_type -- fall back to immediate entry

        ticker = strike_row["ticker"]
        # Strictly AFTER the base bar: base_price IS that bar's close, and the
        # intrabar test below would otherwise fire on the very bar the level
        # was measured from.
        series = day_df.loc[
            (day_df["ticker"] == ticker)
            & (day_df["datetime_utc"] > strike_row["datetime_utc"])
            & (day_df["trade_time"] <= self.entry_day_cutoff)
        ].sort_values("datetime_utc")

        if series.empty:
            return None

        # Momentum fires the moment price TOUCHES the level, so test the bar's
        # extreme in the direction being watched -- not its close, which only
        # sees the level minutes later (or never, on a bar that spikes and
        # retraces). Verified against AlgoTest on Jan 2026: the high/low test
        # reproduces its entry minute 11/11, the close test only 5/11.
        up = str(momentum_type).endswith("_UP")
        if is_underlying:
            extreme_col = "underlying_high" if up else "underlying_low"
            if extreme_col not in series.columns:      # pre-OHLC merged files
                extreme_col = "underlying_price"
        else:
            extreme_col = "high" if up else "low"
        reference = series[extreme_col].to_numpy()

        hit_mask = reference >= trigger_price if up else reference <= trigger_price
        if not hit_mask.any():
            return None

        idx = int(hit_mask.argmax())
        fill_row = series.iloc[idx]

        if is_underlying:
            # The level lives on the index, not on this option, so it says
            # nothing about the premium payable -- fill at the bar's close,
            # as underlying-triggered exits do.
            return fill_row

        # Instrument momentum: the level IS a premium, so that is the fill.
        # A bar that OPENED beyond it gapped through, and the touch happened
        # at the open -- the same rule the premium target/SL exits apply.
        bar_open = float(fill_row["open"])
        gapped = bar_open >= trigger_price if up else bar_open <= trigger_price
        fill_row = fill_row.copy()
        fill_row["close"] = round(bar_open if gapped else float(trigger_price), 2)
        return fill_row


    def _calc_momentum_trigger(self, leg_meta, momentum_type: str, momentum_value: float, base_price: float):
        direction = 1 if str(momentum_type).endswith("_UP") else -1 if str(momentum_type).endswith("_DOWN") else None
        if direction is None:
            logger.warning(f"Leg {leg_meta['leg_number']}: unsupported momentum_type '{momentum_type}'")
            return None

        if momentum_type in ("POINTS_UP", "POINTS_DOWN", "UNDERLYING_POINTS_UP", "UNDERLYING_POINTS_DOWN"):
            return round(base_price + direction * momentum_value, 2)
        if momentum_type in ("PERCENT_UP", "PERCENT_DOWN", "UNDERLYING_PERCENT_UP", "UNDERLYING_PERCENT_DOWN"):
            return round(base_price * (1 + direction * momentum_value / 100), 2)

        logger.warning(f"Leg {leg_meta['leg_number']}: unsupported momentum_type '{momentum_type}'")
        return None


    def _get_range_high_low(self, day_df: pd.DataFrame, range_end, ticker):
        cache_key = (range_end, ticker)
        cached = self._range_cache.get(cache_key)
        if cached is not None:
            return cached

        if ticker is not None:
            series = self._get_ticker_series(day_df, ticker)
            price = series["close"]
        else:
            series = self._get_underlying_series(day_df)
            price = series["underlying_price"]
        trade_time = series["trade_time"]

        lo = np.searchsorted(trade_time, self.entry_time, side="left")
        hi = np.searchsorted(trade_time, range_end, side="right")

        result = (None, None) if lo >= hi else (float(np.max(price[lo:hi])), float(np.min(price[lo:hi])))
        self._range_cache[cache_key] = result
        return result


    def _resolve_range_breakout_fill(self, leg_meta, day_df, strike_row):
        range_end = leg_meta.get("_range_end_parsed")
        if range_end is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: range_end_time missing or could not "
                f"be parsed (raw value: '{leg_meta.get('range_end_time')}') -- skipping leg"
            )
            return None
        if range_end <= self.entry_time:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: range_end_time ({leg_meta.get('range_end_time')}) "
                f"is not after entry_time ({self.entry_time}) -- skipping leg"
            )
            return None

        raw_range_on = leg_meta.get("range_on")
        range_on = str(raw_range_on).strip().lower()
        if range_on not in ("high", "low"):
            logger.warning(
                f"Leg {leg_meta['leg_number']}: range_on has an invalid value "
                f"'{raw_range_on}' -- must be exactly 'High' or 'Low'. Skipping leg."
            )
            return None

        watch_high = range_on == "high"

        range_breakout_type = str(leg_meta.get("range_breakout_type")).strip().lower()
        if range_breakout_type not in ("underlying", "instrument"):
            logger.warning(
                f"Leg {leg_meta['leg_number']}: unrecognized range_breakout_type "
                f"'{range_breakout_type}' -- defaulting to 'instrument'"
            )
            range_breakout_type = "instrument"
        is_underlying = range_breakout_type == "underlying"
        range_ticker = None if is_underlying else strike_row["ticker"]

        range_high, range_low = self._get_range_high_low(day_df, range_end, range_ticker)
        if range_high is None:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: no candles found in range window "
                f"[{self.entry_time} - {leg_meta.get('range_end_time')}] -- skipping leg"
            )
            return None

        trigger_price = range_high if watch_high else range_low

        ticker_series = self._get_ticker_series(day_df, strike_row["ticker"])
        trade_time = ticker_series["trade_time"]

        lo = np.searchsorted(trade_time, range_end, side="right")
        hi = np.searchsorted(trade_time, self.entry_day_cutoff, side="right")
        if lo >= hi:
            logger.warning(
                f"Leg {leg_meta['leg_number']}: no candles available between "
                f"range_end_time and exit_time for ticker {strike_row['ticker']} -- skipping leg"
            )
            return None

        reference = (ticker_series["underlying_price"] if is_underlying else ticker_series["close"])[lo:hi]
        hit_mask = reference >= trigger_price if watch_high else reference <= trigger_price

        if not hit_mask.any():
            return None

        rel_idx = int(hit_mask.argmax())
        return ticker_series["frame"].iloc[lo + rel_idx]


    def _resolve_reentry(self, leg_meta: dict, mode: str, prev_result: dict, day_df: pd.DataFrame):
        """Resolve re-entry after SL/TGT hit for RE_ASAP / RE_ASAP_REVERSE /
        RE_COST / RE_COST_REVERSE / RE_MOMENTUM / RE_MOMENTUM_REVERSE /
        LAZY_LEG.

        LAZY_LEG is handled separately, before the generic reverse/
        position_type logic below -- a lazy leg specifies its OWN
        position_type (it's a full leg definition, see the "Create New
        Lazy Leg" modal), so there's no parent position to flip."""
        if mode == "LAZY_LEG":
            return self._reentry_lazy_leg(leg_meta, prev_result, day_df)

        reverse = mode.endswith("_REVERSE")
        position_type = self._flip_position(leg_meta["position_type"]) if reverse else leg_meta["position_type"]

        if mode in ("RE_ASAP", "RE_ASAP_REVERSE"):
            return self._reentry_asap(leg_meta, position_type, prev_result, day_df, mode)
        if mode in ("RE_COST", "RE_COST_REVERSE"):
            return self._reentry_at_cost(leg_meta, position_type, prev_result, day_df, mode)
        if mode in ("RE_MOMENTUM", "RE_MOMENTUM_REVERSE"):
            return self._reentry_momentum(leg_meta, position_type, prev_result, day_df, mode)

        logger.warning(f"Leg {leg_meta['leg_number']}: unsupported reentry mode '{mode}'")
        return None


    def _reentry_asap(self, leg_meta: dict, position_type: str, prev_result: dict, day_df: pd.DataFrame, mode: str):
        """RE ASAP / RE ASAP (Reverse): re-enter immediately in the NEW ATM
        strike at market price, at (or right after) the previous exit."""
        chain_snapshot = self._get_chain_snapshot_at(day_df, prev_result["exit_datetime"])
        if chain_snapshot is None:
            return None

        leg_chain = self._filter_leg_chain(chain_snapshot, leg_meta)
        if leg_chain.empty:
            return None

        strike_row = self.select_strike(leg_meta, leg_chain)
        if strike_row is None:
            return None

        return self._build_entry_result(leg_meta, position_type, strike_row, strike_row, is_reentry=True, reentry_mode=mode)


    def _reentry_at_cost(self, leg_meta: dict, position_type: str, prev_result: dict, day_df: pd.DataFrame, mode: str):
        """RE COST / RE COST (Reverse): SAME option (ticker unchanged), wait
        for its price to come back to the entry price of the attempt that
        just exited, then re-enter there."""
        ticker = prev_result["ticker"]
        return_price = float(prev_result["entry_price"])

        series = day_df.loc[
            (day_df["ticker"] == ticker)
            & (day_df["datetime_utc"] > prev_result["exit_datetime"])
            & (day_df["trade_time"] <= self.entry_day_cutoff)
        ].sort_values("datetime_utc")

        if series.empty:
            return None

        # The price has "come back to cost" the moment a bar TOUCHES it, so
        # test the bar's range rather than its close, and fill AT the cost --
        # that is the whole point of the mode, and it is what AlgoTest books
        # (its RE_COST entry equals the original entry price to the paisa).
        # Testing the close inside a tolerance band instead filled wherever
        # the bar happened to settle, several points away from the cost.
        low = series["low"].to_numpy()
        high = series["high"].to_numpy()
        bar_open = series["open"].to_numpy()
        close = series["close"].to_numpy()
        touched = (low <= return_price) & (high >= return_price)

        # ...or it GAPPED over the level: on one side at the previous close,
        # the other at this open, never trading it. A resting order fills at
        # the first available price, the open -- the same gap-through rule the
        # premium SL/target exits use. Overnight gaps make this the norm for
        # BTST: on 2026-01-29 the cost sat at 112.60, Day 1 closed above it
        # and Day 2 opened at 95.05, which is where AlgoTest fills.
        prev_close = np.concatenate(([float(prev_result["exit_price"])], close[:-1]))
        gapped = (prev_close - return_price) * (bar_open - return_price) < 0

        hit_mask = touched | gapped
        if not hit_mask.any():
            return None

        idx = int(hit_mask.argmax())
        fill_row = series.iloc[idx].copy()
        fill_row["close"] = round(
            return_price if touched[idx] else float(bar_open[idx]), 2
        )
        return self._build_entry_result(leg_meta, position_type, prev_result, fill_row, is_reentry=True, reentry_mode=mode)


    def _reentry_momentum(self, leg_meta: dict, position_type: str, prev_result: dict, day_df: pd.DataFrame, mode: str):
        """RE MOMENTUM / RE MOMENTUM (Reverse): new ATM strike, then apply
        the leg's OWN Simple Momentum settings (reusing _resolve_momentum_fill
        unchanged) before entering. If the leg doesn't have Simple Momentum
        enabled, this behaves exactly like RE ASAP / RE ASAP Reverse, per spec."""
        chain_snapshot = self._get_chain_snapshot_at(day_df, prev_result["exit_datetime"])
        if chain_snapshot is None:
            return None

        leg_chain = self._filter_leg_chain(chain_snapshot, leg_meta)
        if leg_chain.empty:
            return None

        strike_row = self.select_strike(leg_meta, leg_chain)
        if strike_row is None:
            return None

        if leg_meta.get("is_simple_momentum"):
            fill_row = self._resolve_momentum_fill(leg_meta, day_df, strike_row)
            if fill_row is None:
                return None
        else:
            fill_row = strike_row

        return self._build_entry_result(leg_meta, position_type, strike_row, fill_row, is_reentry=True, reentry_mode=mode)


    def _reentry_lazy_leg(self, leg_meta: dict, prev_result: dict, day_df: pd.DataFrame):
        lazy_leg_config = leg_meta.get("lazy_leg")
        if not lazy_leg_config:
            logger.warning(f"Leg {leg_meta['leg_number']}: LAZY_LEG re-entry configured but no lazy_leg definition present")
            return None

        lazy_leg_meta = self._prepare_lazy_leg_meta(leg_meta, lazy_leg_config)

        chain_snapshot = self._get_chain_snapshot_at(day_df, prev_result["exit_datetime"])
        if chain_snapshot is None:
            return None

        leg_chain = self._filter_leg_chain(chain_snapshot, lazy_leg_meta)
        if leg_chain.empty:
            return None

        strike_row = self.select_strike(lazy_leg_meta, leg_chain)
        if strike_row is None:
            return None

        if lazy_leg_meta.get("is_simple_momentum"):
            fill_row = self._resolve_momentum_fill(lazy_leg_meta, day_df, strike_row)
            if fill_row is None:
                return None
        else:
            fill_row = strike_row

        entry_result = self._build_entry_result(
            lazy_leg_meta, lazy_leg_meta["position_type"], strike_row, fill_row, is_reentry=True, reentry_mode="LAZY_LEG",
        )
        entry_result["_lazy_leg_meta"] = lazy_leg_meta
        return entry_result


    def _prepare_lazy_leg_meta(self, parent_leg_meta: dict, lazy_leg_config: dict) -> dict:
        return self._prepare_nested_leg_meta(parent_leg_meta, lazy_leg_config)


    def _get_chain_snapshot_at(self, day_df: pd.DataFrame, reference_datetime):
        """Get option chain snapshot at or after reference_datetime."""
        candidates = day_df.loc[day_df["datetime_utc"] >= reference_datetime, "datetime_utc"]
        if candidates.empty:
            return None
        return self._asof_chain_snapshot(day_df, candidates.min())


    def _asof_chain_snapshot(self, day_df: pd.DataFrame, snapshot_time) -> pd.DataFrame:
        """The tradeable chain as of `snapshot_time`.

        Bars exist only for minutes a contract actually printed, so an
        exact-minute slice of day_df is NOT the chain -- it is only the
        contracts that happened to trade in that one minute. Taken
        literally it corrupts strike selection two ways: a contract that
        was quiet that minute vanishes (the leg dies as NO_CHAIN_DATA),
        and the `moneyness` column -- precomputed per minute over whatever
        rows exist -- can crown a strike hundreds of points away as ATM
        purely because it was the only one to print. Both were real: on
        2024-01-19 the 71800 PE was quiet at 09:30 and the 72000 PE, 155
        points from spot, was labeled ATM and traded.

        So carry each contract's last trade forward: one row per ticker at
        its most recent bar, restamped to `snapshot_time` and re-measured
        against the spot at `snapshot_time`. Prices stay real -- only
        their timestamp moves -- which is how an LTP-driven chain behaves.
        Exits are unaffected: they scan this contract's actual bars, so a
        stop/target still only fires on a price that genuinely printed.

        Limits, all inherent to trade-only data. Only today's bars are
        carried, so an entry in the first minutes of the session has little
        history to draw on and degrades toward the old exact-minute
        behaviour. A strike that has not traded at all today cannot be
        conjured, so ATM falls back to the nearest strike that HAS traded.
        And premium-based strike_criteria match on a last traded price that
        may be old -- unlike ATM selection, which only needs the strike to
        exist, they are sensitive to how stale that price is.
        """
        snapshot_time = pd.Timestamp(snapshot_time)
        # Kept so the ladder selector can reach a listed strike that has not
        # printed yet -- see _first_print_row.
        self._snapshot_day_df = day_df
        cached = self._snapshot_cache.get(snapshot_time)
        if cached is not None:
            return cached

        rows = day_df.loc[day_df["datetime_utc"] <= snapshot_time]
        if rows.empty:
            return day_df.iloc[0:0]

        snapshot = (
            rows.sort_values("datetime_utc", kind="stable")
                .drop_duplicates("ticker", keep="last")
                .copy()
        )
        spot_rows = day_df.loc[day_df["datetime_utc"] == snapshot_time, "underlying_price"]
        spot = float(spot_rows.iloc[0]) if not spot_rows.empty else float(
            rows["underlying_price"].iloc[-1]
        )

        snapshot["price_age_minutes"] = (
            (snapshot_time - snapshot["datetime_utc"]).dt.total_seconds() / 60.0
        )

        snapshot["datetime_utc"] = snapshot_time
        snapshot["trade_time"] = snapshot_time.time()
        snapshot["underlying_price"] = spot
        snapshot["distance_from_underlying"] = (snapshot["strike"] - spot).abs()

        is_itm = np.where(
            snapshot["option_type"] == "CE",
            snapshot["strike"] < spot,
            snapshot["strike"] > spot,
        )
        snapshot["moneyness"] = np.where(is_itm, "ITM", "OTM")
        atm_idx = snapshot.groupby(
            ["expiration_date", "option_type"], sort=False
        )["distance_from_underlying"].idxmin()
        snapshot.loc[atm_idx, "moneyness"] = "ATM"

        self._snapshot_cache[snapshot_time] = snapshot
        return snapshot
       

    def _get_ticker_series(self, day_df: pd.DataFrame, ticker: str):
        """Sorted numpy arrays for one ticker on the current day, cached
        so repeated legs/re-entries scanning the same contract don't
        re-filter day_df from scratch."""
        cached = self._ticker_series_cache.get(ticker)
        if cached is not None:
            return cached

        rows = day_df.loc[day_df["ticker"] == ticker]
        result = {
            "trade_time": rows["trade_time"].to_numpy(),
            "close": rows["close"].to_numpy(),
            "underlying_price": rows["underlying_price"].to_numpy(),
            "frame": rows,
        }
        self._ticker_series_cache[ticker] = result
        return result


    def _get_underlying_series(self, day_df: pd.DataFrame):
        if self._underlying_series_cache is not None:
            return self._underlying_series_cache

        dedup = day_df.drop_duplicates(subset="datetime_utc", keep="first")
        result = {
            "trade_time": dedup["trade_time"].to_numpy(),
            "underlying_price": dedup["underlying_price"].to_numpy(),
        }
        self._underlying_series_cache = result
        return result


    def _apply_overall_risk_management(self, day_df: pd.DataFrame, result: dict) -> dict:
        has_sl = bool(self.strategy.get("is_strategy_sl"))
        has_target = bool(self.strategy.get("is_strategy_target"))

        if not has_sl and not has_target:
            return result

        active_legs = [
            leg for leg in result["legs"]
            if leg.get("status") in ("ENTRY_DONE", "EXIT_DONE", "HELD_OVERNIGHT")
        ]
        if not active_legs:
            return result

        if self.strategy_type != "btst":
            return self._run_overall_risk_cycles(
                day_df, result, active_legs, has_sl, has_target, cutoff=self.exit_time
            )
            
        today_date = day_df["trade_date"].iloc[0] if not day_df.empty else None

        carryover_legs = [
            leg for leg in active_legs
            if today_date is not None and leg["entry_datetime"].date() < today_date
        ]
        carryover_ids = {id(leg) for leg in carryover_legs}
        fresh_legs = [leg for leg in active_legs if id(leg) not in carryover_ids]

        if carryover_legs:
            result = self._run_overall_risk_cycles(
                day_df, result, carryover_legs, has_sl, has_target, cutoff=self.exit_time
            )

        if fresh_legs:
            result = self._run_overall_risk_cycles(
                day_df, result, fresh_legs, has_sl, has_target, cutoff=self.day1_market_close
            )
        return result


    def _run_overall_risk_cycles(self, day_df: pd.DataFrame, result: dict, active_legs: list,
                                  has_sl: bool, has_target: bool, cutoff) -> dict:
        remaining_sl_reentries = (
            int(self.strategy.get("overall_reentry_sl_value"))
            if self.strategy.get("is_overall_reentry_sl") else 0
        )
        remaining_target_reentries = (
            int(self.strategy.get("overall_reentry_target_value"))
            if self.strategy.get("is_overall_reentry_target") else 0
        )

        cycle = 0
        while True:
            cycle += 1

            breach = self._find_overall_breach(day_df, active_legs, has_sl, has_target, cutoff)

            if breach is None:
                break  # combined MTM never breached this cycle

            breach_datetime, breach_reason, breach_mtm = breach

            self._truncate_legs_at_overall_breach(result, active_legs, day_df, breach_datetime, breach_reason)

            if self.strategy_type == "btst":
                for leg_result in active_legs:
                    if leg_result.get("status") == "EXIT_DONE":
                        for held_key, held_leg in list(self.held_from_previous_day.items()):
                            if held_leg is leg_result:
                                del self.held_from_previous_day[held_key]

            combined_pnl = round(sum(
                leg.get("pnl") or 0.0
                for leg in active_legs
                if leg.get("status") == "EXIT_DONE"
                and leg.get("exit_datetime") == breach_datetime
            ), 2)

            result.setdefault("overall_exits", []).append({
                "cycle": cycle,
                "exit_datetime": breach_datetime,
                "exit_reason": breach_reason,
                "combined_pnl": combined_pnl,
            })

            if breach_reason == "OVERALL_STOPLOSS":
                if remaining_sl_reentries <= 0:
                    break
                remaining_sl_reentries -= 1
                mode = self.strategy.get("overall_reentry_sl_type", "RE_ASAP")
            else:
                if remaining_target_reentries <= 0:
                    break
                remaining_target_reentries -= 1
                mode = self.strategy.get("overall_reentry_target_type", "RE_ASAP")

            if mode not in OVERALL_REENTRY_MODES:
                logger.warning(f"Unsupported overall reentry mode '{mode}'")
                break

            # active_legs are the positions just closed by this breach -- a
            # *_REVERSE mode re-enters on the opposite side of those.
            new_legs = self._reenter_all_legs(mode, breach_datetime, day_df, active_legs)
            if not new_legs:
                break

            if self.strategy_type == "btst":
                for rl in new_legs:
                    rl["is_held_overnight"] = (rl.get("status") == "HELD_OVERNIGHT")
                    if rl.get("status") == "HELD_OVERNIGHT":
                        rl["_trade_date"] = rl["entry_datetime"].date()
                        rl_key = f"leg_{rl['leg']}_{rl['entry_datetime'].isoformat()}"
                        self.held_from_previous_day[rl_key] = rl

            result["legs"].extend(new_legs)
            active_legs = new_legs

        return result
    
    
    def _overall_threshold_series(self, entry_value: np.ndarray, has_sl: bool, has_target: bool):
        """(sl_threshold, target_threshold) as per-bar arrays.

        POINTS/MTM are absolute, so they come out constant. PERCENT is a share
        of the capital currently in the position, which is why it has to track
        `entry_value` bar by bar rather than being fixed up front: Simple
        Momentum staggers leg entries, so the position -- and therefore the
        threshold -- grows as legs join. Fixing it from the full leg set let a
        leg that had not entered yet raise the bar it was not part of, pushing
        the exit later (AlgoTest closes on the legs open at that instant).

        The value is money, matching combined_mtm (priced x QUANTITY x
        lot_size); summing raw premium points made it ~20x too small.
        """
        def build(threshold_type, value):
            if not has_sl and not has_target:
                return None
            if threshold_type in ("PERCENT", "TOTAL_PREMIUM_PERCENT") and value is not None:
                return np.abs(entry_value) * abs(value) / 100.0
            scalar = self._calc_overall_threshold(threshold_type, value, 0.0)
            return None if scalar is None else np.full(len(entry_value), scalar, dtype=float)

        sl_threshold = build(
            self.strategy.get("strategy_sl_type"), self.strategy.get("strategy_sl_value")
        ) if has_sl else None
        target_threshold = build(
            self.strategy.get("strategy_target_type"), self.strategy.get("strategy_target_value")
        ) if has_target else None
        return sl_threshold, target_threshold


    def _calc_overall_threshold(self, threshold_type, value, total_entry_value: float):
        """'POINTS'/'MTM'   -> absolute combined threshold, taken as given.
        'PERCENT'/'TOTAL_PREMIUM_PERCENT' -> % of the combined ENTRY VALUE of
        all legs (premium x QUANTITY x lot_size), so the result is money and
        comparable with combined_mtm."""
        if threshold_type is None or value is None:
            return None
        if threshold_type in ("POINTS", "MTM"):
            return abs(value)
        elif threshold_type in ("PERCENT", "TOTAL_PREMIUM_PERCENT"):
            return round(abs(total_entry_value) * value / 100, 2)

        logger.warning(f"Unsupported overall SL/target type '{threshold_type}'")
        return None


    def _calc_overall_trailing_sl_series(self, combined_mtm: np.ndarray, sl_threshold):
        if not self.strategy.get("is_overall_trail_sl") or sl_threshold is None:
            return sl_threshold

        trail_sl_type = self.strategy.get("overall_trail_sl_type")
        instrument_moves = self.strategy.get("overall_instrument_move")
        stoploss_moves = self.strategy.get("overall_stoploss_move")

        if trail_sl_type is None or not instrument_moves or stoploss_moves is None:
            logger.warning(
                "is_overall_trail_sl=True but overall_trail_sl_type/"
                "overall_instrument_move/overall_stoploss_move missing -- "
                "skipping overall trailing"
            )
            return sl_threshold

        # MTM behaves like POINTS here: both give the step and the lock as
        # absolute amounts ("every 1000 of profit, tighten the stop by 200"),
        # and combined_mtm is already money. Without MTM in this branch the
        # entire overall-trailing block was skipped as unsupported.
        if trail_sl_type in ("POINTS", "MTM"):
            step_move = instrument_moves
            step_gain = stoploss_moves
        elif trail_sl_type == "PERCENT":
            step_move = round(sl_threshold * instrument_moves / 100, 2)
            step_gain = round(sl_threshold * stoploss_moves / 100, 2)
        else:
            logger.warning(f"Unsupported overall_trail_sl_type '{trail_sl_type}'")
            return sl_threshold

        if not step_move or step_move <= 0:
            return sl_threshold

        best_so_far = np.maximum.accumulate(combined_mtm)
        favorable_move = np.clip(best_so_far, a_min=0, a_max=None)
        steps = np.floor(favorable_move / step_move)

        return np.clip(sl_threshold - steps * step_gain, a_min=0, a_max=None)


    def _find_overall_breach(self, day_df: pd.DataFrame, active_legs: list, has_sl: bool, has_target: bool, cutoff):
        if not active_legs:
            return None

        start_dt = min(leg["entry_datetime"] for leg in active_legs)
        tickers = {leg["ticker"] for leg in active_legs}

        subset = day_df.loc[
            (day_df["ticker"].isin(tickers))
            & (day_df["datetime_utc"] >= start_dt)
            & (day_df["trade_time"] <= cutoff)
        ]
        if subset.empty:
            return None

        pivot = subset.pivot_table(index="datetime_utc", columns="ticker", values="close", aggfunc="last")
        pivot = pivot.sort_index().ffill()

        combined_mtm = pd.Series(0.0, index=pivot.index)
        # Capital in the position bar by bar -- a leg only counts once it has
        # entered, so a PERCENT threshold grows as staggered legs join.
        entry_value = pd.Series(0.0, index=pivot.index)
        today_date = day_df["trade_date"].iloc[0] if not day_df.empty else None

        for leg in active_legs:
            if leg["ticker"] not in pivot.columns:
                continue

            direction = 1 if leg["position"] == "BUY" else -1
            lot_size = self._lot_size_by_leg.get(leg["leg"], 1)
            price_series = pivot[leg["ticker"]]
            
            if leg.get("status") == "EXIT_DONE" and leg.get("exit_datetime") is not None:
                # Leg already exited - freeze P&L at exit price
                price_series = price_series.where(price_series.index <= leg["exit_datetime"], leg["exit_price"])
            elif leg.get("status") == "HELD_OVERNIGHT":
                # Leg is held overnight - continues to contribute live prices (Day-2)
                pass

            leg_contribution = (direction * (price_series - leg["entry_price"]) * QUANTITY * lot_size)
            leg_contribution = leg_contribution.where(pivot.index > leg["entry_datetime"], 0.0)
            combined_mtm = combined_mtm + leg_contribution
            entry_value = entry_value + pd.Series(
                leg["entry_price"] * QUANTITY * lot_size, index=pivot.index
            ).where(pivot.index >= leg["entry_datetime"], 0.0)

        live = combined_mtm.notna()
        combined_mtm = combined_mtm[live]
        entry_value = entry_value[live]
        if combined_mtm.empty:
            return None

        values = combined_mtm.to_numpy()
        timestamps = combined_mtm.index.to_numpy()

        sl_threshold, target_threshold = self._overall_threshold_series(
            entry_value.to_numpy(), has_sl, has_target
        )

        trailing_sl_threshold = (
            self._calc_overall_trailing_sl_series(values, sl_threshold)
            if sl_threshold is not None else None
        )

        sl_hit_mask = values <= -trailing_sl_threshold if trailing_sl_threshold is not None else np.zeros(len(values), dtype=bool)
        target_hit_mask = values >= target_threshold if target_threshold is not None else np.zeros(len(values), dtype=bool)
        # Nothing can breach while the position is empty -- before the first
        # leg opens both the MTM and a PERCENT threshold are 0, and 0 >= 0
        # would read as an instant target hit.
        has_position = entry_value.to_numpy() > 0
        hit_mask = (sl_hit_mask | target_hit_mask) & has_position

        if not hit_mask.any():
            return None

        idx = int(hit_mask.argmax())
        reason = "OVERALL_STOPLOSS" if sl_hit_mask[idx] else "OVERALL_TARGET"
        return pd.Timestamp(timestamps[idx]), reason, round(float(values[idx]), 2)


    def _truncate_legs_at_overall_breach(self, result: dict, active_legs: list, day_df: pd.DataFrame, breach_datetime, breach_reason: str) -> None:
        """Once the overall SL/target fires, the strategy is flat as of the
        breach candle: every leg still open is force-closed there, and any
        entry that would only have happened AFTER the breach (delayed
        momentum / range-breakout fills, per-leg re-entries) is CANCELLED
        outright -- no fresh leg may enter once the overall exit has
        triggered. Only legs spawned by an OVERALL re-entry cycle trade on
        after this instant (they arrive as the next cycle's active_legs and
        are judged against that cycle's own breach)."""
        active_ids = {id(leg) for leg in active_legs}

        cancelled_ids = set()
        for leg_result in active_legs:
            entry_dt = leg_result.get("entry_datetime")
            if entry_dt is not None and entry_dt > breach_datetime:
                cancelled_ids.add(id(leg_result))

        kept_legs = []
        for leg_result in result["legs"]:
            if id(leg_result) in cancelled_ids:
                continue
            entry_dt = leg_result.get("entry_datetime")
            if entry_dt is not None and entry_dt > breach_datetime and id(leg_result) not in active_ids:
                continue
            kept_legs.append(leg_result)
        result["legs"] = kept_legs

        # A cancelled BTST leg may already be registered for overnight
        # carryover -- drop it there too, it never entered.
        if cancelled_ids and self.strategy_type == "btst":
            for held_key, held_leg in list(self.held_from_previous_day.items()):
                if id(held_leg) in cancelled_ids:
                    del self.held_from_previous_day[held_key]

        exit_row_cache = {}
        for leg_result in active_legs:
            if id(leg_result) in cancelled_ids:
                continue

            if leg_result.get("exit_datetime") is not None:
                if leg_result["exit_datetime"] <= breach_datetime:
                    continue

            ticker = leg_result["ticker"]
            if ticker not in exit_row_cache:
                candles = day_df.loc[
                    (day_df["ticker"] == ticker) & (day_df["datetime_utc"] <= breach_datetime)
                ].sort_values("datetime_utc")
                exit_row_cache[ticker] = candles.iloc[-1] if not candles.empty else None

            exit_row = exit_row_cache[ticker]
            if exit_row is None:
                continue

            direction = 1 if leg_result["position"] == "BUY" else -1
            lot_size = self._lot_size_by_leg.get(leg_result["leg"], 1)
            leg_result["exit_datetime"] = exit_row["datetime_utc"]
            leg_result["exit_price"] = round(float(exit_row["close"]), 2)
            leg_result["underlying_exit_price"] = self._underlying_at(exit_row)
            leg_result["exit_reason"] = breach_reason
            leg_result["status"] = "EXIT_DONE"
            leg_result["is_held_overnight"] = False  
            leg_result["quantity_multiplier"] = QUANTITY * lot_size
            leg_result["pnl"] = round((leg_result["exit_price"] - leg_result["entry_price"]) * QUANTITY * lot_size * direction, 2)


    def _reenter_all_legs(self, mode: str, breach_datetime, day_df: pd.DataFrame,
                          exited_legs: list | None = None) -> list:
        chain_snapshot = self._get_chain_snapshot_at(day_df, breach_datetime)
        if chain_snapshot is None:
            return []

        reverse = mode.endswith("_REVERSE")
        held_position = {
            leg["leg"]: leg["position"]
            for leg in (exited_legs or [])
            if leg.get("leg") is not None and leg.get("position") is not None
        }
        new_legs = []

        for leg_meta in self.legs_meta:
            base_leg_meta = self._effective_leg_meta(leg_meta)
            lot_size = base_leg_meta.get("lot_size", 1)
            leg_chain = self._filter_leg_chain(chain_snapshot, base_leg_meta)
            if leg_chain.empty:
                continue

            strike_row = self.select_strike(base_leg_meta, leg_chain)
            if strike_row is None:
                continue

            previous = held_position.get(base_leg_meta["leg_number"], base_leg_meta["position_type"])
            position_type = self._flip_position(previous) if reverse else previous

            if mode.startswith("RE_MOMENTUM") and base_leg_meta.get("is_simple_momentum"):
                fill_row = self._resolve_momentum_fill(base_leg_meta, day_df, strike_row)
                if fill_row is None:
                    continue
            else:
                fill_row = strike_row

            entry_result = self._build_entry_result(
                base_leg_meta, position_type, strike_row, fill_row, is_reentry=True, reentry_mode=f"OVERALL_{mode}",
            )

            if self.strategy_type == "btst":
                entry_result["status"] = "HELD_OVERNIGHT"
                entry_result["exit_datetime"] = None
                entry_result["exit_price"] = None
                entry_result["underlying_exit_price"] = None
                entry_result["exit_reason"] = None
                entry_result["quantity_multiplier"] = QUANTITY * lot_size
                entry_result["pnl"] = None
                entry_result["is_held_overnight"] = True
                entry_result["_trade_date"] = entry_result["entry_datetime"].date()
            else:
                exit_eval_meta = base_leg_meta
                if reverse:
                    exit_eval_meta = dict(base_leg_meta)
                    exit_eval_meta["position_type"] = position_type

                entry_result = self.evaluate_leg_exit(exit_eval_meta, entry_result, day_df)

                if entry_result.get("status") == "EXIT_DONE":
                    direction = 1 if entry_result["position"] == "BUY" else -1
                    entry_result["quantity_multiplier"] = QUANTITY * lot_size
                    entry_result["pnl"] = round(
                        (entry_result["exit_price"] - entry_result["entry_price"]) * QUANTITY * lot_size * direction, 2
                    )
                else:
                    entry_result["pnl"] = None
                entry_result["is_held_overnight"] = False

            entry_result["is_reentry"] = True
            entry_result["reentry_mode"] = f"OVERALL_{mode}"
            new_legs.append(entry_result)

        return new_legs


    def _flip_position(self, position_type: str) -> str:
        return "SELL" if position_type == "BUY" else "BUY"