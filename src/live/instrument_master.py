"""The XTS contract master, reduced to the Sensex option chain.

Downloaded once per day (cached to disk), parsed into a few thousand
`Contract` records -- (expiry, strike, CE/PE) -> exchangeInstrumentID -- and
an expiry calendar that resolves the strategy's `weekly` / `next_weekly` /
`monthly` / `next_monthly` labels with the SAME rules as the backtest engine
(`BacktestEngine._build_day_expiry_map`).
"""
from src.core.modules import os, np, dataclasses, date, datetime, timedelta
from src.core.logger import get_logger
from src.live.xts_client import XTSMarketDataClient, SEG_BSECM, SEG_BSEFO

logger = get_logger(__name__)


@dataclasses.dataclass(slots=True, frozen=True)
class Contract:
    instrument_id: int
    expiry: date
    strike: int
    option_type: str        # CE | PE
    lot_size: int
    tick_size: float
    freeze_qty: int
    symbol: str             # broker's trading symbol / description
    segment: int = SEG_BSEFO


class InstrumentMaster:
    UNDERLYING = "SENSEX"
    STRIKE_STEP = 100          # BSE lists Sensex strikes every 100 points

    def __init__(self, contracts: list[Contract], index_instrument_id: int | None, as_of: date):
        self.as_of = as_of
        self.index_instrument_id = index_instrument_id
        self.by_key: dict[tuple[date, int, str], Contract] = {}
        self.by_id: dict[int, Contract] = {}
        strikes: dict[tuple[date, str], set[int]] = {}
        lots: dict[int, int] = {}
        for c in contracts:
            self.by_key[(c.expiry, c.strike, c.option_type)] = c
            self.by_id[c.instrument_id] = c
            strikes.setdefault((c.expiry, c.option_type), set()).add(c.strike)
            lots[c.lot_size] = lots.get(c.lot_size, 0) + 1
        self._strikes = {k: np.array(sorted(v), dtype=np.int64) for k, v in strikes.items()}
        self.expiries: list[date] = sorted({c.expiry for c in contracts})
        self.lot_size: int = max(lots, key=lots.get) if lots else 20
        # month -> last expiry listed in that month (the monthly contract)
        self._monthly: dict[tuple[int, int], date] = {}
        for e in self.expiries:
            self._monthly[(e.year, e.month)] = e


    def contract(self, expiry: date, strike: int, option_type: str) -> Contract | None:
        return self.by_key.get((expiry, int(strike), option_type))


    def strikes(self, expiry: date, option_type: str) -> np.ndarray:
        return self._strikes.get((expiry, option_type), np.empty(0, dtype=np.int64))


    def ladder_step(self, expiry: date, option_type: str) -> int:
        """Sensex strikes are listed every STRIKE_STEP points. Hardcoded on
        purpose (no per-call derivation); change the constant if BSE changes it."""
        return self.STRIKE_STEP


    def resolve_expiries(self, today: date, exclude_same_day: bool = False) -> dict[str, date]:
        """weekly / next_weekly: nearest listed expiries; monthly / next_monthly:
        last expiry of the month, rolling once it has passed. BTST passes
        exclude_same_day so an overnight hold never expires on day 1."""
        min_date = today + timedelta(days=1 if exclude_same_day else 0)
        upcoming = [e for e in self.expiries if e >= min_date]
        out: dict[str, date] = {}
        if upcoming:
            out["weekly"] = upcoming[0]
            if len(upcoming) > 1:
                out["next_weekly"] = upcoming[1]
        monthlies = [e for _, e in sorted(self._monthly.items()) if e >= min_date]
        if monthlies:
            out["monthly"] = monthlies[0]
            if len(monthlies) > 1:
                out["next_monthly"] = monthlies[1]
        return out


    @classmethod
    def parse(cls, text: str, underlying: str = UNDERLYING) -> list[Contract]:
        """Options rows of the master (InstrumentType 2):
        ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|
        NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|
        LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|
        StrikePrice|OptionType|DisplayName|..."""
        contracts: list[Contract] = []
        underlying = underlying.upper()
        for line in text.splitlines():
            if not line or "|" not in line:
                continue
            p = line.split("|")
            if len(p) < 19:
                continue
            if p[2].strip() not in ("2", "Options"):
                continue
            if p[3].strip().upper() != underlying:
                continue
            opt = p[18].strip().upper()
            opt = {"3": "CE", "4": "PE"}.get(opt, opt)
            if opt not in ("CE", "PE"):
                continue
            try:
                expiry = datetime.fromisoformat(p[16].strip()[:19]).date()
                contracts.append(Contract(
                    instrument_id=int(p[1]),
                    expiry=expiry,
                    strike=int(round(float(p[17]))),
                    option_type=opt,
                    lot_size=int(float(p[12]) or 0) or 20,
                    tick_size=float(p[11] or 0.05) or 0.05,
                    freeze_qty=int(float(p[10]) or 0),
                    symbol=(p[19] if len(p) > 19 and p[19] else p[4]).strip(),
                    segment=int(p[0]) if p[0].strip().isdigit() else SEG_BSEFO,
                ))
            except (ValueError, IndexError):
                continue
        return contracts


    @staticmethod
    def parse_index_id(index_list: list[str], name: str = UNDERLYING) -> int | None:
        """indexlist entries look like 'NIFTY 50_26000' -> 26000."""
        for entry in index_list:
            label, _, ident = str(entry).rpartition("_")
            if label.strip().upper() == name.upper() and ident.strip().isdigit():
                return int(ident)
        return None


    @classmethod
    async def load(cls, md: XTSMarketDataClient, cache_dir: str, today: date,
                   index_id_override: int = 0) -> "InstrumentMaster":
        os.makedirs(cache_dir, exist_ok=True)
        path = os.path.join(cache_dir, f"xts_master_BSEFO_{today:%Y%m%d}.txt")
        text = None
        if os.path.exists(path) and os.path.getsize(path) > 1000:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
            logger.info(f"[MASTER] loaded cached master {path}")
        if text is None:
            text = await md.master(["BSEFO"])
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            logger.info(f"[MASTER] downloaded BSEFO master ({len(text) // 1024} KB) -> {path}")
        contracts = cls.parse(text)
        if not contracts:
            raise RuntimeError("Contract master has no SENSEX option rows -- check the market data login / segment.")

        index_id = index_id_override or None
        if not index_id:
            try:
                index_id = cls.parse_index_id(await md.index_list(SEG_BSECM))
            except Exception as e:
                logger.warning(f"[MASTER] index list lookup failed: {e}")
        if not index_id:
            logger.warning("[MASTER] SENSEX index instrument id not found -- set LIVE_SENSEX_INDEX_ID; "
                           "underlying-based rules will be unavailable")
        master = cls(contracts, index_id, today)
        logger.info(f"[MASTER] {len(contracts)} SENSEX option contracts, expiries "
                    f"{master.expiries[:4]}..., lot size {master.lot_size}, index id {index_id}")
        return master