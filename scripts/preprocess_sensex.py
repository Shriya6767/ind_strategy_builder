"""One-time batch preprocessor for Sensex 1-min data.

Reads the raw daily spot + F&O parquet files, derives every column the
backtest engine needs (strike, option_type, dte, underlying_price,
distance_from_underlying, moneyness), and writes ONE merged file per
trading day, mirroring the raw folder layout:

    <SENSEX_PROCESSED_PATH>/<YYYY>/<MON_YYYY>/SENSEX_MERGED_DDMMYYYY.parquet

Processes one day at a time (a day is ~85K option rows), so memory stays
flat no matter how many years are converted. Days whose output file
already exists are skipped, so the script is safe to re-run and resumes
where it left off; pass --overwrite to rebuild them.

Usage (from repo root):
    venv\\Scripts\\python.exe scripts\\preprocess_sensex.py
    venv\\Scripts\\python.exe scripts\\preprocess_sensex.py --start 2025-01-01 --end 2025-12-31
    venv\\Scripts\\python.exe scripts\\preprocess_sensex.py --overwrite
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.modules import re, pd, np
from src.core.config import SENSEX_SPOT_PATH, SENSEX_FNO_PATH, SENSEX_PROCESSED_PATH

# SENSEX01JUL2573200PE.BFO -> expiry 01JUL25, strike 73200, type PE.
# Futures tickers (SENSEX22JUL25FUT.BFO, SENSEX-I.BFO) don't match and are dropped.
OPTION_TICKER_PATTERN = re.compile(r"^SENSEX(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)\.BFO$")
FNO_FILE_PATTERN = re.compile(r"^BFO_BACKADJUSTED_(\d{2})(\d{2})(\d{4})\.parquet$")

SPOT_TICKER = "SENSEX.BSE_IDX"

RAW_COLUMNS = ["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume"]

FINAL_COLUMNS = [
    "datetime_utc",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "expiration_date",
    "option_type",
    "strike",
    "dte",
    "underlying_price",
    "underlying",
    "distance_from_underlying",
    "moneyness",
]


def build_datetime(df: pd.DataFrame) -> pd.Series:
    """Date (DD/MM/YYYY string) + Time (datetime.time) -> one timestamp per
    bar, labeled by the bar's COMPLETION minute: the raw 09:15:59 bar covers
    09:15:00-09:15:59, so it is labeled 09:16:00 -- the first moment its
    close price actually exists. This makes every engine time comparison
    look-ahead free: 'entry 09:20' acts on the candle completed at 09:19:59,
    'exit 15:15' on the candle completed at 15:14:59. Session labels run
    09:16:00-15:30:00. All timestamps are IST wall-clock; the column is
    named datetime_utc because the backtest engine expects that name."""
    time_map = {
        t: pd.Timedelta(hours=t.hour, minutes=t.minute + 1)
        for t in df["Time"].unique()
    }
    dates = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    return dates + df["Time"].map(time_map)


def parse_option_tickers(tickers) -> pd.DataFrame:
    rows = []
    for ticker in tickers:
        match = OPTION_TICKER_PATTERN.match(ticker)
        if not match:
            continue
        expiry_str, strike_str, option_type = match.groups()
        rows.append((ticker, expiry_str, int(strike_str), option_type))

    if not rows:
        raise ValueError("No option tickers matched the SENSEX option pattern.")

    meta = pd.DataFrame(rows, columns=["Ticker", "expiration_date", "strike", "option_type"])
    meta["expiration_date"] = pd.to_datetime(meta["expiration_date"], format="%d%b%y")
    return meta


def process_day(spot_path: str, fno_path: str) -> pd.DataFrame:
    """Merges one day's spot + option chain and derives the engine columns."""
    spot = pd.read_parquet(spot_path, columns=["Ticker", "Date", "Time", "Close"])
    spot = spot[spot["Ticker"] == SPOT_TICKER]
    if spot.empty:
        raise ValueError(f"No '{SPOT_TICKER}' rows in {spot_path}")
    spot = spot.assign(datetime_utc=build_datetime(spot))
    spot = (
        spot[["datetime_utc", "Close"]]
        .rename(columns={"Close": "underlying_price"})
        .drop_duplicates("datetime_utc")
    )

    fno = pd.read_parquet(fno_path, columns=RAW_COLUMNS)
    # Parse only the unique tickers (~600/day), then map onto all rows.
    # Inner merge drops futures rows (FUT / SENSEX-I) in the same step.
    ticker_meta = parse_option_tickers(fno["Ticker"].unique())
    fno = fno.merge(ticker_meta, on="Ticker", how="inner")
    fno = fno.assign(datetime_utc=build_datetime(fno))

    df = fno.merge(spot, on="datetime_utc", how="inner")

    df["dte"] = (df["expiration_date"] - df["datetime_utc"].dt.normalize()).dt.days.astype("int16")
    df["distance_from_underlying"] = (df["strike"] - df["underlying_price"]).abs()

    # Moneyness: CE below spot / PE above spot = ITM, the reverse = OTM,
    # and per (minute, expiry, option_type) the strike nearest to spot
    # becomes ATM -- the engine's ITM-n/OTM-n selection sorts on
    # distance_from_underlying within these labels.
    is_itm = np.where(
        df["option_type"] == "CE",
        df["strike"] < df["underlying_price"],
        df["strike"] > df["underlying_price"],
    )
    df["moneyness"] = np.where(is_itm, "ITM", "OTM")
    atm_idx = df.groupby(
        ["datetime_utc", "expiration_date", "option_type"], sort=False
    )["distance_from_underlying"].idxmin()
    df.loc[atm_idx, "moneyness"] = "ATM"

    df["underlying"] = "SENSEX"
    df = df.rename(columns={
        "Ticker": "ticker",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    df["strike"] = df["strike"].astype("int32")
    df["volume"] = df["volume"].astype("int32")
    return df[FINAL_COLUMNS].sort_values("datetime_utc", ignore_index=True)


def find_trading_days() -> list[tuple[pd.Timestamp, str, str, str]]:
    """Walks the raw F&O tree and returns (date, year_dir, month_dir, filename)
    for every daily file found, sorted by date."""
    days = []
    for year in sorted(os.listdir(SENSEX_FNO_PATH)):
        year_path = os.path.join(SENSEX_FNO_PATH, year)
        if not os.path.isdir(year_path):
            continue
        for month_dir in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month_dir)
            if not os.path.isdir(month_path):
                continue
            for fname in sorted(os.listdir(month_path)):
                match = FNO_FILE_PATTERN.match(fname)
                if not match:
                    continue
                dd, mm, yyyy = match.groups()
                days.append((pd.Timestamp(f"{yyyy}-{mm}-{dd}"), year, month_dir, fname))
    days.sort(key=lambda d: d[0])
    return days


def main():
    parser = argparse.ArgumentParser(description="Convert raw Sensex daily files to merged engine-ready files.")
    parser.add_argument("--start", help="Only convert days on/after this date (YYYY-MM-DD).")
    parser.add_argument("--end", help="Only convert days on/before this date (YYYY-MM-DD).")
    parser.add_argument("--overwrite", action="store_true", help="Rebuild days whose output already exists.")
    args = parser.parse_args()

    if not (SENSEX_SPOT_PATH and SENSEX_FNO_PATH and SENSEX_PROCESSED_PATH):
        sys.exit("SENSEX_SPOT_PATH, SENSEX_FNO_PATH and SENSEX_PROCESSED_PATH must be set in .env")

    start = pd.Timestamp(args.start) if args.start else None
    end = pd.Timestamp(args.end) if args.end else None

    days = find_trading_days()
    if start is not None:
        days = [d for d in days if d[0] >= start]
    if end is not None:
        days = [d for d in days if d[0] <= end]

    print(f"Found {len(days)} raw trading day(s) to consider.")

    converted = skipped = missing_spot = failed = 0
    t0 = time.perf_counter()

    for i, (date, year, month_dir, fno_name) in enumerate(days, start=1):
        stamp = date.strftime("%d%m%Y")
        fno_path = os.path.join(SENSEX_FNO_PATH, year, month_dir, fno_name)
        spot_path = os.path.join(SENSEX_SPOT_PATH, year, month_dir, f"BSE_INDICES_{stamp}.parquet")
        out_dir = os.path.join(SENSEX_PROCESSED_PATH, year, month_dir)
        out_path = os.path.join(out_dir, f"SENSEX_MERGED_{stamp}.parquet")

        if os.path.exists(out_path) and not args.overwrite:
            skipped += 1
            continue

        if not os.path.exists(spot_path):
            print(f"  WARN {date.date()}: no spot file, day skipped.")
            missing_spot += 1
            continue

        try:
            df = process_day(spot_path, fno_path)
            os.makedirs(out_dir, exist_ok=True)
            df.to_parquet(out_path, index=False)
            converted += 1
        except Exception as exc:
            print(f"  FAIL {date.date()}: {exc}")
            failed += 1
            continue

        if converted % 50 == 0:
            rate = converted / (time.perf_counter() - t0)
            print(f"  ... {i}/{len(days)} days done ({rate:.1f} days/s)")

    elapsed = time.perf_counter() - t0
    print(
        f"\nDone in {elapsed:.1f}s -- converted {converted}, "
        f"already existed {skipped}, missing spot {missing_spot}, failed {failed}."
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
