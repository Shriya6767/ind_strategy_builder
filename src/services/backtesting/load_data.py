from src.core.modules import os, pd, ds, timedelta
from src.core.config import SENSEX_PROCESSED_FULL_PATH
from src.core.data_store import DataStore
from src.core.logger import get_logger

logger = get_logger(__name__)

MONTH_ABBR = ("", "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
              "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

REQUIRED_COLUMNS = [
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
    "underlying_open",
    "underlying_high",
    "underlying_low",
    "underlying",
    "distance_from_underlying",
    "moneyness",
]


class DataLoader:
    """Loads pre-merged Sensex 1-min option-chain data for a date range and
    stores the DataFrame in DataStore.
    The heavy work (spot+options merge, ticker parse, dte / moneyness /
    distance derivation) is done ONCE by scripts/preprocess_sensex.py, which
    writes one engine-ready file per trading day:
    """

    def __init__(self, symbol: str = "sensex"):
        if str(symbol).lower() != "sensex":
            raise ValueError("Only symbol 'sensex' is supported.")
        if not SENSEX_PROCESSED_FULL_PATH:
            raise ValueError("SENSEX_PROCESSED_FULL_PATH must be set in .env")
        self.symbol = "sensex"


    def load(self, start_date: str, end_date: str) -> int:
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)

        if start_dt > end_dt:
            raise ValueError("start_date must be <= end_date")

        files = self._get_required_files(start_dt, end_dt)
        logger.info(f"Trading days found: {len(files)}")

        dataset = ds.dataset(files, format="parquet")
        df = dataset.to_table(columns=REQUIRED_COLUMNS).to_pandas()
        DataStore.set_df(df)
        rows = len(df)
        logger.info(
            f"Loaded {rows:,} option rows with {len(df.columns)} columns "
            f"from {len(files)} trading day(s)."
        )
        return rows


    def _get_required_files(self, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> list[str]:
        """One merged file per trading day; days with no file (weekends,
        holidays) are skipped."""
        files = []

        current = start_dt.normalize()
        while current <= end_dt:
            month_dir = f"{MONTH_ABBR[current.month]}_{current.year}"
            path = os.path.join(
                SENSEX_PROCESSED_FULL_PATH, str(current.year), month_dir,
                f"SENSEX_MERGED_FULL_{current.strftime('%d%m%Y')}.parquet"
            )
            if os.path.exists(path):
                files.append(path)
            current += timedelta(days=1)

        if not files:
            raise FileNotFoundError(
                f"No merged sensex files found between {start_dt.date()} and "
                f"{end_dt.date()} -- run scripts/preprocess_sensex.py first."
            )
        return files