from src.core.modules import os, pd, ds, month_name
from src.core.config import BASE_PATH
from src.core.data_store import DataStore
from src.core.logger import get_logger

logger = get_logger(__name__)

class DataLoader:

    REQUIRED_COLUMNS = [
        "datetime_utc",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "expiration_date",
        "option_type",
        "strike",
        "dte",
        "underlying_price",
        "underlying",
        "distance_from_underlying",
        "moneyness"
    ]

    def __init__(self, dte_type: str = "0dte"):

        if dte_type not in ("0dte", "1dte", "combined_dte"):
            raise ValueError("dte_type must be '0dte', '1dte' or 'combined_dte'")  

        self.dte_type = dte_type
        
        if dte_type == "combined_dte":
            self.folder_path = os.path.join(BASE_PATH, "processed_btst")
        else:
            self.folder_path = os.path.join(
                BASE_PATH,
                f"processed_{dte_type}"
            )

    def _get_required_files(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> list[str]:
        current = start_date.replace(day=1)
        end = end_date.replace(day=1)

        files = []

        while current <= end:

            month = month_name[current.month].lower()
            year = current.year

            if self.dte_type == "combined_dte":
                filename = f"spx_btst_{month}{year}_merged.parquet"
                path = os.path.join(self.folder_path, filename)
            else:
                filename = (
                    f"spx_{self.dte_type}_{month}{year}"
                    "_complete_processed.parquet"
                )

                path = os.path.join(self.folder_path, filename)

                if not os.path.exists(path):
                    filename = (
                        f"spx_{self.dte_type}_{month}{year}"
                        "_complete_utc_processed.parquet"
                    )

                    path = os.path.join(self.folder_path, filename)

            if os.path.exists(path):
                files.append(path)

            current += pd.DateOffset(months=1)

        if not files:
            raise FileNotFoundError(
                f"No parquet files found between "
                f"{start_date.date()} and {end_date.date()}"
            )

        return files

    def load(self, start_date: str, end_date: str) -> pd.DataFrame:

        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)

        if start_dt > end_dt:
            raise ValueError("start_date must be <= end_date")

        logger.info(
            f"Loading {self.dte_type} data "
            f"from {start_dt.date()} to {end_dt.date()}"
        )

        files = self._get_required_files(start_dt, end_dt)

        logger.info(f"Files selected: {len(files)}")

        dataset = ds.dataset(
            files,
            format="parquet"
        )

        if "datetime" not in dataset.schema.names:
            raise ValueError("Column 'datetime' not found in parquet files.")

        table = dataset.to_table(
            columns=self.REQUIRED_COLUMNS,
            filter=(
                (ds.field("datetime") >= start_dt)
                &
                (ds.field("datetime") < end_dt + pd.Timedelta(days=1))
            )
        )

        df = table.to_pandas()
        DataStore.set_df(df)
        rows = len(df)

        logger.info(
            f"Loaded {rows:,} rows "
            f"with {len(df.columns)} columns "
            f"from {len(files)} file(s)."
        )
        return rows