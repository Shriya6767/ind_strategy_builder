from src.core.modules import pd

class DataStore:
    historical_df: pd.DataFrame | None = None
    loaded_range: tuple[str, str] | None = None

    @classmethod
    def set_df(cls, df: pd.DataFrame, start_date: str | None = None, end_date: str | None = None):
        cls.historical_df = df
        cls.loaded_range = (start_date, end_date) if start_date and end_date else None

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        if cls.historical_df is None:
            raise ValueError("Historical data is not loaded.")
        return cls.historical_df

    @classmethod
    def covers(cls, start_date: str, end_date: str) -> bool:
        """True when a frame for exactly this range is already loaded."""
        return cls.historical_df is not None and cls.loaded_range == (start_date, end_date)

    @classmethod
    def clear_df(cls):
        cls.historical_df = None
        cls.loaded_range = None