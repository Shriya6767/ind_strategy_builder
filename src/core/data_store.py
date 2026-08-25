from src.core.modules import pd

class DataStore:
    historical_df: pd.DataFrame | None = None

    @classmethod
    def set_df(cls, df: pd.DataFrame):
        cls.historical_df = df

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        if cls.historical_df is None:
            raise ValueError("Historical data is not loaded.")
        return cls.historical_df

    @classmethod
    def clear_df(cls):
        cls.historical_df = None