from src.core.modules import pd, threading


class DataStore:
    """ONE resident market-data frame per process, loaded at startup for
    the whole available range (or DATA_PRELOAD_RANGE) and never replaced
    by user requests. Every backtest works on a SLICE of it bounded by the
    strategy's own start/end dates: `bounds()` finds the row range with two
    binary searches on the time-sorted datetime_utc column and `iloc`
    hands out a view -- no copy, no reload, and two users asking for
    different dates never touch each other's data.

    The frame is time-sorted (daily files concatenated in date order) and
    carries the engine's derived columns, so slices are engine-ready and
    forked portfolio workers inherit them copy-on-write."""
    historical_df: pd.DataFrame | None = None
    loaded_range: tuple[str, str] | None = None
    _lock = threading.Lock()

    @classmethod
    def set_df(cls, df: pd.DataFrame, start_date: str | None = None, end_date: str | None = None):
        with cls._lock:
            cls.historical_df = df
            cls.loaded_range = (start_date, end_date) if start_date and end_date else None

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        df = cls.historical_df
        if df is None:
            raise ValueError("Market data is not loaded yet.")
        return df

    @classmethod
    def is_loaded(cls) -> bool:
        return cls.historical_df is not None

    @classmethod
    def covers(cls, start_date: str, end_date: str) -> bool:
        """True when the resident frame spans this whole range."""
        if cls.historical_df is None or cls.loaded_range is None:
            return False
        lo, hi = cls.loaded_range
        return lo <= str(start_date)[:10] and str(end_date)[:10] <= hi

    @classmethod
    def bounds(cls, start_date, end_date) -> tuple[int, int]:
        """Row range [lo, hi) of the resident frame covering the calendar
        days start_date..end_date inclusive. O(log n)."""
        stamps = cls.get_df()["datetime_utc"]
        start = pd.Timestamp(start_date, tz="UTC").normalize()
        end = pd.Timestamp(end_date, tz="UTC").normalize() + pd.Timedelta(days=1)
        lo = int(stamps.searchsorted(start, side="left"))
        hi = int(stamps.searchsorted(end, side="left"))
        return lo, hi

    @classmethod
    def slice(cls, start_date, end_date) -> pd.DataFrame:
        lo, hi = cls.bounds(start_date, end_date)
        return cls.get_df().iloc[lo:hi]

    @classmethod
    def clear_df(cls):
        with cls._lock:
            cls.historical_df = None
            cls.loaded_range = None