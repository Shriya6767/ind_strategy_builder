from src.core.modules import os, load_dotenv, psycopg2, psycopg2_pool, threading

load_dotenv()

def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw not in (None, "") else default
    except ValueError:
        return default


SENSEX_PROCESSED_FULL_PATH: str = os.getenv("SENSEX_PROCESSED_FULL_PATH")
SENSEX_SPOT_PATH: str | None = os.getenv("SENSEX_SPOT_PATH") or None
SENSEX_FNO_PATH: str | None = os.getenv("SENSEX_FNO_PATH") or None

DATA_PRELOAD: bool = _bool("DATA_PRELOAD", True)
DATA_PRELOAD_RANGE: str | None = os.getenv("DATA_PRELOAD_RANGE") or None

BACKTEST_WORKERS: int = _int("BACKTEST_WORKERS", 0)            # 0 = half the cores
PORTFOLIO_MAX_WORKERS: int = _int("PORTFOLIO_MAX_WORKERS", 0)  # 0 = all cores

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")

DB_POOL_MIN: int = _int("DB_POOL_MIN", 5)
DB_POOL_MAX: int = _int("DB_POOL_MAX", 20)
DB_POOL_WAIT_SECONDS: int = _int("DB_POOL_WAIT_SECONDS", 10)

JWT_SECRET: str | None = os.getenv("JWT_SECRET") or None
JWT_EXPIRES_HOURS: int = _int("JWT_EXPIRES_HOURS", 12)         # 12 hours
OTP_DEV_MODE: bool = _bool("OTP_DEV_MODE", False)               # echo OTP in responses (testing only)
GOOGLE_CLIENT_ID: str | None = os.getenv("GOOGLE_CLIENT_ID") or None

OTP_PROVIDER: str = (os.getenv("OTP_PROVIDER") or "console").strip().lower()
SMTP_HOST: str = os.getenv("SMTP_HOST") or "smtp.gmail.com"
SMTP_PORT: int = _int("SMTP_PORT", 587)
SMTP_USER: str | None = os.getenv("SMTP_USER") or None
SMTP_PASSWORD: str | None = os.getenv("SMTP_PASSWORD") or None
SMTP_FROM: str | None = os.getenv("SMTP_FROM") or SMTP_USER
SMTP_FROM_NAME: str = os.getenv("SMTP_FROM_NAME") or "Strategy Builder"

CORS_ORIGINS: list[str] = [
    o.strip() for o in (os.getenv("CORS_ORIGINS") or "*").split(",") if o.strip()
]

BROKER_SECRET_KEY: str | None = os.getenv("BROKER_SECRET_KEY") or None

LIVE_WORKER_URL: str = (os.getenv("LIVE_WORKER_URL") or "http://127.0.0.1:8001").rstrip("/")
LIVE_WORKER_PORT: int = _int("LIVE_WORKER_PORT", 8001)
LIVE_INTERNAL_TOKEN: str | None = os.getenv("LIVE_INTERNAL_TOKEN") or None

LIVE_FEED_ROOT: str | None = os.getenv("LIVE_FEED_ROOT") or None          # e.g. https://xts.broker.com/apimarketdata
LIVE_FEED_APP_KEY: str | None = os.getenv("LIVE_FEED_APP_KEY") or None
LIVE_FEED_SECRET: str | None = os.getenv("LIVE_FEED_SECRET") or None
LIVE_FEED_PUBLISH_FORMAT: str = (os.getenv("LIVE_FEED_PUBLISH_FORMAT") or "JSON").strip()   # JSON | Binary

LIVE_SENSEX_INDEX_ID: int = _int("LIVE_SENSEX_INDEX_ID", 0)

LIVE_ORDER_QTY_IN_LOTS: bool = _bool("LIVE_ORDER_QTY_IN_LOTS", False)
LIVE_ORDER_TIMEOUT_SECONDS: int = _int("LIVE_ORDER_TIMEOUT_SECONDS", 50)# Where the daily contract master is cached (one text file per day).
LIVE_MASTER_CACHE_DIR: str = os.getenv("LIVE_MASTER_CACHE_DIR") or "cache"
LIVE_HOLIDAYS: list[str] = [d.strip() for d in (os.getenv("LIVE_HOLIDAYS") or "").split(",") if d.strip()]
LIVE_AUTO_ACTIVATE_TIME: str = os.getenv("LIVE_AUTO_ACTIVATE_TIME") or "09:05:00"
LIVE_SESSION_RESET_TIME: str = os.getenv("LIVE_SESSION_RESET_TIME") or "08:30:00"


class _PooledConnection:
    """Behaves like a psycopg2 connection for the services (cursor(),
    commit(), rollback(), close()), except that close() RETURNS the
    connection to the pool instead of disconnecting. The services keep
    their `conn.close()` in `finally` unchanged."""
    __slots__ = ("_conn", "_pool", "_gate")

    def __init__(self, conn, pool, gate):
        self._conn = conn
        self._pool = pool
        self._gate = gate

    def __getattr__(self, name):
        conn = object.__getattribute__(self, "_conn")
        if conn is None:
            raise psycopg2.InterfaceError("connection already returned to the pool")
        return getattr(conn, name)

    def close(self):
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            if conn.closed:
                self._pool.putconn(conn, close=True)      # dead socket: drop, pool reconnects later
                return
            # A service that raised before commit leaves an open/failed
            # transaction; roll it back so the next borrower starts clean.
            conn.rollback()
            self._pool.putconn(conn)
        except Exception:
            try:
                self._pool.putconn(conn, close=True)
            except Exception:
                pass
        finally:
            self._gate.release()


class Database:
    """Process-wide PostgreSQL connection pool.

    `get_connection()` keeps its old contract -- call it, use cursor /
    commit / rollback, `close()` in `finally` -- but the connection now
    comes from a `ThreadedConnectionPool` and goes back to it on close().
    The pool is created lazily and belongs to ONE process: a forked
    backtest worker inherits the parent's pool object but must never reuse
    the parent's sockets, so a pid check gives every process its own pool.
    TCP keepalives stop idle pooled connections from being silently dropped
    by the OS/PostgreSQL after hours of quiet."""
    _pool = None
    _gate = None
    _pool_pid = None
    _lock = threading.Lock()

    @classmethod
    def _get_pool(cls):
        pid = os.getpid()
        with cls._lock:
            if cls._pool is None or cls._pool_pid != pid:
                cls._pool = psycopg2_pool.ThreadedConnectionPool(
                    DB_POOL_MIN, DB_POOL_MAX,
                    host=DATABASE_HOST,
                    port=DATABASE_PORT,
                    database=DATABASE_NAME,
                    user=DATABASE_USER,
                    password=DATABASE_PASSWORD,
                    keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=3,
                )
                cls._gate = threading.BoundedSemaphore(DB_POOL_MAX)
                cls._pool_pid = pid
            return cls._pool, cls._gate

    @classmethod
    def get_connection(cls):
        pool, gate = cls._get_pool()
        if not gate.acquire(timeout=DB_POOL_WAIT_SECONDS):
            raise psycopg2.OperationalError(
                f"Database busy: all {DB_POOL_MAX} pooled connections in use for "
                f"{DB_POOL_WAIT_SECONDS}s (raise DB_POOL_MAX)."
            )
        try:
            conn = pool.getconn()
            if conn.closed:
                pool.putconn(conn, close=True)
                conn = pool.getconn()
        except Exception:
            gate.release()
            raise
        return _PooledConnection(conn, pool, gate)

    @classmethod
    def close_all(cls):
        """Shutdown hook: disconnect every pooled connection of this process."""
        with cls._lock:
            if cls._pool is not None and cls._pool_pid == os.getpid():
                try:
                    cls._pool.closeall()
                finally:
                    cls._pool = None
                    cls._gate = None