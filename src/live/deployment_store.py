"""Raw-SQL persistence for deployments, legs, orders, events and execution
settings (tables in scripts/sql/live_trade_migration.sql).

Every function is synchronous psycopg2 like the rest of the services. The
worker never calls them on its event loop directly: `DbWriter` runs them on
one background thread, in submission order, so a burst of order updates is
persisted without ever stalling a tick.
"""
from src.core.modules import RealDictCursor, Json, threading, queue, orjson, datetime, date
from src.core.config import Database
from src.core.logger import get_logger
from src.live.timeutil import now_ist, to_naive_ist

logger = get_logger(__name__)

ACTIVE_STATUSES = ("scheduled", "running", "paused")


def _run(fn):
    """Open a pooled connection, run fn(cursor), commit, close."""
    conn = None
    try:
        conn = Database.get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            out = fn(cur)
            conn.commit()
            return out
        finally:
            cur.close()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()


def _jsonable(obj):
    return orjson.loads(orjson.dumps(obj, default=str))


def upsert_execution_setting(user_id: int, strategy_id: int, version: int, broker_account_id, settings: dict,
                             auto_activate: bool) -> None:
    def q(cur):
        cur.execute("""
            INSERT INTO live_execution_setting (user_id, strategy_id, version, broker_account_id, settings, auto_activate, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (user_id, strategy_id) DO UPDATE SET
                version = EXCLUDED.version, broker_account_id = EXCLUDED.broker_account_id,
                settings = EXCLUDED.settings, auto_activate = EXCLUDED.auto_activate, updated_at = NOW()
        """, (user_id, strategy_id, version, broker_account_id, Json(_jsonable(settings)), auto_activate))
    _run(q)


def get_execution_setting(user_id: int, strategy_id: int) -> dict | None:
    def q(cur):
        cur.execute("SELECT * FROM live_execution_setting WHERE user_id = %s AND strategy_id = %s", (user_id, strategy_id))
        return cur.fetchone()
    return _run(q)


def list_execution_settings(user_id: int) -> list[dict]:
    def q(cur):
        cur.execute("SELECT * FROM live_execution_setting WHERE user_id = %s", (user_id,))
        return cur.fetchall()
    return _run(q)


def list_auto_activate() -> list[dict]:
    def q(cur):
        cur.execute("SELECT * FROM live_execution_setting WHERE auto_activate = TRUE AND broker_account_id IS NOT NULL")
        return cur.fetchall()
    return _run(q)


def create_deployment(*, user_id: int, strategy_id: int, strategy_name: str, version: int, broker_account_id,
                      mode: str, trade_date: date, exit_date: date, settings: dict, snapshot: dict) -> int:
    def q(cur):
        cur.execute("""
            INSERT INTO live_deployment (user_id, strategy_id, strategy_name, version, broker_account_id, mode,
                                         trade_date, exit_date, status, settings, strategy_snapshot)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'scheduled', %s, %s)
            RETURNING deployment_id
        """, (user_id, strategy_id, strategy_name, version, broker_account_id, mode, trade_date, exit_date,
              Json(_jsonable(settings)), Json(_jsonable(snapshot))))
        return cur.fetchone()["deployment_id"]
    return _run(q)


def get_deployment(deployment_id: int, user_id: int | None = None) -> dict | None:
    def q(cur):
        sql = "SELECT * FROM live_deployment WHERE deployment_id = %s"
        params = [deployment_id]
        if user_id is not None:
            sql += " AND user_id = %s"
            params.append(user_id)
        cur.execute(sql, params)
        return cur.fetchone()
    return _run(q)


def list_deployments(user_id: int, trade_date: date | None = None, include_archived: bool = False,
                     with_active: bool = False) -> list[dict]:
    def q(cur):
        sql = """SELECT deployment_id, strategy_id, strategy_name, version, broker_account_id, mode, trade_date,
                        exit_date, status, status_reason, realised_pnl, is_archived, created_at, updated_at
                 FROM live_deployment WHERE user_id = %s"""
        params = [user_id]
        if trade_date is not None:
            if with_active:
                sql += " AND (%s BETWEEN trade_date AND exit_date OR status IN %s)"
                params += [trade_date, ACTIVE_STATUSES]
            else:
                sql += " AND %s BETWEEN trade_date AND exit_date"
                params.append(trade_date)
        if not include_archived:
            sql += " AND is_archived = FALSE"
        sql += " ORDER BY created_at DESC"
        cur.execute(sql, params)
        return cur.fetchall()
    return _run(q)


def has_active_deployment(user_id: int, strategy_id: int, trade_date: date) -> bool:
    def q(cur):
        cur.execute("""SELECT 1 FROM live_deployment WHERE user_id = %s AND strategy_id = %s AND trade_date = %s
                       AND status IN %s LIMIT 1""", (user_id, strategy_id, trade_date, ACTIVE_STATUSES))
        return cur.fetchone() is not None
    return _run(q)


def list_active_deployments() -> list[dict]:
    """Everything the worker must (re)start: today's + BTST holds exiting today."""
    def q(cur):
        cur.execute("SELECT * FROM live_deployment WHERE status IN %s ORDER BY deployment_id", (ACTIVE_STATUSES,))
        return cur.fetchall()
    return _run(q)


def update_deployment_status(deployment_id: int, status: str, reason: str | None = None) -> None:
    def q(cur):
        cur.execute("UPDATE live_deployment SET status = %s, status_reason = %s, updated_at = NOW() WHERE deployment_id = %s",
                    (status, reason, deployment_id))
    _run(q)


def set_realised_pnl(deployment_id: int, pnl: float) -> None:
    def q(cur):
        cur.execute("UPDATE live_deployment SET realised_pnl = %s, updated_at = NOW() WHERE deployment_id = %s",
                    (round(pnl, 2), deployment_id))
    _run(q)


def archive_deployment(deployment_id: int, user_id: int) -> bool:
    def q(cur):
        cur.execute("""UPDATE live_deployment SET is_archived = TRUE, updated_at = NOW()
                       WHERE deployment_id = %s AND user_id = %s AND status NOT IN %s""",
                    (deployment_id, user_id, ACTIVE_STATUSES))
        return cur.rowcount > 0
    return _run(q)


_LEG_COLS = ("deployment_id", "leg_number", "attempt", "exchange_segment", "instrument_id", "symbol", "expiry",
             "strike", "option_type", "side", "quantity", "lots", "status", "entry_order_id", "entry_price",
             "entry_time", "underlying_at_entry", "stoploss_price", "target_price", "exit_order_id", "exit_price",
             "exit_time", "exit_reason", "pnl", "error", "entry_mode", "sl_order_id", "ref_price")


def update_deployment_dates(deployment_id: int, trade_date: date, exit_date: date) -> None:
    """Positional runs compute their expiry cycle in the worker (the API has
    no contract master) and store the resulting entry/exit days here."""
    def q(cur):
        cur.execute("UPDATE live_deployment SET trade_date = %s, exit_date = %s, updated_at = NOW() WHERE deployment_id = %s",
                    (trade_date, exit_date, deployment_id))
    _run(q)


def insert_leg(row: dict) -> int:
    cols = [c for c in _LEG_COLS if c in row]
    def q(cur):
        cur.execute(f"INSERT INTO live_leg ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) RETURNING live_leg_id",
                    [row[c] for c in cols])
        return cur.fetchone()["live_leg_id"]
    return _run(q)


def update_leg(live_leg_id: int, **fields) -> None:
    fields = {k: v for k, v in fields.items() if k in _LEG_COLS}
    if not fields:
        return
    def q(cur):
        sets = ", ".join(f"{k} = %s" for k in fields) + ", updated_at = NOW()"
        cur.execute(f"UPDATE live_leg SET {sets} WHERE live_leg_id = %s", [*fields.values(), live_leg_id])
    _run(q)


def list_legs(deployment_id: int) -> list[dict]:
    def q(cur):
        cur.execute("SELECT * FROM live_leg WHERE deployment_id = %s ORDER BY leg_number, attempt", (deployment_id,))
        return cur.fetchall()
    return _run(q)


def log_order(*, deployment_id: int, live_leg_id: int | None, app_order_id: str | None, unique_tag: str,
              action: str, side: str | None = None, order_type: str | None = None, quantity: int | None = None,
              price: float | None = None, status: str | None = None, filled_qty: int | None = None,
              avg_price: float | None = None, reason: str | None = None, payload: dict | None = None) -> None:
    def q(cur):
        cur.execute("""
            INSERT INTO live_order (deployment_id, live_leg_id, app_order_id, unique_tag, action, side, order_type,
                                    quantity, price, status, filled_qty, avg_price, reason, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (deployment_id, live_leg_id, app_order_id, unique_tag[:20] if unique_tag else None, action, side,
              order_type, quantity, price, status, filled_qty, avg_price, (reason or "")[:500] or None,
              Json(_jsonable(payload)) if payload is not None else None))
    _run(q)


def log_event(deployment_id: int, message: str, level: str = "info") -> None:
    def q(cur):
        cur.execute("INSERT INTO live_event (deployment_id, level, message) VALUES (%s, %s, %s)",
                    (deployment_id, level, message[:2000]))
    _run(q)


def list_events(deployment_id: int, limit: int = 200) -> list[dict]:
    def q(cur):
        cur.execute("SELECT level, message, created_at FROM live_event WHERE deployment_id = %s ORDER BY created_at DESC LIMIT %s",
                    (deployment_id, limit))
        return cur.fetchall()
    return _run(q)


def list_orders(deployment_id: int, limit: int = 500) -> list[dict]:
    def q(cur):
        cur.execute("""SELECT live_leg_id, app_order_id, unique_tag, action, side, order_type, quantity, price, status,
                              filled_qty, avg_price, reason, created_at
                       FROM live_order WHERE deployment_id = %s ORDER BY created_at DESC LIMIT %s""", (deployment_id, limit))
        return cur.fetchall()
    return _run(q)


# -- pending conditional entries ----------------------------------------------
_WAIT_COLS = ("deployment_id", "leg_number", "attempt", "kind", "leg_kind", "entry_mode", "side", "instrument_id",
              "watch_segment", "watch_instrument_id", "quantity", "lots", "trigger_price", "trigger_up", "range_hi",
              "range_lo", "range_end_at", "range_high_side", "deadline", "trade_id", "reentry_sl_left",
              "reentry_tgt_left", "meta", "status")


def upsert_wait(row: dict) -> None:
    """Insert or refresh one pending wait, keyed by (deployment, leg, attempt)."""
    cols = [c for c in _WAIT_COLS if c in row]
    vals = [Json(_jsonable(row[c])) if c == "meta" else row[c] for c in cols]
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("deployment_id", "leg_number", "attempt"))
    def q(cur):
        cur.execute(f"INSERT INTO live_wait ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                    f"ON CONFLICT (deployment_id, leg_number, attempt) DO UPDATE SET {updates}, updated_at = NOW()", vals)
    _run(q)


def update_wait(deployment_id: int, leg_number: int, attempt: int, **fields) -> None:
    fields = {k: v for k, v in fields.items() if k in _WAIT_COLS}
    if not fields:
        return
    def q(cur):
        sets = ", ".join(f"{k} = %s" for k in fields) + ", updated_at = NOW()"
        cur.execute(f"UPDATE live_wait SET {sets} WHERE deployment_id = %s AND leg_number = %s AND attempt = %s",
                    [*fields.values(), deployment_id, leg_number, attempt])
    _run(q)


def list_waits(deployment_id: int) -> list[dict]:
    def q(cur):
        cur.execute("SELECT * FROM live_wait WHERE deployment_id = %s AND status = 'waiting' ORDER BY leg_number, attempt",
                    (deployment_id,))
        return cur.fetchall()
    return _run(q)


# -- ordered background writer (worker process) -------------------------------
class DbWriter:
    """Runs persistence calls on one thread in FIFO order. `submit` returns
    immediately; the event loop never waits on PostgreSQL."""

    def __init__(self):
        self._q: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._loop, name="live-db-writer", daemon=True)
        self._thread.start()

    def submit(self, fn, *args, **kwargs):
        self._q.put((fn, args, kwargs))

    def _loop(self):
        while True:
            item = self._q.get()
            if item is None:
                return
            fn, args, kwargs = item
            try:
                fn(*args, **kwargs)
            except Exception as e:
                logger.exception(f"[DB-WRITER] {getattr(fn, '__name__', fn)} failed: {e}")
            finally:
                self._q.task_done()

    def flush(self, timeout: float = 5.0):
        try:
            self._q.join()
        except Exception:
            pass

    def stop(self):
        self._q.put(None)


def ts_now():
    """Naive IST timestamp for the DB."""
    return to_naive_ist(now_ist())
