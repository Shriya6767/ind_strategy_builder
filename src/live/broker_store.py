"""Broker Setup: store a user's Open XTS credentials (encrypted), log in to
the broker each day, and hand decrypted sessions to the live worker.

The API process only ever logs into the INTERACTIVE API here. Market data
sessions belong to the worker (Symphony allows one session per market data
app key, so a second login from the API would kick the worker's feed).
"""
from src.core.modules import RealDictCursor, datetime, timedelta
from urllib.parse import urlparse
from src.core.config import Database, LIVE_SESSION_RESET_TIME
from src.core.logger import get_logger
from src.live import crypto
from src.live.xts_client import XTSInteractiveClient, XTSError
from src.live.timeutil import now_ist, to_naive_ist, parse_hms

logger = get_logger(__name__)

HOST_LOOKUP_DEFAULT_PASSWORD = "2021HostLookUpAccess"   # Symphony's documented default


def session_expiry(now: datetime) -> datetime:
    """A token lives until the broker's next daily session reset (naive IST)."""
    reset = parse_hms(LIVE_SESSION_RESET_TIME)
    nxt = now.replace(hour=reset // 3600, minute=(reset % 3600) // 60, second=reset % 60, microsecond=0)
    return nxt if nxt > now else nxt + timedelta(days=1)


def _split_url(raw: str, default_path: str) -> tuple[str, str]:
    """'https://xts.broker.com/interactive/' -> ('https://xts.broker.com', '/interactive')"""
    u = urlparse(str(raw).strip())
    if not u.scheme or not u.netloc:
        raise ValueError("connection_url must be a full URL like https://xts.broker.com")
    origin = f"{u.scheme}://{u.netloc}"
    path = u.path.rstrip("/")
    return origin, (path if path and path != "/" else default_path)


class BrokerAccountService:
    REQUIRED = ("connection_name", "interactive_key", "interactive_secret", "connection_url")

    @staticmethod
    def add(user_id: int, payload: dict) -> dict:
        missing = [k for k in BrokerAccountService.REQUIRED if not str(payload.get(k) or "").strip()]
        if missing:
            raise ValueError(f"Missing: {', '.join(missing)}")
        origin, interactive_path = _split_url(payload["connection_url"], "/interactive")
        md_path = "/apimarketdata"
        if payload.get("marketdata_url"):
            _, md_path = _split_url(payload["marketdata_url"], md_path)
        host_lookup_url = (payload.get("host_lookup_url") or "").strip() or None
        conn = Database.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                INSERT INTO broker_account (user_id, broker, connection_name, interactive_key_enc, interactive_secret_enc,
                    marketdata_key_enc, marketdata_secret_enc, connection_url, host_lookup_url, host_lookup_password_enc,
                    interactive_path, marketdata_path, dealer_client_id)
                VALUES (%s, 'open_xts', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING broker_account_id
            """, (user_id, str(payload["connection_name"]).strip()[:100],
                  crypto.encrypt(str(payload["interactive_key"]).strip()),
                  crypto.encrypt(str(payload["interactive_secret"]).strip()),
                  crypto.encrypt((payload.get("marketdata_key") or "").strip() or None),
                  crypto.encrypt((payload.get("marketdata_secret") or "").strip() or None),
                  origin, host_lookup_url, crypto.encrypt((payload.get("host_lookup_password") or "").strip() or None),
                  interactive_path, md_path, (payload.get("dealer_client_id") or "").strip() or None))
            new_id = cur.fetchone()["broker_account_id"]
            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return BrokerAccountService.get_public(new_id, user_id)


    @staticmethod
    def _public_view(row: dict) -> dict:
        sess = {
            "logged_in": bool(row.get("interactive_token_enc")) and row.get("expires_at") is not None
                         and row["expires_at"] > to_naive_ist(now_ist()),
            "expires_at": str(row["expires_at"]) if row.get("expires_at") else None,
            "logged_in_at": str(row["logged_in_at"]) if row.get("logged_in_at") else None,
            "client_id": row.get("client_id"),
            "is_investor_client": row.get("is_investor_client"),
            "last_error": row.get("last_error"),
        }
        return {
            "broker_account_id": row["broker_account_id"],
            "broker": row["broker"],
            "connection_name": row["connection_name"],
            "connection_url": row["connection_url"],
            "interactive_path": row["interactive_path"],
            "host_lookup_url": row.get("host_lookup_url"),
            "interactive_key_masked": crypto.mask(crypto.decrypt(row["interactive_key_enc"])),
            "has_marketdata_keys": bool(row.get("marketdata_key_enc")),
            "dealer_client_id": row.get("dealer_client_id"),
            "is_active": row["is_active"],
            "created_at": str(row["created_at"]),
            "session": sess,
        }


    _SELECT = """
        SELECT a.*, s.interactive_token_enc, s.interactive_user_id, s.client_id, s.is_investor_client,
               s.logged_in_at, s.expires_at, s.last_error,
               s.interactive_origin AS session_origin, s.interactive_path AS session_path, s.order_types
        FROM broker_account a LEFT JOIN broker_session s USING (broker_account_id)
    """


    @staticmethod
    def list(user_id: int) -> list[dict]:
        conn = Database.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(BrokerAccountService._SELECT + " WHERE a.user_id = %s AND a.is_active ORDER BY a.created_at", (user_id,))
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return [BrokerAccountService._public_view(r) for r in rows]


    @staticmethod
    def get_public(broker_account_id: int, user_id: int) -> dict | None:
        row = BrokerAccountService._row(broker_account_id, user_id)
        return BrokerAccountService._public_view(row) if row else None


    @staticmethod
    def _row(broker_account_id: int, user_id: int | None = None) -> dict | None:
        conn = Database.get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            sql = BrokerAccountService._SELECT + " WHERE a.broker_account_id = %s AND a.is_active"
            params = [broker_account_id]
            if user_id is not None:
                sql += " AND a.user_id = %s"
                params.append(user_id)
            cur.execute(sql, params)
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        return row


    @staticmethod
    def delete(broker_account_id: int, user_id: int) -> bool:
        conn = Database.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM broker_session WHERE broker_account_id = %s", (broker_account_id,))
            cur.execute("UPDATE broker_account SET is_active = FALSE, updated_at = NOW() WHERE broker_account_id = %s AND user_id = %s",
                        (broker_account_id, user_id))
            ok = cur.rowcount > 0
            conn.commit()
            cur.close()
            return ok
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


    @staticmethod
    def get_credentials(broker_account_id: int, user_id: int | None = None) -> dict | None:
        row = BrokerAccountService._row(broker_account_id, user_id)
        if not row:
            return None
        return {
            "broker_account_id": row["broker_account_id"],
            "user_id": row["user_id"],
            "connection_name": row["connection_name"],
            "origin": row["connection_url"],
            "interactive_path": row["interactive_path"],
            "marketdata_path": row["marketdata_path"],
            "interactive_key": crypto.decrypt(row["interactive_key_enc"]),
            "interactive_secret": crypto.decrypt(row["interactive_secret_enc"]),
            "marketdata_key": crypto.decrypt(row.get("marketdata_key_enc")),
            "marketdata_secret": crypto.decrypt(row.get("marketdata_secret_enc")),
            "host_lookup_url": row.get("host_lookup_url"),
            "host_lookup_password": crypto.decrypt(row.get("host_lookup_password_enc")),
            "dealer_client_id": row.get("dealer_client_id"),
        }


    @staticmethod
    def get_session(broker_account_id: int) -> dict | None:
        """Decrypted interactive session, or None when not logged in / expired."""
        row = BrokerAccountService._row(broker_account_id)
        if not row or not row.get("interactive_token_enc"):
            return None
        # expires_at is stored as naive IST; compare in IST whatever the server's own timezone is
        if row.get("expires_at") is None or row["expires_at"] <= to_naive_ist(now_ist()):
            return None
        return {
            "broker_account_id": row["broker_account_id"],
            "user_id": row["user_id"],
            # the address the token was issued at (HostLookUp may differ from connection_url)
            "origin": row.get("session_origin") or row["connection_url"],
            "interactive_path": (row.get("session_path") or "") if row.get("session_origin") else row["interactive_path"],
            "token": crypto.decrypt(row["interactive_token_enc"]),
            "order_types": row.get("order_types"),      # BSEFO order types the broker enables (from the login reply)
            # the XTS login id (used for the order socket); differs from client_id on dealer accounts
            "xts_user_id": row.get("interactive_user_id") or row.get("client_id"),
            "client_id": row.get("client_id"),
            "is_investor_client": row.get("is_investor_client"),
            "expires_at": row["expires_at"],
        }


    @staticmethod
    def _save_session(broker_account_id: int, *, token: str | None, xts_user_id: str | None, client_id: str | None,
                      is_investor: bool | None, interactive_path: str | None, error: str | None,
                      origin: str | None = None, order_types: str | None = None) -> None:
        now = to_naive_ist(now_ist())
        expires = session_expiry(now) if token else None
        conn = Database.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO broker_session (broker_account_id, interactive_token_enc, interactive_user_id, client_id,
                                            is_investor_client, logged_in_at, expires_at, last_error,
                                            interactive_origin, interactive_path, order_types)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (broker_account_id) DO UPDATE SET
                    interactive_token_enc = EXCLUDED.interactive_token_enc, interactive_user_id = EXCLUDED.interactive_user_id,
                    client_id = EXCLUDED.client_id, is_investor_client = EXCLUDED.is_investor_client,
                    logged_in_at = EXCLUDED.logged_in_at, expires_at = EXCLUDED.expires_at, last_error = EXCLUDED.last_error,
                    interactive_origin = EXCLUDED.interactive_origin, interactive_path = EXCLUDED.interactive_path,
                    order_types = EXCLUDED.order_types
            """, (broker_account_id, crypto.encrypt(token), xts_user_id, client_id, is_investor,
                  now if token else None, expires, error,
                  origin if token else None, (interactive_path or "") if token else None,
                  order_types if token else None))
            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


    @staticmethod
    def _bsefo_order_types(enums: dict) -> str | None:
        """'StopLimit,StopMarket,Limit,Market' from the login reply's
        enums.exchangeInfo.BSEFO.orderType, or None when the broker omits it."""
        try:
            types = ((enums or {}).get("exchangeInfo") or {}).get("BSEFO", {}).get("orderType")
            if isinstance(types, dict):
                types = [k for k in types if not str(k).isdigit()]
            return ",".join(str(t) for t in types)[:200] if types else None
        except Exception:
            return None

    @staticmethod
    async def login(broker_account_id: int, user_id: int) -> dict:
        creds = BrokerAccountService.get_credentials(broker_account_id, user_id)
        if not creds:
            raise LookupError("Broker account not found")
        origin, path = creds["origin"], creds["interactive_path"]
        unique_key = None
        try:
            if creds.get("host_lookup_url"):
                # XTS installs share one well-known HostLookUp access password
                # (Symphony's SDK default); a broker-specific one can be stored.
                unique_key, conn_str = await XTSInteractiveClient.host_lookup(
                    creds["host_lookup_url"], creds.get("host_lookup_password") or HOST_LOOKUP_DEFAULT_PASSWORD)
                if conn_str:
                    # use the connection string EXACTLY: no path in it means the
                    # API sits at the root of that server (no /interactive prefix)
                    origin, path = _split_url(conn_str, "")
            client = XTSInteractiveClient(origin, path)
            try:
                await client.login(creds["interactive_key"], creds["interactive_secret"], unique_key,
                                   creds.get("dealer_client_id"))
                # make sure the token is usable before we store it
                await client.balance()
            finally:
                await client.aclose()
        except XTSError as e:
            BrokerAccountService._save_session(broker_account_id, token=None, xts_user_id=None, client_id=None,
                                               is_investor=None, interactive_path=None, error=str(e))
            raise
        BrokerAccountService._save_session(
            broker_account_id, token=client.token, xts_user_id=client.user_id, client_id=client.client_id,
            is_investor=client.is_investor_client, interactive_path=path, error=None, origin=origin,
            order_types=BrokerAccountService._bsefo_order_types(client.enums))
        logger.info(f"[BROKER] login ok account={broker_account_id} user={user_id} xts_user={client.user_id}")
        return BrokerAccountService.get_public(broker_account_id, user_id)


    @staticmethod
    def invalidate_session(broker_account_id: int, reason: str) -> None:
        """The broker rejected the stored token (daily reset / login elsewhere):
        clear it so the UI shows logged-out and nothing else uses it."""
        BrokerAccountService._save_session(broker_account_id, token=None, xts_user_id=None, client_id=None,
                                           is_investor=None, interactive_path=None, error=reason)

    @staticmethod
    async def logout(broker_account_id: int, user_id: int) -> dict | None:
        sess = BrokerAccountService.get_session(broker_account_id)
        if sess and sess["user_id"] == user_id:
            client = XTSInteractiveClient(sess["origin"], sess["interactive_path"])
            client.set_token(sess["token"], sess.get("xts_user_id"))
            try:
                await client.logout()
            except XTSError as e:
                logger.warning(f"[BROKER] logout call failed (clearing session anyway): {e}")
            finally:
                await client.aclose()
        BrokerAccountService._save_session(broker_account_id, token=None, xts_user_id=None, client_id=None,
                                           is_investor=None, interactive_path=None, error=None)
        return BrokerAccountService.get_public(broker_account_id, user_id)


    @staticmethod
    async def funds(broker_account_id: int, user_id: int) -> dict:
        sess = BrokerAccountService.get_session(broker_account_id)
        if not sess or sess["user_id"] != user_id:
            raise PermissionError("Broker not logged in")
        client = XTSInteractiveClient(sess["origin"], sess["interactive_path"])
        client.set_token(sess["token"], sess.get("xts_user_id"))
        client.client_id = sess.get("client_id")
        try:
            bal = await client.balance()
        finally:
            await client.aclose()
        # BalanceList is an object in Symphony's docs and a LIST of them on some
        # brokers (one entry per limit header, "ALL|ALL|ALL" first) -- accept both.
        if isinstance(bal, list):
            bal = bal[0] if bal else {}
        balance_list = bal.get("BalanceList") if isinstance(bal, dict) else None
        if isinstance(balance_list, list):
            balance_list = balance_list[0] if balance_list else {}
        limits = ((balance_list or {}).get("limitObject") or {}).get("RMSSubLimits") or {}
        return {
            "cash_available": limits.get("cashAvailable"),
            "margin_utilized": limits.get("marginUtilized"),
            "net_margin_available": limits.get("netMarginAvailable"),
            "mtm": limits.get("MTM"),
            "raw": bal,
        }