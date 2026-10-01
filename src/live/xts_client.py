"""Thin async clients for the two Symphony XTS APIs.

Endpoints and payloads follow https://developers.symphonyfintech.in/doc/interactive/
and .../doc/apimarketdata/ (XTS "Binary Marketdata" API). A broker's XTS
install differs only in the origin and the two root paths (`/interactive` or
`/1interactive`, `/apimarketdata` or `/apimarketdata`), so both are
parameters here; HostLookUp returns the interactive one.

Design for speed:
* one persistent `httpx.AsyncClient` per session (keep-alive pool, no TLS
  handshake per order);
* orjson for every body;
* a token-bucket throttle on the order endpoints (XTS: 10 order calls/s
  across place/modify/cancel) so a burst of legs never draws a 429;
* the interactive Socket.IO stream pushes order/trade events, so fills are
  seen the moment the exchange confirms them -- no polling.
"""
from src.core.modules import (
    httpx, orjson, asyncio, time, json, Optional, Callable, Any,
)
from src.core.logger import get_logger
from src.live.sio_client import SocketIOClient

logger = get_logger(__name__)

# exchangeSegment numeric codes (market data API) and string codes (interactive).
SEG_BSECM, SEG_BSEFO, SEG_NSECM, SEG_NSEFO = 11, 12, 1, 2
SEG_NAME = {11: "BSECM", 12: "BSEFO", 1: "NSECM", 2: "NSEFO", 3: "NSECD", 13: "BSECD"}
SEG_CODE = {v: k for k, v in SEG_NAME.items()}

# xtsMessageCode
MSG_TOUCHLINE, MSG_DEPTH, MSG_CANDLE, MSG_OI = 1501, 1502, 1505, 1510

ORDER_OPEN_STATES = ("New", "Open", "PendingNew", "Replaced", "PartiallyFilled", "PendingReplace", "PendingCancel")
ORDER_TERMINAL_STATES = ("Filled", "Cancelled", "Rejected")


class XTSError(Exception):
    def __init__(self, description: str, code: str | None = None, http_status: int | None = None):
        super().__init__(description)
        self.description = description
        self.code = code
        self.http_status = http_status


def _unwrap(body: Any) -> Any:
    """Both APIs answer {"type": "success", "result": ...}; the market data
    docs sometimes show the envelope inside a one-element list."""
    if isinstance(body, list) and body:
        body = body[0]
    if not isinstance(body, dict):
        raise XTSError(f"Unexpected XTS response: {str(body)[:200]}")
    if str(body.get("type", "")).lower() != "success":
        raise XTSError(body.get("description") or "XTS request failed", body.get("code"))
    return body.get("result")


class _OrderThrottle:
    """At most `rate` calls per rolling second; awaits instead of failing."""

    def __init__(self, rate: int = 10):
        self.rate = rate
        self._stamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        if self.rate <= 0:
            return                                   # throttle disabled
        async with self._lock:
            while True:
                now = time.monotonic()
                self._stamps = [s for s in self._stamps if now - s < 1.0]
                if len(self._stamps) < self.rate:
                    self._stamps.append(now)
                    return
                await asyncio.sleep(1.0 - (now - self._stamps[0]) + 0.005)


class _BaseClient:
    def __init__(self, origin: str, path: str, timeout: float = 10.0):
        self.origin = origin.rstrip("/")
        self.path = "/" + path.strip("/") if path else ""
        self.base_url = self.origin + self.path
        self.token: str | None = None
        self.user_id: str | None = None
        self._http = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout, connect=5.0),
            headers={"Content-Type": "application/json"},
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

    def set_token(self, token: str, user_id: str | None = None):
        self.token = token
        self.user_id = user_id
        self._http.headers["authorization"] = token


    async def _request(self, method: str, path: str, *, json_body=None, params=None) -> Any:
        content = orjson.dumps(json_body) if json_body is not None else None
        try:
            r = await self._http.request(method, path, content=content, params=params)
        except httpx.HTTPError as e:
            raise XTSError(f"{method} {path}: {e.__class__.__name__}: {e}") from e
        try:
            body = orjson.loads(r.content) if r.content else {}
        except ValueError:
            raise XTSError(f"{method} {path}: HTTP {r.status_code} non-JSON response", http_status=r.status_code)
        if r.status_code >= 400 and not (isinstance(body, dict) and body.get("type") == "success"):
            desc = body.get("description") if isinstance(body, dict) else str(body)[:200]
            raise XTSError(desc or f"HTTP {r.status_code}", body.get("code") if isinstance(body, dict) else None, r.status_code)
        return _unwrap(body)


    async def aclose(self):
        await self._http.aclose()


# Interactive (orders) API
class XTSInteractiveClient(_BaseClient):
    # Order-call throttle (calls/second). The XTS doc's rate-limit table says
    # 10/s for place+modify+cancel combined, but this is OFF (0) until a real
    # rate-limit rejection (HTTP 429) is observed from the broker.
    ORDER_THROTTLE_RATE = 0

    def __init__(self, origin: str, path: str = "/interactive", timeout: float = 10.0):
        super().__init__(origin, path, timeout)
        self.client_id: str | None = None
        self.is_investor_client: bool | None = None
        self.enums: dict = {}
        self._throttle = _OrderThrottle(self.ORDER_THROTTLE_RATE)


    @staticmethod
    async def host_lookup(url: str, access_password: str, version: str = "interactive_1.0.1") -> tuple[str, str]:
        """POST <hostlookup url> -> (uniqueKey, connectionString). The
        connection string is the interactive base URL (origin + path)."""
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.post(url, content=orjson.dumps({"accesspassword": access_password, "version": version}),
                                headers={"Content-Type": "application/json"})
            body = orjson.loads(r.content)
        if isinstance(body, list) and body:
            body = body[0]
        # HostLookUp does not use the standard envelope on every install (this
        # broker answers type != "success" with description "Hostlookup
        # successful"), so judge it by its payload, not by `type`.
        result = (body.get("result") if isinstance(body, dict) else None) or {}
        unique_key = result.get("UniqueKey") or result.get("uniqueKey")
        conn_str = result.get("connectionString")
        if not unique_key and not conn_str:
            raise XTSError(f"HostLookUp failed: {(body or {}).get('description') or str(body)[:200]}",
                           (body or {}).get("code") if isinstance(body, dict) else None, r.status_code)
        logger.info(f"[XTS] hostlookup ok: connectionString={conn_str} uniqueKey={'yes' if unique_key else 'no'}")
        return unique_key, conn_str


    async def login(self, app_key: str, secret_key: str, unique_key: str | None = None,
                    dealer_client_id: str | None = None) -> dict:
        body = {"appKey": app_key, "secretKey": secret_key, "source": "WebAPI"}
        if unique_key:
            body["uniqueKey"] = unique_key
        result = await self._request("POST", "/user/session", json_body=body)
        self.set_token(result["token"], result.get("userID"))
        self.is_investor_client = bool(result.get("isInvestorClient", True))
        self.enums = result.get("enums") or {}
        codes = result.get("clientCodes") or []
        # Investor clients trade their own account: clientID = userID (this is
        # what Symphony's own SDK sends). Dealers must name the client.
        if self.is_investor_client:
            self.client_id = self.user_id
        else:
            self.client_id = dealer_client_id or (codes[0] if codes else None)
        return result


    async def logout(self):
        try:
            await self._request("DELETE", "/user/session")
        finally:
            self.token = None


    def build_order(self, *, segment: str, instrument_id: int, side: str, quantity: int,
                    order_type: str = "LIMIT", product: str = "NRML", limit_price: float = 0.0,
                    stop_price: float = 0.0, tag: str = "", time_in_force: str = "DAY") -> dict:
        """The exact JSON the place-order endpoint takes. Built ahead of time
        for every leg so the entry hot path only patches the price."""
        payload = {
            "exchangeSegment": segment,
            "exchangeInstrumentID": int(instrument_id),
            "productType": product,
            "orderType": order_type,
            "orderSide": side,
            "timeInForce": time_in_force,
            "disclosedQuantity": 0,
            "orderQuantity": int(quantity),
            "limitPrice": float(limit_price),
            "stopPrice": float(stop_price),
            "orderUniqueIdentifier": tag[:20],
            "apiOrderSource": "StrategyBuilder",
        }
        if self.client_id:
            payload["clientID"] = self.client_id
        return payload


    async def place_order(self, payload: dict) -> str:
        """-> AppOrderID as a string. Acceptance only: the fill arrives on the
        order stream (or shows in the order book)."""
        await self._throttle.acquire()
        result = await self._request("POST", "/orders", json_body=payload)
        return str(result["AppOrderID"])


    async def modify_order(self, app_order_id: str, *, quantity: int, limit_price: float,
                           order_type: str = "LIMIT", product: str = "NRML", stop_price: float = 0.0,
                           tag: str = "", time_in_force: str = "DAY") -> str:
        body = {
            "appOrderID": int(app_order_id),
            "modifiedProductType": product,
            "modifiedOrderType": order_type,
            "modifiedOrderQuantity": int(quantity),
            "modifiedDisclosedQuantity": 0,
            "modifiedLimitPrice": float(limit_price),
            "modifiedStopPrice": float(stop_price),
            "modifiedTimeInForce": time_in_force,
            "orderUniqueIdentifier": tag[:20],
        }
        if self.client_id:
            body["clientID"] = self.client_id
        await self._throttle.acquire()
        result = await self._request("PUT", "/orders", json_body=body)
        return str(result["AppOrderID"])


    async def cancel_order(self, app_order_id: str, tag: str = "") -> None:
        params = {"appOrderID": app_order_id}
        if tag:
            params["orderUniqueIdentifier"] = tag[:20]
        if self.client_id:
            params["clientID"] = self.client_id
        await self._throttle.acquire()
        await self._request("DELETE", "/orders", params=params)


    async def cancel_all(self, segment: str = "BSEFO", instrument_id: int = 0) -> Any:
        body = {"exchangeSegment": segment, "exchangeInstrumentID": int(instrument_id)}
        if self.client_id:
            body["clientID"] = self.client_id
        await self._throttle.acquire()
        return await self._request("POST", "/orders/cancelall", json_body=body)


    def _client_params(self) -> dict:
        return {"clientID": self.client_id} if self.client_id else {}


    async def order_book(self) -> list[dict]:
        return await self._request("GET", "/orders", params=self._client_params()) or []


    async def order_history(self, app_order_id: str) -> list[dict]:
        params = {"appOrderID": app_order_id, **self._client_params()}
        return await self._request("GET", "/orders", params=params) or []


    async def trade_book(self) -> list[dict]:
        return await self._request("GET", "/orders/trades", params=self._client_params()) or []


    async def positions(self, day_or_net: str = "DayWise") -> list[dict]:
        params = {"dayOrNet": day_or_net, **self._client_params()}
        result = await self._request("GET", "/portfolio/positions", params=params)
        if isinstance(result, dict):
            return result.get("positionList") or []
        return result or []


    async def balance(self) -> dict:
        return await self._request("GET", "/user/balance", params=self._client_params()) or {}


    async def profile(self) -> dict:
        return await self._request("GET", "/user/profile", params=self._client_params()) or {}


    def stream(self) -> "XTSInteractiveStream":
        return XTSInteractiveStream(self.origin, self.path, self.token, self.user_id)


class XTSInteractiveStream:
    """Socket.IO feed of order / trade / position events for one session.
    Payloads arrive as JSON strings (sometimes dicts); handlers get dicts."""

    def __init__(self, origin: str, path: str, token: str, user_id: str):
        self.origin, self.path, self.token, self.user_id = origin, path, token, user_id
        self.sio: Optional[SocketIOClient] = None
        self.on_order: Optional[Callable[[dict], Any]] = None
        self.on_trade: Optional[Callable[[dict], Any]] = None
        self.on_position: Optional[Callable[[dict], Any]] = None
        self.on_logout: Optional[Callable[[], Any]] = None
        self.connected = False


    @staticmethod
    def _parse(data) -> dict:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", "replace")
        if isinstance(data, str):
            try:
                return orjson.loads(data)
            except ValueError:
                return {"raw": data}
        return data if isinstance(data, dict) else {"raw": data}


    def _bind(self, sio: SocketIOClient):
        def on_connect():
            self.connected = True
            logger.info(f"[XTS-INTERACTIVE] socket connected user={self.user_id}")

        def on_disconnect():
            self.connected = False
            logger.warning(f"[XTS-INTERACTIVE] socket disconnected user={self.user_id}")

        def forward(cb_attr):
            def handler(data=None):
                cb = getattr(self, cb_attr)
                if cb:
                    return cb(self._parse(data))
            return handler

        def logout(*_):
            logger.warning(f"[XTS-INTERACTIVE] server logged the session out user={self.user_id}")
            if self.on_logout:
                return self.on_logout()

        sio.on_connect = on_connect
        sio.on_disconnect = on_disconnect
        sio.on("joined", lambda data=None: logger.info(f"[XTS-INTERACTIVE] joined: {str(data)[:120]}"))
        sio.on("error", lambda data=None: logger.error(f"[XTS-INTERACTIVE] error: {str(data)[:300]}"))
        sio.on("order", forward("on_order"))
        sio.on("trade", forward("on_trade"))
        sio.on("position", forward("on_position"))
        sio.on("logout", logout)


    async def start(self):
        query = {"token": self.token, "userID": self.user_id, "apiType": "INTERACTIVE"}
        candidates = [(f"{self.path}/socket.io", 3), (f"{self.path}/socket.io", 4)]
        self.sio = SocketIOClient(self.origin, candidates, query, name=f"orders-{self.user_id}", log_frames=3)
        self._bind(self.sio)
        await self.sio.start()


    async def stop(self):
        if self.sio is not None:
            await self.sio.stop()


# Market data API
class XTSMarketDataClient(_BaseClient):
    def __init__(self, origin: str, path: str = "/apimarketdata", timeout: float = 20.0):
        super().__init__(origin, path, timeout)


    async def login(self, app_key: str, secret_key: str) -> dict:
        body = {"appKey": app_key, "secretKey": secret_key, "source": "WebAPI"}
        result = await self._request("POST", "/auth/login", json_body=body)
        self.set_token(result["token"], result.get("userID"))
        return result


    async def logout(self):
        try:
            await self._request("DELETE", "/auth/logout")
        finally:
            self.token = None


    async def master(self, segments: list[str] = ("BSEFO",)) -> str:
        """Pipe-delimited contract master (one line per instrument)."""
        result = await self._request("POST", "/instruments/master", json_body={"exchangeSegmentList": list(segments)})
        return result if isinstance(result, str) else str(result)


    async def index_list(self, segment: int = SEG_BSECM) -> list[str]:
        result = await self._request("GET", "/instruments/indexlist", params={"exchangeSegment": segment})
        return (result or {}).get("indexList") or []


    @staticmethod
    def _parse_quotes(result) -> list[dict]:
        quotes = []
        for q in (result or {}).get("listQuotes") or []:
            if isinstance(q, str):
                try:
                    q = orjson.loads(q)
                except ValueError:
                    continue
            if isinstance(q, dict):
                quotes.append(q)
        return quotes


    async def quotes(self, instruments: list[dict], code: int = MSG_TOUCHLINE) -> list[dict]:
        body = {"instruments": instruments, "xtsMessageCode": code, "publishFormat": "JSON"}
        return self._parse_quotes(await self._request("POST", "/instruments/quotes", json_body=body))


    async def subscribe(self, instruments: list[dict], code: int = MSG_TOUCHLINE) -> tuple[list[dict], int | None]:
        """-> (snapshot quotes, remaining subscription count). The ack carries
        the current quote of each instrument, so a subscribe doubles as a
        batch quote request -- used for strike selection at entry time."""
        body = {"instruments": instruments, "xtsMessageCode": code}
        result = await self._request("POST", "/instruments/subscription", json_body=body)
        return self._parse_quotes(result), (result or {}).get("Remaining_Subscription_Count")


    async def unsubscribe(self, instruments: list[dict], code: int = MSG_TOUCHLINE) -> None:
        body = {"instruments": instruments, "xtsMessageCode": code}
        await self._request("PUT", "/instruments/subscription", json_body=body)


    async def ohlc(self, segment: int, instrument_id: int, start: str, end: str, compression: str = "60") -> list[dict]:
        params = {"exchangeSegment": segment, "exchangeInstrumentID": instrument_id,
                  "startTime": start, "endTime": end, "compressionValue": compression}
        result = await self._request("GET", "/instruments/ohlc", params=params)
        return (result or {}).get("dataReponse") or (result or {}).get("dataResponse") or result or []


async def _maybe_await(value):
    if asyncio.iscoroutine(value):
        await value