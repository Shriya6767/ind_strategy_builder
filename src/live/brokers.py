"""Broker adapters with one interface: `XTSBroker` (real orders through the
Interactive API) and `PaperBroker` (fills against the live feed, no broker
call -- AlgoTest's "Virtual Execution Environment").

`OrderTracker` turns the asynchronous order lifecycle into awaitables: a
runner places an order, then `await tracker.wait_terminal(id, timeout)`
resolves the moment the socket delivers Filled / Cancelled / Rejected. If
the socket is silent (some XTS installs drop events), the XTS adapter falls
back to polling the order book at the documented 1 request/second.
"""
from src.core.modules import asyncio, time, dataclasses, Optional, Callable, Any
from src.core.logger import get_logger
from src.live.xts_client import (
    XTSInteractiveClient, XTSInteractiveStream, XTSError, ORDER_TERMINAL_STATES,
)
from src.live.feed import TickStore, Key
from src.live.execution import apply_slippage

logger = get_logger(__name__)


@dataclasses.dataclass(slots=True)
class OrderUpdate:
    app_order_id: str
    status: str
    filled_qty: int = 0
    avg_price: float = 0.0
    reason: str = ""
    tag: str = ""
    raw: dict | None = None

    @property
    def terminal(self) -> bool:
        return self.status in ORDER_TERMINAL_STATES

    @staticmethod
    def from_xts(d: dict) -> "OrderUpdate":
        return OrderUpdate(
            app_order_id=str(d.get("AppOrderID", "")),
            status=str(d.get("OrderStatus", "")),
            filled_qty=int(float(d.get("CumulativeQuantity") or 0)),
            avg_price=float(d.get("OrderAverageTradedPrice") or 0),
            reason=str(d.get("CancelRejectReason") or ""),
            tag=str(d.get("OrderUniqueIdentifier") or ""),
            raw=d,
        )


class OrderTracker:
    def __init__(self):
        self._latest: dict[str, OrderUpdate] = {}
        self._futures: dict[str, asyncio.Future] = {}
        self.listeners: list[Callable[[OrderUpdate], Any]] = []

    def handle(self, upd: OrderUpdate):
        if not upd.app_order_id:
            return
        prev = self._latest.get(upd.app_order_id)
        # never let a stale "New" overwrite a "Filled" that arrived first
        if prev is not None and prev.terminal and not upd.terminal:
            return
        self._latest[upd.app_order_id] = upd
        for cb in self.listeners:
            try:
                cb(upd)
            except Exception as e:
                logger.exception(f"[TRACKER] listener failed: {e}")
        if upd.terminal:
            fut = self._futures.pop(upd.app_order_id, None)
            if fut is not None and not fut.done():
                fut.set_result(upd)

    def latest(self, app_order_id: str) -> OrderUpdate | None:
        return self._latest.get(app_order_id)

    async def wait_terminal(self, app_order_id: str, timeout: float) -> OrderUpdate | None:
        cur = self._latest.get(app_order_id)
        if cur is not None and cur.terminal:
            return cur
        fut = self._futures.get(app_order_id)
        if fut is None:
            fut = self._futures[app_order_id] = asyncio.get_running_loop().create_future()
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            return None

    def forget(self, app_order_id: str):
        self._latest.pop(app_order_id, None)
        self._futures.pop(app_order_id, None)


class Broker:
    """Interface every adapter implements."""
    name = "broker"
    mode = "paper"

    def __init__(self):
        self.tracker = OrderTracker()
        self.client_id: str | None = None

    async def connect(self): ...
    async def close(self): ...

    def build_order(self, *, segment: str, instrument_id: int, side: str, quantity: int, order_type: str,
                    product: str, limit_price: float, tag: str, stop_price: float = 0.0) -> dict:
        return {
            "exchangeSegment": segment, "exchangeInstrumentID": int(instrument_id), "orderSide": side,
            "orderQuantity": int(quantity), "orderType": order_type, "productType": product,
            "limitPrice": float(limit_price), "stopPrice": float(stop_price), "orderUniqueIdentifier": tag[:20],
        }

    # None = unknown (try it), False = the broker does not list StopLimit for the segment
    supports_stop_limit: bool | None = None

    async def modify(self, app_order_id: str, payload: dict, *, order_type: str, limit_price: float,
                     stop_price: float = 0.0) -> str:
        """Re-price / re-type an OPEN order in place (SL-L trail, chase, SL-L -> LIMIT on target or exit time)."""
        raise NotImplementedError

    async def refresh_order(self, app_order_id: str) -> None:
        """Pull the order's current state into the tracker (slow safety net for resting orders)."""

    async def place(self, payload: dict) -> str: ...
    async def cancel(self, app_order_id: str, tag: str = "") -> None: ...
    async def wait_terminal(self, app_order_id: str, timeout: float) -> OrderUpdate | None:
        return await self.tracker.wait_terminal(app_order_id, timeout)
    async def order_book(self) -> list[dict]: return []
    async def positions(self) -> list[dict]: return []
    async def balance(self) -> dict: return {}


# ---------------------------------------------------------------------------
class XTSBroker(Broker):
    name = "open_xts"
    mode = "live"
    POLL_AFTER_SECONDS = 3.0        # socket silent this long -> poll the order book

    def __init__(self, origin: str, path: str, token: str, user_id: str, client_id: str | None):
        super().__init__()
        self.client = XTSInteractiveClient(origin, path)
        self.client.set_token(token, user_id)
        self.client.client_id = client_id or user_id
        self.client_id = self.client.client_id
        self.stream: XTSInteractiveStream | None = None
        self._last_event = 0.0
        self.session_lost = False

    async def connect(self):
        self.stream = self.client.stream()
        self.stream.on_order = self._on_order
        self.stream.on_trade = self._on_order          # trade events carry the same order fields
        self.stream.on_logout = self._on_logout
        try:
            await self.stream.start()
        except Exception as e:
            logger.error(f"[XTS-BROKER] order stream connect failed (will poll order book): {e}")
        # sanity: the token works; a dead token must not leave the order stream running
        try:
            await self.client.order_book()
        except Exception:
            await self.close()
            raise

    async def close(self):
        if self.stream:
            await self.stream.stop()
        await self.client.aclose()

    def _on_order(self, d: dict):
        self._last_event = time.time()
        self.tracker.handle(OrderUpdate.from_xts(d))

    def _on_logout(self):
        self.session_lost = True

    def build_order(self, **kw) -> dict:
        return self.client.build_order(**kw)

    async def place(self, payload: dict) -> str:
        return await self.client.place_order(payload)

    async def cancel(self, app_order_id: str, tag: str = "") -> None:
        await self.client.cancel_order(app_order_id, tag)

    async def modify(self, app_order_id: str, payload: dict, *, order_type: str, limit_price: float,
                     stop_price: float = 0.0) -> str:
        out = await self.client.modify_order(
            app_order_id, quantity=payload["orderQuantity"], limit_price=limit_price, order_type=order_type,
            product=payload["productType"], stop_price=stop_price, tag=payload.get("orderUniqueIdentifier", ""))
        payload.update(orderType=order_type, limitPrice=float(limit_price), stopPrice=float(stop_price))
        return out

    async def refresh_order(self, app_order_id: str) -> None:
        try:
            for row in await self.client.order_book():
                if str(row.get("AppOrderID")) == str(app_order_id):
                    self.tracker.handle(OrderUpdate.from_xts(row))
                    return
        except XTSError as e:
            logger.warning(f"[XTS-BROKER] order refresh failed: {e}")

    async def wait_terminal(self, app_order_id: str, timeout: float) -> OrderUpdate | None:
        """Socket first; if nothing arrives for POLL_AFTER_SECONDS, poll the
        order book once a second until terminal or timeout."""
        deadline = time.monotonic() + timeout
        upd = await self.tracker.wait_terminal(app_order_id, min(timeout, self.POLL_AFTER_SECONDS))
        while upd is None and time.monotonic() < deadline:
            try:
                for row in await self.client.order_book():
                    if str(row.get("AppOrderID")) == str(app_order_id):
                        self.tracker.handle(OrderUpdate.from_xts(row))
                        break
            except XTSError as e:
                logger.warning(f"[XTS-BROKER] order book poll failed: {e}")
            cur = self.tracker.latest(app_order_id)
            if cur is not None and cur.terminal:
                return cur
            upd = await self.tracker.wait_terminal(app_order_id, min(1.0, max(0.0, deadline - time.monotonic())))
        return upd

    async def order_book(self) -> list[dict]:
        return await self.client.order_book()

    async def positions(self) -> list[dict]:
        # NetWise includes positions carried from earlier days (BTST / positional holds)
        return await self.client.positions("NetWise")

    async def balance(self) -> dict:
        return await self.client.balance()


class PaperBroker(Broker):
    """Fills at the feed's LTP (plus optional slippage). A marketable limit
    fills at once; a resting limit waits for a tick through its price."""
    name = "paper"
    mode = "paper"
    _seq = 0

    def __init__(self, store: TickStore, slippage_pct: float = 0.0, segment_code: int = 12):
        super().__init__()
        self.store = store
        self.slippage_pct = slippage_pct
        self.segment_code = segment_code
        self.client_id = "PAPER"
        self._open: dict[str, dict] = {}
        self._watch: dict[Key, list[str]] = {}

    async def connect(self): ...
    async def close(self): ...

    def _key(self, payload: dict) -> Key:
        return (self.segment_code, int(payload["exchangeInstrumentID"]))

    def _fill(self, oid: str, payload: dict, price: float):
        payload = self._open.pop(oid, payload)
        price = apply_slippage(price, payload["orderSide"], self.slippage_pct)
        self.tracker.handle(OrderUpdate(oid, "Filled", int(payload["orderQuantity"]), price, "",
                                        payload.get("orderUniqueIdentifier", "")))

    def _marketable(self, payload: dict, ltp: float) -> bool:
        if payload["orderType"] == "STOPLIMIT" and not payload.get("_triggered"):
            # an SL-L rests until the LTP reaches its trigger, then behaves as a limit order
            stop = float(payload.get("stopPrice") or 0)
            if (payload["orderSide"] == "BUY" and ltp >= stop) or (payload["orderSide"] == "SELL" and ltp <= stop):
                payload["_triggered"] = True
            else:
                return False
        lim = float(payload["limitPrice"])
        return ltp <= lim if payload["orderSide"] == "BUY" else ltp >= lim

    async def place(self, payload: dict) -> str:
        PaperBroker._seq += 1
        oid = f"P{int(time.time()) % 100000}{PaperBroker._seq:04d}"
        key = self._key(payload)
        ltp = self.store.ltp(key)
        self.tracker.handle(OrderUpdate(oid, "New", 0, 0.0, "", payload.get("orderUniqueIdentifier", "")))
        if ltp and self._marketable(payload, ltp):
            asyncio.get_running_loop().call_soon(self._fill, oid, payload, ltp)
            return oid
        self._open[oid] = payload
        if key not in self._watch:
            self._watch[key] = []
            self.store.on_tick(key, self._on_tick)
        self._watch[key].append(oid)
        return oid


    def _on_tick(self, key: Key, ltp: float, ts: float):
        for oid in list(self._watch.get(key, [])):
            payload = self._open.get(oid)
            if payload is None:
                self._watch[key].remove(oid)
                continue
            if self._marketable(payload, ltp):
                self._watch[key].remove(oid)
                self._fill(oid, payload, ltp)


    async def cancel(self, app_order_id: str, tag: str = "") -> None:
        payload = self._open.pop(app_order_id, None)
        if payload is not None:
            self.tracker.handle(OrderUpdate(app_order_id, "Cancelled", 0, 0.0, "cancelled", tag))


    async def modify(self, app_order_id: str, payload: dict, *, order_type: str, limit_price: float,
                     stop_price: float = 0.0) -> str:
        p = self._open.get(app_order_id)
        if p is None:
            raise XTSError("order is not open (already filled or cancelled)")
        p.update(orderType=order_type, limitPrice=float(limit_price), stopPrice=float(stop_price))
        p.pop("_triggered", None)
        payload.update(orderType=order_type, limitPrice=float(limit_price), stopPrice=float(stop_price))
        ltp = self.store.ltp(self._key(p))
        if ltp and self._marketable(p, ltp):
            self._fill(app_order_id, p, ltp)
        return app_order_id
