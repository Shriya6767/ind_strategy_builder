"""Market data: one XTS market-data socket per credential, a tick store
every runner reads from, and 1-minute candles built the way the backtest
frame labels them (by completion minute).

Hot path: a `1501-json-full` event -> `TickStore.update()` -> the
instrument's tick listeners run synchronously in the same event-loop step.
A runner's stop-loss check is therefore evaluated within microseconds of
the tick arriving, with no polling loop and no queue in between.

Memory: one `_Instrument` object (slots) per subscribed contract holding the
last tick and the candle in progress; no tick history is kept.
"""
from src.core.modules import (
    asyncio, orjson, struct, zlib, time, Callable, Any,
)
from src.live.sio_client import SocketIOClient
from src.core.logger import get_logger
from src.live.xts_client import XTSMarketDataClient, XTSError, MSG_TOUCHLINE, MSG_CANDLE
from src.live.timeutil import minute_label

logger = get_logger(__name__)

Key = tuple[int, int]  # (exchangeSegment code, exchangeInstrumentID)


class Candle:
    __slots__ = ("label", "end_epoch", "open", "high", "low", "close")

    def __init__(self, label: int, end_epoch: float, price: float):
        self.label = label            # completion label, seconds since midnight IST
        self.end_epoch = end_epoch
        self.open = self.high = self.low = self.close = price


class _Instrument:
    __slots__ = ("ltp", "bid", "ask", "ts", "candle", "last_close", "tick_listeners", "candle_listeners")

    def __init__(self):
        self.ltp: float | None = None
        self.bid: float | None = None
        self.ask: float | None = None
        self.ts: float = 0.0
        self.candle: Candle | None = None
        self.last_close: Candle | None = None
        self.tick_listeners: list[Callable[[Key, float, float], Any]] = []
        self.candle_listeners: list[Callable[[Key, Candle], Any]] = []


class TickStore:
    def __init__(self):
        self._inst: dict[Key, _Instrument] = {}

    def _get(self, key: Key) -> _Instrument:
        inst = self._inst.get(key)
        if inst is None:
            inst = self._inst[key] = _Instrument()
        return inst

    def ltp(self, key: Key) -> float | None:
        inst = self._inst.get(key)
        return inst.ltp if inst else None

    def quote(self, key: Key) -> tuple[float | None, float | None, float | None]:
        inst = self._inst.get(key)
        return (inst.ltp, inst.bid, inst.ask) if inst else (None, None, None)

    def age(self, key: Key) -> float:
        inst = self._inst.get(key)
        return time.time() - inst.ts if inst and inst.ts else float("inf")

    def on_tick(self, key: Key, cb):
        self._get(key).tick_listeners.append(cb)

    def off_tick(self, key: Key, cb):
        inst = self._inst.get(key)
        if inst and cb in inst.tick_listeners:
            inst.tick_listeners.remove(cb)

    def on_candle(self, key: Key, cb):
        self._get(key).candle_listeners.append(cb)

    def off_candle(self, key: Key, cb):
        inst = self._inst.get(key)
        if inst and cb in inst.candle_listeners:
            inst.candle_listeners.remove(cb)


    def update(self, key: Key, ltp: float, ts: float | None = None, bid: float | None = None, ask: float | None = None):
        if ltp is None or ltp <= 0:
            return
        ts = ts or time.time()
        inst = self._get(key)
        inst.ltp, inst.ts = ltp, ts
        if bid is not None:
            inst.bid = bid
        if ask is not None:
            inst.ask = ask
        # candle bookkeeping (completion-labelled, like the backtest bars)
        c = inst.candle
        if c is None or ts >= c.end_epoch:
            if c is not None:
                self._close_candle(key, inst, c)
            label = minute_label(ts)
            end_epoch = ts - (ts % 60) + 60
            inst.candle = Candle(label, end_epoch, ltp)
        else:
            if ltp > c.high:
                c.high = ltp
            elif ltp < c.low:
                c.low = ltp
            c.close = ltp
        for cb in inst.tick_listeners:
            try:
                cb(key, ltp, ts)
            except Exception as e:  # a runner bug must never kill the feed
                logger.exception(f"[FEED] tick listener failed for {key}: {e}")


    def _close_candle(self, key: Key, inst: _Instrument, c: Candle):
        inst.last_close = c
        for cb in inst.candle_listeners:
            try:
                cb(key, c)
            except Exception as e:
                logger.exception(f"[FEED] candle listener failed for {key}: {e}")

    def flush(self, now: float | None = None):
        """Close candles whose minute has ended even if no tick arrived since
        (illiquid strikes). Called once a second by the feed manager."""
        now = now or time.time()
        for key, inst in self._inst.items():
            c = inst.candle
            if c is not None and now >= c.end_epoch:
                inst.candle = None
                self._close_candle(key, inst, c)


# Binary 1501 packet decoder (doc: "Binary Marketdata Event")
_HDR = struct.Struct("<bHhiHhHH")          # isGzip, msgCode, seg, instId, bookType, marketType, uncompressedSize, compressedSize


def decode_binary_touchline(data: bytes) -> tuple[Key, float, float | None, float | None] | None:
    """-> ((segment, instrument_id), ltp, bid, ask) for a 1501 packet, else None.
    Layout per the Symphony doc; verify byte order against a live packet
    before relying on Binary publishFormat (JSON is the default)."""
    try:
        is_gzip, msg, seg, inst_id, _bt, _mt, _usz, _csz = _HDR.unpack_from(data, 0)
        payload = data[_HDR.size:]
        if is_gzip:
            payload = zlib.decompress(payload)
        off = 0
        code, version, _app = struct.unpack_from("<HHH", payload, off); off += 6
        off += 8                                                     # tokenID uint64
        if version >= 4:
            off += 8                                                 # sequenceNumber
            (skip,) = struct.unpack_from("<i", payload, off); off += 4
            off += max(skip, 0)
        if code != MSG_TOUCHLINE:
            return None
        seg2, inst2 = struct.unpack_from("<hi", payload, off); off += 6
        off += 8                                                     # exchangeTimestamp
        bid_size, bid_price, _bo, _bf = struct.unpack_from("<qdIh", payload, off); off += 22
        ask_size, ask_price, _ao, _af = struct.unpack_from("<qdIh", payload, off); off += 22
        off += 8                                                     # LastUpdateTime
        (ltp,) = struct.unpack_from("<d", payload, off)
        return (int(seg2 or seg), int(inst2 or inst_id)), float(ltp), float(bid_price) or None, float(ask_price) or None
    except Exception:
        return None


# One market data socket
class XTSMarketFeed:
    def __init__(self, origin: str, path: str, app_key: str, secret: str, store: TickStore,
                 publish_format: str = "JSON", name: str = "feed"):
        self.name = name
        self.client = XTSMarketDataClient(origin, path)
        self.app_key, self.secret = app_key, secret
        self.store = store
        self.publish_format = "Binary" if publish_format.lower().startswith("bin") else "JSON"
        self.sio: SocketIOClient | None = None
        self._refs: dict[Key, int] = {}
        self._codes: dict[Key, int] = {}
        self.remaining_subscriptions: int | None = None
        self.connected = False
        self._flush_task: asyncio.Task | None = None
        self._binary_warned = False


    def _bind(self, sio: SocketIOClient):
        def on_connect():
            self.connected = True
            logger.info(f"[FEED:{self.name}] socket connected")
            if self._refs:
                asyncio.get_running_loop().create_task(self._resubscribe_all())

        def on_disconnect():
            self.connected = False
            logger.warning(f"[FEED:{self.name}] socket disconnected")

        sio.on_connect = on_connect
        sio.on_disconnect = on_disconnect
        sio.on("error", lambda data=None: logger.error(f"[FEED:{self.name}] error: {str(data)[:300]}"))
        sio.on("1501-json-full", self._apply_touchline_json)
        sio.on("1501-json-partial", self._apply_partial)
        sio.on("xts-binary-packet", self._on_binary)


    def _on_binary(self, data):
        decoded = decode_binary_touchline(data) if isinstance(data, (bytes, bytearray)) else None
        if decoded:
            key, ltp, bid, ask = decoded
            self.store.update(key, ltp, None, bid, ask)
        elif not self._binary_warned:
            self._binary_warned = True
            logger.warning(f"[FEED:{self.name}] could not decode a binary packet -- switch LIVE_FEED_PUBLISH_FORMAT=JSON")


    def _apply_touchline_json(self, data):
        try:
            d = orjson.loads(data) if isinstance(data, (str, bytes)) else data
            touch = d.get("Touchline") or d
            key = (int(d.get("ExchangeSegment")), int(d.get("ExchangeInstrumentID")))
            ltp = touch.get("LastTradedPrice")
            bid = (touch.get("BidInfo") or {}).get("Price")
            ask = (touch.get("AskInfo") or {}).get("Price")
            self.store.update(key, float(ltp), None, _f(bid), _f(ask))
        except Exception as e:
            logger.debug(f"[FEED:{self.name}] bad 1501 payload: {e}")


    def _apply_partial(self, data):
        """'t:12_123456,ltp:310.5,...' (pipe/comma variants exist)."""
        try:
            text = data.decode() if isinstance(data, (bytes, bytearray)) else str(data)
            fields = dict(part.split(":", 1) for part in text.replace("|", ",").split(",") if ":" in part)
            seg, inst = fields["t"].split("_")
            ltp = fields.get("ltp") or fields.get("LastTradedPrice")
            if ltp:
                self.store.update((int(seg), int(inst)), float(ltp))
        except Exception:
            pass


    async def start(self):
        await self.client.login(self.app_key, self.secret)
        query = {"token": self.client.token, "userID": self.client.user_id,
                 "publishFormat": self.publish_format, "broadcastMode": "Full"}
        # Classic XTS installs serve Socket.IO v2 (Engine.IO 3) at <path>/socket.io;
        # the newer binary market data API uses <path>/socketio. Probe in that order.
        p = self.client.path
        candidates = [(f"{p}/socket.io", 3), (f"{p}/socket.io", 4), (f"{p}/socketio", 3), (f"{p}/socketio", 4)]
        self.sio = SocketIOClient(self.client.origin, candidates, query, name=self.name, log_frames=3)
        self._bind(self.sio)
        try:
            await self.sio.start()
        except Exception as e:
            raise XTSError(f"market data socket connect failed: {e}")
        self._flush_task = asyncio.get_running_loop().create_task(self._flush_loop())


    async def stop(self):
        if self._flush_task:
            self._flush_task.cancel()
        if self.sio is not None:
            await self.sio.stop()
        try:
            await self.client.logout()
        except Exception:
            pass
        await self.client.aclose()


    async def _flush_loop(self):
        while True:
            await asyncio.sleep(1.0)
            self.store.flush()


    @staticmethod
    def _instrument_bodies(keys: list[Key]) -> list[dict]:
        return [{"exchangeSegment": seg, "exchangeInstrumentID": inst} for seg, inst in keys]


    def _apply_snapshot(self, quotes: list[dict]):
        for q in quotes:
            try:
                key = (int(q.get("ExchangeSegment")), int(q.get("ExchangeInstrumentID")))
                touch = q.get("Touchline") or q
                ltp = touch.get("LastTradedPrice")
                if ltp:
                    self.store.update(key, float(ltp), None,
                                      _f((touch.get("BidInfo") or {}).get("Price")),
                                      _f((touch.get("AskInfo") or {}).get("Price")))
            except Exception:
                continue


    async def subscribe(self, keys: list[Key], code: int = MSG_TOUCHLINE) -> None:
        """Subscribe (idempotent, ref-counted). The ack's snapshot quotes are
        applied to the store, so LTPs are readable right after this returns."""
        new = []
        for k in keys:
            self._refs[k] = self._refs.get(k, 0) + 1
            if self._refs[k] == 1:
                self._codes[k] = code
                new.append(k)
        if not new:
            return
        for i in range(0, len(new), 50):                       # keep request bodies small
            chunk = new[i:i + 50]
            try:
                quotes, remaining = await self.client.subscribe(self._instrument_bodies(chunk), code)
            except XTSError as e:
                for k in chunk:
                    self._refs.pop(k, None)
                    self._codes.pop(k, None)
                raise
            self.remaining_subscriptions = remaining
            self._apply_snapshot(quotes)


    async def unsubscribe(self, keys: list[Key]) -> None:
        drop = []
        for k in keys:
            n = self._refs.get(k, 0) - 1
            if n <= 0:
                self._refs.pop(k, None)
                if k in self._codes:
                    drop.append((k, self._codes.pop(k)))
            else:
                self._refs[k] = n
        by_code: dict[int, list[Key]] = {}
        for k, code in drop:
            by_code.setdefault(code, []).append(k)
        for code, ks in by_code.items():
            try:
                await self.client.unsubscribe(self._instrument_bodies(ks), code)
            except XTSError as e:
                logger.warning(f"[FEED:{self.name}] unsubscribe failed: {e}")

    async def _resubscribe_all(self):
        """After a socket reconnect. XTS keeps subscriptions on the login
        session, so the server usually answers 'Instrument Already
        Subscribed' -- that is success; refresh the quotes and move on."""
        by_code: dict[int, list[Key]] = {}
        for k, code in self._codes.items():
            by_code.setdefault(code, []).append(k)
        for code, ks in by_code.items():
            for i in range(0, len(ks), 50):
                chunk = ks[i:i + 50]
                try:
                    quotes, remaining = await self.client.subscribe(self._instrument_bodies(chunk), code)
                    self._apply_snapshot(quotes)
                    self.remaining_subscriptions = remaining
                except XTSError as e:
                    if "already subscribed" in str(e).lower():
                        await self.snapshot(chunk)
                    else:
                        logger.error(f"[FEED:{self.name}] resubscribe failed: {e}")

    async def snapshot(self, keys: list[Key]) -> None:
        """One-off REST quote fetch into the store. Works for instruments that
        are NOT subscribed and consumes no subscription slots, so strike
        scans use this and subscribe only to the contract they pick."""
        for i in range(0, len(keys), 50):
            try:
                self._apply_snapshot(await self.client.quotes(self._instrument_bodies(keys[i:i + 50]), MSG_TOUCHLINE))
            except XTSError as e:
                logger.warning(f"[FEED:{self.name}] quotes failed: {e}")


def _f(v):
    try:
        return float(v) if v not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        return None
