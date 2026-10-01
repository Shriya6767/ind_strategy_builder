"""Minimal Socket.IO client over a raw WebSocket, written for the XTS servers.

Why not python-socketio: Symphony installs differ in Engine.IO version
(v3 = the client sends pings, v4 = the server sends pings) and one broker's
server answered neither library version's keep-alive correctly, dropping
the socket every 65 s. This client is protocol-agnostic about who pings:

  * it answers every server PING ("2") with a PONG ("3");
  * on Engine.IO v3 it also SENDS a PING every pingInterval;
  * it only reconnects when the WebSocket really closes or when NOTHING at
    all has been received for pingInterval + pingTimeout.

Wire format (Engine.IO packet type + Socket.IO packet type + JSON):
  0{handshake}      open       40            connected (default namespace)
  2 / 3             ping/pong  42["ev",arg]  event (optional ack id digits)
  1                 close      44"error"     error
  45N-["ev",{"_placeholder":true,"num":0}] + N binary frames  binary event
"""
from src.core.modules import asyncio, orjson, time, Callable, Any, Optional
from urllib.parse import urlencode
import websockets
from src.core.logger import get_logger

logger = get_logger(__name__)


class SocketIOClient:
    RECONNECT_MIN, RECONNECT_MAX = 1.0, 15.0
    OPEN_TIMEOUT = 15.0

    def __init__(self, origin: str, candidates: list[tuple[str, int]], query: dict, name: str = "sio",
                 log_frames: int = 0):
        """`candidates`: (socket.io path, Engine.IO version) pairs tried in
        order on the first connect; the one that works is reused after."""
        self.origin = origin.rstrip("/")
        self.candidates = candidates
        self.query = query
        self.name = name
        self.log_frames = log_frames
        self.handlers: dict[str, list[Callable]] = {}
        self.on_connect: Optional[Callable] = None
        self.on_disconnect: Optional[Callable] = None
        self.connected = False
        self.path: str | None = None
        self.eio: int | None = None
        self.ping_interval = 25.0
        self.ping_timeout = 60.0
        self.last_rx = 0.0
        self.frames_rx = 0
        self._ws = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._first = None  # future resolved by the first connect attempt
        self._pending_binary: tuple[str, int, list] | None = None


    def on(self, event: str, cb: Callable):
        self.handlers.setdefault(event, []).append(cb)


    async def start(self):
        """Connect (probing the candidates) and keep the session alive in a
        background task. Raises if no candidate connects the first time."""
        loop = asyncio.get_running_loop()
        self._first = loop.create_future()
        self._task = loop.create_task(self._run_forever(), name=f"sio-{self.name}")
        err = await self._first
        if err is not None:
            self._stopping = True
            self._task.cancel()
            raise err


    async def stop(self):
        self._stopping = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


    async def emit(self, event: str, data=None):
        if self._ws is None or not self.connected:
            raise ConnectionError("socket not connected")
        payload = [event] if data is None else [event, data]
        await self._ws.send("42" + orjson.dumps(payload).decode())


    async def _run_forever(self):
        delay = self.RECONNECT_MIN
        while not self._stopping:
            try:
                if self.path is None:
                    await self._probe()
                else:
                    await self._session(self.path, self.eio)
                delay = self.RECONNECT_MIN
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._first is not None and not self._first.done():
                    self._first.set_result(e)
                    return
                logger.warning(f"[SIO:{self.name}] session ended: {e.__class__.__name__}: {e}")
            if self._stopping:
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.RECONNECT_MAX)


    async def _probe(self):
        last = None
        for path, eio in self.candidates:
            try:
                await self._session(path, eio)
                return
            except Exception as e:
                last = e
                if self.path is not None:          # connected once, then dropped: not a probe failure
                    raise
                logger.warning(f"[SIO:{self.name}] {path} EIO={eio} failed: {e.__class__.__name__}: {e}")
        raise ConnectionError(f"no Socket.IO endpoint answered: {last}")


    def _ws_url(self, path: str, eio: int) -> str:
        scheme = "wss" if self.origin.startswith("https") else "ws"
        host = self.origin.split("://", 1)[1]
        q = urlencode({"EIO": eio, "transport": "websocket", **self.query})
        return f"{scheme}://{host}/{path.strip('/')}/?{q}"


    async def _session(self, path: str, eio: int):
        url = self._ws_url(path, eio)
        async with websockets.connect(url, ping_interval=None, max_size=2 ** 23, open_timeout=self.OPEN_TIMEOUT) as ws:
            self._ws = ws
            first = await asyncio.wait_for(ws.recv(), self.OPEN_TIMEOUT)
            if not isinstance(first, str) or not first.startswith("0"):
                raise ConnectionError(f"expected Engine.IO OPEN, got {str(first)[:80]!r}")
            hs = orjson.loads(first[1:])
            self.ping_interval = float(hs.get("pingInterval", 25000)) / 1000
            self.ping_timeout = float(hs.get("pingTimeout", 60000)) / 1000
            if eio >= 4:
                await ws.send("40")
            self.path, self.eio = path, eio
            self.last_rx = time.time()
            logger.info(f"[SIO:{self.name}] open {path} EIO={eio} sid={hs.get('sid')} "
                        f"pingInterval={self.ping_interval:.0f}s pingTimeout={self.ping_timeout:.0f}s")
            pinger = asyncio.get_running_loop().create_task(self._ping_loop(ws, eio))
            watchdog = asyncio.get_running_loop().create_task(self._watchdog(ws))
            try:
                async for msg in ws:
                    self.last_rx = time.time()
                    self.frames_rx += 1
                    if self.log_frames and self.frames_rx <= self.log_frames:
                        logger.info(f"[SIO:{self.name}] rx {str(msg)[:160]!r}")
                    if isinstance(msg, (bytes, bytearray)):
                        self._handle_binary(bytes(msg))
                    else:
                        await self._handle_text(ws, msg)
            finally:
                pinger.cancel()
                watchdog.cancel()
                self._ws = None
                was = self.connected
                self.connected = False
                if was:
                    logger.warning(f"[SIO:{self.name}] disconnected")
                    self._fire(self.on_disconnect)
        if not self._stopping:
            raise ConnectionError("websocket closed by server")


    async def _ping_loop(self, ws, eio: int):
        if eio >= 4:
            return                                   # v4: the server pings, we only pong
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                await ws.send("2")
            except Exception:
                return


    async def _watchdog(self, ws):
        limit = self.ping_interval + self.ping_timeout + 5
        while True:
            await asyncio.sleep(5)
            if time.time() - self.last_rx > limit:
                logger.warning(f"[SIO:{self.name}] nothing received for {limit:.0f}s -- reconnecting")
                try:
                    await ws.close()
                except Exception:
                    pass
                return


    async def _handle_text(self, ws, msg: str):
        t = msg[0]
        if t == "2":                                 # server ping -> pong
            await ws.send("3" + msg[1:])
        elif t == "3":                               # pong to our ping
            pass
        elif t == "1":                               # server close
            await ws.close()
        elif t == "4" and len(msg) > 1:
            st = msg[1]
            if st == "0":
                if not self.connected:
                    self.connected = True
                    if self._first is not None and not self._first.done():
                        self._first.set_result(None)
                    logger.info(f"[SIO:{self.name}] connected")
                    self._fire(self.on_connect)
            elif st == "2":
                name, args = self._parse_event(msg[2:])
                if name:
                    self._dispatch(name, args)
            elif st == "5":
                self._prepare_binary(msg[2:])
            elif st == "4":
                logger.error(f"[SIO:{self.name}] server error: {msg[2:][:300]}")
            elif st == "1":
                logger.warning(f"[SIO:{self.name}] server sent namespace disconnect")
                await ws.close()


    @staticmethod
    def _parse_event(body: str):
        """'123["ev",arg]' / '/ns,["ev",arg]' -> ("ev", [arg]); binary
        attachments count prefix 'N-' is handled by the caller."""
        i = 0
        if body.startswith("/"):
            i = body.find(",") + 1
            if i == 0:
                return None, None
        while i < len(body) and body[i].isdigit():
            i += 1
        try:
            arr = orjson.loads(body[i:])
        except ValueError:
            return None, None
        if not isinstance(arr, list) or not arr:
            return None, None
        return str(arr[0]), arr[1:]


    def _prepare_binary(self, body: str):
        dash = body.find("-")
        if dash <= 0:
            return
        count = int(body[:dash])
        name, args = self._parse_event(body[dash + 1:])
        if name:
            self._pending_binary = (name, count, [])


    def _handle_binary(self, data: bytes):
        if self._pending_binary is None:
            self._dispatch("xts-binary-packet", [data])
            return
        name, count, got = self._pending_binary
        got.append(data)
        if len(got) >= count:
            self._pending_binary = None
            self._dispatch(name, got)


    def _dispatch(self, name: str, args: list):
        for cb in self.handlers.get(name, ()):
            try:
                r = cb(*args) if args else cb()
                if asyncio.iscoroutine(r):
                    asyncio.get_running_loop().create_task(r)
            except Exception as e:
                logger.exception(f"[SIO:{self.name}] handler {name} failed: {e}")


    def _fire(self, cb):
        if cb is None:
            return
        try:
            r = cb()
            if asyncio.iscoroutine(r):
                asyncio.get_running_loop().create_task(r)
        except Exception as e:
            logger.exception(f"[SIO:{self.name}] callback failed: {e}")