"""`LiveEngine`: the worker process's brain. Owns the tick store, the market
data feeds, one broker adapter per (account, mode), one runner per active
deployment, and the browser WebSocket fan-out.

Startup: connect the platform feed (or defer to the first per-user feed),
load the contract master, restore every deployment that is scheduled /
running / paused in the DB, then run three background loops:
  * broadcast -- every second, push a snapshot to the users whose runners
    changed (or whose legs have live quotes);
  * scan      -- every 30 s, start deployments the API created while the
    worker was unreachable, and run daily auto-activation;
  * housekeeping -- drop finished runners from memory after the session.
"""
from src.core.modules import asyncio, time, orjson, datetime, date, timedelta, WebSocket, Optional
from src.core import config
from src.core.logger import get_logger
from src.live import deployment_store as store
from src.live.deployment_store import DbWriter
from src.live.feed import TickStore, XTSMarketFeed, Key
from src.live.brokers import Broker, XTSBroker, PaperBroker
from src.live.instrument_master import InstrumentMaster
from src.live.broker_store import BrokerAccountService
from src.live.runner import DeploymentRunner, TERMINAL_STATUSES
from src.live.execution import ExecutionSettings
from src.live.xts_client import SEG_BSECM, XTSError
from src.live.timeutil import today_ist, now_ist, parse_hms, secs_now, weekday_code, is_trading_day

logger = get_logger(__name__)

AUTO_RESTART_FROM_SECS = 8 * 3600
AUTO_RESTART_SCAN_SECS = 8 * 3600 + 45 * 60


class LiveEngine:
    def __init__(self):
        self.db = DbWriter()
        self.store = TickStore()
        self.master: InstrumentMaster | None = None
        self.index_key: Key | None = None
        self.runners: dict[int, DeploymentRunner] = {}
        self.feeds: dict[str, XTSMarketFeed] = {}          # app_key -> feed
        self.brokers: dict[int, XTSBroker] = {}            # broker_account_id -> live adapter
        self._ws: dict[int, set[WebSocket]] = {}           # user_id -> sockets
        self._dirty: set[int] = set()
        self._tasks: list[asyncio.Task] = []
        self._auto_done_for: date | None = None
        self._waiting_broker: set[int] = set()            # restores blocked on a stale broker session (logged once)
        self._lock = asyncio.Lock()
        self.started_at = time.time()


    async def start(self):
        if config.LIVE_FEED_APP_KEY and config.LIVE_FEED_SECRET and config.LIVE_FEED_ROOT:
            try:
                await self._open_feed("platform", config.LIVE_FEED_ROOT, "", config.LIVE_FEED_APP_KEY, config.LIVE_FEED_SECRET)
            except Exception as e:
                logger.error(f"[ENGINE] platform feed failed to start: {e}")
        await self._restore_active()
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(self._broadcast_loop()), loop.create_task(self._scan_loop())]
        logger.info("[ENGINE] live engine started")


    async def stop(self):
        for t in self._tasks:
            t.cancel()
        for r in list(self.runners.values()):
            await r.stop()
        for b in self.brokers.values():
            await b.close()
        for f in self.feeds.values():
            await f.stop()
        self.db.flush()
        self.db.stop()


    async def _open_feed(self, name: str, root: str, path: str, app_key: str, secret: str) -> XTSMarketFeed:
        feed = self.feeds.get(app_key)
        if feed is not None:
            return feed
        if path:
            origin, p = root, path
        else:
            from src.live.broker_store import _split_url
            origin, p = _split_url(root, "/apimarketdata")
        feed = XTSMarketFeed(origin, p, app_key, secret, self.store, config.LIVE_FEED_PUBLISH_FORMAT, name=name)
        await feed.start()
        self.feeds[app_key] = feed
        if self.master is None or self.master.as_of != today_ist():
            self.master = await InstrumentMaster.load(feed.client, config.LIVE_MASTER_CACHE_DIR, today_ist(),
                                                      config.LIVE_SENSEX_INDEX_ID)
            self.index_key = (SEG_BSECM, self.master.index_instrument_id) if self.master.index_instrument_id else None
            if self.index_key:
                await feed.subscribe([self.index_key])
        return feed


    async def feed_for(self, broker_account_id: int | None) -> XTSMarketFeed:
        if "platform" in [f.name for f in self.feeds.values()]:
            return next(f for f in self.feeds.values() if f.name == "platform")
        if broker_account_id is None:
            raise RuntimeError("No market data feed: set LIVE_FEED_* in .env or add market data keys to the broker account")
        creds = BrokerAccountService.get_credentials(broker_account_id)
        if not creds or not creds.get("marketdata_key"):
            raise RuntimeError("Broker account has no market data API keys and no platform feed is configured")
        return await self._open_feed(f"acct{broker_account_id}", creds["origin"], creds["marketdata_path"],
                                     creds["marketdata_key"], creds["marketdata_secret"])


    async def broker_for(self, dep: dict) -> Broker:
        settings = ExecutionSettings.from_dict(dep["settings"])
        if dep["mode"] != "live":
            return PaperBroker(self.store, settings.paper_slippage_pct)
        acct = dep.get("broker_account_id")
        cached = self.brokers.get(acct)
        sess = BrokerAccountService.get_session(acct) if acct else None
        if not sess:
            raise RuntimeError("Broker session missing or expired -- log in again on the Broker Setup page")
        # reuse the adapter only while it holds TODAY's token: a fresh broker
        # login issues a new token and the old one stops working
        if cached is not None and not cached.session_lost and cached.client.token == sess["token"]:
            return cached
        if cached is not None:
            await cached.close()
        broker = XTSBroker(sess["origin"], sess["interactive_path"], sess["token"],
                           sess.get("xts_user_id") or sess.get("client_id"), sess.get("client_id"))
        # StopLimit support as reported at login: False disables broker-side SL-L orders
        # for this account (stops stay in software); unknown -> try and fall back on rejection
        listed = (sess.get("order_types") or "").lower()
        broker.supports_stop_limit = ("stoplimit" in listed) if listed else None
        await broker.connect()
        self.brokers[acct] = broker
        # runners that started on an older session (overnight BTST / positional
        # holds) must exit through TODAY's token, not yesterday's
        for r in self.runners.values():
            if r.active and r.dep.get("broker_account_id") == acct and r.mode == "live":
                r.broker = broker
        await self.auto_restart(acct)
        return broker

    async def auto_restart(self, broker_account_id: int | None = None):
        """after 08:00, a broker login (or the 08:45 scan) restarts the
        overnight-paused BTST / positional deployments that opted in."""
        if secs_now() < AUTO_RESTART_FROM_SECS or not is_trading_day(today_ist()):
            return
        for r in list(self.runners.values()):
            if r.overnight_paused and r.settings.auto_restart \
                    and (broker_account_id is None or r.dep.get("broker_account_id") == broker_account_id):
                try:
                    await r.resume()
                    logger.info(f"[ENGINE] auto-restarted deployment {r.id}")
                except Exception as e:
                    logger.error(f"[ENGINE] auto-restart of deployment {r.id} failed: {e}")


    async def activate(self, deployment_id: int, restore: bool = False) -> dict:
        async with self._lock:
            runner = self.runners.get(deployment_id)
            if runner is not None and runner.active:
                return runner.snapshot()
            dep = store.get_deployment(deployment_id)
            if not dep:
                raise LookupError("Deployment not found")
            if dep["status"] in TERMINAL_STATUSES:
                raise ValueError(f"Deployment is {dep['status']}")
            try:
                feed = await self.feed_for(dep.get("broker_account_id"))
                broker = await self.broker_for(dep)
            except Exception as e:
                if restore:
                    # a held position must not be abandoned because the broker session is stale
                    # (yesterday's token): keep the status and let the 30 s scan retry after login
                    if deployment_id not in self._waiting_broker:
                        self._waiting_broker.add(deployment_id)
                        store.log_event(deployment_id, f"Waiting for broker login to resume: {e}", "warn")
                    raise
                store.update_deployment_status(deployment_id, "error", str(e)[:300])
                store.log_event(deployment_id, f"Cannot start: {e}", "error")
                raise
            self._waiting_broker.discard(deployment_id)
            runner = DeploymentRunner(self, dep, feed, broker, restore=restore)
            self.runners[deployment_id] = runner
            runner.start()
            self.notify(runner)
            return runner.snapshot()


    async def command(self, deployment_id: int, cmd: str, body: dict | None = None) -> dict:
        if cmd == "activate":
            return await self.activate(deployment_id)
        runner = self.runners.get(deployment_id)
        if runner is None:
            dep = store.get_deployment(deployment_id)
            if not dep:
                raise LookupError("Deployment not found")
            if dep["status"] in ("scheduled", "running", "paused"):
                runner = self.runners.get((await self.activate(deployment_id, restore=True))["deployment_id"])
            else:
                raise ValueError(f"Deployment is {dep['status']}")
        if cmd == "pause":
            await runner.pause()
        elif cmd == "resume":
            raw = (body or {}).get("exit_date")
            await runner.resume(date.fromisoformat(str(raw)) if raw else None)
        elif cmd == "squareoff":
            await runner.squareoff("manual")
        elif cmd == "manual":
            await runner.switch_to_manual("manual")
        elif cmd == "cancel":
            if runner.status not in ("scheduled", "paused"):
                raise ValueError("Cancel Deployment is only available while scheduled or paused -- use Square Off or Switch to Manual")
            await runner.cancel_deployment()
        else:
            raise ValueError("Unknown command")
        return runner.snapshot()


    async def squareoff_all(self, user_id: int) -> list[dict]:
        out = []
        for r in list(self.runners.values()):
            if r.user_id == user_id and r.active:
                await r.squareoff("manual_all")
                out.append(r.snapshot())
        return out

    async def manual_all(self, user_id: int) -> list[dict]:
        out = []
        for r in list(self.runners.values()):
            if r.user_id == user_id and r.active:
                await r.switch_to_manual("manual_all")
                out.append(r.snapshot())
        return out


    def snapshots(self, user_id: int) -> list[dict]:
        return [r.snapshot() for r in self.runners.values() if r.user_id == user_id]


    async def _restore_active(self):
        rows = store.list_active_deployments()
        for dep in rows:
            try:
                await self.activate(dep["deployment_id"], restore=dep["status"] != "scheduled")
                logger.info(f"[ENGINE] restored deployment {dep['deployment_id']} ({dep['status']})")
            except Exception as e:
                logger.error(f"[ENGINE] could not restore deployment {dep['deployment_id']}: {e}")


    async def _scan_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                for dep in store.list_active_deployments():
                    if dep["deployment_id"] not in self.runners:
                        try:
                            await self.activate(dep["deployment_id"], restore=dep["status"] != "scheduled")
                        except Exception as e:
                            if dep["deployment_id"] not in self._waiting_broker:
                                logger.error(f"[ENGINE] scan: deployment {dep['deployment_id']} failed to start: {e}")
                await self._auto_activate()
                if secs_now() >= AUTO_RESTART_SCAN_SECS:
                    await self.auto_restart()
                # forget finished runners after 16:00 so memory does not grow across days
                if secs_now() > 16 * 3600:
                    for did, r in list(self.runners.items()):
                        if not r.active and r.exit_date < today_ist() + timedelta(days=1):
                            self.runners.pop(did, None)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"[ENGINE] scan loop error: {e}")


    async def _auto_activate(self):
        today = today_ist()
        if self._auto_done_for == today or not is_trading_day(today):
            return
        if secs_now() < parse_hms(config.LIVE_AUTO_ACTIVATE_TIME):
            return
        self._auto_done_for = today
        from src.live.service import LiveTradeService, LiveValidationError
        for row in store.list_auto_activate():
            try:
                settings = ExecutionSettings.from_dict(row["settings"])
                if weekday_code(today) not in settings.execution_days:
                    continue
                if store.has_active_deployment(row["user_id"], row["strategy_id"], today):
                    continue
                dep = LiveTradeService.create_deployment(row["user_id"], {"strategy_id": row["strategy_id"]})
                await self.activate(dep["deployment_id"])
                logger.info(f"[ENGINE] auto-activated strategy {row['strategy_id']} for user {row['user_id']}")
            except LiveValidationError as e:
                logger.warning(f"[ENGINE] auto-activate skipped strategy {row['strategy_id']} user {row['user_id']}: {e}")
            except Exception as e:
                logger.error(f"[ENGINE] auto-activate failed strategy {row['strategy_id']}: {e}")


    def notify(self, runner: DeploymentRunner):
        self._dirty.add(runner.user_id)


    def ws_register(self, user_id: int, ws: WebSocket):
        self._ws.setdefault(user_id, set()).add(ws)
        self._dirty.add(user_id)


    def ws_unregister(self, user_id: int, ws: WebSocket):
        socks = self._ws.get(user_id)
        if socks:
            socks.discard(ws)
            if not socks:
                self._ws.pop(user_id, None)


    def _payload(self, user_id: int) -> bytes:
        deps = self.snapshots(user_id)
        index_ltp = self.store.ltp(self.index_key) if self.index_key else None
        return orjson.dumps({
            "type": "snapshot", "ts": time.time(), "sensex": index_ltp,
            "total_mtm": round(sum(d["mtm"] for d in deps), 2),
            "deployments": deps,
        }, default=str)


    async def _broadcast_loop(self):
        while True:
            try:
                await asyncio.sleep(1.0)
                if not self._ws:
                    self._dirty.clear()
                    continue
                for user_id, socks in list(self._ws.items()):
                    # users with open positions get a tick every second; idle ones only on change
                    has_live = any(r.active for r in self.runners.values() if r.user_id == user_id)
                    if user_id not in self._dirty and not has_live:
                        continue
                    self._dirty.discard(user_id)
                    payload = self._payload(user_id)
                    for ws in list(socks):
                        try:
                            await ws.send_bytes(payload)
                        except Exception:
                            self.ws_unregister(user_id, ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"[ENGINE] broadcast error: {e}")


    def health(self) -> dict:
        return {
            "status": "ok",
            "uptime_sec": int(time.time() - self.started_at),
            "master_loaded": self.master is not None,
            "master_as_of": str(self.master.as_of) if self.master else None,
            "index_key": self.index_key,
            "sensex_ltp": self.store.ltp(self.index_key) if self.index_key else None,
            "feeds": {k[-4:]: {"connected": f.connected, "remaining_subscriptions": f.remaining_subscriptions}
                      for k, f in self.feeds.items()},
            "brokers": {acct: {"stream": bool(b.stream and b.stream.connected), "session_lost": b.session_lost}
                        for acct, b in self.brokers.items()},
            "runners": {did: r.status for did, r in self.runners.items()},
            "ws_users": len(self._ws),
        }