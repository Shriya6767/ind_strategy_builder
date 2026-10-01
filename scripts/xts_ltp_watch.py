"""Print every LTP tick the market data server pushes, as it arrives, for
cross-checking against TradingView / the broker terminal.

    venv\\Scripts\\python scripts\\xts_ltp_watch.py                       # SENSEX + weekly ATM CE & PE, 120 s
    venv\\Scripts\\python scripts\\xts_ltp_watch.py 300                   # 300 s
    venv\\Scripts\\python scripts\\xts_ltp_watch.py 300 73800CE 73900PE   # specific weekly strikes
    venv\\Scripts\\python scripts\\xts_ltp_watch.py 300 73800CE --raw     # also dump the raw 1501 packet

STOP THE LIVE WORKER FIRST: Symphony allows one market data session per app
key, so this script and the worker would keep logging each other out.

Each line:  <IST clock of arrival>  <symbol>  ltp=..  bid=..  ask=..  (+ change vs previous tick)
"""
import asyncio
import os
import sys
import time

os.environ.setdefault("DATA_PRELOAD", "false")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core import config
from src.core.modules import orjson
from src.live.broker_store import _split_url
from src.live.feed import TickStore, XTSMarketFeed
from src.live.instrument_master import InstrumentMaster
from src.live.strike_resolver import ladder_atm
from src.live.timeutil import today_ist, now_ist
from src.live.xts_client import SEG_BSECM, XTSError


def parse_args(argv):
    seconds, raw, picks = 120, False, []
    for a in argv:
        if a == "--raw":
            raw = True
        elif a.isdigit():
            seconds = int(a)
        else:
            picks.append(a.upper())
    return seconds, raw, picks


async def main():
    seconds, raw, picks = parse_args(sys.argv[1:])
    if not (config.LIVE_FEED_ROOT and config.LIVE_FEED_APP_KEY and config.LIVE_FEED_SECRET):
        sys.exit("LIVE_FEED_ROOT / LIVE_FEED_APP_KEY / LIVE_FEED_SECRET must be set in .env")
    origin, path = _split_url(config.LIVE_FEED_ROOT, "/apibinarymarketdata")

    store = TickStore()
    feed = XTSMarketFeed(origin, path, config.LIVE_FEED_APP_KEY, config.LIVE_FEED_SECRET, store,
                         config.LIVE_FEED_PUBLISH_FORMAT, name="watch")
    await feed.start()
    master = await InstrumentMaster.load(feed.client, config.LIVE_MASTER_CACHE_DIR, today_ist(), config.LIVE_SENSEX_INDEX_ID)
    if not master.index_instrument_id:
        sys.exit("SENSEX index id not found -- set LIVE_SENSEX_INDEX_ID")
    index_key = (SEG_BSECM, master.index_instrument_id)
    names = {index_key: "SENSEX"}

    await feed.subscribe([index_key])
    for _ in range(50):
        if store.ltp(index_key):
            break
        await asyncio.sleep(0.1)
    spot = store.ltp(index_key)
    expiry = master.resolve_expiries(today_ist())["weekly"]

    contracts = []
    if picks:
        for p in picks:
            strike, opt = int(p[:-2]), p[-2:]
            c = master.contract(expiry, strike, opt)
            if c is None:
                sys.exit(f"{p} not listed for {expiry}")
            contracts.append(c)
    else:
        atm = ladder_atm(spot, master.ladder_step(expiry, "CE"))
        contracts = [master.contract(expiry, atm, "CE"), master.contract(expiry, atm, "PE")]
    keys = [(c.segment, c.instrument_id) for c in contracts]
    for c in contracts:
        names[(c.segment, c.instrument_id)] = c.symbol
    await feed.subscribe(keys)

    print(f"{now_ist():%H:%M:%S} IST  spot={spot}  expiry={expiry}  watching: {', '.join(names.values())}")
    print("-" * 100)

    last = {}
    counts = {k: 0 for k in names}

    def on_tick(key, ltp, ts):
        counts[key] += 1
        _, bid, ask = store.quote(key)
        prev = last.get(key)
        delta = f"  ({ltp - prev:+.2f})" if prev is not None and ltp != prev else ""
        last[key] = ltp
        print(f"{now_ist():%H:%M:%S.%f}"[:-3] + f"  {names[key]:<28} ltp={ltp:<10} bid={bid!s:<9} ask={ask!s:<9}{delta}")

    for k in names:
        store.on_tick(k, on_tick)

    if raw:
        feed.sio.on("1501-json-full", lambda d: print("   RAW", orjson.dumps(orjson.loads(d) if isinstance(d, (str, bytes)) else d).decode()[:400]))

    end = time.time() + seconds
    while time.time() < end:
        await asyncio.sleep(1)

    print("-" * 100)
    total = sum(counts.values())
    print(f"{seconds}s: {total} ticks -- " + ", ".join(f"{names[k]}: {n} ({n / seconds:.1f}/s)" for k, n in counts.items()))
    await feed.unsubscribe(keys + [index_key])
    await feed.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
