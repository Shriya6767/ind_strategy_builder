"""Measure how many ticks per second the market data server pushes per instrument.

    venv\\Scripts\\python scripts\\xts_tick_rate.py                 # SENSEX + weekly ATM CE & PE, 60 s
    venv\\Scripts\\python scripts\\xts_tick_rate.py 120             # 120 s
    venv\\Scripts\\python scripts\\xts_tick_rate.py 120 73800CE     # add specific weekly strikes

STOP THE LIVE WORKER FIRST: Symphony allows one market data session per app
key, so this script and the worker would keep logging each other out.

Prints one line per second with the tick count of each instrument in that
second, then a summary: total ticks, average and peak ticks/second, and the
longest gap without a tick (seconds). Run during market hours.
"""
import asyncio
import os
import sys
import time

os.environ.setdefault("DATA_PRELOAD", "false")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core import config
from src.live.broker_store import _split_url
from src.live.feed import TickStore, XTSMarketFeed
from src.live.instrument_master import InstrumentMaster
from src.live.strike_resolver import ladder_atm
from src.live.timeutil import today_ist, now_ist
from src.live.xts_client import SEG_BSECM, MARKETDATA_PATH


async def main(seconds: int, extra: list[str]):
    origin, _ = _split_url(config.LIVE_FEED_ROOT, MARKETDATA_PATH)
    store = TickStore()
    feed = XTSMarketFeed(origin, MARKETDATA_PATH, config.LIVE_FEED_APP_KEY, config.LIVE_FEED_SECRET, store,
                         config.LIVE_FEED_PUBLISH_FORMAT, name="rate")
    await feed.start()
    master = await InstrumentMaster.load(feed.client, config.LIVE_MASTER_CACHE_DIR, today_ist(), config.LIVE_SENSEX_INDEX_ID)
    index_key = (SEG_BSECM, master.index_instrument_id)
    await feed.subscribe([index_key])
    for _ in range(50):
        if store.ltp(index_key):
            break
        await asyncio.sleep(0.1)
    spot = store.ltp(index_key)
    expiry = master.resolve_expiries(today_ist()).get("weekly")
    atm = ladder_atm(spot, master.ladder_step(expiry, "CE"))
    wanted = [(atm, "CE"), (atm, "PE")] + [(int(s[:-2]), s[-2:].upper()) for s in extra]
    names = {index_key: "SENSEX"}
    for strike, typ in wanted:
        c = master.contract(expiry, strike, typ)
        if c:
            names[(c.segment, c.instrument_id)] = c.symbol
    keys = [k for k in names if k != index_key]
    await feed.subscribe(keys)
    print(f"{origin}{MARKETDATA_PATH}  spot {spot}  {', '.join(names.values())}  ({now_ist():%Y-%m-%d %H:%M:%S} IST)")

    per_sec = {k: 0 for k in names}
    total = {k: 0 for k in names}
    peak = {k: 0 for k in names}
    last_tick = {k: time.time() for k in names}
    gap = {k: 0.0 for k in names}

    def on_tick(key, ltp, ts):
        now = time.time()
        per_sec[key] += 1
        total[key] += 1
        gap[key] = max(gap[key], now - last_tick[key])
        last_tick[key] = now

    for k in names:
        store.on_tick(k, on_tick)

    print("   time      " + "  ".join(f"{n[-16:]:>16}" for n in names.values()))
    for _ in range(seconds):
        await asyncio.sleep(1)
        print(f"   {now_ist():%H:%M:%S}  " + "  ".join(f"{per_sec[k]:>16}" for k in names))
        for k in names:
            peak[k] = max(peak[k], per_sec[k])
            per_sec[k] = 0

    print("\nsummary over", seconds, "s")
    for k, n in names.items():
        print(f"   {n:<28} total={total[k]:>5}  avg/s={total[k] / seconds:>5.2f}  peak/s={peak[k]:>3}  longest gap={gap[k]:>5.1f}s")
    await feed.unsubscribe(list(names))
    await feed.stop()


if __name__ == "__main__":
    args = sys.argv[1:]
    secs = int(args[0]) if args and args[0].isdigit() else 60
    asyncio.run(main(secs, [a for a in args if not a.isdigit()]))
