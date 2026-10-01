"""Market data smoke test for the live feed. Needs only LIVE_FEED_ROOT /
LIVE_FEED_APP_KEY / LIVE_FEED_SECRET in .env (no broker account).

    venv\\Scripts\\python scripts\\xts_feed_check.py            # 30 s of ticks
    venv\\Scripts\\python scripts\\xts_feed_check.py 120        # 2 minutes

Steps, each printed with OK / FAIL:
  1. login to the Market Data API
  2. download the BSEFO contract master, count SENSEX options, list expiries
  3. resolve the SENSEX index instrument id from the BSECM index list
  4. open the Socket.IO stream
  5. subscribe to the index + this week's ATM CE/PE and print live LTPs
Run during market hours (09:15-15:30 IST) to see ticks; outside hours the
subscribe snapshot still shows the last traded prices.
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
from src.live.xts_client import SEG_BSECM, SEG_BSEFO, XTSError


def ok(msg):
    print(f"  OK   {msg}")


def fail(msg):
    print(f"  FAIL {msg}")
    sys.exit(1)


async def main(seconds: int):
    if not (config.LIVE_FEED_ROOT and config.LIVE_FEED_APP_KEY and config.LIVE_FEED_SECRET):
        fail("LIVE_FEED_ROOT / LIVE_FEED_APP_KEY / LIVE_FEED_SECRET are not all set in .env")
    origin, path = _split_url(config.LIVE_FEED_ROOT, "/apimarketdata")
    print(f"Market data server: {origin}{path}   ({now_ist():%Y-%m-%d %H:%M:%S} IST)")

    store = TickStore()
    feed = XTSMarketFeed(origin, path, config.LIVE_FEED_APP_KEY, config.LIVE_FEED_SECRET, store,
                         config.LIVE_FEED_PUBLISH_FORMAT, name="check")

    # 1. login (part of feed.start) -- do it separately first for a clear message
    print("1. login")
    try:
        result = await feed.client.login(config.LIVE_FEED_APP_KEY, config.LIVE_FEED_SECRET)
    except XTSError as e:
        fail(f"login rejected: {e}")
    ok(f"userID={result.get('userID')} appVersion={result.get('appVersion')} expires={result.get('application_expiry_date')}")

    # 2. master
    print("2. contract master (BSEFO)")
    try:
        master = await InstrumentMaster.load(feed.client, config.LIVE_MASTER_CACHE_DIR, today_ist(),
                                             config.LIVE_SENSEX_INDEX_ID)
    except Exception as e:
        fail(f"master: {e}")
    ok(f"{len(master.by_id)} SENSEX option contracts, lot size {master.lot_size}")
    ok(f"expiries: {', '.join(str(e) for e in master.expiries[:6])} ...")
    labels = master.resolve_expiries(today_ist())
    ok(f"resolved labels: { {k: str(v) for k, v in labels.items()} }")

    # 3. index id
    print("3. SENSEX index instrument (BSECM index list)")
    if not master.index_instrument_id:
        fail("SENSEX not found in the BSECM index list -- set LIVE_SENSEX_INDEX_ID in .env")
    ok(f"index id = {master.index_instrument_id}")
    index_key = (SEG_BSECM, master.index_instrument_id)

    # 4. socket
    print("4. market data socket")
    await feed.client.logout()                       # feed.start() logs in again (one session per key)
    try:
        await feed.start()
    except Exception as e:
        fail(f"socket: {e}")
    ok(f"connected via socket.io (publishFormat={feed.publish_format})")

    # 5. subscribe index, wait for spot, then ATM CE/PE of the weekly expiry
    print("5. subscriptions + ticks")
    await feed.subscribe([index_key])
    spot = None
    for _ in range(50):
        spot = store.ltp(index_key)
        if spot:
            break
        await asyncio.sleep(0.1)
    if not spot:
        fail("no SENSEX LTP received (snapshot empty and no tick in 5 s)")
    ok(f"SENSEX spot = {spot}")

    expiry = labels.get("weekly")
    step = master.ladder_step(expiry, "CE")
    atm = ladder_atm(spot, step)
    ce, pe = master.contract(expiry, atm, "CE"), master.contract(expiry, atm, "PE")
    if not ce or not pe:
        fail(f"ATM {atm} not listed for {expiry} (strike step {step})")
    keys = [(ce.segment, ce.instrument_id), (pe.segment, pe.instrument_id)]
    await feed.subscribe(keys)
    ok(f"subscribed {ce.symbol} (id {ce.instrument_id}) and {pe.symbol} (id {pe.instrument_id}); "
       f"remaining subscription count = {feed.remaining_subscriptions}")

    counts = {index_key: 0, keys[0]: 0, keys[1]: 0}

    def on_tick(key, ltp, ts):
        counts[key] += 1

    for k in counts:
        store.on_tick(k, on_tick)

    print(f"   streaming for {seconds}s ...")
    end = time.time() + seconds
    while time.time() < end:
        await asyncio.sleep(5)
        c = store.quote(keys[0]); p = store.quote(keys[1])
        print(f"   {now_ist():%H:%M:%S}  SENSEX {store.ltp(index_key)}  |  {ce.strike} CE ltp={c[0]} bid={c[1]} ask={c[2]}"
              f"  |  {pe.strike} PE ltp={p[0]} bid={p[1]} ask={p[2]}  |  ticks so far {list(counts.values())}")

    total = sum(counts.values())
    if total == 0:
        print("  WARN no ticks arrived while streaming. Outside market hours this is normal; during hours check the "
              "socket path / publishFormat (the snapshot LTPs above came from REST, so login and master are fine).")
    else:
        ok(f"{total} ticks received in {seconds}s -- feed is working")

    await feed.unsubscribe(keys + [index_key])
    await feed.stop()
    print("done")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 30))
