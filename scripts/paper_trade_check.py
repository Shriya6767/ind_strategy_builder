"""Drive one PAPER deployment through the real API + worker and tail its
event log in the terminal.

    venv\\Scripts\\python scripts\\paper_trade_check.py <email> <password> <strategy_id> [api_url]

Prerequisites (two other terminals):
    uvicorn src.main:app --host 0.0.0.0 --port 8000
    uvicorn src.live.worker:app --host 127.0.0.1 --port 8001 --workers 1
and LIVE_FEED_* set in .env (verified by scripts/xts_feed_check.py).

The strategy must be intraday or btst without lazy/sequential/range/momentum
legs, and its exit_time must still be ahead of the clock today; if its
entry_time has already passed the worker enters immediately.
"""
import sys
import time

import httpx

email, password, strategy_id = sys.argv[1], sys.argv[2], int(sys.argv[3])
api = (sys.argv[4] if len(sys.argv) > 4 else "http://127.0.0.1:8000/api").rstrip("/")
http = httpx.Client(base_url=api, timeout=30)


def call(method, path, **kw):
    try:
        r = http.request(method, path, **kw)
    except httpx.RemoteProtocolError:
        # uvicorn closes idle keep-alive connections after 5 s; retry once on a fresh one
        r = http.request(method, path, **kw)
    try:
        body = r.json()
    except ValueError:
        body = {"raw": r.text}
    if r.status_code >= 400:
        detail = body.get("detail", body)
        sys.exit(f"{method} {path} -> HTTP {r.status_code}: {detail.get('message') if isinstance(detail, dict) else detail}")
    return body


# 1. login
tok = call("POST", "/auth/login", json={"email": email, "password": password})
token = tok.get("access_token") or (tok.get("data") or {}).get("access_token")
if not token:
    sys.exit(f"login returned no access_token: {tok}")
http.headers["Authorization"] = f"Bearer {token}"
print("login ok")

# 2. worker reachable?
try:
    h = httpx.get(api.replace("/api", "").replace(":8000", ":8001") + "/internal/health", timeout=5).json()
    print(f"worker: master_loaded={h.get('master_loaded')} sensex={h.get('sensex_ltp')} feeds={h.get('feeds')}")
except Exception as e:
    print(f"worker health check failed ({e}) -- is the worker running on :8001?")

# 3. paper execution settings
settings = call("PUT", "/live/execution-settings", json={
    "strategy_id": strategy_id,
    "settings": {"mode": "paper", "entry_order_type": "LIMIT", "exit_order_type": "LIMIT",
                 "entry_limit_buffer": 3, "exit_limit_buffer": 3, "trade_monitoring": "LTP",
                 "order_timeout_sec": 50, "execution_days": ["M", "T", "W", "Th", "F"]},
})
print("execution settings saved:", settings["data"]["settings"]["mode"])

# 4. activate
dep = call("POST", "/live/deployments", json={"strategy_id": strategy_id})["data"]
dep_id = dep["deployment_id"]
print(f"deployment {dep_id} created: status={dep['status']} worker={dep.get('worker')}")

# 5. tail events + legs until the deployment finishes
seen = set()
last_status = None
while True:
    d = call("GET", f"/live/deployments/{dep_id}")["data"]
    if d["status"] != last_status:
        print(f"\n=== status: {d['status']} ({d.get('status_reason')})  realised {d['realised_pnl']:+.2f}")
        last_status = d["status"]
    for ev in reversed(d["events"]):                    # API returns newest first
        key = (ev["created_at"], ev["message"])
        if key not in seen:
            seen.add(key)
            print(f"{str(ev['created_at'])[11:19]} [{ev['level']}] {ev['message']}")
    if d["legs"]:
        row = " | ".join(f"L{l['leg_number']}#{l['attempt']} {l['status']} {l['side']} {l.get('symbol') or ''} "
                         f"in={l.get('entry_price')} sl={l.get('stoploss_price')} tgt={l.get('target_price')} "
                         f"out={l.get('exit_price')} pnl={l.get('pnl')}" for l in d["legs"])
        print("   legs:", row)
    if d["status"] in ("squared_off", "completed", "error", "cancelled"):
        print("\nfinished. orders logged:", len(d["orders"]))
        break
    time.sleep(5)
