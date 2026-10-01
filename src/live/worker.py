"""The live worker process.

    uvicorn src.live.worker:app --host 127.0.0.1 --port 8001 --workers 1

Runs `LiveEngine` for the lifetime of the process and exposes:
  * POST /internal/deployments/{id}/{activate|pause|resume|squareoff}
  * POST /internal/users/{user_id}/squareoff-all
  * GET  /internal/users/{user_id}/snapshots
  * GET  /internal/health
  * WS   /ws/live?token=<user JWT>   -- 1 Hz snapshots of the user's deployments

The /internal routes are for the API process only: bind to localhost or
set LIVE_INTERNAL_TOKEN and send it as X-Internal-Token. Browsers reach
only /ws/live (proxy it through nginx next to /api). Exactly ONE worker
process must run: it holds the broker sockets and the order state.
"""
from src.core.modules import (
    FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, asynccontextmanager, asyncio, jwt,
)
from src.core import config
from src.core.security import decode_access_token
from src.core.logger import get_logger
from src.live.engine import LiveEngine

logger = get_logger(__name__)

engine = LiveEngine()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(lifespan=lifespan, title="live-worker")


def _internal(request: Request):
    if config.LIVE_INTERNAL_TOKEN and request.headers.get("x-internal-token") != config.LIVE_INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="bad internal token")


@app.get("/internal/health")
async def health():
    return engine.health()


@app.post("/internal/deployments/{deployment_id}/{cmd}")
async def deployment_command(deployment_id: int, cmd: str, request: Request):
    _internal(request)
    try:
        return {"status": True, "data": await engine.command(deployment_id, cmd)}
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"[WORKER] {cmd} {deployment_id} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/users/{user_id}/squareoff-all")
async def squareoff_all(user_id: int, request: Request):
    _internal(request)
    return {"status": True, "data": await engine.squareoff_all(user_id)}


@app.get("/internal/users/{user_id}/snapshots")
async def snapshots(user_id: int, request: Request):
    _internal(request)
    return {"status": True, "data": engine.snapshots(user_id)}


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket, token: str = ""):
    """Browser stream. Auth = the same JWT the API issues, passed as ?token=.
    The client may send 'ping' (any text) to keep proxies from idling out."""
    try:
        claims = decode_access_token(token)
        user_id = int(claims["sub"])
    except (jwt.InvalidTokenError, KeyError, ValueError):
        await ws.close(code=4401)
        return
    await ws.accept()
    engine.ws_register(user_id, ws)
    try:
        await ws.send_bytes(engine._payload(user_id))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"[WS] user {user_id} closed: {e}")
    finally:
        engine.ws_unregister(user_id, ws)