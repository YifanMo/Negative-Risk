from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sys

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from negrisk.config import AppConfig
from negrisk.engine import NegativeRiskEngine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

config = AppConfig.load_default()
engine = NegativeRiskEngine(config)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(title="Negative-Risk Local Monitor", version="0.1.0", lifespan=lifespan)

WEB_DIR = ROOT / "web"
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.head("/")
async def index_head() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/api/status")
async def status() -> dict:
    return engine.status()


@app.get("/api/opportunities")
async def opportunities() -> list[dict]:
    return engine.get_opportunities()


@app.get("/api/events/{event_id}")
async def event_detail(event_id: str) -> dict:
    detail = engine.get_event_detail(event_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return detail


@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    queue = await engine.subscribe()
    try:
        while True:
            payload = await queue.get()
            await websocket.send_json(payload)
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    finally:
        engine.unsubscribe(queue)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Negative-Risk local monitor")
    parser.add_argument("--host", default=config.server.host)
    parser.add_argument("--port", default=config.server.port, type=int)
    args = parser.parse_args()

    import uvicorn

    uvicorn.run("api.server:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
