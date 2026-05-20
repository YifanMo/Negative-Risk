from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sys

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from negrisk.config import AppConfig
from negrisk.engine import NegativeRiskEngine
from negrisk.historical_replay import BOOK_SNAPSHOT_SOURCE, HistoricalReplayService, normalized_replay_params


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

config = AppConfig.load_default()
engine = NegativeRiskEngine(config)
history_replay = HistoricalReplayService(config, book_store=engine.book_history)


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


@app.get("/monitor")
async def monitor() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.head("/monitor")
async def monitor_head() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/history")
async def history_page() -> FileResponse:
    return FileResponse(WEB_DIR / "history.html")


@app.head("/history")
async def history_head() -> FileResponse:
    return FileResponse(WEB_DIR / "history.html")


@app.get("/api/status")
async def status() -> dict:
    return engine.status()


@app.get("/api/opportunities")
async def opportunities() -> list[dict]:
    return engine.get_opportunities()


@app.get("/api/events")
async def events() -> list[dict]:
    return engine.get_events_summary()


@app.get("/api/events/{event_id}")
async def event_detail(event_id: str) -> dict:
    detail = engine.get_event_detail(event_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return detail


def replay_params(
    hours: int,
    fidelity: int,
    investment_usd: float,
    slippage_pct: float,
    source: str = BOOK_SNAPSHOT_SOURCE,
):
    return normalized_replay_params(
        hours=hours,
        fidelity_minutes=fidelity,
        investment_usd=investment_usd,
        slippage_pct=slippage_pct,
        source=source,
    )


@app.get("/api/history/replay")
async def history_replay_api(
    hours: int = Query(default=config.history.lookback_hours, ge=1, le=168),
    fidelity: int = Query(default=config.history.fidelity_minutes, ge=1, le=60),
    investment_usd: float = Query(default=1000.0, gt=0),
    slippage_pct: float = Query(default=0.5, ge=0, le=20),
    source: str = Query(default=BOOK_SNAPSHOT_SOURCE),
    refresh: bool = Query(default=False),
) -> dict:
    return await history_replay.replay(
        replay_params(hours, fidelity, investment_usd, slippage_pct, source),
        force_refresh=refresh,
    )


@app.get("/api/history/summary")
async def history_summary_api(
    hours: int = Query(default=config.history.lookback_hours, ge=1, le=168),
    fidelity: int = Query(default=config.history.fidelity_minutes, ge=1, le=60),
    investment_usd: float = Query(default=1000.0, gt=0),
    slippage_pct: float = Query(default=0.5, ge=0, le=20),
    source: str = Query(default=BOOK_SNAPSHOT_SOURCE),
) -> dict:
    payload = await history_replay.replay(replay_params(hours, fidelity, investment_usd, slippage_pct, source))
    return payload["summary"]


@app.get("/api/history/top-events")
async def history_top_events_api(
    hours: int = Query(default=config.history.lookback_hours, ge=1, le=168),
    fidelity: int = Query(default=config.history.fidelity_minutes, ge=1, le=60),
    investment_usd: float = Query(default=1000.0, gt=0),
    slippage_pct: float = Query(default=0.5, ge=0, le=20),
    source: str = Query(default=BOOK_SNAPSHOT_SOURCE),
    limit: int = Query(default=10, ge=1, le=100),
) -> list[dict]:
    payload = await history_replay.replay(replay_params(hours, fidelity, investment_usd, slippage_pct, source))
    return payload["top_events"][:limit]


@app.get("/api/history/hourly")
async def history_hourly_api(
    hours: int = Query(default=config.history.lookback_hours, ge=1, le=168),
    fidelity: int = Query(default=config.history.fidelity_minutes, ge=1, le=60),
    investment_usd: float = Query(default=1000.0, gt=0),
    slippage_pct: float = Query(default=0.5, ge=0, le=20),
    source: str = Query(default=BOOK_SNAPSHOT_SOURCE),
) -> list[dict]:
    payload = await history_replay.replay(replay_params(hours, fidelity, investment_usd, slippage_pct, source))
    return payload["hourly"]


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
        pass
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
