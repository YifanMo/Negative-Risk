from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from typing import Any

import httpx
import websockets

from .config import AppConfig
from .gamma import fetch_negrisk_events
from .models import NegRiskEvent
from .orderbook import LocalBook, levels_from_payload, parse_decimal
from .signal import check_event


LOG = logging.getLogger(__name__)


@dataclass
class ShardStatus:
    index: int
    token_count: int
    connected: bool = False
    last_message_at: datetime | None = None
    reconnects: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "token_count": self.token_count,
            "connected": self.connected,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "reconnects": self.reconnects,
            "error": self.error,
        }


@dataclass
class EngineState:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    running: bool = False
    loading: bool = True
    last_event_refresh_at: datetime | None = None
    last_scan_at: datetime | None = None
    last_error: str | None = None
    event_count: int = 0
    token_count: int = 0


class NegativeRiskEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state = EngineState()
        self.events: dict[str, NegRiskEvent] = {}
        self.books: dict[str, LocalBook] = {}
        self.token_to_events: dict[str, set[str]] = {}
        self.opportunities: dict[str, dict[str, Any]] = {}
        self.shards: dict[int, ShardStatus] = {}
        self._task: asyncio.Task | None = None
        self._refresh_task: asyncio.Task | None = None
        self._scan_task: asyncio.Task | None = None
        self._ws_tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue] = set()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self.state.running = True
        self._task = asyncio.create_task(self._run(), name="negrisk-engine")

    async def stop(self) -> None:
        self.state.running = False
        tasks = [t for t in [self._task, self._refresh_task, self._scan_task] if t]
        tasks.extend(self._ws_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._ws_tasks.clear()

    async def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=5)
        self._subscribers.add(queue)
        await queue.put(self.dashboard_payload())
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def status(self) -> dict[str, Any]:
        connected = sum(1 for shard in self.shards.values() if shard.connected)
        return {
            "running": self.state.running,
            "loading": self.state.loading,
            "started_at": self.state.started_at.isoformat(),
            "last_event_refresh_at": self.state.last_event_refresh_at.isoformat()
            if self.state.last_event_refresh_at
            else None,
            "last_scan_at": self.state.last_scan_at.isoformat() if self.state.last_scan_at else None,
            "last_error": self.state.last_error,
            "event_count": self.state.event_count,
            "token_count": self.state.token_count,
            "opportunity_count": len(self.opportunities),
            "execution_enabled": False,
            "shards": {
                "connected": connected,
                "total": len(self.shards),
                "items": [s.to_dict() for s in sorted(self.shards.values(), key=lambda s: s.index)],
            },
            "config": {
                "min_gross_profit": float(self.config.arb.min_gross_profit),
                "min_total_usd": float(self.config.arb.min_total_usd),
                "max_depth_pct": float(self.config.arb.max_depth_pct),
                "ws_chunk_size": self.config.engine.ws_chunk_size,
            },
        }

    def dashboard_payload(self) -> dict[str, Any]:
        return {
            "type": "dashboard",
            "status": self.status(),
            "opportunities": self.get_opportunities(),
        }

    def get_opportunities(self) -> list[dict[str, Any]]:
        return sorted(
            self.opportunities.values(),
            key=lambda opp: (opp["total_usd"], opp["gross_profit"]),
            reverse=True,
        )

    def get_event_detail(self, event_id: str) -> dict[str, Any] | None:
        event = self.events.get(event_id)
        if event is None:
            return None
        markets: list[dict[str, Any]] = []
        for idx, market in enumerate(event.markets):
            yes_book = self.books.get(market.yes_token_id, LocalBook())
            no_book = self.books.get(market.no_token_id, LocalBook())
            markets.append(
                {
                    "index": idx,
                    "question": market.question,
                    "condition_id": market.condition_id,
                    "question_index": market.question_index,
                    "yes_token_id": market.yes_token_id,
                    "no_token_id": market.no_token_id,
                    "min_tick_size": float(market.min_tick_size) if market.min_tick_size else None,
                    "yes_book": yes_book.to_dict(),
                    "no_book": no_book.to_dict(),
                }
            )
        return {
            "event_id": event.event_id,
            "title": event.title,
            "neg_risk_market_id": event.neg_risk_market_id,
            "liquidity": float(event.liquidity) if event.liquidity is not None else None,
            "markets": markets,
            "opportunity": self.opportunities.get(event_id),
        }

    async def _run(self) -> None:
        try:
            await self._refresh_events_once()
            self._refresh_task = asyncio.create_task(self._refresh_loop(), name="negrisk-refresh")
            self._scan_task = asyncio.create_task(self._scan_loop(), name="negrisk-scan")
            self.state.loading = False
            await self._publish()
            await asyncio.Future()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.state.last_error = str(exc)
            self.state.loading = False
            LOG.exception("NegativeRiskEngine stopped after error")
            await self._publish()

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.engine.event_refresh_secs)
            try:
                await self._refresh_events_once()
                await self._publish()
            except Exception as exc:
                self.state.last_error = f"event refresh failed: {exc}"
                LOG.exception("event refresh failed")
                await self._publish()

    async def _scan_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.engine.scan_interval_ms / 1000)
            changed = await self._scan_once()
            if changed:
                await self._publish()

    async def _refresh_events_once(self) -> None:
        timeout = httpx.Timeout(30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            events = await fetch_negrisk_events(
                client,
                self.config.gamma,
                min_liquidity=self.config.engine.min_event_liquidity,
                max_markets_per_event=self.config.engine.max_markets_per_event,
                max_events=self.config.engine.max_events,
            )

        async with self._lock:
            old_tokens = set(self.books)
            self.events = {event.event_id: event for event in events}
            active_tokens: set[str] = set()
            self.token_to_events.clear()
            for event in events:
                for market in event.markets:
                    active_tokens.add(market.yes_token_id)
                    active_tokens.add(market.no_token_id)
                    self.books.setdefault(market.yes_token_id, LocalBook(tick_size=market.min_tick_size))
                    self.books.setdefault(market.no_token_id, LocalBook())
                    self.token_to_events.setdefault(market.yes_token_id, set()).add(event.event_id)
                    self.token_to_events.setdefault(market.no_token_id, set()).add(event.event_id)
                    if market.min_tick_size is not None:
                        self.books[market.yes_token_id].tick_size = market.min_tick_size

            for token_id in set(self.books) - active_tokens:
                self.books.pop(token_id, None)

            self.opportunities = {
                event_id: opp for event_id, opp in self.opportunities.items() if event_id in self.events
            }
            self.state.event_count = len(self.events)
            self.state.token_count = len(active_tokens)
            self.state.last_event_refresh_at = datetime.now(timezone.utc)
            self.state.last_error = None

        if active_tokens != old_tokens:
            await self._restart_websockets(sorted(active_tokens))

    async def _restart_websockets(self, token_ids: list[str]) -> None:
        for task in self._ws_tasks:
            task.cancel()
        if self._ws_tasks:
            await asyncio.gather(*self._ws_tasks, return_exceptions=True)
        self._ws_tasks.clear()
        self.shards.clear()

        chunk_size = max(1, self.config.engine.ws_chunk_size)
        chunks = [token_ids[i : i + chunk_size] for i in range(0, len(token_ids), chunk_size)]
        for idx, chunk in enumerate(chunks):
            self.shards[idx] = ShardStatus(index=idx, token_count=len(chunk))
            self._ws_tasks.append(asyncio.create_task(self._ws_loop(idx, chunk), name=f"negrisk-ws-{idx}"))

    async def _ws_loop(self, shard_idx: int, token_ids: list[str]) -> None:
        while True:
            status = self.shards[shard_idx]
            try:
                async with websockets.connect(
                    self.config.clob.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=8 * 1024 * 1024,
                    max_queue=2048,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "assets_ids": token_ids,
                                "type": "market",
                                "custom_feature_enabled": True,
                            }
                        )
                    )
                    status.connected = True
                    status.error = None
                    await self._publish()
                    async for message in ws:
                        status.last_message_at = datetime.now(timezone.utc)
                        if message == "PING":
                            await ws.send("PONG")
                            continue
                        await self._handle_ws_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status.connected = False
                status.reconnects += 1
                status.error = str(exc)
                self._clear_tokens(token_ids)
                self.state.last_error = f"websocket shard {shard_idx} failed: {exc}"
                await self._publish()
                await asyncio.sleep(self.config.engine.ws_reconnect_secs)

    async def _handle_ws_message(self, message: str | bytes) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        items = payload if isinstance(payload, list) else [payload]
        async with self._lock:
            for item in items:
                if isinstance(item, dict):
                    self._apply_market_message(item)

    def _apply_market_message(self, item: dict[str, Any]) -> None:
        event_type = str(item.get("event_type") or item.get("type") or "").lower()
        token_id = str(item.get("asset_id") or item.get("assetId") or item.get("token_id") or "")
        if not token_id and isinstance(item.get("market"), str):
            token_id = str(item.get("market"))

        if event_type == "book":
            token_id = token_id or str(item.get("asset_id") or "")
            if not token_id:
                return
            book = self.books.setdefault(token_id, LocalBook())
            book.apply_snapshot(levels_from_payload(item.get("bids") or []), levels_from_payload(item.get("asks") or []))
            return

        if event_type == "price_change":
            changes = item.get("price_changes") or item.get("changes") or []
            for change in changes:
                asset_id = str(change.get("asset_id") or change.get("assetId") or change.get("token_id") or "")
                price = parse_decimal(change.get("price"))
                size = parse_decimal(change.get("size"))
                side = str(change.get("side") or "")
                if not asset_id or price is None or size is None:
                    continue
                self.books.setdefault(asset_id, LocalBook()).apply_delta(side, price, size)
            return

        if event_type == "tick_size_change":
            token_id = token_id or str(item.get("asset_id") or "")
            tick = parse_decimal(item.get("new_tick_size") or item.get("tick_size") or item.get("minimum_tick_size"))
            if token_id and tick is not None:
                self.books.setdefault(token_id, LocalBook()).tick_size = tick
            return

        if event_type == "best_bid_ask":
            token_id = token_id or str(item.get("asset_id") or "")
            bid = parse_decimal(item.get("best_bid") or item.get("bid"))
            ask = parse_decimal(item.get("best_ask") or item.get("ask"))
            if token_id:
                self.books.setdefault(token_id, LocalBook()).apply_best_bid_ask(bid, ask)

    def _clear_tokens(self, token_ids: list[str]) -> None:
        for token_id in token_ids:
            book = self.books.get(token_id)
            if book is not None:
                book.clear()

    async def _scan_once(self) -> bool:
        changed = False
        async with self._lock:
            next_opportunities: dict[str, dict[str, Any]] = {}
            for event in self.events.values():
                opportunity = check_event(
                    event,
                    self.books,
                    min_gross_profit=self.config.arb.min_gross_profit,
                    min_total_usd=self.config.arb.min_total_usd,
                    max_depth_pct=self.config.arb.max_depth_pct,
                )
                if opportunity:
                    next_opportunities[event.event_id] = opportunity.to_dict()

            if json.dumps(next_opportunities, sort_keys=True) != json.dumps(self.opportunities, sort_keys=True):
                changed = True
            self.opportunities = next_opportunities
            self.state.last_scan_at = datetime.now(timezone.utc)
        return changed

    async def _publish(self) -> None:
        if not self._subscribers:
            return
        payload = self.dashboard_payload()
        for queue in list(self._subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass
