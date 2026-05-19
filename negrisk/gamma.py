from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from .config import GammaConfig
from .models import NegRiskEvent, NegRiskMarket


ALLOWED_TICKS = {Decimal("0.1"), Decimal("0.01"), Decimal("0.001"), Decimal("0.0001")}


async def fetch_negrisk_events(
    client: httpx.AsyncClient,
    config: GammaConfig,
    *,
    min_liquidity: Decimal,
    max_markets_per_event: int,
    max_events: int,
) -> list[NegRiskEvent]:
    events: list[NegRiskEvent] = []
    offset = 0
    page_size = config.page_size

    while len(events) < max_events:
        params = {
            "negRisk": "true",
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": page_size,
            "offset": offset,
        }
        response = await client.get(f"{config.base_url.rstrip('/')}/events", params=params)
        response.raise_for_status()
        batch = response.json()
        if not isinstance(batch, list):
            raise ValueError("Gamma /events response is not a list")

        for raw in batch:
            event = parse_gamma_event(raw, max_markets_per_event)
            if event is None:
                continue
            if min_liquidity > 0 and event.liquidity is not None and event.liquidity < min_liquidity:
                continue
            events.append(event)
            if len(events) >= max_events:
                break

        if len(batch) < page_size:
            break
        offset += page_size

    return events


def parse_gamma_event(raw: dict[str, Any], max_markets_per_event: int) -> NegRiskEvent | None:
    if not raw.get("negRisk") or not raw.get("active") or raw.get("closed") or raw.get("archived"):
        return None

    tags = raw.get("tags") or []
    if any((tag or {}).get("slug") == "sports" for tag in tags if isinstance(tag, dict)):
        return None

    markets = parse_markets(raw.get("markets") or [], max_markets_per_event)
    if len(markets) < 2:
        return None

    return NegRiskEvent(
        event_id=str(raw.get("id") or ""),
        title=str(raw.get("title") or raw.get("slug") or "Untitled NegRisk event"),
        neg_risk_market_id=str(raw.get("negRiskMarketID") or ""),
        markets=tuple(markets),
        liquidity=parse_decimal(raw.get("liquidity")),
    )


def parse_markets(raw_markets: list[dict[str, Any]], max_markets_per_event: int) -> list[NegRiskMarket]:
    markets: list[NegRiskMarket] = []
    for raw in raw_markets:
        if not raw.get("active") or raw.get("closed"):
            continue

        token_ids = parse_clob_token_ids(raw.get("clobTokenIds"))
        if len(token_ids) < 2 or not token_ids[0] or not token_ids[1]:
            continue

        markets.append(
            NegRiskMarket(
                condition_id=str(raw.get("conditionId") or ""),
                question=str(raw.get("question") or "Untitled outcome"),
                yes_token_id=str(token_ids[0]),
                no_token_id=str(token_ids[1]),
                question_id=str(raw.get("questionID") or ""),
                question_index=parse_int(raw.get("groupItemThreshold")),
                min_tick_size=normalize_tick_size(parse_decimal(raw.get("orderPriceMinTickSize"))),
            )
        )

    if len(markets) > max_markets_per_event:
        return []
    return markets


def parse_clob_token_ids(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(v) for v in parsed]


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def normalize_tick_size(value: Decimal | None) -> Decimal | None:
    if value in ALLOWED_TICKS:
        return value
    return None
