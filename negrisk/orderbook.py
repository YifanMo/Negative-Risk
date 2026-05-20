from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping, Any


def parse_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


@dataclass
class LocalBook:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    tick_size: Decimal | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.updated_at = datetime.min.replace(tzinfo=timezone.utc)

    def apply_snapshot(
        self,
        bids: Iterable[tuple[Decimal, Decimal]],
        asks: Iterable[tuple[Decimal, Decimal]],
    ) -> None:
        self.bids = {p: s for p, s in bids if s > 0}
        self.asks = {p: s for p, s in asks if s > 0}
        self.updated_at = datetime.now(timezone.utc)

    def apply_delta(self, side: str, price: Decimal, size: Decimal) -> None:
        target = self.bids if side.lower() in {"buy", "bid", "bids"} else self.asks
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
        self.updated_at = datetime.now(timezone.utc)

    def apply_best_bid_ask(self, best_bid: Decimal | None, best_ask: Decimal | None) -> None:
        if best_bid is not None:
            self.bids[best_bid] = max(self.bids.get(best_bid, Decimal("0")), Decimal("1"))
        if best_ask is not None:
            self.asks[best_ask] = max(self.asks.get(best_ask, Decimal("0")), Decimal("1"))
        self.updated_at = datetime.now(timezone.utc)

    @property
    def is_empty(self) -> bool:
        return not self.bids and not self.asks

    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    def ask_depth_up_to(self, max_price: Decimal) -> Decimal:
        return sum(size for price, size in self.asks.items() if price <= max_price)

    def bid_depth_at_or_above(self, min_price: Decimal) -> Decimal:
        return sum(size for price, size in self.bids.items() if price >= min_price)

    def total_bid_depth(self) -> Decimal:
        return sum(self.bids.values(), Decimal("0"))

    def total_ask_depth(self) -> Decimal:
        return sum(self.asks.values(), Decimal("0"))

    def to_dict(self, *, level_limit: int = 5) -> dict[str, Any]:
        return {
            "best_bid": _float(self.best_bid()),
            "best_ask": _float(self.best_ask()),
            "bid_depth": _float(self.total_bid_depth()),
            "ask_depth": _float(self.total_ask_depth()),
            "bid_levels": _levels(self.bids.items(), reverse=True, limit=level_limit),
            "ask_levels": _levels(self.asks.items(), reverse=False, limit=level_limit),
            "tick_size": _float(self.tick_size),
            "updated_at": self.updated_at.isoformat(),
        }


def levels_from_payload(levels: Iterable[Mapping[str, Any]]) -> list[tuple[Decimal, Decimal]]:
    parsed: list[tuple[Decimal, Decimal]] = []
    for level in levels or []:
        price = parse_decimal(level.get("price"))
        size = parse_decimal(level.get("size"))
        if price is None or size is None:
            continue
        parsed.append((price, size))
    return parsed


def _float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _levels(
    levels: Iterable[tuple[Decimal, Decimal]],
    *,
    reverse: bool,
    limit: int,
) -> list[dict[str, float]]:
    rows = sorted(levels, key=lambda row: row[0], reverse=reverse)[: max(0, limit)]
    return [
        {
            "price": float(price),
            "size": float(size),
            "notional": float(price * size),
        }
        for price, size in rows
    ]
