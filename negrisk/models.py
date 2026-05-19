from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class NegRiskMarket:
    condition_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    question_id: str = ""
    question_index: int | None = None
    min_tick_size: Decimal | None = None


@dataclass(frozen=True)
class NegRiskEvent:
    event_id: str
    title: str
    neg_risk_market_id: str
    markets: tuple[NegRiskMarket, ...]
    liquidity: Decimal | None = None


@dataclass(frozen=True)
class ConversionOpportunity:
    event_id: str
    event_title: str
    n_markets: int
    sum_yes_ask: Decimal
    gross_profit: Decimal
    best_no_idx: int
    best_market_question: str
    best_no_ask: Decimal
    sum_other_yes_bid: Decimal
    max_qty: Decimal
    total_usd: Decimal
    yes_bids: tuple[Decimal, ...]
    yes_asks: tuple[Decimal, ...]
    no_bids: tuple[Decimal, ...]
    no_asks: tuple[Decimal, ...]
    oldest_book_update: datetime
    detected_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        age_seconds = max(0.0, (utc_now() - self.oldest_book_update).total_seconds())
        return {
            "event_id": self.event_id,
            "event_title": self.event_title,
            "n_markets": self.n_markets,
            "sum_yes_ask": _num(self.sum_yes_ask),
            "gross_profit": _num(self.gross_profit),
            "best_no_idx": self.best_no_idx,
            "best_market_question": self.best_market_question,
            "best_no_ask": _num(self.best_no_ask),
            "sum_other_yes_bid": _num(self.sum_other_yes_bid),
            "max_qty": _num(self.max_qty),
            "total_usd": _num(self.total_usd),
            "yes_bids": [_num(v) for v in self.yes_bids],
            "yes_asks": [_num(v) for v in self.yes_asks],
            "no_bids": [_num(v) for v in self.no_bids],
            "no_asks": [_num(v) for v in self.no_asks],
            "book_age_seconds": age_seconds,
            "detected_at": self.detected_at.isoformat(),
            "simulation": {
                "step_1": f"Buy NO for outcome #{self.best_no_idx + 1} at about {self.best_no_ask}",
                "step_2": "Simulate NegRisk convertPositions: NO_i -> YES for every other outcome",
                "step_3": f"Sell converted YES legs at aggregate bid {self.sum_other_yes_bid}",
                "real_trading": False,
            },
        }


def _num(value: Decimal) -> float:
    return float(value)
