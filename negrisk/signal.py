from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from .models import ConversionOpportunity, NegRiskEvent
from .orderbook import LocalBook


def simulate_market_profits_1_share(
    event: NegRiskEvent,
    books: Mapping[str, LocalBook],
    *,
    max_depth_pct: Decimal,
) -> list[dict[str, Any]]:
    yes_bids: list[Decimal | None] = []
    no_asks: list[Decimal | None] = []
    no_ask_depth: list[Decimal | None] = []
    yes_bid_depth: list[Decimal | None] = []

    for market in event.markets:
        yes_book = books.get(market.yes_token_id)
        no_book = books.get(market.no_token_id)
        yes_bid = yes_book.best_bid() if yes_book is not None else None
        no_ask = no_book.best_ask() if no_book is not None else None
        tick = market.min_tick_size or (yes_book.tick_size if yes_book is not None else None)

        yes_bids.append(yes_bid)
        no_asks.append(no_ask)
        no_ask_depth.append(
            no_book.ask_depth_up_to(Decimal("1")) * max_depth_pct if no_book is not None and no_ask is not None else None
        )
        if yes_book is not None and yes_bid is not None and tick is not None:
            sell_limit = max(yes_bid - tick, Decimal("0.01"))
            yes_bid_depth.append(yes_book.bid_depth_at_or_above(sell_limit) * max_depth_pct)
        else:
            yes_bid_depth.append(None)

    sum_yes_bid = sum((bid for bid in yes_bids if bid is not None), Decimal("0")) if all(
        bid is not None for bid in yes_bids
    ) else None

    rows: list[dict[str, Any]] = []
    for idx, market in enumerate(event.markets):
        reason = None
        sum_other_yes_bid = None
        profit_1_share = None
        return_pct = None
        max_qty = None
        executable_1_share = False

        if sum_yes_bid is None:
            reason = "missing YES bid"
        elif no_asks[idx] is None:
            reason = "missing NO ask"
        elif yes_bids[idx] is None:
            reason = "missing market YES bid"
        else:
            sum_other_yes_bid = sum_yes_bid - yes_bids[idx]
            profit_1_share = sum_other_yes_bid - no_asks[idx]
            if no_asks[idx] > 0:
                return_pct = (profit_1_share / no_asks[idx]) * Decimal("100")

            sell_caps = [depth for depth_idx, depth in enumerate(yes_bid_depth) if depth_idx != idx and depth is not None]
            if len(event.markets) > 1 and no_ask_depth[idx] is not None and len(sell_caps) == len(event.markets) - 1:
                max_qty = min(no_ask_depth[idx], min(sell_caps), Decimal("10000"))
                executable_1_share = max_qty >= Decimal("1")
            else:
                reason = "missing depth or tick"

        rows.append(
            {
                "index": idx,
                "question": market.question,
                "buy_no_ask": _num(no_asks[idx]),
                "sum_other_yes_bid": _num(sum_other_yes_bid),
                "profit_1_share": _num(profit_1_share),
                "return_pct": _num(return_pct),
                "max_qty": _num(max_qty),
                "executable_1_share": executable_1_share,
                "status": "ok" if reason is None else "incomplete",
                "reason": reason,
            }
        )

    return rows


def check_event(
    event: NegRiskEvent,
    books: Mapping[str, LocalBook],
    *,
    min_gross_profit: Decimal,
    min_total_usd: Decimal,
    max_depth_pct: Decimal,
) -> ConversionOpportunity | None:
    n = len(event.markets)
    if n < 2:
        return None

    yes_bids: list[Decimal] = []
    yes_asks: list[Decimal] = []
    no_bids: list[Decimal] = []
    no_asks: list[Decimal] = []
    no_ask_depth: list[Decimal] = []
    yes_bid_depth: list[Decimal] = []
    oldest = datetime.max.replace(tzinfo=timezone.utc)

    for market in event.markets:
        yes_book = books.get(market.yes_token_id)
        no_book = books.get(market.no_token_id)
        if yes_book is None or no_book is None or yes_book.is_empty or no_book.is_empty:
            return None

        yes_bid = yes_book.best_bid()
        yes_ask = yes_book.best_ask()
        no_bid = no_book.best_bid()
        no_ask = no_book.best_ask()
        if yes_bid is None or yes_ask is None or no_ask is None:
            return None

        tick = market.min_tick_size or yes_book.tick_size
        if tick is None:
            return None

        sell_limit = max(yes_bid - tick, Decimal("0.01"))
        yes_bids.append(yes_bid)
        yes_asks.append(yes_ask)
        no_bids.append(no_bid or Decimal("0"))
        no_asks.append(no_ask)
        no_ask_depth.append(no_book.ask_depth_up_to(Decimal("1")) * max_depth_pct)
        yes_bid_depth.append(yes_book.bid_depth_at_or_above(sell_limit) * max_depth_pct)
        oldest = min(oldest, yes_book.updated_at, no_book.updated_at)

    sum_yes_ask = sum(yes_asks, Decimal("0"))
    if sum_yes_ask <= Decimal("1") + min_gross_profit:
        return None

    sum_yes_bid = sum(yes_bids, Decimal("0"))
    best_idx = 0
    best_gross = Decimal("0")
    for idx in range(n):
        gross = (sum_yes_bid - yes_bids[idx]) - no_asks[idx]
        if gross > best_gross:
            best_gross = gross
            best_idx = idx

    if best_gross < min_gross_profit:
        return None

    no_buy_cap = no_ask_depth[best_idx]
    yes_sell_caps = [depth for idx, depth in enumerate(yes_bid_depth) if idx != best_idx]
    if not yes_sell_caps:
        return None
    max_qty = min(no_buy_cap, min(yes_sell_caps), Decimal("10000"))
    if max_qty < Decimal("1"):
        return None

    total_usd = best_gross * max_qty
    if total_usd < min_total_usd:
        return None

    return ConversionOpportunity(
        event_id=event.event_id,
        event_title=event.title,
        n_markets=n,
        sum_yes_ask=sum_yes_ask,
        gross_profit=best_gross,
        best_no_idx=best_idx,
        best_market_question=event.markets[best_idx].question,
        best_no_ask=no_asks[best_idx],
        sum_other_yes_bid=sum_yes_bid - yes_bids[best_idx],
        max_qty=max_qty,
        total_usd=total_usd,
        yes_bids=tuple(yes_bids),
        yes_asks=tuple(yes_asks),
        no_bids=tuple(no_bids),
        no_asks=tuple(no_asks),
        oldest_book_update=oldest,
    )


def _num(value: Decimal | None) -> float | None:
    return None if value is None else float(value)
