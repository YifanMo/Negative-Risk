from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Mapping

from .models import ConversionOpportunity, NegRiskEvent
from .orderbook import LocalBook


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
