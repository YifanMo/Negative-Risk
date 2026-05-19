from decimal import Decimal

from negrisk.models import NegRiskEvent, NegRiskMarket
from negrisk.orderbook import LocalBook
from negrisk.signal import check_event


def make_book(bid, ask, bid_size="100", ask_size="100", tick=None):
    book = LocalBook(tick_size=Decimal(tick) if tick else None)
    book.apply_snapshot(
        bids=[(Decimal(bid), Decimal(bid_size))],
        asks=[(Decimal(ask), Decimal(ask_size))],
    )
    return book


def make_event():
    return NegRiskEvent(
        event_id="e1",
        title="Election test",
        neg_risk_market_id="0x1",
        markets=(
            NegRiskMarket("c1", "A", "yes1", "no1", question_index=0, min_tick_size=Decimal("0.01")),
            NegRiskMarket("c2", "B", "yes2", "no2", question_index=1, min_tick_size=Decimal("0.01")),
            NegRiskMarket("c3", "C", "yes3", "no3", question_index=2, min_tick_size=Decimal("0.01")),
        ),
    )


def test_signal_selects_best_no_leg():
    books = {
        "yes1": make_book("0.50", "0.52"),
        "no1": make_book("0.46", "0.48"),
        "yes2": make_book("0.35", "0.36"),
        "no2": make_book("0.62", "0.65"),
        "yes3": make_book("0.30", "0.31"),
        "no3": make_book("0.68", "0.70"),
    }

    opp = check_event(
        make_event(),
        books,
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("1"),
        max_depth_pct=Decimal("0.5"),
    )

    assert opp is not None
    assert opp.best_no_idx == 0
    assert opp.gross_profit == Decimal("0.17")
    assert opp.sum_yes_ask == Decimal("1.19")


def test_signal_filters_below_sum_yes_threshold():
    books = {
        "yes1": make_book("0.30", "0.31"),
        "no1": make_book("0.60", "0.70"),
        "yes2": make_book("0.30", "0.31"),
        "no2": make_book("0.60", "0.70"),
        "yes3": make_book("0.30", "0.31"),
        "no3": make_book("0.60", "0.70"),
    }

    opp = check_event(
        make_event(),
        books,
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("1"),
        max_depth_pct=Decimal("0.5"),
    )

    assert opp is None


def test_signal_fail_closed_on_missing_book():
    books = {
        "yes1": make_book("0.50", "0.52"),
        "no1": make_book("0.46", "0.48"),
    }

    assert (
        check_event(
            make_event(),
            books,
            min_gross_profit=Decimal("0.01"),
            min_total_usd=Decimal("1"),
            max_depth_pct=Decimal("0.5"),
        )
        is None
    )
