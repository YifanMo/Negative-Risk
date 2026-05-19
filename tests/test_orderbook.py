from decimal import Decimal

from negrisk.orderbook import LocalBook


def test_orderbook_snapshot_and_best_prices():
    book = LocalBook()
    book.apply_snapshot(
        bids=[(Decimal("0.40"), Decimal("10")), (Decimal("0.41"), Decimal("5"))],
        asks=[(Decimal("0.43"), Decimal("7")), (Decimal("0.42"), Decimal("8"))],
    )

    assert book.best_bid() == Decimal("0.41")
    assert book.best_ask() == Decimal("0.42")
    assert book.total_bid_depth() == Decimal("15")
    assert book.total_ask_depth() == Decimal("15")


def test_orderbook_delta_updates_and_deletes_levels():
    book = LocalBook()
    book.apply_snapshot(bids=[(Decimal("0.40"), Decimal("10"))], asks=[(Decimal("0.42"), Decimal("8"))])

    book.apply_delta("buy", Decimal("0.41"), Decimal("4"))
    book.apply_delta("sell", Decimal("0.42"), Decimal("0"))

    assert book.best_bid() == Decimal("0.41")
    assert book.best_ask() is None


def test_orderbook_depth_helpers():
    book = LocalBook()
    book.apply_snapshot(
        bids=[(Decimal("0.40"), Decimal("10")), (Decimal("0.39"), Decimal("20"))],
        asks=[(Decimal("0.42"), Decimal("8")), (Decimal("0.45"), Decimal("12"))],
    )

    assert book.bid_depth_at_or_above(Decimal("0.395")) == Decimal("10")
    assert book.ask_depth_up_to(Decimal("0.43")) == Decimal("8")
