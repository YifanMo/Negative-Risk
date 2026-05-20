from datetime import datetime, timedelta, timezone
from decimal import Decimal

from negrisk.book_history import BookHistoryStore
from negrisk.models import NegRiskEvent, NegRiskMarket
from negrisk.orderbook import LocalBook


def make_book(bid, ask, tick=None):
    book = LocalBook(tick_size=Decimal(tick) if tick else None)
    book.apply_snapshot(
        bids=[(Decimal(bid), Decimal("10"))],
        asks=[(Decimal(ask), Decimal("12"))],
    )
    return book


def test_book_history_writes_reads_and_prunes(tmp_path):
    store = BookHistoryStore(tmp_path / "books.sqlite", retention_hours=1)
    event = NegRiskEvent(
        event_id="e1",
        title="Snapshot event",
        neg_risk_market_id="0x1",
        markets=(
            NegRiskMarket("c1", "A", "yes1", "no1", question_index=0, min_tick_size=Decimal("0.01")),
            NegRiskMarket("c2", "B", "yes2", "no2", question_index=1, min_tick_size=Decimal("0.01")),
        ),
    )
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    books = {
        "yes1": make_book("0.60", "0.61", tick="0.01"),
        "no1": make_book("0.38", "0.40"),
        "yes2": make_book("0.48", "0.50", tick="0.01"),
        "no2": make_book("0.49", "0.52"),
    }
    for book in books.values():
        book.updated_at = now

    assert store.record_snapshots([event], books, now=now, max_events=10, max_book_age_secs=30) == 1
    records = store.load_snapshots(
        start_ts=int((now - timedelta(minutes=1)).timestamp()),
        end_ts=int((now + timedelta(minutes=1)).timestamp()),
        bucket_seconds=300,
    )
    assert len(records) == 1
    assert records[0]["data"]["markets"][0]["yes_bid"] == "0.60"

    later = now + timedelta(hours=2)
    for book in books.values():
        book.updated_at = later
    store.record_snapshots([event], books, now=later, max_events=10, max_book_age_secs=30)
    coverage = store.coverage(
        start_ts=int((now - timedelta(minutes=1)).timestamp()),
        end_ts=int((later + timedelta(minutes=1)).timestamp()),
    )
    assert coverage["snapshot_count"] == 1
