from decimal import Decimal
from datetime import datetime, timezone
import json

from negrisk.historical_replay import (
    PMXT_ARCHIVE_SOURCE,
    ReplayParams,
    find_local_pmxt_files,
    parse_pmxt_levels,
    parse_batch_prices_history,
    pmxt_file_hour_starts,
    pmxt_file_range,
    replay_from_pmxt_archive,
    replay_from_book_snapshots,
    replay_from_histories,
    replay_from_pmxt_rows,
)
from negrisk.config import AppConfig, HistoryConfig
from negrisk.models import NegRiskEvent, NegRiskMarket
from negrisk.orderbook import LocalBook
from negrisk.signal import check_event


def make_event():
    return NegRiskEvent(
        event_id="e1",
        title="Test NegRisk",
        neg_risk_market_id="0x1",
        markets=(
            NegRiskMarket("c1", "A", "yes1", "no1"),
            NegRiskMarket("c2", "B", "yes2", "no2"),
            NegRiskMarket("c3", "C", "yes3", "no3"),
        ),
    )


def make_two_market_event():
    return NegRiskEvent(
        event_id="e2",
        title="PMXT NegRisk",
        neg_risk_market_id="0x2",
        markets=(
            NegRiskMarket("c1", "A", "yes1", "no1", question_index=0, min_tick_size=Decimal("0.01")),
            NegRiskMarket("c2", "B", "yes2", "no2", question_index=1, min_tick_size=Decimal("0.01")),
        ),
    )


def make_book(bid, ask, bid_size="100", ask_size="100", tick=None):
    book = LocalBook(tick_size=Decimal(tick) if tick else None)
    book.apply_snapshot(
        bids=[(Decimal(bid), Decimal(bid_size))],
        asks=[(Decimal(ask), Decimal(ask_size))],
    )
    return book


def make_book_snapshot_record(timestamp_s=300):
    return {
        "bucket_ts": timestamp_s,
        "snapshot_ts": timestamp_s + 4,
        "timestamp": "1970-01-01T00:05:04+00:00",
        "event_id": "e1",
        "event_title": "Test NegRisk",
        "data": {
            "event_id": "e1",
            "event_title": "Test NegRisk",
            "neg_risk_market_id": "0x1",
            "markets": [
                {
                    "condition_id": "c1",
                    "question": "A",
                    "yes_token_id": "yes1",
                    "no_token_id": "no1",
                    "question_index": 0,
                    "min_tick_size": "0.01",
                    "tick_size": "0.01",
                    "yes_bid": "0.50",
                    "yes_ask": "0.52",
                    "no_bid": "0.46",
                    "no_ask": "0.48",
                    "yes_bid_depth": "100",
                    "no_ask_depth": "10",
                },
                {
                    "condition_id": "c2",
                    "question": "B",
                    "yes_token_id": "yes2",
                    "no_token_id": "no2",
                    "question_index": 1,
                    "min_tick_size": "0.01",
                    "tick_size": "0.01",
                    "yes_bid": "0.35",
                    "yes_ask": "0.36",
                    "no_bid": "0.62",
                    "no_ask": "0.65",
                    "yes_bid_depth": "30",
                    "no_ask_depth": "100",
                },
                {
                    "condition_id": "c3",
                    "question": "C",
                    "yes_token_id": "yes3",
                    "no_token_id": "no3",
                    "question_index": 2,
                    "min_tick_size": "0.01",
                    "tick_size": "0.01",
                    "yes_bid": "0.30",
                    "yes_ask": "0.31",
                    "no_bid": "0.68",
                    "no_ask": "0.70",
                    "yes_bid_depth": "40",
                    "no_ask_depth": "100",
                },
            ],
        },
    }


def test_parse_batch_prices_history_map_response():
    parsed = parse_batch_prices_history(
        {
            "history": {
                "yes1": [{"t": 100, "p": 0.4}, {"t": 160, "p": "0.41"}],
                "no1": [{"timestamp": 100, "price": "0.58"}],
            }
        }
    )

    assert parsed["yes1"] == [(100, Decimal("0.4")), (160, Decimal("0.41"))]
    assert parsed["no1"] == [(100, Decimal("0.58"))]


def test_parse_pmxt_levels_from_json_pairs():
    levels = parse_pmxt_levels(json.dumps([["0.41", "12.5"], ["0.40", "7"]]))

    assert levels == [(Decimal("0.41"), Decimal("12.5")), (Decimal("0.40"), Decimal("7"))]


def test_replay_detects_opportunity_and_equity_curve_after_slippage():
    histories = {
        "yes1": [(0, Decimal("0.50")), (300, Decimal("0.51"))],
        "no1": [(0, Decimal("0.48")), (300, Decimal("0.47"))],
        "yes2": [(0, Decimal("0.35")), (300, Decimal("0.36"))],
        "no2": [(0, Decimal("0.64")), (300, Decimal("0.63"))],
        "yes3": [(0, Decimal("0.30")), (300, Decimal("0.31"))],
        "no3": [(0, Decimal("0.69")), (300, Decimal("0.68"))],
    }

    replay = replay_from_histories(
        [make_event()],
        histories,
        start_ts=0,
        end_ts=600,
        fidelity_minutes=5,
        investment_usd=Decimal("1000"),
        slippage_pct=Decimal("0.5"),
        min_gross_profit=Decimal("0.01"),
    )

    assert replay["summary"]["opportunity_count"] == 3
    assert replay["summary"]["simulated_trade_count"] == 3
    assert replay["summary"]["ending_equity"] > 1000
    assert replay["opportunities"][0]["best_no_idx"] == 0
    assert replay["opportunities"][0]["buy"]["side"] == "BUY_NO"
    assert replay["opportunities"][0]["convert"]["action"] == "NO_i -> YES_j for j != i"
    assert replay["opportunities"][0]["sell"]["side"] == "SELL_CONVERTED_YES"
    assert replay["opportunities"][0]["settlement"]["pnl_usd"] > 0
    assert len(replay["trades"]) == 3
    assert [trade["timestamp_s"] for trade in replay["trades"]] == [0, 300, 600]
    assert all(trade["settlement"]["equity_after"] is not None for trade in replay["trades"])
    assert replay["equity_curve"][1]["trade"]["settlement"]["equity_after"] == replay["trades"][0]["settlement"]["equity_after"]
    assert replay["top_events"][0]["event_id"] == "e1"
    assert replay["hourly"][0]["count"] == 3


def test_replay_filters_when_slippage_erases_profit():
    histories = {
        "yes1": [(0, Decimal("0.50"))],
        "no1": [(0, Decimal("0.48"))],
        "yes2": [(0, Decimal("0.35"))],
        "no2": [(0, Decimal("0.64"))],
        "yes3": [(0, Decimal("0.30"))],
        "no3": [(0, Decimal("0.69"))],
    }

    replay = replay_from_histories(
        [make_event()],
        histories,
        start_ts=0,
        end_ts=0,
        fidelity_minutes=5,
        investment_usd=Decimal("1000"),
        slippage_pct=Decimal("20"),
        min_gross_profit=Decimal("0.01"),
    )

    assert replay["summary"]["opportunity_count"] == 0
    assert replay["summary"]["ending_equity"] == 1000
    assert replay["trades"] == []


def test_book_snapshot_replay_matches_realtime_signal_and_caps_trade_size():
    books = {
        "yes1": make_book("0.50", "0.52", bid_size="100", tick="0.01"),
        "no1": make_book("0.46", "0.48", ask_size="10"),
        "yes2": make_book("0.35", "0.36", bid_size="30", tick="0.01"),
        "no2": make_book("0.62", "0.65", ask_size="100"),
        "yes3": make_book("0.30", "0.31", bid_size="40", tick="0.01"),
        "no3": make_book("0.68", "0.70", ask_size="100"),
    }
    live = check_event(
        make_event(),
        books,
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("0.5"),
        max_depth_pct=Decimal("0.5"),
    )

    replay = replay_from_book_snapshots(
        [make_book_snapshot_record()],
        start_ts=0,
        end_ts=600,
        fidelity_minutes=5,
        investment_usd=Decimal("1000"),
        slippage_pct=Decimal("0"),
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("0.5"),
        max_depth_pct=Decimal("0.5"),
        coverage={
            "coverage_start": "1970-01-01T00:05:04+00:00",
            "coverage_end": "1970-01-01T00:05:04+00:00",
            "coverage_start_s": 304,
            "coverage_end_s": 304,
            "snapshot_count": 1,
            "event_count": 1,
        },
        bucket_seconds=300,
    )

    assert live is not None
    trade = replay["trades"][0]
    assert trade["best_no_idx"] == live.best_no_idx
    assert Decimal(str(trade["raw_gross_profit"])) == live.gross_profit
    assert Decimal(str(trade["max_qty"])) == live.max_qty
    assert trade["executed_qty"] == 5.0
    assert trade["used_capital_usd"] == 2.4
    assert trade["unused_capital_usd"] == 997.6
    assert trade["settlement"]["net_pnl_usd"] == 0.85
    assert replay["coverage"]["source_mode"] == "book-snapshot"


def test_pmxt_rows_reconstruct_books_and_replay_opportunity():
    ts = datetime.fromtimestamp(0, timezone.utc)
    rows = [
        {
            "timestamp_received": ts,
            "event_type": "book",
            "asset_id": "yes1",
            "bids": json.dumps([["0.50", "100"]]),
            "asks": json.dumps([["0.60", "100"]]),
        },
        {
            "timestamp_received": ts,
            "event_type": "book",
            "asset_id": "no1",
            "bids": json.dumps([["0.30", "100"]]),
            "asks": json.dumps([["0.35", "100"]]),
        },
        {
            "timestamp_received": ts,
            "event_type": "book",
            "asset_id": "yes2",
            "bids": json.dumps([["0.40", "100"]]),
            "asks": json.dumps([["0.50", "100"]]),
        },
        {
            "timestamp_received": ts,
            "event_type": "book",
            "asset_id": "no2",
            "bids": json.dumps([["0.50", "100"]]),
            "asks": json.dumps([["0.60", "100"]]),
        },
    ]

    replay = replay_from_pmxt_rows(
        [make_two_market_event()],
        rows,
        start_ts=0,
        end_ts=300,
        seed_start_ts=0,
        fidelity_minutes=5,
        investment_usd=Decimal("10"),
        slippage_pct=Decimal("0"),
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("0.5"),
        max_depth_pct=Decimal("0.5"),
        file_count=1,
        requested_file_count=1,
    )

    assert replay["params"]["source"] == "pmxt-archive"
    assert replay["coverage"]["archive_row_count"] == 4
    assert replay["summary"]["opportunity_count"] == 2
    assert replay["trades"][0]["best_no_idx"] == 0
    assert replay["trades"][0]["raw_no_ask"] == 0.35
    assert replay["trades"][0]["raw_sum_other_yes_bid"] == 0.4
    assert replay["trades"][0]["settlement"]["net_pnl_usd"] > 1.4


def test_pmxt_replay_params_ignore_hours_in_cache_key():
    one_day = ReplayParams(24, 5, Decimal("1000"), Decimal("0.5"), PMXT_ARCHIVE_SOURCE)
    seven_days = ReplayParams(168, 5, Decimal("1000"), Decimal("0.5"), PMXT_ARCHIVE_SOURCE)

    assert one_day.cache_key() == seven_days.cache_key()


def test_local_pmxt_files_define_replay_range(tmp_path):
    first = tmp_path / "polymarket_orderbook_2026-05-18T17.parquet"
    second = tmp_path / "polymarket_orderbook_2026-05-18T19.parquet"
    ignored = tmp_path / "other.parquet"
    first.write_bytes(b"x")
    second.write_bytes(b"x")
    ignored.write_bytes(b"x")

    files = find_local_pmxt_files(str(tmp_path))
    start_ts, end_ts = pmxt_file_range(files)

    assert [path.name for path in files] == [first.name, second.name]
    assert datetime.fromtimestamp(start_ts, timezone.utc).isoformat() == "2026-05-18T17:00:00+00:00"
    assert datetime.fromtimestamp(end_ts, timezone.utc).isoformat() == "2026-05-18T20:00:00+00:00"
    assert len(pmxt_file_hour_starts(files)) == 2


def test_pmxt_archive_returns_empty_payload_without_local_files(tmp_path):
    config = AppConfig(history=HistoryConfig(pmxt_cache_dir=str(tmp_path)))

    replay = replay_from_pmxt_archive(
        config,
        [make_two_market_event()],
        fidelity_minutes=5,
        investment_usd=Decimal("10"),
        slippage_pct=Decimal("0"),
        min_gross_profit=Decimal("0.01"),
        min_total_usd=Decimal("0.5"),
        max_depth_pct=Decimal("0.5"),
    )

    assert replay["params"]["source"] == PMXT_ARCHIVE_SOURCE
    assert replay["params"]["hours"] == 0.0
    assert replay["coverage"]["local_file_count"] == 0
    assert replay["coverage"]["coverage_basis"] == "local-parquet-file-range"
    assert replay["trades"] == []
