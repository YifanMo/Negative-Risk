from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import ROOT
from .models import NegRiskEvent
from .orderbook import LocalBook


class BookHistoryStore:
    def __init__(self, db_path: str | Path, *, retention_hours: int) -> None:
        path = Path(db_path)
        self.path = path if path.is_absolute() else ROOT / path
        self.retention_hours = retention_hours

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS book_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_s INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_title TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    UNIQUE(timestamp_s, event_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_book_snapshots_time ON book_snapshots(timestamp_s)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_book_snapshots_event_time ON book_snapshots(event_id, timestamp_s)"
            )

    def record_snapshots(
        self,
        events: Iterable[NegRiskEvent],
        books: Mapping[str, LocalBook],
        *,
        now: datetime,
        max_events: int,
        max_book_age_secs: int,
    ) -> int:
        self.initialize()
        rows: list[tuple[int, str, str, str, str]] = []
        timestamp_s = int(now.timestamp())
        timestamp = now.isoformat()
        for event in list(events)[:max_events]:
            snapshot = serialize_event_snapshot(
                event,
                books,
                now=now,
                max_book_age_secs=max_book_age_secs,
            )
            if snapshot is None:
                continue
            rows.append(
                (
                    timestamp_s,
                    timestamp,
                    event.event_id,
                    event.title,
                    json.dumps(snapshot, separators=(",", ":")),
                )
            )

        cutoff = timestamp_s - self.retention_hours * 3600
        with self._connect() as conn:
            if rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO book_snapshots
                        (timestamp_s, timestamp, event_id, event_title, data_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            conn.execute("DELETE FROM book_snapshots WHERE timestamp_s < ?", (cutoff,))
        return len(rows)

    def load_snapshots(
        self,
        *,
        start_ts: int,
        end_ts: int,
        bucket_seconds: int,
    ) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp_s, timestamp, event_id, event_title, data_json
                FROM book_snapshots
                WHERE timestamp_s BETWEEN ? AND ?
                ORDER BY timestamp_s ASC, event_id ASC
                """,
                (start_ts, end_ts),
            ).fetchall()

        grouped: dict[tuple[int, str], dict[str, Any]] = {}
        for row in rows:
            bucket_ts = bucket_from_start(int(row["timestamp_s"]), start_ts, bucket_seconds)
            key = (bucket_ts, str(row["event_id"]))
            grouped[key] = {
                "bucket_ts": bucket_ts,
                "snapshot_ts": int(row["timestamp_s"]),
                "timestamp": str(row["timestamp"]),
                "event_id": str(row["event_id"]),
                "event_title": str(row["event_title"]),
                "data": json.loads(str(row["data_json"])),
            }

        return [grouped[key] for key in sorted(grouped)]

    def coverage(self, *, start_ts: int, end_ts: int) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    MIN(timestamp_s) AS min_ts,
                    MAX(timestamp_s) AS max_ts,
                    COUNT(*) AS snapshot_count,
                    COUNT(DISTINCT event_id) AS event_count
                FROM book_snapshots
                WHERE timestamp_s BETWEEN ? AND ?
                """,
                (start_ts, end_ts),
            ).fetchone()
        min_ts = row["min_ts"]
        max_ts = row["max_ts"]
        return {
            "coverage_start": _iso(min_ts),
            "coverage_end": _iso(max_ts),
            "coverage_start_s": min_ts,
            "coverage_end_s": max_ts,
            "snapshot_count": int(row["snapshot_count"] or 0),
            "event_count": int(row["event_count"] or 0),
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn


def serialize_event_snapshot(
    event: NegRiskEvent,
    books: Mapping[str, LocalBook],
    *,
    now: datetime,
    max_book_age_secs: int,
) -> dict[str, Any] | None:
    markets: list[dict[str, Any]] = []
    max_age = max(1, max_book_age_secs)
    for market in event.markets:
        yes_book = books.get(market.yes_token_id)
        no_book = books.get(market.no_token_id)
        if yes_book is None or no_book is None or yes_book.is_empty or no_book.is_empty:
            return None

        yes_bid = yes_book.best_bid()
        yes_ask = yes_book.best_ask()
        no_bid = no_book.best_bid()
        no_ask = no_book.best_ask()
        tick = market.min_tick_size or yes_book.tick_size
        if yes_bid is None or yes_ask is None or no_ask is None or tick is None:
            return None

        oldest = min(yes_book.updated_at, no_book.updated_at)
        if oldest <= datetime.min.replace(tzinfo=timezone.utc):
            return None
        if (now - oldest).total_seconds() > max_age:
            return None

        sell_limit = max(yes_bid - tick, Decimal("0.01"))
        markets.append(
            {
                "condition_id": market.condition_id,
                "question": market.question,
                "yes_token_id": market.yes_token_id,
                "no_token_id": market.no_token_id,
                "question_id": market.question_id,
                "question_index": market.question_index,
                "min_tick_size": _decimal_str(market.min_tick_size),
                "tick_size": _decimal_str(tick),
                "yes_bid": _decimal_str(yes_bid),
                "yes_ask": _decimal_str(yes_ask),
                "no_bid": _decimal_str(no_bid),
                "no_ask": _decimal_str(no_ask),
                "yes_bid_depth": _decimal_str(yes_book.bid_depth_at_or_above(sell_limit)),
                "no_ask_depth": _decimal_str(no_book.ask_depth_up_to(Decimal("1"))),
                "yes_updated_at": yes_book.updated_at.isoformat(),
                "no_updated_at": no_book.updated_at.isoformat(),
                "oldest_book_update": oldest.isoformat(),
            }
        )

    return {
        "event_id": event.event_id,
        "event_title": event.title,
        "neg_risk_market_id": event.neg_risk_market_id,
        "liquidity": _decimal_str(event.liquidity),
        "markets": markets,
    }


def floor_to_bucket(timestamp_s: int, bucket_seconds: int) -> int:
    bucket_seconds = max(1, bucket_seconds)
    return timestamp_s - (timestamp_s % bucket_seconds)


def bucket_from_start(timestamp_s: int, start_ts: int, bucket_seconds: int) -> int:
    bucket_seconds = max(1, bucket_seconds)
    if timestamp_s <= start_ts:
        return start_ts
    return start_ts + ((timestamp_s - start_ts) // bucket_seconds) * bucket_seconds


def _decimal_str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _iso(timestamp_s: int | None) -> str | None:
    if timestamp_s is None:
        return None
    return datetime.fromtimestamp(int(timestamp_s), timezone.utc).isoformat()
