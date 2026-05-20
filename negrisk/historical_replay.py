from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Iterable

import httpx

from .book_history import BookHistoryStore
from .config import AppConfig, ROOT
from .gamma import fetch_negrisk_events
from .models import NegRiskEvent, NegRiskMarket
from .orderbook import LocalBook, levels_from_payload
from .signal import check_event


PriceSeries = dict[str, list[tuple[int, Decimal]]]
BOOK_SNAPSHOT_SOURCE = "book-snapshot"
PRICE_PROXY_SOURCE = "price-proxy"
PMXT_ARCHIVE_SOURCE = "pmxt-archive"


@dataclass(frozen=True)
class ReplayParams:
    hours: int
    fidelity_minutes: int
    investment_usd: Decimal
    slippage_pct: Decimal
    source: str = BOOK_SNAPSHOT_SOURCE

    def cache_key(self) -> tuple[int, int, str, str, str]:
        cache_hours = 0 if self.source == PMXT_ARCHIVE_SOURCE else self.hours
        return (
            cache_hours,
            self.fidelity_minutes,
            str(self.investment_usd),
            str(self.slippage_pct),
            self.source,
        )


class HistoricalReplayService:
    def __init__(self, config: AppConfig, book_store: BookHistoryStore | None = None) -> None:
        self.config = config
        self.book_store = book_store or BookHistoryStore(
            config.history.book_db_path,
            retention_hours=config.history.retention_hours,
        )
        self._cache: dict[tuple[int, int, str, str, str], tuple[float, dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def replay(self, params: ReplayParams, *, force_refresh: bool = False) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        cache_key = params.cache_key()
        cache_ttl = (
            self.config.history.cache_ttl_secs
            if params.source in {PRICE_PROXY_SOURCE, PMXT_ARCHIVE_SOURCE}
            else max(1, self.config.history.snapshot_interval_secs)
        )
        cached = self._cache.get(cache_key)
        if cached and not force_refresh:
            cached_at, payload = cached
            if now.timestamp() - cached_at < cache_ttl:
                return payload

        async with self._lock:
            cached = self._cache.get(cache_key)
            if cached and not force_refresh:
                cached_at, payload = cached
                if now.timestamp() - cached_at < cache_ttl:
                    return payload

            if params.source == BOOK_SNAPSHOT_SOURCE:
                payload = self._replay_book_snapshots(params, now)
                self._cache[cache_key] = (now.timestamp(), payload)
                return payload

            timeout = httpx.Timeout(45.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                events = await fetch_negrisk_events(
                    client,
                    self.config.gamma,
                    min_liquidity=self.config.engine.min_event_liquidity,
                    max_markets_per_event=self.config.engine.max_markets_per_event,
                    max_events=min(self.config.history.max_events, self.config.engine.max_events),
                )
                if params.source == PMXT_ARCHIVE_SOURCE:
                    payload = await asyncio.to_thread(
                        replay_from_pmxt_archive,
                        self.config,
                        events,
                        fidelity_minutes=params.fidelity_minutes,
                        investment_usd=params.investment_usd,
                        slippage_pct=params.slippage_pct,
                        min_gross_profit=self.config.arb.min_gross_profit,
                        min_total_usd=self.config.arb.min_total_usd,
                        max_depth_pct=self.config.arb.max_depth_pct,
                    )
                    payload["generated_at"] = now.isoformat()
                    payload["source"] = {
                        "mode": PMXT_ARCHIVE_SOURCE,
                        "method": "local-pmxt-hourly-parquet-orderbook",
                        "cache_dir": str(resolve_pmxt_cache_dir(self.config.history.pmxt_cache_dir)),
                        "note": "Replays local PMXT Polymarket CLOB orderbook Parquet files; lookback hours is ignored.",
                    }
                    self._cache[cache_key] = (now.timestamp(), payload)
                    return payload

                start_ts = int(now.timestamp()) - params.hours * 3600
                end_ts = int(now.timestamp())
                histories = await fetch_batch_prices_history(
                    client,
                    self.config.clob.rest_url,
                    token_ids=collect_token_ids(events),
                    start_ts=start_ts,
                    end_ts=end_ts,
                    fidelity_minutes=params.fidelity_minutes,
                    batch_size=self.config.history.batch_size,
                )

            payload = replay_from_histories(
                events,
                histories,
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity_minutes=params.fidelity_minutes,
                investment_usd=params.investment_usd,
                slippage_pct=params.slippage_pct,
                min_gross_profit=self.config.arb.min_gross_profit,
            )
            payload["generated_at"] = now.isoformat()
            payload["source"] = {
                "gamma": self.config.gamma.base_url,
                "clob": self.config.clob.rest_url,
                "mode": PRICE_PROXY_SOURCE,
                "method": "batch-prices-history",
                "note": "Historical replay uses price points only; historical orderbook depth is unavailable.",
            }
            self._cache[cache_key] = (now.timestamp(), payload)
            return payload

    def _replay_book_snapshots(self, params: ReplayParams, now: datetime) -> dict[str, Any]:
        end_ts = int(now.timestamp())
        start_ts = end_ts - params.hours * 3600
        bucket_seconds = params.fidelity_minutes * 60
        records = self.book_store.load_snapshots(
            start_ts=start_ts,
            end_ts=end_ts,
            bucket_seconds=bucket_seconds,
        )
        coverage = self.book_store.coverage(start_ts=start_ts, end_ts=end_ts)
        payload = replay_from_book_snapshots(
            records,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity_minutes=params.fidelity_minutes,
            investment_usd=params.investment_usd,
            slippage_pct=params.slippage_pct,
            min_gross_profit=self.config.arb.min_gross_profit,
            min_total_usd=self.config.arb.min_total_usd,
            max_depth_pct=self.config.arb.max_depth_pct,
            coverage=coverage,
            bucket_seconds=bucket_seconds,
        )
        payload["generated_at"] = now.isoformat()
        payload["source"] = {
            "mode": BOOK_SNAPSHOT_SOURCE,
            "method": "local-orderbook-snapshots",
            "db_path": str(self.book_store.path),
            "note": "Strict replay uses locally recorded bid/ask/depth snapshots only.",
        }
        return payload


def normalized_replay_params(
    *,
    hours: int,
    fidelity_minutes: int,
    investment_usd: float,
    slippage_pct: float,
    source: str = BOOK_SNAPSHOT_SOURCE,
) -> ReplayParams:
    replay_source = (
        source if source in {BOOK_SNAPSHOT_SOURCE, PRICE_PROXY_SOURCE, PMXT_ARCHIVE_SOURCE} else BOOK_SNAPSHOT_SOURCE
    )
    return ReplayParams(
        hours=max(1, min(int(hours), 168)),
        fidelity_minutes=max(1, min(int(fidelity_minutes), 60)),
        investment_usd=max(Decimal("1"), _dec(investment_usd, Decimal("1000"))),
        slippage_pct=max(Decimal("0"), min(_dec(slippage_pct, Decimal("0.5")), Decimal("20"))),
        source=replay_source,
    )


async def fetch_batch_prices_history(
    client: httpx.AsyncClient,
    clob_base_url: str,
    *,
    token_ids: Iterable[str],
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    batch_size: int,
) -> PriceSeries:
    result: PriceSeries = {}
    ids = sorted({str(token_id) for token_id in token_ids if str(token_id)})
    batch_size = max(1, min(batch_size, 20))
    for idx in range(0, len(ids), batch_size):
        batch = ids[idx : idx + batch_size]
        response = await client.post(
            f"{clob_base_url.rstrip('/')}/batch-prices-history",
            json={
                "markets": batch,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "fidelity": fidelity_minutes,
            },
        )
        response.raise_for_status()
        result.update(parse_batch_prices_history(response.json()))
    return result


def parse_batch_prices_history(payload: Any) -> PriceSeries:
    result: PriceSeries = {}
    items: Iterable[Any]
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("history"), dict):
            for token_id, points in payload["history"].items():
                result[str(token_id)] = parse_price_points(points)
            return result
        items = payload.get("data") or payload.get("markets") or payload.get("results") or []
    else:
        return result

    for item in items:
        if not isinstance(item, dict):
            continue
        token_id = str(item.get("market") or item.get("asset_id") or item.get("assetId") or item.get("token_id") or "")
        if not token_id:
            continue
        points = item.get("history") or item.get("prices") or item.get("data") or []
        result[token_id] = parse_price_points(points)
    return result


def parse_price_points(points: Any) -> list[tuple[int, Decimal]]:
    parsed: list[tuple[int, Decimal]] = []
    if not isinstance(points, list):
        return parsed
    for point in points:
        if not isinstance(point, dict):
            continue
        ts = point.get("t", point.get("timestamp", point.get("ts")))
        price = point.get("p", point.get("price"))
        try:
            parsed.append((int(ts), _dec(price)))
        except (TypeError, ValueError, InvalidOperation):
            continue
    parsed.sort(key=lambda row: row[0])
    return parsed


def replay_from_pmxt_archive(
    config: AppConfig,
    events: list[NegRiskEvent],
    *,
    fidelity_minutes: int,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
    min_total_usd: Decimal,
    max_depth_pct: Decimal,
) -> dict[str, Any]:
    cache_dir = resolve_pmxt_cache_dir(config.history.pmxt_cache_dir)
    files = find_local_pmxt_files(config.history.pmxt_cache_dir)
    if not files:
        return empty_pmxt_replay_payload(
            events,
            fidelity_minutes=fidelity_minutes,
            investment_usd=investment_usd,
            slippage_pct=slippage_pct,
            min_gross_profit=min_gross_profit,
            min_total_usd=min_total_usd,
            max_depth_pct=max_depth_pct,
            cache_dir=cache_dir,
        )

    start_ts, end_ts = pmxt_file_range(files)
    seed_start_ts = start_ts
    token_ids = collect_token_ids(events)
    rows = iter_pmxt_rows(
        files,
        token_ids=token_ids,
        start_ts=seed_start_ts,
        end_ts=end_ts,
    )
    covered_hour_starts = pmxt_file_hour_starts(files)
    payload = replay_from_pmxt_rows(
        events,
        rows,
        start_ts=start_ts,
        end_ts=end_ts,
        seed_start_ts=seed_start_ts,
        fidelity_minutes=fidelity_minutes,
        investment_usd=investment_usd,
        slippage_pct=slippage_pct,
        min_gross_profit=min_gross_profit,
        min_total_usd=min_total_usd,
        max_depth_pct=max_depth_pct,
        file_count=len(files),
        requested_file_count=len(files),
        covered_hour_starts=covered_hour_starts,
    )
    payload["coverage"]["cache_dir"] = str(cache_dir)
    payload["coverage"]["local_file_count"] = len(files)
    payload["coverage"]["file_range_start"] = datetime.fromtimestamp(start_ts, timezone.utc).isoformat()
    payload["coverage"]["file_range_end"] = datetime.fromtimestamp(end_ts, timezone.utc).isoformat()
    return payload


def empty_pmxt_replay_payload(
    events: list[NegRiskEvent],
    *,
    fidelity_minutes: int,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
    min_total_usd: Decimal,
    max_depth_pct: Decimal,
    cache_dir: Path,
) -> dict[str, Any]:
    return {
        "params": {
            "hours": 0.0,
            "fidelity_minutes": fidelity_minutes,
            "investment_usd": _float(investment_usd),
            "slippage_pct": _float(slippage_pct),
            "min_gross_profit": _float(min_gross_profit),
            "min_total_usd": _float(min_total_usd),
            "max_depth_pct": _float(max_depth_pct),
            "source": PMXT_ARCHIVE_SOURCE,
            "fee_rate_bps": 0.0,
        },
        "coverage": {
            "event_count": len(events),
            "token_count": len(collect_token_ids(events)),
            "requested_hours": 0.0,
            "covered_hours": 0.0,
            "coverage_start": None,
            "coverage_end": None,
            "seed_start": None,
            "source_mode": PMXT_ARCHIVE_SOURCE,
            "coverage_basis": "local-parquet-file-range",
            "source_note": (
                "No local PMXT orderbook Parquet files were found. "
                f"Place polymarket_orderbook_YYYY-MM-DDTHH.parquet files under {cache_dir} and rerun."
            ),
            "requested_file_count": 0,
            "downloaded_file_count": 0,
            "local_file_count": 0,
            "read_file_count": 0,
            "archive_row_count": 0,
            "book_event_count": 0,
            "price_change_count": 0,
            "bucket_count": 0,
            "cache_dir": str(cache_dir),
            "is_complete_requested": False,
            "is_complete_local_range": False,
        },
        "summary": {
            "opportunity_count": 0,
            "simulated_trade_count": 0,
            "ending_equity": _float(investment_usd),
            "total_pnl_usd": 0.0,
            "max_gross_after_slippage": 0.0,
            "max_trade_pnl_usd": 0.0,
        },
        "top_events": [],
        "hourly": [],
        "equity_curve": [{"timestamp": None, "equity": _float(investment_usd), "pnl": 0.0, "event_title": "Initial capital"}],
        "trades": [],
        "opportunities": [],
    }


def replay_from_pmxt_rows(
    events: list[NegRiskEvent],
    rows: Iterable[dict[str, Any]],
    *,
    start_ts: int,
    end_ts: int,
    seed_start_ts: int,
    fidelity_minutes: int,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
    min_total_usd: Decimal,
    max_depth_pct: Decimal,
    file_count: int,
    requested_file_count: int,
    covered_hour_starts: set[int] | None = None,
) -> dict[str, Any]:
    bucket_seconds = fidelity_minutes * 60
    buckets = list(range(ceil_to_bucket(start_ts, bucket_seconds), end_ts + 1, bucket_seconds))
    next_bucket_idx = 0
    books: dict[str, LocalBook] = {}
    opportunities: list[dict[str, Any]] = []
    best_by_bucket: dict[int, dict[str, Any]] = {}
    row_count = 0
    book_event_count = 0
    price_change_count = 0

    def evaluate(bucket_ts: int) -> None:
        if covered_hour_starts is not None:
            bucket_hour = bucket_ts - (bucket_ts % 3600)
            if bucket_hour not in covered_hour_starts:
                return
        record = {
            "bucket_ts": bucket_ts,
            "snapshot_ts": bucket_ts,
            "timestamp": datetime.fromtimestamp(bucket_ts, timezone.utc).isoformat(),
        }
        for event in events:
            opportunity = check_event(
                event,
                books,
                min_gross_profit=min_gross_profit,
                min_total_usd=min_total_usd,
                max_depth_pct=max_depth_pct,
            )
            if opportunity is None:
                continue
            candidate = simulated_trade_from_opportunity(
                opportunity.to_dict(),
                record,
                investment_usd=investment_usd,
                slippage_pct=slippage_pct,
                min_gross_profit=min_gross_profit,
            )
            if candidate is None:
                continue
            opportunities.append(candidate)
            existing = best_by_bucket.get(bucket_ts)
            if existing is None or candidate["simulated_pnl_usd"] > existing["simulated_pnl_usd"]:
                best_by_bucket[bucket_ts] = candidate

    for row in rows:
        row_ts = int(row["timestamp_received"].timestamp())
        while next_bucket_idx < len(buckets) and buckets[next_bucket_idx] < row_ts:
            evaluate(buckets[next_bucket_idx])
            next_bucket_idx += 1

        event_type = str(row.get("event_type") or "")
        if event_type == "book":
            book_event_count += 1
        elif event_type == "price_change":
            price_change_count += 1
        apply_pmxt_row(row, books)
        row_count += 1

    while next_bucket_idx < len(buckets):
        evaluate(buckets[next_bucket_idx])
        next_bucket_idx += 1

    opportunities.sort(key=lambda item: (item["timestamp_s"], item["simulated_pnl_usd"]))
    curve, trades = build_equity_curve(best_by_bucket, investment_usd)
    top_events = aggregate_top_events(opportunities)
    hourly = aggregate_hourly(opportunities)
    requested_hours = (end_ts - start_ts) / 3600
    covered_hours = (
        float(len(covered_hour_starts)) if covered_hour_starts is not None else (end_ts - seed_start_ts) / 3600
    )
    file_gap_hours = max(0.0, requested_hours - covered_hours)
    return {
        "params": {
            "hours": requested_hours,
            "fidelity_minutes": fidelity_minutes,
            "investment_usd": _float(investment_usd),
            "slippage_pct": _float(slippage_pct),
            "min_gross_profit": _float(min_gross_profit),
            "min_total_usd": _float(min_total_usd),
            "max_depth_pct": _float(max_depth_pct),
            "source": PMXT_ARCHIVE_SOURCE,
            "fee_rate_bps": 0.0,
        },
        "coverage": {
            "event_count": len(events),
            "token_count": len(collect_token_ids(events)),
            "requested_hours": requested_hours,
            "covered_hours": covered_hours,
            "coverage_start": datetime.fromtimestamp(start_ts, timezone.utc).isoformat(),
            "coverage_end": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
            "seed_start": datetime.fromtimestamp(seed_start_ts, timezone.utc).isoformat(),
            "source_mode": PMXT_ARCHIVE_SOURCE,
            "coverage_basis": "local-parquet-file-range",
            "source_note": "PMXT local hourly Parquet replay of Polymarket CLOB book and price_change events. The replay window is inferred from local files.",
            "requested_file_count": requested_file_count,
            "downloaded_file_count": file_count,
            "local_file_count": file_count,
            "read_file_count": file_count,
            "archive_row_count": row_count,
            "book_event_count": book_event_count,
            "price_change_count": price_change_count,
            "bucket_count": len(buckets),
            "file_gap_hours": file_gap_hours,
            "is_complete_requested": True,
            "is_complete_local_range": file_gap_hours == 0,
        },
        "summary": {
            "opportunity_count": len(opportunities),
            "simulated_trade_count": max(0, len(curve) - 1),
            "ending_equity": curve[-1]["equity"] if curve else _float(investment_usd),
            "total_pnl_usd": (curve[-1]["equity"] - _float(investment_usd)) if curve else 0.0,
            "max_gross_after_slippage": max((row["gross_after_slippage"] for row in opportunities), default=0.0),
            "max_trade_pnl_usd": max((row["simulated_pnl_usd"] for row in opportunities), default=0.0),
        },
        "top_events": top_events,
        "hourly": hourly,
        "equity_curve": curve,
        "trades": trades,
        "opportunities": sorted(opportunities, key=lambda item: item["simulated_pnl_usd"], reverse=True)[:200],
    }


def apply_pmxt_row(row: dict[str, Any], books: dict[str, LocalBook]) -> None:
    asset_id = str(row.get("asset_id") or "")
    if not asset_id:
        return
    event_type = str(row.get("event_type") or "")
    timestamp = row.get("timestamp_received")
    if not isinstance(timestamp, datetime):
        return
    timestamp = timestamp.astimezone(timezone.utc)
    book = books.setdefault(asset_id, LocalBook())

    if event_type == "book":
        bids = parse_pmxt_levels(row.get("bids"))
        asks = parse_pmxt_levels(row.get("asks"))
        book.apply_snapshot(bids=bids, asks=asks)
        book.updated_at = timestamp
        return

    if event_type == "price_change":
        price = _optional_dec(row.get("price"))
        size = _optional_dec(row.get("size"))
        side = str(row.get("side") or "")
        if price is not None and size is not None and side:
            book.apply_delta(side, price, size)
            book.updated_at = timestamp
        return

    if event_type == "tick_size_change":
        tick = _optional_dec(row.get("new_tick_size"))
        if tick is not None:
            book.tick_size = tick
            book.updated_at = timestamp


def parse_pmxt_levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    if raw is None:
        return []
    if isinstance(raw, bytes):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
    else:
        parsed = raw
    if not isinstance(parsed, list):
        return []
    if parsed and isinstance(parsed[0], dict):
        return levels_from_payload(parsed)
    levels: list[tuple[Decimal, Decimal]] = []
    for item in parsed:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            price = Decimal(str(item[0]))
            size = Decimal(str(item[1]))
        except (InvalidOperation, ValueError):
            continue
        if size > 0:
            levels.append((price, size))
    return levels


def iter_pmxt_rows(
    files: list[Path],
    *,
    token_ids: set[str],
    start_ts: int,
    end_ts: int,
) -> Iterable[dict[str, Any]]:
    if not files or not token_ids:
        return iter(())
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required for PMXT archive replay") from exc

    conn = duckdb.connect(database=":memory:")
    conn.execute("CREATE TEMP TABLE token_filter(asset_id VARCHAR)")
    conn.executemany("INSERT INTO token_filter VALUES (?)", [(token_id,) for token_id in sorted(token_ids)])
    parquet_list = ", ".join(sql_quote(str(path)) for path in files)
    query = f"""
        SELECT
            p.timestamp_received,
            p.event_type,
            p.asset_id,
            p.bids,
            p.asks,
            p.price,
            p.size,
            p.side,
            p.new_tick_size
        FROM read_parquet([{parquet_list}]) p
        JOIN token_filter t ON p.asset_id = t.asset_id
        WHERE p.timestamp_received >= ? AND p.timestamp_received <= ?
        ORDER BY p.timestamp_received ASC
    """
    start_dt = datetime.fromtimestamp(start_ts, timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, timezone.utc)
    reader = conn.execute(query, [start_dt, end_dt]).fetch_record_batch(rows_per_batch=100_000)

    def _iter() -> Iterable[dict[str, Any]]:
        try:
            for batch in reader:
                for row in batch.to_pylist():
                    yield row
        finally:
            conn.close()

    return _iter()


def find_local_pmxt_files(cache_dir: str) -> list[Path]:
    cache = resolve_pmxt_cache_dir(cache_dir)
    if not cache.exists():
        return []
    rows: list[tuple[datetime, Path]] = []
    for path in cache.rglob("*.parquet"):
        hour = pmxt_hour_from_path(path)
        if hour is None:
            continue
        try:
            if path.stat().st_size <= 0:
                continue
        except OSError:
            continue
        rows.append((hour, path))
    rows.sort(key=lambda item: (item[0], str(item[1])))
    return [path for _, path in rows]


def pmxt_file_range(files: list[Path]) -> tuple[int, int]:
    hours = [hour for path in files if (hour := pmxt_hour_from_path(path)) is not None]
    if not hours:
        raise ValueError("No PMXT parquet files with polymarket_orderbook_YYYY-MM-DDTHH.parquet names were found.")
    start = min(hours)
    end = max(hours).replace(minute=0, second=0, microsecond=0)
    return int(start.timestamp()), int(end.timestamp()) + 3600


def pmxt_file_hour_starts(files: list[Path]) -> set[int]:
    return {
        int(hour.timestamp())
        for path in files
        if (hour := pmxt_hour_from_path(path)) is not None
    }


def pmxt_hour_from_path(path: Path) -> datetime | None:
    prefix = "polymarket_orderbook_"
    suffix = ".parquet"
    name = path.name
    if not name.startswith(prefix) or not name.endswith(suffix):
        return None
    stamp = name[len(prefix) : -len(suffix)]
    try:
        return datetime.strptime(stamp, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def resolve_pmxt_cache_dir(cache_dir: str) -> Path:
    path = Path(cache_dir)
    return path if path.is_absolute() else ROOT / path


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def replay_from_book_snapshots(
    records: list[dict[str, Any]],
    *,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
    min_total_usd: Decimal,
    max_depth_pct: Decimal,
    coverage: dict[str, Any],
    bucket_seconds: int,
) -> dict[str, Any]:
    opportunities: list[dict[str, Any]] = []
    best_by_bucket: dict[int, dict[str, Any]] = {}
    token_ids: set[str] = set()

    for record in records:
        restored = event_books_from_snapshot(record)
        if restored is None:
            continue
        event, books = restored
        for market in event.markets:
            token_ids.add(market.yes_token_id)
            token_ids.add(market.no_token_id)

        opportunity = check_event(
            event,
            books,
            min_gross_profit=min_gross_profit,
            min_total_usd=min_total_usd,
            max_depth_pct=max_depth_pct,
        )
        if opportunity is None:
            continue

        candidate = simulated_trade_from_opportunity(
            opportunity.to_dict(),
            record,
            investment_usd=investment_usd,
            slippage_pct=slippage_pct,
            min_gross_profit=min_gross_profit,
        )
        if candidate is None:
            continue

        opportunities.append(candidate)
        bucket_ts = int(candidate["timestamp_s"])
        existing = best_by_bucket.get(bucket_ts)
        if existing is None or candidate["simulated_pnl_usd"] > existing["simulated_pnl_usd"]:
            best_by_bucket[bucket_ts] = candidate

    opportunities.sort(key=lambda item: (item["timestamp_s"], item["simulated_pnl_usd"]))
    curve, trades = build_equity_curve(best_by_bucket, investment_usd)
    top_events = aggregate_top_events(opportunities)
    hourly = aggregate_hourly(opportunities)
    enriched_coverage = snapshot_coverage_payload(
        coverage,
        records,
        start_ts=start_ts,
        end_ts=end_ts,
        token_count=len(token_ids),
        bucket_seconds=bucket_seconds,
    )
    return {
        "params": {
            "hours": (end_ts - start_ts) / 3600,
            "fidelity_minutes": fidelity_minutes,
            "investment_usd": _float(investment_usd),
            "slippage_pct": _float(slippage_pct),
            "min_gross_profit": _float(min_gross_profit),
            "min_total_usd": _float(min_total_usd),
            "max_depth_pct": _float(max_depth_pct),
            "source": BOOK_SNAPSHOT_SOURCE,
            "fee_rate_bps": 0.0,
        },
        "coverage": enriched_coverage,
        "summary": {
            "opportunity_count": len(opportunities),
            "simulated_trade_count": max(0, len(curve) - 1),
            "ending_equity": curve[-1]["equity"] if curve else _float(investment_usd),
            "total_pnl_usd": (curve[-1]["equity"] - _float(investment_usd)) if curve else 0.0,
            "max_gross_after_slippage": max((row["gross_after_slippage"] for row in opportunities), default=0.0),
            "max_trade_pnl_usd": max((row["simulated_pnl_usd"] for row in opportunities), default=0.0),
        },
        "top_events": top_events,
        "hourly": hourly,
        "equity_curve": curve,
        "trades": trades,
        "opportunities": sorted(opportunities, key=lambda item: item["simulated_pnl_usd"], reverse=True)[:200],
    }


def event_books_from_snapshot(record: dict[str, Any]) -> tuple[NegRiskEvent, dict[str, LocalBook]] | None:
    data = record.get("data")
    if not isinstance(data, dict):
        return None
    raw_markets = data.get("markets")
    if not isinstance(raw_markets, list) or len(raw_markets) < 2:
        return None

    snapshot_time = datetime.fromtimestamp(int(record["snapshot_ts"]), timezone.utc)
    markets: list[NegRiskMarket] = []
    books: dict[str, LocalBook] = {}
    for raw in raw_markets:
        if not isinstance(raw, dict):
            return None
        tick = _dec(raw.get("min_tick_size") or raw.get("tick_size"))
        yes_bid = _dec(raw.get("yes_bid"))
        yes_ask = _dec(raw.get("yes_ask"))
        no_ask = _dec(raw.get("no_ask"))
        no_bid = _dec(raw.get("no_bid"), Decimal("0"))
        yes_depth = _dec(raw.get("yes_bid_depth"), Decimal("0"))
        no_depth = _dec(raw.get("no_ask_depth"), Decimal("0"))
        if yes_bid is None or yes_ask is None or no_ask is None or tick is None:
            return None

        market = NegRiskMarket(
            str(raw.get("condition_id") or ""),
            str(raw.get("question") or ""),
            str(raw.get("yes_token_id") or ""),
            str(raw.get("no_token_id") or ""),
            question_id=str(raw.get("question_id") or ""),
            question_index=raw.get("question_index"),
            min_tick_size=tick,
        )
        if not market.yes_token_id or not market.no_token_id:
            return None
        markets.append(market)

        yes_book = LocalBook(tick_size=tick)
        yes_book.apply_snapshot(
            bids=[(yes_bid, yes_depth)],
            asks=[(yes_ask, Decimal("1"))],
        )
        yes_book.updated_at = snapshot_time

        no_book = LocalBook()
        no_bids = [(no_bid, Decimal("1"))] if no_bid > 0 else []
        no_book.apply_snapshot(
            bids=no_bids,
            asks=[(no_ask, no_depth)],
        )
        no_book.updated_at = snapshot_time
        books[market.yes_token_id] = yes_book
        books[market.no_token_id] = no_book

    event = NegRiskEvent(
        event_id=str(data.get("event_id") or record.get("event_id") or ""),
        title=str(data.get("event_title") or record.get("event_title") or ""),
        neg_risk_market_id=str(data.get("neg_risk_market_id") or ""),
        liquidity=_optional_dec(data.get("liquidity")),
        markets=tuple(markets),
    )
    if not event.event_id:
        return None
    return event, books


def simulated_trade_from_opportunity(
    opportunity: dict[str, Any],
    record: dict[str, Any],
    *,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
) -> dict[str, Any] | None:
    slippage = slippage_pct / Decimal("100")
    raw_no_ask = _dec(opportunity["best_no_ask"])
    raw_sum_other_yes_bid = _dec(opportunity["sum_other_yes_bid"])
    max_qty = _dec(opportunity["max_qty"])
    effective_no_ask = raw_no_ask * (Decimal("1") + slippage)
    effective_sum_other_yes_bid = raw_sum_other_yes_bid * (Decimal("1") - slippage)
    gross_after_slippage = effective_sum_other_yes_bid - effective_no_ask
    if effective_no_ask <= 0 or gross_after_slippage < min_gross_profit:
        return None

    requested_qty = investment_usd / effective_no_ask
    executed_qty = min(requested_qty, max_qty)
    if executed_qty <= 0:
        return None

    used_capital = executed_qty * effective_no_ask
    unused_capital = max(Decimal("0"), investment_usd - used_capital)
    proceeds = executed_qty * effective_sum_other_yes_bid
    gross_pnl = executed_qty * gross_after_slippage
    fee_rate_bps = Decimal("0")
    fee_usd = (used_capital + proceeds) * fee_rate_bps / Decimal("10000")
    net_pnl = gross_pnl - fee_usd
    if net_pnl <= 0:
        return None

    return_pct = (net_pnl / used_capital) * Decimal("100") if used_capital > 0 else Decimal("0")
    timestamp_s = int(record["bucket_ts"])
    timestamp = datetime.fromtimestamp(timestamp_s, timezone.utc).isoformat()
    raw_gross_profit = _dec(opportunity["gross_profit"])
    result = {
        "timestamp": timestamp,
        "timestamp_s": timestamp_s,
        "snapshot_timestamp": datetime.fromtimestamp(int(record["snapshot_ts"]), timezone.utc).isoformat(),
        "snapshot_timestamp_s": int(record["snapshot_ts"]),
        "event_id": opportunity["event_id"],
        "event_title": opportunity["event_title"],
        "n_markets": opportunity["n_markets"],
        "best_no_idx": opportunity["best_no_idx"],
        "best_market_question": opportunity["best_market_question"],
        "sum_yes_ask": opportunity["sum_yes_ask"],
        "raw_gross_profit": _float(raw_gross_profit),
        "gross_after_slippage": _float(gross_after_slippage),
        "gross_per_share": _float(gross_after_slippage),
        "raw_no_ask": _float(raw_no_ask),
        "raw_sum_other_yes_bid": _float(raw_sum_other_yes_bid),
        "effective_no_ask": _float(effective_no_ask),
        "effective_sum_other_yes_bid": _float(effective_sum_other_yes_bid),
        "max_qty": _float(max_qty),
        "executed_qty": _float(executed_qty),
        "shares": _float(executed_qty),
        "investment_usd": _float(investment_usd),
        "used_capital_usd": _float(used_capital),
        "unused_capital_usd": _float(unused_capital),
        "simulated_pnl_usd": _float(net_pnl),
        "simulated_return_pct": _float(return_pct),
        "fee_rate_bps": _float(fee_rate_bps),
        "fee_usd": _float(fee_usd),
        "buy": {
            "side": "BUY_NO",
            "outcome": opportunity["best_market_question"],
            "raw_no_ask": _float(raw_no_ask),
            "raw_no_price": _float(raw_no_ask),
            "effective_no_ask": _float(effective_no_ask),
            "effective_no_price": _float(effective_no_ask),
            "shares": _float(executed_qty),
            "executed_qty": _float(executed_qty),
            "max_qty": _float(max_qty),
            "cost_usd": _float(used_capital),
            "used_capital_usd": _float(used_capital),
            "unused_capital_usd": _float(unused_capital),
        },
        "convert": {
            "action": "NO_i -> YES_j for j != i",
            "source_outcome": opportunity["best_market_question"],
            "converted_yes_count": int(opportunity["n_markets"]) - 1,
            "shares_per_yes": _float(executed_qty),
        },
        "sell": {
            "side": "SELL_CONVERTED_YES",
            "raw_sum_other_yes_bid": _float(raw_sum_other_yes_bid),
            "raw_sum_other_yes_price": _float(raw_sum_other_yes_bid),
            "effective_sum_other_yes_bid": _float(effective_sum_other_yes_bid),
            "effective_sum_other_yes": _float(effective_sum_other_yes_bid),
            "proceeds_usd": _float(proceeds),
        },
        "settlement": {
            "cost_usd": _float(used_capital),
            "used_capital_usd": _float(used_capital),
            "unused_capital_usd": _float(unused_capital),
            "proceeds_usd": _float(proceeds),
            "gross_pnl_usd": _float(gross_pnl),
            "fee_rate_bps": _float(fee_rate_bps),
            "fee_usd": _float(fee_usd),
            "pnl_usd": _float(net_pnl),
            "net_pnl_usd": _float(net_pnl),
            "return_pct": _float(return_pct),
            "equity_after": None,
        },
    }
    return result


def snapshot_coverage_payload(
    coverage: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    start_ts: int,
    end_ts: int,
    token_count: int,
    bucket_seconds: int,
) -> dict[str, Any]:
    start_s = coverage.get("coverage_start_s")
    end_s = coverage.get("coverage_end_s")
    requested_hours = (end_ts - start_ts) / 3600
    covered_hours = 0.0
    if start_s is not None and end_s is not None:
        covered_hours = max(0.0, (int(end_s) - int(start_s)) / 3600)
    bucket_seconds = max(1, bucket_seconds)
    complete_requested = (
        start_s is not None
        and end_s is not None
        and int(start_s) <= start_ts + bucket_seconds
        and int(end_s) >= end_ts - bucket_seconds
    )
    return {
        **coverage,
        "requested_hours": requested_hours,
        "covered_hours": covered_hours,
        "is_complete_requested": complete_requested,
        "is_complete_24h": complete_requested and requested_hours >= 24,
        "source_mode": BOOK_SNAPSHOT_SOURCE,
        "source_note": "Local orderbook snapshots; coverage starts when this service has been running and receiving complete books.",
        "token_count": token_count,
        "bucket_count": len({record["bucket_ts"] for record in records}),
        "bucketed_snapshot_count": len(records),
        "price_point_count": 0,
    }


def replay_from_histories(
    events: list[NegRiskEvent],
    histories: PriceSeries,
    *,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    investment_usd: Decimal,
    slippage_pct: Decimal,
    min_gross_profit: Decimal,
) -> dict[str, Any]:
    bucket_seconds = fidelity_minutes * 60
    buckets = list(range(ceil_to_bucket(start_ts, bucket_seconds), end_ts + 1, bucket_seconds))
    bucketed = {token_id: forward_fill(points, buckets) for token_id, points in histories.items()}
    opportunities: list[dict[str, Any]] = []
    best_by_bucket: dict[int, dict[str, Any]] = {}

    slippage = slippage_pct / Decimal("100")
    for event in events:
        for ts in buckets:
            yes_prices: list[Decimal] = []
            no_prices: list[Decimal] = []
            complete = True
            for market in event.markets:
                yes_price = bucketed.get(market.yes_token_id, {}).get(ts)
                no_price = bucketed.get(market.no_token_id, {}).get(ts)
                if yes_price is None or no_price is None:
                    complete = False
                    break
                yes_prices.append(yes_price)
                no_prices.append(no_price)
            if not complete or len(yes_prices) < 2:
                continue

            sum_yes = sum(yes_prices, Decimal("0"))
            if sum_yes <= Decimal("1") + min_gross_profit:
                continue

            best = None
            for idx, no_price in enumerate(no_prices):
                raw_sum_other_yes = sum_yes - yes_prices[idx]
                raw_gross = raw_sum_other_yes - no_price
                effective_no = no_price * (Decimal("1") + slippage)
                effective_sum_other_yes = raw_sum_other_yes * (Decimal("1") - slippage)
                gross_after_slippage = effective_sum_other_yes - effective_no
                if effective_no <= 0:
                    continue
                shares = investment_usd / effective_no
                pnl = shares * gross_after_slippage
                proceeds = shares * effective_sum_other_yes
                candidate = {
                    "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                    "timestamp_s": ts,
                    "event_id": event.event_id,
                    "event_title": event.title,
                    "n_markets": len(event.markets),
                    "best_no_idx": idx,
                    "best_market_question": event.markets[idx].question,
                    "sum_yes_price": _float(sum_yes),
                    "raw_gross_profit": _float(raw_gross),
                    "gross_after_slippage": _float(gross_after_slippage),
                    "no_price": _float(no_price),
                    "sum_other_yes_price": _float(raw_sum_other_yes),
                    "effective_no_price": _float(effective_no),
                    "effective_sum_other_yes": _float(effective_sum_other_yes),
                    "shares": _float(shares),
                    "investment_usd": _float(investment_usd),
                    "simulated_pnl_usd": _float(pnl),
                    "simulated_return_pct": _float((pnl / investment_usd) * Decimal("100")),
                    "buy": {
                        "side": "BUY_NO",
                        "outcome": event.markets[idx].question,
                        "raw_no_price": _float(no_price),
                        "effective_no_price": _float(effective_no),
                        "shares": _float(shares),
                        "cost_usd": _float(investment_usd),
                    },
                    "convert": {
                        "action": "NO_i -> YES_j for j != i",
                        "source_outcome": event.markets[idx].question,
                        "converted_yes_count": len(event.markets) - 1,
                        "shares_per_yes": _float(shares),
                    },
                    "sell": {
                        "side": "SELL_CONVERTED_YES",
                        "raw_sum_other_yes_price": _float(raw_sum_other_yes),
                        "effective_sum_other_yes": _float(effective_sum_other_yes),
                        "proceeds_usd": _float(proceeds),
                    },
                    "settlement": {
                        "cost_usd": _float(investment_usd),
                        "proceeds_usd": _float(proceeds),
                        "pnl_usd": _float(pnl),
                        "return_pct": _float((pnl / investment_usd) * Decimal("100")),
                        "equity_after": None,
                    },
                }
                if best is None or candidate["simulated_pnl_usd"] > best["simulated_pnl_usd"]:
                    best = candidate

            if best and Decimal(str(best["gross_after_slippage"])) >= min_gross_profit:
                opportunities.append(best)
                existing = best_by_bucket.get(ts)
                if existing is None or best["simulated_pnl_usd"] > existing["simulated_pnl_usd"]:
                    best_by_bucket[ts] = best

    opportunities.sort(key=lambda item: (item["timestamp_s"], item["simulated_pnl_usd"]))
    curve, trades = build_equity_curve(best_by_bucket, investment_usd)
    top_events = aggregate_top_events(opportunities)
    hourly = aggregate_hourly(opportunities)
    return {
        "params": {
            "hours": (end_ts - start_ts) / 3600,
            "fidelity_minutes": fidelity_minutes,
            "investment_usd": _float(investment_usd),
            "slippage_pct": _float(slippage_pct),
            "min_gross_profit": _float(min_gross_profit),
            "source": PRICE_PROXY_SOURCE,
        },
        "coverage": {
            "event_count": len(events),
            "token_count": len(collect_token_ids(events)),
            "price_token_count": len(histories),
            "bucket_count": len(buckets),
            "price_point_count": sum(len(points) for points in histories.values()),
            "coverage_start": datetime.fromtimestamp(start_ts, timezone.utc).isoformat(),
            "coverage_end": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
            "requested_hours": (end_ts - start_ts) / 3600,
            "covered_hours": (end_ts - start_ts) / 3600,
            "is_complete_requested": True,
            "is_complete_24h": (end_ts - start_ts) >= 24 * 3600,
            "source_mode": PRICE_PROXY_SOURCE,
            "source_note": "Price proxy uses official historical price points and does not include historical orderbook depth.",
        },
        "summary": {
            "opportunity_count": len(opportunities),
            "simulated_trade_count": max(0, len(curve) - 1),
            "ending_equity": curve[-1]["equity"] if curve else _float(investment_usd),
            "total_pnl_usd": (curve[-1]["equity"] - _float(investment_usd)) if curve else 0.0,
            "max_gross_after_slippage": max((row["gross_after_slippage"] for row in opportunities), default=0.0),
            "max_trade_pnl_usd": max((row["simulated_pnl_usd"] for row in opportunities), default=0.0),
        },
        "top_events": top_events,
        "hourly": hourly,
        "equity_curve": curve,
        "trades": trades,
        "opportunities": sorted(opportunities, key=lambda item: item["simulated_pnl_usd"], reverse=True)[:200],
    }


def build_equity_curve(
    best_by_bucket: dict[int, dict[str, Any]],
    investment_usd: Decimal,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    equity = _float(investment_usd)
    curve = [{"timestamp": None, "equity": equity, "pnl": 0.0, "event_title": "Initial capital"}]
    trades: list[dict[str, Any]] = []
    for ts in sorted(best_by_bucket):
        trade = best_by_bucket[ts]
        pnl = float(trade["simulated_pnl_usd"])
        equity += pnl
        trade = dict(trade)
        trade["settlement"] = dict(trade["settlement"])
        trade["settlement"]["equity_after"] = equity
        curve.append(
            {
                "timestamp": trade["timestamp"],
                "equity": equity,
                "pnl": pnl,
                "event_title": trade["event_title"],
                "gross_after_slippage": trade["gross_after_slippage"],
                "trade": trade,
            }
        )
        trades.append(trade)
    return curve, trades


def aggregate_top_events(opportunities: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in opportunities:
        item = grouped.setdefault(
            row["event_id"],
            {
                "event_id": row["event_id"],
                "event_title": row["event_title"],
                "count": 0,
                "max_gross_after_slippage": 0.0,
                "max_trade_pnl_usd": 0.0,
                "total_pnl_usd": 0.0,
                "latest_at": None,
            },
        )
        item["count"] += 1
        item["max_gross_after_slippage"] = max(item["max_gross_after_slippage"], row["gross_after_slippage"])
        item["max_trade_pnl_usd"] = max(item["max_trade_pnl_usd"], row["simulated_pnl_usd"])
        item["total_pnl_usd"] += row["simulated_pnl_usd"]
        if item["latest_at"] is None or row["timestamp"] > item["latest_at"]:
            item["latest_at"] = row["timestamp"]
    return sorted(grouped.values(), key=lambda item: item["max_trade_pnl_usd"], reverse=True)[:limit]


def aggregate_hourly(opportunities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in opportunities:
        hour = datetime.fromtimestamp(row["timestamp_s"], timezone.utc).replace(minute=0, second=0, microsecond=0)
        key = hour.isoformat()
        item = grouped.setdefault(
            key,
            {"hour_start": key, "count": 0, "max_gross_after_slippage": 0.0, "max_trade_pnl_usd": 0.0},
        )
        item["count"] += 1
        item["max_gross_after_slippage"] = max(item["max_gross_after_slippage"], row["gross_after_slippage"])
        item["max_trade_pnl_usd"] = max(item["max_trade_pnl_usd"], row["simulated_pnl_usd"])
    return [grouped[key] for key in sorted(grouped)]


def forward_fill(points: list[tuple[int, Decimal]], buckets: list[int]) -> dict[int, Decimal]:
    result: dict[int, Decimal] = {}
    idx = 0
    current: Decimal | None = None
    for bucket in buckets:
        while idx < len(points) and points[idx][0] <= bucket:
            current = points[idx][1]
            idx += 1
        if current is not None:
            result[bucket] = current
    return result


def collect_token_ids(events: Iterable[NegRiskEvent]) -> set[str]:
    token_ids: set[str] = set()
    for event in events:
        for market in event.markets:
            token_ids.add(market.yes_token_id)
            token_ids.add(market.no_token_id)
    return token_ids


def ceil_to_bucket(ts: int, bucket_seconds: int) -> int:
    remainder = ts % bucket_seconds
    return ts if remainder == 0 else ts + bucket_seconds - remainder


def _dec(value: Any, default: Decimal | None = None) -> Decimal:
    if value is None:
        if default is None:
            raise InvalidOperation("missing decimal")
        return default
    return Decimal(str(value))


def _optional_dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _float(value: Decimal) -> float:
    return float(value)
