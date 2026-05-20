from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8010


@dataclass(frozen=True)
class GammaConfig:
    base_url: str = "https://gamma-api.polymarket.com"
    page_size: int = 500


@dataclass(frozen=True)
class ClobConfig:
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    rest_url: str = "https://clob.polymarket.com"


@dataclass(frozen=True)
class ExecutionConfig:
    enabled: bool = False


@dataclass(frozen=True)
class HistoryConfig:
    lookback_hours: int = 24
    fidelity_minutes: int = 5
    batch_size: int = 20
    max_events: int = 100
    cache_ttl_secs: int = 300
    book_db_path: str = "data/negrisk_book_history.sqlite"
    snapshot_interval_secs: int = 5
    retention_hours: int = 72
    max_snapshot_events: int = 100
    max_book_age_secs: int = 30
    pmxt_cache_dir: str = "data/pmxt_cache"


@dataclass(frozen=True)
class EngineConfig:
    event_refresh_secs: int = 300
    scan_interval_ms: int = 250
    min_event_liquidity: Decimal = Decimal("0")
    max_markets_per_event: int = 60
    max_events: int = 500
    ws_chunk_size: int = 400
    ws_reconnect_secs: int = 3


@dataclass(frozen=True)
class ArbConfig:
    min_gross_profit: Decimal = Decimal("0.01")
    min_total_usd: Decimal = Decimal("2.0")
    cooldown_secs: int = 120
    max_depth_pct: Decimal = Decimal("0.5")


@dataclass(frozen=True)
class AppConfig:
    server: ServerConfig = ServerConfig()
    gamma: GammaConfig = GammaConfig()
    clob: ClobConfig = ClobConfig()
    execution: ExecutionConfig = ExecutionConfig()
    history: HistoryConfig = HistoryConfig()
    engine: EngineConfig = EngineConfig()
    arb: ArbConfig = ArbConfig()

    @classmethod
    def load_default(cls) -> "AppConfig":
        local_path = ROOT / "config" / "local.toml"
        default_path = ROOT / "config" / "default.toml"
        return cls.load(local_path if local_path.exists() else default_path)

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        with Path(path).open("rb") as f:
            raw = tomllib.load(f)

        return cls(
            server=ServerConfig(**raw.get("server", {})),
            gamma=GammaConfig(**raw.get("gamma", {})),
            clob=ClobConfig(**raw.get("clob", {})),
            execution=ExecutionConfig(**raw.get("execution", {})),
            history=_history_config(raw.get("history", {})),
            engine=_engine_config(raw.get("engine", {})),
            arb=_arb_config(raw.get("arb", {})),
        )


def _decimal(value: object, default: str) -> Decimal:
    if value is None:
        return Decimal(default)
    return Decimal(str(value))


def _engine_config(raw: dict) -> EngineConfig:
    return EngineConfig(
        event_refresh_secs=int(raw.get("event_refresh_secs", 300)),
        scan_interval_ms=int(raw.get("scan_interval_ms", 250)),
        min_event_liquidity=_decimal(raw.get("min_event_liquidity"), "0"),
        max_markets_per_event=int(raw.get("max_markets_per_event", 60)),
        max_events=int(raw.get("max_events", 500)),
        ws_chunk_size=int(raw.get("ws_chunk_size", 400)),
        ws_reconnect_secs=int(raw.get("ws_reconnect_secs", 3)),
    )


def _history_config(raw: dict) -> HistoryConfig:
    max_events = int(raw.get("max_events", 100))
    return HistoryConfig(
        lookback_hours=int(raw.get("lookback_hours", 24)),
        fidelity_minutes=int(raw.get("fidelity_minutes", 5)),
        batch_size=int(raw.get("batch_size", 20)),
        max_events=max_events,
        cache_ttl_secs=int(raw.get("cache_ttl_secs", 300)),
        book_db_path=str(raw.get("book_db_path", "data/negrisk_book_history.sqlite")),
        snapshot_interval_secs=int(raw.get("snapshot_interval_secs", 5)),
        retention_hours=int(raw.get("retention_hours", 72)),
        max_snapshot_events=int(raw.get("max_snapshot_events", max_events)),
        max_book_age_secs=int(raw.get("max_book_age_secs", 30)),
        pmxt_cache_dir=str(raw.get("pmxt_cache_dir", "data/pmxt_cache")),
    )


def _arb_config(raw: dict) -> ArbConfig:
    return ArbConfig(
        min_gross_profit=_decimal(raw.get("min_gross_profit"), "0.01"),
        min_total_usd=_decimal(raw.get("min_total_usd"), "2.0"),
        cooldown_secs=int(raw.get("cooldown_secs", 120)),
        max_depth_pct=_decimal(raw.get("max_depth_pct"), "0.5"),
    )
