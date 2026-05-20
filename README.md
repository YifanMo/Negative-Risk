# Negative-Risk Local Monitor

Local paper-trading monitor for Polymarket NegRisk conversion arbitrage.

It fetches active NegRisk events from Gamma, maintains CLOB market WebSocket
order books, and shows paper opportunities for:

```text
Buy NO_i -> simulate convertPositions -> sell YES_j for j != i
```

Real trading is intentionally disabled. This project does not load private keys,
submit orders, call the relayer, or touch Gnosis Safe state.

## Run

```bash
python3 api/server.py
```

Open:

- `http://127.0.0.1:8010/monitor` for the live monitor
- `http://127.0.0.1:8010/history` for local orderbook snapshot replay

The history page defaults to strict `book-snapshot` mode. It replays local
bid/ask/depth snapshots collected while this service is running, so coverage
starts only after the monitor has connected and recorded complete books. The
older Polymarket historical price-point approximation is still available as
`source=price-proxy`, but it is only a comparison mode and does not represent
executable historical depth.

`source=pmxt-archive` replays local PMXT hourly Polymarket CLOB Parquet files
from `data/pmxt_cache`. In this mode the lookback-hours input is ignored: the
backtest window is the range covered by local
`polymarket_orderbook_YYYY-MM-DDTHH.parquet` files. The app does not download
archive files automatically.

## Test

```bash
python3 -m pytest
```

## Config

Defaults live in `config/default.toml`. Put local overrides in
`config/local.toml`; it has the same shape and is loaded first when present.
