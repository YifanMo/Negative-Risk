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

Open `http://127.0.0.1:8010/`.

## Test

```bash
python3 -m pytest
```

## Config

Defaults live in `config/default.toml`. Put local overrides in
`config/local.toml`; it has the same shape and is loaded first when present.
