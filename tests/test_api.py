from fastapi.testclient import TestClient

import api.server as server


async def noop() -> None:
    return None


def test_monitor_and_history_pages(monkeypatch):
    monkeypatch.setattr(server.engine, "start", noop)
    monkeypatch.setattr(server.engine, "stop", noop)

    with TestClient(server.app) as client:
        monitor = client.get("/monitor")
        history = client.get("/history")

    assert monitor.status_code == 200
    assert "Negative-Risk Monitor" in monitor.text
    assert "All Events" in monitor.text
    assert "Equity Curve" not in monitor.text
    assert history.status_code == 200
    assert "Negative-Risk History" in history.text
    assert "Investment USD" in history.text
    assert "Trade Details" in history.text
    assert "Hourly Distribution" not in history.text


def test_history_replay_api(monkeypatch):
    monkeypatch.setattr(server.engine, "start", noop)
    monkeypatch.setattr(server.engine, "stop", noop)

    async def fake_replay(params, *, force_refresh=False):
        return {
            "summary": {"opportunity_count": 0, "ending_equity": 1000, "total_pnl_usd": 0},
            "top_events": [],
            "hourly": [],
            "equity_curve": [{"timestamp": None, "equity": 1000, "pnl": 0}],
            "trades": [],
            "params": {"hours": params.hours, "fidelity_minutes": params.fidelity_minutes},
            "coverage": {"event_count": 0, "token_count": 0, "price_point_count": 0},
        }

    monkeypatch.setattr(server.history_replay, "replay", fake_replay)

    with TestClient(server.app) as client:
        replay = client.get("/api/history/replay?hours=24&fidelity=5&investment_usd=1000&slippage_pct=0.5")
        summary = client.get("/api/history/summary?hours=24&fidelity=5&investment_usd=1000&slippage_pct=0.5")
        top = client.get("/api/history/top-events?hours=24&fidelity=5&investment_usd=1000&slippage_pct=0.5")
        hourly = client.get("/api/history/hourly?hours=24&fidelity=5&investment_usd=1000&slippage_pct=0.5")

    assert replay.status_code == 200
    assert replay.json()["summary"]["ending_equity"] == 1000
    assert summary.status_code == 200
    assert summary.json()["opportunity_count"] == 0
    assert top.json() == []
    assert hourly.json() == []


def test_events_list_api(monkeypatch):
    monkeypatch.setattr(server.engine, "start", noop)
    monkeypatch.setattr(server.engine, "stop", noop)
    monkeypatch.setattr(
        server.engine,
        "get_events_summary",
        lambda: [
            {
                "event_id": "e1",
                "event_title": "Test event",
                "n_markets": 3,
                "complete_market_count": 3,
                "best_market_question": "A",
                "best_profit_1_share": 0.12,
                "has_opportunity": False,
            }
        ],
    )

    with TestClient(server.app) as client:
        response = client.get("/api/events")

    assert response.status_code == 200
    assert response.json()[0]["event_id"] == "e1"


def test_event_detail_api_includes_orderbook_levels(monkeypatch):
    monkeypatch.setattr(server.engine, "start", noop)
    monkeypatch.setattr(server.engine, "stop", noop)
    monkeypatch.setattr(
        server.engine,
        "get_event_detail",
        lambda event_id: {
            "event_id": event_id,
            "title": "Test event",
            "markets": [
                {
                    "index": 0,
                    "question": "A",
                    "yes_book": {
                        "best_bid": 0.4,
                        "best_ask": 0.42,
                        "bid_levels": [{"price": 0.4, "size": 10, "notional": 4.0}],
                        "ask_levels": [{"price": 0.42, "size": 8, "notional": 3.36}],
                    },
                    "no_book": {"bid_levels": [], "ask_levels": []},
                }
            ],
        },
    )

    with TestClient(server.app) as client:
        response = client.get("/api/events/e1")

    assert response.status_code == 200
    market = response.json()["markets"][0]
    assert market["yes_book"]["bid_levels"][0]["notional"] == 4.0
    assert market["no_book"]["ask_levels"] == []
