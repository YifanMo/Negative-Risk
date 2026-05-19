from decimal import Decimal

from negrisk.gamma import parse_gamma_event


def base_event(markets):
    return {
        "id": "1",
        "title": "Test event",
        "negRisk": True,
        "active": True,
        "closed": False,
        "archived": False,
        "negRiskMarketID": "0xabc",
        "liquidity": "100.5",
        "markets": markets,
    }


def market(idx, token_ids='["yes", "no"]'):
    return {
        "conditionId": f"cond-{idx}",
        "question": f"Outcome {idx}",
        "clobTokenIds": token_ids,
        "active": True,
        "closed": False,
        "questionID": f"q-{idx}",
        "groupItemThreshold": str(idx),
        "orderPriceMinTickSize": "0.01",
    }


def test_parse_gamma_event_happy_path():
    event = parse_gamma_event(base_event([market(0), market(1, '["yes2", "no2"]')]), 60)

    assert event is not None
    assert event.event_id == "1"
    assert event.liquidity == Decimal("100.5")
    assert len(event.markets) == 2
    assert event.markets[0].yes_token_id == "yes"
    assert event.markets[0].question_index == 0
    assert event.markets[0].min_tick_size == Decimal("0.01")


def test_parse_gamma_event_rejects_missing_tokens():
    event = parse_gamma_event(base_event([market(0, "[]"), market(1, "not-json")]), 60)

    assert event is None


def test_parse_gamma_event_rejects_too_many_markets():
    event = parse_gamma_event(base_event([market(i, f'["yes{i}", "no{i}"]') for i in range(3)]), 2)

    assert event is None


def test_parse_gamma_event_rejects_non_negrisk():
    raw = base_event([market(0), market(1, '["yes2", "no2"]')])
    raw["negRisk"] = False

    assert parse_gamma_event(raw, 60) is None
