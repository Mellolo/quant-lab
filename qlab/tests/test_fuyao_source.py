"""FuyaoDataSource 单测 — 假 HTTP 客户端，不触网。"""

from __future__ import annotations

import pandas as pd
import pytest

from qlab.core.enums import Freq
from qlab.core.exceptions import DataUnavailableError
from qlab.core.schema import SCHEMA_DAILY_BAR, validate_schema
from qlab.data.sources.fuyao import (
    FuyaoAPIError,
    FuyaoDataSource,
    _ms_to_ts,
    _split_10y,
    _to_ms,
)


class FakeClient:
    def __init__(self, routes: dict[str, object] | None = None):
        self.routes = routes or {}
        self.calls: list[tuple[str, dict]] = []

    def get(self, path: str, **params):
        self.calls.append((path, params))
        payload = self.routes.get(path)
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list):
            return {"item": payload}
        raise FuyaoAPIError(2003, f"no route {path}")

    def items(self, path: str, **params):
        data = self.get(path, **params)
        item = data.get("item")
        if item is None:
            return []
        if isinstance(item, list):
            return [x for x in item if isinstance(x, dict)]
        if isinstance(item, dict):
            return [item]
        return []


def _ms(day: str) -> int:
    return _to_ms(pd.Timestamp(day))


def _bar(day: str, close: float, volume: int = 1_000_000, turnover: float = 1.0e8):
    return {
        "date_ms": _ms(day),
        "open_price": close * 0.99,
        "high_price": close * 1.01,
        "low_price": close * 0.98,
        "close_price": close,
        "volume": volume,
        "turnover": turnover,
    }


def test_ms_roundtrip_shanghai():
    ts = _ms_to_ts(_to_ms("2024-06-03"))
    assert ts == pd.Timestamp("2024-06-03")


def test_split_10y_cuts_long_window():
    chunks = _split_10y("2010-01-01", "2026-01-01")
    assert len(chunks) >= 2
    assert chunks[0][0] == pd.Timestamp("2010-01-01")
    assert chunks[-1][1] == pd.Timestamp("2026-01-01")


def test_fetch_bars_maps_adjust_and_schema():
    raw = [_bar("2024-06-03", 100.0), _bar("2024-06-04", 110.0, volume=0, turnover=0.0)]
    post = [_bar("2024-06-03", 200.0), _bar("2024-06-04", 220.0, volume=0, turnover=0.0)]
    client = FakeClient()

    def get(path, **params):
        client.calls.append((path, params))
        assert params["thscode"] == "600519.SH"
        rows = post if params["adjust"] == "backward" else raw
        return {"item": rows}

    client.get = get  # type: ignore[method-assign]
    src = FuyaoDataSource(client=client)
    bars = src.fetch_bars(
        ["600519.SH"],
        pd.Timestamp("2024-06-03"),
        pd.Timestamp("2024-06-04"),
    )
    validate_schema(bars, SCHEMA_DAILY_BAR)
    row = bars.xs("600519.SH", level="symbol").loc[pd.Timestamp("2024-06-03")]
    assert row["close"] == 200.0
    assert row["close_raw"] == 100.0
    assert row["adj_factor"] == 2.0
    assert bool(row["is_suspended"]) is False
    sus = bars.xs("600519.SH", level="symbol").loc[pd.Timestamp("2024-06-04")]
    assert bool(sus["is_suspended"]) is True
    assert sus["volume"] == 0
    assert pd.isna(sus["close"])


def test_fetch_bars_rejects_intraday():
    src = FuyaoDataSource(client=FakeClient())
    with pytest.raises(DataUnavailableError, match="日线"):
        src.fetch_bars(
            ["600519.SH"],
            pd.Timestamp("2024-06-03"),
            pd.Timestamp("2024-06-04"),
            freq=Freq.MIN_1,
        )


def test_anomaly_and_dragon_tiger():
    client = FakeClient({
        "/api/a-share/special-data/anomaly-analysis-stock": [
            {"thscode": "600519.SH", "reason": "涨停"},
        ],
        "/api/a-share/special-data/dragon-tiger-list": {
            "trade_date": "2024-06-04",
            "board_type": "all",
            "stock_items": [
                {
                    "thscode": "600519.SH",
                    "hot_rank": 1,
                    "limit_reason": "涨幅偏离",
                    "buy_value": 1.2e8,
                    "sell_value": 3e7,
                    "net_value": 9e7,
                }
            ],
        },
    })
    src = FuyaoDataSource(client=client)
    anomaly = src.anomaly_analysis_stock("600519.SH")
    assert list(anomaly["symbol"]) == ["600519.SH"]
    board = src.dragon_tiger_list(pd.Timestamp("2024-06-04"))
    assert list(board["symbol"]) == ["600519.SH"]
    assert board.iloc[0]["net_value"] == 9e7
    assert board.iloc[0]["trade_date"] == "2024-06-04"


def test_current_universe_hs_a_filters_bj_and_st():
    client = FakeClient({
        "/api/meta/tickers/list": [
            {"thscode": "600519.SH", "name": "贵州茅台"},
            {"thscode": "688001.SH", "name": "华兴源创"},
            {"thscode": "830799.BJ", "name": "艾融软件"},
            {"thscode": "600000.SH", "name": "*ST示例"},
        ],
    })
    src = FuyaoDataSource(client=client)
    uni = src.current_universe("hs_a")
    symbols = set(uni.index.astype(str))
    assert "600519.SH" in symbols
    assert "688001.SH" in symbols
    assert "830799.BJ" not in symbols
    assert "600000.SH" not in symbols


def test_financial_indicators_flattens_abilities():
    client = FakeClient({
        "/api/a-share/financials/indicators": {
            "thscode": "600519.SH",
            "report": "2024-4",
            "abilities": [
                {
                    "ability": "growth",
                    "indicators": [
                        {"index_id": "total_assets_growth_ratio", "value": "15.7"},
                    ],
                }
            ],
        },
    })
    src = FuyaoDataSource(client=client)
    df = src.financial_indicators("600519.SH", "2024-4")
    assert list(df["index_id"]) == ["total_assets_growth_ratio"]
    assert df.iloc[0]["value"] == 15.7
    assert df.iloc[0]["ability"] == "growth"


def test_limit_up_ladder_flattens_boards():
    client = FakeClient({
        "/api/a-share/special-data/limit-up-ladder": {
            "item": [
                {
                    "date": "2026-08-14",
                    "boards": {
                        "two_board": [
                            {"thscode": "603986.SH", "name": "兆易创新", "board_num": 2},
                        ],
                        "three_board": [],
                    },
                }
            ],
        },
    })
    src = FuyaoDataSource(client=client)
    df = src.limit_up_ladder()
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "603986.SH"
    assert df.iloc[0]["board"] == "two_board"


def test_limit_pool_sends_date_ms():
    client = FakeClient({
        "/api/a-share/special-data/limit-up-pool": [
            {"thscode": "600519.SH", "name": "贵州茅台"},
        ],
    })
    src = FuyaoDataSource(client=client)
    df = src.limit_up_pool("2026-07-15")
    path, params = client.calls[-1]
    assert path.endswith("limit-up-pool")
    assert "date_ms" in params
    assert "date" not in params
    assert df.iloc[0]["date"] == pd.Timestamp("2026-07-15")


def test_fetch_call_auction_rejects_historical_date():
    src = FuyaoDataSource(client=FakeClient())
    with pytest.raises(DataUnavailableError, match="集合竞价"):
        src.fetch_call_auction(
            ["600519.SH"],
            pd.Timestamp("2026-07-15"),
            pd.Timestamp("2026-07-15"),
        )


def test_fundamentals_do_not_fake_total_revenue_or_stale_announce():
    client = FakeClient({
        "/api/a-share/financials/income-statements": [
            {
                "thscode": "600519.SH",
                "fiscal_year": 2025,
                "fiscal_period": "Q2",
                "report_date": "2026-08-15",
                "period_end": "2025-06-30",
                "operating_income": 8.9e10,
                "net_profit": 4.7e10,
                "parent_holder_net_profit": 4.5e10,
            },
            {
                "thscode": "600519.SH",
                "fiscal_year": 2026,
                "fiscal_period": "Q1",
                "report_date": "2026-04-25",
                "period_end": "2026-03-31",
                "operating_income": 5.4e10,
                "net_profit": 2.8e10,
                "parent_holder_net_profit": 2.7e10,
            },
        ],
        "/api/a-share/financials/balance-sheets": [],
        "/api/a-share/financials/cash-flow-statements": [],
    })
    src = FuyaoDataSource(client=client)
    df = src.fetch_fundamentals(
        ["600519.SH"],
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2026-07-31"),
    )
    stale = df[df["report_period"] == pd.Timestamp("2025-06-30")].iloc[0]
    fresh = df[df["report_period"] == pd.Timestamp("2026-03-31")].iloc[0]
    assert pd.isna(stale["total_revenue"])
    assert stale["operating_revenue"] == 8.9e10
    assert pd.isna(stale["announce_date"])
    assert fresh["announce_date"] == pd.Timestamp("2026-04-25")
    assert pd.isna(fresh["total_revenue"])


def test_code_3002_is_empty_frame():
    client = FakeClient({
        "/api/a-share/corporate-actions/adjustment-factors": FuyaoAPIError(
            3002, "No adjustment events"
        ),
    })
    src = FuyaoDataSource(client=client)
    df = src.adjustment_factors("600519.SH", "2024-06-01", "2024-06-10")
    assert df.empty


def test_missing_protocol_fields_fail_loud():
    src = FuyaoDataSource(client=FakeClient())
    with pytest.raises(DataUnavailableError, match="两融"):
        src.fetch_margin_trading(["600519.SH"], pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
    with pytest.raises(DataUnavailableError, match="指数估值"):
        src.fetch_index_valuation("000985.CSI", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
    with pytest.raises(DataUnavailableError, match="PIT"):
        src.fetch_universe("hs_a", (pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-04")))
    with pytest.raises(DataUnavailableError, match="席位"):
        src.fetch_billboard(pd.Timestamp("2024-06-04"))
    with pytest.raises(DataUnavailableError, match="热度"):
        src.hot_stock_list_history("2026-07-15")
    with pytest.raises(DataUnavailableError, match="持仓"):
        src.fund_stock_history("510300.SH")
    with pytest.raises(DataUnavailableError, match="新闻"):
        src.fund_news("510300.SH")


@pytest.mark.fuyao
def test_live_snapshot_and_historical():
    src = FuyaoDataSource()
    days = src.trading_days()
    assert not days.empty
    snap = src.prices_snapshot(["600519.SH"])
    assert not snap.empty
    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=14)
    bars = src.fetch_bars(["600519.SH"], start, end)
    validate_schema(bars, SCHEMA_DAILY_BAR)
    assert len(bars) >= 3
