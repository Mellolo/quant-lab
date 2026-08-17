"""资金博弈：仓位打分。"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

def test_long_hold_uses_strong_center_not_origin():
    from qlab.diagnostics.flow.book import EXTEND_ATR, score_book

    prices = np.linspace(10.0, 16.0, 21)
    out = score_book(
        direction=1.0,
        close_i=16.0,
        atr_i=0.5,
        camp_high=16.0,
        camp_low=10.0,
        prices=prices,
        highs=prices,
        lows=prices,
        weight=np.ones(21),
        pts=[(0, 10.0), (20, 16.0)],
        origin_px=10.0,
    )
    assert out["trapped"] == pytest.approx(0.0)
    assert out["poc"] == pytest.approx(13.0)
    assert out["extend"] == pytest.approx(6.0)
    assert out["hold"] == pytest.approx(
        min(1.0, EXTEND_ATR / (EXTEND_ATR + 6.0), out["oil"])
    )


def test_long_hold_weak_overhead_hurts_more_than_strong():
    from qlab.diagnostics.flow import score_book

    prices = np.array([10.0] * 8 + [14.0] * 8 + [20.0] + [12.0])
    lows = prices.copy()
    weight = np.ones(18)
    weight[16] = 0.05

    strong_highs = prices.copy()
    weak_highs = prices.copy()
    weak_highs[8:16] = 15.5

    def _hold(highs: np.ndarray) -> dict[str, float]:
        return score_book(
            direction=1.0,
            close_i=12.0,
            atr_i=1.0,
            camp_high=20.0,
            camp_low=10.0,
            prices=prices,
            highs=highs,
            lows=lows,
            weight=weight,
            pts=[(0, 10.0), (8, 14.0), (16, 20.0), (17, 12.0)],
            origin_px=10.0,
        )

    strong = _hold(strong_highs)
    weak = _hold(weak_highs)
    assert weak["trapped"] > strong["trapped"]
    assert weak["hold"] < strong["hold"]


def test_long_hold_falls_on_high_climax():
    from qlab.diagnostics.flow import score_book

    prices = np.array([10.0, 10.0, 20.0, 20.0, 20.0, 20.0])
    weight = np.array([1.0, 1.0, 10.0, 10.0, 10.0, 10.0])
    out = score_book(
        direction=1.0,
        close_i=20.0,
        atr_i=1.0,
        camp_high=20.0,
        camp_low=10.0,
        prices=prices,
        highs=prices,
        lows=prices,
        weight=weight,
        pts=[(0, 10.0), (5, 20.0)],
        origin_px=10.0,
    )
    assert out["trapped"] == pytest.approx(0.0)
    assert out["oil"] == pytest.approx(2.0 / 42.0)
    assert out["hold"] == pytest.approx(out["oil"])


def test_long_hold_empty_without_volume():
    from qlab.diagnostics.flow import score_book

    prices = np.linspace(10.0, 16.0, 8)
    out = score_book(
        direction=1.0,
        close_i=16.0,
        atr_i=0.5,
        camp_high=16.0,
        camp_low=10.0,
        prices=prices,
        highs=prices,
        lows=prices,
        weight=None,
        pts=[(0, 10.0), (7, 16.0)],
        origin_px=10.0,
    )
    assert np.isnan(out["hold"])


def test_short_absorbed_bounce_hurts_less_than_thin_squeeze():
    from qlab.diagnostics.flow import score_book

    prices = np.array([20.0, 18.0, 16.0, 14.0, 12.0, 10.0, 11.0, 12.0, 13.0, 13.5, 14.0])
    pts = [(0, 20.0), (5, 10.0), (10, 14.0)]

    def _hold(highs: np.ndarray, lows: np.ndarray, weight: np.ndarray) -> dict[str, float]:
        return score_book(
            direction=-1.0,
            close_i=14.0,
            atr_i=1.0,
            camp_high=20.0,
            camp_low=10.0,
            prices=prices,
            highs=highs,
            lows=lows,
            weight=weight,
            pts=pts,
            origin_px=20.0,
        )

    absorbed = _hold(
        prices + 0.05,
        prices - 0.05,
        np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 8.0, 8.0, 8.0, 8.0]),
    )
    thin = _hold(
        np.concatenate([prices[:6] + 0.2, prices[6:] + 2.0]),
        np.concatenate([prices[:6] - 0.2, prices[6:] - 0.2]),
        np.array([8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
    )
    assert absorbed["absorb"] > thin["absorb"]
    assert absorbed["squeeze"] > thin["squeeze"]
    assert absorbed["hold"] > thin["hold"]


def test_short_hold_is_squeeze_without_volume():
    from qlab.diagnostics.flow import score_book

    out = score_book(
        direction=-1.0,
        close_i=12.0,
        atr_i=1.0,
        camp_high=20.0,
        camp_low=10.0,
        prices=np.array([20.0, 10.0, 12.0]),
        highs=np.array([20.0, 10.0, 12.0]),
        lows=np.array([20.0, 10.0, 12.0]),
        weight=None,
        pts=[(0, 20.0), (1, 10.0), (2, 12.0)],
        origin_px=20.0,
    )
    assert out["squeeze"] == pytest.approx(0.8)
    assert np.isnan(out["oil"])
    assert out["hold"] == pytest.approx(0.8)


def test_short_hold_falls_on_flush():
    from qlab.diagnostics.flow import score_book

    prices = np.array([20.0, 19.0, 10.0, 10.0, 10.0, 10.0])
    weight = np.array([1.0, 1.0, 10.0, 10.0, 10.0, 10.0])
    out = score_book(
        direction=-1.0,
        close_i=10.0,
        atr_i=1.0,
        camp_high=20.0,
        camp_low=10.0,
        prices=prices,
        highs=prices,
        lows=prices,
        weight=weight,
        pts=[(0, 20.0), (5, 10.0)],
        origin_px=20.0,
    )
    assert out["squeeze"] == pytest.approx(1.0)
    assert out["oil"] == pytest.approx(2.0 / 42.0)
    assert out["hold"] == pytest.approx(out["oil"])


def test_short_hold_uses_last_completed_lower_high():
    from qlab.diagnostics.flow import score_book

    out = score_book(
        direction=-1.0,
        close_i=12.0,
        atr_i=1.0,
        camp_high=20.0,
        camp_low=8.0,
        prices=np.array([20.0, 10.0, 14.0, 8.0, 12.0]),
        highs=np.array([20.0, 10.0, 14.0, 8.0, 12.0]),
        lows=np.array([20.0, 10.0, 14.0, 8.0, 12.0]),
        weight=None,
        pts=[(0, 20.0), (1, 10.0), (2, 14.0), (3, 8.0), (4, 12.0)],
        origin_px=20.0,
    )
    assert out["squeeze"] == pytest.approx(1.0 - (12.0 - 8.0) / (14.0 - 8.0))
    assert out["hold"] == pytest.approx(out["squeeze"])


def test_short_hold_does_not_punish_deep_decline():
    from qlab.diagnostics.flow import score_book

    prices = np.linspace(20.0, 8.0, 13)
    out = score_book(
        direction=-1.0,
        close_i=8.0,
        atr_i=0.5,
        camp_high=20.0,
        camp_low=8.0,
        prices=prices,
        highs=prices,
        lows=prices,
        weight=np.ones(13),
        pts=[(0, 20.0), (12, 8.0)],
        origin_px=20.0,
    )
    assert np.isnan(out["extend"])
    assert out["squeeze"] == pytest.approx(1.0)
    assert out["hold"] > 0.7


def test_diagnose_flow_is_separate_from_trend():
    from qlab.diagnostics.flow import diagnose_flow, flow_panels
    from qlab.diagnostics.trend import diagnose_trend, trend_panels

    idx = pd.bdate_range("2020-01-01", periods=80)
    t = np.arange(80, dtype=float)
    close = 10.0 + 0.08 * t
    ohlcv = pd.DataFrame(
        {
            "close": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "volume": np.full(80, 1e6),
        },
        index=idx,
    )
    trend = diagnose_trend(ohlcv)
    flow = diagnose_flow(ohlcv)
    assert not hasattr(trend, "hold")
    assert not hasattr(trend, "tape")
    assert not hasattr(trend, "structure")
    assert "hold" not in trend.components
    assert "tape" not in trend.components
    assert "structure" not in trend.components
    assert "hold" in flow.components
    if trend.direction != 0:
        assert 0.0 <= flow.hold <= 1.0 or np.isnan(flow.hold)

    close_w = pd.DataFrame({"S": close}, index=idx)
    high_w = pd.DataFrame({"S": close + 0.2}, index=idx)
    low_w = pd.DataFrame({"S": close - 0.2}, index=idx)
    vol_w = pd.DataFrame({"S": np.full(80, 1e6)}, index=idx)
    assert "hold" not in trend_panels(close_w, high=high_w, low=low_w)
    assert "hold" in flow_panels(close_w, high=high_w, low=low_w, volume=vol_w)


