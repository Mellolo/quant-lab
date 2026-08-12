"""趋势诊断：合成路径 + PIT 枢轴确认."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qlab.core.price_panels import (
    _confirmed_swing_series,
    structure_state_panels,
    swing_pivot_panels,
)
from qlab.diagnostics.trend import (
    PHASE_CODE,
    diagnose_trend,
    format_trend_summary,
    trend_panels,
)


def _dates(n: int, start: str = "2020-01-01") -> pd.DatetimeIndex:
    return pd.bdate_range(start, periods=n)


def test_swing_confirmed_only_after_right_bars():
    """枢轴在右端 L 根之后才在确认日出现（PIT）."""
    L = 3
    n = 40
    idx = _dates(n)
    # 在 i=10 造一个明显高点
    close = pd.Series(100.0, index=idx)
    high = close.copy()
    low = close - 1.0
    high.iloc[10] = 120.0
    # 确认日应为 10+L=13
    sh = _confirmed_swing_series(high, left_right=L, mode="high")
    assert np.isnan(sh.iloc[10])
    assert np.isnan(sh.iloc[12])
    assert sh.iloc[13] == pytest.approx(120.0)


def test_uptrend_structure_direction_positive():
    """稳步抬高的序列 → slow/fast 方向偏多."""
    n = 300
    idx = _dates(n)
    # 带噪声的上升 + 周期性小回撤
    t = np.arange(n, dtype=float)
    wave = 3.0 * np.sin(t / 8.0)
    close = pd.Series(50.0 + 0.15 * t + wave, index=idx)
    high = close + 0.5
    low = close - 0.8
    # 在回撤谷底压低 low，峰顶抬高 high，便于形成 HL/HH
    for i in range(8, n, 16):
        low.iloc[i] = close.iloc[i] - 2.0
    for i in range(0, n, 16):
        high.iloc[i] = close.iloc[i] + 2.0

    wide_c = close.to_frame("S")
    wide_h = high.to_frame("S")
    wide_l = low.to_frame("S")
    st = structure_state_panels(wide_c, wide_h, wide_l, left_right=3)
    # 后半段应稳定为多头
    tail_dir = st["direction"]["S"].iloc[-60:]
    assert (tail_dir == 1).mean() > 0.5


def test_diagnose_trend_uptrend_report():
    n = 320
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = 40.0 + 0.2 * t + 2.0 * np.sin(t / 10.0)
    ohlcv = pd.DataFrame(
        {
            "close": close,
            "high": close + 1.0,
            "low": close - 1.2,
            "volume": np.full(n, 1e6),
        },
        index=idx,
    )
    # 制造更清晰的摆动
    for i in range(5, n, 20):
        ohlcv.loc[idx[i], "low"] = ohlcv.loc[idx[i], "close"] - 3.0
    for i in range(15, n, 20):
        ohlcv.loc[idx[i], "high"] = ohlcv.loc[idx[i], "close"] + 3.0

    report = diagnose_trend(ohlcv)
    assert report.asof == idx[-1]
    assert report.direction in (-1, 0, 1)
    assert report.phase in PHASE_CODE
    assert 0.0 <= report.strength <= 1.0
    assert 0.0 <= report.quality <= 1.0
    assert 0.0 <= report.risk <= 1.0
    assert report.summary
    assert "强度" in format_trend_summary(report)
    # 长上升趋势不应判成稳定空头
    assert report.direction >= 0


def test_diagnose_trend_range_like():
    n = 260
    idx = _dates(n)
    close = 100.0 + 2.0 * np.sin(np.arange(n) / 5.0)
    ohlcv = pd.DataFrame(
        {"close": close, "high": close + 0.5, "low": close - 0.5},
        index=idx,
    )
    report = diagnose_trend(ohlcv)
    # 震荡更可能 range / 低强度，或 direction=0
    assert report.phase in PHASE_CODE
    assert report.direction in (-1, 0, 1)


def test_trend_panels_shapes_and_phase_codes():
    n = 280
    idx = _dates(n)
    rng = np.random.default_rng(0)
    data = {}
    for sym, drift in [("A", 0.1), ("B", -0.05), ("C", 0.0)]:
        t = np.arange(n, dtype=float)
        data[sym] = 50 + drift * t + rng.normal(0, 0.5, size=n)
    close = pd.DataFrame(data, index=idx)
    panels = trend_panels(close, include_xs_rank=True)
    assert "direction" in panels and "strength_xs" in panels
    for k, df in panels.items():
        assert df.shape == close.shape, k
    codes = set(np.unique(panels["phase_code"].to_numpy()))
    assert codes <= set(PHASE_CODE.values()) | {np.nan}


def test_swing_pivot_panels_alignment():
    idx = _dates(30)
    high = pd.DataFrame({"A": np.linspace(1, 2, 30), "B": np.linspace(2, 1, 30)}, index=idx)
    high.iloc[10, 0] = 5.0
    low = high - 0.1
    sh, sl = swing_pivot_panels(high, low, left_right=3)
    assert sh.shape == high.shape
    assert sl.shape == low.shape
    # A 的高峰应在确认日 13 出现
    assert sh["A"].iloc[13] == pytest.approx(5.0)


def test_bull_trend_mask_runs():
    from qlab.labeling.sample_masks import bull_trend_mask, trend_phase_mask

    n = 280
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame(
        {"A": 40 + 0.2 * t, "B": 80 - 0.05 * t},
        index=idx,
    )
    high = close + 1.0
    low = close - 1.0
    m1 = trend_phase_mask(close, high, low, phases=["early", "mid", "late", "range"])
    m2 = bull_trend_mask(close, high, low, min_strength=0.0, max_risk=1.0)
    assert m1.dtype == bool
    assert m2.dtype == bool
    assert m1.index.names == ["date", "symbol"] or set(m1.index.names) >= {"date", "symbol"}
