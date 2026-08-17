"""趋势诊断：方向 / 起点 / 效率 + PIT 枢轴确认."""

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
    if report.direction == 0:
        assert np.isnan(report.efficiency)
        assert np.isnan(report.overnight_efficiency)
        assert np.isnan(report.session_efficiency)
    else:
        assert 0.0 <= report.efficiency <= 1.0 or np.isnan(report.efficiency)
        assert -1.0 <= report.overnight_efficiency <= 1.0 or np.isnan(report.overnight_efficiency)
        assert -1.0 <= report.session_efficiency <= 1.0 or np.isnan(report.session_efficiency)
        assert "效率" in format_trend_summary(report)
        assert "隔夜" in format_trend_summary(report)
        assert "盘中" in format_trend_summary(report)
    assert report.summary
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
    assert report.direction in (-1, 0, 1)
    if report.direction == 0:
        assert np.isnan(report.efficiency)
        assert np.isnan(report.overnight_efficiency)
        assert np.isnan(report.session_efficiency)
        assert "区间" in report.summary
        assert "range_ok" in report.components


def test_trend_panels_shapes():
    n = 280
    idx = _dates(n)
    rng = np.random.default_rng(0)
    data = {}
    for sym, drift in [("A", 0.1), ("B", -0.05), ("C", 0.0)]:
        t = np.arange(n, dtype=float)
        data[sym] = 50 + drift * t + rng.normal(0, 0.5, size=n)
    close = pd.DataFrame(data, index=idx)
    panels = trend_panels(close, include_xs_rank=True)
    assert "direction" in panels and "efficiency" in panels and "origin" in panels
    assert "overnight_efficiency" in panels and "session_efficiency" in panels
    assert "path" not in panels and "structure" not in panels and "tape" not in panels
    assert "efficiency_xs" in panels
    for k, df in panels.items():
        assert df.shape == close.shape, k


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


def test_hysteresis_direction_requires_hold():
    from qlab.diagnostics.trend import hysteresis_direction

    idx = _dates(10)
    raw = pd.DataFrame(
        {"S": [1, 1, 1, -1, -1, -1, 1, 1, 1, 1]},
        index=idx,
        dtype=float,
    )
    out = hysteresis_direction(raw, hold=3)["S"]
    # 第 4 根开始连续 -1，需满 3 根才切换 → 索引 5 才变成 -1
    assert out.iloc[3] == 1.0
    assert out.iloc[5] == -1.0
    assert out.iloc[-1] == 1.0


def test_bear_grind_direction_not_mostly_bull():
    """长期阴跌序列：后半段方向不应以多头为主."""
    n = 420
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = 80.0 - 0.06 * t + 1.2 * np.sin(t / 9.0)
    ohlcv = pd.DataFrame(
        {
            "close": close,
            "high": close + 0.8,
            "low": close - 0.9,
        },
        index=idx,
    )
    panels = trend_panels(
        ohlcv[["close"]].rename(columns={"close": "S"}),
        high=ohlcv[["high"]].rename(columns={"high": "S"}),
        low=ohlcv[["low"]].rename(columns={"low": "S"}),
        include_xs_rank=False,
    )
    tail = panels["direction"]["S"].iloc[-120:]
    assert (tail > 0).mean() < 0.35


def test_direction_is_minus_one_zero_or_one():
    n = 300
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame({"S": 40 + 0.15 * t + np.sin(t / 8.0)}, index=idx)
    high = close + 1.0
    low = close - 1.0
    panels = trend_panels(close, high=high, low=low, include_xs_rank=False)
    assert set(np.unique(panels["direction"]["S"].dropna())) <= {-1.0, 0.0, 1.0}


def test_swing_age_counts_from_confirmation():
    L = 3
    n = 40
    idx = _dates(n)
    close = pd.Series(np.linspace(100.0, 110.0, n), index=idx)
    high = close + 0.3
    low = close - 0.3
    high.iloc[10] = 130.0
    st = structure_state_panels(
        close.to_frame("S"), high.to_frame("S"), low.to_frame("S"), left_right=L
    )
    # 确认日 = 10+L = 13，当天 age=0
    assert st["swing_age"]["S"].iloc[13] == pytest.approx(0.0)
    assert st["swing_age"]["S"].iloc[16] == pytest.approx(3.0)


def test_direction_panels_matches_trend_panels():
    """direction_panels 与全量 trend_panels 的 direction 一致."""
    from qlab.diagnostics.trend import direction_panels

    n = 80
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame({"S": 20 + 0.08 * t + 0.4 * np.sin(t / 6.0)}, index=idx)
    high = close + 0.2
    low = close - 0.2
    full = trend_panels(close, high=high, low=low, dir_hold=1, include_xs_rank=False)
    only = direction_panels(close, high=high, low=low, dir_hold=1)
    pd.testing.assert_series_equal(full["direction"]["S"], only["direction"]["S"])


def test_linear_uptrend_has_positive_direction():
    """直线上升：双 SuperTrend 同向为多."""
    n = 90
    idx = _dates(n)
    close = pd.DataFrame({"S": np.linspace(10.0, 20.0, n)}, index=idx)
    high = close + 0.1
    low = close - 0.1
    panels = trend_panels(
        close, high=high, low=low, dir_hold=1, include_xs_rank=False
    )
    assert (panels["direction"]["S"].iloc[-20:] == 1.0).mean() > 0.8


def test_choppy_window_direction_unknown():
    """重叠震荡：快慢轨分歧 → direction 以 0 为主."""
    n = 120
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame(
        {"S": 100.0 + 0.8 * np.sin(t) + 0.7 * np.sin(t * 2.3)},
        index=idx,
    )
    high = close + 0.4
    low = close - 0.4
    panels = trend_panels(close, high=high, low=low, dir_hold=1, include_xs_rank=False)
    assert (panels["direction"]["S"].iloc[-40:] == 0.0).mean() > 0.5


def test_live_direction_keeps_this_path_retention():
    """对外有方向时，闸只看这条路径自己还留着多少，不拿全局效率卡。"""
    n = 160
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.Series(30.0 + 0.8 * np.sin(t / 3.0), index=idx)
    ohlcv = pd.DataFrame(
        {"close": close, "high": close + 0.25, "low": close - 0.25},
        index=idx,
    )
    panels = trend_panels(
        ohlcv[["close"]].rename(columns={"close": "S"}),
        high=ohlcv[["high"]].rename(columns={"high": "S"}),
        low=ohlcv[["low"]].rename(columns={"low": "S"}),
        dir_hold=1,
        include_xs_rank=False,
    )
    live = panels["direction"]["S"].iloc[-80:] != 0
    if live.any():
        keep = panels["retention"]["S"].iloc[-80:][live]
        assert (keep >= 0.50).all()


def test_this_path_giveback_gates_direction():
    """这段自己吐回一半以上：即使 SuperTrend 还停在一边，对外方向回到 0。"""
    from qlab.diagnostics.trend.direction import gate_direction
    from qlab.diagnostics.trend import campaign_panels

    idx = _dates(20)
    close = pd.DataFrame(
        {"S": np.concatenate([np.linspace(10.0, 20.0, 12), np.linspace(20.0, 13.0, 8)])},
        index=idx,
    )
    high, low = close + 0.1, close - 0.1
    raw = pd.DataFrame({"S": 1.0}, index=idx)
    camp = campaign_panels(close, high, low, direction=raw)
    out = gate_direction(raw, camp["retention"], camp["origin_age"])
    assert camp["retention"]["S"].iloc[-1] < 0.50
    assert out["S"].iloc[-1] == 0.0


def test_this_path_grind_keeps_direction():
    """慢磨但贴着这段自己的极值：留存高，方向留下。"""
    from qlab.diagnostics.trend.direction import gate_direction
    from qlab.diagnostics.trend import campaign_panels

    idx = _dates(40)
    t = np.arange(40, dtype=float)
    close = pd.DataFrame({"S": 10.0 - 0.08 * t + 0.04 * np.sin(t)}, index=idx)
    high, low = close + 0.05, close - 0.05
    raw = pd.DataFrame({"S": -1.0}, index=idx)
    camp = campaign_panels(close, high, low, direction=raw)
    out = gate_direction(raw, camp["retention"], camp["origin_age"])
    assert camp["retention"]["S"].iloc[-1] >= 0.50
    assert out["S"].iloc[-1] == -1.0


def test_gate_direction_zeros_weak_retention():
    from qlab.diagnostics.trend.direction import gate_direction

    idx = _dates(8)
    raw = pd.DataFrame({"S": [1.0] * 8}, index=idx)
    age = pd.DataFrame({"S": np.arange(8, dtype=float)}, index=idx)
    keep = pd.DataFrame({"S": [0.9, 0.9, 0.9, 0.9, 0.9, 0.2, 0.2, 0.2]}, index=idx)
    out = gate_direction(raw, keep, age, thresh=0.50, min_age=5, hold=1)["S"]
    assert out.iloc[4] == 1.0
    assert out.iloc[5] == 0.0
    assert out.iloc[7] == 0.0


def test_retention_is_high_when_path_holds_its_extreme():
    from qlab.diagnostics.trend.score import _retention

    close = np.array([10.0, 12.0, 14.0, 13.5])
    assert _retention(close, 0, 3, 1.0, 10.0) > 0.8


def test_retention_drops_after_giving_back_the_path():
    from qlab.diagnostics.trend.score import _retention

    close = np.array([10.0, 14.0, 10.5])
    assert _retention(close, 0, 2, 1.0, 10.0) < 0.3


def test_supertrend_flips_after_close_through_band():
    from qlab.core.price_panels import supertrend_panels

    n = 40
    idx = _dates(n)
    close = pd.Series(np.linspace(10.0, 16.0, 25).tolist() + np.linspace(15.5, 8.0, 15).tolist(), index=idx)
    high = close + 0.15
    low = close - 0.15
    st = supertrend_panels(
        high.to_frame("S"), low.to_frame("S"), close.to_frame("S"),
        atr_window=10, multiplier=3.0,
    )
    d = st["direction"]["S"].dropna()
    assert (d.iloc[:10] > 0).mean() > 0.5 or (d == 1).any()
    assert (d.iloc[-5:] == -1.0).all()


def test_score_trend_panels_runs():
    from qlab.diagnostics.trend_eval import score_trend_panels

    n = 320
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.Series(50 + 0.1 * t + np.sin(t / 7.0), index=idx)
    m = score_trend_panels(close, close + 0.5, close - 0.5)
    assert "score" in m
    assert 0.0 <= m["score"] <= 1.0 or np.isnan(m["score"])


def test_bullish_fvg_detected_on_third_bar():
    from qlab.core.price_panels import fvg_state_panels

    idx = _dates(6)
    # c0,c1 flat; c2 displacement up leaving gap vs c0 high
    high = pd.Series([10.0, 10.2, 12.0, 12.1, 12.0, 11.9], index=idx)
    low = pd.Series([9.5, 9.6, 10.5, 11.0, 11.2, 11.0], index=idx)
    close = pd.Series([9.8, 10.0, 11.8, 11.5, 11.4, 11.3], index=idx)
    # At i=2: high[0]=10 < low[2]=10.5 → bullish FVG
    fvg = fvg_state_panels(high.to_frame("S"), low.to_frame("S"), close.to_frame("S"))
    assert fvg["imbalance"]["S"].iloc[2] == 1.0


def test_velocity_sign_follows_move():
    from qlab.core.price_panels import trend_velocity_panel

    idx = _dates(40)
    close = pd.Series(np.linspace(10, 20, 40), index=idx)
    vel = trend_velocity_panel(close.to_frame("S"))["S"]
    assert vel.iloc[-1] > 0


def test_efficiency_exists_on_a_downtrend():
    from qlab.diagnostics.trend import campaign_panels

    n = 280
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame({"S": 80.0 - 0.08 * t}, index=idx)
    high = close + 0.4
    low = close - 0.4
    out = campaign_panels(close, high=high, low=low)
    assert "efficiency" in out and "origin" in out
    assert "overlap" not in out and "path" not in out
    tail = out["efficiency"]["S"].iloc[-40:]
    assert tail.notna().any()
    assert ((tail.dropna() >= 0.0) & (tail.dropna() <= 1.0)).all()


def test_efficiency_is_high_on_a_one_way_move():
    from qlab.diagnostics.trend.score import _efficiency

    close = np.linspace(10.0, 20.0, 12)
    assert _efficiency(close, 0, 11, 1.0) > 0.95
    assert _efficiency(close, 0, 11, -1.0) < 0.05


def test_efficiency_drops_after_a_full_retrace():
    from qlab.diagnostics.trend.score import _efficiency

    up = np.linspace(10.0, 20.0, 8)
    down = np.linspace(20.0, 10.2, 8)[1:]
    close = np.concatenate([up, down])
    assert _efficiency(close, 0, len(close) - 1, 1.0) < 0.25


def test_efficiency_is_low_when_price_chops():
    from qlab.diagnostics.trend.score import _efficiency

    close = np.array([10.0, 11.0, 10.0, 11.0, 10.0, 11.0, 10.2])
    assert _efficiency(close, 0, len(close) - 1, 1.0) < 0.25


def test_recent_impulse_lifts_efficiency():
    from qlab.diagnostics.trend.score import _efficiency

    grind = np.array([10.0, 10.2, 10.0, 10.2, 10.0, 10.2, 10.0, 10.2])
    rocket = np.array([11.0, 12.5, 14.0, 15.5])
    close = np.concatenate([grind, rocket])
    assert _efficiency(close, 0, len(close) - 1, 1.0) > _efficiency(grind, 0, len(grind) - 1, 1.0) + 0.2


def test_overnight_efficiency_is_negative_when_gaps_fight_the_trend():
    from qlab.diagnostics.trend.score import _overnight_efficiency

    close = np.array([10.0, 11.0, 12.0, 13.0, 14.0])
    open_ = np.array([10.0, 10.2, 10.4, 10.6, 10.8])
    assert _overnight_efficiency(open_, close, 0, 4, 1.0) < 0.0


def test_session_efficiency_is_negative_when_the_day_fades():
    from qlab.diagnostics.trend.score import _session_efficiency

    close = np.array([10.0, 10.2, 10.3, 10.1, 10.0])
    open_ = np.array([10.0, 11.0, 11.2, 11.0, 10.8])
    assert _session_efficiency(open_, close, 0, 4, 1.0) < 0.0


def test_close_efficiency_stays_non_negative():
    from qlab.diagnostics.trend.score import _efficiency

    close = np.array([10.0, 9.0, 8.0, 7.0])
    assert _efficiency(close, 0, 3, 1.0) == 0.0


def test_daily_direction_flip_starts_a_new_leg():
    """日线方向翻了就是新的一段：起点换。"""
    from qlab.diagnostics.trend import campaign_panels, direction_panels

    n = 260
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close_arr = 50.0 + 0.12 * t
    high_arr = close_arr * 1.012
    low_arr = close_arr * 0.988
    peak = close_arr[159]
    close_arr[160:200] = peak * np.linspace(1.0, 0.82, 40)
    high_arr = np.maximum(high_arr, close_arr * 1.008)
    low_arr = np.minimum(low_arr, close_arr * 0.992)
    low_arr[160:200] = close_arr[160:200] * 0.99

    close = pd.DataFrame({"S": close_arr}, index=idx)
    high = pd.DataFrame({"S": high_arr}, index=idx)
    low = pd.DataFrame({"S": low_arr}, index=idx)

    d = direction_panels(close, high, low, dir_hold=1)["direction"]["S"]
    camp = campaign_panels(close, high, low, direction=d.to_frame("S"))
    assert (d.iloc[160:200] < 0).any(), "深回撤应让日线方向出现空头"
    flipped = d < 0
    if flipped.any():
        first = flipped.idxmax()
        assert camp["origin"]["S"].loc[first] != camp["origin"]["S"].iloc[155]


def test_bull_trend_mask_runs():
    from qlab.labeling.sample_masks import bull_trend_mask

    n = 280
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame(
        {"A": 40 + 0.2 * t, "B": 80 - 0.05 * t},
        index=idx,
    )
    high = close + 1.0
    low = close - 1.0
    m2 = bull_trend_mask(close, high, low)
    assert m2.dtype == bool
    assert m2.index.names == ["date", "symbol"] or set(m2.index.names) >= {"date", "symbol"}


def test_efficiency_is_nan_without_a_campaign():
    from qlab.diagnostics.trend import campaign_panels

    n = 80
    idx = _dates(n)
    close = pd.DataFrame({"S": 100.0 + 1.5 * np.sin(np.arange(n) / 3.0)}, index=idx)
    high, low = close + 0.4, close - 0.4
    camp = campaign_panels(close, high, low)
    origin = camp["origin"]["S"]
    assert camp["efficiency"]["S"][origin.isna()].isna().all()


def test_one_sided_uptrend_has_efficiency():
    from qlab.diagnostics.trend import campaign_panels, direction_panels

    n = 260
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame({"S": 40.0 + 0.15 * t + 0.9 * np.sin(t / 8.0)}, index=idx)
    high, low = close * 1.01, close * 0.99
    d = direction_panels(close, high, low)["direction"]["S"]
    efficiency = campaign_panels(close, high, low)["efficiency"]["S"]
    trending = d != 0
    assert trending.iloc[-40:].any()
    assert 0.0 <= float(efficiency.iloc[-1]) <= 1.0


def test_range_panels_nan_when_trending():
    from qlab.diagnostics.trend import range_panels, trend_panels

    n = 180
    idx = _dates(n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame({"S": 40.0 + 0.15 * t}, index=idx)
    high, low = close * 1.01, close * 0.99
    full = trend_panels(close, high, low, include_xs_rank=False)
    trending = full["direction"]["S"] != 0
    if trending.any():
        assert full["range_ok"]["S"][trending].isna().all()
        assert full["efficiency"]["S"][trending].notna().any()


def test_range_box_on_sine_is_usable():
    """来回振荡应走出箱子，趋势侧不评价。"""
    from qlab.diagnostics.trend import diagnose_trend, range_panels

    n = 200
    idx = _dates(n)
    close = 100.0 + 3.0 * np.sin(np.arange(n) / 6.0)
    ohlcv = pd.DataFrame(
        {"close": close, "high": close + 0.4, "low": close - 0.4, "volume": 1e6},
        index=idx,
    )
    wide_c = ohlcv[["close"]].rename(columns={"close": "S"})
    wide_h = ohlcv[["high"]].rename(columns={"high": "S"})
    wide_l = ohlcv[["low"]].rename(columns={"low": "S"})
    rng = range_panels(wide_c, wide_h, wide_l)
    # 强制方向全 0 时，后半段应能评出箱子
    from qlab.diagnostics.trend.range import range_one
    from qlab.core.price_panels import atr_panel

    atr = atr_panel(wide_h, wide_l, wide_c, window=14)["S"].to_numpy()
    d0 = np.zeros(n)
    one = range_one(
        wide_h["S"].to_numpy(),
        wide_l["S"].to_numpy(),
        wide_c["S"].to_numpy(),
        d0,
        atr,
    )
    assert np.nanmean(one["range_ok"][-40:]) > 0.3
    report = diagnose_trend(ohlcv)
    if report.direction == 0:
        assert np.isnan(report.efficiency)
        assert np.isnan(report.overnight_efficiency)
        assert np.isnan(report.session_efficiency)
        assert "区间" in report.summary


def test_tv_pivot_needs_two_bars_each_side():
    """TradingView 2/2：枢轴左右各 2 根，确认日 = 枢轴 + 2。"""
    from qlab.diagnostics.trend.origin import tv_pivots

    n = 20
    high = np.full(n, 10.0)
    low = np.full(n, 9.0)
    high[8] = 15.0
    pivots = tv_pivots(high, low, left_right=2, asof=9)
    assert pivots == []
    pivots = tv_pivots(high, low, left_right=2, asof=10)
    assert (8, 15.0, "H") in pivots


def test_zigzag_leg_needs_time_or_atr():
    """短而小的反向点丢掉；短但走出约 2ATR，或走满约 5 根，都留。"""
    from qlab.diagnostics.trend.origin import zigzag

    atr = np.full(20, 1.0)
    tiny = zigzag([(0, 10.0, "L"), (1, 10.4, "H"), (10, 13.0, "H")], atr=atr)
    assert (1, 10.4, "H") not in tiny
    assert tiny == [(0, 10.0, "L"), (10, 13.0, "H")]
    sharp = zigzag([(0, 10.0, "L"), (1, 12.0, "H"), (10, 8.0, "L")], atr=atr)
    assert sharp == [(0, 10.0, "L"), (1, 12.0, "H"), (10, 8.0, "L")]
    slow = zigzag([(0, 10.0, "L"), (5, 10.3, "H")], atr=atr)
    assert slow == [(0, 10.0, "L"), (5, 10.3, "H")]


def test_bear_origin_is_lh_peak_not_late_bounce():
    """急拉后崩、方向晚才翻空：起点应是孤立最高点，不是翻向窗口里的反弹高。"""
    from qlab.diagnostics.trend import campaign_panels, direction_panels

    n = 220
    idx = _dates(n)
    close = np.concatenate(
        [
            np.linspace(10.0, 11.0, 140),
            np.linspace(11.0, 22.0, 12),
            np.linspace(22.0, 14.0, 18),
            np.linspace(14.0, 17.0, 10),
            np.linspace(17.0, 13.0, 40),
        ]
    )
    high = close * 1.01
    low = close * 0.99
    peak_i = 140 + 11
    high[peak_i] = 22.8
    close_df = pd.DataFrame({"S": close}, index=idx)
    high_df = pd.DataFrame({"S": high}, index=idx)
    low_df = pd.DataFrame({"S": low}, index=idx)
    d = direction_panels(close_df, high_df, low_df)["direction"]["S"]
    camp = campaign_panels(close_df, high_df, low_df, direction=d.to_frame("S"))
    bear = d < 0
    assert bear.iloc[-30:].any()
    last_bear = bear[bear].index[-1]
    origin = float(camp["origin"]["S"].loc[last_bear])
    assert origin > 20.0
    assert abs(origin - 22.8) < 0.5


def test_bull_origin_is_hl_trough_not_earlier_higher_low():
    """更早的更高低点不是这段起点；HL 从更近的谷算起。"""
    from qlab.diagnostics.trend.origin import structure_origin, zigzag

    zig = zigzag(
        [
            (10, 12.0, "L"),
            (20, 16.0, "H"),
            (30, 9.0, "L"),
            (40, 18.0, "H"),
            (50, 11.0, "L"),
            (60, 20.0, "H"),
        ],
    )
    found = structure_origin(zig, 1.0)
    assert found is not None
    assert found[0] == 30
    assert found[1] == 9.0
    found_bear = structure_origin(
        zigzag(
            [(10, 22.0, "H"), (20, 14.0, "L"), (30, 18.0, "H"), (40, 12.0, "L"), (50, 15.0, "H")],
        ),
        -1.0,
    )
    assert found_bear is not None
    assert found_bear[0] == 10
    assert found_bear[1] == 22.0
