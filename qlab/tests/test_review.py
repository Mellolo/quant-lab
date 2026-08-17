"""复盘 ①市场 + ②风格。"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qlab.core.schema import (
    SCHEMA_REVIEW_MARKET,
    SCHEMA_REVIEW_STYLE,
    validate_schema,
)
from qlab.review.market import classify_emotion, classify_volume, market_from_daily
from qlab.review.panels import score_spread
from qlab.review.spec import ReviewSpec
from qlab.review.style import style_from_daily


def test_volume_bins_and_climax_fade():
    spec = ReviewSpec()
    days = pd.bdate_range("2023-01-02", periods=30)
    amt = pd.Series(100.0, index=days)
    amt.iloc[25] = 250.0
    amt.iloc[26] = 80.0
    bins = classify_volume(amt, spec)
    assert pd.isna(bins.iloc[0])
    assert bins.iloc[24] == "正常"
    assert bins.iloc[25] == "天量"
    assert bins.iloc[26] == "天量后缩"

    dry = amt.copy()
    dry.iloc[25] = 50.0
    assert classify_volume(dry, spec).iloc[25] == "地量"
    hot = amt.copy()
    hot.iloc[25] = 150.0
    assert classify_volume(hot, spec).iloc[25] == "放量"


def test_emotion_tree():
    spec = ReviewSpec()
    assert classify_emotion(
        amount_ratio=1.6, market_ret=0.001, advance_ratio=0.55,
        n_limit_up=3, n_limit_down=2, attack_defense=0.0,
        volume_bin="放量", above_ma20=True, trend_proxy=0.01, spec=spec,
    ) == "分配"
    assert classify_emotion(
        amount_ratio=1.0, market_ret=-0.02, advance_ratio=0.2,
        n_limit_up=1, n_limit_down=12, attack_defense=-0.01,
        volume_bin="放量", above_ma20=False, trend_proxy=-0.01, spec=spec,
    ) == "冰点/恐慌"
    assert classify_emotion(
        amount_ratio=1.4, market_ret=0.02, advance_ratio=0.75,
        n_limit_up=20, n_limit_down=1, attack_defense=0.01,
        volume_bin="放量", above_ma20=True, trend_proxy=0.01, spec=spec,
    ) == "狂热"
    assert classify_emotion(
        amount_ratio=1.1, market_ret=0.01, advance_ratio=0.65,
        n_limit_up=8, n_limit_down=2, attack_defense=0.0,
        volume_bin="正常", above_ma20=True, trend_proxy=0.01, spec=spec,
    ) == "乐观"
    assert classify_emotion(
        amount_ratio=1.0, market_ret=0.002, advance_ratio=0.55,
        n_limit_up=2, n_limit_down=2, attack_defense=0.0,
        volume_bin="正常", above_ma20=True, trend_proxy=0.01, spec=spec,
    ) == "认知"
    assert classify_emotion(
        amount_ratio=0.8, market_ret=0.005, advance_ratio=0.48,
        n_limit_up=1, n_limit_down=1, attack_defense=0.0,
        volume_bin="地量", above_ma20=False, trend_proxy=0.0, spec=spec,
    ) == "修复"
    # 深 V：量只是正常、涨跌比未到 0.60，仍应是乐观不是修复
    assert classify_emotion(
        amount_ratio=0.96, market_ret=0.025, advance_ratio=0.57,
        n_limit_up=125, n_limit_down=24, attack_defense=0.02,
        volume_bin="正常", above_ma20=False, trend_proxy=-0.08, spec=spec,
        median_ret=0.006, n_eligible=5000,
    ) == "乐观"
    assert classify_emotion(
        amount_ratio=1.0, market_ret=0.0, advance_ratio=0.5,
        n_limit_up=3, n_limit_down=3, attack_defense=0.0,
        volume_bin="正常", above_ma20=False, trend_proxy=0.0, spec=spec,
    ) == "中性/撕裂"
    # 指数红、中位数不红：不写成乐观
    assert classify_emotion(
        amount_ratio=1.1, market_ret=0.012, advance_ratio=0.65,
        n_limit_up=8, n_limit_down=2, attack_defense=0.0,
        volume_bin="正常", above_ma20=True, trend_proxy=0.01, spec=spec,
        median_ret=-0.002,
    ) == "认知"
    # 结算已宽、量未确认：乐观，不升狂热，也不因缩量写成修复
    assert classify_emotion(
        amount_ratio=1.0, market_ret=0.02, advance_ratio=0.75,
        n_limit_up=20, n_limit_down=1, attack_defense=0.01,
        volume_bin="正常", above_ma20=False, trend_proxy=0.0, spec=spec,
        n_eligible=5000,
    ) == "乐观"
    # 地量不否掉结算宽
    assert classify_emotion(
        amount_ratio=0.65, market_ret=0.01, advance_ratio=0.65,
        n_limit_up=8, n_limit_down=2, attack_defense=0.0,
        volume_bin="地量", above_ma20=False, trend_proxy=0.0, spec=spec,
    ) == "乐观"
    # 放量也不把轻度修复挡成别的档
    assert classify_emotion(
        amount_ratio=1.35, market_ret=0.005, advance_ratio=0.48,
        n_limit_up=2, n_limit_down=2, attack_defense=0.0,
        volume_bin="放量", above_ma20=False, trend_proxy=0.0, spec=spec,
    ) == "修复"


def test_score_spread_thresholds():
    spec = ReviewSpec()
    assert score_spread(0.001, spec) == 0
    assert score_spread(0.004, spec) == 1
    assert score_spread(0.01, spec) == 2
    assert score_spread(-0.01, spec) == -2
    assert score_spread(float("nan"), spec) == 0


def _synth_daily(
    dates: pd.DatetimeIndex,
    n: int,
    close: np.ndarray,
    *,
    float_shares: np.ndarray | None = None,
    amount: np.ndarray | None = None,
    limit_up: np.ndarray | None = None,
    limit_down: np.ndarray | None = None,
) -> pd.DataFrame:
    """close / shares / amount 都是 (n_dates, n_symbols)。"""
    symbols = [f"{i:06d}.SZ" for i in range(n)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["date", "symbol"])
    close = np.asarray(close, dtype=float)
    if float_shares is None:
        float_shares = np.full(close.shape, 1.0e8)
    if amount is None:
        amount = np.full(close.shape, 1.0e8)
    if limit_up is None:
        limit_up = np.zeros(close.shape, dtype=bool)
    if limit_down is None:
        limit_down = np.zeros(close.shape, dtype=bool)
    n_rows = len(dates) * n
    df = pd.DataFrame({
        "open": close.ravel(),
        "high": close.ravel(),
        "low": close.ravel(),
        "close": close.ravel(),
        "vwap": close.ravel(),
        "open_raw": close.ravel(),
        "high_raw": close.ravel(),
        "low_raw": close.ravel(),
        "close_raw": close.ravel(),
        "volume": np.full(n_rows, 1_000_000, dtype=np.int64),
        "amount": amount.ravel(),
        "adj_factor": np.ones(n_rows),
        "limit_up_price": close.ravel(),
        "limit_down_price": close.ravel(),
        "is_suspended": np.zeros(n_rows, dtype=bool),
        "is_limit_up": limit_up.ravel(),
        "is_limit_down": limit_down.ravel(),
        "is_st": np.zeros(n_rows, dtype=bool),
        "days_since_listing": np.full(n_rows, 1000, dtype=np.int32),
        "total_shares": float_shares.ravel().astype(np.int64),
        "float_shares": float_shares.ravel().astype(np.int64),
        "free_float_shares": float_shares.ravel().astype(np.int64),
    }, index=idx)
    return df


def test_size_axis_small_beats_large():
    spec = ReviewSpec(min_names=4, quintile=0.25)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    close[-1, :10] = 10.4
    close[-1, 10:] = 9.6
    shares = np.ones((t, n))
    shares[:, :10] = 1.0e8
    shares[:, 10:] = 1.0e10
    daily = _synth_daily(dates, n, close, float_shares=shares)
    out = style_from_daily(daily, dates[-1:], spec)
    assert out.loc[dates[-1], "size_score"] == 2
    assert out.loc[dates[-1], "size_s"] == pytest.approx(0.08, abs=1e-6)


def test_size_groups_use_t_minus_1_mcap():
    """T 日暴涨把小票变成大票，分组仍按 T-1 市值。"""
    spec = ReviewSpec(min_names=4, quintile=0.25)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    shares = np.ones((t, n))
    shares[:, 0] = 1.0e8
    shares[:, 1:5] = 1.1e8
    shares[:, 5:15] = 1.5e8
    shares[:, 15:] = 2.0e8
    # T-1 市值：0 最小；T 日 0 涨到 40，市值超过大盘组。分组必须仍用 T-1
    close[-1, 0] = 40.0
    daily = _synth_daily(dates, n, close, float_shares=shares)
    out = style_from_daily(daily, dates[-1:], spec)
    assert out.loc[dates[-1], "size_s"] > 0
    assert out.loc[dates[-1], "size_score"] >= 1


def test_trend_axis_winners_keep_winning():
    spec = ReviewSpec(min_names=4, quintile=0.25, mom_window=20)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    # [T-20, T-1]：0-9 每天 +1%，10-19 每天 -1%
    for i in range(t - 21, t - 1):
        close[i + 1, :10] = close[i, :10] * 1.01
        close[i + 1, 10:] = close[i, 10:] * 0.99
    close[-1, :10] = close[-2, :10] * 1.02
    close[-1, 10:] = close[-2, 10:] * 0.98
    daily = _synth_daily(dates, n, close)
    out = style_from_daily(daily, dates[-1:], spec)
    assert out.loc[dates[-1], "trend_score"] == 2
    assert out.loc[dates[-1], "trend_s"] == pytest.approx(0.04, abs=1e-6)


def test_growth_pe_then_industry_fallback():
    spec = ReviewSpec(min_names=4, quintile=0.25)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    close[-1, :10] = 10.3
    close[-1, 10:] = 9.7
    shares = np.full((t, n), 1.0e8)
    daily = _synth_daily(dates, n, close, float_shares=shares)
    symbols = [f"{i:06d}.SZ" for i in range(n)]
    # 高 PE = 利润小；0-9 贵且今日更强 → 要成长
    profit = pd.DataFrame(1.0e8, index=dates, columns=symbols)
    profit.iloc[:, :10] = 1.0e6
    profit.iloc[:, 10:] = 1.0e9
    out = style_from_daily(daily, dates[-1:], spec, pe_profit=profit)
    assert out.loc[dates[-1], "growth_source"] == "pe"
    assert out.loc[dates[-1], "growth_score"] == 2

    industry = pd.DataFrame(index=dates, columns=symbols, dtype=object)
    industry.iloc[:, :10] = "801080"
    industry.iloc[:, 10:] = "801780"
    out2 = style_from_daily(daily, dates[-1:], spec, industry=industry)
    assert out2.loc[dates[-1], "growth_source"] == "industry"
    assert out2.loc[dates[-1], "growth_score"] == 2


def test_market_volume_and_stall_from_daily():
    spec = ReviewSpec()
    dates = pd.bdate_range("2023-01-02", periods=40)
    n, t = 12, len(dates)
    close = np.ones((t, n)) * 10.0
    amount = np.full((t, n), 1.0e8)
    amount[-1, :] = 1.6e8
    daily = _synth_daily(dates, n, close, amount=amount)
    out = market_from_daily(daily, dates[-1:], spec)
    validate_schema(out, SCHEMA_REVIEW_MARKET)
    row = out.loc[dates[-1]]
    assert row["volume_bin"] == "放量"
    assert row["emotion_bin"] == "分配"
    assert row["n_up"] == 0 and row["n_down"] == 0
    assert row["n_tradable"] == n
    assert row["n_active"] == n
    assert row["crowding_bin"] == "拥挤待切"
    assert row["turnover"] > 0
    assert row["active_share"] == pytest.approx(1.0, abs=1e-3)
    # 价没动；放量只把活筹比例顶满，0AMV 涨跌应接近 0
    assert abs(row["live_ret"]) < 0.02
    assert abs(row["live_gap"]) < 0.02


def test_run_fake_source_schema():
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    from qlab.review import run

    data = DataLayer(source=FakeDataSource(seed=1, n_symbols=30, start_year=2022))
    result = run(data, "2023-06-01", "2023-06-10", universe="hs_a")
    assert not result.market.empty
    assert len(result.market) == len(result.style)
    validate_schema(result.market, SCHEMA_REVIEW_MARKET)
    validate_schema(result.style, SCHEMA_REVIEW_STYLE)
    text = result.summary()
    assert "量：" in text and "风格：" in text
    assert "栖息" in text and "五日" in text
    assert result.style["size_score"].isin({-2, -1, 0, 1, 2}).all()
    assert result.style["liq_score"].isin({-2, -1, 0, 1, 2}).all()
    assert result.style["habitat"].isin({"权重", "弹性", "混合"}).all()
    assert result.style["lu_yest_score"].isin({-2, -1, 0, 1, 2}).all()
    assert "集中：" in text
    assert "短线：" in text


def test_liquidity_low_turnover_wins():
    spec = ReviewSpec(min_names=4, quintile=0.25)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    close[-1, :10] = 10.3
    close[-1, 10:] = 9.7
    amount = np.full((t, n), 1.0e8)
    amount[:, :10] = 2.0e7
    amount[:, 10:] = 5.0e8
    daily = _synth_daily(dates, n, close, amount=amount)
    out = style_from_daily(daily, dates[-1:], spec)
    assert out.loc[dates[-1], "liq_score"] == 2


def test_amv_follows_active_chips_not_dead_float():
    """0AMV：高换手票涨、低换手票跌 → 活跃市值涨、相对 0 号为正。"""
    spec = ReviewSpec()
    dates = pd.bdate_range("2023-01-02", periods=40)
    n, t = 10, len(dates)
    close = np.ones((t, n)) * 10.0
    close[-1, :5] = 10.5
    close[-1, 5:] = 9.5
    amount = np.full((t, n), 1.0e8)
    amount[:, :5] = 5.0e8
    amount[:, 5:] = 2.0e6
    daily = _synth_daily(dates, n, close, amount=amount)
    out = market_from_daily(daily, dates[-1:], spec)
    row = out.loc[dates[-1]]
    assert row["live_ret"] > 0
    assert row["live_gap"] > 0
    assert row["active_share"] < 0.80
    assert row["n_active"] == 5


def test_chip_active_frac_soft_in_turnover():
    from qlab.review.panels import chip_active_frac

    spec = ReviewSpec(active_tau=0.02)
    to = pd.DataFrame([[0.0, 0.02, 1.0]], columns=list("abc"))
    frac = chip_active_frac(to, spec).iloc[0]
    assert frac["a"] == pytest.approx(0.0)
    assert frac["b"] == pytest.approx(1.0 - np.exp(-1.0))
    assert frac["c"] > 0.99


def test_score_z_uses_sigma_not_headcount():
    from qlab.review.panels import score_z

    spec = ReviewSpec()
    assert score_z(0.2, spec) == 0
    assert score_z(0.8, spec) == 1
    assert score_z(1.6, spec) == 2
    assert score_z(-1.6, spec) == -2
    assert score_z(float("nan"), spec) == 0


def test_short_style_limit_continuation():
    """昨涨停今日、晋级、二板：相对量，不写死 80 家 / 29%。"""
    spec = ReviewSpec(min_names=4)
    dates = pd.bdate_range("2023-01-02", periods=70)
    n, t = 20, len(dates)
    close = np.ones((t, n)) * 10.0
    close[-1, :8] = 10.4
    close[-1, 8:] = 10.0
    lu = np.zeros((t, n), dtype=bool)
    lu[-2, :8] = True
    lu[-1, :6] = True
    daily = _synth_daily(dates, n, close, limit_up=lu)
    out = style_from_daily(daily, dates[-1:], spec)
    row = out.loc[dates[-1]]
    validate_schema(out, SCHEMA_REVIEW_STYLE)
    assert row["n_lu_yest"] == 8
    assert row["lu_yest_ret"] == pytest.approx(0.04, abs=1e-6)
    assert row["lu_yest_s"] > 0
    assert row["lu_yest_score"] == 2
    assert row["lu_promote"] == pytest.approx(0.75, abs=1e-6)
    assert row["n_board2"] == 6
    assert row["board2_share"] == pytest.approx(1.0, abs=1e-6)
    assert row["max_board"] == 2


def test_habitat_needs_m5_and_clock_same_way():
    from qlab.review.style import _habitat

    spec = ReviewSpec()
    assert _habitat(0.02, -0.01, "短博弈", spec) == "弹性"
    assert _habitat(-0.02, 0.01, "中线", spec) == "权重"
    assert _habitat(0.02, 0.01, "混合", spec) == "混合"
    assert _habitat(0.001, -0.001, "混合", spec) == "混合"


def test_classify_crowding_needs_stall_for_cut():
    from qlab.review.panels import classify_crowding

    spec = ReviewSpec()
    assert classify_crowding(0.70, 0.55, 0.02, 1.6, 0.001, spec) == "拥挤待切"
    assert classify_crowding(0.62, 0.55, 0.02, 1.0, 0.01, spec) == "聚集"
    assert classify_crowding(0.50, 0.58, -0.02, 1.0, 0.0, spec) == "分散"
    assert classify_crowding(0.56, 0.55, 0.005, 1.0, 0.0, spec) == "正常"
