"""采样合约单元测试：确认日 → 入场 + 经典三重屏障出场."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qlab.core.calendar import get_default_calendar
from qlab.core.enums import EntryAt, EntryTiming
from qlab.labeling import (
    EXIT_RESEARCH_DEFAULT,
    EXIT_TB_1_5_1_V20,
    CUSUMFilter,
    ExitSettings,
    NewHighBreakoutSampler,
    SampleSpec,
    VolumeCUSUMFilter,
    confirmation_to_entry,
    daily_event_pairs,
    filter_pairs,
    liquidity_top_n_mask,
    to_event_dataframe,
)


class _GridEntry:
    def __init__(self, symbols, dates):
        self._pairs = daily_event_pairs(symbols, dates)

    def sample_per_symbol(self, prices):
        return self._pairs.copy()


def _synth_ohlc(n: int = 40, symbols: list[str] | None = None):
    symbols = symbols or ["AAA.SZ"]
    days = pd.bdate_range("2024-01-02", periods=n)
    close = pd.DataFrame({s: 100.0 + i for i, s in enumerate(symbols)}, index=days)
    open_ = close * 0.99
    return days, open_, close


def test_confirmation_to_entry_maps_each_day_to_next_session():
    cal = get_default_calendar()
    days = pd.bdate_range("2024-01-02", periods=5)
    pairs = daily_event_pairs(["A.SZ"], days[:3])
    out = confirmation_to_entry(pairs, cal)
    expect = [cal.next_trading_day(pd.Timestamp(d), 1) for d in days[:3]]
    assert list(pd.DatetimeIndex(out["timestamp"]).normalize()) == [
        pd.Timestamp(t).normalize() for t in expect
    ]


def test_sample_spec_default_is_next_open():
    days, open_, close = _synth_ohlc()
    spec = SampleSpec(entry=_GridEntry(["AAA.SZ"], days[:3]), exit=EXIT_TB_1_5_1_V20)
    assert spec._entry_at() == EntryAt.NEXT_OPEN
    confirm = spec.confirmation_pairs(close)
    entry = spec.sample_pairs(close)
    cal = get_default_calendar()
    for c, e in zip(confirm["timestamp"], entry["timestamp"]):
        assert pd.Timestamp(e).normalize() == cal.next_trading_day(
            pd.Timestamp(c).normalize(), 1
        )
    events = spec.build_events(entry, target=0.02)
    assert (events["entry_timing"] == EntryTiming.OPEN.value).all()


def test_sample_spec_confirm_close_keeps_confirm_day():
    days, _, close = _synth_ohlc()
    confirm_days = days[:2]
    spec = SampleSpec(
        entry=_GridEntry(["AAA.SZ"], confirm_days),
        exit=EXIT_TB_1_5_1_V20,
        entry_at=EntryAt.CONFIRM_CLOSE,
    )
    pairs = spec.sample_pairs(close)
    assert set(pd.DatetimeIndex(pairs["timestamp"]).normalize()) == {
        confirm_days[0].normalize(),
        confirm_days[1].normalize(),
    }
    events = spec.build_events(pairs, target=0.02)
    assert (events["entry_timing"] == EntryTiming.CLOSE.value).all()


def test_grid_is_daily_confirmation_then_next_open():
    """网格：每天确认 → 次日开盘入场."""
    days, open_, close = _synth_ohlc(n=15, symbols=["X.SH", "Y.SZ"])
    spec = SampleSpec(
        entry=_GridEntry(["X.SH", "Y.SZ"], days[:3]),
        exit=EXIT_RESEARCH_DEFAULT,
    )
    labs = spec.run(close, open=open_, target=0.02, drop_no_data=False)
    # 3 confirm × 2 sym → 3 entry days × 2
    assert len(labs) == 6
    cal = get_default_calendar()
    entry_days = {
        cal.next_trading_day(pd.Timestamp(d).normalize(), 1) for d in days[:3]
    }
    assert set(pd.DatetimeIndex(labs.index).normalize()) == entry_days
    assert (labs["entry_timing"] == "open").all()


def test_new_high_emits_confirm_day_only():
    n = 40
    idx = pd.bdate_range("2024-01-02", periods=n)
    px = np.full(n, 100.0)
    px[19] = 110.0
    px[20:] = 110.0 + np.arange(n - 20)
    s = pd.Series(px, index=idx)
    confirm = NewHighBreakoutSampler(window=20, cooldown_days=30).sample(s)
    assert confirm[0] == idx[19]

    wide = s.to_frame("S.SZ")
    spec = SampleSpec(
        entry=NewHighBreakoutSampler(window=20, cooldown_days=30),
        exit=EXIT_RESEARCH_DEFAULT,
    )
    entry = spec.sample_pairs(wide)
    cal = get_default_calendar()
    assert pd.Timestamp(entry["timestamp"].iloc[0]).normalize() == cal.next_trading_day(
        idx[19].normalize(), 1
    )


def test_run_next_open_requires_open_panel():
    days, _, close = _synth_ohlc()
    spec = SampleSpec(entry=_GridEntry(["AAA.SZ"], days[:2]))
    with pytest.raises(ValueError, match="open"):
        spec.run(close, target=0.02)


def test_run_confirm_close_accepts_close_only():
    days, _, close = _synth_ohlc()
    # 制造后续上涨，便于触上屏障
    close.iloc[1:] = 105.0
    spec = SampleSpec(
        entry=_GridEntry(["AAA.SZ"], days[:1]),
        exit=EXIT_TB_1_5_1_V20,
        entry_at=EntryAt.CONFIRM_CLOSE,
    )
    labs = spec.run(close, target=0.01, drop_no_data=False)
    assert len(labs) == 1
    assert labs["entry_timing"].iloc[0] == "close"


def test_exit_settings_drives_t1_and_barrier():
    days = pd.bdate_range("2024-01-02", periods=5)
    pairs = daily_event_pairs(["A.SZ"], days[:1])
    # 入场日 pairs（已映射）
    entry = confirmation_to_entry(pairs)
    exit_ = ExitSettings(pt=3.0, sl=1.0, vertical_days=20)
    ev = to_event_dataframe(entry, target=0.02, exit=exit_)
    cal = get_default_calendar()
    t0 = pd.Timestamp(entry["timestamp"].iloc[0]).normalize()
    assert ev["t1"].iloc[0] == cal.next_trading_day(t0, 20)
    assert exit_.barrier().pt == 3.0
    assert exit_.barrier().sl == 1.0
    assert EXIT_RESEARCH_DEFAULT.name() == "TB_3_1_v20"


def test_volume_cusum_requires_volume_kwarg():
    days, _, close = _synth_ohlc()
    vol = close * 0 + 1e6
    spec = SampleSpec(entry=VolumeCUSUMFilter(h=0.5))
    with pytest.raises(ValueError, match="volume"):
        spec.sample_pairs(close)
    pairs = spec.sample_pairs(close, volume=vol)
    assert list(pairs.columns) == ["timestamp", "symbol"]


def test_cusum_through_sample_spec_pipeline():
    days = pd.bdate_range("2024-01-02", periods=60)
    rng = np.random.default_rng(1)
    close = pd.DataFrame(
        {"A.SZ": 100 + np.cumsum(rng.normal(0, 1.5, len(days)))},
        index=days,
    )
    open_ = close * 0.995
    spec = SampleSpec(entry=CUSUMFilter(h=0.03), exit=EXIT_RESEARCH_DEFAULT)
    confirm = spec.confirmation_pairs(close)
    entry = spec.sample_pairs(close)
    if len(confirm) == 0:
        pytest.skip("合成路径未触发 CUSUM")
    assert len(entry) == len(confirm)
    labs = spec.run(close, open=open_, target=0.02, drop_no_data=False)
    assert len(labs) > 0
    assert labs["event_id"].is_unique
    assert set(labs["touch_type"]).issubset({"upper", "lower", "vertical", "no_data"})


def test_event_entry_timing_maps_from_entry_at():
    spec = SampleSpec(entry=CUSUMFilter(h=0.05))
    assert spec.event_entry_timing == EntryTiming.OPEN
    spec_c = SampleSpec(
        entry=CUSUMFilter(h=0.05), entry_at=EntryAt.CONFIRM_CLOSE,
    )
    assert spec_c.event_entry_timing == EntryTiming.CLOSE


def test_build_labeled_samples_forces_matching_entry_timing():
    """安全入口：事件与特征矩阵 entry_timing 一致，且 X 行对齐 labels."""
    import qlab.features.library  # noqa: F401
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    from qlab.labeling import build_labeled_samples

    data = DataLayer(source=FakeDataSource(seed=2, n_symbols=4))
    start, end = "2023-03-01", "2023-06-30"
    universe = data.universe("csi500", start, end)
    daily = data.daily(universe.all_symbols(), start, end)
    close = daily["close"].unstack("symbol")
    open_ = daily["open"].unstack("symbol")

    spec = SampleSpec(entry=CUSUMFilter(h=0.04), exit=EXIT_TB_1_5_1_V20)
    out = build_labeled_samples(
        spec,
        close,
        target=0.03,
        features=["mom_5d", "ewm_vol_20d"],
        data=data,
        universe=universe,
        date_range=(start, end),
        open=open_,
        label_prices=daily[["open", "close"]],
        drop_no_data=True,
        generate_mask=False,
    )
    assert len(out.labels) > 0
    assert len(out.X) == len(out.labels) == len(out.events)
    assert out.feature_matrix.entry_timing == EntryTiming.OPEN
    assert (out.events["entry_timing"] == "open").all()
    assert out.X.index.names == ["event_start", "symbol"]
    assert {"mom_5d", "ewm_vol_20d"} <= set(out.X.columns)
    # 标签 event_id 与事件一致
    assert list(out.labels["event_id"]) == list(out.events["event_id"])


def test_attach_rejects_mismatched_timing_even_if_matrix_built_wrong():
    """手拼时若矩阵 entry_timing 与事件不一致，必须失败."""
    import qlab.features.library  # noqa: F401
    from qlab.core.exceptions import PITViolationError
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    from qlab.features import attach_features_to_events, build_feature_matrix

    data = DataLayer(source=FakeDataSource(seed=3, n_symbols=3))
    start, end = "2023-03-01", "2023-05-31"
    universe = data.universe("csi500", start, end)
    daily = data.daily(universe.all_symbols(), start, end)
    close = daily["close"].unstack("symbol")
    open_ = daily["open"].unstack("symbol")

    spec = SampleSpec(entry=CUSUMFilter(h=0.04), exit=EXIT_TB_1_5_1_V20)
    labs = spec.run(
        close, open=open_, target=0.03,
        label_prices=daily[["open", "close"]], drop_no_data=True,
    )
    if len(labs) == 0:
        pytest.skip("无标签")
    # 故意用 close 矩阵去接 open 事件
    events = spec.build_events(
        spec.sample_pairs(close), target=0.03,
    )
    # 裁到有标签的
    from qlab.labeling.sample_frame import _events_matching_labels

    events = _events_matching_labels(events, labs)
    bad_m = build_feature_matrix(
        features=["mom_5d"],
        data=data,
        universe=universe,
        date_range=(start, end),
        entry_timing=EntryTiming.CLOSE,  # 错
        generate_mask=False,
    )
    with pytest.raises(PITViolationError, match="入场时点不一致"):
        attach_features_to_events(events, bad_m)


def test_liquidity_top_n_mask_filters_confirm_pairs():
    days = pd.bdate_range("2024-01-02", periods=30)
    # A 成交额始终最大，B 中等，C 最小
    amount = pd.DataFrame(
        {
            "A.SZ": np.linspace(100, 200, len(days)),
            "B.SZ": np.linspace(50, 80, len(days)),
            "C.SZ": np.linspace(1, 2, len(days)),
        },
        index=days,
    )
    mask = liquidity_top_n_mask(amount, n=2, window=5)
    pairs = daily_event_pairs(["A.SZ", "B.SZ", "C.SZ"], days[10:15])
    kept = filter_pairs(pairs, mask)
    assert set(kept["symbol"].unique()) <= {"A.SZ", "B.SZ"}
    assert "C.SZ" not in set(kept["symbol"])
    # 每日恰好 2 只入选（在有足够滚动窗口后）
    by_day = kept.groupby(pd.to_datetime(kept["timestamp"]).dt.normalize()).size()
    assert (by_day == 2).all()


def _synth_close_panel(n: int = 40):
    """构造可区分强弱的三标的收盘价面板."""
    days = pd.bdate_range("2024-01-02", periods=n)
    t = np.arange(n, dtype=float)
    close = pd.DataFrame(
        {
            "STRONG.SZ": 100.0 * np.exp(0.01 * t),   # 强
            "MID.SZ": 100.0 * np.exp(0.003 * t),     # 中
            "WEAK.SZ": 100.0 * np.exp(-0.005 * t),   # 弱
        },
        index=days,
    )
    return days, close


def test_relative_strength_mask_keeps_leaders():
    from qlab.labeling import relative_strength_mask

    days, close = _synth_close_panel()
    mask = relative_strength_mask(close, window=10, top_pct=0.4)
    pairs = daily_event_pairs(list(close.columns), days[15:20])
    kept = filter_pairs(pairs, mask)
    assert "STRONG.SZ" in set(kept["symbol"])
    assert "WEAK.SZ" not in set(kept["symbol"])


def test_masks_share_core_price_panels_with_features():
    """采样门与特征共用 core.price_panels，避免双份实现漂移."""
    from qlab.core.price_panels import (
        dist_to_high_panel,
        is_stage2_panel,
        smooth_momentum_panel,
    )
    from qlab.labeling import (
        near_high_mask,
        relative_strength_mask,
        smooth_momentum_score,
        stage2_mask,
    )

    days, close = _synth_close_panel(n=80)
    s2 = stage2_mask(close, ma_window=20, slope_lookback=5)
    expect_s2 = is_stage2_panel(close, ma_window=20, slope_lookback=5).stack(
        future_stack=True
    )
    expect_s2.index = expect_s2.index.set_names(["date", "symbol"])
    aligned = s2.reindex(expect_s2.index).fillna(False)
    assert (aligned == expect_s2.fillna(False)).all()

    dist = dist_to_high_panel(close, window=20)
    nh = near_high_mask(close, window=20, max_dist=0.1)
    expect_nh = (dist.notna() & (dist >= -0.1)).stack(future_stack=True)
    expect_nh.index = expect_nh.index.set_names(["date", "symbol"])
    assert (nh.reindex(expect_nh.index).fillna(False) == expect_nh.fillna(False)).all()

    sm = smooth_momentum_score(close, window=20)
    assert sm.equals(smooth_momentum_panel(close, window=20))
    # method=smooth 应能产出与 score 路径一致的 Top-1
    a = relative_strength_mask(close, method="smooth", window=20, top_n=1)
    b = relative_strength_mask(score=sm, top_n=1)
    assert a.equals(b)


def test_cross_sectional_rank_mask_top_n():
    from qlab.labeling import cross_sectional_rank_mask

    days = pd.bdate_range("2024-01-02", periods=5)
    score = pd.DataFrame(
        {"A.SZ": [3, 3, 3, 3, 3], "B.SZ": [2, 2, 2, 2, 2], "C.SZ": [1, 1, 1, 1, 1]},
        index=days,
    )
    mask = cross_sectional_rank_mask(score, top_n=1)
    pairs = daily_event_pairs(["A.SZ", "B.SZ", "C.SZ"], days)
    kept = filter_pairs(pairs, mask)
    assert set(kept["symbol"]) == {"A.SZ"}
    assert len(kept) == 5


def test_industry_rs_and_leader_masks():
    from qlab.labeling import industry_leader_mask, industry_rs_mask

    days, close = _synth_close_panel(n=50)
    # STRONG/MID 同行业电子；WEAK 在银行（弱行业）
    industry = pd.DataFrame(
        {
            "STRONG.SZ": "801080",
            "MID.SZ": "801080",
            "WEAK.SZ": "801780",
        },
        index=days,
    )
    # 再加一只同属银行但更弱，保证行业分化
    close["BANK2.SZ"] = 100.0 * np.exp(-0.008 * np.arange(len(days)))
    industry["BANK2.SZ"] = "801780"

    ind_mask = industry_rs_mask(close, industry, window=10, top_n_industries=1)
    pairs = daily_event_pairs(list(close.columns), days[20:25])
    kept_ind = filter_pairs(pairs, ind_mask)
    assert set(kept_ind["symbol"]) <= {"STRONG.SZ", "MID.SZ"}
    assert "WEAK.SZ" not in set(kept_ind["symbol"])

    leader = industry_leader_mask(close, industry, window=10, top_pct=0.5)
    kept_leader = filter_pairs(pairs, leader)
    # 电子行业内 STRONG 应强于 MID；银行内 WEAK 强于 BANK2
    assert "STRONG.SZ" in set(kept_leader["symbol"])
    assert "BANK2.SZ" not in set(kept_leader["symbol"])


def test_volume_confirm_and_anti_climax():
    from qlab.labeling import anti_climax_mask, volume_confirm_mask

    days = pd.bdate_range("2024-01-02", periods=30)
    vol = pd.DataFrame(
        {
            "HOT.SZ": np.concatenate([np.full(25, 100.0), np.full(5, 300.0)]),
            "DRY.SZ": np.full(30, 100.0),
        },
        index=days,
    )
    vmask = volume_confirm_mask(vol, window=5, min_ratio=2.0)
    pairs = daily_event_pairs(["HOT.SZ", "DRY.SZ"], days[25:30])
    kept_v = filter_pairs(pairs, vmask)
    assert set(kept_v["symbol"]) == {"HOT.SZ"}

    close = pd.DataFrame(
        {
            "HOT.SZ": 100.0 * np.exp(0.05 * np.arange(30)),  # 暴涨
            "OK.SZ": 100.0 * np.exp(0.002 * np.arange(30)),
        },
        index=days,
    )
    amask = anti_climax_mask(close, short_window=5, max_pct=0.5)
    pairs2 = daily_event_pairs(["HOT.SZ", "OK.SZ"], days[20:25])
    kept_a = filter_pairs(pairs2, amask)
    assert "HOT.SZ" not in set(kept_a["symbol"])
    assert "OK.SZ" in set(kept_a["symbol"])


def test_tradable_hygiene_and_market_breadth():
    from qlab.labeling import (
        combine_masks,
        market_breadth_mask,
        tradable_hygiene_mask,
    )

    days = pd.bdate_range("2024-01-02", periods=20)
    syms = ["A.SZ", "B.SZ"]
    idx = pd.MultiIndex.from_product([days, syms], names=["date", "symbol"])
    daily = pd.DataFrame(
        {
            "is_st": [False] * len(idx),
            "is_suspended": [False] * len(idx),
            "is_limit_up": [False] * len(idx),
            "days_since_listing": [100] * len(idx),
        },
        index=idx,
    )
    # A 在最后一天涨停；B 是 ST
    daily.loc[(days[-1], "A.SZ"), "is_limit_up"] = True
    daily.loc[(days[-1], "B.SZ"), "is_st"] = True
    hmask = tradable_hygiene_mask(daily, min_listing_days=30)
    pairs = daily_event_pairs(syms, days[-1:])
    kept = filter_pairs(pairs, hmask)
    assert len(kept) == 0

    # 广度：前半段普涨，后半段普跌
    close = pd.DataFrame(
        {
            "A.SZ": np.concatenate([np.linspace(100, 120, 10), np.linspace(120, 100, 10)]),
            "B.SZ": np.concatenate([np.linspace(50, 60, 10), np.linspace(60, 40, 10)]),
        },
        index=days,
    )
    bmask = market_breadth_mask(close, min_advance_pct=0.5)
    pairs_up = daily_event_pairs(syms, days[5:8])
    pairs_dn = daily_event_pairs(syms, days[15:18])
    assert len(filter_pairs(pairs_up, bmask)) == len(pairs_up)
    assert len(filter_pairs(pairs_dn, bmask)) == 0

    # combine AND
    m1 = tradable_hygiene_mask(daily)
    m2 = market_breadth_mask(close)
    both = combine_masks(m1, m2, how="and")
    assert both.dtype == bool


def test_stack_sampler_then_gates():
    """采样器 → 多门 AND 叠加的端到端烟雾."""
    from qlab.labeling import (
        NewHighBreakoutSampler,
        combine_masks,
        near_high_mask,
        relative_strength_mask,
        stage2_mask,
        volume_confirm_mask,
    )

    days, close = _synth_close_panel(n=60)
    # 给 STRONG 做一次明显新高 + 放量
    close = close.copy()
    close.loc[days[40], "STRONG.SZ"] = close.loc[days[39], "STRONG.SZ"] * 1.08
    amount = pd.DataFrame(100.0, index=days, columns=close.columns)
    amount.loc[days[40], "STRONG.SZ"] = 400.0

    pairs = NewHighBreakoutSampler(window=20, cooldown_days=30).sample_per_symbol(close)
    assert len(pairs) > 0
    gates = combine_masks(
        relative_strength_mask(close, window=10, top_pct=0.5),
        volume_confirm_mask(amount, window=5, min_ratio=2.0),
        how="and",
    )
    kept = filter_pairs(pairs, gates)
    # 放量新高的强票应能留下；弱票即使有事件也应被 RS/量能挡掉
    assert "WEAK.SZ" not in set(kept["symbol"])
    if len(kept):
        assert "STRONG.SZ" in set(kept["symbol"])

    # stage2 / near_high / smooth RS 可叠加且不炸
    s2 = stage2_mask(close, ma_window=20, slope_lookback=5)
    nh = near_high_mask(close, window=20, max_dist=0.5)
    rs_s = relative_strength_mask(close, method="smooth", window=20, top_n=2)
    assert s2.dtype == bool and nh.dtype == bool and rs_s.dtype == bool


def test_stage2_near_high_smooth_rs_and_hygiene_price_amount():
    from qlab.labeling import (
        mask_to_wide,
        near_high_mask,
        relative_strength_mask,
        stage2_mask,
        tradable_hygiene_mask,
    )

    days, close = _synth_close_panel(n=80)
    # 短均线 Stage2：STRONG 应更容易为 True
    s2 = stage2_mask(close, ma_window=20, slope_lookback=5)
    pairs = daily_event_pairs(["STRONG.SZ", "WEAK.SZ"], days[40:45])
    kept_s2 = filter_pairs(pairs, s2)
    assert "STRONG.SZ" in set(kept_s2["symbol"])
    assert "WEAK.SZ" not in set(kept_s2["symbol"])

    # WEAK 持续下跌，距 20 日高点回撤远超 5% → 剔除
    nh = near_high_mask(close, window=20, max_dist=0.05)
    kept_nh = filter_pairs(pairs, nh)
    assert "WEAK.SZ" not in set(kept_nh["symbol"])
    assert "STRONG.SZ" in set(kept_nh["symbol"])

    # smooth RS + 外部 score 路径
    rs = relative_strength_mask(close, method="smooth", window=30, top_pct=0.4)
    kept_rs = filter_pairs(pairs, rs)
    assert "WEAK.SZ" not in set(kept_rs["symbol"])
    score = close  # 任意分数：高价≈更强（仅测 API）
    rs2 = relative_strength_mask(score=score, top_n=1)
    assert filter_pairs(pairs, rs2)["symbol"].nunique() <= 1

    # 卫生门：价格 + 成交额
    syms = ["A.SZ", "B.SZ"]
    idx = pd.MultiIndex.from_product([days[:25], syms], names=["date", "symbol"])
    daily = pd.DataFrame(
        {
            "is_st": False,
            "is_suspended": False,
            "is_limit_up": False,
            "days_since_listing": 100,
            "close_raw": [10.0 if s == "A.SZ" else 1.0 for _, s in idx],
            "amount": [1e8 if s == "A.SZ" else 1e3 for _, s in idx],
        },
        index=idx,
    )
    h = tradable_hygiene_mask(
        daily, min_close=2.0, min_avg_amount=1e7, avg_amount_window=5
    )
    kept_h = filter_pairs(daily_event_pairs(syms, days[10:15]), h)
    assert set(kept_h["symbol"]) == {"A.SZ"}
    elig = mask_to_wide(h)
    assert elig.shape[1] == 2
    assert elig.dtypes.nunique() == 1


def test_market_breadth_index_stage2_and_net_limit():
    from qlab.labeling import market_breadth_mask, market_breadth_ok

    days = pd.bdate_range("2020-01-02", periods=250)
    # 多数股票跟随指数缓涨
    t = np.arange(len(days), dtype=float)
    close = pd.DataFrame(
        {f"S{i}.SZ": 100.0 * np.exp(0.001 * t) for i in range(5)},
        index=days,
    )
    idx = pd.Series(100.0 * np.exp(0.001 * t), index=days)
    ok = market_breadth_ok(
        close,
        min_advance_pct=0.4,
        index_close=idx,
        require_index_stage2=True,
        ma_window=60,
        slope_lookback=10,
    )
    # 均线就绪后应为 True
    assert bool(ok.iloc[-1])

    # 涨停净占比：制造跌停多于涨停 → 关掉
    lu = pd.DataFrame(False, index=days, columns=close.columns)
    ld = pd.DataFrame(False, index=days, columns=close.columns)
    ld.iloc[-1, :] = True
    ok2 = market_breadth_ok(
        close,
        min_advance_pct=0.0,
        is_limit_up=lu,
        is_limit_down=ld,
        min_net_limit_ratio=0.0,
    )
    assert not bool(ok2.iloc[-1])

    m = market_breadth_mask(
        close, min_advance_pct=0.4, index_close=idx, require_index_above_ma=True,
        ma_window=60,
    )
    assert m.dtype == bool
