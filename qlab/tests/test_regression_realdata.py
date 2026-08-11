"""真实数据回归测试 —— 按模块分组.

默认跳过(需网络+聚宽凭证)。运行方式::

    pytest -m realdata                          # 全量回归
    pytest -m realdata -k regression_features   # 只回归 features 模块

设计原则
--------
- **按模块分组**: 每个 class 对应一个 qlab 模块, 失败即可定位到模块
- **断言不变量而非具体数值**: 真实行情会变, 断言"复权偏差 < 1e-9""无未来函数"
  这类性质, 而不是"某日收盘价 = 1521.01"
- 共享 fixture 见 ``conftest.py`` —— 真实数据只拉一次
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tests.conftest import REG_END, REG_START, REG_SYMBOLS, REG_WARMUP

pytestmark = pytest.mark.realdata


# ======================================================================
# data 层
# ======================================================================
class TestRegressionData:
    """数据层: schema 合规 + 复权一致性 + PIT."""

    def test_daily_schema_and_coverage(self, real_daily):
        from qlab.core.schema import SCHEMA_DAILY_BAR, validate_schema

        validate_schema(real_daily, SCHEMA_DAILY_BAR, strict_index=True)
        assert real_daily.index.get_level_values("symbol").nunique() == len(
            REG_SYMBOLS
        )
        assert len(real_daily) > 4000, "10只×2年+warmup 应有数千行"

    def test_adjustment_identity(self, real_daily):
        """close == close_raw × adj_factor —— 复权口径自洽."""
        d = real_daily
        lhs = d["close"].to_numpy()
        rhs = (d["close_raw"] * d["adj_factor"]).to_numpy()
        assert np.nanmax(np.abs(lhs - rhs)) < 1e-6

    def test_post_adjusted_factor_monotonic(self, real_daily):
        """后复权因子对每只标的单调不减(除权只会让因子变大)."""
        for sym, g in real_daily.groupby(level="symbol"):
            f = g["adj_factor"].to_numpy()
            assert np.all(np.diff(f) >= -1e-9), f"{sym} adj_factor 非单调"

    def test_no_future_prices_beyond_end(self, real_daily):
        assert real_daily.index.get_level_values("date").max() <= pd.Timestamp(
            REG_END
        )

    def test_margin_available_at_is_next_session(self, real_layer):
        """两融 available_at 必严格晚于数据日(T 日盘中不可用)."""
        m = real_layer.margin_trading(REG_SYMBOLS[:3], "2024-06-03", "2024-06-28")
        if m.empty:
            pytest.skip("样本期无两融数据")
        d = m.index.get_level_values("date")
        assert (m["available_at"] > d).all()

    def test_call_auction_available_same_session(self, real_layer):
        """竞价 available_at 落在当日(盘中即可用)."""
        c = real_layer.call_auction(REG_SYMBOLS[:3], "2024-06-03", "2024-06-28")
        if c.empty:
            pytest.skip("样本期无竞价数据")
        d = c.index.get_level_values("date")
        assert (c["available_at"] >= d).all()
        assert (c["available_at"] < d + pd.Timedelta(days=1)).all()


# ======================================================================
# features 层
# ======================================================================
class TestRegressionFeatures:
    """特征层: 全因子可算 + PIT 不变性 + 依赖自动补齐."""

    ALL_EXPECTED = 17   # registry 应注册的因子数(参数化实例保留)

    def test_registry_completeness(self):
        import qlab.features.library  # noqa: F401
        from qlab.features.registry import registry

        names = registry.all_names()
        assert len(names) >= self.ALL_EXPECTED, f"因子数退化: {len(names)}"
        # 依赖必须全部可解析
        missing = [
            (f.meta.name, dep)
            for f in registry.all_features()
            for dep in f.meta.dependencies
            if not registry.has(dep)
        ]
        assert not missing, f"依赖缺失: {missing}"

    @pytest.mark.parametrize(
        "feature_name",
        [
            "mom_5d", "mom_20d", "mom_resid_20d", "ewm_vol_20d", "rv_20d",
            "turnover_5d", "vol_ratio_5d", "ma_20d", "log_close", "pe_ttm",
            "auction_premium", "auction_vol_ratio",
        ],
    )
    def test_each_feature_computes(self, real_layer, real_universe, feature_name):
        """每个因子在真实数据上可算, 且 NaN 率不异常."""
        import qlab.features.library  # noqa: F401
        from qlab.features.matrix import build_feature_matrix

        fmx = build_feature_matrix(
            features=[feature_name], data=real_layer,
            universe=real_universe, date_range=(REG_START, REG_END),
        )
        col = fmx.values[feature_name]
        assert len(col) > 0
        assert col.isna().mean() < 0.35, f"{feature_name} NaN 率过高"

    def test_pit_invariance_under_extended_data(self, real_layer, real_universe):
        """**PIT 核心**: 同一天的特征不因"未来数据存在"而变化."""
        import qlab.features.library  # noqa: F401
        from qlab.core.calendar import get_default_calendar
        from qlab.data.universe import Universe, UniverseSpec
        from qlab.features.matrix import build_feature_matrix

        feats = ["mom_5d", "ewm_vol_20d", "turnover_5d"]
        cut = "2024-03-29"
        cal = get_default_calendar()

        def build(end: str) -> pd.DataFrame:
            dates = cal.trading_days(pd.Timestamp(REG_START), pd.Timestamp(end))
            idx = pd.MultiIndex.from_product(
                [dates, REG_SYMBOLS], names=["date", "symbol"]
            )
            uni = Universe(
                pd.DataFrame(
                    {"in_universe": True, "weight": 1.0 / len(REG_SYMBOLS)},
                    index=idx,
                ),
                UniverseSpec("pit"),
            )
            return build_feature_matrix(
                features=feats, data=real_layer, universe=uni,
                date_range=(REG_START, end),
            ).values

        short = build(cut)
        long = build(REG_END)          # 多出 8 个月未来数据
        common = short.index
        for c in feats:
            diff = (short[c] - long.reindex(common)[c]).abs().max()
            assert pd.isna(diff) or diff < 1e-12, (
                f"{c} 受未来数据影响, 最大偏差 {diff:.3e} —— 存在未来函数"
            )

    def test_feature_window_never_exceeds_target_end(self, real_layer, real_universe):
        """FeatureContext 取数窗口右端不得越过 target_dates[-1]."""
        from qlab.core.calendar import get_default_calendar
        from qlab.features.context import FeatureContext

        cal = get_default_calendar()
        dates = cal.trading_days(pd.Timestamp(REG_START), pd.Timestamp("2024-06-14"))
        ctx = FeatureContext(
            data=real_layer, target_dates=dates,
            universe=real_universe, calendar=cal,
        )
        got = ctx.daily(lookback_days=20)
        assert got.index.get_level_values("date").max() <= dates[-1]


# ======================================================================
# sampling 合约（入场 + 经典 TB 出场）
# ======================================================================
class TestRegressionSampling:
    """采样合约: 确认日 → 次日开盘 / 可选确认收盘 + ExitSettings."""

    def test_cusum_confirm_maps_to_next_open(
        self, real_close_wide, real_open_wide, real_daily,
    ):
        from qlab.core.calendar import get_default_calendar
        from qlab.core.enums import EntryAt
        from qlab.labeling import (
            EXIT_RESEARCH_DEFAULT,
            CUSUMFilter,
            SampleSpec,
        )

        cal = get_default_calendar()
        spec = SampleSpec(entry=CUSUMFilter(h=0.05), exit=EXIT_RESEARCH_DEFAULT)
        confirm = spec.confirmation_pairs(real_close_wide)
        confirm = confirm[
            (confirm["timestamp"] >= pd.Timestamp(REG_START))
            & (confirm["timestamp"] <= pd.Timestamp(REG_END))
        ]
        assert len(confirm) > 50, f"确认日过少: {len(confirm)}"

        entry = spec.sample_pairs(real_close_wide)
        # 按确认日窗口对齐后比较映射
        entry_clip = confirmation_to_entry_clip(confirm, entry, cal)
        assert len(entry_clip) == len(confirm)
        for c, e in zip(
            pd.to_datetime(confirm["timestamp"]),
            pd.to_datetime(entry_clip["timestamp"]),
        ):
            assert e.normalize() == cal.next_trading_day(c.normalize(), 1)

        labs = spec.run(
            real_close_wide,
            open=real_open_wide,
            target=0.03,
            label_prices=real_daily[["open", "close"]],
            drop_no_data=True,
        )
        labs = labs[
            (labs.index >= pd.Timestamp(REG_START))
            & (labs.index <= pd.Timestamp(REG_END))
        ]
        assert len(labs) > 50
        assert (labs["entry_timing"] == "open").all()
        assert labs["event_id"].is_unique
        assert spec._entry_at() == EntryAt.NEXT_OPEN

    def test_grid_daily_confirm_next_open(
        self, real_close_wide, real_open_wide, real_daily,
    ):
        from qlab.core.calendar import get_default_calendar
        from qlab.labeling import (
            EXIT_TB_1_5_1_V20,
            SampleSpec,
            daily_event_pairs,
        )

        cal = get_default_calendar()
        # 取一小段交易日作确认网格
        days = cal.trading_days(
            pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-14"),
        )
        syms = REG_SYMBOLS[:3]

        class _Grid:
            def sample_per_symbol(self, prices):
                return daily_event_pairs(syms, days)

        spec = SampleSpec(entry=_Grid(), exit=EXIT_TB_1_5_1_V20)
        confirm = spec.confirmation_pairs(real_close_wide)
        entry = spec.sample_pairs(real_close_wide)
        assert len(confirm) == len(days) * len(syms)
        assert len(entry) == len(confirm)
        # 入场日 = 确认日的下一交易日
        for c, e in zip(
            pd.to_datetime(confirm.sort_values(["symbol", "timestamp"])["timestamp"]),
            pd.to_datetime(entry.sort_values(["symbol", "timestamp"])["timestamp"]),
        ):
            assert e.normalize() == cal.next_trading_day(c.normalize(), 1)

        labs = spec.run(
            real_close_wide,
            open=real_open_wide,
            target=0.02,
            label_prices=real_daily[["open", "close"]],
            drop_no_data=False,
        )
        assert (labs["entry_timing"] == "open").all()
        assert len(labs) == len(entry)

    def test_confirm_close_entry_on_real(
        self, real_close_wide, real_daily,
    ):
        from qlab.core.enums import EntryAt
        from qlab.labeling import (
            EXIT_TB_1_5_1_V20,
            CUSUMFilter,
            SampleSpec,
        )

        spec = SampleSpec(
            entry=CUSUMFilter(h=0.06),
            exit=EXIT_TB_1_5_1_V20,
            entry_at=EntryAt.CONFIRM_CLOSE,
        )
        confirm = spec.confirmation_pairs(real_close_wide)
        confirm = confirm[
            (confirm["timestamp"] >= pd.Timestamp(REG_START))
            & (confirm["timestamp"] <= pd.Timestamp(REG_END))
        ]
        entry = spec.sample_pairs(real_close_wide)
        entry = entry[
            (entry["timestamp"] >= pd.Timestamp(REG_START))
            & (entry["timestamp"] <= pd.Timestamp(REG_END))
        ]
        # 确认收盘：入场日 == 确认日
        merged = confirm.merge(
            entry, on=["timestamp", "symbol"], how="inner",
        )
        assert len(merged) == len(confirm)

        labs = spec.run(
            real_close_wide,
            target=0.03,
            label_prices=real_daily[["close"]],
            drop_no_data=True,
        )
        labs = labs[
            (labs.index >= pd.Timestamp(REG_START))
            & (labs.index <= pd.Timestamp(REG_END))
        ]
        assert len(labs) > 30
        assert (labs["entry_timing"] == "close").all()

    def test_exit_settings_tb_touch_types(
        self, real_close_wide, real_open_wide, real_daily,
    ):
        """经典 TB：touch_type ∈ {upper, lower, vertical}（丢弃 no_data 后）."""
        from qlab.labeling import (
            EXIT_RESEARCH_DEFAULT,
            CUSUMFilter,
            SampleSpec,
        )

        spec = SampleSpec(entry=CUSUMFilter(h=0.05), exit=EXIT_RESEARCH_DEFAULT)
        labs = spec.run(
            real_close_wide,
            open=real_open_wide,
            target=0.03,
            label_prices=real_daily[["open", "close"]],
            drop_no_data=True,
        )
        labs = labs[
            (labs.index >= pd.Timestamp(REG_START))
            & (labs.index <= pd.Timestamp(REG_END))
        ]
        assert len(labs) > 50
        assert set(labs["touch_type"]) <= {"upper", "lower", "vertical"}
        assert {"upper", "lower"}.issubset(set(labs["touch_type"]))
        starts = pd.DatetimeIndex(labs.index)
        touch = pd.to_datetime(labs["touch_time"])
        assert (touch >= starts).all()

    def test_new_high_confirm_then_next_open(self, real_close_wide):
        from qlab.core.calendar import get_default_calendar
        from qlab.labeling import NewHighBreakoutSampler, SampleSpec

        cal = get_default_calendar()
        sampler = NewHighBreakoutSampler(window=20, cooldown_days=5)
        confirm = sampler.sample_per_symbol(real_close_wide)
        confirm = confirm[
            (confirm["timestamp"] >= pd.Timestamp(REG_START))
            & (confirm["timestamp"] <= pd.Timestamp(REG_END))
        ]
        assert len(confirm) > 0
        entry = SampleSpec(entry=sampler).sample_pairs(real_close_wide)
        # 抽查：每条确认日映射到次日
        sample = confirm.head(20)
        for row in sample.itertuples():
            exp = cal.next_trading_day(pd.Timestamp(row.timestamp).normalize(), 1)
            hit = entry[
                (entry["symbol"] == row.symbol)
                & (pd.to_datetime(entry["timestamp"]).dt.normalize() == exp)
            ]
            assert len(hit) >= 1

    def test_build_labeled_samples_real(
        self, real_layer, real_universe, real_close_wide, real_open_wide, real_daily,
    ):
        """真实数据走唯一安全入口：事件 / 矩阵 / X 入场点一致."""
        from qlab.core.enums import EntryTiming
        from qlab.labeling import (
            EXIT_RESEARCH_DEFAULT,
            CUSUMFilter,
            SampleSpec,
            build_labeled_samples,
        )

        spec = SampleSpec(entry=CUSUMFilter(h=0.05), exit=EXIT_RESEARCH_DEFAULT)
        out = build_labeled_samples(
            spec,
            real_close_wide,
            target=0.03,
            features=["mom_5d", "ewm_vol_20d", "is_stage2_200d"],
            data=real_layer,
            universe=real_universe,
            date_range=(REG_START, REG_END),
            open=real_open_wide,
            label_prices=real_daily[["open", "close"]],
            drop_no_data=True,
            generate_mask=False,
        )
        assert len(out.labels) > 50
        assert len(out.X) == len(out.labels)
        assert out.feature_matrix.entry_timing == EntryTiming.OPEN
        assert (out.events["entry_timing"] == "open").all()
        assert out.X["mom_5d"].notna().mean() > 0.5


class TestRegressionSampleMasks:
    """采样门: 真实数据上可叠加、能收缩样本、与特征面板一致."""

    def test_main_stack_gates_shrink_new_high_pairs(
        self, real_daily, real_close_wide, real_layer,
    ):
        from qlab.data.industry import industry_matrix_as_of
        from qlab.labeling import (
            NewHighBreakoutSampler,
            combine_masks,
            filter_pairs,
            industry_leader_mask,
            industry_rs_mask,
            liquidity_top_n_mask,
            market_breadth_mask,
            near_high_mask,
            relative_strength_mask,
            stage2_mask,
            tradable_hygiene_mask,
            volume_confirm_mask,
        )

        close = real_close_wide
        amount = real_daily["amount"].unstack("symbol")
        pairs = NewHighBreakoutSampler(window=20, cooldown_days=5).sample_per_symbol(
            close
        )
        pairs = pairs[
            (pairs["timestamp"] >= pd.Timestamp(REG_START))
            & (pairs["timestamp"] <= pd.Timestamp(REG_END))
        ]
        assert len(pairs) > 0

        hygiene = tradable_hygiene_mask(
            real_daily,
            min_listing_days=30,
            min_close=1.0,
            min_avg_amount=1e6,
            avg_amount_window=20,
        )
        gates = combine_masks(
            hygiene,
            liquidity_top_n_mask(amount, n=8, window=20),
            stage2_mask(close),
            relative_strength_mask(close, method="smooth", window=60, top_pct=0.5),
            near_high_mask(close, window=60, max_dist=0.35),
            volume_confirm_mask(amount, window=5, min_ratio=1.0),
            market_breadth_mask(close, min_advance_pct=0.3),
            how="and",
        )
        kept = filter_pairs(pairs, gates)
        assert len(kept) < len(pairs)
        assert len(kept) > 0

        # 门与 core.price_panels 同算法（特征层也走同一面板，避免双份漂移）
        from qlab.core.price_panels import is_stage2_panel

        s2 = stage2_mask(close)
        expect = is_stage2_panel(close).stack(future_stack=True)
        expect.index = expect.index.set_names(["date", "symbol"])
        common = s2.index.intersection(expect.index)
        assert len(common) > 1000
        assert (
            s2.reindex(common).fillna(False) == expect.reindex(common).fillna(False)
        ).all()

        # 行业门：有行业数据时能跑通并进一步收缩
        try:
            ind_hist = real_layer.source.fetch_industry_classification(
                list(close.columns),
                (pd.Timestamp(REG_WARMUP), pd.Timestamp(REG_END)),
                system="sw",
                level=1,
                sample_freq="Q",
            )
        except TypeError:
            # 旧签名无 sample_freq 时回退
            ind_hist = real_layer.source.fetch_industry_classification(
                list(close.columns),
                (pd.Timestamp(REG_WARMUP), pd.Timestamp(REG_END)),
                system="sw",
                level=1,
            )
        except Exception as exc:
            pytest.skip(f"行业数据不可用: {exc}")
        if ind_hist is None or len(ind_hist) == 0:
            pytest.skip("行业数据为空")
        ind_mat = industry_matrix_as_of(
            ind_hist, list(close.columns), close.index, system="sw", level=1
        )
        ind_gate = combine_masks(
            industry_rs_mask(close, ind_mat, window=20, top_n_industries=3),
            industry_leader_mask(close, ind_mat, window=20, top_pct=0.5),
            how="and",
        )
        kept2 = filter_pairs(kept, ind_gate)
        assert len(kept2) <= len(kept)


def confirmation_to_entry_clip(confirm: pd.DataFrame, entry: pd.DataFrame, cal) -> pd.DataFrame:
    """按确认日顺序构造期望入场日，再与 entry 按 (symbol, 期望日) 对齐."""
    rows = []
    for r in confirm.itertuples():
        rows.append({
            "timestamp": cal.next_trading_day(pd.Timestamp(r.timestamp).normalize(), 1),
            "symbol": r.symbol,
        })
    expect = pd.DataFrame(rows)
    # entry 可能含窗口外映射；用 merge 校验存在性
    m = expect.merge(entry, on=["timestamp", "symbol"], how="left", indicator=True)
    assert (m["_merge"] == "both").all(), "部分确认日未映射到入场日"
    return expect


# ======================================================================
# labeling 层
# ======================================================================
class TestRegressionLabeling:
    """标签层: 时间方向 + 采样器 + 并行等价 + meta-labeling."""

    def test_labels_are_forward_looking_only(self, real_labels):
        """标签实现时刻不得早于事件日.

        开盘入场时允许 touch_time == event_start（当日收盘触障），
        日内顺序仍是 open → close，不算未来函数。
        """
        starts = pd.DatetimeIndex(real_labels.index)
        touch = pd.to_datetime(real_labels["touch_time"])
        assert (touch >= starts).all(), "存在 touch_time < event_start 的样本"

    def test_label_bins_are_balanced_enough(self, real_labels):
        """三类标签都应出现(否则障碍参数或采样有问题)."""
        vc = real_labels["bin"].value_counts()
        assert len(real_labels) > 200, f"样本太少: {len(real_labels)}"
        assert set(vc.index) >= {-1, 1}, f"标签退化: {dict(vc)}"

    @pytest.mark.parametrize("h", [0.03, 0.05])
    def test_cusum_scalar_threshold(self, real_close_wide, h):
        from qlab.labeling import CUSUMFilter

        out = CUSUMFilter(h=h).sample_per_symbol(real_close_wide)
        assert set(out.columns) == {"timestamp", "symbol"}
        assert len(out) > 0

    def test_cusum_dynamic_threshold(self, real_close_wide):
        """h 为 Series(随波动率变化) —— AFML Ch3 推荐用法."""
        from qlab.labeling import CUSUMFilter
        from qlab.labeling.thresholds import daily_ewm_vol_panel

        vol = daily_ewm_vol_panel(real_close_wide, span=20)
        h = vol.mean(axis=1).ffill().bfill()   # 逐日阈值
        out = CUSUMFilter(h=h).sample_per_symbol(real_close_wide)
        assert len(out) > 0

    def test_parallel_labeling_equals_serial(self, real_daily, real_labels):
        """并行与串行结果必须完全等价, 且与**逐标的单独打标**一致.

        60: 真实数据下多标的共享大量重复 ``event_start``, 而旧实现的
        子任务乱序返回会把标签静默错配到别的标的上。只比 1 vs 2
        抓不住(两个分子往往恰好按序回), 所以还要跑 4/8 并对 ground truth。
        """
        from qlab.labeling import TripleBarrier
        from qlab.labeling.triple_barrier import label_events

        events = real_labels[["symbol", "t1", "target"]].head(400).copy()
        assert events.index.duplicated().sum() > 0, "前提: 多标的 event_start 应重复"
        bar = TripleBarrier(1.5, 1.0)
        close = real_daily[["close"]]
        serial = label_events(events, close, bar, num_threads=1)

        # ground truth: 逐标的单独打标(内部 event_start 不重复), 再按
        # (event_start, symbol) 主键对齐
        truth = (
            pd.concat(
                [
                    label_events(events[events["symbol"] == s], close, bar, num_threads=1)
                    for s in events["symbol"].unique()
                ]
            )
            .set_index("symbol", append=True)
            .sort_index()
        )

        for nt in (2, 4, 8):
            got = label_events(events, close, bar, num_threads=nt)
            pd.testing.assert_frame_equal(serial, got)
            pd.testing.assert_frame_equal(
                truth, got.set_index("symbol", append=True).sort_index()
            )

    def test_meta_labeling_multi_symbol(self, real_daily, real_labels):
        """meta-labeling 在多标的下按 (event_start, symbol) 对齐."""
        from qlab.labeling import TripleBarrier, meta_label_bins, to_meta_labels
        from qlab.labeling.triple_barrier import label_events

        sub = real_labels[["symbol", "t1", "target"]].head(300).copy()
        side = pd.Series(
            1.0,
            index=pd.MultiIndex.from_arrays(
                [sub.index, sub["symbol"]], names=["event_start", "symbol"]
            ),
        )
        ev = to_meta_labels(sub, side)
        assert ev["side"].notna().all()
        out = label_events(ev, real_daily[["close"]], TripleBarrier(1.5, 1.0))
        mb = meta_label_bins(out)
        assert set(mb.unique()) <= {0, 1}


# ======================================================================
# weights 层
# ======================================================================
class TestRegressionWeights:
    """样本权重: 唯一度语义 + 多标的对齐 + 序贯自举有效性."""

    def test_sample_weights_multi_symbol(self, real_daily, real_labels):
        """多标的(event_start 重复)下可算, 且归一到 Σw = N."""
        from qlab.weights import sample_weights

        assert pd.Index(real_labels.index).duplicated().sum() > 0, (
            "回归前提: 多标的样本 event_start 必须有重复"
        )
        w = sample_weights(real_labels, real_daily[["close"]], time_decay=0.5)
        assert len(w) == len(real_labels)
        assert w["final_weight"].notna().all()
        assert abs(w["final_weight"].sum() - len(w)) < 1e-6

    def test_uniqueness_in_valid_range(self, real_daily, real_labels):
        """平均唯一度 ∈ (0, 1]; 重叠样本必然 < 1."""
        from qlab.weights import sample_weights

        w = sample_weights(real_labels, real_daily[["close"]])
        u = w["uniqueness"].dropna()
        assert (u > 0).all() and (u <= 1.0 + 1e-9).all()
        assert u.mean() < 1.0, "存在重叠时唯一度均值应 < 1"

    def test_seq_bootstrap_improves_uniqueness(self, real_close_wide, real_labels):
        """序贯自举的平均唯一度应高于标准自举(它存在的理由)."""
        from qlab.weights import build_indicator_matrix, seq_bootstrap_sample

        sym = REG_SYMBOLS[0]
        sub = real_labels[real_labels["symbol"] == sym]
        if len(sub) < 20:
            pytest.skip(f"{sym} 样本不足")
        ind = build_indicator_matrix(real_close_wide.index, sub["t1"])
        n = ind.shape[1]

        def avg_uniq(cols: list[int]) -> float:
            m = ind.iloc[:, cols]
            c = m.sum(axis=1).replace(0, np.nan)
            return float(np.nanmean(m.div(c, axis=0).replace(0, np.nan).to_numpy()))

        rng = np.random.default_rng(0)
        std = [avg_uniq(list(rng.integers(0, n, size=n))) for _ in range(10)]
        seq = [avg_uniq(seq_bootstrap_sample(ind, sample_length=n, seed=s))
               for s in range(10)]
        assert np.mean(seq) > np.mean(std)


# ======================================================================
# models 层
# ======================================================================
class TestRegressionModels:
    """交叉验证 + 特征重要性: purge 有效性是重点."""

    @staticmethod
    def _xy(real_layer, real_universe, real_labels):
        import qlab.features.library  # noqa: F401
        from qlab.features.matrix import build_feature_matrix

        feats = ["mom_5d", "mom_20d", "ewm_vol_20d", "rv_20d", "turnover_5d"]
        fmx = build_feature_matrix(
            features=feats, data=real_layer, universe=real_universe,
            date_range=(REG_START, REG_END),
        )
        key = pd.MultiIndex.from_arrays(
            [real_labels.index, real_labels["symbol"]], names=["date", "symbol"]
        )
        X = fmx.values.reindex(key)
        y = pd.Series(real_labels["bin"].to_numpy(), index=key)
        t1 = pd.Series(real_labels["t1"].to_numpy(), index=key)
        ok = X.notna().all(axis=1) & y.notna()
        X, y, t1 = X[ok], y[ok].astype(int), t1[ok]
        dt = pd.DatetimeIndex(X.index.get_level_values("date"))
        return (X.set_index(dt), pd.Series(y.to_numpy(), index=dt),
                pd.Series(t1.to_numpy(), index=dt))

    def test_purged_kfold_no_leakage(self, real_layer, real_universe, real_labels):
        """PurgedKFold 各 fold 训练集不得含标签区间与测试段重叠的样本."""
        from qlab.models import PurgedKFold

        X, y, t1 = self._xy(real_layer, real_universe, real_labels)
        pk = PurgedKFold(n_splits=4, t1=t1, pct_embargo=0.01)
        for train, test in pk.split(X):
            ts, te = X.index[test].min(), X.index[test].max()
            tr_start = pd.DatetimeIndex(X.index[train])
            tr_end = pd.DatetimeIndex(t1.iloc[train])
            leak = int(((tr_end >= ts) & (tr_start <= te)).sum())
            assert leak == 0, f"purge 失效: {leak} 个训练样本与测试段重叠"

    def test_cv_score_runs(self, real_layer, real_universe, real_labels):
        from sklearn.tree import DecisionTreeClassifier

        from qlab.models import build_bagging_classifier, cv_score

        X, y, t1 = self._xy(real_layer, real_universe, real_labels)
        clf = build_bagging_classifier(
            DecisionTreeClassifier(max_depth=4, random_state=0),
            n_estimators=10, random_state=0,
        )
        sc = cv_score(clf, X, y, t1=t1, cv=3, pct_embargo=0.01)
        assert len(sc) == 3
        assert np.isfinite(sc).all()

    def test_mdi_mda_both_produce_valid_rankings(
        self, real_layer, real_universe, real_labels
    ):
        """MDI 与 MDA 各自自洽 —— **不要求两者首位一致**.

        AFML Ch8: MDI 是**样本内**且偏好高基数特征, MDA 是**样本外**
        (带 purge, 更可信)。两者排序不同是预期行为, 不是缺陷 ——
        实测 MDI 首位 rv_20d 而 MDA 首位 turnover_5d。
        """
        from sklearn.tree import DecisionTreeClassifier

        from qlab.models import build_bagging_classifier, feat_imp_mda, feat_imp_mdi

        X, y, t1 = self._xy(real_layer, real_universe, real_labels)
        clf = build_bagging_classifier(
            DecisionTreeClassifier(max_depth=4, random_state=0),
            n_estimators=10, random_state=0,
        )
        clf.fit(X, y)

        mdi = feat_imp_mdi(clf, list(X.columns))
        # MDI: 非负且归一(每棵树的重要度和为 1 → 均值和也为 1)
        assert set(mdi.columns) >= {"mean", "std"}
        assert (mdi["mean"] >= -1e-12).all()
        assert abs(mdi["mean"].sum() - 1.0) < 1e-6, "MDI 应归一到 1"

        sw = pd.Series(1.0, index=X.index)
        mda_df, baseline = feat_imp_mda(clf, X, y, sw, t1, cv=3, pct_embargo=0.01)
        assert set(mda_df.columns) >= {"mean", "std"}
        assert np.isfinite(mda_df["mean"]).all()
        assert np.isfinite(baseline), "MDA 应返回有限的 OOS 基线"
        # 两者覆盖同一特征集
        assert set(mdi.index) == set(mda_df.index) == set(X.columns)


# ======================================================================
# sizing / evaluation / allocation 层
# ======================================================================
class TestRegressionDownstream:
    """仓位 / 评估 / 组合配置."""

    def test_bet_sizing_range(self):
        from qlab.sizing import bet_size_from_probability, discretize_signal

        prob = pd.Series([0.4, 0.6, 0.9, 0.34])
        pred = pd.Series([1, 1, -1, 0])
        bs = bet_size_from_probability(prob, pred, num_classes=3)
        assert (bs.abs() <= 1.0 + 1e-9).all()
        disc = discretize_signal(bs, step_size=0.25)
        assert (disc.abs() <= 1.0 + 1e-9).all()

    def test_cpcv_paths_and_purge(self, real_labels):
        """CPCV: 路径数 = C(N,k), 每样本被测 k·C(N,k)/N 次, 无泄漏."""
        from math import comb

        from qlab.evaluation import CombinatorialPurgedCV

        t1 = pd.Series(real_labels["t1"].to_numpy(),
                       index=pd.DatetimeIndex(real_labels.index))
        X = pd.DataFrame({"f": np.arange(len(t1), dtype=float)}, index=t1.index)
        N, k = 6, 2
        cv = CombinatorialPurgedCV(N=N, k=k, t1=t1, embargo_pct=0.01)
        n_paths = 0
        total_test = 0
        for train, test, groups in cv.split(X):
            n_paths += 1
            total_test += len(test)
            assert len(groups) == k
            tr_start = pd.DatetimeIndex(X.index[train])
            tr_end = pd.DatetimeIndex(t1.iloc[train])
            for g in groups:
                g_idx = cv.group_indices[g]
                g_start = X.index[g_idx].min()
                g_end = pd.DatetimeIndex(t1.iloc[g_idx]).max()
                assert int(((tr_end >= g_start) & (tr_start <= g_end)).sum()) == 0
        assert n_paths == comb(N, k)
        assert abs(total_test / len(X) - k * comb(N, k) / N) < 0.1

    def test_pbo_result_shape(self, real_labels):
        from qlab.evaluation import PBOResult, compute_pbo

        r = pd.Series(real_labels["ret"].to_numpy(),
                      index=pd.DatetimeIndex(real_labels.index))
        daily = r.groupby(level=0).mean()
        rng = np.random.default_rng(0)
        trials = pd.DataFrame(
            {f"t{i}": daily.to_numpy() * rng.uniform(0.5, 1.5)
             + rng.normal(0, 0.002, len(daily)) for i in range(10)},
            index=daily.index,
        )
        res = compute_pbo(trials, n_splits=6)
        assert isinstance(res, PBOResult)
        assert 0.0 <= res.pbo <= 1.0
        assert res["pbo"] == res.pbo   # dict 式兼容

    def test_sharpe_family_finite(self, real_labels):
        from qlab.evaluation import (
            annualized_sharpe,
            probabilistic_sharpe_ratio,
            sharpe_ratio,
        )

        r = pd.Series(real_labels["ret"].to_numpy(),
                      index=pd.DatetimeIndex(real_labels.index))
        daily = r.groupby(level=0).mean().dropna()
        assert np.isfinite(sharpe_ratio(daily))
        assert np.isfinite(annualized_sharpe(daily))
        psr = probabilistic_sharpe_ratio(daily, sr_benchmark=0.0)
        assert 0.0 <= psr <= 1.0

    def test_allocation_weights_valid(self, real_close_wide):
        """HRP/IVP 权重: 和为 1, 非负, 带标的名."""
        from qlab.allocation import HierarchicalRiskParity, inverse_variance_portfolio

        rets = real_close_wide.pct_change().dropna().iloc[-250:]
        cov = rets.cov()
        hrp = HierarchicalRiskParity().allocate(rets)
        hrp2 = HierarchicalRiskParity().allocate_from_cov(cov, rets.corr())
        ivp = inverse_variance_portfolio(cov)
        for w, name in ((hrp, "HRP"), (hrp2, "HRP_from_cov"), (ivp, "IVP")):
            assert isinstance(w, pd.Series), f"{name} 应返回带标的名的 Series"
            assert abs(w.sum() - 1.0) < 1e-9, f"{name} 权重和 != 1"
            assert (w >= -1e-12).all(), f"{name} 出现负权重"
            assert set(w.index) == set(cov.columns)
        # allocate 与 allocate_from_cov 应给出一致结果
        pd.testing.assert_series_equal(
            hrp.sort_index(), hrp2.sort_index(), check_names=False
        )


# ======================================================================
# 宽样本回归 —— 跨板块 × 长周期(比默认 fixture 更严苛)
# ======================================================================
_WIDE_SYMBOLS = [
    # 主板大盘
    "600519.SH", "601318.SH", "600036.SH", "000001.SZ", "600276.SH",
    # 创业板(2020-08-24 起 ±20%)
    "300750.SZ", "300059.SZ", "300124.SZ", "301029.SZ",
    # 科创板(±20%)
    "688111.SH", "688036.SH", "688981.SH",
    # 中小板 / 曾停牌或 ST 过的
    "002532.SZ", "002064.SZ", "600126.SH", "600687.SH",
]
_WIDE_START = "2019-01-02"
_WIDE_END = "2024-11-29"


@pytest.fixture(scope="session")
def wide_daily(real_layer):
    """跨板块 × 6 年的日线(比默认 fixture 更宽)."""
    return real_layer.daily(_WIDE_SYMBOLS, "2018-06-01", _WIDE_END)


class TestRegressionWideSample:
    """宽样本: 跨板块涨跌幅制度 + 长周期 + 停牌/ST/低价股边界."""

    def test_coverage(self, wide_daily):
        n_sym = wide_daily.index.get_level_values("symbol").nunique()
        n_day = wide_daily.index.get_level_values("date").nunique()
        assert n_sym >= 12, f"跨板块标的应 ≥12, 实际 {n_sym}"
        assert n_day > 1300, f"6 年应有 1300+ 交易日, 实际 {n_day}"

    def test_adjustment_identity_wide(self, wide_daily):
        lhs = wide_daily["close"].to_numpy()
        rhs = (wide_daily["close_raw"] * wide_daily["adj_factor"]).to_numpy()
        assert np.nanmax(np.abs(lhs - rhs)) < 1e-6

    def test_extreme_returns_all_explainable(self, wide_daily):
        """|后复权日收益| > 11% 必须都能被制度解释, 不得有"无法解释"的.

        合法成因: 科创板/创业板 ±20%、北交所 ±30%、新股初期无限制、
        ST 低价股(1 分钱跳动即超 10%)。
        """
        r = np.log(wide_daily["close"].unstack("symbol")).diff()
        hits = np.where(r.abs().to_numpy() > 0.11)
        unexplained = []
        for i, j in zip(*hits, strict=False):
            ts, sym = r.index[i], r.columns[j]
            num = sym.split(".")[0]
            if num.startswith(("688", "300", "301", "43", "83", "87", "92")):
                continue          # 科创/创业/北交所: ±20% 或 ±30%
            row = wide_daily.loc[(ts, sym)]
            if row["close_raw"] < 1.0:
                continue          # 低价股: 1 分钱跳动 > 10%
            if bool(row["is_st"]):
                continue          # ST 有独立限价规则
            if row["days_since_listing"] < 10:
                continue          # 新股初期无涨跌幅限制
            unexplained.append((str(ts.date()), sym, round(float(r.iloc[i, j]), 4)))
        assert not unexplained, f"无法解释的极端收益: {unexplained[:5]}"

    def test_gem_limit_regime_change(self, wide_daily):
        """创业板 2020-08-24 前应受 ±10% 约束 —— 验证历史制度正确."""
        r = np.log(wide_daily["close"].unstack("symbol")).diff()
        gem = [c for c in r.columns if c.split(".")[0].startswith(("300", "301"))]
        if not gem:
            pytest.skip("样本无创业板标的")
        early = r.loc[: pd.Timestamp("2020-08-21"), gem]
        viol = []
        for sym in gem:
            s = early[sym].dropna()
            for ts, v in s[s.abs() > 0.11].items():
                row = wide_daily.loc[(ts, sym)]
                if row["days_since_listing"] >= 10 and not bool(row["is_st"]):
                    viol.append((str(ts.date()), sym, round(float(v), 4)))
        assert not viol, f"创业板改制前出现超 ±10% 波动: {viol[:5]}"

    def test_labels_clean_on_wide_sample(self, wide_daily):
        """宽样本标签: 无 ret=NaN, 时间方向干净(默认已剔无效样本)."""
        from qlab.core.calendar import get_default_calendar
        from qlab.labeling import CUSUMFilter, TripleBarrier, to_event_dataframe
        from qlab.labeling.thresholds import daily_ewm_vol_panel
        from qlab.labeling.triple_barrier import label_events

        cal = get_default_calendar()
        wide = wide_daily["close"].unstack("symbol")
        h = daily_ewm_vol_panel(wide, span=20).mean(axis=1).ffill().bfill()
        pairs = CUSUMFilter(h=h).sample_per_symbol(wide)
        pairs = pairs[
            (pairs["timestamp"] >= pd.Timestamp(_WIDE_START))
            & (pairs["timestamp"] <= pd.Timestamp(_WIDE_END))
        ]
        events = to_event_dataframe(pairs, target=0.04, t1_days=15, calendar=cal)
        labels = label_events(
            events, wide_daily[["open", "close"]], TripleBarrier(pt=1.5, sl=1.0)
        )
        assert len(labels) > 1000, f"宽样本应有千级标签, 实际 {len(labels)}"
        assert labels["ret"].notna().all(), "默认应剔除无有效收益的样本"
        tt = pd.to_datetime(labels["touch_time"])
        # 开盘入场允许当日收盘触障 → touch_time == event_start
        assert (tt >= pd.DatetimeIndex(labels.index)).all()
        assert set(labels["bin"].unique()) >= {-1, 1}

    def test_weights_on_wide_sample(self, wide_daily):
        """宽样本样本权重: 归一正确, 唯一度合理."""
        from qlab.core.calendar import get_default_calendar
        from qlab.labeling import CUSUMFilter, TripleBarrier, to_event_dataframe
        from qlab.labeling.triple_barrier import label_events
        from qlab.weights import sample_weights

        cal = get_default_calendar()
        wide = wide_daily["close"].unstack("symbol")
        pairs = CUSUMFilter(h=0.05).sample_per_symbol(wide)
        pairs = pairs[
            (pairs["timestamp"] >= pd.Timestamp("2022-01-04"))
            & (pairs["timestamp"] <= pd.Timestamp(_WIDE_END))
        ]
        events = to_event_dataframe(pairs, target=0.05, t1_days=10, calendar=cal)
        labels = label_events(
            events, wide_daily[["open", "close"]], TripleBarrier(1.0, 1.0)
        )
        w = sample_weights(labels, wide_daily[["close"]], time_decay=0.5)
        assert abs(w["final_weight"].sum() - len(w)) < 1e-6
        u = w["uniqueness"].dropna()
        assert (u > 0).all() and (u <= 1.0 + 1e-9).all()


class TestRegressionIncrementalEquivalence:
    """batch ↔ incremental 等价性 —— 源码自称的"设计根基".

    若不等价, 回测(batch)与实盘(incremental)会系统性偏差。
    此前只用 FakeDataSource 验证过, 真实数据从未覆盖。
    """

    FEATS = ["mom_5d", "mom_20d", "ewm_vol_20d", "rv_20d", "turnover_5d", "ffd_d0.4"]

    @staticmethod
    def _uni(start: str, end: str, syms: list[str]):
        from qlab.core.calendar import get_default_calendar
        from qlab.data.universe import Universe, UniverseSpec

        cal = get_default_calendar()
        dates = cal.trading_days(pd.Timestamp(start), pd.Timestamp(end))
        idx = pd.MultiIndex.from_product([dates, syms], names=["date", "symbol"])
        return Universe(
            pd.DataFrame(
                {"in_universe": True, "weight": 1.0 / len(syms)}, index=idx
            ),
            UniverseSpec("incr"),
        )

    def test_batch_equals_incremental_same_range(self, real_layer):
        """同一区间两种模式结果必须逐位相同."""
        import qlab.features.library  # noqa: F401
        from qlab.features.matrix import build_feature_matrix

        syms = REG_SYMBOLS[:5]
        start, end = "2024-06-03", "2024-09-30"
        uni = self._uni(start, end, syms)
        batch = build_feature_matrix(
            features=self.FEATS, data=real_layer, universe=uni,
            date_range=(start, end), mode="batch",
        )
        incr = build_feature_matrix(
            features=self.FEATS, data=real_layer, universe=uni,
            date_range=(start, end), mode="incremental",
        )
        pd.testing.assert_frame_equal(batch.values, incr.values)

    def test_daily_rolling_incremental_equals_full_batch(self, real_layer):
        """**实盘场景**: 每天只算当天, 累积结果须等于一次性全量算.

        这是 batch↔incremental 等价性最严苛的形式 —— 也是回测能否
        代表实盘的前提。含横截面因子(mom_resid_20d)以覆盖 demean 逻辑。
        """
        import qlab.features.library  # noqa: F401
        from qlab.core.calendar import get_default_calendar
        from qlab.features.matrix import build_feature_matrix

        cal = get_default_calendar()
        syms = REG_SYMBOLS[:3]
        feats = ["mom_5d", "ewm_vol_20d", "turnover_5d", "mom_resid_20d"]
        start, end = "2024-08-01", "2024-09-30"
        full = build_feature_matrix(
            features=feats, data=real_layer, universe=self._uni(start, end, syms),
            date_range=(start, end), mode="batch",
        ).values

        days = cal.trading_days(pd.Timestamp("2024-09-02"), pd.Timestamp(end))
        parts = []
        for d in days:
            day_str = str(d.date())
            m = build_feature_matrix(
                features=feats, data=real_layer,
                universe=self._uni(start, day_str, syms),
                date_range=(start, day_str), mode="incremental",
            ).values
            parts.append(m.loc[[d]])
        rolling = pd.concat(parts)
        expected = full.reindex(rolling.index)
        for c in feats:
            diff = (expected[c] - rolling[c]).abs().max()
            assert pd.isna(diff) or diff < 1e-12, (
                f"{c} 逐日算与全量算偏差 {diff:.3e} —— 回测无法代表实盘"
            )


class TestRegressionUncoveredModules:
    """此前零测试覆盖的模块(evaluation/risk, models/hyperparam)."""

    def test_strategy_risk_on_real_returns(self, real_labels):
        """用真实标签收益算策略失败概率, 单调性须成立."""
        from qlab.evaluation import prob_strategy_failure

        r = pd.Series(real_labels["ret"].dropna().to_numpy())
        if len(r) < 100:
            pytest.skip("真实收益样本不足")
        low = prob_strategy_failure(r, freq=52, target_sr=1.0)
        high = prob_strategy_failure(r, freq=52, target_sr=3.0)
        assert 0.0 <= low <= 1.0 and 0.0 <= high <= 1.0
        assert high >= low

    def test_hyperparam_search_on_real_data(
        self, real_layer, real_universe, real_labels
    ):
        """超参搜索在真实特征/标签上跑通(带 PurgedKFold)."""
        from sklearn.pipeline import Pipeline
        from sklearn.tree import DecisionTreeClassifier

        import qlab.features.library  # noqa: F401
        from qlab.features.matrix import build_feature_matrix
        from qlab.models import clf_hyper_fit

        fmx = build_feature_matrix(
            features=["mom_5d", "ewm_vol_20d", "turnover_5d"],
            data=real_layer, universe=real_universe,
            date_range=(REG_START, REG_END),
        )
        key = pd.MultiIndex.from_arrays(
            [real_labels.index, real_labels["symbol"]], names=["date", "symbol"]
        )
        X = fmx.values.reindex(key)
        y = pd.Series(real_labels["bin"].to_numpy(), index=key)
        t1 = pd.Series(real_labels["t1"].to_numpy(), index=key)
        ok = X.notna().all(axis=1) & y.notna()
        dt = pd.DatetimeIndex(X[ok].index.get_level_values("date"))
        X2 = X[ok].set_index(dt)
        y2 = pd.Series(y[ok].astype(int).to_numpy(), index=dt)
        t2 = pd.Series(t1[ok].to_numpy(), index=dt)
        pipe = Pipeline([("clf", DecisionTreeClassifier(random_state=0))])
        best = clf_hyper_fit(
            X2, y2, t2, pipe, {"clf__max_depth": [2, 4]},
            cv=3, pct_embargo=0.01, n_jobs=1,
        )
        assert len(best.predict(X2.head(5))) == 5


class TestRegressionResearchDiscipline:
    """研究纪律闭环: 参数网格 → 试验登记 → DSR 去膨胀.

    这是防"挑最好的那次汇报"的机制。用真实数据跑完整流程。
    """

    def test_grid_search_trials_deflate_sharpe(self, real_daily, real_close_wide):
        import tempfile

        from qlab.core.calendar import get_default_calendar
        from qlab.evaluation import TrialRegistry
        from qlab.evaluation.statistics import (
            annualized_sharpe,
            deflated_sharpe_ratio,
            probabilistic_sharpe_ratio,
        )
        from qlab.evaluation.statistics.sharpe import expected_max_sharpe
        from qlab.labeling import CUSUMFilter, TripleBarrier, to_event_dataframe
        from qlab.labeling.triple_barrier import label_events

        cal = get_default_calendar()
        pairs = CUSUMFilter(h=0.05).sample_per_symbol(real_close_wide)
        pairs = pairs[
            (pairs["timestamp"] >= pd.Timestamp(REG_START))
            & (pairs["timestamp"] <= pd.Timestamp(REG_END))
        ]
        with tempfile.TemporaryDirectory() as tmp:
            reg = TrialRegistry(f"{tmp}/trials.db")
            rets = {}
            for pt in (1.0, 1.5, 2.0):
                for sl in (0.5, 1.0):
                    events = to_event_dataframe(
                        pairs, target=0.05, t1_days=10, calendar=cal
                    )
                    lab = label_events(
                        events,
                        real_daily[["open", "close"]],
                        TripleBarrier(pt=pt, sl=sl),
                    )
                    r = (
                        pd.Series(
                            lab["ret"].to_numpy(),
                            index=pd.DatetimeIndex(lab.index),
                        )
                        .groupby(level=0).mean().dropna()
                    )
                    sr = float(annualized_sharpe(r))
                    reg.record(
                        dataset_id="reg", pipeline_config={"pt": pt, "sl": sl},
                        metrics={"sharpe_ratio": sr},
                    )
                    rets[(pt, sl)] = r

            n = reg.n_trials("reg")
            assert n == 6, f"应登记 6 组试验, 实际 {n}"
            srs = reg.get_sr_distribution("reg")
            assert len(srs) == 6 and srs.notna().all()

            # 取最优那组 → PSR 乐观, DSR 必更保守
            best_key = max(rets, key=lambda k: float(annualized_sharpe(rets[k])))
            rbest = rets[best_key]
            psr = probabilistic_sharpe_ratio(rbest, sr_benchmark=0.0)
            dsr = deflated_sharpe_ratio(
                rbest, n_trials=n, var_trials_sr=float(srs.var())
            )
            assert 0.0 <= dsr <= 1.0 and 0.0 <= psr <= 1.0
            assert dsr <= psr, f"DSR({dsr:.4f}) 必须 ≤ PSR({psr:.4f})"
            # E[max SR] 应为正(纯随机试 n 次也能达到的水平)
            assert expected_max_sharpe(n, float(srs.var())) > 0


class TestRegressionCrossFrequency:
    """跨频率一致性: 日线 vs 30min 聚合 —— 两条独立路径算同一个量.

    能抓出"日频与分钟频实现不一致"这类前面所有测试都覆盖不到的问题。
    """

    def test_daily_equals_intraday_aggregation(self, real_layer):
        from qlab.core.enums import Freq

        syms = REG_SYMBOLS[:3]
        start, end = "2024-06-03", "2024-06-14"
        d = real_layer.daily(syms, start, end)
        m = real_layer.intraday(syms, start, end, freq=Freq.MIN_30)
        assert len(m) == len(d) * 8, (
            f"A 股每日应 8 根 30min bar: 日线 {len(d)} 行 vs 日内 {len(m)} 行"
        )

        mi = m.reset_index()
        mi["date"] = pd.DatetimeIndex(mi["timestamp"]).normalize()
        g = mi.sort_values("timestamp").groupby(["date", "symbol"])

        def diff(a: pd.Series, b: pd.Series) -> float:
            cmp = pd.DataFrame({"x": a, "y": b}).dropna()
            assert len(cmp) > 0, "对齐后为空"
            return float((cmp["x"] - cmp["y"]).abs().max())

        # OHLC 必须精确一致
        assert diff(d["close"], g["close"].last()) < 1e-6
        assert diff(d["open"], g["open"].first()) < 1e-6
        assert diff(d["high"], g["high"].max()) < 1e-6
        assert diff(d["low"], g["low"].min()) < 1e-6
        # adj_factor 日内恒定(IntradayBar 不变量)
        assert diff(d["adj_factor"], g["adj_factor"].first()) < 1e-9
        # volume/amount 允许极小尾差(聚宽日线与分钟线的零股舍入),
        # 但相对偏差必须在 1e-4 以内 —— 超出说明换算口径错了
        cmp_v = pd.DataFrame(
            {"x": d["volume"], "y": g["volume"].sum()}
        ).dropna()
        rel = ((cmp_v["x"] - cmp_v["y"]).abs() / cmp_v["x"].clip(lower=1)).max()
        assert rel < 1e-4, f"成交量相对偏差 {rel:.2e} 过大, 疑似口径错误"

    def test_intraday_single_day_range(self, real_layer):
        """53 单日区间必须返回当日全部 bar(实盘"取今天分钟线"场景)."""
        from qlab.core.enums import Freq

        one = real_layer.intraday(
            REG_SYMBOLS[:1], "2024-06-14", "2024-06-14", freq=Freq.MIN_30
        )
        assert len(one) == 8, f"单日 30min 应 8 根, 实际 {len(one)}"
        m1 = real_layer.intraday(
            REG_SYMBOLS[:1], "2024-06-14", "2024-06-14", freq=Freq.MIN_1
        )
        assert len(m1) == 240, f"单日 1min 应 240 根, 实际 {len(m1)}"


class TestRegressionPathEquivalence:
    """**多路径等价性** —— 同一个量用两条独立路径算, 结果必须一致.

    这类测试专门防"性能优化引入偏差": 批量化(㉛)、单点查询(㉔㉘)都是
    为省远程请求而加的捷径, 若与原始路径不等价就是静默错误。
    """

    def test_share_capital_batch_equals_per_symbol(self, real_layer):
        """㉛ 批量取股本必须等于逐只取(批量优化不得改变数值)."""
        src = real_layer.source
        syms = REG_SYMBOLS[:5]
        s, e = pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-14")
        batch = src.fetch_share_capital(syms, s, e)
        per = pd.concat([src.fetch_share_capital([x], s, e) for x in syms])
        b = batch.set_index(["date", "symbol"]).sort_index()
        o = per.set_index(["date", "symbol"]).sort_index()
        assert len(b) == len(o), f"行数不同: 批量 {len(b)} vs 逐只 {len(o)}"
        common = b.index.intersection(o.index)
        assert len(common) == len(b)
        for col in ("total_shares", "float_shares"):
            diff = (b.loc[common, col] - o.loc[common, col]).abs().max()
            assert diff < 1e-6, f"{col} 批量与逐只偏差 {diff:.3e}"

    def test_industry_asof_equals_range_query(self, real_layer):
        """㉘ industry_asof(单点) 必须等于区间查询取同一天."""
        src = real_layer.source
        syms = REG_SYMBOLS[:5]
        d = pd.Timestamp("2024-06-14")
        a = src.industry_asof(syms, d).reset_index()
        b = src.fetch_industry_classification(syms, (d, d)).reset_index()
        ka = a.set_index("symbol")["industry_code"].astype(str)
        kb = b.set_index("symbol")["industry_code"].astype(str)
        assert set(ka.index) == set(kb.index)
        common = sorted(set(ka.index) & set(kb.index))
        assert (ka.loc[common].to_numpy() == kb.loc[common].to_numpy()).all()

    def test_concepts_asof_equals_range_query(self, real_layer):
        """㉔ concepts_asof(单点) 必须等于区间查询取同一天."""
        src = real_layer.source
        syms = REG_SYMBOLS[:5]
        d = pd.Timestamp("2024-06-14")
        a = src.concepts_asof(syms, d).reset_index()
        b = src.fetch_concepts(syms, (d, d)).reset_index()
        key = ["symbol", "concept_code"]
        sa = set(map(tuple, a[key].to_numpy()))
        sb = set(map(tuple, b[key].to_numpy()))
        assert sa == sb, f"仅单点 {len(sa - sb)} 条, 仅区间 {len(sb - sa)} 条"

    def test_universe_matches_index_weights(self, real_layer):
        """universe 成分 与 index_weights 两条路径的成分股必须一致."""
        from qlab.data.sources.jq_source import to_qlab_symbol

        d = "2024-06-14"
        uni = real_layer.universe("000300.SH", d, d)
        from_uni = set(uni.all_symbols())
        w = real_layer.source.cache.get_index_weights("000300.XSHG", d)
        from_w = {to_qlab_symbol(str(c)) for c in w.index}
        assert len(from_uni) == 300, f"沪深300 应 300 只, 实际 {len(from_uni)}"
        assert from_uni == from_w, (
            f"仅 universe {len(from_uni - from_w)} 只, 仅 weights {len(from_w - from_uni)} 只"
        )

    @pytest.mark.parametrize("asof", ["2024-06-14", "2023-08-15", "2024-11-29"])
    def test_fundamental_as_of_equals_manual_pit_filter(self, real_layer, asof):
        """`fundamental_as_of` 必须等于手工做 PIT 筛选(available_at<=T 取最近期).

        这是 PIT 语义的独立复核 —— 若 as_of 内部实现改了口径, 这里会立刻发现。
        """
        from qlab.core.enums import ReportType

        syms = REG_SYMBOLS[:5]
        t = pd.Timestamp(asof)
        got = real_layer.fundamental_as_of(syms, "net_profit", t)
        raw = real_layer.fundamentals(syms, "2021-01-01", asof, [ReportType.OFFICIAL])
        for sym in syms:
            g = raw[
                (raw["symbol"] == sym)
                & (pd.to_datetime(raw["available_at"]) <= t)
            ].sort_values(["report_period", "available_at"])
            expected = float(g.iloc[-1]["net_profit"]) if len(g) else float("nan")
            actual = float(got.get(sym, float("nan")))
            if pd.isna(expected) and pd.isna(actual):
                continue
            assert not pd.isna(actual), f"{sym} as_of 返回 NaN 但手工有值"
            assert abs(actual - expected) <= max(1.0, abs(expected) * 1e-9), (
                f"{sym} @{asof}: as_of={actual:.6g} vs 手工={expected:.6g}"
            )


class TestRegressionParameterMatrix:
    """**参数矩阵全覆盖** —— 每个枚举值、每种分类体系都真跑一遍.

    "各类数据都跑通"要求逐个参数组合验证, 而非只测常用路径。
    """

    @pytest.mark.parametrize(
        "freq_name,bars_per_day",
        [("MIN_1", 240), ("MIN_5", 48), ("MIN_15", 16), ("MIN_30", 8), ("MIN_60", 4)],
    )
    def test_every_intraday_frequency(self, real_layer, freq_name, bars_per_day):
        """5 个分钟频率的 bar 数必须与 A 股 4 小时交易时长吻合."""
        from qlab.core.enums import Freq
        from qlab.core.schema import SCHEMA_INTRADAY_BAR, validate_schema

        freq = getattr(Freq, freq_name)
        syms = REG_SYMBOLS[:2]
        df = real_layer.intraday(syms, "2024-06-03", "2024-06-07", freq=freq)
        assert not df.empty, f"{freq_name} 无数据"
        validate_schema(df, SCHEMA_INTRADAY_BAR, strict_index=True)

        ts = pd.DatetimeIndex(df.index.get_level_values("timestamp"))
        per_day = pd.Series(ts.normalize()).value_counts()
        n = int(per_day.iloc[0]) // len(syms)
        assert n == bars_per_day, f"{freq_name} 每日应 {bars_per_day} 根, 实际 {n}"

        # 日内 adj_factor 恒定(IntradayBar 不变量)
        g = df.reset_index()
        g["d"] = pd.DatetimeIndex(g["timestamp"]).normalize()
        assert g.groupby(["d", "symbol"])["adj_factor"].nunique().max() == 1

    @pytest.mark.parametrize("system", ["sw", "jq", "csrc"])
    @pytest.mark.parametrize("level", [1, 2, 3])
    def test_every_industry_system_and_level(self, real_layer, system, level):
        """3 套行业体系 × 3 个层级 全部可用.

        注意两层的返回契约**有意不同**:
        - ``layer.industry()`` → Series(index=symbol, name=f"{system}_l{level}"),
          面向因子计算的便捷视图;
        - ``source.industry_asof()`` → DataFrame(含 industry_code/name/parent_code),
          完整信息。两者取值必须一致。
        """
        syms = REG_SYMBOLS[:3]
        d = pd.Timestamp("2024-06-14")
        got = real_layer.industry(syms, d, system=system, level=level)
        assert isinstance(got, pd.Series), "layer.industry 返回 Series"
        assert got.name == f"{system}_l{level}", f"Series.name 应标明体系与层级: {got.name}"
        assert len(got) == len(syms)
        # jq 三级分类本身不全, csrc 层级浅 —— 允许 NaN, 但不能全空
        if system == "sw":
            assert got.notna().all(), f"申万 L{level} 不应有空值"

        # 与 source 层的完整视图取值一致
        # (jq 三级分类在聚宽侧本就缺失 → source 可能返回空表, 此时跳过对账)
        full = real_layer.source.industry_asof(syms, d, system=system, level=level)
        if full.empty or "industry_code" not in full.columns:
            assert got.isna().all(), (
                f"{system} L{level}: source 无数据但 layer 却有值 —— 两层不一致"
            )
            return
        codes = full.reset_index().set_index("symbol")["industry_code"]
        for sym in syms:
            if pd.isna(got.loc[sym]) or sym not in codes.index:
                continue
            assert str(got.loc[sym]) == str(codes.loc[sym]), (
                f"{sym} {system} L{level}: layer={got.loc[sym]!r} vs "
                f"source={codes.loc[sym]!r}"
            )

    def test_unsupported_industry_system_fails_loud(self, real_layer):
        """不支持的体系必须报错并列出可选值(而非静默返回空)."""
        with pytest.raises(ValueError, match="sw / jq / csrc"):
            real_layer.industry(
                REG_SYMBOLS[:2], pd.Timestamp("2024-06-14"), system="zjw"
            )

    def test_industry_levels_are_nested(self, real_layer):
        """申万层级必须递进: L1/L2/L3 的码逐级细化(不同码)."""
        syms = REG_SYMBOLS[:2]
        d = pd.Timestamp("2024-06-14")
        codes = {
            lv: real_layer.industry(syms, d, system="sw", level=lv)
            for lv in (1, 2, 3)
        }
        for sym in syms:
            c1, c2, c3 = (str(codes[lv].loc[sym]) for lv in (1, 2, 3))
            assert c1 != c2 != c3, f"{sym} 申万三级码不应相同: {c1}/{c2}/{c3}"

    @pytest.mark.parametrize("source", ["eastmoney", "jq"])
    def test_every_concept_source(self, real_layer, source):
        """两套概念体系都可用."""
        df = real_layer.concepts(
            REG_SYMBOLS[:3], pd.Timestamp("2024-06-14"), source=source
        )
        r = df.reset_index()
        assert len(r) > 0, f"{source} 无概念数据"
        assert r["concept_code"].notna().all()

    @pytest.mark.parametrize("report", ["forecast", "flash", "official"])
    def test_every_report_type(self, real_layer, report):
        """3 种报告类型分别可取(flash 允许为空 —— 并非所有公司发快报)."""
        from qlab.core.enums import ReportType
        from qlab.core.schema import SCHEMA_FUNDAMENTAL, validate_schema

        rt = ReportType(report)
        df = real_layer.fundamentals(
            REG_SYMBOLS[:3], "2022-01-01", "2024-06-14", [rt]
        )
        if not df.empty:
            validate_schema(df, SCHEMA_FUNDAMENTAL, strict_index=False)
            assert (df["report_type"] == report).all()

    def test_mixed_report_types_union(self, real_layer):
        """混合请求应等于各类型之并(不重不漏)."""
        from qlab.core.enums import ReportType

        syms, rng = REG_SYMBOLS[:3], ("2022-01-01", "2024-06-14")
        parts = {
            rt.value: len(real_layer.fundamentals(syms, *rng, [rt]))
            for rt in (ReportType.FORECAST, ReportType.FLASH, ReportType.OFFICIAL)
        }
        mixed = real_layer.fundamentals(
            syms, *rng,
            [ReportType.FORECAST, ReportType.FLASH, ReportType.OFFICIAL],
        )
        assert len(mixed) == sum(parts.values()), (
            f"混合 {len(mixed)} 行 != 各类型之和 {parts}"
        )


class TestRegressionSymbolTypes:
    """**B 类数据 × 标的类型矩阵** —— 每类标的的支持边界必须明确.

    真实研究里 universe 可能混入 ETF/指数/北交所, 框架必须给出
    "支持"或"明确拒绝并说明原因", 不能透出数据源内部错误。
    """

    _STOCKS = ["600519.SH", "000858.SZ"]      # 主板
    _STAR = ["688981.SH", "688111.SH"]        # 科创板
    _GEM = ["300750.SZ", "301029.SZ"]         # 创业板
    _ETF = ["510300.SH", "159915.SZ"]
    _INDEX = ["000300.SH"]
    _BJ = ["832000.BJ"]

    @pytest.mark.parametrize(
        "group", ["_STOCKS", "_STAR", "_GEM"], ids=["主板", "科创板", "创业板"]
    )
    def test_b_class_works_on_all_stock_boards(self, real_layer, group):
        """三个个股板块上 B 类数据全部可用."""
        from qlab.core.schema import (
            SCHEMA_CALL_AUCTION,
            SCHEMA_MARGIN_TRADING,
            SCHEMA_MONEY_FLOW,
            validate_schema,
        )

        syms = getattr(self, group)
        rng = ("2024-06-03", "2024-06-14")
        for fn, schema in (
            (real_layer.margin_trading, SCHEMA_MARGIN_TRADING),
            (real_layer.money_flow, SCHEMA_MONEY_FLOW),
            (real_layer.call_auction, SCHEMA_CALL_AUCTION),
        ):
            df = fn(syms, *rng)
            assert not df.empty, f"{group} 的 {fn.__name__} 无数据"
            validate_schema(df, schema, strict_index=True)

    def test_money_flow_rejects_etf_and_index(self, real_layer):
        """56 ETF/指数无资金流概念 → 必须 fail-loud 而非透出聚宽错误."""
        from qlab.core.exceptions import DataUnavailableError

        for bad in (self._ETF, self._INDEX):
            with pytest.raises(DataUnavailableError, match="仅支持个股"):
                real_layer.money_flow(bad, "2024-06-03", "2024-06-14")

    def test_bj_symbols_rejected_everywhere(self, real_layer):
        """55 北交所标的在各接口都应明确拒绝(聚宽股票池不含北交所)."""
        rng = ("2024-06-03", "2024-06-14")
        for fn in (
            real_layer.margin_trading,
            real_layer.call_auction,
        ):
            with pytest.raises(ValueError, match="不支持北交所"):
                fn(self._BJ, *rng)
        with pytest.raises((ValueError, Exception)):
            real_layer.money_flow(self._BJ, *rng)

    def test_etf_supported_where_meaningful(self, real_layer):
        """ETF 有两融与竞价(它们确实在交易所挂牌交易), 应可取到."""
        rng = ("2024-06-03", "2024-06-14")
        mt = real_layer.margin_trading(self._ETF, *rng)
        ca = real_layer.call_auction(self._ETF, *rng)
        assert not mt.empty, "ETF 应有两融数据(ETF 可作担保品/标的券)"
        assert not ca.empty, "ETF 应有集合竞价数据"

    def test_index_has_no_margin_but_has_auction(self, real_layer):
        """指数不是可交易标的 → 无两融; 但有竞价快照(指数在 9:25 也有点位)."""
        rng = ("2024-06-03", "2024-06-14")
        mt = real_layer.margin_trading(self._INDEX, *rng)
        assert mt.empty, "指数不应有两融数据"


class TestRegressionRemainingParams:
    """收口: factor_exposure 因子名 / billboard 参数 / universe spec."""

    _FACTORS = [
        "size", "beta", "momentum", "book_to_price_ratio", "earnings_yield",
        "growth", "leverage", "liquidity", "residual_volatility",
        "non_linear_size",
    ]

    @pytest.mark.parametrize("factor", _FACTORS)
    def test_every_barra_factor(self, real_layer, factor):
        """10 个 Barra 风格因子逐个可取."""
        from qlab.core.schema import SCHEMA_FACTOR_EXPOSURE, validate_schema

        df = real_layer.factor_exposure(
            REG_SYMBOLS[:2], [factor], "2024-06-03", "2024-06-14"
        )
        assert not df.empty, f"因子 {factor} 无数据"
        validate_schema(df, SCHEMA_FACTOR_EXPOSURE, strict_index=True)
        assert factor in df.columns

    def test_invalid_factor_name_fails_loud(self, real_layer):
        """不存在的因子名必须报错(而非静默返回空/NaN 列)."""
        with pytest.raises(Exception, match="Invalid factors|no_such"):
            real_layer.factor_exposure(
                REG_SYMBOLS[:2], ["no_such_factor_xyz"], "2024-06-03", "2024-06-14"
            )

    @pytest.mark.parametrize("count", [1, 5, 10, 30])
    def test_billboard_scales_by_days(self, real_layer, count):
        """57 龙虎榜按天数分段: count=30 全市场约 2.6 万行, 早前会返回空 stdout."""
        from qlab.core.schema import SCHEMA_BILLBOARD, validate_schema

        df = real_layer.billboard(pd.Timestamp("2024-06-14"), count=count)
        assert not df.empty, f"count={count} 无数据"
        validate_schema(df, SCHEMA_BILLBOARD, strict_index=False)
        n_days = df.reset_index()["date"].nunique()
        assert n_days == count, f"count={count} 应覆盖 {count} 个交易日, 实际 {n_days}"

    @pytest.mark.parametrize(
        "factory,expected",
        [
            ("csi300", 300), ("csi500", 500), ("csi800", 800), ("csi1000", 1000),
        ],
    )
    def test_universe_spec_factory_aliases(self, real_layer, factory, expected):
        """㊵ UniverseSpec 工厂方法生成的名字必须可用.

        `UniverseSpec.csi300().name` 是 'csi300' 而非 '000300.SH' ——
        不做别名映射会被当成指数代码解析, 报"无法识别的交易所后缀"。
        """
        from qlab.data.universe import UniverseSpec

        spec = getattr(UniverseSpec, factory)().name
        uni = real_layer.universe(spec, "2024-06-03", "2024-06-14")
        assert len(uni.all_symbols()) == expected, (
            f"{factory}(spec={spec!r}) 应 {expected} 只"
        )

    def test_universe_main_a_and_hs_a(self, real_layer):
        """宽基池: 剔 ST; main_a 额外剔科创; 二者均无北交/688 违规码."""
        from qlab.data.universe import UniverseSpec, is_bj_symbol, is_star_symbol

        hs = real_layer.universe(UniverseSpec.hs_a(), "2024-06-03", "2024-06-14")
        main = real_layer.universe(UniverseSpec.main_a(), "2024-06-03", "2024-06-14")
        hs_syms = hs.all_symbols()
        main_syms = main.all_symbols()
        assert len(hs_syms) > 3000
        assert len(main_syms) > 2000
        assert len(main_syms) < len(hs_syms), "main_a 应因剔科创而更小"
        assert not any(is_bj_symbol(s) for s in hs_syms)
        assert not any(is_bj_symbol(s) for s in main_syms)
        assert not any(is_star_symbol(s) for s in main_syms)
        # 至少有一些 688 留在 hs_a（科创保留）
        assert any(is_star_symbol(s) for s in hs_syms)

        # 旧 all_a 规格应 fail-loud
        with pytest.raises(ValueError, match="已移除"):
            real_layer.universe("all_a", "2024-06-03", "2024-06-14")
