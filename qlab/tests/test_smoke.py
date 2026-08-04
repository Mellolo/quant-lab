"""烟雾测试 — 验证各模块基本能跑通."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# ---- core ------------------------------------------------------------------

def test_core_imports():
    from qlab.core import (
        AdjustMode,
        Freq,
    )
    assert AdjustMode.BACKWARD == "backward"
    assert Freq.DAILY == "1d"


def test_calendar():
    from qlab.core.calendar import get_default_calendar
    cal = get_default_calendar()
    # 2024-01-02 是周二，应当是交易日
    assert cal.is_trading_day(pd.Timestamp("2024-01-02"))


def test_parallel_linear_partition():
    from qlab.core.parallel import linear_partitions
    parts = linear_partitions(100, 4)
    assert len(parts) == 5
    assert parts[0] == 0 and parts[-1] == 100


# ---- data ------------------------------------------------------------------

def test_fake_data_source():
    from qlab.data.sources import FakeDataSource
    src = FakeDataSource(seed=1, n_symbols=5)
    bars = src.fetch_bars(
        src.all_symbols[:2],
        pd.Timestamp("2023-01-01"), pd.Timestamp("2023-03-01"),
    )
    assert not bars.empty
    assert "close" in bars.columns
    assert "close_raw" in bars.columns
    assert "is_suspended" in bars.columns


def test_data_layer():
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    data = DataLayer(source=FakeDataSource(seed=1, n_symbols=5))
    universe = data.universe("csi500", "2023-01-01", "2023-03-01")
    assert len(universe.all_symbols()) > 0
    symbols = universe.all_symbols()[:2]
    df = data.daily(symbols, "2023-01-01", "2023-03-01", validate=False)
    assert not df.empty


# ---- features --------------------------------------------------------------

def test_feature_registry():
    from qlab.features import registry
    from qlab.features.library import momentum  # noqa: F401 - 触发注册
    assert registry.has("mom_5d")


def test_build_feature_matrix():
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    from qlab.features import build_feature_matrix
    from qlab.features.library import Momentum, RealizedVol

    data = DataLayer(source=FakeDataSource(seed=1, n_symbols=5))
    universe = data.universe("csi500", "2023-01-01", "2023-06-30")

    X = build_feature_matrix(
        features=[Momentum(5), RealizedVol(20)],
        data=data,
        universe=universe,
        date_range=("2023-03-01", "2023-06-30"),
    )
    assert "mom_5d" in X.feature_names
    assert "rv_20d" in X.feature_names


# ---- labeling --------------------------------------------------------------

def test_cusum_filter():
    from qlab.labeling import CUSUMFilter
    rng = np.random.default_rng(42)
    n = 100
    prices = pd.Series(100 + rng.normal(0, 1, n).cumsum(),
                       index=pd.date_range("2023-01-01", periods=n, freq="D"))
    sampler = CUSUMFilter(h=0.05)
    events = sampler.sample(prices)
    assert isinstance(events, pd.DatetimeIndex)


def test_volume_cusum_filter():
    from qlab.labeling import VolumeCUSUMFilter
    rng = np.random.default_rng(42)
    n = 100
    base_vol = 1_000_000
    # 前50天正常量，后50天放量
    volume = pd.Series(
        np.concatenate([
            rng.normal(base_vol, base_vol * 0.1, 50),
            rng.normal(base_vol * 5, base_vol * 0.5, 50),
        ]),
        index=pd.date_range("2023-01-01", periods=n, freq="D"),
    )
    sampler = VolumeCUSUMFilter(h=0.5)
    events = sampler.sample(volume)
    assert isinstance(events, pd.DatetimeIndex)
    assert len(events) > 0


def test_run_sampler():
    from qlab.labeling import RunSampler
    dates = pd.date_range("2023-01-01", periods=20, freq="D")
    # 前10天连涨，后10天随机
    prices = pd.Series(
        np.concatenate([np.arange(100, 110, 1.0), np.full(10, 110.0)]),
        index=dates,
    )
    sampler = RunSampler(min_run=5)
    events = sampler.sample(prices)
    assert isinstance(events, pd.DatetimeIndex)
    assert len(events) > 0
    # 触发点应在连涨期间
    assert all(e <= dates[9] for e in events)


def test_entropy_sampler():
    from qlab.labeling import EntropySampler
    rng = np.random.default_rng(0)
    n = 200
    # 前100天随机震荡，后100天单向趋势
    random_part = 100 + rng.normal(0, 1, 100).cumsum()
    trend_part = random_part[-1] + np.arange(100) * 0.5
    prices = pd.Series(
        np.concatenate([random_part, trend_part]),
        index=pd.date_range("2023-01-01", periods=n, freq="D"),
    )
    sampler = EntropySampler(window=20, n_bins=10, h=0.3)
    events = sampler.sample(prices)
    assert isinstance(events, pd.DatetimeIndex)


def test_triple_barrier():
    from qlab.labeling import TripleBarrier, label_events
    rng = np.random.default_rng(0)
    dates = pd.date_range("2023-01-01", periods=100, freq="D")
    close = pd.DataFrame({
        ("close",): 100 + rng.normal(0, 1, 100).cumsum(),
    }, index=pd.MultiIndex.from_product([dates, ["A"]], names=["date", "symbol"]))
    close.columns = ["close"]

    events = pd.DataFrame({
        "symbol": ["A"] * 3,
        "t1": [dates[20], dates[40], dates[60]],
        "target": [0.02, 0.02, 0.02],
    }, index=[dates[10], dates[30], dates[50]])

    labels = label_events(events, close, TripleBarrier(pt=2, sl=1))
    assert "bin" in labels.columns
    assert "ret" in labels.columns


# ---- weights ---------------------------------------------------------------

def test_uniqueness():
    from qlab.weights import average_uniqueness, num_concurrent_events
    dates = pd.date_range("2023-01-01", periods=20, freq="D")
    t1 = pd.Series([dates[5], dates[10], dates[15]],
                   index=[dates[0], dates[3], dates[8]])
    co = num_concurrent_events(dates, t1)
    assert (co >= 0).all()
    u = average_uniqueness(t1, co)
    assert (u.dropna() >= 0).all() and (u.dropna() <= 1).all()


# ---- models ----------------------------------------------------------------

def test_purged_kfold():
    from qlab.models import PurgedKFold
    dates = pd.date_range("2023-01-01", periods=20, freq="D")
    X = pd.DataFrame(np.random.randn(20, 3), index=dates)
    t1 = pd.Series(
        [d + pd.Timedelta(days=2) for d in dates],
        index=dates,
    )
    cv = PurgedKFold(n_splits=4, t1=t1, pct_embargo=0.05)
    splits = list(cv.split(X))
    assert len(splits) == 4
    for train, test in splits:
        assert len(test) > 0
        # 训练/测试不能交集
        assert len(set(train) & set(test)) == 0


def test_log_uniform():
    from qlab.models import log_uniform
    dist = log_uniform(0.01, 100)
    samples = dist.rvs(size=1000, random_state=42)
    assert samples.min() >= 0.01
    assert samples.max() <= 100


# ---- sizing ----------------------------------------------------------------

def test_bet_size_from_probability():
    from qlab.sizing import bet_size_from_probability
    sizes = bet_size_from_probability(
        prob=pd.Series([0.9, 0.6, 0.51]),
        pred=pd.Series([1, 1, 1]),
        num_classes=2,
    )
    assert all(-1 <= s <= 1 for s in sizes)
    assert sizes.iloc[0] > sizes.iloc[2]  # 高概率 → 大 size


def test_discretize_signal():
    from qlab.sizing import discretize_signal
    s = pd.Series([0.12, 0.47, -0.93])
    out = discretize_signal(s, step_size=0.1)
    assert out.iloc[0] == pytest.approx(0.1)
    assert out.iloc[1] == pytest.approx(0.5)
    assert out.iloc[2] == pytest.approx(-0.9)


# ---- evaluation ------------------------------------------------------------

def test_sharpe_family():
    from qlab.evaluation.statistics import (
        annualized_sharpe,
        deflated_sharpe_ratio,
        probabilistic_sharpe_ratio,
        sharpe_ratio,
    )
    rng = np.random.default_rng(0)
    returns = pd.Series(rng.normal(0.001, 0.01, 250))
    sr = sharpe_ratio(returns)
    assert isinstance(sr, float)
    assert isinstance(annualized_sharpe(returns), float)
    psr = probabilistic_sharpe_ratio(returns)
    dsr = deflated_sharpe_ratio(returns, n_trials=10, var_trials_sr=0.5)
    assert 0 <= psr <= 1
    assert 0 <= dsr <= 1


def test_compute_dd():
    from qlab.evaluation.statistics import compute_dd_tuw
    nav = pd.Series([1.0, 1.1, 1.05, 0.95, 1.0, 1.2],
                    index=pd.date_range("2023-01-01", periods=6, freq="D"))
    dd, tuw = compute_dd_tuw(nav)
    assert (dd >= 0).all() and (dd <= 1).all()


def test_pbo():
    from qlab.evaluation import compute_pbo
    rng = np.random.default_rng(42)
    perf = pd.DataFrame(rng.normal(0, 1, (200, 20)))
    result = compute_pbo(perf, n_splits=8)
    assert 0 <= result["pbo"] <= 1


def test_trial_registry(tmp_path):
    from qlab.evaluation import TrialRegistry
    reg = TrialRegistry(tmp_path / "trials.db")
    tid = reg.record(
        dataset_id="ds1",
        pipeline_config={"feat": ["mom_5d"], "model": "rf"},
        metrics={"sharpe_ratio": 1.5, "accuracy": 0.6},
    )
    assert tid >= 1
    assert reg.n_trials("ds1") == 1
    sr_dist = reg.get_sr_distribution("ds1")
    assert sr_dist.iloc[0] == 1.5


# ---- allocation ------------------------------------------------------------

def test_ivp():
    from qlab.allocation import inverse_variance_portfolio
    cov = np.diag([1.0, 4.0, 9.0])
    w = inverse_variance_portfolio(cov)
    assert abs(w.sum() - 1.0) < 1e-10
    assert w[0] > w[1] > w[2]  # 方差小权重大


def test_hrp():
    from qlab.allocation import HierarchicalRiskParity
    rng = np.random.default_rng(0)
    returns = pd.DataFrame(
        rng.normal(0, 0.01, (200, 5)),
        columns=[f"A{i}" for i in range(5)],
    )
    hrp = HierarchicalRiskParity()
    w = hrp.allocate(returns)
    assert abs(w.sum() - 1.0) < 1e-6
    assert (w >= 0).all() and (w <= 1).all()


# ---- concepts --------------------------------------------------------------

def test_fake_concept_data():
    from qlab.core.schema import SCHEMA_CONCEPT, validate_schema
    from qlab.data.sources import FakeDataSource
    src = FakeDataSource(seed=1, n_symbols=5)
    df = src.fetch_concepts(
        src.all_symbols[:3],
        (pd.Timestamp("2023-01-01"), pd.Timestamp("2023-12-31")),
    )
    assert not df.empty
    assert "concept_code" in (df.columns.tolist() + list(df.index.names))
    validate_schema(df, SCHEMA_CONCEPT, strict_index=True)


def test_data_layer_concepts():
    from qlab.data import DataLayer
    from qlab.data.sources import FakeDataSource
    data = DataLayer(source=FakeDataSource(seed=1, n_symbols=5))
    symbols = data.source.all_symbols[:3]
    result = data.concepts(symbols, pd.Timestamp("2023-06-01"))
    assert isinstance(result, pd.DataFrame)
    assert "concept_code" in result.columns
    assert "concept_name" in result.columns
    assert len(result) > 0


def test_concept_query():
    from qlab.data.concept import concept_members_as_of, concepts_as_of
    df = pd.DataFrame({
        "effective_date": [pd.Timestamp("2023-01-01")] * 4,
        "symbol": ["A", "A", "B", "B"],
        "source": ["eastmoney"] * 4,
        "concept_code": ["BK01", "BK02", "BK01", "BK03"],
        "concept_name": ["光伏", "AI", "光伏", "储能"],
        "expired_date": [pd.NaT, pd.Timestamp("2023-06-01"), pd.NaT, pd.NaT],
    })
    # A has BK01 active, BK02 expired by 2023-07-01
    result = concepts_as_of(df, ["A", "B"], pd.Timestamp("2023-07-01"), "eastmoney")
    a_concepts = result[result["symbol"] == "A"]["concept_code"].tolist()
    assert "BK01" in a_concepts
    assert "BK02" not in a_concepts  # expired

    members = concept_members_as_of(df, "BK01", pd.Timestamp("2023-07-01"), "eastmoney")
    assert "A" in members
    assert "B" in members


# ---- workspace -------------------------------------------------------------

def test_workspace_create_remove(tmp_path):
    from qlab.workspace import ExperimentConfig, Workspace
    ws = Workspace(tmp_path, "test_exp")
    cfg = ExperimentConfig(
        name="test_exp", dataset_id="ds1",
        universe="csi500", date_range=("2023-01-01", "2023-12-31"),
    )
    ws.init(cfg)
    assert ws.exists()
    assert ws.config_path.exists()
    assert ws.trials_db_path.exists()

    loaded = ws.load_config()
    assert loaded.name == "test_exp"

    ws.remove(force=True)
    assert not ws.exists()


def test_registry_preserves_parametrized_instances():
    """㊲ register(instance=X) 必须注册 X 本身, 不能用 type(X)() 重建.

    重建会丢掉构造参数: Momentum(5)/(10)/(20) 全变成默认参数的同一个
    因子(同名幂等忽略), 参数化因子静默消失。
    """
    import qlab.features.library  # noqa: F401  触发注册
    from qlab.features.registry import registry

    for name in ("mom_5d", "mom_10d", "mom_20d",
                 "ewm_vol_20d", "ewm_vol_100d",
                 "turnover_5d", "turnover_20d"):
        assert registry.has(name), f"参数化因子 {name} 丢失(register 重建了实例?)"
    # 窗口参数必须与注册时一致
    assert registry.get("mom_20d").meta.lookback_days == 21
    assert registry.get("ewm_vol_20d").meta.lookback_days == 20


def test_registry_dependencies_all_resolvable():
    """所有因子声明的 dependencies 必须都在注册表里 —— 否则按名构建会失败."""
    import qlab.features.library  # noqa: F401
    from qlab.features.registry import registry

    missing = [
        (f.meta.name, dep)
        for f in registry.all_features()
        for dep in f.meta.dependencies
        if not registry.has(dep)
    ]
    assert not missing, f"依赖缺失: {missing}"


def test_registry_empty_error_points_to_missing_import():
    """注册表为空时的报错须指出"忘记导入因子库", 而非只说"已注册: []".

    因子靠导入副作用注册, 这是最容易踩的坑。
    """
    from qlab.features.registry import FeatureRegistry

    empty = FeatureRegistry()
    with pytest.raises(KeyError, match="注册表是"):
        empty.get("mom_5d")
    try:
        empty.get("mom_5d")
    except KeyError as e:
        assert "import qlab.features.library" in str(e), "应给出可照抄的导入语句"

    # 非空表时保持原有信息(列出已注册项), 不误导为导入问题
    import qlab.features.library  # noqa: F401
    from qlab.features.registry import registry

    try:
        registry.get("no_such_feature_xyz")
    except KeyError as e:
        msg = str(e)
        assert "已注册" in msg and "mom_5d" in msg
        assert "import qlab.features.library" not in msg, (
            "表非空时不应提示导入问题"
        )


def test_purged_kfold_purges_labels_ending_on_test_start():
    """㊳ 标签恰在测试段首日实现的训练样本必须被 purge.

    t1 == t0 时该样本用到了测试段第一天的价格, 与测试样本共享信息。
    早前 split() 用 `t1 <= t0` 会留下它, 而同文件 get_train_times()
    用闭区间会剔除 —— 两处判据不一致, 且 split 侧偏松(泄漏)。
    """
    import pandas as pd

    from qlab.models.cv.purged_kfold import PurgedKFold

    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
                          "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11"])
    t1 = pd.Series(
        pd.to_datetime(["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-08",
                        "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12"]),
        index=idx,
    )
    X = pd.DataFrame(index=idx)
    for train, test in PurgedKFold(n_splits=2, t1=t1, pct_embargo=0.0).split(X):
        t_start, t_end = idx[test].min(), idx[test].max()
        for i in train:
            s, e = idx[i], t1.iloc[i]
            overlap = (e >= t_start) and (s <= t_end)
            assert not overlap, (
                f"训练样本 {s.date()}→{e.date()} 的标签区间与测试段 "
                f"{t_start.date()}~{t_end.date()} 重叠, 未被 purge"
            )


def test_purged_kfold_consistent_with_get_train_times():
    """split() 的 purge 严格程度不得低于 get_train_times() —— 同文件判据须一致."""
    import pandas as pd

    from qlab.models.cv.purged_kfold import PurgedKFold, get_train_times

    idx = pd.to_datetime([f"2024-01-{d:02d}" for d in range(2, 18)])
    t1 = pd.Series(idx[2:].tolist() + [idx[-1]] * 2, index=idx)
    X = pd.DataFrame(index=idx)
    for train, test in PurgedKFold(n_splits=3, t1=t1, pct_embargo=0.0).split(X):
        test_times = pd.Series(t1.iloc[test].max(), index=[idx[test].min()])
        allowed = set(get_train_times(t1, test_times).index)
        got = set(idx[train])
        assert got <= allowed, (
            f"split() 留下了 get_train_times() 会剔除的样本: "
            f"{sorted(str(x.date()) for x in got - allowed)}"
        )


def test_sample_weights_handles_duplicate_event_start_multi_symbol():
    """㊴ 多标的时 event_start 必然重复, sample_weights 必须能处理.

    早前用 pd.concat(...).reindex(labels.index) 回填, 重复索引直接
    ValueError: cannot reindex on an axis with duplicate labels。
    跨截面选股必然踩到 —— 而样本权重恰恰是多标的场景最需要的。
    """
    import numpy as np
    import pandas as pd

    from qlab.weights import sample_weights

    dates = pd.bdate_range("2024-01-02", periods=12)
    # 两个标的在**同一天**都有事件 → event_start 重复
    starts = list(dates[:6]) * 2
    syms = ["600519.SH"] * 6 + ["000858.SZ"] * 6
    t1s = list(dates[3:9]) * 2
    labels = pd.DataFrame({"symbol": syms, "t1": t1s}, index=pd.DatetimeIndex(starts))
    labels.index.name = "event_start"
    assert labels.index.duplicated().sum() > 0, "构造前提: index 必须有重复"

    close = pd.DataFrame(
        {"close": np.linspace(100, 120, len(dates) * 2)},
        index=pd.MultiIndex.from_product(
            [dates, ["600519.SH", "000858.SZ"]], names=["date", "symbol"]
        ),
    )
    w = sample_weights(labels, close, time_decay=0.5)
    assert len(w) == len(labels)
    assert w["final_weight"].notna().all()
    # 同一天不同标的应各有独立权重(不是被覆盖成同一个值)
    same_day = w.loc[dates[0]]
    assert len(same_day) == 2, "同一 event_start 的两个标的都应保留"
    # 归一化: Σw = N
    assert abs(w["final_weight"].sum() - len(w)) < 1e-6


def test_seq_bootstrap_beats_standard_bootstrap_on_uniqueness():
    """序贯自举的平均唯一度应高于标准自举 —— 这是它存在的理由(AFML Ch4)."""
    import numpy as np
    import pandas as pd

    from qlab.weights import build_indicator_matrix, seq_bootstrap_sample

    idx = pd.bdate_range("2024-01-02", periods=60)
    # 构造强重叠: 每个样本跨 8 个 bar
    starts = idx[:40]
    t1 = pd.Series([idx[min(i + 8, len(idx) - 1)] for i in range(40)], index=starts)
    ind = build_indicator_matrix(idx, t1)
    n = ind.shape[1]

    def avg_uniq(cols: list[int]) -> float:
        m = ind.iloc[:, cols]
        c = m.sum(axis=1).replace(0, np.nan)
        return float(np.nanmean(m.div(c, axis=0).replace(0, np.nan).to_numpy()))

    rng = np.random.default_rng(0)
    std = [avg_uniq(list(rng.integers(0, n, size=n))) for _ in range(15)]
    seq = [
        avg_uniq(seq_bootstrap_sample(ind, sample_length=n, seed=s))
        for s in range(15)
    ]
    assert np.mean(seq) > np.mean(std), (
        f"序贯自举唯一度 {np.mean(seq):.4f} 未超过标准自举 {np.mean(std):.4f}"
    )


def test_pbo_result_is_dataclass_with_dict_compat():
    """compute_pbo 返回 dataclass(属性访问) 且兼容旧的 dict 式访问."""
    import numpy as np
    import pandas as pd

    from qlab.evaluation import PBOResult, compute_pbo

    rng = np.random.default_rng(0)
    perf = pd.DataFrame(rng.normal(0, 0.01, (60, 8)),
                        index=pd.bdate_range("2024-01-01", periods=60))
    res = compute_pbo(perf, n_splits=6)
    assert isinstance(res, PBOResult)
    # 属性访问(新)
    assert 0.0 <= res.pbo <= 1.0
    assert res.n_combinations == 20  # C(6,3)
    assert len(res.logits) == res.n_combinations
    # dict 式访问(旧代码兼容)
    assert res["pbo"] == res.pbo
    with pytest.raises(KeyError):
        res["no_such_key"]


def test_ivp_returns_series_with_symbol_names():
    """IVP 传 DataFrame 应返回带标的名的 Series —— 与 HRP.allocate 一致.

    组合权重必须能对回标的; 返回裸数组会让调用方靠位置对齐, 顺序一变就错配。
    """
    import numpy as np
    import pandas as pd

    from qlab.allocation import inverse_variance_portfolio

    syms = ["600519.SH", "000858.SZ", "601318.SH"]
    cov = pd.DataFrame(np.diag([0.04, 0.01, 0.09]), index=syms, columns=syms)
    w = inverse_variance_portfolio(cov)
    assert isinstance(w, pd.Series)
    assert list(w.index) == syms
    assert abs(w.sum() - 1.0) < 1e-12
    # 方差最小的标的权重最大
    assert w.idxmax() == "000858.SZ"
    # 裸 ndarray 输入仍返回 ndarray(无名可用)
    assert isinstance(inverse_variance_portfolio(cov.to_numpy()), np.ndarray)


def test_daily_ewm_vol_rejects_wrong_shape_with_clear_error():
    """单只/panel 版本传错类型必须 fail-loud 并指向正确函数.

    早前传 DataFrame 会在 .rename() 处报 pandas 内部的
    "'str' object is not callable", 完全看不出原因。
    """
    import numpy as np
    import pandas as pd

    from qlab.labeling.thresholds import daily_ewm_vol, daily_ewm_vol_panel

    idx = pd.bdate_range("2024-01-01", periods=30)
    ser = pd.Series(np.linspace(100, 110, 30), index=idx)
    panel = pd.DataFrame({"a": ser, "b": ser * 1.1})

    with pytest.raises(TypeError, match="daily_ewm_vol_panel"):
        daily_ewm_vol(panel)
    with pytest.raises(TypeError, match="daily_ewm_vol"):
        daily_ewm_vol_panel(ser)
    # 正确用法不受影响
    assert isinstance(daily_ewm_vol(ser), pd.Series)
    assert isinstance(daily_ewm_vol_panel(panel), pd.DataFrame)


def test_feature_deps_auto_resolved_from_registry():
    """㊵ 只传下游因子时, 依赖应从 registry 自动补齐.

    dependencies 元数据存在的意义就是让框架自动解析;
    早前要求调用方手工把 mom_20d 一起传, 否则 FeatureComputationError。
    """
    import qlab.features.library  # noqa: F401
    from qlab.features.matrix import _topological_sort
    from qlab.features.registry import registry

    ordered = _topological_sort([registry.get("mom_resid_20d")])
    names = [f.meta.name for f in ordered]
    assert "mom_20d" in names, "依赖 mom_20d 应被自动补齐"
    # 依赖必须排在下游之前
    assert names.index("mom_20d") < names.index("mom_resid_20d")


def test_feature_missing_dep_fails_loud():
    """依赖既不在入参也不在 registry 时必须报错并给出出路."""
    import pandas as pd

    from qlab.core.exceptions import FeatureComputationError
    from qlab.features.base import DailyFeature, FeatureMeta
    from qlab.features.matrix import _topological_sort

    class _Bogus(DailyFeature):
        meta = FeatureMeta(
            name="bogus_feat", version="1.0", lookback_days=1,
            dependencies=("no_such_dep",),
        )

        def compute(self, ctx):
            return pd.Series(dtype=float)

    with pytest.raises(FeatureComputationError, match="registry"):
        _topological_sort([_Bogus()])


def test_cpcv_handles_duplicate_time_index():
    """㊶ CPCV 在重复时间索引下必须可用(多标的样本按 date 索引时必然重复).

    早前用 X.index.get_loc(idx) 定位, 重复索引会返回 slice/数组,
    导致 embargo 过滤处 TypeError: unhashable type: 'slice'。
    """
    import numpy as np
    import pandas as pd

    from qlab.evaluation import CombinatorialPurgedCV

    # 每个日期 3 个样本(模拟 3 只标的) → index 大量重复
    days = pd.bdate_range("2024-01-01", periods=40)
    idx = pd.DatetimeIndex(np.repeat(days, 3))
    X = pd.DataFrame({"f": np.arange(len(idx), dtype=float)}, index=idx)
    t1 = pd.Series(
        pd.DatetimeIndex(np.repeat(days.shift(5, freq="B"), 3)), index=idx
    )
    assert idx.duplicated().sum() > 0, "构造前提: 时间索引必须有重复"

    cv = CombinatorialPurgedCV(N=6, k=2, t1=t1, embargo_pct=0.02)
    n_paths = 0
    for train, test, groups in cv.split(X):
        n_paths += 1
        assert len(groups) == 2
        assert len(np.intersect1d(train, test)) == 0, "训练/测试不得重叠"
        # purge 生效: 训练标签区间不得与测试段相交
        te_start = X.index[test].min()
        te_end = pd.DatetimeIndex(t1.iloc[test]).max()
        tr_start = pd.DatetimeIndex(X.index[train])
        tr_end = pd.DatetimeIndex(t1.iloc[train])
        overlap = ((tr_end >= te_start) & (tr_start <= te_end)).sum()
        assert overlap == 0, f"purge 失效: {overlap} 个训练样本与测试段重叠"
    assert n_paths == 15, f"C(6,2)=15 条路径, 实际 {n_paths}"


def test_meta_labels_align_by_symbol_not_just_date():
    """㊷ 多标的时 side 必须按 (event_start, symbol) 对齐, 不能只按日期.

    只按 event_start 对齐会把 A 股的方向配给 B 股(或直接
    ValueError: cannot reindex on an axis with duplicate labels)。
    """
    import pandas as pd

    from qlab.labeling import to_meta_labels

    days = pd.bdate_range("2024-01-01", periods=3)
    rows = [(d, s) for d in days for s in ["600519.SH", "000858.SZ"]]
    idx = pd.DatetimeIndex([r[0] for r in rows])
    events = pd.DataFrame(
        {"symbol": [r[1] for r in rows], "t1": idx, "target": 0.02}, index=idx
    )
    events.index.name = "event_start"

    # 正确用法: MultiIndex(event_start, symbol)
    side = pd.Series(
        [1.0, -1.0] * 3,
        index=pd.MultiIndex.from_arrays(
            [idx, events["symbol"]], names=["event_start", "symbol"]
        ),
    )
    out = to_meta_labels(events, side)
    assert out["side"].notna().all()
    # 每只标的拿到自己的方向(不是被另一只覆盖)
    assert (out.loc[out["symbol"] == "600519.SH", "side"] == 1.0).all()
    assert (out.loc[out["symbol"] == "000858.SZ", "side"] == -1.0).all()

    # 危险用法: 多标的却只按日期索引 → 必须 fail-loud 而非静默错配
    bad_side = pd.Series(1.0, index=pd.DatetimeIndex(list(days) * 2))
    with pytest.raises(ValueError, match="MultiIndex|无法区分"):
        to_meta_labels(events, bad_side)


def test_meta_label_bins_maps_profit_to_binary():
    """meta 标签只判断"该不该下注": ret>0 → 1, 否则 0."""
    import pandas as pd

    from qlab.labeling import meta_label_bins

    labels = pd.DataFrame({"ret": [0.05, -0.03, 0.0, 0.01]})
    mb = meta_label_bins(labels)
    assert mb.tolist() == [1, 0, 0, 1]
    with pytest.raises(ValueError, match="ret"):
        meta_label_bins(pd.DataFrame({"bin": [1, -1]}))


def test_label_events_parallel_equals_serial():
    """㊸ 并行路径必须可用且与串行结果**完全等价**.

    旧实现 worker 是局部函数, 无法 pickle:
    "Can't pickle local object 'label_events.<locals>._worker'"
    → num_threads>1 必崩(而大样本正是需要并行的场景)。
    切分改按位置: 多标的时 event_start 重复, events.loc[timestamps] 会多取行。

    60: 还要比对**逐标的单独打标**的 ground truth, 且跑多个 num_threads ——
    旧实现用 imap_unordered, 子任务乱序返回后 sort_index 在重复 event_start
    下无法恢复行序, 标签会**静默错配到别的标的上**。
    只测 num_threads=2 抓不住: 两个分子往往恰好按序返回。
    """
    import numpy as np
    import pandas as pd

    from qlab.labeling.triple_barrier import TripleBarrier, label_events

    days = pd.bdate_range("2024-01-01", periods=30)
    syms = ["600519.SH", "000858.SZ", "601318.SH"]
    rows = [(d, s) for d in days[:20] for s in syms]
    idx = pd.DatetimeIndex([r[0] for r in rows])
    events = pd.DataFrame(
        {
            "symbol": [r[1] for r in rows],
            "t1": [days[min(i // 3 + 6, 29)] for i in range(len(idx))],
            "target": 0.02,
        },
        index=idx,
    )
    events.index.name = "event_start"
    assert idx.duplicated().sum() > 0, "构造前提: 多标的 event_start 必须重复"

    cidx = pd.MultiIndex.from_product([days, syms], names=["date", "symbol"])
    rng = np.random.default_rng(0)
    close = pd.DataFrame(
        {"close": 100 * np.exp(np.cumsum(rng.normal(0, 0.01, len(cidx))))}, index=cidx
    )
    serial = label_events(events, close, TripleBarrier(1.0, 1.0), num_threads=1)

    # ground truth: 每个标的单独打标(内部 event_start 无重复), 再拼起来
    truth = (
        pd.concat(
            [
                label_events(
                    events[events["symbol"] == s], close,
                    TripleBarrier(1.0, 1.0), num_threads=1,
                )
                for s in syms
            ]
        )
        .set_index("symbol", append=True)
        .sort_index()
    )
    assert len(truth) == len(serial)

    for nt in (1, 2, 4, 8):
        got = label_events(events, close, TripleBarrier(1.0, 1.0), num_threads=nt)
        pd.testing.assert_frame_equal(serial, got)
        pd.testing.assert_frame_equal(
            truth, got.set_index("symbol", append=True).sort_index()
        )


def test_cusum_accepts_series_threshold():
    """㊹ 动态阈值(h=Series)必须可用 —— AFML Ch3 的核心推荐用法.

    pandas 3.0 移除了 fillna(method=), 旧代码在此路径必崩:
    TypeError: NDFrame.fillna() got an unexpected keyword argument 'method'
    """
    import numpy as np
    import pandas as pd

    from qlab.labeling import CUSUMFilter

    days = pd.bdate_range("2024-01-01", periods=60)
    syms = ["600519.SH", "000858.SZ"]
    rng = np.random.default_rng(1)
    wide = pd.DataFrame(
        {s: 100 * np.exp(np.cumsum(rng.normal(0, 0.02, len(days)))) for s in syms},
        index=days,
    )
    # 随时间变化的阈值(如随波动率放大)
    h = pd.Series(np.linspace(0.01, 0.05, len(days)), index=days)
    out = CUSUMFilter(h=h).sample_per_symbol(wide)
    assert set(out.columns) == {"timestamp", "symbol"}
    # 标量阈值仍可用
    out2 = CUSUMFilter(h=0.02).sample_per_symbol(wide)
    assert len(out2) > 0


def test_feature_importance_dtypes_consistent():
    """㊺ MDI/MDA/SFI 的重要度必须都是 float64, 不能是 object.

    空 DataFrame 默认 object dtype, .loc 逐格赋值不会推回 float →
    np.isfinite/np.log 等 numpy 运算直接 TypeError, 且与 MDI 不一致。
    """
    import numpy as np
    import pandas as pd
    from sklearn.tree import DecisionTreeClassifier

    from qlab.models import (
        build_bagging_classifier,
        feat_imp_mda,
        feat_imp_mdi,
        feat_imp_sfi,
    )

    rng = np.random.default_rng(0)
    idx = pd.DatetimeIndex(np.repeat(pd.bdate_range("2024-01-01", periods=80), 4))
    n = len(idx)
    X = pd.DataFrame(rng.normal(0, 1, (n, 4)), columns=list("abcd"), index=idx)
    y = pd.Series(rng.choice([-1, 1], n), index=idx)
    t1 = pd.Series(idx + pd.Timedelta(days=4), index=idx)
    sw = pd.Series(1.0, index=idx)
    clf = build_bagging_classifier(
        DecisionTreeClassifier(max_depth=3, random_state=0),
        n_estimators=8, random_state=0,
    )
    clf.fit(X, y)

    mdi = feat_imp_mdi(clf, list(X.columns))
    mda, baseline = feat_imp_mda(clf, X, y, sw, t1, cv=3, pct_embargo=0.01)
    sfi = feat_imp_sfi(clf, X, y, sw, t1, cv=3)
    for name, df in (("MDI", mdi), ("MDA", mda), ("SFI", sfi)):
        for col in ("mean", "std"):
            assert df[col].dtype == np.float64, (
                f"{name}[{col}] dtype 是 {df[col].dtype}, 应为 float64"
            )
        # numpy 运算必须可用
        assert np.isfinite(df["mean"]).all(), f"{name} 含非有限值"
    assert np.isfinite(baseline)


def test_label_events_drops_samples_without_valid_return():
    """㊻ 无有效收益的样本必须默认剔除, 不能以 bin=0 混入训练集.

    两种成因: (1) 事件在数据末尾 → touch_type='no_data';
    (2) 触及障碍那天停牌 → touch_type 正常但 ret=NaN。
    两者 bin 都被赋 0, 与"到期且收益≈0"的真实中性样本无法区分。
    """
    import numpy as np
    import pandas as pd

    from qlab.labeling.triple_barrier import TripleBarrier, label_events

    days = pd.bdate_range("2024-01-01", periods=12)
    sym = "600519.SH"
    # 最后两天的事件没有后续路径 → no_data
    events = pd.DataFrame(
        {"symbol": sym, "t1": list(days[4:]) + [days[-1]] * 4, "target": 0.02},
        index=days,
    )
    events.index.name = "event_start"
    close = pd.DataFrame(
        {"close": np.linspace(100, 110, len(days))},
        index=pd.MultiIndex.from_product([days, [sym]], names=["date", "symbol"]),
    )
    kept = label_events(events, close, TripleBarrier(1.0, 1.0))
    assert kept["ret"].notna().all(), "剔除后不应残留 ret=NaN"
    assert (kept["touch_type"] != "no_data").all()
    tt = pd.to_datetime(kept["touch_time"])
    assert (tt > pd.DatetimeIndex(kept.index)).all(), "剔除后时间方向必须干净"

    # 逃生口: drop_no_data=False 保留全部
    raw = label_events(events, close, TripleBarrier(1.0, 1.0), drop_no_data=False)
    assert len(raw) == len(events)
    assert len(raw) >= len(kept)
    if len(raw) > len(kept):
        assert kept.attrs.get("n_dropped_no_data", 0) == len(raw) - len(kept)


def test_strategy_risk_matches_afml_snippet_15_3():
    """evaluation/risk 数值对齐 AFML Snippet 15.3 并自洽.

    此前该模块零测试覆盖。书中例子: sl=-2%, pt=2%, freq=52, SR=2 → p≈0.6336
    """
    from qlab.evaluation import implied_precision
    from qlab.evaluation.risk.strategy_risk import binary_implied_freq

    p = implied_precision(sl=-0.02, pt=0.02, freq=52, target_sr=2.0)
    assert abs(p - 0.6336) < 1e-3, f"与书中 0.6336 不符: {p}"
    # 反函数自洽: 用 p 反推频率应回到 52
    f = binary_implied_freq(sl=-0.02, pt=0.02, p=p, target_sr=2.0)
    assert abs(f - 52.0) < 1e-6, f"反推频率 {f} != 52"
    # 目标 SR 越高, 所需精度单调递增
    ps = [implied_precision(-0.02, 0.02, 52, sr) for sr in (0.5, 1.0, 2.0, 3.0)]
    assert all(ps[i] < ps[i + 1] for i in range(len(ps) - 1)), f"非单调: {ps}"


def test_prob_strategy_failure_monotonic_in_target():
    """目标 SR 越高 → 策略失败概率越高."""
    import numpy as np
    import pandas as pd

    from qlab.evaluation import prob_strategy_failure

    rng = np.random.default_rng(0)
    r = pd.Series(rng.normal(0.001, 0.02, 500))
    low = prob_strategy_failure(r, freq=52, target_sr=1.0)
    high = prob_strategy_failure(r, freq=52, target_sr=3.0)
    assert 0.0 <= low <= 1.0 and 0.0 <= high <= 1.0
    assert high >= low, f"SR=3 的失败概率 {high} 应 ≥ SR=1 的 {low}"


def test_log_uniform_is_uniform_in_log_space():
    """log_uniform 的关键性质: log(x) 均匀 → 各数量级样本数接近.

    此前该模块零测试覆盖。若退化成普通 uniform, 大数量级会占绝大多数。
    """
    import collections

    import numpy as np

    from qlab.models import log_uniform

    s = log_uniform(a=1e-3, b=1e3).rvs(size=20000, random_state=0)
    assert (s >= 1e-3).all() and (s <= 1e3).all()
    counts = [
        c for _, c in sorted(
            collections.Counter(np.floor(np.log10(s)).astype(int)).items()
        )
    ]
    assert len(counts) == 6, f"1e-3~1e3 应覆盖 6 个数量级, 实际 {len(counts)}"
    spread = (max(counts) - min(counts)) / float(np.mean(counts))
    assert spread < 0.15, f"各数量级样本数不均匀(极差/均值={spread:.3f})"


def test_clf_hyper_fit_both_search_modes():
    """超参搜索两条路径(Grid/Randomized)都能返回可用模型."""
    import numpy as np
    import pandas as pd
    from sklearn.pipeline import Pipeline
    from sklearn.tree import DecisionTreeClassifier

    from qlab.models import clf_hyper_fit

    rng = np.random.default_rng(0)
    idx = pd.DatetimeIndex(np.repeat(pd.bdate_range("2024-01-01", periods=60), 4))
    n = len(idx)
    X = pd.DataFrame(rng.normal(0, 1, (n, 3)), columns=list("abc"), index=idx)
    y = pd.Series(rng.choice([-1, 1], n), index=idx)
    t1 = pd.Series(idx + pd.Timedelta(days=3), index=idx)
    pipe = Pipeline([("clf", DecisionTreeClassifier(random_state=0))])

    grid = clf_hyper_fit(X, y, t1, pipe, {"clf__max_depth": [2, 4]}, cv=3, n_jobs=1)
    assert len(grid.predict(X.head(5))) == 5
    rnd = clf_hyper_fit(
        X, y, t1, pipe, {"clf__max_depth": [2, 3, 4, 5]},
        cv=3, rnd_search_iter=3, n_jobs=1,
    )
    assert len(rnd.predict(X.head(5))) == 5


def test_trial_registry_feeds_deflated_sharpe():
    """研究纪律闭环: 登记 N 次试验 → n_trials/SR方差 → DSR 必比 PSR 保守.

    这是防"挑最好的那次汇报"的核心机制。若 DSR ≥ PSR 说明去膨胀失效。
    """
    import tempfile

    import numpy as np
    import pandas as pd

    from qlab.evaluation import TrialRegistry
    from qlab.evaluation.statistics import (
        deflated_sharpe_ratio,
        probabilistic_sharpe_ratio,
    )
    from qlab.evaluation.statistics.sharpe import expected_max_sharpe

    with tempfile.TemporaryDirectory() as tmp:
        reg = TrialRegistry(f"{tmp}/trials.db")
        rng = np.random.default_rng(0)
        # 12 组"试验": SR 各不相同(模拟网格搜索)
        for i in range(12):
            reg.record(
                dataset_id="ds", pipeline_config={"p": i},
                metrics={"sharpe_ratio": float(rng.normal(0.3, 0.4))},
            )
        assert reg.n_trials("ds") == 12
        srs = reg.get_sr_distribution("ds")
        assert len(srs) == 12 and srs.dtype == np.float64
        # 另一个 dataset 不应混入
        reg.record(dataset_id="other", pipeline_config={}, metrics={"sharpe_ratio": 9.0})
        assert reg.n_trials("ds") == 12, "不同 dataset_id 的试验不得混计"

        # DSR 必比 PSR 保守
        r = pd.Series(rng.normal(0.002, 0.02, 400))
        psr = probabilistic_sharpe_ratio(r, sr_benchmark=0.0)
        dsr = deflated_sharpe_ratio(r, n_trials=12, var_trials_sr=float(srs.var()))
        assert dsr <= psr, f"DSR({dsr:.4f}) 应 ≤ PSR({psr:.4f})"
        # 试验次数越多 → 随机基准越高 → DSR 越低
        sr_star_few = expected_max_sharpe(2, float(srs.var()))
        sr_star_many = expected_max_sharpe(100, float(srs.var()))
        assert sr_star_many > sr_star_few, "试验越多, E[max SR] 应越高"
        dsr_many = deflated_sharpe_ratio(r, n_trials=100, var_trials_sr=float(srs.var()))
        assert dsr_many <= dsr, "试验次数增加, DSR 不应上升"


def test_feature_store_invalidates_on_version_change():
    """因子 version 变更必须使缓存失效 —— 否则会拿到旧版计算结果.

    同名同缓存键但实现不同, 是最危险的静默错误。
    """
    import numpy as np

    from qlab.data.layer import DataLayer  # noqa: I001
    from qlab.data.sources import FakeDataSource
    from qlab.features.base import DailyFeature, FeatureMeta
    from qlab.features.matrix import build_feature_matrix
    from qlab.features.store import InMemoryFeatureStore

    class _V1(DailyFeature):
        meta = FeatureMeta(name="probe", version="1.0", lookback_days=2)

        def compute(self, ctx):
            c = ctx.daily(["close"], lookback_days=2)["close"]
            return (c * 0.0 + 1.0).rename("probe")

    class _V2(DailyFeature):
        meta = FeatureMeta(name="probe", version="2.0", lookback_days=2)

        def compute(self, ctx):
            c = ctx.daily(["close"], lookback_days=2)["close"]
            return (c * 0.0 + 2.0).rename("probe")

    src = FakeDataSource(seed=3, n_symbols=3, start_year=2023)
    layer = DataLayer(source=src)
    uni = layer.universe("all_a", "2023-02-01", "2023-02-28")
    fs = InMemoryFeatureStore()
    m1 = build_feature_matrix(
        features=[_V1()], data=layer, universe=uni,
        date_range=("2023-02-01", "2023-02-28"), feature_store=fs,
    )
    m2 = build_feature_matrix(
        features=[_V2()], data=layer, universe=uni,
        date_range=("2023-02-01", "2023-02-28"), feature_store=fs,
    )
    v1 = m1.values["probe"].dropna()
    v2 = m2.values["probe"].dropna()
    assert np.allclose(v1, 1.0), "v1 应全为 1.0"
    assert np.allclose(v2, 2.0), "v2 应全为 2.0 —— 若为 1.0 说明复用了旧缓存"


def test_workspace_lifecycle_and_shared_store():
    """Workspace: 布局 / config 往返(tuple 保持) / clone 历史控制."""
    import tempfile

    from qlab.core.exceptions import QlabError
    from qlab.workspace import ExperimentConfig, Workspace

    with tempfile.TemporaryDirectory() as root:
        ws = Workspace(root, "exp_a")
        assert not ws.exists()
        cfg = ExperimentConfig(
            name="exp_a", dataset_id="ds1", universe="000300.SH",
            date_range=("2024-06-03", "2024-09-30"),
            features=["mom_5d"], labeling={"pt": 1.5, "sl": 1.0},
        )
        ws.init(cfg)
        assert ws.exists()
        assert ws.config_path.exists() and ws.trials_db_path.exists()
        # 重复 init 必须报错(防覆盖已有实验)
        with pytest.raises(QlabError, match="已存在"):
            ws.init(cfg)

        loaded = ws.load_config()
        assert loaded.features == cfg.features
        assert loaded.labeling == cfg.labeling
        assert isinstance(loaded.date_range, tuple), "date_range 往返后应仍是 tuple"
        assert loaded.date_range == cfg.date_range

        # store 是**跨 workspace 共享**的(root/store 而非 workspace 目录内)
        assert ws.shared_bar_store.is_relative_to(ws.root)
        assert not ws.shared_bar_store.is_relative_to(ws.path)

        reg = ws.trial_registry()
        reg.record(dataset_id="ds1", pipeline_config={}, metrics={"sharpe_ratio": 0.5})
        assert reg.n_trials("ds1") == 1

        # clone: 默认不带历史, keep_history=True 才带
        fresh = ws.clone("exp_b")
        assert fresh.load_config().features == cfg.features
        assert fresh.trial_registry().n_trials("ds1") == 0
        withhist = ws.clone("exp_c", keep_history=True)
        assert withhist.trial_registry().n_trials("ds1") == 1


# ======================================================================
# 退化/极端输入 —— 真实研究中常见(筛选后为空、只剩 1 只、区间无事件)
# ======================================================================


def test_feature_matrix_degenerate_inputs():
    """㊼㊽ 空 universe / 空 features / 空日期区间应返回空矩阵, 而非 pandas 内部错.

    早前分别抛 "None of [Index(['close'])] are in the columns" 和
    "No objects to concatenate", 完全看不出真实原因。
    """
    import pandas as pd

    import qlab.features.library  # noqa: F401
    from qlab.data.layer import DataLayer
    from qlab.data.sources import FakeDataSource
    from qlab.data.universe import Universe, UniverseSpec
    from qlab.features.matrix import build_feature_matrix

    src = FakeDataSource(seed=5, n_symbols=3, start_year=2023)
    layer = DataLayer(source=src)
    rng = ("2023-03-01", "2023-03-31")

    # 空 universe
    empty_idx = pd.MultiIndex.from_arrays(
        [pd.DatetimeIndex([]), pd.Index([], dtype=object)], names=["date", "symbol"]
    )
    empty_uni = Universe(
        pd.DataFrame(
            {"in_universe": pd.Series(dtype=bool), "weight": pd.Series(dtype=float)},
            index=empty_idx,
        ),
        UniverseSpec("empty"),
    )
    m = build_feature_matrix(
        features=["mom_5d"], data=layer, universe=empty_uni, date_range=rng
    )
    assert len(m.values) == 0
    assert list(m.values.index.names) == ["date", "symbol"]
    assert "mom_5d" in m.values.columns, "空矩阵也应保留列结构"

    # 空 features 列表
    uni = layer.universe("all_a", *rng)
    m2 = build_feature_matrix(features=[], data=layer, universe=uni, date_range=rng)
    assert len(m2.values) == 0 and len(m2.metas) == 0


def test_sample_weights_empty_labels():
    """㊾ 空标签集应返回空权重表, 而非 KeyError: 'symbol'."""
    import pandas as pd

    from qlab.weights import sample_weights

    labels = pd.DataFrame(
        {"symbol": pd.Series(dtype=object), "t1": pd.Series(dtype="datetime64[ns]")},
        index=pd.DatetimeIndex([], name="event_start"),
    )
    close = pd.DataFrame(
        {"close": pd.Series(dtype=float)},
        index=pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([]), pd.Index([], dtype=object)],
            names=["date", "symbol"],
        ),
    )
    w = sample_weights(labels, close)
    assert len(w) == 0
    assert {"uniqueness", "return_attr", "time_decay", "final_weight"} <= set(w.columns)


def test_cv_splitters_fail_loud_on_insufficient_samples():
    """㊿ 样本数不足时须明确报错, 而非 pandas/numpy 内部 IndexError."""
    import numpy as np
    import pandas as pd

    from qlab.evaluation import CombinatorialPurgedCV
    from qlab.models import PurgedKFold

    idx = pd.DatetimeIndex(pd.bdate_range("2024-01-01", periods=3))
    X = pd.DataFrame({"f": np.arange(3.0)}, index=idx)
    t1 = pd.Series(idx + pd.Timedelta(days=2), index=idx)

    with pytest.raises(ValueError, match="少于 n_splits"):
        list(PurgedKFold(n_splits=5, t1=t1).split(X))
    with pytest.raises(ValueError, match="少于分组数"):
        list(CombinatorialPurgedCV(N=6, k=2, t1=t1).split(X))
    # k > N//2 本就该报错
    with pytest.raises(ValueError, match=r"k \(3\)"):
        CombinatorialPurgedCV(N=4, k=3, t1=t1)


def test_allocation_degenerate_inputs():
    """51/52 单资产可用; 零方差必须报错而非静默产出 NaN 权重.

    NaN 权重会一路传到下单环节 —— 静默比崩溃危险得多。
    """
    import numpy as np
    import pandas as pd

    from qlab.allocation import HierarchicalRiskParity, inverse_variance_portfolio

    # 单资产: 权重必为 1.0
    one = pd.DataFrame([[0.04]], index=["a"], columns=["a"])
    assert inverse_variance_portfolio(one).tolist() == [1.0]
    hrp_one = HierarchicalRiskParity().allocate_from_cov(one, pd.DataFrame([[1.0]], index=["a"], columns=["a"]))
    assert hrp_one.tolist() == [1.0]

    # 零方差 → 必须 fail-loud(否则 1/0 产出 NaN)
    zero = pd.DataFrame(np.diag([0.0, 0.01]), index=list("ab"), columns=list("ab"))
    with pytest.raises(ValueError, match="方差为正|非正"):
        inverse_variance_portfolio(zero)
    # NaN 方差同样拦住
    nan_cov = pd.DataFrame(np.diag([np.nan, 0.01]), index=list("ab"), columns=list("ab"))
    with pytest.raises(ValueError, match="方差为正|非正"):
        inverse_variance_portfolio(nan_cov)


def test_store_get_range_end_is_inclusive_for_intraday():
    """53 纯日期 end 必须包含当日全部日内 bar.

    intraday 索引带时分秒(10:00/15:00), 而 end='2024-06-03' 解析为当日 00:00
    —— 直接 `<= end` 会把整天排除, 导致 intraday(D, D) 返回空。
    """
    import numpy as np
    import pandas as pd

    from qlab.data.store import InMemoryShardedBarStore

    store = InMemoryShardedBarStore()
    day = pd.Timestamp("2024-06-03")
    stamps = [day + pd.Timedelta(hours=h) for h in (10, 11, 13, 15)]
    idx = pd.MultiIndex.from_product(
        [pd.DatetimeIndex(stamps), ["600519.SH"]], names=["timestamp", "symbol"]
    )
    df = pd.DataFrame({"close": np.arange(len(idx), dtype=float)}, index=idx)
    store.put_range(df, kind="intraday", source_version="v1", freq="30m")

    same_day = store.get_range(
        kind="intraday", symbols=["600519.SH"], start=day, end=day,
        source_version="v1", freq="30m",
    )
    assert len(same_day) == 4, f"单日区间应取到全部 4 根, 实际 {len(same_day)}"

    # 显式时刻要被尊重, 不能一律撑到当日末尾
    until_11 = store.get_range(
        kind="intraday", symbols=["600519.SH"], start=day,
        end=day + pd.Timedelta(hours=11), source_version="v1", freq="30m",
    )
    assert len(until_11) == 2, f"end=11:00 应只取 2 根, 实际 {len(until_11)}"

    # 日频(索引即当日 00:00)行为不变
    didx = pd.MultiIndex.from_product(
        [pd.DatetimeIndex([day]), ["600519.SH"]], names=["date", "symbol"]
    )
    store.put_range(
        pd.DataFrame({"close": [1.0]}, index=didx), kind="daily",
        source_version="v1",
    )
    dd = store.get_range(
        kind="daily", symbols=["600519.SH"], start=day, end=day, source_version="v1"
    )
    assert len(dd) == 1


def test_daily_rejects_adjust_switching():
    """54 `daily(adjust=)` 不能切换口径 —— 静默无效的参数比报错危险.

    DailyBar 同时含 close/close_raw/adj_factor, adjust 只参与缓存键不影响内容。
    早前三种 adjust 返回**完全相同**的值, 调用方误以为拿到了不同口径,
    且同一份数据会按不同缓存键重复落盘。
    """
    from qlab.core.enums import AdjustMode, Freq
    from qlab.data.layer import DataLayer
    from qlab.data.sources import FakeDataSource

    layer = DataLayer(source=FakeDataSource(seed=2, n_symbols=2, start_year=2023))
    syms = layer.source.fetch_universe(
        "all_a", (pd.Timestamp("2023-02-01"), pd.Timestamp("2023-02-10"))
    ).index.get_level_values("symbol").unique().tolist()[:2]
    rng = ("2023-02-01", "2023-02-10")

    # 默认口径可用
    assert len(layer.daily(syms, *rng)) > 0
    # 显式传默认值放行
    assert len(layer.daily(syms, *rng, adjust=AdjustMode.BACKWARD)) > 0
    # 非默认口径必须 fail-loud 并指向 apply_adjust
    for mode in (AdjustMode.NONE, AdjustMode.FORWARD):
        with pytest.raises(ValueError, match="apply_adjust"):
            layer.daily(syms, *rng, adjust=mode)
        with pytest.raises(ValueError, match="apply_adjust"):
            layer.intraday(syms, *rng, freq=Freq.MIN_30, adjust=mode)


def test_apply_adjust_actually_changes_price_scale():
    """apply_adjust 才是切换口径的正确入口: 三种模式必须给出不同的价格."""
    import numpy as np
    import pandas as pd

    from qlab.core.enums import AdjustMode
    from qlab.data.adjust import apply_adjust

    idx = pd.MultiIndex.from_product(
        [pd.bdate_range("2024-06-03", periods=3), ["600519.SH"]],
        names=["date", "symbol"],
    )
    raw = np.array([100.0, 101.0, 102.0])
    factor = np.array([2.0, 2.0, 2.0])
    df = pd.DataFrame(
        {
            "close_raw": raw, "open_raw": raw, "high_raw": raw, "low_raw": raw,
            "close": raw * factor, "open": raw * factor,
            "high": raw * factor, "low": raw * factor,
            "adj_factor": factor,
        },
        index=idx,
    )
    none_v = apply_adjust(df, AdjustMode.NONE)["close"].to_numpy()
    back_v = apply_adjust(df, AdjustMode.BACKWARD)["close"].to_numpy()
    assert np.allclose(none_v, raw), "none 应给不复权价"
    assert np.allclose(back_v, raw * factor), "backward 应给后复权价"
    assert not np.allclose(none_v, back_v), "两种口径必须不同"


def _order_probe(molecule, *, delay_first: bool = True):
    """mp_pandas_obj 顺序契约的探针 worker(必须模块级才能 pickle).

    第一个分子故意慢一点 —— imap_unordered 下它会**最后**返回,
    从而暴露"返回顺序 != 提交顺序"。
    """
    import time

    import pandas as pd

    if delay_first and molecule[0] == 0:
        time.sleep(0.3)
    # 索引全用同一个时间戳: 模拟多标的共享 event_start 的重复索引
    ts = pd.Timestamp("2024-01-01")
    return pd.DataFrame({"pos": list(molecule)}, index=[ts] * len(molecule))


def test_mp_pandas_obj_preserves_submission_order():
    """60 mp_pandas_obj 必须保证"返回顺序 == 提交顺序".

    索引含重复值时 sort_index 无法从乱序恢复行序 —— 调用方按位置对齐
    结果就会静默错配。旧实现用 imap_unordered, 慢的分子最后回来, 顺序全乱。
    """
    from qlab.core.parallel import mp_pandas_obj

    atoms = list(range(12))
    serial = mp_pandas_obj(_order_probe, ("molecule", atoms), num_threads=1)
    assert serial["pos"].tolist() == atoms

    for nt in (2, 4):
        got = mp_pandas_obj(_order_probe, ("molecule", atoms), num_threads=nt)
        assert got["pos"].tolist() == atoms, (
            f"num_threads={nt} 下行序被打乱: {got['pos'].tolist()}"
        )
