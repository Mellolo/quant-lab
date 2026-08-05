"""FeatureMatrix — 特征矩阵组装.

§3.9 设计：起步全稠密；接口不暴露稠密/稀疏。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field

import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import EntryTiming
from qlab.core.exceptions import FeatureComputationError
from qlab.data.layer import DataLayer
from qlab.data.universe import Universe, UniverseSpec
from qlab.features.alignment import align_features_for_entry
from qlab.features.base import Feature, FeatureMeta, FeatureValueMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry
from qlab.features.store import FeatureStore, InMemoryFeatureStore, make_feature_key


@dataclass
class FeatureMatrix:
    """已构建的特征矩阵.

    **契约**: ``values`` 第 T 行 = 在 ``entry_timing`` 决策之前可知的因子值。
    拼到样本上时请用 :func:`~qlab.features.alignment.attach_features_to_events`，
    勿直接 ``reindex`` 绕过入场时点校验。
    """

    values: pd.DataFrame
    """MultiIndex(date, symbol) × feature columns（已按 entry_timing 对齐）."""

    metas: dict[str, FeatureMeta]
    """每列的元数据."""

    mask: pd.DataFrame | None = None
    """与 values 同形状的 bool 矩阵，True=可信. None 表示未生成."""

    value_metas: dict[str, FeatureValueMeta] = field(default_factory=dict)
    """每列的计算审计元数据（feature_value 级别）."""

    entry_timing: EntryTiming = EntryTiming.OPEN
    """本矩阵的 as-of 入场时点；必须与 Event.entry_timing 一致才能拼接."""

    @property
    def feature_names(self) -> list[str]:
        return list(self.values.columns)

    @property
    def date_range(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        dates = self.values.index.get_level_values("date").unique()
        return dates.min(), dates.max()

    def trustworthy_values(self) -> pd.DataFrame:
        """返回 mask 应用后的矩阵——不可信单元被 NaN 替换. 无 mask 时原样返回."""
        if self.mask is None:
            return self.values.copy()
        return self.values.where(self.mask)

    def __repr__(self) -> str:
        return (f"<FeatureMatrix shape={self.values.shape} "
                f"features={len(self.feature_names)} "
                f"entry={self.entry_timing.value} "
                f"mask={'on' if self.mask is not None else 'off'}>")


def _topological_sort(features: Sequence[Feature]) -> list[Feature]:
    """按 dependencies 拓扑排序, 并从 registry **自动补齐**缺失依赖.

    依赖因子已在 registry 中可查时无需让调用方手工列出 ——
    ``features=['mom_resid_20d']`` 应直接可用(自动拉入 ``mom_20d``)。
    补齐的依赖会**保留在结果列中**: 它们本身也是有效特征,
    且已经算好, 丢弃只是浪费。

    Raises:
        FeatureComputationError: 循环依赖, 或依赖既不在入参也不在 registry。
    """
    name_to_feat = {f.meta.name: f for f in features}
    visited: set[str] = set()
    result: list[Feature] = []

    def visit(name: str, stack: set[str]) -> None:
        if name in visited:
            return
        if name in stack:
            raise FeatureComputationError(name, "检测到循环依赖")
        stack.add(name)
        feat = name_to_feat.get(name)
        if feat is None:
            # 从 registry 自动补齐(dependencies 元数据就是为此而存)
            if registry.has(name):
                feat = registry.get(name)
                name_to_feat[name] = feat
            else:
                raise FeatureComputationError(
                    name,
                    f"依赖 '{name}' 既不在 features 入参中, 也未在 registry 注册。\n"
                    "  要么把它加进 features 列表, 要么先"
                    "registry.register(instance=...) 注册它。",
                )
        for dep in feat.meta.dependencies:
            visit(dep, stack)
        stack.remove(name)
        visited.add(name)
        result.append(feat)

    for f in features:
        visit(f.meta.name, set())
    return result


def _compute_pipeline_hash(
    metas: list[FeatureMeta], dataset_id: str, universe_id: str,
    date_range: tuple, data_version: str, entry_timing: str,
) -> str:
    """整条特征流水线的哈希——含全部 feature (name,version) + 数据上下文 + 入场对齐."""
    spec = {
        "features": sorted([(m.name, m.version) for m in metas]),
        "dataset_id": dataset_id,
        "universe_id": universe_id,
        "start": str(date_range[0])[:10],
        "end": str(date_range[1])[:10],
        "data_version": data_version,
        "entry_timing": entry_timing,
    }
    return hashlib.sha1(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:16]


def _build_mask(
    data: DataLayer, universe: Universe,
    target_dates: pd.DatetimeIndex, feature_names: list[str],
) -> pd.DataFrame:
    """生成 mask DataFrame: True=可信. 涨跌停 / 停牌单元 → False.

    返回形状 = (target_dates × universe.all_symbols, feature_names).
    """
    symbols = universe.all_symbols()
    bars = data.daily(
        symbols, target_dates[0], target_dates[-1],
        validate=False,
    )
    keep_cols = [c for c in ("is_suspended", "is_limit_up", "is_limit_down") if c in bars.columns]
    if not keep_cols:
        # 数据源没给状态字段——视为全部可信
        idx = pd.MultiIndex.from_product([target_dates, symbols], names=["date", "symbol"])
        return pd.DataFrame(True, index=idx, columns=feature_names)

    status = bars[keep_cols].copy()
    is_sus = status.get("is_suspended", pd.Series(False, index=status.index)).fillna(False)
    is_up = status.get("is_limit_up", pd.Series(False, index=status.index)).fillna(False)
    is_dn = status.get("is_limit_down", pd.Series(False, index=status.index)).fillna(False)
    trustworthy = ~(is_sus | is_up | is_dn)

    idx = pd.MultiIndex.from_product([target_dates, symbols], names=["date", "symbol"])
    trustworthy = trustworthy.reindex(idx, fill_value=True)
    return pd.DataFrame(
        {name: trustworthy for name in feature_names},
        index=idx,
    )


def build_feature_matrix(
    features: Sequence[Feature | str],
    data: DataLayer,
    universe: Universe | UniverseSpec | str,
    date_range: tuple[str | pd.Timestamp, str | pd.Timestamp],
    *,
    calendar: Calendar | None = None,
    feature_store: FeatureStore | None = None,
    mode: str = "batch",
    dataset_id: str = "default",
    generate_mask: bool = True,
    entry_timing: EntryTiming | str = EntryTiming.OPEN,
) -> FeatureMatrix:
    """构建 FeatureMatrix.

    参数
    ----
    features : 特征实例列表或注册名列表
    data : DataLayer 实例
    universe : Universe 实例 / UniverseSpec / 字符串
    date_range : (start, end)
    calendar : 交易日历（默认 A 股）
    feature_store : 特征缓存（默认内存）
    mode : 'batch' 或 'incremental'
    dataset_id : 数据集标识（影响缓存键）
    generate_mask : 是否生成 mask DataFrame 标记不可信单元（涨跌停、停牌）.
                    默认 True. 设为 False 在没有 DataSource 状态字段时使用.
    entry_timing : 样本入场时点，决定按 ``available_at`` 如何 shift。

        默认 ``open``（与 ``to_event_dataframe`` 一致）。对齐后矩阵满足：

        **第 T 行的所有因子值，在 T 的该入场时点之前均已可知。**

        - ``today_open`` → 留在 T（竞价等）
        - ``today_close`` / ``next_open`` → shift 到 T+1 行

        接到样本时用 ``attach_features_to_events(events, matrix)``，
        它会强制 ``events.entry_timing == matrix.entry_timing``。
    """
    timing = EntryTiming(entry_timing)
    calendar = calendar or get_default_calendar()
    feature_store = feature_store or InMemoryFeatureStore()

    # 1. 解析 features
    resolved: list[Feature] = []
    for f in features:
        if isinstance(f, str):
            resolved.append(registry.get(f))
        else:
            resolved.append(f)

    # 2. 解析 universe
    if isinstance(universe, (str, UniverseSpec)):
        uni = data.universe(universe, *date_range)
    else:
        uni = universe

    # 3. 拓扑排序
    ordered = _topological_sort(resolved)

    # 4. 准备日期
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    target_dates = calendar.trading_days(start, end)

    # 4b. **退化输入提前返回** —— 空 universe / 空 features / 空日期区间
    # 在真实研究中很常见(筛选后无标的、区间内无交易日)。
    # 不拦的话会在内部抛出 pandas 原始错误
    # ("None of [Index(['close'])] are in the columns" / "No objects to concatenate"),
    # 完全看不出真实原因。返回结构正确的空矩阵, 让下游 concat/reindex 能照常工作。
    symbols = list(uni.all_symbols())
    if not ordered or not symbols or len(target_dates) == 0:
        empty_idx = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([]), pd.Index([], dtype=object)],
            names=["date", "symbol"],
        )
        empty_metas = {f.meta.name: f.meta for f in ordered}
        return FeatureMatrix(
            values=pd.DataFrame(
                {name: pd.Series(dtype="float64") for name in empty_metas},
                index=empty_idx,
            ),
            metas=empty_metas,
            mask=None,
            value_metas={},
            entry_timing=timing,
        )

    # 流水线哈希（用 ordered 的全部 meta 一起算，便于审计）
    data_version = getattr(data.source, "source_version", "unknown")
    pipeline_hash = _compute_pipeline_hash(
        [f.meta for f in ordered], dataset_id, uni.name, (start, end),
        data_version, timing.value,
    )

    # 5. 依次计算（缓存存**未按入场对齐**的原始值；对齐在步骤 7）
    computed: dict[str, pd.Series] = {}
    metas: dict[str, FeatureMeta] = {}
    value_metas: dict[str, FeatureValueMeta] = {}

    for feat in ordered:
        meta = feat.meta
        cache_key = make_feature_key(meta, dataset_id, uni.name, (start, end))

        if feature_store.has(cache_key):
            value = feature_store.get(cache_key)
            cached_meta = feature_store.get_meta(cache_key)
            if cached_meta is not None:
                value_metas[meta.name] = cached_meta
        else:
            ctx = FeatureContext(
                data=data,
                target_dates=target_dates,
                universe=uni,
                calendar=calendar,
                mode=mode,
                history_extra_days=meta.lookback_days,
                upstream_values={
                    name: computed[name] for name in meta.dependencies if name in computed
                },
            )
            try:
                value = feat.compute(ctx)
            except Exception as e:
                raise FeatureComputationError(meta.name, str(e), cause=e) from e

            value.name = meta.name
            vm = FeatureValueMeta(
                feature_name=meta.name,
                feature_version=meta.version,
                computed_at=FeatureValueMeta.now_iso(),
                dataset_id=dataset_id,
                universe_id=uni.name,
                date_range=(str(start.date()), str(end.date())),
                pipeline_hash=pipeline_hash,
                data_version=data_version,
            )
            feature_store.put(cache_key, value, meta=vm)
            value_metas[meta.name] = vm

        computed[meta.name] = value
        metas[meta.name] = meta

    # 6. 横向拼接（稠密）
    df = pd.concat(computed.values(), axis=1, keys=computed.keys())
    df.columns = list(computed.keys())

    # 限定到 target_dates × universe
    # （特征计算时可能产生超出范围的索引，统一裁剪）
    universe_idx = pd.MultiIndex.from_product(
        [target_dates, uni.all_symbols()], names=["date", "symbol"]
    )
    df = df.reindex(universe_idx)

    # 7. 按 entry_timing × available_at 做 PIT 对齐
    df = align_features_for_entry(df, metas, timing)

    mask = None
    if generate_mask:
        try:
            mask = _build_mask(data, uni, target_dates, list(metas.keys()))
        except Exception:
            # 状态字段缺失等情况下不致命——降级为不生成 mask
            mask = None

    return FeatureMatrix(
        values=df, metas=metas, mask=mask, value_metas=value_metas,
        entry_timing=timing,
    )
