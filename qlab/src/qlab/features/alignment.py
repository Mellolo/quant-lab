"""特征可知时点 ↔ 入场时点对齐，以及「先特征、后采样」的硬门禁.

保证
----
``build_feature_matrix(entry_timing=...)`` 产出的矩阵第 T 行，表示在该
入场时点**之前**已可知的信息（通过对 ``available_at`` 做 shift 实现）。

拼样本时必须走 :func:`attach_features_to_events`：它强制
``FeatureMatrix.entry_timing == events.entry_timing``，不一致则抛
:class:`~qlab.core.exceptions.PITViolationError` —— 不允许把未对齐、
或入场时点不匹配的因子静默接到开盘/收盘样本上。
"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from qlab.core.enums import EntryTiming
from qlab.core.exceptions import FeatureComputationError, PITViolationError
from qlab.features.base import FEATURE_AVAILABLE_AT, FeatureMeta


def feature_shift_days(
    available_at: str,
    entry_timing: EntryTiming | str,
) -> int:
    """原始特征值相对计算日需向前平移几个交易日才能用于该入场时点.

    参数
    ----
    available_at : ``today_open`` / ``today_close`` / ``next_open``
    entry_timing : 样本入场时点（与 labeling.EntryTiming 一致）

    返回
    ----
    0 或 1（当前日频网格下最多移一日）
    """
    timing = EntryTiming(entry_timing)
    avail = str(available_at)
    if avail not in FEATURE_AVAILABLE_AT:
        raise ValueError(
            f"非法 available_at: {avail!r}（支持 {sorted(FEATURE_AVAILABLE_AT)}）"
        )

    if timing == EntryTiming.OPEN:
        # T 开盘决策: 仅 today_open 当日可用；收盘后可知的一律等到 T+1 开盘
        return 0 if avail == "today_open" else 1

    # T 收盘决策: today_open / today_close 当日可用；next_open 等到次日
    return 1 if avail == "next_open" else 0


def assert_features_admissible(
    metas: Mapping[str, FeatureMeta],
    entry_timing: EntryTiming | str,
) -> None:
    """校验特征集合的 available_at 合法（对齐前的静态检查）."""
    timing = EntryTiming(entry_timing)
    bad = []
    for name, meta in metas.items():
        if meta.available_at not in FEATURE_AVAILABLE_AT:
            bad.append(f"{name}: available_at={meta.available_at!r}")
    if bad:
        raise FeatureComputationError(
            "*",
            "特征 available_at 非法，无法按 "
            f"entry_timing={timing.value} 对齐:\n  - " + "\n  - ".join(bad),
        )


def align_features_for_entry(
    values: pd.DataFrame,
    metas: Mapping[str, FeatureMeta],
    entry_timing: EntryTiming | str,
) -> pd.DataFrame:
    """按入场时点对特征列做 PIT 对齐（按 symbol 分组 shift）.

    对齐后：行 T = 该 ``entry_timing`` 下决策前可知的因子值。
    返回新 DataFrame；不修改入参。
    """
    timing = EntryTiming(entry_timing)
    assert_features_admissible(metas, timing)
    out = values.copy()
    if out.empty:
        return out

    for name, meta in metas.items():
        if name not in out.columns:
            continue
        n = feature_shift_days(meta.available_at, timing)
        if n <= 0:
            continue
        out[name] = out.groupby(level="symbol")[name].shift(n)
    return out


def events_entry_timing(events: pd.DataFrame) -> EntryTiming:
    """从 Event 表读取统一的 ``entry_timing``；缺失或不一致则报错."""
    if events is None or events.empty:
        raise PITViolationError(
            "events 为空，无法校验 entry_timing。\n"
            "  请用 to_event_dataframe(..., entry_timing=...) 生成事件。"
        )
    if "entry_timing" not in events.columns:
        raise PITViolationError(
            "events 缺少 entry_timing 列，无法保证因子在采样时点之前可知。\n"
            "  请用 to_event_dataframe(..., entry_timing=...) 生成事件；\n"
            "  不要手写 Event 表却省略入场时点。"
        )
    vals = events["entry_timing"].dropna().astype(str).unique().tolist()
    if not vals:
        raise PITViolationError("events.entry_timing 全为缺失。")
    if len(vals) > 1:
        raise PITViolationError(
            f"同一批 events 混用了多种 entry_timing: {vals}。\n"
            "  开盘样本与收盘样本必须分开构建特征矩阵再拼接。"
        )
    try:
        return EntryTiming(vals[0])
    except ValueError as e:
        raise PITViolationError(
            f"events.entry_timing 非法: {vals[0]!r}（支持 open / close）"
        ) from e


def attach_features_to_events(
    events: pd.DataFrame,
    features: object,
    *,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """把**已按入场时点对齐**的特征接到事件上（硬门禁）.

    这是「因子一定在采样之前可知」的强制入口：

    1. ``features`` 必须是带 ``entry_timing`` 的 :class:`FeatureMatrix`
       （由 ``build_feature_matrix`` 产出，内部已做 available_at 对齐）；
    2. ``events.entry_timing`` 必须与 ``features.entry_timing`` **完全一致**；
    3. 按 ``(event_start 日期, symbol)`` 对齐取行。

    禁止 ``X.values.reindex(events)`` 自行拼接绕过本检查。

    参数
    ----
    events : Event 表（含 symbol、entry_timing）
    features : FeatureMatrix
    columns : 只取部分特征列；默认全部

    返回
    ----
    DataFrame，index=MultiIndex(event_start, symbol)，列=特征。
    行序与 ``events`` 一致。

    Raises
    ------
    PITViolationError : 入场时点缺失、不一致，或 features 未声明 entry_timing
    """
    # 延迟注解避免循环导入；运行时检查接口
    entry = getattr(features, "entry_timing", None)
    values = getattr(features, "values", None)
    if entry is None or values is None or not isinstance(values, pd.DataFrame):
        raise PITViolationError(
            "attach_features_to_events 需要 FeatureMatrix"
            "（含 entry_timing 与已对齐的 values）。\n"
            "  请先 build_feature_matrix(..., entry_timing=与采样相同)。"
        )

    ev_timing = events_entry_timing(events)
    feat_timing = EntryTiming(entry)
    if feat_timing != ev_timing:
        raise PITViolationError(
            "特征矩阵与事件的入场时点不一致，拒绝拼接（防止未来函数）:\n"
            f"  FeatureMatrix.entry_timing = {feat_timing.value}\n"
            f"  events.entry_timing        = {ev_timing.value}\n"
            "  二者必须相同。开盘采样 → build_feature_matrix(entry_timing='open')；\n"
            "  收盘采样 → 两侧都传 'close'。"
        )

    if "symbol" not in events.columns:
        raise PITViolationError("events 缺少 symbol 列。")

    dates = pd.DatetimeIndex(events.index).normalize()
    syms = events["symbol"].to_numpy()
    key = pd.MultiIndex.from_arrays([dates, syms], names=["date", "symbol"])

    cols = list(columns) if columns is not None else list(values.columns)
    missing = [c for c in cols if c not in values.columns]
    if missing:
        raise PITViolationError(
            f"特征矩阵缺少列: {missing}"
        )

    out = values.reindex(key)[cols]
    out.index = pd.MultiIndex.from_arrays(
        [pd.DatetimeIndex(events.index), syms],
        names=["event_start", "symbol"],
    )
    return out
