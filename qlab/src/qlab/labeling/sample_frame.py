"""研究用「采样 + 因子 + 标签」唯一安全入口.

禁止
----
``sampler.sample_per_symbol`` → ``to_event_dataframe(open)`` → 手拼因子。
那会把**确认日**当成开盘入场日，引入未来函数。

推荐
----
只调用 :func:`build_labeled_samples`：内部固定

1. :class:`~qlab.labeling.sample_spec.SampleSpec` 产出入场日事件与标签；
2. ``build_feature_matrix(entry_timing=spec.event_entry_timing)``；
3. :func:`~qlab.features.alignment.attach_features_to_events` 拼接。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd

from qlab.core.calendar import Calendar
from qlab.features.alignment import attach_features_to_events
from qlab.features.base import Feature
from qlab.features.matrix import FeatureMatrix, build_feature_matrix
from qlab.labeling.sample_spec import (
    SampleSpec,
    _infer_price_end,
    _resolve_label_prices,
)


@dataclass(frozen=True)
class LabeledSamples:
    """一次安全采样的产物.

    Attributes
    ----------
    events :
        入场日事件表（含 ``entry_timing`` / ``event_id``），与 ``labels`` 同行对齐。
    labels :
        三重屏障标签。
    X :
        已按入场时点对齐的因子，index=``(event_start, symbol)``。
    feature_matrix :
        完整日频特征矩阵（已 PIT 对齐）。
    spec :
        所用采样合约。
    """

    events: pd.DataFrame
    labels: pd.DataFrame
    X: pd.DataFrame
    feature_matrix: FeatureMatrix
    spec: SampleSpec


def _events_matching_labels(
    events: pd.DataFrame,
    labels: pd.DataFrame,
) -> pd.DataFrame:
    """按 ``event_id``（优先）把 events 裁到与 labels 同一批、同一顺序."""
    if events.empty or labels.empty:
        return events.iloc[0:0].copy()

    if "event_id" in labels.columns and "event_id" in events.columns:
        order = labels["event_id"].tolist()
        keyed = events.reset_index().set_index("event_id")
        missing = [i for i in order if i not in keyed.index]
        if missing:
            raise ValueError(
                f"labels 中有 {len(missing)} 个 event_id 不在 events 中，无法拼因子。"
            )
        return keyed.loc[order].reset_index().set_index("event_start")

    ev = events.reset_index()
    lab = labels.reset_index()[["event_start", "symbol"]]
    merged = lab.merge(ev, on=["event_start", "symbol"], how="left", indicator=True)
    if (merged["_merge"] != "both").any():
        n = int((merged["_merge"] != "both").sum())
        raise ValueError(f"{n} 条 label 找不到对应 event，无法拼因子。")
    return merged.drop(columns=["_merge"]).set_index("event_start")


def build_labeled_samples(
    spec: SampleSpec,
    prices: pd.DataFrame,
    *,
    target: pd.Series | float,
    features: Sequence[str | Feature],
    data: Any,
    universe: Any,
    date_range: tuple[Any, Any],
    side: pd.Series | int | None = None,
    calendar: Calendar | None = None,
    label_prices: pd.DataFrame | None = None,
    open: pd.DataFrame | None = None,
    close: pd.DataFrame | None = None,
    volume: pd.DataFrame | None = None,
    price_end: pd.Timestamp | None = None,
    feature_columns: list[str] | None = None,
    generate_mask: bool = True,
    drop_na_features: bool = False,
    **label_kwargs: Any,
) -> LabeledSamples:
    """采样合约 + PIT 因子 + 标签（防未来函数的推荐入口）.

    Parameters
    ----------
    spec :
        采样合约。``event_entry_timing`` 会强制传给特征矩阵。
    prices :
        采样器用的宽表（多数为 close）；``VolumeCUSUMFilter`` 另传 ``volume=``。
    target / side / open / close / volume / label_prices :
        与 :meth:`SampleSpec.run` 相同。
    features / data / universe / date_range :
        交给 ``build_feature_matrix``；``entry_timing`` **不可自选**，
        固定为 ``spec.event_entry_timing``。
    drop_na_features :
        True 时丢掉全部因子均为 NaN 的样本（序列首端 shift 常见）。

    Returns
    -------
    LabeledSamples
    """
    # 字符串特征名依赖 registry；确保库已导入（副作用注册）
    if any(isinstance(f, str) for f in features):
        import qlab.features.library  # noqa: F401

    timing = spec.event_entry_timing
    at = spec._entry_at()
    px = _resolve_label_prices(
        prices=prices,
        open_=open,
        close=close,
        label_prices=label_prices,
        entry_at=at,
    )
    pe = price_end if price_end is not None else _infer_price_end(px)
    pairs = spec.sample_pairs(prices, volume=volume, calendar=calendar)
    events = spec.build_events(
        pairs, target=target, side=side, calendar=calendar, price_end=pe,
    )
    labels = spec.label(events, px, **label_kwargs)
    events_use = _events_matching_labels(events, labels)

    feature_matrix = build_feature_matrix(
        features=list(features),
        data=data,
        universe=universe,
        date_range=date_range,
        calendar=calendar,
        entry_timing=timing,
        generate_mask=generate_mask,
    )
    X = attach_features_to_events(
        events_use, feature_matrix, columns=feature_columns,
    )

    if drop_na_features and len(X) and "event_id" in events_use.columns:
        keep = X.notna().any(axis=1).to_numpy()
        if not keep.all():
            keep_ids = set(events_use["event_id"].to_numpy()[keep])
            labels = labels[labels["event_id"].isin(keep_ids)].copy()
            events_use = _events_matching_labels(events, labels)
            X = attach_features_to_events(
                events_use, feature_matrix, columns=feature_columns,
            )

    return LabeledSamples(
        events=events_use,
        labels=labels,
        X=X,
        feature_matrix=feature_matrix,
        spec=spec,
    )
