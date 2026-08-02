"""Meta-Labeling — 书 Ch3 §3.6.

辅助函数：给已有方向（来自主模型）的事件，生成 {0, 1} 二分类标签。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def to_meta_labels(events: pd.DataFrame, side: pd.Series) -> pd.DataFrame:
    """把方向信号加到 events 上，转为 meta-labeling 输入.

    Args:
        events: 事件 DataFrame, index=event_start, 含 ``symbol`` 列。
        side: 主模型给出的方向, 值 ∈ {-1, 1}。索引形式二选一:

            - ``MultiIndex(event_start, symbol)`` —— **推荐**, 多标的下唯一可靠;
            - ``DatetimeIndex(event_start)`` —— 仅单标的场景安全。

    Returns:
        ``events`` 副本, 新增 ``side`` 列。

    Raises:
        ValueError: 多标的场景下 side 只按 event_start 索引 —— 此时同一天
            多个标的无法区分, 按位置对齐会把 A 股的方向配给 B 股。

    Note:
        多标的时 ``event_start`` **必然重复**(同一天多个标的触发事件),
        直接 ``side.reindex(events.index)`` 会抛
        ``cannot reindex on an axis with duplicate labels``, 或在索引顺序
        恰好一致时侥幸通过却埋下错配风险 —— 故按 (event_start, symbol) 对齐。
    """
    events = events.copy()
    has_symbol = "symbol" in events.columns
    n_dup = int(pd.Index(events.index).duplicated().sum())

    # side 已是 (event_start, symbol) 二级索引: 按主键精确对齐
    if isinstance(side.index, pd.MultiIndex) and has_symbol:
        key = pd.MultiIndex.from_arrays(
            [events.index, events["symbol"]], names=["event_start", "symbol"]
        )
        aligned = side.reindex(key)
        events["side"] = aligned.to_numpy()
        return events

    # side 仅按时间索引
    if n_dup > 0:
        if not has_symbol:
            raise ValueError(
                "events 的 event_start 有重复但缺少 symbol 列, 无法对齐 side。"
            )
        # 逐 (event_start, symbol) 定位: side 需按 symbol 拆分才能区分
        raise ValueError(
            f"events 含 {n_dup} 个重复的 event_start(多标的场景), 但 side 只按 "
            "event_start 索引 —— 同一天的多个标的无法区分, 会把某只股的方向"
            "配给另一只。\n"
            "  出路: 把 side 组织成 MultiIndex(event_start, symbol), 例如\n"
            "      side.index = pd.MultiIndex.from_arrays([dates, symbols])"
        )

    events["side"] = side.reindex(events.index).to_numpy()
    return events


def meta_label_bins(labels: pd.DataFrame) -> pd.Series:
    """从三屏障结果生成 meta 标签 {0, 1}.

    1 = 主模型方向正确(该下注), 0 = 方向错误(该放弃)。
    与直接用 ``bin`` 的区别: meta 标签只判断"要不要下注", 不判断方向。
    """
    if "ret" not in labels.columns:
        raise ValueError("labels 需含 'ret' 列(三屏障实现收益)")
    return pd.Series(
        np.where(labels["ret"] > 0, 1, 0), index=labels.index, name="meta_bin"
    )
