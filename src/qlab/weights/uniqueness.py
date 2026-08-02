"""平均唯一性 + 收益归因权重 — 书 Ch4 §4.3-4.6."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.schema import SCHEMA_SAMPLE_WEIGHT, validate_schema


def num_concurrent_events(
    close_idx: pd.DatetimeIndex,
    t1: pd.Series,
    molecule: list[pd.Timestamp] | None = None,
) -> pd.Series:
    """每个 bar 上重叠事件的数量 c_t — 书 Ch4 Snippet 4.1.

    参数
    ----
    close_idx : 价格序列的时间索引
    t1 : Series indexed by event_start，值为事件结束时间。NaN 表示未结束（用 close_idx[-1]）
    molecule : 限定只算这些事件（并行用）；None 表示全部

    返回
    ----
    Series indexed by close_idx，每个 bar 上活跃事件数 c_t
    """
    t1 = t1.fillna(close_idx[-1])
    if molecule is not None:
        molecule = sorted(molecule)
        t1_active = t1[(t1.index <= max(molecule)) | (t1 >= min(molecule))]
    else:
        t1_active = t1

    # 找需要覆盖的时间范围
    if len(t1_active) == 0:
        return pd.Series(0, index=close_idx)

    start = max(close_idx[0], t1_active.index.min())
    end = min(close_idx[-1], t1_active.max())
    idx_mask = (close_idx >= start) & (close_idx <= end)
    target_idx = close_idx[idx_mask]
    count = pd.Series(0, index=target_idx, dtype=np.int32)

    for t_in, t_out in t1_active.items():
        seg = (target_idx >= t_in) & (target_idx <= t_out)
        count.loc[seg] += 1
    return count


def average_uniqueness(
    t1: pd.Series,
    num_co_events: pd.Series,
) -> pd.Series:
    """每个事件的平均唯一性 ū_i — 书 Ch4 §4.4.

    返回 Series indexed by event_start.
    """
    out = pd.Series(np.nan, index=t1.index)
    co_events_clean = num_co_events.replace(0, np.nan)
    for t_in, t_out in t1.dropna().items():
        try:
            window = co_events_clean.loc[t_in:t_out]
        except KeyError:
            continue
        if window.empty:
            continue
        # u_t = 1/c_t；ū = mean(u_t)
        out.loc[t_in] = (1.0 / window).mean()
    return out


def return_attribution_weights(
    t1: pd.Series,
    num_co_events: pd.Series,
    close: pd.Series,
) -> pd.Series:
    """按收益归因的权重 — 书 Ch4 §4.6.

    w_i = | sum_{t∈[t0,t1]} r_{t-1,t} / c_t |
    """
    log_ret = np.log(close).diff()
    w = pd.Series(np.nan, index=t1.index)
    co_events_clean = num_co_events.replace(0, np.nan)
    for t_in, t_out in t1.dropna().items():
        try:
            seg_ret = log_ret.loc[t_in:t_out]
            seg_co = co_events_clean.loc[t_in:t_out]
        except KeyError:
            continue
        if seg_ret.empty:
            continue
        w.loc[t_in] = (seg_ret / seg_co).sum()
    return w.abs()


def sample_weights(
    labels: pd.DataFrame,
    close: pd.DataFrame,
    *,
    use_return_attribution: bool = True,
    use_uniqueness: bool = True,
    time_decay: float | None = None,
    normalize_sum_to_n: bool = True,
) -> pd.DataFrame:
    """计算样本权重一站式接口.

    参数
    ----
    labels : 含 t1 列的 DataFrame (index=event_start)
    close : MultiIndex(date, symbol) 的价格 DataFrame
    use_return_attribution : 是否按收益归因
    use_uniqueness : 是否按唯一性
    time_decay : 时间衰减参数（与 time_decay_factors 一致；None 表示不衰减）
    normalize_sum_to_n : 是否把 final_weight 归一到总和 = N

    返回
    ----
    DataFrame, index=event_start, 列 [uniqueness, return_attr, time_decay, final_weight]
    """
    from qlab.weights.time_decay import time_decay_factors

    out = pd.DataFrame(index=labels.index)

    # 空输入直接返回结构正确的空表 —— 筛选后无样本在研究中很常见,
    # 不拦会在 groupby("symbol") 处报 KeyError: 'symbol'。
    if len(labels) == 0:
        empty = pd.DataFrame(
            {
                "uniqueness": pd.Series(dtype="float64"),
                "return_attr": pd.Series(dtype="float64"),
                "time_decay": pd.Series(dtype="float64"),
                "final_weight": pd.Series(dtype="float64"),
            },
            index=labels.index,
        )
        return empty

    # 按 symbol 分别算（因为价格不同）
    # 注: 多标的时 event_start **必然重复**(同一天多个标的触发事件),
    # 因此不能用 ``reindex(labels.index)`` 回填(重复索引会 ValueError),
    # 必须按 **(event_start, symbol)** 定位。
    uniq_parts: dict[str, pd.Series] = {}
    attr_parts: dict[str, pd.Series] = {}

    for symbol, sym_events in labels.groupby("symbol"):
        if isinstance(close.index, pd.MultiIndex):
            try:
                sym_close = close.xs(symbol, level="symbol")["close"]
            except KeyError:
                continue
        else:
            sym_close = close["close"] if "close" in close.columns else close.iloc[:, 0]

        co = num_concurrent_events(sym_close.index, sym_events["t1"])

        if use_uniqueness:
            uniq_parts[symbol] = average_uniqueness(sym_events["t1"], co)
        if use_return_attribution:
            attr_parts[symbol] = return_attribution_weights(
                sym_events["t1"], co, sym_close
            )

    def _scatter(parts: dict[str, pd.Series]) -> pd.Series:
        """把 per-symbol 结果按 (event_start, symbol) 回填到 labels 行序."""
        res = pd.Series(np.nan, index=range(len(labels)), dtype="float64")
        sym_col = labels["symbol"].to_numpy()
        starts = labels.index
        for pos in range(len(labels)):
            s = parts.get(sym_col[pos])
            if s is None:
                continue
            key = starts[pos]
            if key in s.index:
                val = s.loc[key]
                # 同一 symbol 内 event_start 应唯一; 万一重复取首个
                res.iloc[pos] = (
                    float(val.iloc[0]) if isinstance(val, pd.Series) else float(val)
                )
        res.index = labels.index
        return res

    out["uniqueness"] = _scatter(uniq_parts) if uniq_parts else 1.0
    out["return_attr"] = _scatter(attr_parts) if attr_parts else 1.0

    # 时间衰减
    if time_decay is not None and use_uniqueness:
        # 按 event_start 时间顺序累加 uniqueness
        # (多 symbol 时 index 有重复, 故先降为位置序列)
        sorted_u = out["uniqueness"].reset_index(drop=True).sort_index()
        decay = time_decay_factors(sorted_u, clf_last_w=time_decay)
        # 按位置回填到原 index（保持 labels.index 顺序）
        out["time_decay"] = decay.values
    else:
        out["time_decay"] = 1.0

    # 合成最终权重
    final = (
        out["uniqueness"].fillna(1.0)
        * out["return_attr"].fillna(1.0)
        * out["time_decay"].fillna(1.0)
    )

    if normalize_sum_to_n and final.sum() > 0:
        n = len(final)
        final = final * n / final.sum()

    out["final_weight"] = final
    validate_schema(out, SCHEMA_SAMPLE_WEIGHT, strict_index=False)
    return out
