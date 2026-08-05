"""Triple-Barrier 标注法 — 书 Ch3 §3.4-3.5.

设计：
- Event 输入：(event_start, symbol, t1, target, side[, entry_timing])
- Label 输出：(event_start, symbol, bin, ret, touch_time, touch_type)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from qlab.core.enums import EntryTiming
from qlab.core.parallel import mp_pandas_obj
from qlab.core.schema import SCHEMA_LABEL, validate_schema


@dataclass
class TripleBarrier:
    """三屏障配置.

    pt : 止盈倍数（乘以 event.target 得到止盈宽度）。0 表示无止盈屏障。
    sl : 止损倍数。0 表示无止损屏障。
    """
    pt: float = 1.0
    sl: float = 1.0


def _series_for_symbol(
    prices: pd.DataFrame | pd.Series,
    symbol: str,
    field: str,
) -> pd.Series:
    """从宽表/长表里取出单标的、单字段的日期序列."""
    if isinstance(prices.index, pd.MultiIndex):
        try:
            sub = prices.xs(symbol, level="symbol")
        except KeyError:
            return pd.Series(dtype=float)
        if isinstance(sub, pd.DataFrame):
            if field in sub.columns:
                return sub[field]
            if field == "close" and sub.shape[1] == 1:
                return sub.iloc[:, 0]
            return pd.Series(dtype=float)
        # 已是 Series: 仅当调用方要 close 时视为该序列
        return sub if field == "close" else pd.Series(dtype=float)

    if isinstance(prices, pd.DataFrame):
        if field in prices.columns:
            return prices[field]
        if field == "close" and prices.shape[1] == 1:
            return prices.iloc[:, 0]
        return pd.Series(dtype=float)
    return prices if field == "close" else pd.Series(dtype=float)


def _entry_timing_of(row: pd.Series) -> EntryTiming:
    raw = row["entry_timing"] if "entry_timing" in row.index else EntryTiming.CLOSE
    if pd.isna(raw) or raw == "":
        return EntryTiming.CLOSE
    return EntryTiming(str(raw))


def _apply_barriers_single(
    events: pd.DataFrame,
    close: pd.DataFrame | pd.Series,
    pt: float,
    sl: float,
) -> pd.DataFrame:
    """对一组 events 应用三屏障. 单线程实现.

    输入
    ----
    events : index 是 event_start，列含 t1 / target / side / symbol，
             可选 entry_timing（列缺失时按 close，兼容手写旧表；
             ``to_event_dataframe`` 默认写入 open）
    close  : MultiIndex(date, symbol) 的价格表。
             - 开盘入场: 需 open + close（entry = open_T，盯市用 close）
             - 收盘入场: 需 close 列（或单列 DataFrame / Series）

    返回
    ----
    DataFrame，index 同 events，列 [touch_time, touch_type, ret]
    """
    out = []
    needs_open = (
        "entry_timing" in events.columns
        and (events["entry_timing"].astype(str) == EntryTiming.OPEN.value).any()
    )
    if needs_open:
        if not (isinstance(close, pd.DataFrame) and "open" in close.columns):
            raise ValueError(
                "events 含 entry_timing='open'，但价格表缺少 open 列。\n"
                "  传入含 ['open','close'] 的 DailyBar 子集，例如 "
                "bars[['open','close']]。"
            )

    for event_start, row in events.iterrows():
        symbol = row["symbol"]
        t1 = row["t1"]
        target = row["target"]
        side = row["side"] if "side" in row and not pd.isna(row["side"]) else 1.0
        timing = _entry_timing_of(row)

        if pd.isna(target) or target <= 0:
            out.append({
                "event_start": event_start,
                "touch_time": pd.NaT, "touch_type": "invalid", "ret": np.nan,
            })
            continue

        close_path = _series_for_symbol(close, symbol, "close").loc[event_start:t1]

        if timing == EntryTiming.OPEN:
            # 开盘入场: 至少需要当日一根 close 盯市；首日收益 = close_T/open_T - 1
            if len(close_path) < 1:
                out.append({
                    "event_start": event_start,
                    "touch_time": pd.NaT, "touch_type": "no_data", "ret": np.nan,
                })
                continue
            open_series = _series_for_symbol(close, symbol, "open")
            try:
                entry_price = float(open_series.loc[event_start])
            except KeyError:
                entry_price = float("nan")
            price_path = close_path
        else:
            # 收盘入场: 入场价 = 当日 close，至少还要一根后续 close
            if len(close_path) < 2:
                out.append({
                    "event_start": event_start,
                    "touch_time": pd.NaT, "touch_type": "no_data", "ret": np.nan,
                })
                continue
            entry_price = float(close_path.iloc[0])
            price_path = close_path

        if pd.isna(entry_price) or entry_price <= 0:
            out.append({
                "event_start": event_start,
                "touch_time": pd.NaT, "touch_type": "invalid", "ret": np.nan,
            })
            continue

        # 路径收益（按 side 调整）
        path_returns = (price_path / entry_price - 1) * side

        # 计算触屏障时间
        upper_threshold = pt * target if pt > 0 else np.inf
        lower_threshold = -sl * target if sl > 0 else -np.inf

        touch_upper = path_returns[path_returns >= upper_threshold].index.min()
        touch_lower = path_returns[path_returns <= lower_threshold].index.min()
        touch_vertical = t1

        # 找最早触碰
        candidates = []
        if pd.notna(touch_upper):
            candidates.append((touch_upper, "upper"))
        if pd.notna(touch_lower):
            candidates.append((touch_lower, "lower"))
        candidates.append((touch_vertical, "vertical"))

        candidates.sort(key=lambda x: x[0])
        touch_time, touch_type = candidates[0]

        # 实际收益
        if touch_time in path_returns.index:
            ret = float(path_returns.loc[touch_time])
        else:
            ret = float(path_returns.iloc[-1])

        out.append({
            "event_start": event_start,
            "touch_time": touch_time, "touch_type": touch_type, "ret": ret,
        })

    return pd.DataFrame(out).set_index("event_start")


def _barrier_worker(
    positions: list[int],
    *,
    events: pd.DataFrame,
    close: pd.DataFrame | pd.Series,
    pt: float,
    sl: float,
) -> pd.DataFrame:
    """并行 worker —— 必须在**模块级**才能被 pickle 传给子进程.

    按**位置**而非时间戳切分: 多标的时 event_start 重复,
    ``events.loc[timestamps]`` 会多取行导致结果行数不对。

    返回值的索引也换成**整数位置**(而非 event_start): 位置全局唯一,
    父进程 ``concat + sort_index`` 后就是严格的原始行序 —— 不再依赖
    子任务的返回顺序。用 event_start 做索引时, 重复值之间的排序无法确定,
    下游按位置贴回 ``events.index`` 就会把标签静默错配到别的标的上。
    """
    sub = events.iloc[positions]
    out = _apply_barriers_single(sub, close, pt, sl)
    out.index = pd.Index(positions, name="_pos")
    return out


def label_events(
    events: pd.DataFrame,
    close: pd.DataFrame,
    barrier: TripleBarrier,
    *,
    num_threads: int = 1,
    is_meta_labeling: bool | None = None,
    drop_no_data: bool = True,
) -> pd.DataFrame:
    """对一批 events 应用 triple-barrier 标注.

    参数
    ----
    events : DataFrame, index=event_start, 必备列 [symbol, t1, target]，
             可选 [side, entry_timing]
    close  : MultiIndex(date, symbol) 的价格 DataFrame。

             - ``entry_timing='open'``（``to_event_dataframe`` 默认）:
               **同一张表**需 ``open`` + ``close``；入场价取 open，盯市用 close。
             - ``entry_timing='close'`` 或列缺失: 提供 ``close`` 即可。
    barrier : TripleBarrier(pt, sl)
    num_threads : 并行进程数。

        .. warning::
           ``num_threads > 1`` 走 multiprocessing。macOS/Windows 默认用 spawn,
           子进程会重新导入 ``__main__`` —— 调用方脚本**必须**把入口
           包在 ``if __name__ == "__main__":`` 里, 否则会无限递归创建进程而卡死。
           （Jupyter / pytest 下无此问题。）
    is_meta_labeling : None=自动判断（events 含 side → meta-labeling）
    drop_no_data : 是否剔除**无有效收益**的样本。

        默认 True。两种成因都会被剔:

        1. ``touch_type == 'no_data'`` —— 事件落在数据末尾, 之后无价格可跟;
        2. ``ret`` 为 NaN —— 触及障碍那天正好**停牌**。

        两者的 ``bin`` 都被赋 0, 与“到期且收益≈ 0”的**真实中性**样本
        无法区分 —— 拿去训练等于注入噪声。剔除数量记在
        ``df.attrs['n_dropped_no_data']``。需要保留(如实盘待定仓位)时传 False。

    返回
    ----
    Label DataFrame, index=event_start, 列 [symbol, t1, target, ret, touch_time,
                                            touch_type, bin]
    """
    if events.empty:
        return pd.DataFrame()

    required = ["symbol", "t1", "target"]
    for col in required:
        if col not in events.columns:
            raise ValueError(f"events 缺少必备列: {col}")

    has_side = "side" in events.columns and not events["side"].isna().all()
    if is_meta_labeling is None:
        is_meta_labeling = has_side

    if not has_side:
        events = events.copy()
        events["side"] = 1.0

    # 并行调用: worker 必须是**模块级**函数 —— 局部函数无法 pickle,
    # multiprocessing 传不过去(旧实现 num_threads>1 必报
    # "Can't pickle local object 'label_events.<locals>._worker'")。
    # 切分用**位置**而非时间戳: 多标的时 event_start 重复,
    # ``events.loc[timestamps]`` 会取出多余行。
    if num_threads <= 1:
        result = _apply_barriers_single(events, close, barrier.pt, barrier.sl)
    else:
        result = mp_pandas_obj(
            _barrier_worker,
            pd_obj=("positions", list(range(len(events)))),
            num_threads=num_threads,
            events=events,
            close=close,
            pt=barrier.pt,
            sl=barrier.sl,
        )

    # 合并 events 元数据 (按位置 concat, 避免 join 在重复 event_start 下做笛卡尔积)
    if len(result) != len(events):
        raise ValueError(
            f"label_events: barrier 应用结果行数 ({len(result)}) 与 events ({len(events)}) 不符"
        )
    result_aligned = result.copy()
    result_aligned.index = events.index
    new_cols = [c for c in result_aligned.columns if c not in events.columns]
    full = events.copy()
    for col in new_cols:
        full[col] = result_aligned[col].values

    # 计算 bin
    if is_meta_labeling:
        # meta-labeling: bin ∈ {0, 1}
        full["bin"] = (full["ret"] > 0).astype("int8")
        # ret ≤ 0 强制 bin=0；NaN ret → bin=0
        full.loc[full["ret"] <= 0, "bin"] = 0
        full.loc[full["ret"].isna(), "bin"] = 0
    else:
        # 普通: bin ∈ {-1, 0, 1}
        if barrier.pt == 0 and barrier.sl == 0:
            # 无上下屏障，退化为收益符号标注
            full["bin"] = np.sign(full["ret"].fillna(0)).astype("int8")
        else:
            full["bin"] = 0
            full.loc[full["touch_type"] == "upper", "bin"] = 1
            full.loc[full["touch_type"] == "lower", "bin"] = -1
            full["bin"] = full["bin"].astype("int8")

    # 校验输出符合 SCHEMA_LABEL（index 用宽松模式，因实际是单层 event_start）
    validate_schema(full, SCHEMA_LABEL, strict_index=False)

    # 剔除**无有效收益**的样本。有两种成因:
    #   1. touch_type='no_data' —— 事件落在数据末尾, 之后无价格可跟;
    #   2. touch_type 正常但 ret 为 NaN —— 触及障碍那天正好**停牌**
    #      (实测 6 年×150只 中 32 例, 均为 vertical + 停牌)。
    # 两者的 bin 都被赋 0, 会与“到期且收益≈ 0”的**真实中性**样本
    # 混淆。按 ret 是否有效判定比只看 touch_type 更本质。
    if drop_no_data:
        keep = full["ret"].notna()
        if "touch_type" in full.columns:
            keep &= full["touch_type"] != "no_data"
        n_drop = int((~keep).sum())
        if n_drop:
            full = full[keep]
            full.attrs["n_dropped_no_data"] = n_drop
    return full
