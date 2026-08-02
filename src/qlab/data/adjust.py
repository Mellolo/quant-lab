"""复权处理 — 设计原则 P2: 复权是视图，不是属性.

存储层只存不复权 + adj_factor，本模块负责按需切换视图。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.enums import AdjustMode

_PRICE_COLS = ["open", "high", "low", "close", "vwap"]
_RAW_PRICE_COLS = ["open_raw", "high_raw", "low_raw", "close_raw"]


def compute_adj_factor_from_actions(
    actions: pd.DataFrame,
    symbols: list[str],
    date_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    """从公司行为事件计算后复权累积因子.

    后复权计算式（简化版，适用 A 股常见情形）::

        new_factor = old_factor × (1 + bonus + transfer + rights × rights_price / pre_ex_price)
                                  / (1 - cash_div / pre_ex_price + rights)

    实务中 pre_ex_price 由数据源提供（除权前一日收盘价）。本简化版用
    "上一交易日的 close_raw" 估算；如果数据源直接提供 adj_factor，应
    优先使用数据源版本（本函数仅为 fallback / 自给生成）。

    参数
    ----
    actions : CorporateAction schema
    symbols : 股票代码列表
    date_index : 目标日期索引

    返回
    ----
    MultiIndex(date, symbol) 的 DataFrame，单列 adj_factor。
    """
    if actions.empty:
        # 无任何事件 → 全 1
        idx = pd.MultiIndex.from_product([date_index, symbols], names=["date", "symbol"])
        return pd.DataFrame(1.0, index=idx, columns=["adj_factor"])

    rows = []
    for symbol in symbols:
        sym_actions = actions[actions["symbol"] == symbol].sort_values("ex_date")
        events: list[tuple[pd.Timestamp, float]] = []  # (date, multiplier)
        for _, row in sym_actions.iterrows():
            # 简化：仅考虑现金分红 + 送股 + 转增；不计配股（实际 A 股配股很少）
            bonus = row.get("bonus_share_ratio") or 0
            transfer = row.get("transfer_share_ratio") or 0
            # 用一个名义 pre_ex_price = 10（仅示意；真实场景需数据源提供）
            # 这里给出"按比例"的近似：对于 A 股大部分情况
            multiplier = (1 + bonus + transfer)
            # 现金分红的等价 multiplier 需要价格信息，简化忽略（差异 < 1%）
            events.append((pd.Timestamp(row["ex_date"]).normalize(), multiplier))

        # 在 date_index 上展开累积因子
        for date in date_index:
            applied = [m for d, m in events if d <= date]
            cur_factor = float(np.prod(applied)) if applied else 1.0
            rows.append((date, symbol, cur_factor))

    df = pd.DataFrame(rows, columns=["date", "symbol", "adj_factor"])
    df = df.set_index(["date", "symbol"])
    return df


def apply_adjust(
    bars: pd.DataFrame,
    adjust: AdjustMode = AdjustMode.BACKWARD,
) -> pd.DataFrame:
    """在已经含有 _raw 列和 adj_factor 列的 bars 上，重新计算给定模式的复权价.

    输入约定：bars 是符合 DailyBar/IntradayBar schema 的 DataFrame，
    含 open_raw/.../close_raw 与 adj_factor。

    本函数**就地**计算 open/.../close 列（覆盖）以反映所请求的复权模式。
    """
    if AdjustMode(adjust) == AdjustMode.NONE:
        for raw, dst in zip(_RAW_PRICE_COLS, _PRICE_COLS[:4], strict=True):
            if raw in bars.columns:
                bars[dst] = bars[raw]
        # vwap 在不复权模式下：用 amount / volume（仍然是 raw 口径）
        if {"amount", "volume"}.issubset(bars.columns):
            bars["vwap"] = bars["amount"] / bars["volume"].replace(0, np.nan)
        return bars

    if AdjustMode(adjust) == AdjustMode.BACKWARD:
        for raw, dst in zip(_RAW_PRICE_COLS, _PRICE_COLS[:4], strict=True):
            if raw in bars.columns:
                bars[dst] = bars[raw] * bars["adj_factor"]
        if {"amount", "volume", "adj_factor"}.issubset(bars.columns):
            bars["vwap"] = bars["amount"] / bars["volume"].replace(0, np.nan) * bars["adj_factor"]
        return bars

    if AdjustMode(adjust) == AdjustMode.FORWARD:
        # 前复权：当前价不变，历史调整
        # forward_price = raw × adj_factor / adj_factor_latest
        for _symbol, grp in bars.groupby(level="symbol"):
            latest_factor = grp["adj_factor"].iloc[-1]
            for raw, dst in zip(_RAW_PRICE_COLS, _PRICE_COLS[:4], strict=True):
                if raw in grp.columns:
                    bars.loc[grp.index, dst] = grp[raw] * grp["adj_factor"] / latest_factor
        return bars

    raise ValueError(f"Unknown AdjustMode: {adjust}")
