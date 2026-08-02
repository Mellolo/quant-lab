"""Fundamental 数据查询工具 — PIT 安全.

§3.14 设计：(announce_date, symbol, report_type) 三维索引。
PIT 查询语义：在 available_at ≤ t 的所有记录里，按 report_period 取最近一期；
同一 report_period 优先级 official > flash > forecast。
"""

from __future__ import annotations

import pandas as pd

from qlab.core.enums import ReportType


def latest_fundamental_as_of(
    fundamentals: pd.DataFrame,
    field: str,
    date: pd.Timestamp,
    symbols: list[str] | None = None,
) -> pd.Series:
    """对每个 symbol 返回 date 时刻可见的 field 最新值.

    参数
    ----
    fundamentals : Fundamental schema 的 DataFrame
    field : 业务字段名（如 'net_profit_to_shareholders'）
    date : 查询时点
    symbols : 限定股票（None 表示所有）

    返回
    ----
    Series indexed by symbol，值为该字段在 date 时刻可见的最新值（找不到为 NaN）。
    """
    date = pd.Timestamp(date)
    df = fundamentals

    # 1) 过滤：available_at ≤ date
    df = df[df["available_at"] <= date]
    if symbols is not None:
        df = df[df["symbol"].isin(symbols)]

    if df.empty:
        if symbols:
            return pd.Series(index=symbols, dtype=float)
        return pd.Series(dtype=float)

    # 2) 排序：按 (symbol, report_period desc, report_type priority desc)
    df = df.copy()
    df["_priority"] = df["report_type"].map(
        lambda x: ReportType(x).priority if isinstance(x, str) else x.priority
    )
    df = df.sort_values(
        ["symbol", "report_period", "_priority"],
        ascending=[True, False, False],
    )

    # 3) 每个 symbol 取第一条（最新 report_period 的最高优先级版本）
    latest = df.groupby("symbol").first()
    return latest[field]


def ttm_value(
    fundamentals: pd.DataFrame,
    field: str,
    date: pd.Timestamp,
    symbols: list[str] | None = None,
) -> pd.Series:
    """计算 Trailing Twelve Months 累计值（针对损益类字段如 net_profit）.

    简化策略：当前 fiscal_quarter 累计 + 上年报年累计 - 上年同期累计。
    若数据不全，返回 NaN。
    """
    date = pd.Timestamp(date)
    df = fundamentals[fundamentals["available_at"] <= date].copy()
    if symbols is not None:
        df = df[df["symbol"].isin(symbols)]

    if df.empty:
        if symbols:
            return pd.Series(index=symbols, dtype=float)
        return pd.Series(dtype=float)

    # 仅用 official 报告做 TTM（更准确）
    df = df[df["report_type"] == ReportType.OFFICIAL.value]

    out = {}
    for symbol, grp in df.groupby("symbol"):
        grp = grp.sort_values("report_period")
        if grp.empty:
            continue

        last = grp.iloc[-1]
        last_value = last[field]
        last_quarter = int(last["fiscal_quarter"])
        last_year = int(last["fiscal_year"])

        if pd.isna(last_value):
            continue

        if last_quarter == 4:
            # 已是年报，直接用
            out[symbol] = last_value
            continue

        # 找上年年报
        prev_annual = grp[(grp["fiscal_year"] == last_year - 1) & (grp["fiscal_quarter"] == 4)]
        if prev_annual.empty:
            continue
        prev_annual_value = prev_annual.iloc[-1][field]

        # 找上年同期
        prev_same = grp[(grp["fiscal_year"] == last_year - 1) & (grp["fiscal_quarter"] == last_quarter)]
        if prev_same.empty:
            continue
        prev_same_value = prev_same.iloc[-1][field]

        if pd.isna(prev_annual_value) or pd.isna(prev_same_value):
            continue

        out[symbol] = last_value + prev_annual_value - prev_same_value

    result = pd.Series(out, name=f"{field}_ttm")
    if symbols is not None:
        result = result.reindex(symbols)
    return result
