"""IndustryClassification 查询工具 — PIT 安全."""

from __future__ import annotations

import pandas as pd


def industry_as_of(
    industry: pd.DataFrame,
    symbols: list[str],
    date: pd.Timestamp,
    system: str = "sw",
    level: int = 1,
) -> pd.Series:
    """返回某日某 system/level 下各 symbol 的行业代码.

    使用 merge_asof 找到 ≤ date 的最近变动后归属。

    兼容 SCHEMA_INDUSTRY 的 index 形式(``system`` 在 MultiIndex 里)与平铺列形式:
    先 ``reset_index`` 把 index 层化为列, 再按列过滤。
    """
    date = pd.Timestamp(date).normalize()
    flat = (
        industry.reset_index()
        if isinstance(industry.index, pd.MultiIndex)
        else industry.copy()
    )
    if "system" not in flat.columns or "level" not in flat.columns:
        return pd.Series(index=symbols, dtype="object", name=f"{system}_l{level}")
    df = flat[(flat["system"] == system) & (flat["level"] == level)]

    if df.empty:
        return pd.Series(index=symbols, dtype="object", name=f"{system}_l{level}")

    out = {}
    for symbol in symbols:
        sym_df = df[df["symbol"] == symbol]
        valid = sym_df[sym_df["date"] <= date]
        if valid.empty:
            out[symbol] = None
            continue
        row = valid.sort_values("date").iloc[-1]
        out[symbol] = row["industry_code"]

    return pd.Series(out, name=f"{system}_l{level}")


def industry_matrix_as_of(
    industry: pd.DataFrame,
    symbols: list[str],
    dates: pd.DatetimeIndex,
    system: str = "sw",
    level: int = 1,
) -> pd.DataFrame:
    """返回 (date × symbol) 的行业代码矩阵."""
    out = pd.DataFrame(index=dates, columns=symbols, dtype="object")
    for date in dates:
        out.loc[date] = industry_as_of(industry, symbols, date, system, level)
    return out
