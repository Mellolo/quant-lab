"""Universe — PIT 正确的成分股查询.

设计：
- UniverseSpec 是声明（指数代码 / 自定义列表 / 全 A 等）
- Universe 是查询接口，按日返回成分股集合
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from qlab.core.exceptions import UniverseError


@dataclass(frozen=True)
class UniverseSpec:
    """Universe 规范声明."""

    name: str  # 'csi300' / 'csi500' / 'csi800' / 'csi1000' / 'all_a' / 'all_a_raw' / 'custom:xxx'

    @classmethod
    def csi300(cls) -> UniverseSpec:
        return cls("csi300")

    @classmethod
    def csi500(cls) -> UniverseSpec:
        return cls("csi500")

    @classmethod
    def csi800(cls) -> UniverseSpec:
        return cls("csi800")

    @classmethod
    def csi1000(cls) -> UniverseSpec:
        return cls("csi1000")

    @classmethod
    def all_a(cls, exclude_st: bool = True, min_listing_days: int = 30) -> UniverseSpec:
        suffix = f"st={int(exclude_st)},days={min_listing_days}"
        return cls(f"all_a:{suffix}")

    @classmethod
    def custom(cls, name: str) -> UniverseSpec:
        return cls(f"custom:{name}")


class Universe:
    """PIT 成分股查询.

    底层存储是 DataFrame，但对外暴露按日查询的语义。
    """

    def __init__(self, df: pd.DataFrame, spec: UniverseSpec):
        """
        df : MultiIndex(date, symbol) 的 DataFrame，含 in_universe / weight 列
        spec : 对应的 UniverseSpec
        """
        if not isinstance(df.index, pd.MultiIndex):
            raise UniverseError("Universe DataFrame 必须是 MultiIndex(date, symbol)")
        if "in_universe" not in df.columns:
            raise UniverseError("Universe DataFrame 缺少 in_universe 列")
        self._df = df.sort_index()
        self._spec = spec

    @property
    def spec(self) -> UniverseSpec:
        return self._spec

    @property
    def name(self) -> str:
        return self._spec.name

    def members(self, date: pd.Timestamp) -> list[str]:
        """返回某日的成分股代码列表."""
        date = pd.Timestamp(date).normalize()
        try:
            sub = self._df.xs(date, level="date")
        except KeyError:
            return []
        return sub[sub["in_universe"]].index.tolist()

    def weights(self, date: pd.Timestamp) -> pd.Series:
        """返回某日的成分股权重（指数 universe 有效）."""
        date = pd.Timestamp(date).normalize()
        try:
            sub = self._df.xs(date, level="date")
        except KeyError:
            return pd.Series(dtype=float)
        return sub.loc[sub["in_universe"], "weight"].dropna()

    def is_member(self, date: pd.Timestamp, symbol: str) -> bool:
        """某日某股票是否在 universe 内."""
        return symbol in self.members(date)

    def date_range(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        dates = self._df.index.get_level_values("date").unique()
        return dates.min(), dates.max()

    def all_symbols(self) -> list[str]:
        """所有曾出现过的成分股. 用于上游数据拉取."""
        symbols = self._df.index.get_level_values("symbol").unique()
        return sorted(symbols.tolist())

    def as_dataframe(self) -> pd.DataFrame:
        """返回完整 DataFrame 副本（不希望被修改）."""
        return self._df.copy()

    def intersect(self, mask: pd.Series, *, name_suffix: str = "") -> Universe:
        """与 bool mask 求交，得到更小的 Universe.

        参数
        ----
        mask : MultiIndex(date, symbol) 的 bool Series；True = 保留。
        name_suffix : 追加到 spec.name（便于审计），如 ``'|amt20d'``。
        """
        if not isinstance(mask.index, pd.MultiIndex):
            raise UniverseError("mask 必须是 MultiIndex(date, symbol)")
        m = mask.reindex(self._df.index).fillna(False).astype(bool)
        keep = self._df["in_universe"].astype(bool) & m
        out = self._df.copy()
        out["in_universe"] = keep
        # 权重：剔出的置 NaN，在池内重新归一（若原权重可用）
        if "weight" in out.columns:
            w = out["weight"].where(keep)
            sums = w.groupby(level="date").transform("sum")
            out["weight"] = w / sums.replace(0, np.nan)
        suffix = name_suffix or "|filtered"
        new_spec = UniverseSpec(f"{self._spec.name}{suffix}")
        return Universe(out, new_spec)

    def __repr__(self) -> str:
        start, end = self.date_range()
        n_symbols = len(self.all_symbols())
        return (f"Universe(spec={self._spec.name}, "
                f"dates={start.date()}~{end.date()}, n_symbols={n_symbols})")


def filter_by_dollar_volume(
    universe: Universe,
    daily: pd.DataFrame,
    *,
    min_avg_amount: float = 5.0e7,
    lookback_days: int = 20,
) -> Universe:
    """成交额宇宙过滤 — trading-books 07/12 流动性门槛.

    保留滚动 ``lookback_days`` 日均成交额（``amount``，元）≥ ``min_avg_amount``
    的 (date, symbol)。默认 20 日均额 ≥ 5e7（约 5000 万）。

    使用截至当日的 amount（``today_close`` 语义）。开盘入场时请用
    T 日过滤结果服务 T+1 开盘，或先对 mask ``groupby(symbol).shift(1)``。

    参数
    ----
    universe : 基础宇宙
    daily : DailyBar（至少含 ``amount``），index=(date, symbol)
    min_avg_amount : 日均成交额下限（元）
    lookback_days : 滚动窗口
    """
    if "amount" not in daily.columns:
        raise UniverseError("filter_by_dollar_volume 需要 daily 含 amount 列")
    if lookback_days < 1:
        raise ValueError("lookback_days 必须 >= 1")

    amt = daily["amount"].astype("float64").unstack("symbol")
    avg = amt.rolling(lookback_days, min_periods=max(1, lookback_days // 2)).mean()
    mask = (avg >= float(min_avg_amount)).stack(future_stack=True)
    mask.index.names = ["date", "symbol"]
    return universe.intersect(
        mask,
        name_suffix=f"|amt{lookback_days}d>={min_avg_amount:.0e}",
    )
