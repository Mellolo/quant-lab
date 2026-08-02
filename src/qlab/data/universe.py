"""Universe — PIT 正确的成分股查询.

设计：
- UniverseSpec 是声明（指数代码 / 自定义列表 / 全 A 等）
- Universe 是查询接口，按日返回成分股集合
"""

from __future__ import annotations

from dataclasses import dataclass

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

    def __repr__(self) -> str:
        start, end = self.date_range()
        n_symbols = len(self.all_symbols())
        return (f"Universe(spec={self._spec.name}, "
                f"dates={start.date()}~{end.date()}, n_symbols={n_symbols})")
