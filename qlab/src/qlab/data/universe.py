"""Universe — PIT 正确的成分股查询.

设计：
- :class:`UniverseSpec` 是声明（指数 / 沪深宽基池）
- :class:`Universe` 是按日查询的成分集合

指数成分随调样变化：数据源按采样日取权重并前填到每个交易日
（见 JQ 实现），禁止「取今天成分用到全部历史」。

成交额等可交易性门槛属于**采样门**（如 ``liquidity_top_n_mask``），
不在宇宙层表达。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from qlab.core.exceptions import UniverseError

# 公开宇宙名 → 说明（也用于解析）
_UNIVERSE_DOC = {
    "csi300": "沪深300 指数成分（PIT）",
    "csi500": "中证500 指数成分（PIT）",
    "csi800": "中证800 指数成分（PIT）",
    "csi1000": "中证1000 指数成分（PIT）",
    "main_a": "沪深A股 − 北交所 − 科创板 − ST（PIT）",
    "hs_a": "沪深A股 − 北交所 − ST（保留科创/创业板，PIT）",
}


@dataclass(frozen=True)
class UniverseSpec:
    """Universe 规范声明.

    宽基池
    ------
    - ``main_a``: 全市场 − 北交所 − 科创板(688) − ST
    - ``hs_a``: 全市场 − 北交所 − ST（含科创板、创业板）

    指数池（成分随调样日变化，PIT）
    --------------------------------
    ``csi300`` / ``csi500`` / ``csi800`` / ``csi1000``
    """

    name: str

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
    def main_a(cls) -> UniverseSpec:
        """全市场 − 北交所 − 科创板 − ST."""
        return cls("main_a")

    @classmethod
    def hs_a(cls) -> UniverseSpec:
        """全市场 − 北交所 − ST（保留科创板）."""
        return cls("hs_a")

    def describe(self) -> str:
        return _UNIVERSE_DOC.get(self.name, self.name)


def is_bj_symbol(symbol: str) -> bool:
    """北交所：``.BJ`` 或代码段 43/83/87/92."""
    s = str(symbol).upper()
    if s.endswith(".BJ"):
        return True
    code = s.split(".", 1)[0]
    return code.startswith(("43", "83", "87", "92"))


def is_star_symbol(symbol: str) -> bool:
    """科创板：沪市 ``688xxx``."""
    s = str(symbol).upper()
    code, _, exch = s.partition(".")
    if exch and exch not in ("SH", "XSHG"):
        return False
    return code.startswith("688")


def apply_board_filters(
    symbols: list[str],
    *,
    exclude_bj: bool = True,
    exclude_star: bool = False,
) -> list[str]:
    """按代码规则剔除板块（与日期无关）."""
    out = []
    for sym in symbols:
        if exclude_bj and is_bj_symbol(sym):
            continue
        if exclude_star and is_star_symbol(sym):
            continue
        out.append(sym)
    return out


def parse_broad_universe(spec: str) -> tuple[bool, bool] | None:
    """若是宽基池，返回 ``(exclude_star, exclude_st)``；否则 None.

    - ``main_a`` → 剔科创、剔 ST
    - ``hs_a`` → 保留科创、剔 ST
    """
    head = str(spec).strip().split(":", 1)[0].lower()
    if head == "main_a":
        return True, True
    if head == "hs_a":
        return False, True
    return None


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
        name_suffix : 追加到 spec.name（便于审计），如 ``'|custom'``。
        """
        if not isinstance(mask.index, pd.MultiIndex):
            raise UniverseError("mask 必须是 MultiIndex(date, symbol)")
        m = mask.reindex(self._df.index).fillna(False).astype(bool)
        keep = self._df["in_universe"].astype(bool) & m
        out = self._df.copy()
        out["in_universe"] = keep
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
        return (
            f"Universe(spec={self._spec.name}, "
            f"dates={start.date()}~{end.date()}, n_symbols={n_symbols})"
        )
