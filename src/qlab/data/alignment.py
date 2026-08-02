"""IntradayAligner — 日内数据派生为日级特征的对齐器.

数据层提供给特征层的最高抽象。特征层不需要知道：
  - 分钟数据怎么 lazy load
  - 怎么处理停牌日的缺口
  - PIT 怎么保证不泄漏
  - 怎么并行

简化版（v1）：内存中已加载的 intraday DataFrame 直接处理。
未来扩展（v2）：lazy load 分钟数据 + 文件分片并行。
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar
from qlab.core.enums import Freq


class IntradayAligner:
    """日内 → 日级特征对齐器."""

    def __init__(
        self,
        intraday: pd.DataFrame | Callable[[list[str], pd.Timestamp, pd.Timestamp], pd.DataFrame],
        calendar: Calendar,
    ):
        """
        intraday : 可以是已加载的 DataFrame，也可以是 lazy load 函数
                   (symbols, start, end) -> DataFrame
        calendar : 交易日历
        """
        self._intraday = intraday
        self._cal = calendar

    def _load_window(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """加载某窗口的分钟数据."""
        if callable(self._intraday):
            return self._intraday(symbols, start, end)
        df = self._intraday
        # 索引可能是 MultiIndex(timestamp, symbol) 或 (date, symbol, time)
        if isinstance(df.index, pd.MultiIndex):
            mask = (
                (df.index.get_level_values("timestamp") >= start)
                & (df.index.get_level_values("timestamp") <= end)
                & (df.index.get_level_values("symbol").isin(symbols))
            )
            return df.loc[mask]
        # 平坦索引（仅 timestamp）
        return df.loc[(df.index >= start) & (df.index <= end)]

    def apply_rolling(
        self,
        compute_fn: Callable[[pd.DataFrame], float],
        symbols: list[str],
        dates: pd.DatetimeIndex,
        lookback_days: int,
        freq: Freq = Freq.MIN_30,
    ) -> pd.Series:
        """对每个 (date, symbol) 调用 compute_fn(过去 lookback_days 的日内数据).

        返回 MultiIndex(date, symbol) 的 Series.

        重要：PIT 切片严格 ≤ date 当日的最后一个 bar（含集合竞价）.
        """
        out: dict[tuple[pd.Timestamp, str], float] = {}

        for date in dates:
            # 历史窗口
            start_date = self._cal.prev_trading_day(date, lookback_days - 1)
            start_ts = pd.Timestamp(start_date).normalize()
            end_ts = pd.Timestamp(date).normalize() + pd.Timedelta(hours=15)

            window = self._load_window(symbols, start_ts, end_ts)
            if window.empty:
                for s in symbols:
                    out[(date, s)] = np.nan
                continue

            # 按 symbol 分组计算
            if isinstance(window.index, pd.MultiIndex):
                for symbol in symbols:
                    try:
                        sym_window = window.xs(symbol, level="symbol")
                    except KeyError:
                        out[(date, symbol)] = np.nan
                        continue
                    if sym_window.empty:
                        out[(date, symbol)] = np.nan
                    else:
                        try:
                            out[(date, symbol)] = float(compute_fn(sym_window))
                        except Exception:
                            out[(date, symbol)] = np.nan
            else:
                # 单一 symbol 模式
                for symbol in symbols:
                    if window.empty:
                        out[(date, symbol)] = np.nan
                    else:
                        try:
                            out[(date, symbol)] = float(compute_fn(window))
                        except Exception:
                            out[(date, symbol)] = np.nan

        idx = pd.MultiIndex.from_tuples(out.keys(), names=["date", "symbol"])
        return pd.Series(list(out.values()), index=idx, name="value").sort_index()
