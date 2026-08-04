"""FeatureContext — 特征计算时的数据访问上下文.

§4.4 设计：Feature.compute 的唯一入口，封装数据访问 + 计算服务。
预留 batch / incremental 双模式（议题 8.6 决议）。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

import pandas as pd

from qlab.core.calendar import Calendar
from qlab.core.enums import Freq
from qlab.data.layer import DataLayer
from qlab.data.universe import Universe


class FeatureContext:
    """特征计算上下文.

    职责：
    - 提供 PIT 安全的数据访问（daily / intraday / fundamental / industry）
    - 缓存中间结果
    - 支持 batch 与 incremental 两种模式（CI 强制等价性）
    """

    def __init__(
        self,
        data: DataLayer,
        target_dates: pd.DatetimeIndex,
        universe: Universe,
        calendar: Calendar,
        mode: Literal["batch", "incremental"] = "batch",
        history_extra_days: int = 0,
        upstream_values: dict[str, pd.Series] | None = None,
    ):
        """
        data : DataLayer 实例
        target_dates : 本次要算的日期
        universe : 投资域
        calendar : 交易日历
        mode : 'batch'（全量）或 'incremental'（增量）
        history_extra_days : 在 target_dates 之外额外保留多少天历史（满足 lookback）
        upstream_values : 已计算好的依赖特征值
        """
        self.data = data
        self.target_dates = pd.DatetimeIndex(sorted(target_dates))
        self.universe = universe
        self.calendar = calendar
        self.mode = mode
        self.history_extra_days = history_extra_days
        self._upstream = upstream_values or {}

        # PIT 起点
        if len(self.target_dates) > 0:
            self.history_from = self.calendar.prev_trading_day(
                self.target_dates[0], history_extra_days
            )
        else:
            self.history_from = pd.Timestamp("1970-01-01")

    # ---- 数据访问 -----------------------------------------------------------

    def daily(self, fields: list[str] | None = None,
              lookback_days: int | None = None) -> pd.DataFrame:
        """取日线数据.

        参数
        ----
        fields : 想要的列（None 表示所有标准列）
        lookback_days : 在 target_dates 之外多取多少天历史。
                        与 history_extra_days 取最大值。

        返回
        ----
        MultiIndex(date, symbol) 的 DataFrame，已切到 PIT 范围。

        模式语义
        --------
        - batch:       拉 [target_dates[0] - max(lookback, extra), target_dates[-1]]
        - incremental: 同上窗口（只取最小必要的历史）
          关键：两种模式下 compute 看到的数据**前缀完全一致**（同一段 [T0-N, T1]），
          因此相同 compute 函数产出相同结果——这是 §4.4 batch↔incremental 等价性的根基.
        """
        extra = max(lookback_days or 0, self.history_extra_days)
        start = self.calendar.prev_trading_day(self.target_dates[0], extra)
        end = self.target_dates[-1]
        symbols = self.universe.all_symbols()
        df = self.data.daily(symbols, start, end, validate=False)
        if fields is not None:
            df = df[fields]
        return df

    def intraday_rolling(
        self,
        compute_fn: Callable[[pd.DataFrame], float],
        lookback_days: int,
        freq: Freq = Freq.MIN_30,
    ) -> pd.Series:
        """日内派生因子的标准入口.

        对 target_dates 中的每个 (date, symbol)，调用 compute_fn(过去 lookback_days 的日内数据)。
        返回 MultiIndex(date, symbol) 的 Series。
        """
        aligner = self.data.intraday_aligner(freq)
        return aligner.apply_rolling(
            compute_fn=compute_fn,
            symbols=self.universe.all_symbols(),
            dates=self.target_dates,
            lookback_days=lookback_days,
            freq=freq,
        )

    def fundamental(self, field: str, date: pd.Timestamp | None = None,
                    ttm: bool = False) -> pd.Series:
        """PIT 查询财务字段.

        date 为 None 时，对 target_dates 中每个日期查询。
        ttm=True 时返回 trailing 12 months 累计值（针对损益字段）。
        """
        symbols = self.universe.all_symbols()
        if date is not None:
            if ttm:
                return self.data.fundamental_ttm(symbols, field, date)
            return self.data.fundamental_as_of(symbols, field, date)

        # 对每个 target_date 算一次
        out = {}
        for d in self.target_dates:
            if ttm:
                vals = self.data.fundamental_ttm(symbols, field, d)
            else:
                vals = self.data.fundamental_as_of(symbols, field, d)
            for sym, val in vals.items():
                out[(d, sym)] = val
        idx = pd.MultiIndex.from_tuples(out.keys(), names=["date", "symbol"])
        return pd.Series(list(out.values()), index=idx, name=field).sort_index()

    def industry(self, system: str = "sw", level: int = 1,
                 date: pd.Timestamp | None = None) -> pd.Series:
        symbols = self.universe.all_symbols()
        if date is not None:
            return self.data.industry(symbols, date, system, level)
        out = {}
        for d in self.target_dates:
            ind = self.data.industry(symbols, d, system, level)
            for sym, val in ind.items():
                out[(d, sym)] = val
        idx = pd.MultiIndex.from_tuples(out.keys(), names=["date", "symbol"])
        return pd.Series(list(out.values()), index=idx, name=f"industry_{system}_l{level}").sort_index()

    def concepts(self, source: str | None = None,
                 date: pd.Timestamp | None = None) -> pd.DataFrame:
        """查询概念板块归属（一对多）.

        返回 DataFrame: columns=[date, symbol, concept_code, concept_name]。

        ``source`` 缺省时透传 ``None`` 给 :meth:`DataLayer.concepts`, 由其向数据源
        取 ``concept_source``(如 JQDataSource 为 ``"jq"``) —— 避免在此硬编码 eastmoney
        而与非东财数据源(如聚宽)失配导致静默返回空。
        """
        symbols = self.universe.all_symbols()
        if date is not None:
            result = self.data.concepts(symbols, date, source)
            result["date"] = date
            return result

        parts = []
        for d in self.target_dates:
            result = self.data.concepts(symbols, d, source)
            result["date"] = d
            parts.append(result)
        if not parts:
            return pd.DataFrame(columns=["date", "symbol", "concept_code", "concept_name"])
        return pd.concat(parts, ignore_index=True)

    # ---- 两融 / 资金流 / 集合竞价(时序类, 同 daily 的 PIT 窗口模式) --------

    def _timeseries_window(self, fetch_fn, lookback_days: int | None):
        """时序类数据的统一取数窗口 —— 与 :meth:`daily` 同构。

        保证 batch/incremental 两模式下 compute 看到的前缀一致(同一段
        ``[T0-N, T1]``), 是 §4.4 等价性的前提。
        """
        extra = max(lookback_days or 0, self.history_extra_days)
        start = self.calendar.prev_trading_day(self.target_dates[0], extra)
        end = self.target_dates[-1]
        return fetch_fn(self.universe.all_symbols(), start, end)

    def margin_trading(self, lookback_days: int | None = None) -> pd.DataFrame:
        """两融(融资融券). 返回 MarginTrading schema, index=(date, symbol).

        数据发布有 T+1~T+2 滞后, 使用方需自行对齐 available_at。
        """
        return self._timeseries_window(self.data.margin_trading, lookback_days)

    def money_flow(self, lookback_days: int | None = None) -> pd.DataFrame:
        """资金流向. 返回 MoneyFlow schema, index=(date, symbol)."""
        return self._timeseries_window(self.data.money_flow, lookback_days)

    def call_auction(self, lookback_days: int | None = None) -> pd.DataFrame:
        """集合竞价. 返回 CallAuction schema, index=(date, symbol)."""
        return self._timeseries_window(self.data.call_auction, lookback_days)

    # ---- 依赖特征 -----------------------------------------------------------

    def upstream(self, name: str) -> pd.Series:
        """获取依赖特征的值."""
        if name not in self._upstream:
            raise KeyError(f"上游特征 '{name}' 尚未计算或未声明为依赖")
        return self._upstream[name]

    # ---- 元属性 -------------------------------------------------------------

    @property
    def current_universe(self) -> Universe:
        return self.universe

    @property
    def current_date_range(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        return self.target_dates[0], self.target_dates[-1]

    def __repr__(self) -> str:
        return (f"<FeatureContext mode={self.mode} "
                f"dates={self.target_dates[0].date()}~{self.target_dates[-1].date()} "
                f"universe={self.universe.name}>")
