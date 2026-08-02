"""DataSource 与 BarStore Protocol.

任何外部数据源（tushare / akshare / wind / 本地）实现 DataSource 即可接入。
任何缓存后端（内存 / HDF5 / Parquet）实现 BarStore 即可。
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import pandas as pd

from qlab.core.calendar import Calendar
from qlab.core.enums import AdjustMode, Freq, ReportType


@runtime_checkable
class DataSource(Protocol):
    """外部数据源接入协议.

    所有返回的 DataFrame 必须符合 §3 schema（用 validate_schema 校验）。
    """

    source_version: str  # 数据源版本，进入缓存键

    def fetch_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        freq: Freq = Freq.DAILY,
        adjust: AdjustMode = AdjustMode.BACKWARD,
    ) -> pd.DataFrame:
        """拉取 K 线. 返回 DailyBar 或 IntradayBar schema."""
        ...

    def fetch_universe(
        self,
        spec: str,
        date_range: tuple[pd.Timestamp, pd.Timestamp],
    ) -> pd.DataFrame:
        """拉取 PIT universe. 返回 Universe schema."""
        ...

    def fetch_corporate_actions(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取公司行为事件. 返回 CorporateAction schema."""
        ...

    def fetch_calendar(self, name: str = "SSE") -> Calendar:
        """拉取交易日历. 返回 Calendar Protocol 实现.

        允许数据源用自己的口径提供日历（如离线包里自带的 holiday 列表）；
        若数据源不实现，DataLayer 退回 get_default_calendar()。
        """
        ...

    def fetch_status_overrides(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取状态覆盖数据.

        某些数据源的 fetch_bars 不返回完整的状态字段（如 is_st、is_suspended），
        此方法用于补齐。返回 DataFrame：
          - MultiIndex(date, symbol) 或含 date/symbol 列
          - 可选列子集 ⊆ {is_st, is_suspended, is_limit_up, is_limit_down, ...}
        DataLayer 会以"覆盖"语义合并到 DailyBar 上（同列存在时此处优先）。
        """
        ...

    def fetch_fundamentals(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        report_types: list[ReportType] | None = None,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        """拉取财务数据. 返回 Fundamental schema."""
        ...

    def fetch_industry_classification(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        system: str = "sw",
        level: int = 1,
    ) -> pd.DataFrame:
        """拉取行业分类. 返回 IndustryClassification schema."""
        ...

    def fetch_concepts(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        source: str = "eastmoney",
    ) -> pd.DataFrame:
        """拉取概念板块分类. 返回 ConceptClassification schema.

        概念是一对多的：同一 symbol 可同时属于多个概念。
        """
        ...

    def fetch_share_capital(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取股本变动数据. 列含 date/symbol/total_shares/float_shares/free_float_shares."""
        ...

    def fetch_margin_trading(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取两融(融资融券)数据. 返回 MarginTrading schema."""
        ...

    def fetch_money_flow(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取资金流向数据. 返回 MoneyFlow schema."""
        ...

    def fetch_call_auction(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取集合竞价数据. 返回 CallAuction schema."""
        ...

    def fetch_billboard(
        self,
        end_date: pd.Timestamp,
        symbols: list[str] | None = None,
        count: int = 5,
    ) -> pd.DataFrame:
        """拉取龙虎榜数据. 返回 Billboard schema.

        与其他 fetch_* 不同: 龙虎榜是"某日全市场上榜股", 按截止日 + 回看天数查,
        ``symbols=None`` 表示全市场。
        """
        ...

    def fetch_factor_exposure(
        self,
        symbols: list[str],
        factors: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取因子暴露. 返回 FactorExposure schema(列名 = 因子名)."""
        ...


@runtime_checkable
class BarStore(Protocol):
    """通用 key-based 缓存层协议.

    适用于 universe / corp_actions / fundamentals / industry 等非分片场景。
    """

    def has(self, key: str) -> bool:
        ...

    def get(self, key: str) -> pd.DataFrame:
        ...

    def put(self, key: str, df: pd.DataFrame) -> None:
        ...

    def invalidate(self, key_pattern: str) -> None:
        ...


@runtime_checkable
class ShardedBarStore(Protocol):
    """K 线专用分片缓存层 — 文档 §7.3 增量更新口径.

    分片维度：(kind, source_version, ..., symbol, year-month)。
    实现负责把 DataFrame 自动切片到月级 parquet 文件，
    并支持"按需查询哪些 (symbol, year-month) 缺失"以触发增量拉取。
    """

    def get_range(
        self,
        *,
        kind: str,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        source_version: str,
        **extra_keys: Any,
    ) -> pd.DataFrame:
        """读取 [start, end] × symbols 的全部已缓存行（缺失部分不报错，仅省略）."""
        ...

    def put_range(
        self,
        df: pd.DataFrame,
        *,
        kind: str,
        source_version: str,
        **extra_keys: Any,
    ) -> None:
        """写入 df. 按 (symbol, year-month) 自动切片落盘."""
        ...

    def missing_ranges(
        self,
        *,
        kind: str,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        source_version: str,
        **extra_keys: Any,
    ) -> list[tuple[str, str]]:
        """返回 [(symbol, 'YYYY-MM'), ...] —— 缺失分片列表."""
        ...

    def invalidate_range(
        self,
        *,
        kind: str,
        symbols: list[str] | None = None,
        source_version: str | None = None,
        **extra_keys: Any,
    ) -> None:
        """失效特定分片. 任一参数 None 表示该维度通配."""
        ...
