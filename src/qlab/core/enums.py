"""Global enums."""

from __future__ import annotations

from enum import StrEnum


class AdjustMode(StrEnum):
    """复权模式."""

    NONE = "none"            # 不复权
    FORWARD = "forward"      # 前复权（当前价不变，历史调整）
    BACKWARD = "backward"    # 后复权（起始价不变，未来调整；推荐用于 ML）


class Freq(StrEnum):
    """Bar 频率."""

    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_15 = "15min"
    MIN_30 = "30min"
    MIN_60 = "60min"
    DAILY = "1d"

    @property
    def is_intraday(self) -> bool:
        return self != Freq.DAILY


class Session(StrEnum):
    """A 股交易会话."""

    OPEN_AUCTION = "open_auction"      # 9:15-9:25 开盘集合竞价
    MORNING = "morning"                # 9:30-11:30 上午连续竞价
    AFTERNOON = "afternoon"            # 13:00-14:57 下午连续竞价
    CLOSE_AUCTION = "close_auction"    # 14:57-15:00 收盘集合竞价


class ReportType(StrEnum):
    """财报披露类型 (§3.14)."""

    FORECAST = "forecast"      # 业绩预告
    FLASH = "flash"            # 业绩快报
    OFFICIAL = "official"      # 正式财报

    @property
    def priority(self) -> int:
        """同一报告期的优先级：official > flash > forecast."""
        return {ReportType.FORECAST: 1, ReportType.FLASH: 2, ReportType.OFFICIAL: 3}[self]
