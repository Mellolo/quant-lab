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


class EntryAt(StrEnum):
    """相对确认日何时入场（采样合约层，研究代码应只用这个）.

    - ``next_open``（默认）: 确认日 T → 下一交易日开盘
    - ``confirm_close``: 确认日 T 收盘入场（日线确认时刻 = 收盘）
    """

    NEXT_OPEN = "next_open"
    CONFIRM_CLOSE = "confirm_close"


class EntryTiming(StrEnum):
    """Event 表上的价格点：``event_start`` 日取开盘还是收盘.

    由 :class:`EntryAt` 映射而来（``next_open``→``open``，``confirm_close``→``close``），
    供三重屏障与特征 PIT 使用；一般勿直接当作集合约旋钮。
    """

    OPEN = "open"
    CLOSE = "close"


class ReportType(StrEnum):
    """财报披露类型 (§3.14)."""

    FORECAST = "forecast"      # 业绩预告
    FLASH = "flash"            # 业绩快报
    OFFICIAL = "official"      # 正式财报

    @property
    def priority(self) -> int:
        """同一报告期的优先级：official > flash > forecast."""
        return {ReportType.FORECAST: 1, ReportType.FLASH: 2, ReportType.OFFICIAL: 3}[self]
