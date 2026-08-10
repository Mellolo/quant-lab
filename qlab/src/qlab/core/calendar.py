"""Trading calendar — A 股交易日历.

抽象在 Protocol 层，默认实现用 exchange_calendars 库的 XSHG（上交所）。
A 股深沪两市除少数特殊日外日历完全一致，XSHG 足够。
"""

from __future__ import annotations

from datetime import time
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Calendar(Protocol):
    """交易日历协议."""

    name: str

    def is_trading_day(self, date: pd.Timestamp) -> bool:
        ...

    def trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        ...

    def prev_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        ...

    def next_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        ...

    def count_trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> int:
        ...

    def session_times(self, date: pd.Timestamp) -> dict[str, pd.Timestamp]:
        """返回该日的会话时刻（开盘/上午收盘/下午开盘/收盘等）."""
        ...


class AShareCalendar:
    """A 股交易日历（默认实现）.

    使用 exchange_calendars 库的 XSHG。两市差异极小，XSHG 即可。
    """

    name = "SSE"

    # 标准会话时间（A 股近年统一无半日市；如未来有半日市需要扩展）
    OPEN_AUCTION_START = time(9, 15)
    OPEN_AUCTION_END = time(9, 25)
    MORNING_OPEN = time(9, 30)
    MORNING_CLOSE = time(11, 30)
    AFTERNOON_OPEN = time(13, 0)
    AFTERNOON_CLOSE = time(15, 0)
    CLOSE_AUCTION_START = time(14, 57)

    def __init__(self) -> None:
        # 延迟导入，避免顶层 import 慢
        try:
            import exchange_calendars as xcals
            self._cal = xcals.get_calendar("XSHG")
        except ImportError:
            self._cal = None

    def is_trading_day(self, date: pd.Timestamp) -> bool:
        if self._cal is not None:
            return self._cal.is_session(pd.Timestamp(date).normalize())
        # fallback: 简单工作日判断（不准，仅 fallback）
        return pd.Timestamp(date).weekday() < 5

    def trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        start = pd.Timestamp(start).normalize()
        end = pd.Timestamp(end).normalize()
        if self._cal is not None:
            return self._cal.sessions_in_range(start, end)
        return pd.bdate_range(start, end)

    def prev_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        date = pd.Timestamp(date).normalize()
        if self._cal is not None:
            cur = date
            for _ in range(n):
                cur = self._cal.previous_session(cur)
            return cur
        # fallback
        return date - pd.tseries.offsets.BDay(n)

    def next_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        """返回 ``date`` 之后第 ``n`` 个交易日.

        若 ``date`` 本身不是交易日（价格索引含假期等），先落到其后最近
        交易日并计为第 1 步，避免 ``exchange_calendars`` 抛 NotSessionError。
        """
        date = pd.Timestamp(date).normalize()
        if n < 0:
            raise ValueError("n 必须 >= 0")
        if n == 0:
            return date
        if self._cal is not None:
            cur = date
            if not self._cal.is_session(cur):
                cur = pd.Timestamp(self._cal.date_to_session(cur, direction="next")).normalize()
                n -= 1
                if n == 0:
                    return cur
            for _ in range(n):
                cur = self._cal.next_session(cur)
            return pd.Timestamp(cur).normalize()
        return date + pd.tseries.offsets.BDay(n)

    def count_trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> int:
        return len(self.trading_days(start, end))

    def session_times(self, date: pd.Timestamp) -> dict[str, pd.Timestamp]:
        d = pd.Timestamp(date).normalize()
        return {
            "open_auction_start": d + pd.Timedelta(hours=9, minutes=15),
            "open_auction_end": d + pd.Timedelta(hours=9, minutes=25),
            "morning_open": d + pd.Timedelta(hours=9, minutes=30),
            "morning_close": d + pd.Timedelta(hours=11, minutes=30),
            "afternoon_open": d + pd.Timedelta(hours=13, minutes=0),
            "close_auction_start": d + pd.Timedelta(hours=14, minutes=57),
            "afternoon_close": d + pd.Timedelta(hours=15, minutes=0),
        }


_default_calendar: AShareCalendar | None = None


def get_default_calendar() -> AShareCalendar:
    """单例方式获取默认日历."""
    global _default_calendar
    if _default_calendar is None:
        _default_calendar = AShareCalendar()
    return _default_calendar
