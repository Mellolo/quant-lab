"""完整采样合约 = 入场规则 + 出场设定.

默认硬规则
----------
1. **确认日 T**：采样器判真的那天（网格则每日皆确认）。
2. **入场**：下一交易日开盘（``entry_at=next_open``）。

可选覆盖（仍只一个旋钮）
------------------------
``entry_at=confirm_close``：确认日收盘入场。日线上确认时刻 = 收盘，
不再单独暴露「确认时刻」；分钟级以后再说。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import EntryAt, EntryTiming
from qlab.labeling.events import VolumeCUSUMFilter, to_event_dataframe
from qlab.labeling.exit import EXIT_RESEARCH_DEFAULT, ExitSettings
from qlab.labeling.triple_barrier import label_events


class _EntrySampler(Protocol):
    """入场采样器：产出确认日 (timestamp, symbol)。"""

    def sample_per_symbol(self, prices: pd.DataFrame) -> pd.DataFrame: ...


def wide_ohlc_to_long(
    open_: pd.DataFrame,
    close: pd.DataFrame,
) -> pd.DataFrame:
    """宽表 open/close（columns=symbols）→ MultiIndex(date, symbol) 长表."""
    o = open_.stack(future_stack=True).rename("open")
    c = close.stack(future_stack=True).rename("close")
    long = pd.concat([o, c], axis=1)
    long.index = long.index.set_names(["date", "symbol"])
    return long.sort_index()


def confirmation_to_entry(
    pairs: pd.DataFrame,
    calendar: Calendar | None = None,
) -> pd.DataFrame:
    """确认日 pairs → 下一交易日（入场日）pairs."""
    if pairs.empty:
        return pairs
    cal = calendar or get_default_calendar()
    out = pairs.copy()
    ts = pd.to_datetime(out["timestamp"]).dt.normalize()
    mapping = {t: cal.next_trading_day(pd.Timestamp(t), 1) for t in ts.unique()}
    out["timestamp"] = ts.map(mapping)
    return out.dropna(subset=["timestamp"]).reset_index(drop=True)


def _infer_price_end(label_prices: pd.DataFrame) -> pd.Timestamp | None:
    if label_prices is None or label_prices.empty:
        return None
    if isinstance(label_prices.index, pd.MultiIndex):
        return pd.Timestamp(label_prices.index.get_level_values(0).max()).normalize()
    return pd.Timestamp(label_prices.index.max()).normalize()


def _resolve_label_prices(
    *,
    prices: pd.DataFrame,
    open_: pd.DataFrame | None,
    close: pd.DataFrame | None,
    label_prices: pd.DataFrame | None,
    entry_at: EntryAt,
) -> pd.DataFrame:
    """解析标注用价格表."""
    if label_prices is not None:
        return label_prices

    needs_open = entry_at == EntryAt.NEXT_OPEN

    if (
        isinstance(prices.index, pd.MultiIndex)
        and isinstance(prices, pd.DataFrame)
        and "close" in prices.columns
    ):
        if needs_open and "open" not in prices.columns:
            raise ValueError(
                "entry_at=next_open 需要价格表含 open 列，或传入 open= / label_prices=。"
            )
        return prices

    close_w = close if close is not None else prices
    if needs_open:
        if open_ is None:
            raise ValueError(
                "entry_at=next_open 时 SampleSpec.run 需要:\n"
                "  - label_prices= 长表(含 open+close)，或\n"
                "  - open= 与 close=（或 prices 作 close）宽表 panel。\n"
                "勿只传宽表 close。"
            )
        return wide_ohlc_to_long(open_, close_w)

    if isinstance(close_w.index, pd.MultiIndex) and "close" in getattr(close_w, "columns", []):
        return close_w
    stacked = close_w.stack(future_stack=True).rename("close")
    stacked.index = stacked.index.set_names(["date", "symbol"])
    return stacked.to_frame()


@dataclass
class SampleSpec:
    """采样合约：确认日 + 入场时刻 + 出场设定.

    参数
    ----
    entry :
        触发规则。输出 timestamp = **确认日**（网格则每日皆确认）。
    exit :
        出场：经典三重屏障（:class:`~qlab.labeling.exit.ExitSettings`）。
    entry_at :
        ``next_open``（默认）: 确认日 → 次日开盘。
        ``confirm_close``: 确认日收盘入场（日线确认时刻）。
    """

    entry: _EntrySampler
    exit: ExitSettings = EXIT_RESEARCH_DEFAULT
    entry_at: EntryAt | str = EntryAt.NEXT_OPEN

    def _entry_at(self) -> EntryAt:
        return EntryAt(self.entry_at)

    def confirmation_pairs(
        self,
        prices: pd.DataFrame,
        *,
        volume: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """仅确认日 ``(timestamp, symbol)``，不做入场日映射."""
        if isinstance(self.entry, VolumeCUSUMFilter):
            if volume is None:
                raise ValueError(
                    "SampleSpec.entry 是 VolumeCUSUMFilter，必须传入 volume= 宽表。"
                    "勿把 close 当作成交量。"
                )
            return self.entry.sample_per_symbol(volume)
        return self.entry.sample_per_symbol(prices)

    def sample_pairs(
        self,
        prices: pd.DataFrame,
        *,
        volume: pd.DataFrame | None = None,
        calendar: Calendar | None = None,
    ) -> pd.DataFrame:
        """确认日 → 入场日 ``(timestamp, symbol)``.

        ``next_open`` 映射到次日；``confirm_close`` 保留确认日。
        """
        pairs = self.confirmation_pairs(prices, volume=volume)
        if self._entry_at() == EntryAt.NEXT_OPEN:
            return confirmation_to_entry(pairs, calendar)
        return pairs

    def build_events(
        self,
        pairs: pd.DataFrame,
        *,
        target: pd.Series | float,
        side: pd.Series | int | None = None,
        calendar: Calendar | None = None,
        price_end: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """入场日 pairs → SCHEMA_EVENT."""
        timing = (
            EntryTiming.OPEN
            if self._entry_at() == EntryAt.NEXT_OPEN
            else EntryTiming.CLOSE
        )
        return to_event_dataframe(
            pairs,
            target=target,
            exit=self.exit,
            side=side,
            calendar=calendar,
            entry_timing=timing,
            price_end=price_end,
        )

    def label(
        self,
        events: pd.DataFrame,
        prices: pd.DataFrame,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """应用出场屏障，产出 SCHEMA_LABEL."""
        return label_events(events, prices, self.exit.barrier(), **kwargs)

    def run(
        self,
        prices: pd.DataFrame,
        *,
        target: pd.Series | float,
        side: pd.Series | int | None = None,
        calendar: Calendar | None = None,
        label_prices: pd.DataFrame | None = None,
        open: pd.DataFrame | None = None,
        close: pd.DataFrame | None = None,
        volume: pd.DataFrame | None = None,
        price_end: pd.Timestamp | None = None,
        **label_kwargs: Any,
    ) -> pd.DataFrame:
        """确认日采样 → 按 ``entry_at`` 建事件 → 标注."""
        at = self._entry_at()
        px = _resolve_label_prices(
            prices=prices,
            open_=open,
            close=close,
            label_prices=label_prices,
            entry_at=at,
        )
        pe = price_end if price_end is not None else _infer_price_end(px)
        pairs = self.sample_pairs(
            prices, volume=volume, calendar=calendar,
        )
        events = self.build_events(
            pairs, target=target, side=side, calendar=calendar, price_end=pe,
        )
        return self.label(events, px, **label_kwargs)
