"""事件采样 — 书 Ch2 §2.5 CUSUM Filter."""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.schema import SCHEMA_EVENT, validate_schema


class EventSampler(ABC):
    """事件采样器基类."""

    @abstractmethod
    def sample(self, prices: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        """返回事件时间戳."""
        ...


class CUSUMFilter(EventSampler):
    """对称 CUSUM Filter — 书 Ch2 Snippet 2.4.

    累计偏离超过阈值 h 时触发事件，并重置累加。
    多 symbol 时，按 symbol 分别计算后合并去重。
    """

    def __init__(self, h: float | pd.Series, expected: pd.Series | None = None):
        """
        h : 触发阈值。可以是标量，或按时间变化的 Series（如随波动率变化）。
        expected : E_{t-1}[y_t]，默认用 y_{t-1}（即比较收益率 vs 0）。
        """
        self.h = h
        self.expected = expected

    def sample(self, prices: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        """采样.

        参数
        ----
        prices : 若是 Series，indexed by date（单 symbol）；
                 若是 DataFrame，columns=symbols, index=date。

        返回
        ----
        所有触发事件的时间戳（多 symbol 时合并去重）。
        """
        if isinstance(prices, pd.Series):
            return self._sample_single(prices)
        # DataFrame: 按 symbol 分别采样
        all_events: set[pd.Timestamp] = set()
        for col in prices.columns:
            events = self._sample_single(prices[col])
            all_events.update(events.tolist())
        return pd.DatetimeIndex(sorted(all_events))

    def sample_per_symbol(self, prices: pd.DataFrame) -> pd.DataFrame:
        """采样并返回 (timestamp, symbol) 对."""
        rows = []
        for col in prices.columns:
            events = self._sample_single(prices[col])
            for ts in events:
                rows.append({"timestamp": ts, "symbol": col})
        if not rows:
            return pd.DataFrame(columns=["timestamp", "symbol"])
        return pd.DataFrame(rows)

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        if series.empty:
            return pd.DatetimeIndex([])

        log_close = np.log(series.dropna())
        diff = log_close.diff().dropna()

        events: list[pd.Timestamp] = []
        s_pos, s_neg = 0.0, 0.0

        # 解析阈值
        if isinstance(self.h, (int, float)):
            h_series = pd.Series(float(self.h), index=diff.index)
        else:
            h_series = self.h.reindex(diff.index).ffill()

        for ts, d in diff.items():
            h_t = h_series.loc[ts]
            if pd.isna(h_t):
                continue
            s_pos = max(0.0, s_pos + d)
            s_neg = min(0.0, s_neg + d)
            if s_neg < -h_t:
                events.append(ts)
                s_neg = 0.0
            elif s_pos > h_t:
                events.append(ts)
                s_pos = 0.0
        return pd.DatetimeIndex(events)


class VolumeCUSUMFilter(EventSampler):
    """成交量 CUSUM Filter — 检测量能突变.

    对 log(volume) 的变化率做对称 CUSUM，量能突然放大或缩小时触发事件。
    """

    def __init__(self, h: float | pd.Series):
        self.h = h

    def sample(self, volume: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        if isinstance(volume, pd.Series):
            return self._sample_single(volume)
        all_events: set[pd.Timestamp] = set()
        for col in volume.columns:
            events = self._sample_single(volume[col])
            all_events.update(events.tolist())
        return pd.DatetimeIndex(sorted(all_events))

    def sample_per_symbol(self, volume: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for col in volume.columns:
            events = self._sample_single(volume[col])
            for ts in events:
                rows.append({"timestamp": ts, "symbol": col})
        if not rows:
            return pd.DataFrame(columns=["timestamp", "symbol"])
        return pd.DataFrame(rows)

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        if series.empty:
            return pd.DatetimeIndex([])

        vol = series.dropna()
        vol = vol[vol > 0]
        if len(vol) < 2:
            return pd.DatetimeIndex([])

        log_vol = np.log(vol)
        diff = log_vol.diff().dropna()

        events: list[pd.Timestamp] = []
        s_pos, s_neg = 0.0, 0.0

        if isinstance(self.h, (int, float)):
            h_series = pd.Series(float(self.h), index=diff.index)
        else:
            h_series = self.h.reindex(diff.index).ffill()

        for ts, d in diff.items():
            h_t = h_series.loc[ts]
            if pd.isna(h_t):
                continue
            s_pos = max(0.0, s_pos + d)
            s_neg = min(0.0, s_neg + d)
            if s_neg < -h_t:
                events.append(ts)
                s_neg = 0.0
            elif s_pos > h_t:
                events.append(ts)
                s_pos = 0.0
        return pd.DatetimeIndex(events)


class RunSampler(EventSampler):
    """连续同向收益采样器.

    监控连续正/负 log 收益率的天数（run length），
    当 run 长度达到 min_run 时触发事件。
    """

    def __init__(self, min_run: int = 5):
        if min_run < 2:
            raise ValueError("min_run 必须 >= 2")
        self.min_run = min_run

    def sample(self, prices: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        if isinstance(prices, pd.Series):
            return self._sample_single(prices)
        all_events: set[pd.Timestamp] = set()
        for col in prices.columns:
            events = self._sample_single(prices[col])
            all_events.update(events.tolist())
        return pd.DatetimeIndex(sorted(all_events))

    def sample_per_symbol(self, prices: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for col in prices.columns:
            events = self._sample_single(prices[col])
            for ts in events:
                rows.append({"timestamp": ts, "symbol": col})
        if not rows:
            return pd.DataFrame(columns=["timestamp", "symbol"])
        return pd.DataFrame(rows)

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        if series.empty:
            return pd.DatetimeIndex([])

        log_close = np.log(series.dropna())
        diff = log_close.diff().dropna()
        if diff.empty:
            return pd.DatetimeIndex([])

        events: list[pd.Timestamp] = []
        run_len = 1
        prev_sign = np.sign(diff.iloc[0])

        for i in range(1, len(diff)):
            curr_sign = np.sign(diff.iloc[i])
            if curr_sign == prev_sign and curr_sign != 0:
                run_len += 1
                if run_len == self.min_run:
                    events.append(diff.index[i])
            else:
                run_len = 1
                prev_sign = curr_sign

        return pd.DatetimeIndex(events)


class EntropySampler(EventSampler):
    """信息熵采样器 — 书 Ch18 思想.

    滑动窗口计算收益率分布的 Shannon entropy，
    entropy 变化超过阈值时触发事件（从有序变无序或反之）。
    """

    def __init__(self, window: int = 20, n_bins: int = 10, h: float = 0.3):
        if window < 3:
            raise ValueError("window 必须 >= 3")
        self.window = window
        self.n_bins = n_bins
        self.h = h

    def sample(self, prices: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        if isinstance(prices, pd.Series):
            return self._sample_single(prices)
        all_events: set[pd.Timestamp] = set()
        for col in prices.columns:
            events = self._sample_single(prices[col])
            all_events.update(events.tolist())
        return pd.DatetimeIndex(sorted(all_events))

    def sample_per_symbol(self, prices: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for col in prices.columns:
            events = self._sample_single(prices[col])
            for ts in events:
                rows.append({"timestamp": ts, "symbol": col})
        if not rows:
            return pd.DataFrame(columns=["timestamp", "symbol"])
        return pd.DataFrame(rows)

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        if series.empty:
            return pd.DatetimeIndex([])

        log_close = np.log(series.dropna())
        ret = log_close.diff().dropna()
        if len(ret) < self.window + 1:
            return pd.DatetimeIndex([])

        events: list[pd.Timestamp] = []
        prev_entropy: float | None = None

        for i in range(self.window, len(ret)):
            window_ret = ret.iloc[i - self.window : i].values
            entropy = self._shannon_entropy(window_ret)
            if prev_entropy is not None:
                delta = abs(entropy - prev_entropy)
                if delta >= self.h:
                    events.append(ret.index[i])
            prev_entropy = entropy

        return pd.DatetimeIndex(events)

    @staticmethod
    def _shannon_entropy(values: np.ndarray) -> float:
        counts, _ = np.histogram(values, bins=10)
        probs = counts / counts.sum()
        probs = probs[probs > 0]
        return -float(np.sum(probs * np.log2(probs)))

def to_event_dataframe(
    pairs: pd.DataFrame,
    *,
    target: pd.Series | float,
    t1_days: int = 7,
    side: pd.Series | int | None = None,
    calendar: Calendar | None = None,
) -> pd.DataFrame:
    """把 CUSUMFilter.sample_per_symbol 的 (timestamp, symbol) 对扩展为 Event schema.

    参数
    ----
    pairs : DataFrame 含 columns ['timestamp', 'symbol']
    target : 屏障宽度基准. 标量或 Series indexed by (timestamp, symbol)
    t1_days : 垂直屏障的交易日数
    side : 主模型方向. None 表示不指定（让模型学方向）
    calendar : 交易日历

    返回
    ----
    符合 SCHEMA_EVENT 的 DataFrame: index=event_start, columns=[symbol, t1, target, side]
    """
    cal = calendar or get_default_calendar()
    if pairs.empty:
        out = pd.DataFrame(columns=["symbol", "t1", "target", "side"])
        out.index.name = "event_start"
        return out

    rows = []
    for _, row in pairs.iterrows():
        ts = pd.Timestamp(row["timestamp"]).normalize()
        sym = row["symbol"]
        try:
            t1 = cal.next_trading_day(ts, t1_days)
        except Exception:
            t1 = pd.NaT
        if isinstance(target, pd.Series):
            tgt = float(target.get((ts, sym), np.nan))
        else:
            tgt = float(target)
        sd: float | None
        if side is None:
            sd = np.nan
        elif isinstance(side, pd.Series):
            sd = float(side.get((ts, sym), np.nan))
        else:
            sd = float(side)
        rows.append({
            "event_start": ts, "symbol": sym, "t1": t1, "target": tgt, "side": sd,
        })

    df = pd.DataFrame(rows).set_index("event_start").sort_index()
    validate_schema(df, SCHEMA_EVENT, strict_index=False)
    return df
