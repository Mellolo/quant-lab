"""事件采样 — 书 Ch2 §2.5 CUSUM Filter."""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import EntryTiming
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
            entropy = self._shannon_entropy(window_ret, self.n_bins)
            if prev_entropy is not None:
                delta = abs(entropy - prev_entropy)
                if delta >= self.h:
                    events.append(ret.index[i])
            prev_entropy = entropy

        return pd.DatetimeIndex(events)

    @staticmethod
    def _shannon_entropy(values: np.ndarray, n_bins: int = 10) -> float:
        counts, _ = np.histogram(values, bins=n_bins)
        total = counts.sum()
        if total <= 0:
            return 0.0
        probs = counts / total
        probs = probs[probs > 0]
        return -float(np.sum(probs * np.log2(probs)))

class NewHighBreakoutSampler(EventSampler):
    """N 日收盘新高突破采样器（trading-books 15/12）.

    规则
    ----
    - 触发: ``close`` 创 ``window`` 日新高（含当日）
    - ``cooldown_days``: 触发后若干交易日内不再触发
    - ``signal_lag``: 信号确认日后平移几天再作为事件日。

      默认 ``signal_lag=1``：T 日收盘确认新高 → 事件落在下一根 bar
     （配合 ``entry_timing=open`` = 次日开盘入场，避免用当日收盘信号冒充当日开盘）。
      若做收盘入场实验，可设 ``signal_lag=0``。
    """

    def __init__(
        self,
        window: int = 20,
        cooldown_days: int = 5,
        signal_lag: int = 1,
    ):
        if window < 2:
            raise ValueError("window 必须 >= 2")
        if cooldown_days < 0:
            raise ValueError("cooldown_days 必须 >= 0")
        if signal_lag < 0:
            raise ValueError("signal_lag 必须 >= 0")
        self.window = window
        self.cooldown_days = cooldown_days
        self.signal_lag = signal_lag

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
        close = series.dropna()
        if len(close) < self.window + self.signal_lag:
            return pd.DatetimeIndex([])

        is_high = (close >= close.rolling(self.window).max()).fillna(False)
        # 首次触及新高: 今日是窗口高点，昨日不是（减少平台期连触发）
        first_touch = is_high & ~is_high.shift(1, fill_value=False)

        events: list[pd.Timestamp] = []
        last_i = -self.cooldown_days - 1
        idx = close.index
        n = len(close)
        for i in range(n):
            if not bool(first_touch.iloc[i]):
                continue
            if i - last_i <= self.cooldown_days:
                continue
            j = i + self.signal_lag
            if j >= n:
                continue
            events.append(idx[j])
            last_i = i
        return pd.DatetimeIndex(events)


class TrendBreakoutSampler(EventSampler):
    """因果趋势突破采样器.

    只用截至当日收盘的信息判断「当前可能处于趋势启动」，不使用未来数据:
      1. **状态**: 效率比 ER(er_window) > er_threshold
      2. **趋势方向**: close > MA(ma_window) 且 MA 斜率 > 0
         斜率 = 当前 MA 相对 slope_lookback 个交易日前的变化
      3. **触发**: 收盘创 breakout_window 日新高
      4. **去重**: 触发后 cooldown_days 内不再触发

    满足 PIT——所有量都只用 rolling window 内的历史数据。
    """

    def __init__(
        self,
        er_window: int = 20,
        er_threshold: float = 0.3,
        ma_window: int = 60,
        slope_lookback: int = 20,
        breakout_window: int = 20,
        cooldown_days: int = 10,
    ):
        if er_window < 2:
            raise ValueError("er_window 必须 >= 2")
        if ma_window < 2:
            raise ValueError("ma_window 必须 >= 2")
        if breakout_window < 2:
            raise ValueError("breakout_window 必须 >= 2")
        self.er_window = er_window
        self.er_threshold = er_threshold
        self.ma_window = ma_window
        self.slope_lookback = slope_lookback
        self.breakout_window = breakout_window
        self.cooldown_days = cooldown_days

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

    def _efficiency_ratio(self, close: pd.Series) -> pd.Series:
        """Kaufman Efficiency Ratio (rolling, causal)."""
        net = (close - close.shift(self.er_window)).abs()
        path = close.diff().abs().rolling(self.er_window).sum()
        return net / path.replace(0, np.nan)

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        close = series.dropna()
        min_len = max(self.er_window, self.ma_window, self.breakout_window) + self.slope_lookback + 1
        if len(close) < min_len:
            return pd.DatetimeIndex([])

        er = self._efficiency_ratio(close)
        ma = close.rolling(self.ma_window).mean()
        slope = (ma - ma.shift(self.slope_lookback)) / ma.shift(self.slope_lookback)
        # 创 N 日新高（含当日）
        breakout = close == close.rolling(self.breakout_window).max()

        cond = (
            (er > self.er_threshold)
            & (close > ma)
            & (slope > 0)
            & breakout
        )

        events: list[pd.Timestamp] = []
        last_i = -self.cooldown_days - 1
        for i, ok in enumerate(cond):
            if not ok or pd.isna(ok):
                continue
            if i - last_i <= self.cooldown_days:
                continue
            events.append(close.index[i])
            last_i = i
        return pd.DatetimeIndex(events)


class HMMTrendSampler(EventSampler):
    """因果 HMM 趋势状态采样器.

    用滚动窗口在**历史数据**上拟合一个 2 状态高斯 HMM，推断当日属于
    高波动趋势态的后验概率；概率从低于阈值升到高于阈值时触发事件。
    全程只用截至当日的数据，满足 PIT。

    实现要点
    --------
    - 每个 symbol 独立拟合，窗口 ``window`` 个交易日；
    - 用前 ``burn_in`` 天建立「趋势态 = 平均收益绝对值更大」的映射，
      之后逐日用 ``predict_proba`` 在线滤波（不再用未来数据重训）；
    - 低流动性/停牌或拟合失败时跳过当日。
    """

    def __init__(
        self,
        window: int = 60,
        prob_threshold: float = 0.7,
        min_history: int = 30,
        cooldown_days: int = 10,
        n_iter: int = 25,
        random_state: int = 0,
    ):
        if window < 10:
            raise ValueError("window 必须 >= 10")
        if not 0.0 < prob_threshold < 1.0:
            raise ValueError("prob_threshold 必须在 (0, 1)")
        self.window = window
        self.prob_threshold = prob_threshold
        self.min_history = min_history
        self.cooldown_days = cooldown_days
        self.n_iter = n_iter
        self.random_state = random_state

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
        from hmmlearn.hmm import GaussianHMM  # 延迟导入, 保持 qlab 无硬依赖

        close = series.dropna()
        if len(close) < self.window + self.min_history:
            return pd.DatetimeIndex([])

        ret = np.log(close).diff().dropna()
        events: list[pd.Timestamp] = []
        last_i = -self.cooldown_days - 1
        trend_state: int | None = None
        prev_prob = 0.0

        # 用滚动窗口逐步前进；只在窗口起点识别哪个状态是「趋势」
        for i in range(self.min_history, len(ret)):
            if i - last_i <= self.cooldown_days:
                continue
            hist = ret.iloc[max(0, i - self.window + 1) : i + 1].to_numpy().reshape(-1, 1)
            if len(hist) < 5:
                continue
            try:
                model = GaussianHMM(
                    n_components=2,
                    covariance_type="full",
                    n_iter=self.n_iter,
                    random_state=self.random_state,
                )
                model.fit(hist)
                probs = model.predict_proba(hist)
            except Exception:
                continue

            # 首次确定哪个分量是趋势态（平均 |ret| 更大）
            if trend_state is None:
                means = [float(np.mean(np.abs(hist[probs[:, s] > 0.5]))) for s in range(2)]
                trend_state = int(np.argmax(means))
                prev_prob = float(probs[-1, trend_state])
                continue

            p = float(probs[-1, trend_state])
            if prev_prob < self.prob_threshold <= p:
                events.append(ret.index[i])
                last_i = i
            prev_prob = p

        return pd.DatetimeIndex(events)


def to_event_dataframe(
    pairs: pd.DataFrame,
    *,
    target: pd.Series | float,
    t1_days: int = 7,
    side: pd.Series | int | None = None,
    calendar: Calendar | None = None,
    entry_timing: EntryTiming | str = EntryTiming.OPEN,
) -> pd.DataFrame:
    """把采样器产出的 (timestamp, symbol) 对扩展为 Event schema.

    参数
    ----
    pairs : DataFrame 含 columns ['timestamp', 'symbol']
    target : 屏障宽度基准. 标量或 Series indexed by (timestamp, symbol)
    t1_days : 垂直屏障的交易日数
    side : 主模型方向. None 表示不指定（让模型学方向）
    calendar : 交易日历
    entry_timing : 样本起点 / 入场时点.

        - ``open``(**默认**): ``event_start`` 日**开盘**入场；三重屏障从开盘价起算，
          终点由屏障决定。标签路径仍用日线 close 盯市，
          首日收益 = close_T / open_T - 1。
        - ``close``: ``event_start`` 日**收盘**入场；三重屏障从收盘价起算。

        注意: 若 pairs 来自**收盘** CUSUM，默认 ``open`` 会把当日收盘信息
        泄漏进「开盘决策」。应用日频网格（:func:`daily_event_pairs`）、
        把触发日平移到下一交易日开盘，或显式传 ``entry_timing='close'``。

    返回
    ----
    符合 SCHEMA_EVENT 的 DataFrame:
    index=event_start, columns=[symbol, t1, target, side, entry_timing]
    """
    timing = EntryTiming(entry_timing)
    cal = calendar or get_default_calendar()
    if pairs.empty:
        out = pd.DataFrame(
            columns=["symbol", "t1", "target", "side", "entry_timing"]
        )
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
            "event_start": ts,
            "symbol": sym,
            "t1": t1,
            "target": tgt,
            "side": sd,
            "entry_timing": timing.value,
        })

    df = pd.DataFrame(rows).set_index("event_start").sort_index()
    validate_schema(df, SCHEMA_EVENT, strict_index=False)
    return df


def daily_event_pairs(
    symbols: list[str],
    dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    """日频均匀采样: 每个交易日 × 每个标的 一对 (timestamp, symbol).

    与 CUSUM 等事件过滤器互补。接 ``to_event_dataframe`` 后默认
    ``entry_timing=open``（起点开盘，终点由三重屏障决定）。
    """
    dates = pd.DatetimeIndex(dates).normalize().unique().sort_values()
    syms = list(dict.fromkeys(symbols))
    if len(dates) == 0 or not syms:
        return pd.DataFrame(columns=["timestamp", "symbol"])
    idx = pd.MultiIndex.from_product([dates, syms], names=["timestamp", "symbol"])
    return idx.to_frame(index=False)


def filter_pairs(
    pairs: pd.DataFrame,
    mask: pd.Series,
    *,
    require_true: bool = True,
) -> pd.DataFrame:
    """用 (date, symbol) bool mask 过滤采样对.

    典型用法: Stage2 / 成交额宇宙等 BOTH 规则 —— 先算特征或宇宙 mask，
    再筛 ``NewHighBreakoutSampler`` 的 pairs。

    参数
    ----
    pairs : 含 ``timestamp``, ``symbol``
    mask : MultiIndex(date, symbol) 的 Series；数值型时 ``!=0`` 视为 True
    require_true : True 时只保留 mask 为真的行
    """
    if pairs.empty:
        return pairs.copy()
    if not {"timestamp", "symbol"}.issubset(pairs.columns):
        raise ValueError("pairs 需要列 timestamp, symbol")

    key = pd.MultiIndex.from_arrays(
        [
            pd.DatetimeIndex(pairs["timestamp"]).normalize(),
            pairs["symbol"].to_numpy(),
        ],
        names=["date", "symbol"],
    )
    m = mask.reindex(key)
    if m.dtype != bool:
        m = m.fillna(0).astype(float) != 0.0
    else:
        m = m.fillna(False)
    keep = m.to_numpy() if require_true else ~m.to_numpy()
    return pairs.loc[keep].reset_index(drop=True)
