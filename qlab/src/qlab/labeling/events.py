"""事件采样 — 书 Ch2 §2.5 CUSUM Filter."""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import EntryTiming
from qlab.core.schema import SCHEMA_EVENT, validate_schema


class EventSampler(ABC):
    """事件采样器基类：产出**确认日**（该日收盘后触发条件成立）.

    不负责入场。勿把 ``sample_per_symbol`` 的 timestamp 直接当开盘入场日。
    研究路径请用 :class:`~qlab.labeling.sample_spec.SampleSpec` 或
    :func:`~qlab.labeling.sample_frame.build_labeled_samples`。

    子类只需实现 :meth:`_sample_single`；``sample`` / ``sample_per_symbol``
    由基类按列展开，避免各采样器重复样板代码。
    """

    def sample(self, data: pd.DataFrame | pd.Series) -> pd.DatetimeIndex:
        """返回事件时间戳（多 symbol 时合并去重）."""
        if isinstance(data, pd.Series):
            return self._sample_single(data)
        all_events: set[pd.Timestamp] = set()
        for col in data.columns:
            all_events.update(self._sample_single(data[col]).tolist())
        return pd.DatetimeIndex(sorted(all_events))

    def sample_per_symbol(self, data: pd.DataFrame) -> pd.DataFrame:
        """采样并返回 (timestamp, symbol) 对."""
        rows = []
        for col in data.columns:
            for ts in self._sample_single(data[col]):
                rows.append({"timestamp": ts, "symbol": col})
        if not rows:
            return pd.DataFrame(columns=["timestamp", "symbol"])
        return pd.DataFrame(rows)

    @abstractmethod
    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        """单标的确认日序列."""
        ...


class CUSUMFilter(EventSampler):
    """对称 CUSUM Filter — 书 Ch2 Snippet 2.4（**主栈可选触发**）.

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
    """成交量 CUSUM Filter — 检测量能突变（**实验/辅触发**，非主栈默认）.

    对 log(volume) 的变化率做对称 CUSUM，量能突然放大或缩小时触发事件。
    主栈更常用 :func:`~qlab.labeling.sample_masks.volume_confirm_mask`
    叠在价格触发器上。
    """

    def __init__(self, h: float | pd.Series):
        self.h = h

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
    """连续同向收益采样器（**实验/辅触发**，非主栈默认）.

    监控连续正/负 log 收益率的天数（run length），
    当 run 长度达到 min_run 时触发事件。与「领涨股」主叙事弱相关，
    适合研究探针。
    """

    def __init__(self, min_run: int = 5):
        if min_run < 2:
            raise ValueError("min_run 必须 >= 2")
        self.min_run = min_run

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
    """信息熵采样器 — 书 Ch18 思想（**实验/辅触发**，非主栈默认）.

    滑动窗口计算收益率分布的 Shannon entropy，
    entropy 变化超过阈值时触发事件（从有序变无序或反之）。
    """

    def __init__(self, window: int = 20, n_bins: int = 10, h: float = 0.3):
        if window < 3:
            raise ValueError("window 必须 >= 3")
        self.window = window
        self.n_bins = n_bins
        self.h = h

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
    """N 日收盘新高突破采样器（trading-books 15/12）（**主栈推荐触发**）.

    规则
    ----
    - 触发: ``close`` 创 ``window`` 日新高（含当日）→ 吐出**确认日**
    - ``cooldown_days``: 触发后若干交易日内不再触发
    - 入场日由 :class:`~qlab.labeling.sample_spec.SampleSpec` 统一映射
      （默认次日开盘）

    备注: 全市场退出网格显示 ``window=60`` 显著负期望，现行目录只用短窗
    （默认 20）作候选池，勿裸用长窗新高作进场采样。质量靠采样门
    （Stage2 / RS / 量能等）叠加，而非加长 window。
    """

    def __init__(
        self,
        window: int = 20,
        cooldown_days: int = 5,
    ):
        if window < 2:
            raise ValueError("window 必须 >= 2")
        if cooldown_days < 0:
            raise ValueError("cooldown_days 必须 >= 0")
        self.window = window
        self.cooldown_days = cooldown_days

    def _sample_single(self, series: pd.Series) -> pd.DatetimeIndex:
        close = series.dropna()
        if len(close) < self.window:
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
            events.append(idx[i])
            last_i = i
        return pd.DatetimeIndex(events)


class HMMTrendSampler(EventSampler):
    """因果 HMM 趋势状态采样器（**实验/辅触发**，非主栈默认）.

    用滚动窗口在**历史数据**上拟合一个 2 状态高斯 HMM，推断当日属于
    高波动趋势态的后验概率；概率从低于阈值升到高于阈值时触发事件。
    全程只用截至当日的数据，满足 PIT。

    与 :func:`~qlab.labeling.sample_masks.stage2_mask` /
    :func:`~qlab.labeling.sample_masks.relative_strength_mask` 重叠大且更贵，
    适合专题研究，不作为默认触发。

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


def _make_event_ids(timestamps: pd.Series, symbols: pd.Series) -> list[str]:
    """稳定唯一 event_id: ``{symbol}|{YYYYMMDD}``，同日同标的再追加 ``|n``."""
    base = [
        f"{sym}|{pd.Timestamp(ts).strftime('%Y%m%d')}"
        for ts, sym in zip(timestamps, symbols)
    ]
    seen: dict[str, int] = {}
    out: list[str] = []
    for b in base:
        n = seen.get(b, 0)
        seen[b] = n + 1
        out.append(b if n == 0 else f"{b}|{n}")
    return out


def ensure_event_key(df: pd.DataFrame) -> pd.DataFrame:
    """返回适合安全 join 的视图（不修改调用方原表语义以外的列）.

    - 若有唯一 ``event_id`` 列：以 ``event_id`` 为 index（列保留）
    - 否则：以 ``MultiIndex(event_start, symbol)`` 为 index

    **禁止**只按 ``event_start`` merge（多标的下会错配）。
    """
    if df.empty:
        return df.copy()
    out = df.copy()
    if "event_id" in out.columns and out["event_id"].notna().all():
        if out["event_id"].duplicated().any():
            raise ValueError(
                "event_id 含重复，无法作为 join 键。请检查 to_event_dataframe 输出。"
            )
        out = out.set_index("event_id", drop=False)
        out.index.name = "event_id"
        return out
    if "symbol" not in out.columns:
        raise ValueError("ensure_event_key 需要 event_id 列或 symbol 列")
    if isinstance(out.index, pd.MultiIndex) and list(out.index.names) == [
        "event_start",
        "symbol",
    ]:
        return out
    if isinstance(out.index, pd.MultiIndex):
        event_start = out.index.get_level_values(0)
    else:
        event_start = out.index
    out.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(event_start), out["symbol"].astype(str)],
        names=["event_start", "symbol"],
    )
    return out


def to_event_dataframe(
    pairs: pd.DataFrame,
    *,
    target: pd.Series | float,
    t1_days: int = 7,
    exit: "ExitSettings | None" = None,
    side: pd.Series | int | None = None,
    calendar: Calendar | None = None,
    entry_timing: EntryTiming | str = EntryTiming.OPEN,
    price_end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """把采样器产出的 (timestamp, symbol) 对扩展为 Event schema.

    参数
    ----
    pairs : DataFrame 含 columns ['timestamp', 'symbol']
    target : 屏障宽度基准. 标量或 Series indexed by (timestamp, symbol)
    t1_days : 垂直屏障的交易日数（当 ``exit`` 未传时使用）
    exit : 可选出场设定；若传入则用 ``exit.vertical_days`` 覆盖 ``t1_days``
    side : 主模型方向. None 表示不指定（让模型学方向）
    calendar : 交易日历
    entry_timing : 样本起点 / 入场时点.

        - ``open``(**默认**): ``event_start`` 日**开盘**入场；三重屏障从开盘价起算，
          终点由屏障决定。标签路径仍用日线 close 盯市，
          首日收益 = close_T / open_T - 1。
        - ``close``: ``event_start`` 日**收盘**入场；三重屏障从收盘价起算。

        注意: ``pairs`` 的 timestamp 应是**入场日**。采样器吐的是**确认日**时，
        请走 :class:`~qlab.labeling.sample_spec.SampleSpec`：
        ``entry_at=next_open`` → 次日 + ``open``；
        ``entry_at=confirm_close`` → 确认日 + ``close``。
    price_end : 若给定，将 ``t1`` 裁到不超过该日（减少样本末无谓 no_data）

    返回
    ----
    符合 SCHEMA_EVENT 的 DataFrame:
    index=event_start, columns=[symbol, t1, target, side, entry_timing, event_id]

    注意: ``event_start`` 多标的下会重复；join 请用 ``event_id`` 或
    :func:`ensure_event_key`，勿只按 ``event_start``。
    """
    timing = EntryTiming(entry_timing)
    cal = calendar or get_default_calendar()
    v_days = exit.vertical_days if exit is not None else t1_days
    pe = pd.Timestamp(price_end).normalize() if price_end is not None else None
    if pairs.empty:
        out = pd.DataFrame(
            columns=["symbol", "t1", "target", "side", "entry_timing", "event_id"]
        )
        out.index.name = "event_start"
        return out

    rows = []
    n_clipped = 0
    for _, row in pairs.iterrows():
        ts = pd.Timestamp(row["timestamp"]).normalize()
        sym = row["symbol"]
        try:
            t1 = cal.next_trading_day(ts, v_days)
        except Exception:
            t1 = pd.NaT
        if pe is not None and pd.notna(t1) and t1 > pe and pe > ts:
            t1 = pe
            n_clipped += 1
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

    df = pd.DataFrame(rows)
    df["event_id"] = _make_event_ids(df["event_start"], df["symbol"])
    df = df.set_index("event_start").sort_index()
    if n_clipped:
        df.attrs["n_t1_clipped"] = n_clipped
    validate_schema(df, SCHEMA_EVENT, strict_index=False)
    return df


def daily_event_pairs(
    symbols: list[str],
    dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    """日频网格：每个交易日 × 每个标的 一对确认日 (timestamp, symbol)（**主栈可选**）.

    「每一天都是确认日」。适合横截面动量研究：用采样门收窄宇宙后排序。
    入场由 :class:`~qlab.labeling.sample_spec.SampleSpec`
    决定（默认次日开盘；可选确认日收盘）。
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
