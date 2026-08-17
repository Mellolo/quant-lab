"""宽表价量面板算法 — 特征与采样门共用，避免双份实现漂移.

``core/`` 不依赖 features/labeling；下游各自封装 API。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def stage_panel(
    close: pd.DataFrame,
    *,
    ma_window: int = 200,
    slope_lookback: int = 20,
) -> pd.DataFrame:
    """机械四阶段标签面板（列=symbol）.

    Stage 2: 价在 MA 上且 MA 上升 —— 唯一理想做多区
    Stage 4: 价在 MA 下且 MA 下降
    Stage 3: 价在 MA 上但 MA 未升（见顶/派发近似）
    Stage 1: 其余（筑底 / 吸筹近似）
    """
    close = close.sort_index().astype(float)
    ma = close.rolling(ma_window, min_periods=ma_window).mean()
    prev = ma.shift(slope_lookback)
    slope = (ma - prev) / prev.replace(0, np.nan)
    above = close > ma
    rising = slope > 0

    stage = pd.DataFrame(1, index=close.index, columns=close.columns, dtype="float64")
    stage = stage.mask(above & rising, 2.0)
    stage = stage.mask(above & ~rising, 3.0)
    stage = stage.mask(~above & ~rising, 4.0)
    return stage.where(ma.notna() & slope.notna())


def is_stage2_panel(
    close: pd.DataFrame,
    *,
    ma_window: int = 200,
    slope_lookback: int = 20,
) -> pd.DataFrame:
    """Stage2 bool 宽表."""
    stage = stage_panel(
        close, ma_window=ma_window, slope_lookback=slope_lookback
    )
    return stage.eq(2.0)


def dist_to_high_panel(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """``close / rolling_max - 1``（0=新高）."""
    close = close.sort_index().astype(float)
    hi = close.rolling(window, min_periods=window).max()
    return close / hi.replace(0, np.nan) - 1.0


def atr_panel(
    high: pd.DataFrame,
    low: pd.DataFrame,
    close: pd.DataFrame,
    *,
    window: int = 14,
) -> pd.DataFrame:
    """Wilder 风格 ATR（用 EWM α=1/window 近似）."""
    high = high.sort_index().astype(float)
    low = low.sort_index().astype(float)
    close = close.reindex_like(high).astype(float)
    prev = close.shift(1)
    tr = np.maximum(
        np.maximum((high - low).abs(), (high - prev).abs()),
        (low - prev).abs(),
    )
    return tr.ewm(alpha=1.0 / window, adjust=False, min_periods=window).mean()


def trend_velocity_panel(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    lookback: int = 10,
    atr_window: int = 14,
) -> pd.DataFrame:
    """趋势速度（Volatility-Based Momentum）: ``(close_t - close_{t-n}) / ATR``.

    以 ATR 为尺，跨股票/波动状态可比；正=上行速度，负=下行速度。
    """
    close = close.sort_index().astype(float)
    if high is None:
        high = close
    else:
        high = high.reindex_like(close).astype(float)
    if low is None:
        low = close
    else:
        low = low.reindex_like(close).astype(float)
    atr = atr_panel(high, low, close, window=atr_window)
    return (close - close.shift(lookback)) / atr.replace(0.0, np.nan)


def _supertrend_one(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    atr: pd.Series,
    *,
    multiplier: float,
) -> pd.DataFrame:
    """单标的 SuperTrend（TradingView / Olivier Seban 口径）.

    ATR 就绪前为 NaN；就绪后先记空头，收盘打穿轨才换边。轨只朝趋势方向收紧。
    """
    h = high.to_numpy(dtype=float, copy=False)
    l = low.to_numpy(dtype=float, copy=False)
    c = close.to_numpy(dtype=float, copy=False)
    a = atr.to_numpy(dtype=float, copy=False)
    n = len(c)
    hl2 = (h + l) * 0.5
    final_upper = np.full(n, np.nan)
    final_lower = np.full(n, np.nan)
    line = np.full(n, np.nan)
    direction = np.full(n, np.nan)
    age = np.full(n, np.nan)
    started = False
    prev = -1
    for i in range(n):
        if not (np.isfinite(a[i]) and np.isfinite(hl2[i]) and np.isfinite(c[i])):
            continue
        basic_upper = hl2[i] + multiplier * a[i]
        basic_lower = hl2[i] - multiplier * a[i]
        if not started:
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            direction[i] = -1.0
            line[i] = final_upper[i]
            age[i] = 0.0
            started = True
            prev = i
            continue
        pu, pl, pc, pd_ = final_upper[prev], final_lower[prev], c[prev], direction[prev]
        final_upper[i] = basic_upper if (basic_upper < pu or pc > pu) else pu
        final_lower[i] = basic_lower if (basic_lower > pl or pc < pl) else pl
        if pd_ < 0:
            direction[i] = 1.0 if c[i] > final_upper[i] else -1.0
        else:
            direction[i] = -1.0 if c[i] < final_lower[i] else 1.0
        line[i] = final_lower[i] if direction[i] > 0 else final_upper[i]
        age[i] = 0.0 if direction[i] != pd_ else (age[prev] + 1.0)
        prev = i
    return pd.DataFrame(
        {"direction": direction, "line": line, "age": age},
        index=close.index,
    )


_ST_KEYS = ("direction", "line", "age")


def _empty_st_frames(index, columns) -> dict[str, pd.DataFrame]:
    return {
        k: pd.DataFrame(np.nan, index=index, columns=columns, dtype="float64")
        for k in _ST_KEYS
    }


def supertrend_panels(
    high: pd.DataFrame,
    low: pd.DataFrame,
    close: pd.DataFrame,
    *,
    atr_window: int = 10,
    multiplier: float = 3.0,
    atr: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """SuperTrend 宽表：``direction`` ∈ {-1,+1}、``line``、``age``（距上次翻转的根数）.

    默认 ATR(10)×3，与 TradingView Supertrend 常用参数一致。
    无 0 态：震荡里会来回翻；诊断方向用 :func:`dual_supertrend_panels` 求同。
    """
    high = high.sort_index().astype(float)
    low = low.reindex_like(high).astype(float)
    close = close.reindex_like(high).astype(float)
    if atr is None:
        atr = atr_panel(high, low, close, window=atr_window)
    else:
        atr = atr.reindex_like(high).astype(float)
    out = _empty_st_frames(high.index, high.columns)
    m = float(multiplier)
    for col in high.columns:
        one = _supertrend_one(high[col], low[col], close[col], atr[col], multiplier=m)
        for k in _ST_KEYS:
            out[k][col] = one[k]
    return out


def dual_supertrend_panels(
    high: pd.DataFrame,
    low: pd.DataFrame,
    close: pd.DataFrame,
    *,
    atr_window: int = 10,
    slow: float = 3.0,
    fast: float = 2.0,
) -> dict[str, dict[str, pd.DataFrame]]:
    """慢/快 SuperTrend，**ATR 只算一次**.

    Returns:
        ``{"slow": {direction,line,age}, "fast": {...}}``
    """
    high = high.sort_index().astype(float)
    low = low.reindex_like(high).astype(float)
    close = close.reindex_like(high).astype(float)
    atr = atr_panel(high, low, close, window=atr_window)
    slow_out = _empty_st_frames(high.index, high.columns)
    fast_out = _empty_st_frames(high.index, high.columns)
    for col in high.columns:
        s = _supertrend_one(high[col], low[col], close[col], atr[col], multiplier=float(slow))
        f = _supertrend_one(high[col], low[col], close[col], atr[col], multiplier=float(fast))
        for k in _ST_KEYS:
            slow_out[k][col] = s[k]
            fast_out[k][col] = f[k]
    return {"slow": slow_out, "fast": fast_out}


def _fvg_state_one(
    high: pd.Series, low: pd.Series, close: pd.Series
) -> pd.DataFrame:
    """单标的 ICT 式 FVG 状态（确认日=第 3 根收盘）.

    - imbalance: 未回补失衡方向 +1/-1/0
    - retest: 当日回踩失衡区但仍按方向收盘
    - break: 当日收盘穿过失衡区（inversion / 失效）
    """
    h = high.astype(float).to_numpy()
    l = low.astype(float).to_numpy()
    c = close.astype(float).to_numpy()
    n = len(c)
    imb = np.zeros(n, dtype=float)
    retest = np.zeros(n, dtype=float)
    brk = np.zeros(n, dtype=float)
    z_lo = np.nan
    z_hi = np.nan
    z_dir = 0

    for i in range(n):
        # 先处理已有失衡的回踩 / 破坏（PIT：用当日 OHLC）
        if z_dir == 1 and not np.isnan(z_lo) and not np.isnan(z_hi):
            if c[i] < z_lo:
                brk[i] = 1.0
                z_dir, z_lo, z_hi = 0, np.nan, np.nan
            elif l[i] <= z_hi and c[i] >= z_lo:
                retest[i] = 1.0
            elif l[i] <= z_lo:
                # 完全回补后清空
                z_dir, z_lo, z_hi = 0, np.nan, np.nan
        elif z_dir == -1 and not np.isnan(z_lo) and not np.isnan(z_hi):
            if c[i] > z_hi:
                brk[i] = 1.0
                z_dir, z_lo, z_hi = 0, np.nan, np.nan
            elif h[i] >= z_lo and c[i] <= z_hi:
                retest[i] = 1.0
            elif h[i] >= z_hi:
                z_dir, z_lo, z_hi = 0, np.nan, np.nan

        # 第 3 根收盘确认新 FVG（覆盖旧失衡）
        if i >= 2:
            if h[i - 2] < l[i]:
                z_lo, z_hi, z_dir = float(h[i - 2]), float(l[i]), 1
            elif l[i - 2] > h[i]:
                z_lo, z_hi, z_dir = float(h[i]), float(l[i - 2]), -1

        imb[i] = float(z_dir)

    return pd.DataFrame(
        {"imbalance": imb, "retest": retest, "break": brk},
        index=close.index,
    )


def fvg_state_panels(
    high: pd.DataFrame,
    low: pd.DataFrame,
    close: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """FVG 宽表：``imbalance / retest / break``."""
    high = high.sort_index().astype(float)
    low = low.reindex_like(high).astype(float)
    close = close.reindex_like(high).astype(float)
    keys = ["imbalance", "retest", "break"]
    out = {
        k: pd.DataFrame(np.nan, index=high.index, columns=high.columns, dtype="float64")
        for k in keys
    }
    for col in high.columns:
        one = _fvg_state_one(high[col], low[col], close[col])
        for k in keys:
            out[k][col] = one[k]
    return out


def smooth_momentum_panel(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """Clenow 平滑动量: 年化 log 斜率 × R²."""
    if window < 5:
        raise ValueError("window 必须 >= 5")
    log_c = np.log(close.sort_index().astype(float))
    x = np.arange(window, dtype=float)
    x = x - x.mean()
    ss_x = float((x ** 2).sum())

    def _score(y: np.ndarray) -> float:
        if y.shape[0] != window or np.isnan(y).any():
            return np.nan
        y = y.astype(float)
        y0 = y - y.mean()
        slope = float((x * y0).sum() / ss_x)
        yhat = slope * x + y.mean()
        ss_res = float(((y - yhat) ** 2).sum())
        ss_tot = float((y0 ** 2).sum())
        if ss_tot <= 0:
            return 0.0
        r2 = 1.0 - ss_res / ss_tot
        annualized = float(np.exp(slope * 252.0) - 1.0)
        return annualized * max(r2, 0.0)

    return log_c.rolling(window).apply(_score, raw=True)


def smooth_momentum_r2_panel(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """平滑动量回归的 R² 面板（趋势线性度，供 quality 用）."""
    if window < 5:
        raise ValueError("window 必须 >= 5")
    log_c = np.log(close.sort_index().astype(float))
    x = np.arange(window, dtype=float)
    x = x - x.mean()
    ss_x = float((x ** 2).sum())

    def _r2(y: np.ndarray) -> float:
        if y.shape[0] != window or np.isnan(y).any():
            return np.nan
        y = y.astype(float)
        y0 = y - y.mean()
        slope = float((x * y0).sum() / ss_x)
        yhat = slope * x + y.mean()
        ss_res = float(((y - yhat) ** 2).sum())
        ss_tot = float((y0 ** 2).sum())
        if ss_tot <= 0:
            return 0.0
        return float(max(0.0, 1.0 - ss_res / ss_tot))

    return log_c.rolling(window).apply(_r2, raw=True)


# ---------------------------------------------------------------------------
# 市场结构：孤立高低点 / BOS / CHoCH（PIT：右端确认日后才可用）
# ---------------------------------------------------------------------------


def _confirmed_swing_series(
    price: pd.Series, *, left_right: int, mode: str
) -> pd.Series:
    """在确认日写入枢轴价；确认日前为 NaN.

    枢轴候选在 ``t - L``，于 ``t`` 日收盘确认（左右各 ``L`` 根）。
    """
    if left_right < 1:
        raise ValueError("left_right 必须 >= 1")
    s = price.astype(float)
    out = pd.Series(np.nan, index=s.index, dtype="float64")
    vals = s.to_numpy(dtype=float, copy=False)
    L = int(left_right)
    n = len(vals)
    for t in range(2 * L, n):
        c = t - L
        window = vals[c - L : c + L + 1]
        if np.isnan(vals[c]) or np.isnan(window).all():
            continue
        if mode == "high":
            if vals[c] >= np.nanmax(window):
                out.iloc[t] = vals[c]
        elif mode == "low":
            if vals[c] <= np.nanmin(window):
                out.iloc[t] = vals[c]
        else:
            raise ValueError("mode 必须是 'high' 或 'low'")
    return out


def swing_pivot_panels(
    high: pd.DataFrame,
    low: pd.DataFrame,
    *,
    left_right: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """确认日枢轴高/低价宽表（未确认日为 NaN）.

    Returns:
        ``(swing_high, swing_low)``，索引与输入对齐；值写在**确认日**。
    """
    high = high.sort_index().astype(float)
    low = low.sort_index().astype(float)
    if not high.columns.equals(low.columns) or not high.index.equals(low.index):
        raise ValueError("high/low 的 index/columns 必须一致")
    sh = pd.DataFrame(np.nan, index=high.index, columns=high.columns, dtype="float64")
    sl = pd.DataFrame(np.nan, index=low.index, columns=low.columns, dtype="float64")
    for col in high.columns:
        sh[col] = _confirmed_swing_series(high[col], left_right=left_right, mode="high")
        sl[col] = _confirmed_swing_series(low[col], left_right=left_right, mode="low")
    return sh, sl


def _structure_state_one(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    *,
    left_right: int,
) -> pd.DataFrame:
    """单标的结构状态机 → DataFrame 列:

    direction, bos_count, choch,     last_swing_high, last_swing_low, swing_age,
    impulse_pct, pullback_pct, ext_ratio
    """
    close = close.astype(float)
    sh = _confirmed_swing_series(high, left_right=left_right, mode="high")
    sl = _confirmed_swing_series(low, left_right=left_right, mode="low")

    idx = close.index
    n = len(idx)
    direction = np.zeros(n, dtype=float)
    bos_count = np.zeros(n, dtype=float)
    choch = np.zeros(n, dtype=float)
    last_sh_arr = np.full(n, np.nan)
    last_sl_arr = np.full(n, np.nan)
    impulse_arr = np.full(n, np.nan)
    pullback_arr = np.full(n, np.nan)
    ext_arr = np.full(n, np.nan)
    age_arr = np.full(n, np.nan)
    last_event_i = -1

    last_sh = np.nan
    last_sl = np.nan
    prev_sh = np.nan
    prev_sl = np.nan
    dir_ = 0
    bos_n = 0
    # 最近一段推进的锚点（多头：从 HL 到 SH）
    leg_low = np.nan
    leg_high = np.nan

    cvals = close.to_numpy(dtype=float, copy=False)
    shv = sh.to_numpy(dtype=float, copy=False)
    slv = sl.to_numpy(dtype=float, copy=False)

    for i in range(n):
        event_choch = 0.0

        if not np.isnan(shv[i]):
            prev_sh = last_sh
            last_sh = shv[i]
            leg_high = last_sh
            if not np.isnan(last_sl):
                leg_low = last_sl

        if not np.isnan(slv[i]):
            prev_sl = last_sl
            last_sl = slv[i]
            leg_low = last_sl
            if not np.isnan(last_sh):
                leg_high = last_sh

        # 用最近两个同侧摆动更新结构方向
        if not np.isnan(last_sh) and not np.isnan(prev_sh) and not np.isnan(last_sl) and not np.isnan(prev_sl):
            hh = last_sh > prev_sh
            hl = last_sl > prev_sl
            ll = last_sl < prev_sl
            lh = last_sh < prev_sh
            if hh and hl:
                if dir_ != 1:
                    bos_n = 0
                dir_ = 1
            elif ll and lh:
                if dir_ != -1:
                    bos_n = 0
                dir_ = -1
            elif (hh and ll) or (lh and hl):
                # 混杂：不立即清零方向，保持至破位
                pass

        px = cvals[i]
        prev_px = cvals[i - 1] if i > 0 else np.nan
        if not np.isnan(px) and dir_ == 1 and not np.isnan(last_sh) and not np.isnan(last_sl):
            # BOS: 收盘首次站上最近 swing high（不改写枢轴价）
            if px > last_sh and (np.isnan(prev_px) or prev_px <= last_sh):
                bos_n += 1
                leg_high = max(leg_high, px) if not np.isnan(leg_high) else px
            elif px < last_sl and (np.isnan(prev_px) or prev_px >= last_sl):
                event_choch = 1.0
                dir_ = 0
                bos_n = 0
        elif not np.isnan(px) and dir_ == -1 and not np.isnan(last_sh) and not np.isnan(last_sl):
            if px < last_sl and (np.isnan(prev_px) or prev_px >= last_sl):
                bos_n += 1
                leg_low = min(leg_low, px) if not np.isnan(leg_low) else px
            elif px > last_sh and (np.isnan(prev_px) or prev_px <= last_sh):
                event_choch = 1.0
                dir_ = 0
                bos_n = 0

        # impulse / pullback（相对价格）
        imp = np.nan
        pull = np.nan
        ext = np.nan
        if dir_ == 1 and not np.isnan(leg_low) and not np.isnan(leg_high) and leg_low > 0:
            imp = leg_high / leg_low - 1.0
            if not np.isnan(last_sl) and leg_high > 0:
                pull = max(0.0, 1.0 - last_sl / leg_high)
            if pull is not None and not np.isnan(pull) and pull > 1e-6 and not np.isnan(imp):
                ext = float(np.clip(imp / pull, 0.0, 10.0))
            elif not np.isnan(imp):
                ext = float(np.clip(imp / 0.05, 0.0, 10.0))
        elif dir_ == -1 and not np.isnan(leg_high) and not np.isnan(leg_low) and leg_high > 0:
            imp = 1.0 - leg_low / leg_high
            if not np.isnan(last_sh) and leg_low > 0:
                pull = max(0.0, last_sh / leg_low - 1.0)
            if pull is not None and not np.isnan(pull) and pull > 1e-6 and not np.isnan(imp):
                ext = float(np.clip(imp / pull, 0.0, 10.0))
            elif not np.isnan(imp):
                ext = float(np.clip(imp / 0.05, 0.0, 10.0))

        if not np.isnan(shv[i]) or not np.isnan(slv[i]):
            last_event_i = i
        if last_event_i >= 0:
            age_arr[i] = float(i - last_event_i)

        direction[i] = float(dir_)
        bos_count[i] = float(bos_n)
        choch[i] = event_choch
        last_sh_arr[i] = last_sh
        last_sl_arr[i] = last_sl
        impulse_arr[i] = imp
        pullback_arr[i] = pull
        ext_arr[i] = ext

    return pd.DataFrame(
        {
            "direction": direction,
            "bos_count": bos_count,
            "choch": choch,
            "last_swing_high": last_sh_arr,
            "last_swing_low": last_sl_arr,
            "swing_age": age_arr,
            "impulse_pct": impulse_arr,
            "pullback_pct": pullback_arr,
            "ext_ratio": ext_arr,
        },
        index=idx,
    )


def structure_state_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    left_right: int = 3,
) -> dict[str, pd.DataFrame]:
    """市场结构状态宽表集合.

    Returns:
        字典含 ``direction, bos_count, choch, last_swing_high, last_swing_low,
        swing_age, impulse_pct, pullback_pct, ext_ratio``。
    """
    close = close.sort_index().astype(float)
    if high is None:
        high = close
    else:
        high = high.sort_index().astype(float)
    if low is None:
        low = close
    else:
        low = low.sort_index().astype(float)

    keys = [
        "direction",
        "bos_count",
        "choch",
        "last_swing_high",
        "last_swing_low",
        "swing_age",
        "impulse_pct",
        "pullback_pct",
        "ext_ratio",
    ]
    out = {
        k: pd.DataFrame(np.nan, index=close.index, columns=close.columns, dtype="float64")
        for k in keys
    }
    for col in close.columns:
        one = _structure_state_one(
            close[col], high[col], low[col], left_right=left_right
        )
        for k in keys:
            out[k][col] = one[k]
    return out


def bos_choch_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    left_right: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """便捷接口: ``(direction, bos_count, choch)``."""
    st = structure_state_panels(
        close, high=high, low=low, left_right=left_right
    )
    return st["direction"], st["bos_count"], st["choch"]


def impulse_pullback_panel(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    left_right: int = 3,
) -> pd.DataFrame:
    """结构扩展比 ``impulse / pullback``（截断前原始值，缺失为 NaN）."""
    return structure_state_panels(
        close, high=high, low=low, left_right=left_right
    )["ext_ratio"]


def rolling_rank_panel(score: pd.DataFrame, *, window: int = 252) -> pd.DataFrame:
    """每列滚动百分位秩 ∈ [0,1]（自身历史分位）."""
    score = score.sort_index().astype(float)

    def _rank_last(x: np.ndarray) -> float:
        if x.shape[0] == 0 or np.isnan(x[-1]):
            return np.nan
        v = x[~np.isnan(x)]
        if v.size == 0:
            return np.nan
        return float((v <= v[-1]).mean())

    return score.rolling(window, min_periods=max(20, window // 5)).apply(
        _rank_last, raw=True
    )


def cross_sectional_rank_panel(score: pd.DataFrame) -> pd.DataFrame:
    """截面百分位秩 ∈ [0,1]（按行）."""
    score = score.sort_index().astype(float)
    return score.rank(axis=1, pct=True, method="average")
