"""趋势诊断多样本评估指标（研究用，非交易信号）."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.diagnostics.trend import trend_panels


def _flip_rate(s: pd.Series) -> float:
    s = s.dropna()
    if len(s) < 2:
        return float("nan")
    return float((s.diff().fillna(0) != 0).mean())


def score_trend_panels(
    close: pd.Series,
    high: pd.Series | None = None,
    low: pd.Series | None = None,
    *,
    eval_start: str | None = None,
) -> dict[str, float]:
    """对单标的方向面板打分。翻转少、和长均线同拍、强动量窗能认出多头。"""
    close = close.astype(float).sort_index()
    sym = "S"
    c = close.to_frame(sym)
    h = (high if high is not None else close).astype(float).to_frame(sym)
    l = (low if low is not None else close).astype(float).to_frame(sym)

    panels = trend_panels(c, high=h, low=l, include_xs_rank=False)
    if eval_start is not None:
        panels = {k: df.loc[eval_start:] for k, df in panels.items()}
        close = close.loc[eval_start:]

    d = panels["direction"][sym]
    efficiency = panels["efficiency"][sym]
    n = max(len(d), 1)
    dir_flip = _flip_rate(d)
    stab_dir = float(np.clip(1.0 - dir_flip / 0.12, 0.0, 1.0))

    ma200 = close.rolling(200, min_periods=200).mean()
    ma_slope = ma200.diff(20)
    below_bear = (close < ma200) & (ma_slope < 0)
    above_bull = (close > ma200) & (ma_slope > 0)
    bear_agree = float((d.loc[below_bear] <= 0).mean()) if below_bear.any() else float("nan")
    bull_agree = float((d.loc[above_bull] >= 0).mean()) if above_bull.any() else float("nan")

    ret20 = close.pct_change(20)
    thrust = (ret20 > 0.12) & (efficiency > 0.50)
    thrust_dir = float((d.loc[thrust] > 0).mean()) if thrust.any() else float("nan")

    raw = {
        "stab_dir": stab_dir,
        "bear_agree": bear_agree,
        "bull_agree": bull_agree,
        "thrust_dir": thrust_dir,
    }
    weights = {"stab_dir": 0.25, "bear_agree": 0.30, "bull_agree": 0.20, "thrust_dir": 0.25}
    num = den = 0.0
    used = 0
    for k, w in weights.items():
        v_ = raw[k]
        if np.isnan(v_):
            continue
        num += w * float(v_)
        den += w
        used += 1
    score = float(num / den) if den > 0 else float("nan")
    return {
        "score": score,
        "dir_flip_rate": dir_flip,
        "mean_efficiency": float(efficiency.mean(skipna=True)),
        "n_bars": float(n),
        "n_metrics_used": float(used),
        **{f"w_{k}": (0.0 if np.isnan(v) else float(v)) for k, v in raw.items()},
    }
