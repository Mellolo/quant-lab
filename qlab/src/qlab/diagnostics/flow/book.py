"""资金博弈：用成交足迹推断区间仓强不强。不负责趋势方向/效率。"""

from __future__ import annotations

import numpy as np

EXTEND_ATR = 6.0
OIL_ATR = 1.0
FLOW_KEYS = (
    "hold",
    "trapped",
    "poc",
    "extend",
    "oil",
    "squeeze",
    "absorb",
)


def bar_weight(
    volume: np.ndarray | None,
    close: np.ndarray,
    float_shares: np.ndarray | None,
) -> np.ndarray | None:
    """优先换手 volume/流通股本；没有股本就用成交额。没有量则为空。"""
    if volume is None:
        return None
    if float_shares is not None:
        fs = float_shares.astype(float)
        w = np.where(np.isfinite(fs) & (fs > 0), volume.astype(float) / fs, np.nan)
        if np.isfinite(w).any():
            return w.astype(float)
    money = volume.astype(float) * close.astype(float)
    if np.isfinite(money).any():
        return money
    return None


def _weight_share(mask: np.ndarray, weight: np.ndarray) -> float:
    w = np.where(np.isfinite(weight), weight, 0.0)
    tot = float(w.sum())
    if tot <= 1e-12:
        return float("nan")
    return float(np.where(mask, w, 0.0).sum() / tot)


def _bar_accept(highs: np.ndarray, lows: np.ndarray, weight: np.ndarray) -> np.ndarray:
    """单位权重推动越小，接受度越高：吸收 ≈ 强，追赶/出清 ≈ 弱。典型日为 0.5。"""
    rng = np.maximum(highs.astype(float) - lows.astype(float), 0.0)
    w = np.where(np.isfinite(weight) & (weight > 0), weight.astype(float), np.nan)
    impact = rng / w
    med = float(np.nanmedian(impact))
    if not np.isfinite(med) or med <= 1e-18:
        med = 1.0
    accept = 1.0 / (1.0 + impact / med)
    return np.where(np.isfinite(accept), accept, 0.0)


def _bin_edges(lo: float, hi: float, atr: float, n_max: int = 12) -> np.ndarray:
    span = float(hi) - float(lo)
    if not np.isfinite(span) or span <= 1e-12:
        return np.array([float(lo), float(lo) + 1e-9])
    width = float(atr) if np.isfinite(atr) and atr > 0 else span / 8.0
    width = max(width, span / n_max)
    n = max(int(np.ceil(span / width)), 1)
    return np.linspace(float(lo), float(lo) + n * width, n + 1)


def _zone_book(
    prices: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    weight: np.ndarray,
    close_i: float,
    atr: float,
    camp_low: float,
    camp_high: float,
) -> tuple[float, float, float]:
    """区间簿：价下强仓 vs 价上弱仓。返回 book_ok, trapped, poc。"""
    w = np.where(np.isfinite(weight), weight.astype(float), 0.0)
    accept = _bar_accept(highs, lows, w)
    strong = w * accept
    edges = _bin_edges(camp_low, camp_high, atr)
    n = len(edges) - 1
    idx = np.clip(np.digitize(prices, edges) - 1, 0, n - 1)
    w_bin = np.zeros(n)
    s_bin = np.zeros(n)
    a_w = np.zeros(n)
    for b, wi, si, ai in zip(idx, w, strong, accept):
        w_bin[b] += wi
        s_bin[b] += si
        a_w[b] += wi * ai
    centers = 0.5 * (edges[:-1] + edges[1:])
    accept_bin = np.divide(a_w, w_bin, out=np.zeros(n), where=w_bin > 1e-12)
    below = centers <= close_i
    lock = float(s_bin[below].sum())
    pressure = float((w_bin[~below] * (0.5 + 0.5 * (1.0 - accept_bin[~below]))).sum())
    denom = lock + pressure
    if denom <= 1e-12:
        book_ok = trapped = float("nan")
    else:
        book_ok = lock / denom
        trapped = pressure / denom
    stot = float(strong.sum())
    if stot > 1e-12:
        poc = float(np.dot(np.where(np.isfinite(prices), prices, 0.0), strong) / stot)
    else:
        tot = float(w.sum())
        poc = (
            float(np.dot(np.where(np.isfinite(prices), prices, 0.0), w) / tot)
            if tot > 1e-12
            else float("nan")
        )
    return float(book_ok), float(trapped), float(poc)


def _last_against_span(
    pts: list[tuple[int, float]], direction: float
) -> tuple[int, int] | None:
    last: tuple[int, int] | None = None
    prev_kind: str | None = None
    seg_start: int | None = None
    last_end: int | None = None
    for (i0, p0), (i1, p1) in zip(pts, pts[1:]):
        move = float(p1) - float(p0)
        if abs(move) < 1e-12:
            continue
        kind = (
            "with"
            if (direction > 0 and move > 0) or (direction < 0 and move < 0)
            else "against"
        )
        if kind != prev_kind:
            if prev_kind == "against" and seg_start is not None and last_end is not None:
                last = (seg_start, last_end)
            seg_start = i0
            prev_kind = kind
        last_end = i1
    if prev_kind == "against" and seg_start is not None and last_end is not None:
        last = (seg_start, last_end)
    return last


def _span_absorb(
    span: tuple[int, int] | None,
    origin_i: int,
    highs: np.ndarray,
    lows: np.ndarray,
    weight: np.ndarray,
) -> float:
    if span is None:
        return float("nan")
    acc = _bar_accept(highs, lows, weight)
    a = max(0, span[0] - origin_i + 1)
    b = min(len(weight), span[1] - origin_i + 1)
    if b <= a:
        return float("nan")
    w = np.where(np.isfinite(weight[a:b]), weight[a:b], 0.0)
    tot = float(w.sum())
    if tot <= 1e-12:
        return float("nan")
    return float(np.dot(acc[a:b], w) / tot)


def _short_danger(pts: list[tuple[int, float]], origin_px: float) -> float:
    """最近已完成逆势腿的终点（更低高点）；没有则用起点峰。"""
    danger = float(origin_px)
    prev_kind: str | None = None
    prev_end = float(origin_px)
    for (_i0, p0), (_i1, p1) in zip(pts, pts[1:]):
        move = float(p1) - float(p0)
        if abs(move) < 1e-12:
            continue
        kind = "with" if move < 0 else "against"
        if prev_kind == "against" and kind == "with":
            danger = prev_end
        prev_kind = kind
        prev_end = float(p1)
    return danger


def score_book(
    *,
    direction: float,
    close_i: float,
    atr_i: float,
    camp_high: float,
    camp_low: float,
    prices: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    weight: np.ndarray | None,
    pts: list[tuple[int, float]],
    origin_px: float,
) -> dict[str, float]:
    """仓位分 = min(压力放松, 油还在)。多空公式不同。"""
    trapped = extend = oil = squeeze = poc = absorb = float("nan")
    book_ok = float("nan")
    if weight is not None:
        book_ok, trapped, poc = _zone_book(
            prices, highs, lows, weight, close_i, atr_i, camp_low, camp_high
        )
        if np.isfinite(atr_i) and atr_i > 0:
            if direction > 0 and np.isfinite(camp_high):
                climax = _weight_share(highs >= camp_high - OIL_ATR * atr_i, weight)
                oil = float(np.clip(1.0 - climax, 0.0, 1.0)) if np.isfinite(climax) else float("nan")
            elif direction < 0 and np.isfinite(camp_low):
                climax = _weight_share(lows <= camp_low + OIL_ATR * atr_i, weight)
                oil = float(np.clip(1.0 - climax, 0.0, 1.0)) if np.isfinite(climax) else float("nan")

    if direction > 0:
        if np.isfinite(poc) and np.isfinite(atr_i) and atr_i > 0:
            extend = max(close_i - poc, 0.0) / atr_i
        extend_ok = (
            EXTEND_ATR / (EXTEND_ATR + extend) if np.isfinite(extend) else float("nan")
        )
        parts = [x for x in (book_ok, extend_ok, oil) if np.isfinite(x)]
        hold = float(min(parts)) if parts else float("nan")
    else:
        danger = _short_danger(pts, origin_px)
        span = danger - camp_low if np.isfinite(camp_low) else float("nan")
        bounce = float("nan")
        if np.isfinite(span) and span > 1e-12:
            bounce = float(np.clip((close_i - camp_low) / span, 0.0, 1.0))
        elif np.isfinite(danger):
            bounce = 0.0 if close_i <= danger else 1.0
        if weight is not None and pts:
            absorb = _span_absorb(
                _last_against_span(pts, direction), int(pts[0][0]), highs, lows, weight
            )
        if np.isfinite(bounce) and np.isfinite(absorb):
            squeeze = float(np.clip(1.0 - bounce * (1.5 - absorb), 0.0, 1.0))
        elif np.isfinite(bounce):
            squeeze = float(1.0 - bounce)
        parts = [x for x in (squeeze, oil) if np.isfinite(x)]
        hold = float(min(parts)) if parts else float("nan")
    return {
        "hold": float(hold),
        "trapped": float(trapped),
        "poc": float(poc),
        "extend": float(extend),
        "oil": float(oil),
        "squeeze": float(squeeze),
        "absorb": float(absorb),
    }
