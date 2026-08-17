"""折线找起点。只服务战役，不评方向。"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np

SWING_LEFT_RIGHT = 2
MIN_LEG_BARS = 5
MIN_LEG_ATR = 2.0


def _is_pivot(src: np.ndarray, c: int, L: int, *, peak: bool) -> bool:
    v = src[c]
    left, right = src[c - L : c], src[c + 1 : c + L + 1]
    if not (
        np.isfinite(v)
        and len(left) == L
        and len(right) == L
        and np.isfinite(left).any()
        and np.isfinite(right).any()
    ):
        return False
    if peak:
        return v > float(np.nanmax(left)) and v > float(np.nanmax(right))
    return v < float(np.nanmin(left)) and v < float(np.nanmin(right))


def _tv_pivots_all(
    high: np.ndarray, low: np.ndarray, left_right: int
) -> list[tuple[int, int, float, str]]:
    L = int(left_right)
    if L < 1:
        raise ValueError("left_right 必须 >= 1")
    out: list[tuple[int, int, float, str]] = []
    for t in range(2 * L, len(high)):
        c = t - L
        if _is_pivot(high, c, L, peak=True):
            out.append((t, c, float(high[c]), "H"))
        if _is_pivot(low, c, L, peak=False):
            out.append((t, c, float(low[c]), "L"))
    out.sort(key=lambda x: (x[1], 0 if x[3] == "H" else 1))
    return out


def tv_pivots(
    high: np.ndarray,
    low: np.ndarray,
    *,
    left_right: int = SWING_LEFT_RIGHT,
    asof: int | None = None,
) -> list[tuple[int, float, str]]:
    """TradingView ``pivothigh/pivotlow(src, L, L)``，确认日 ≤ asof。"""
    last = len(high) - 1 if asof is None else int(asof)
    return [(i, px, k) for c, i, px, k in _tv_pivots_all(high, low, left_right) if c <= last]


def _atr_at(atr: np.ndarray | None, *idxs: int) -> float:
    if atr is None or len(atr) == 0:
        return float("nan")
    for j in idxs:
        if 0 <= j < len(atr) and np.isfinite(atr[j]) and atr[j] > 0:
            return float(atr[j])
    return float("nan")


def zigzag(
    pivots: list[tuple[int, float, str]],
    *,
    atr: np.ndarray | None = None,
    min_bars: int = MIN_LEG_BARS,
    min_atr: float = MIN_LEG_ATR,
) -> list[tuple[int, float, str]]:
    """高低交替。同向留更极端的；反向腿要满 ``min_bars`` 根或 ``min_atr`` × ATR。"""
    if not pivots:
        return []
    out = [pivots[0]]
    for idx, px, kind in pivots[1:]:
        last_i, last_px, last_k = out[-1]
        if kind == last_k:
            if (kind == "H" and px >= last_px) or (kind == "L" and px <= last_px):
                out[-1] = (idx, px, kind)
            continue
        long_bars = idx - last_i + 1 >= int(min_bars)
        a = _atr_at(atr, idx, last_i)
        long_atr = np.isfinite(a) and abs(float(px) - float(last_px)) >= float(min_atr) * a
        if long_bars or long_atr:
            out.append((idx, px, kind))
    return out


def structure_origin(
    zig: list[tuple[int, float, str]],
    direction: float,
) -> tuple[int, float] | None:
    """多头沿更高低点回到这段谷；空头沿更低高点回到这段峰。"""
    if not np.isfinite(direction) or direction == 0:
        return None
    want = "H" if direction < 0 else "L"
    better = (lambda prev, cur: prev > cur) if direction < 0 else (lambda prev, cur: prev < cur)
    seq = [(i, p) for i, p, k in zig if k == want]
    if not seq:
        return None
    oi, op = seq[-1]
    for i, p in reversed(seq[:-1]):
        if better(p, op):
            oi, op = i, p
        else:
            break
    return oi, op


def _origin_fallback(
    high: np.ndarray, low: np.ndarray, *, direction: float, asof: int
) -> tuple[int, float] | None:
    src = low if direction > 0 else high
    window = src[max(0, asof - 40) : asof + 1]
    if not np.isfinite(window).any():
        return None
    pick = int(np.nanargmin(window) if direction > 0 else np.nanargmax(window))
    oi = max(0, asof - 40) + pick
    return oi, float(src[oi])


def _iter_campaigns(
    high: np.ndarray,
    low: np.ndarray,
    direction: np.ndarray,
    age: np.ndarray,
    atr: np.ndarray,
    *,
    left_right: int,
) -> Iterator[tuple[int, int, float, list[tuple[int, float, str]]]]:
    """有方向的日子：``(i, 起点下标, 起点价, 当日 zigzag)``。"""
    n = len(direction)
    all_pivots = _tv_pivots_all(high, low, left_right)
    starts = [i for i in range(n) if age[i] == 0]
    for s, end in zip(starts, starts[1:] + [n]):
        dd = direction[s]
        if not np.isfinite(dd) or dd == 0:
            continue
        for i in range(s, end):
            if direction[i] != dd:
                continue
            known = [(idx, px, kind) for c, idx, px, kind in all_pivots if c <= i]
            zig = zigzag(known, atr=atr)
            found = structure_origin(zig, dd) or _origin_fallback(
                high, low, direction=dd, asof=i
            )
            if found is not None:
                yield i, found[0], found[1], zig


def _campaign_pts(
    zig: list[tuple[int, float, str]],
    *,
    origin: tuple[int, float],
    i: int,
    direction: float,
    close: np.ndarray,
) -> list[tuple[int, float]]:
    """起点之后的折线点，今天收盘补到链尾。"""
    oi, op = origin
    after = [(j, p, k) for j, p, k in zig if j >= oi]
    if not after or after[0][0] != oi:
        after = [(oi, op, "H" if direction < 0 else "L")] + after
    pts = [(j, p) for j, p, _k in after if j <= i]
    if not pts or pts[-1][0] != i:
        pts = pts + [(i, float(close[i]))]
    return pts
