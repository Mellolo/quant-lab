"""整段 [起点 → 今天] 的留存和三个效率。"""

from __future__ import annotations

import numpy as np

EFF_HALFLIFE = 16.0


def _on_path(oi: int, i: int, direction: float) -> bool:
    return i >= oi and np.isfinite(direction) and direction != 0


def _retention(close: np.ndarray, oi: int, i: int, direction: float, origin: float) -> float:
    """这条路径最远走到哪，今天还留着多少。"""
    if not _on_path(oi, i, direction) or not np.isfinite(origin):
        return float("nan")
    signed = (close[oi : i + 1].astype(float) - float(origin)) * float(np.sign(direction))
    signed = signed[np.isfinite(signed)]
    if signed.size == 0:
        return float("nan")
    progress = float(np.nanmax(signed))
    if progress <= 1e-12:
        return 0.0
    return float(np.clip(signed[-1] / progress, 0.0, 1.0))


def _signed_er(moves: np.ndarray, direction: float, *, lo: float, hi: float) -> float:
    """方向 × 近权净位移 / 近权路程，裁到 [lo, hi]。"""
    ok = np.isfinite(moves)
    if not ok.any():
        return float("nan")
    w = 2.0 ** ((np.arange(moves.size, dtype=float) - (moves.size - 1)) / EFF_HALFLIFE)
    path = float(np.sum(w[ok] * np.abs(moves[ok])))
    if path <= 1e-12:
        return 0.0
    net = float(np.sum(w[ok] * moves[ok]))
    return float(np.clip(net * float(np.sign(direction)) / path, lo, hi))


def _gap_session(open_: np.ndarray, close: np.ndarray, oi: int, i: int) -> tuple[np.ndarray, np.ndarray]:
    o = open_[oi : i + 1].astype(float)
    c = close[oi : i + 1].astype(float)
    prev = close[oi:i].astype(float)
    return o[1:] - prev, c[1:] - o[1:]


def _efficiency(close: np.ndarray, oi: int, i: int, direction: float) -> float:
    """收盘到收盘。顺势裁到 [0, 1]，对着干记 0。"""
    if not _on_path(oi, i, direction):
        return float("nan")
    if i == oi:
        return 1.0
    return _signed_er(np.diff(close[oi : i + 1].astype(float)), direction, lo=0.0, hi=1.0)


def _overnight_efficiency(
    open_: np.ndarray, close: np.ndarray, oi: int, i: int, direction: float
) -> float:
    """隔夜跳空。跟方向对着干为负，[-1, 1]。"""
    if not _on_path(oi, i, direction) or i == oi:
        return float("nan")
    gap, _sess = _gap_session(open_, close, oi, i)
    return _signed_er(gap, direction, lo=-1.0, hi=1.0)


def _session_efficiency(
    open_: np.ndarray, close: np.ndarray, oi: int, i: int, direction: float
) -> float:
    """开盘到收盘。跟方向对着干为负，[-1, 1]。"""
    if not _on_path(oi, i, direction) or i == oi:
        return float("nan")
    _gap, sess = _gap_session(open_, close, oi, i)
    return _signed_er(sess, direction, lo=-1.0, hi=1.0)


def _open_or_ungapped(open_: np.ndarray | None, close: np.ndarray) -> np.ndarray:
    if open_ is not None:
        return open_.astype(float)
    out = np.empty_like(close, dtype=float)
    out[0] = close[0]
    if len(close) > 1:
        out[1:] = close[:-1]
    return out
