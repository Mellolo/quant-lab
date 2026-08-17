"""宽表整形、粘滞、空面板。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def as_ohlcv_frame(ohlcv: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(ohlcv, pd.DataFrame):
        raise TypeError("ohlcv 必须是 DataFrame")
    df = ohlcv.sort_index().copy()
    if "close" not in df.columns:
        raise ValueError("ohlcv 至少需要 close 列")
    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    return df


def resolve_asof(index: pd.Index, asof: object | None) -> pd.Timestamp:
    if asof is None:
        return pd.Timestamp(index[-1])
    ts = pd.Timestamp(asof)
    if ts in index:
        return ts
    ok = index[index <= ts]
    if len(ok) == 0:
        raise ValueError(f"asof={asof} 早于序列起点")
    return pd.Timestamp(ok[-1])


def wide_ohlc(
    close: pd.DataFrame,
    high: pd.DataFrame | None,
    low: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    close = close.sort_index().astype(float)
    high = close if high is None else high.reindex_like(close).astype(float)
    low = close if low is None else low.reindex_like(close).astype(float)
    return close, high, low


def empty_like(close: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(np.nan, index=close.index, columns=close.columns, dtype="float64")


def run_age(direction: np.ndarray, *, stay_zero: bool = False) -> np.ndarray:
    """连续停留的根数。``stay_zero`` 数方向为 0，否则数同号非 0。"""
    n = len(direction)
    age = np.full(n, np.nan)
    for i in range(n):
        d = direction[i]
        if not np.isfinite(d) or (d == 0) != stay_zero:
            continue
        age[i] = 0.0 if i == 0 or direction[i - 1] != d else age[i - 1] + 1.0
    return age


def hysteresis_series(raw: np.ndarray, hold: int) -> np.ndarray:
    """连续 ``hold`` 根才切换。"""
    n = len(raw)
    out = np.empty(n, dtype=float)
    if n == 0:
        return out
    cur = pending = float(raw[0])
    run = 0
    for i, v in enumerate(raw):
        if v == cur:
            pending, run = v, 0
        elif v == pending:
            run += 1
            if run >= hold:
                cur, run = pending, 0
        else:
            pending, run = v, 1
            if run >= hold:
                cur, run = pending, 0
        out[i] = cur
    return out


def hysteresis_direction(raw: pd.DataFrame, hold: int = 3) -> pd.DataFrame:
    if hold <= 1:
        return raw.fillna(0.0).astype("float64")
    out = pd.DataFrame(0.0, index=raw.index, columns=raw.columns, dtype="float64")
    for col in raw.columns:
        out[col] = hysteresis_series(raw[col].fillna(0.0).to_numpy(dtype=float), hold)
    return out
