"""Runs / HHI — 书 Ch14 §14.5.1."""

from __future__ import annotations

import pandas as pd


def returns_hhi(returns: pd.Series) -> float:
    """Herfindahl-Hirschman 风格的收益集中度.

    h = (sum(w^2) - 1/n) / (1 - 1/n)
    其中 w = r / sum(r), n = len(r)。

    h = 0 全均匀；h = 1 只有一笔非零。
    """
    if returns.empty or len(returns) <= 2:
        return float("nan")
    w = returns / returns.sum() if returns.sum() != 0 else returns / 1
    hhi = (w ** 2).sum()
    n = len(returns)
    return float((hhi - 1.0 / n) / (1.0 - 1.0 / n))


def positive_hhi(returns: pd.Series) -> float:
    return returns_hhi(returns[returns >= 0])


def negative_hhi(returns: pd.Series) -> float:
    return returns_hhi(returns[returns < 0])


def time_hhi(returns: pd.Series, freq: str = "ME") -> float:
    """按时间窗口聚合后的 HHI（衡量 bet 在时间上的集中度）."""
    counts = returns.groupby(pd.Grouper(freq=freq)).count().astype(float)
    return returns_hhi(counts)
