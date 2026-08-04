"""Sharpe 三族 — 书 Ch14 §14.7.

- SR : 原始 Sharpe
- PSR : Probabilistic Sharpe（修正小样本 + 偏度 + 峰度）
- DSR : Deflated Sharpe（PSR + 多重检验校正，需要 trial registry 信息）
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm


def sharpe_ratio(returns: pd.Series) -> float:
    """原始 SR. 假设 returns 已经是超额收益."""
    returns = returns.dropna()
    if len(returns) < 2 or returns.std(ddof=1) == 0:
        return 0.0
    return float(returns.mean() / returns.std(ddof=1))


def annualized_sharpe(returns: pd.Series, periods_per_year: int = 252) -> float:
    sr = sharpe_ratio(returns)
    return sr * np.sqrt(periods_per_year)


def probabilistic_sharpe_ratio(
    returns: pd.Series,
    sr_benchmark: float = 0.0,
) -> float:
    """PSR — 书 Ch14 Snippet 公式.

    PSR = Φ[ (SR_hat - SR*) * sqrt(T-1) /
             sqrt(1 - γ3 * SR_hat + (γ4-1)/4 * SR_hat^2) ]

    SR_hat 在 returns 的"原始采样频率"下计算（非年化）。
    """
    r = returns.dropna()
    T = len(r)
    if T < 4:
        return float("nan")

    sr = sharpe_ratio(r)
    skew = float(((r - r.mean()) ** 3).mean() / (r.std(ddof=1) ** 3))
    kurt = float(((r - r.mean()) ** 4).mean() / (r.std(ddof=1) ** 4))

    denom = 1.0 - skew * sr + (kurt - 1) / 4 * sr ** 2
    if denom <= 0:
        return float("nan")
    z = (sr - sr_benchmark) * np.sqrt(T - 1) / np.sqrt(denom)
    return float(norm.cdf(z))


def expected_max_sharpe(n_trials: int, var_sr: float = 1.0) -> float:
    """E[max SR over N trials] — 书 Ch14 Snippet.

    用 Euler-Mascheroni 近似。
    """
    if n_trials < 2:
        return 0.0
    gamma = 0.5772156649
    term1 = (1 - gamma) * norm.ppf(1 - 1.0 / n_trials)
    term2 = gamma * norm.ppf(1 - 1.0 / (n_trials * np.e))
    return float(np.sqrt(var_sr) * (term1 + term2))


def deflated_sharpe_ratio(
    returns: pd.Series,
    n_trials: int,
    var_trials_sr: float,
) -> float:
    """DSR — 书 Ch14 §14.7.3.

    用"多次试验下最大 SR 的期望" 作为 PSR 的基准。
    """
    sr_star = expected_max_sharpe(n_trials, var_trials_sr)
    return probabilistic_sharpe_ratio(returns, sr_benchmark=sr_star)
