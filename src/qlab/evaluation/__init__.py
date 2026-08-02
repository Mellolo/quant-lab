"""评估模块 — 书 Ch11-15.

- backtest/   WF / CV / CPCV
- statistics/ SR / PSR / DSR / HHI / DD / TuW
- risk/       策略失败概率
- pbo         概率回测过拟合（CSCV）
- trial_registry  实验追踪表（DSR 数据基础）
"""

from qlab.evaluation.backtest.cpcv import CombinatorialPurgedCV
from qlab.evaluation.backtest.walk_forward import walk_forward_backtest
from qlab.evaluation.pbo import PBOResult, compute_pbo
from qlab.evaluation.risk.strategy_risk import implied_precision, prob_strategy_failure
from qlab.evaluation.statistics.classification import classification_scores
from qlab.evaluation.statistics.drawdown import compute_dd_tuw
from qlab.evaluation.statistics.runs import returns_hhi
from qlab.evaluation.statistics.sharpe import (
    annualized_sharpe,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    sharpe_ratio,
)
from qlab.evaluation.trial_registry import TrialRegistry

__all__ = [
    "CombinatorialPurgedCV",
    "walk_forward_backtest",
    "compute_pbo",
    "PBOResult",
    "implied_precision",
    "prob_strategy_failure",
    "classification_scores",
    "compute_dd_tuw",
    "returns_hhi",
    "sharpe_ratio",
    "annualized_sharpe",
    "probabilistic_sharpe_ratio",
    "deflated_sharpe_ratio",
    "TrialRegistry",
]
