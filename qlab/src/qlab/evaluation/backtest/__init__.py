"""回测子模块 — 书 Ch12.

三种范式：
- walk_forward : 历史模拟（Ch12 §12.2）
- cv_backtest  : CV 回测（Ch12 §12.3）
- cpcv         : Combinatorial Purged CV（Ch12 §12.4）★ 推荐
"""

from qlab.evaluation.backtest.cpcv import CombinatorialPurgedCV
from qlab.evaluation.backtest.walk_forward import walk_forward_backtest

__all__ = ["CombinatorialPurgedCV", "walk_forward_backtest"]
