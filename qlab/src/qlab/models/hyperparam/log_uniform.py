"""Log-Uniform 分布 — 书 Ch9 Snippet 9.4.

对于 SVC 的 C、RBF 的 gamma 等非线性响应参数，
均匀采样在对数尺度更合适。
"""

from __future__ import annotations

import numpy as np
from scipy.stats import rv_continuous


class _LogUniform(rv_continuous):
    """log[x] ~ U[log(a), log(b)]."""

    def _cdf(self, x):
        return np.log(x / self.a) / np.log(self.b / self.a)

    def _pdf(self, x):
        return 1.0 / (x * np.log(self.b / self.a))


def log_uniform(a: float = 1.0, b: float = np.exp(1)) -> _LogUniform:
    """构造一个 log-uniform 分布对象，可传给 RandomizedSearchCV."""
    return _LogUniform(a=a, b=b, name="logUniform")
