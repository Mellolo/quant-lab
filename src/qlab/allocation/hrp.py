"""HRP (Hierarchical Risk Parity) — 书 Ch16.

三阶段：tree clustering → quasi-diagonalization → recursive bisection.
不需要协方差矩阵可逆——即使奇异矩阵也能算。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import scipy.cluster.hierarchy as sch

from qlab.allocation.ivp import inverse_variance_portfolio


class HierarchicalRiskParity:
    """HRP 配置器.

    用法::

        hrp = HierarchicalRiskParity()
        weights = hrp.allocate(returns)
    """

    def __init__(self, linkage_method: str = "single"):
        self.linkage_method = linkage_method
        self._link = None
        self._sort_idx = None
        self._weights = None

    def allocate(self, returns: pd.DataFrame) -> pd.Series:
        """根据收益矩阵计算 HRP 权重.

        参数
        ----
        returns : DataFrame, columns = assets

        返回
        ----
        Series indexed by asset, values sum to 1
        """
        cov = returns.cov()
        corr = returns.corr()
        return self.allocate_from_cov(cov, corr)

    def allocate_from_cov(
        self,
        cov: pd.DataFrame,
        corr: pd.DataFrame,
    ) -> pd.Series:
        """直接从协方差/相关矩阵分配.

        Raises:
            ValueError: cov 与 corr 形状/索引不匹配, 或矩阵为空。

        Note:
            资产数 ≤ 1 时直接返回 —— 聚类/二分递归需要至少 2 个资产,
            否则 scipy 会报“empty distance matrix”这种看不出原因的内部错误。
        """
        if not (cov.shape == corr.shape and cov.index.equals(corr.index)):
            raise ValueError("cov 与 corr 的形状或索引不匹配")
        n_assets = len(cov)
        if n_assets == 0:
            raise ValueError("HRP 需要至少 1 个资产, 协方差矩阵为空")
        if n_assets == 1:
            # 单资产: 权重必为 1, 无需聚类
            return pd.Series([1.0], index=cov.index, name="weight")

        # Stage 1: tree clustering
        dist = self._correl_dist(corr)
        # scipy 的 linkage 接受压缩距离向量；直接用 corr-based 距离矩阵
        from scipy.spatial.distance import squareform
        dist_vec = squareform(dist.values, checks=False)
        self._link = sch.linkage(dist_vec, method=self.linkage_method)

        # Stage 2: quasi-diagonalization
        self._sort_idx = self._quasi_diag(self._link)
        sort_labels = [cov.index[i] for i in self._sort_idx]

        # Stage 3: recursive bisection
        weights = self._recursive_bisection(cov, sort_labels)
        self._weights = weights
        return weights

    @staticmethod
    def _correl_dist(corr: pd.DataFrame) -> pd.DataFrame:
        """d_{ij} = sqrt((1 - ρ_{ij}) / 2)."""
        return ((1 - corr) / 2.0) ** 0.5

    @staticmethod
    def _quasi_diag(link: np.ndarray) -> list[int]:
        """从 linkage matrix 还原原始 item 顺序 — 书 Ch16 Snippet 16.2."""
        link = link.astype(int)
        sort_ix = pd.Series([link[-1, 0], link[-1, 1]])
        num_items = link[-1, 3]
        while sort_ix.max() >= num_items:
            sort_ix.index = range(0, sort_ix.shape[0] * 2, 2)
            df0 = sort_ix[sort_ix >= num_items]
            i = df0.index
            j = df0.values - num_items
            sort_ix.loc[i] = link[j, 0]
            df1 = pd.Series(link[j, 1], index=i + 1)
            sort_ix = pd.concat([sort_ix, df1]).sort_index()
            sort_ix.index = range(sort_ix.shape[0])
        return sort_ix.tolist()

    @staticmethod
    def _cluster_var(cov: pd.DataFrame, items: list) -> float:
        cov_ = cov.loc[items, items]
        w_ = inverse_variance_portfolio(cov_.values).reshape(-1, 1)
        return float(np.dot(np.dot(w_.T, cov_.values), w_)[0, 0])

    def _recursive_bisection(
        self,
        cov: pd.DataFrame,
        sort_labels: list,
    ) -> pd.Series:
        """递归二分配权重 — 书 Ch16 Snippet 16.3."""
        w = pd.Series(1.0, index=sort_labels)
        c_items = [sort_labels]

        while len(c_items) > 0:
            new_items: list[list] = []
            for cluster in c_items:
                if len(cluster) > 1:
                    mid = len(cluster) // 2
                    new_items.append(cluster[:mid])
                    new_items.append(cluster[mid:])
            c_items = new_items
            for i in range(0, len(c_items), 2):
                items0 = c_items[i]
                if i + 1 >= len(c_items):
                    break
                items1 = c_items[i + 1]
                v0 = self._cluster_var(cov, items0)
                v1 = self._cluster_var(cov, items1)
                alpha = 1 - v0 / (v0 + v1)
                w[items0] *= alpha
                w[items1] *= 1 - alpha
        return w
