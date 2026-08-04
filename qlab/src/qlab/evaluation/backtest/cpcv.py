"""CPCV (Combinatorial Purged Cross-Validation) — 书 Ch12 §12.4 ★.

WF / CV 只有单一回测路径；CPCV 通过组合训练/测试集生成多条路径。
"""

from __future__ import annotations

from collections.abc import Iterator
from itertools import combinations

import numpy as np
import pandas as pd


class CombinatorialPurgedCV:
    """生成 (训练集, 测试集) 组合 + 重组为路径.

    使用方法::

        cpcv = CombinatorialPurgedCV(N=10, k=2, t1=t1, embargo_pct=0.01)
        for split_id, (train_idx, test_idx) in enumerate(cpcv.split(X)):
            model = fit(X.iloc[train_idx], y.iloc[train_idx])
            preds[test_idx, split_id] = model.predict_proba(X.iloc[test_idx])

        # 然后重组为 φ 条路径
        paths = cpcv.assemble_paths(preds)
    """

    def __init__(
        self,
        N: int,
        k: int,
        t1: pd.Series,
        embargo_pct: float = 0.0,
    ):
        """
        N : 把数据分成多少组
        k : 测试集组数（k <= N//2 推荐）
        t1 : Series indexed by event_start，值为事件结束时间
        embargo_pct : Embargo 比例
        """
        if k > N // 2:
            raise ValueError(f"k ({k}) 应 ≤ N//2 ({N // 2})")
        self.N = N
        self.k = k
        self.t1 = t1
        self.embargo_pct = embargo_pct

    @property
    def n_paths(self) -> int:
        """可生成的回测路径数: φ(N, k) = k/N * C(N, N-k)"""
        from math import comb
        return self.k * comb(self.N, self.N - self.k) // self.N

    @property
    def n_splits(self) -> int:
        """训练/测试组合总数."""
        from math import comb
        return comb(self.N, self.N - self.k)

    def _group_indices(self, n_samples: int) -> list[np.ndarray]:
        """把 [0, n_samples) 分成 N 组."""
        edges = np.linspace(0, n_samples, self.N + 1).astype(int)
        return [np.arange(edges[i], edges[i + 1]) for i in range(self.N)]

    def split(self, X: pd.DataFrame) -> Iterator[tuple[np.ndarray, np.ndarray, list[int]]]:
        """遍历所有组合.

        yield (train_idx, test_idx, test_group_ids)
        """
        if len(X) < self.N:
            raise ValueError(
                f"样本数({len(X)}) 少于分组数 N({self.N}), 无法切分 —— "
                "会产生空分组并让路径失去意义。\n"
                "  出路: 调小 N, 或放宽事件采样阈值以得到更多样本。"
            )
        groups = self._group_indices(len(X))
        embargo = int(len(X) * self.embargo_pct)
        # 按**位置**取 t1, 不依赖 index 唯一性 —— 多标的样本按 date 索引时
        # 同一天必有多个样本, ``X.index.get_loc()`` 会返回 slice/数组而非整数,
        # 旧实现因此在 embargo 过滤处报 ``unhashable type: 'slice'``。
        t1_pos = pd.to_datetime(pd.Series(self.t1.to_numpy()))
        start_pos = pd.to_datetime(pd.Series(X.index))

        for test_groups in combinations(range(self.N), self.k):
            # 测试集索引（所有 test 组的并集）
            test_idx_arr = np.concatenate([groups[g] for g in test_groups])

            # 训练集：其他组
            train_idx_arr = np.concatenate(
                [groups[g] for g in range(self.N) if g not in test_groups]
            )

            # purge: 移除标签区间与测试段重叠的训练样本(按位置向量化判定)
            te_start = start_pos.iloc[test_idx_arr].min()
            te_end = t1_pos.iloc[test_idx_arr].max()
            tr_start = start_pos.iloc[train_idx_arr].to_numpy()
            tr_end = t1_pos.iloc[train_idx_arr].to_numpy()
            # 区间相交即剔除(闭区间: 端点相接也算重叠)
            overlap = (tr_end >= te_start) & (tr_start <= te_end)
            train_purged_idx = train_idx_arr[~overlap]

            # embargo: 测试集结束后再删一段
            if embargo > 0:
                test_max_loc = int(test_idx_arr.max())
                lo, hi = test_max_loc + 1, min(test_max_loc + 1 + embargo, len(X))
                train_purged_idx = train_purged_idx[
                    (train_purged_idx < lo) | (train_purged_idx >= hi)
                ]

            yield (
                np.sort(train_purged_idx),
                np.sort(test_idx_arr),
                list(test_groups),
            )

    def assemble_paths(
        self,
        per_split_predictions: list[dict],
    ) -> list[pd.Series]:
        """把各 split 的预测重组为 φ 条完整路径.

        per_split_predictions : 每个 split 一个 dict {test_group_id: pred_series}
        """
        # 简化版：返回 per-group 的预测列表（每组在多个 split 中出现）
        # 用户可根据具体需求自行 reduce
        from collections import defaultdict
        group_preds: dict[int, list[pd.Series]] = defaultdict(list)
        for split_info in per_split_predictions:
            for gid, preds in split_info.items():
                group_preds[gid].append(preds)

        # 简化路径生成：对每个 group，从其多个预测中按 path_id 取一个
        paths = []
        for path_id in range(self.n_paths):
            path_segments = []
            for gid in range(self.N):
                preds_list = group_preds.get(gid, [])
                if preds_list:
                    seg = preds_list[path_id % len(preds_list)]
                    path_segments.append(seg)
            if path_segments:
                paths.append(pd.concat(path_segments).sort_index())
        return paths
