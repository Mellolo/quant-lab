"""CPCV (Combinatorial Purged Cross-Validation) — 书 Ch12 §12.4 ★.

WF / CV 只有单一回测路径；CPCV 通过组合训练/测试集生成多条路径。
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from itertools import combinations
from math import comb

import numpy as np
import pandas as pd


class CombinatorialPurgedCV:
    """生成 (训练集, 测试集) 组合 + 重组为路径.

    使用方法::

        cpcv = CombinatorialPurgedCV(N=10, k=2, t1=t1, embargo_pct=0.01)
        split_preds: list[dict[int, pd.Series]] = []
        for train_idx, test_idx, test_groups in cpcv.split(X):
            model = fit(X.iloc[train_idx], y.iloc[train_idx])
            pred = pd.Series(model.predict(X.iloc[test_idx]), index=X.index[test_idx])
            # 按测试组切开
            by_group = {}
            for g in test_groups:
                g_idx = cpcv.group_indices[g]  # 原 X 行号
                by_group[g] = pred.reindex(X.index[g_idx]).dropna()
            split_preds.append(by_group)

        paths = cpcv.assemble_paths(split_preds)
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
        # split() 后填充: 每组在**原 X** 中的行号
        self.group_indices: list[np.ndarray] = []
        # split() 后填充: 每个 split 的 {group_id: path_id}
        self._path_map: list[dict[int, int]] = []

    @property
    def n_paths(self) -> int:
        """可生成的回测路径数: φ(N, k) = k/N * C(N, N-k) = C(N-1, k-1)."""
        return self.k * comb(self.N, self.N - self.k) // self.N

    @property
    def n_splits(self) -> int:
        """训练/测试组合总数."""
        return comb(self.N, self.k)

    def _group_indices_sorted(self, n_samples: int) -> list[np.ndarray]:
        """把已按时间排序的 [0, n_samples) 分成 N 组."""
        edges = np.linspace(0, n_samples, self.N + 1).astype(int)
        return [np.arange(edges[i], edges[i + 1]) for i in range(self.N)]

    def _build_path_map(self) -> list[dict[int, int]]:
        """书 Figure 12.2: 对每个 split, 把各测试组映射到路径 id.

        对每个 group g, 按含 g 的 split 的词典序依次赋 path 0..φ-1。
        这保证每条路径恰好覆盖全部 N 个组各一次。
        """
        test_combos = list(combinations(range(self.N), self.k))
        # group -> 有序 split ids
        group_splits: dict[int, list[int]] = {g: [] for g in range(self.N)}
        for sid, combo in enumerate(test_combos):
            for g in combo:
                group_splits[g].append(sid)

        path_map: list[dict[int, int]] = [{} for _ in test_combos]
        for g, sids in group_splits.items():
            for path_id, sid in enumerate(sids):
                path_map[sid][g] = path_id
        return path_map

    def split(self, X: pd.DataFrame) -> Iterator[tuple[np.ndarray, np.ndarray, list[int]]]:
        """遍历所有组合.

        yield (train_idx, test_idx, test_group_ids) —— 索引均为**原 X** 的行位置。
        分组在时间排序后的样本上做, 避免 X 无序时切出非时间块。
        """
        if len(X) < self.N:
            raise ValueError(
                f"样本数({len(X)}) 少于分组数 N({self.N}), 无法切分 —— "
                "会产生空分组并让路径失去意义。\n"
                "  出路: 调小 N, 或放宽事件采样频率以得到更多样本。"
            )

        # 稳定按事件开始时间排序, 再映射回原行号
        start_dt = pd.DatetimeIndex(pd.to_datetime(pd.Series(X.index)))
        order = np.argsort(start_dt.asi8, kind="mergesort")
        # order[s] = 时间序第 s 个样本在原 X 中的行号
        inv = order

        n = len(X)
        groups_sorted = self._group_indices_sorted(n)
        # 暴露为原 X 行号, 供 assemble_paths 调用方按组切预测
        self.group_indices = [inv[g] for g in groups_sorted]
        self._path_map = self._build_path_map()

        embargo = int(n * self.embargo_pct)
        t1_sorted = pd.to_datetime(pd.Series(self.t1.to_numpy()[order]))
        start_sorted = pd.Series(start_dt[order])

        for sid, test_groups in enumerate(combinations(range(self.N), self.k)):
            test_sorted = np.concatenate([groups_sorted[g] for g in test_groups])
            train_sorted = np.concatenate(
                [groups_sorted[g] for g in range(self.N) if g not in test_groups]
            )

            # purge: 与**任一**测试组时间窗相交即剔除(非全局 span —
            # 非邻接测试组时全局 span 会误删中间训练段)
            tr_start = start_sorted.iloc[train_sorted].to_numpy()
            tr_end = t1_sorted.iloc[train_sorted].to_numpy()
            overlap = np.zeros(len(train_sorted), dtype=bool)
            for g in test_groups:
                g_idx = groups_sorted[g]
                g_start = start_sorted.iloc[g_idx].min()
                g_end = t1_sorted.iloc[g_idx].max()
                overlap |= (tr_end >= g_start) & (tr_start <= g_end)
            train_purged = train_sorted[~overlap]

            # embargo: 每个测试组结束后再删一段
            if embargo > 0 and len(train_purged) > 0:
                keep = np.ones(len(train_purged), dtype=bool)
                for g in test_groups:
                    test_max = int(groups_sorted[g].max())
                    lo, hi = test_max + 1, min(test_max + 1 + embargo, n)
                    keep &= (train_purged < lo) | (train_purged >= hi)
                train_purged = train_purged[keep]

            yield (
                np.sort(inv[train_purged]),
                np.sort(inv[test_sorted]),
                list(test_groups),
            )

    def assemble_paths(
        self,
        per_split_predictions: list[dict[int, pd.Series]],
    ) -> list[pd.Series]:
        """把各 split 的预测重组为 φ 条完整路径 — 书 Figure 12.2.

        Parameters
        ----------
        per_split_predictions
            长度 = ``n_splits``, 与 ``split()`` 遍历顺序一致。
            每个元素是 ``{test_group_id: pred_series}``, pred_series 为该组
            在该 split 上的预测(index 任意, 通常为事件时间)。

        Returns
        -------
        list[pd.Series]
            ``n_paths`` 条路径; 每条按组序 0..N-1 拼接各组预测后 ``sort_index``。
        """
        if not self._path_map:
            raise RuntimeError(
                "assemble_paths 需要先调用 split() 以构建路径映射。"
            )
        if len(per_split_predictions) != len(self._path_map):
            raise ValueError(
                f"per_split_predictions 长度 ({len(per_split_predictions)}) "
                f"与 n_splits ({len(self._path_map)}) 不符"
            )

        # path_id -> list of (group_id, series)
        path_segments: dict[int, list[tuple[int, pd.Series]]] = defaultdict(list)
        for sid, split_info in enumerate(per_split_predictions):
            mapping = self._path_map[sid]
            for gid, preds in split_info.items():
                if gid not in mapping:
                    raise ValueError(
                        f"split {sid}: group {gid} 不在该 split 的测试组映射中"
                    )
                path_segments[mapping[gid]].append((gid, preds))

        paths: list[pd.Series] = []
        for path_id in range(self.n_paths):
            segs = sorted(path_segments.get(path_id, []), key=lambda x: x[0])
            if not segs:
                continue
            # 每条路径应覆盖全部 N 组
            groups_present = {g for g, _ in segs}
            if groups_present != set(range(self.N)):
                missing = set(range(self.N)) - groups_present
                raise ValueError(
                    f"path {path_id} 缺少组 {sorted(missing)}; "
                    "请确认每个 split 都提供了全部测试组的预测。"
                )
            paths.append(pd.concat([s for _, s in segs]).sort_index())
        return paths
