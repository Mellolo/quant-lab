"""PurgedKFold — 书 Ch7 §7.4.

防止重叠标签导致的训练/测试集信息泄漏：
- Purge：从训练集中删除标签区间与测试集重叠的样本
- Embargo：测试集结束后再屏蔽一小段训练样本
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd


def get_train_times(t1: pd.Series, test_times: pd.Series) -> pd.Series:
    """从训练样本中 purge 与测试集重叠的样本 — 书 Ch7 Snippet 7.1.

    参数
    ----
    t1 : Series, index=event_start, value=event_end
    test_times : Series, index=test_event_start, value=test_event_end

    返回
    ----
    purge 后的 t1 子集
    """
    trn = t1.copy(deep=True)
    for i, j in test_times.items():
        # 训练起点在测试区间内
        df0 = trn[(i <= trn.index) & (trn.index <= j)].index
        # 训练终点在测试区间内
        df1 = trn[(i <= trn) & (trn <= j)].index
        # 训练区间包住测试区间
        df2 = trn[(trn.index <= i) & (j <= trn)].index
        trn = trn.drop(df0.union(df1).union(df2))
    return trn


class PurgedKFold:
    """带 Purge + Embargo 的 K-Fold — 书 Ch7 Snippet 7.3.

    sklearn 风格接口（实现 split），可直接传给 GridSearchCV / RandomizedSearchCV。
    """

    def __init__(
        self,
        n_splits: int = 3,
        t1: pd.Series | None = None,
        pct_embargo: float = 0.0,
    ):
        if t1 is None:
            raise ValueError("PurgedKFold 需要 t1 (Series of event end times)")
        if not isinstance(t1, pd.Series):
            raise ValueError("t1 必须是 pd.Series")
        self.n_splits = n_splits
        self.t1 = t1
        self.pct_embargo = pct_embargo

    def split(
        self, X: pd.DataFrame, y: pd.Series | None = None, groups: None = None,
    ) -> Iterator[tuple[np.ndarray, np.ndarray]]:
        if not X.index.equals(self.t1.index):
            raise ValueError("X 的 index 必须与 t1 的 index 一致")
        # 样本数不足时 fail-loud —— 不拦会在 np.array_split 后的空分组上
        # 报 IndexError: index 0 is out of bounds, 完全看不出真实原因。
        if len(X) < self.n_splits:
            raise ValueError(
                f"样本数({len(X)}) 少于 n_splits({self.n_splits}), 无法切分。\n"
                "  出路: 调小 n_splits, 或放宽事件采样阈值以得到更多样本。"
            )

        # 统一用"按时间排序的位置序列"做 purge——
        # 支持 DatetimeIndex 与 MultiIndex(date, symbol) 两种 X.index
        if isinstance(self.t1.index, pd.MultiIndex):
            time_lvl = pd.DatetimeIndex(self.t1.index.get_level_values(0))
        else:
            time_lvl = pd.DatetimeIndex(self.t1.index)

        # 强制转成 datetime64[ns] 以便 numpy 比较
        time_arr = np.asarray(time_lvl.values, dtype="datetime64[ns]")
        t1_arr = np.asarray(pd.to_datetime(self.t1.values), dtype="datetime64[ns]")
        sort_order = np.argsort(time_arr, kind="stable")
        sorted_time = time_arr[sort_order]
        sorted_t1 = t1_arr[sort_order]

        n = X.shape[0]
        indices = np.arange(n)
        mbrg = int(n * self.pct_embargo)

        # 按排序后位置等分为 n_splits 段
        test_starts = [
            (chunk[0], chunk[-1] + 1)
            for chunk in np.array_split(indices, self.n_splits)
        ]

        for i, j in test_starts:
            t0 = sorted_time[i]                          # 测试段起点（时间）
            test_pos = indices[i:j]                      # 排序后位置
            # 测试段内最大 t1 → 找其在排序时间中的右插位置, 作为右侧 embargo 起点
            max_t1 = sorted_t1[test_pos].max()
            max_t1_idx = int(np.searchsorted(sorted_time, max_t1, side="right"))

            # 训练集左侧：标签**严格早于**测试段起点的样本。
            # 必须用 `<` 而非 `<=` —— 标签恰在测试首日实现的样本(t1 == t0)
            # 用到了测试段第一天的价格, 与测试样本共享信息, 属泄漏。
            # 这与 :func:`get_train_times` 的闭区间判据保持一致
            # (那里 `(i <= trn) & (trn <= j)` 会剔除 t1 == 测试起点的样本)。
            left_mask = sorted_t1 < t0
            train_pos_left = indices[left_mask]

            # 训练集右侧：max_t1_idx + embargo 之后的全部位置
            if max_t1_idx + mbrg < n:
                right_start = min(max_t1_idx + mbrg, n)
                train_pos_right = indices[right_start:]
            else:
                train_pos_right = np.array([], dtype=int)

            train_pos = np.concatenate([train_pos_left, train_pos_right])

            # 映射回 X 的原始位置
            yield sort_order[train_pos], sort_order[test_pos]

    def get_n_splits(
        self, X: pd.DataFrame | None = None, y: pd.Series | None = None,
        groups: None = None,
    ) -> int:
        return self.n_splits
