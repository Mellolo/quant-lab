"""Batch ↔ Incremental 等价性测试 — 文档 §4.4 硬约束.

要求：对同一段 target_dates，无论以 mode='batch' 一次性算完，还是以
mode='incremental' 切成多段算，结果必须按"窗口内一致"语义相等。

仅检验"窗口型"特征（lookback 完全决定结果）。递归型特征（如 EwmVol）的
等价性依赖调用方提供足够的 history_extra_days，不在本测试覆盖范围内。
"""

from __future__ import annotations

import pandas as pd
import pytest

from qlab.data import DataLayer
from qlab.data.sources import FakeDataSource
from qlab.features import build_feature_matrix
from qlab.features.library import Momentum, RealizedVol


@pytest.fixture(scope="module")
def data_layer() -> DataLayer:
    src = FakeDataSource(seed=7, n_symbols=8, start_year=2022)
    return DataLayer(source=src)


@pytest.fixture(scope="module")
def universe(data_layer):
    return data_layer.universe("csi500", "2023-01-01", "2023-12-31")


WINDOW_FEATURES = [
    ("momentum_5", Momentum(5)),
    ("momentum_20", Momentum(20)),
    ("realized_vol_20", RealizedVol(20)),
]


@pytest.mark.parametrize("name,feature", WINDOW_FEATURES, ids=[n for n, _ in WINDOW_FEATURES])
def test_window_feature_batch_vs_incremental(data_layer, universe, name, feature):
    """窗口型特征：batch 全范围与 incremental 子范围应该在重叠区间逐值相等."""
    full_range = ("2023-01-01", "2023-12-31")
    incr_range = ("2023-07-01", "2023-12-31")

    batch_X = build_feature_matrix(
        features=[feature], data=data_layer, universe=universe,
        date_range=full_range, mode="batch", generate_mask=False,
    )
    incr_X = build_feature_matrix(
        features=[feature], data=data_layer, universe=universe,
        date_range=incr_range, mode="incremental", generate_mask=False,
    )

    fname = feature.meta.name
    batch_slice = batch_X.values[fname]
    incr_series = incr_X.values[fname]

    # 对齐到 incremental 的索引
    aligned = batch_slice.reindex(incr_series.index)

    # 同时 dropna 后比较——两边都可能在 universe 内有停牌 NaN
    both = pd.concat([aligned, incr_series], axis=1, keys=["batch", "incr"]).dropna()

    assert not both.empty, f"{name}: 对齐后样本为空，测试无效"
    pd.testing.assert_series_equal(
        both["batch"], both["incr"],
        check_names=False, rtol=1e-9, atol=1e-9,
    )


def test_build_matrix_incremental_only_returns_target_dates(data_layer, universe):
    """incremental 模式输出索引必须严格限定在 target_dates × universe 内."""
    incr_range = ("2023-09-01", "2023-09-30")
    X = build_feature_matrix(
        features=[Momentum(5)], data=data_layer, universe=universe,
        date_range=incr_range, mode="incremental", generate_mask=False,
    )
    dates = X.values.index.get_level_values("date").unique()
    assert dates.min() >= pd.Timestamp("2023-09-01")
    assert dates.max() <= pd.Timestamp("2023-09-30")
