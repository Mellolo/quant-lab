"""Shared test fixtures.

单元测试不需要外部依赖；标记 ``@pytest.mark.realdata`` 的回归测试
使用本文件的会话级 fixture 从真实聚宽数据源取数（只取一次，各模块复用）。
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

# 真实数据回归的固定参数 —— 改动会使所有 realdata 基线失效, 需同步更新预期值
REG_SYMBOLS = [
    "600519.SH", "000858.SZ", "601318.SH", "000333.SZ", "600036.SH",
    "300750.SZ", "002594.SZ", "601899.SH", "000001.SZ", "600276.SH",
]
REG_START = "2023-01-03"
REG_END = "2024-11-29"
REG_WARMUP = "2022-09-01"   # 特征回看窗口需要的额外历史


@pytest.fixture(scope="session")
def real_layer():
    """真实聚宽数据源的 DataLayer(会话级, 只建一次)."""
    os.environ.setdefault(
        "JQ_CACHE_DIR",
        str(__import__("pathlib").Path(__file__).resolve().parents[1] / ".jqcache"),
    )
    pytest.importorskip("jq", reason="需要 jq 连接器")
    from qlab.data.layer import DataLayer
    from qlab.data.store import InMemoryBarStore, InMemoryShardedBarStore

    try:
        from qlab.data.sources import JQDataSource

        src = JQDataSource()
    except Exception as exc:  # 凭证失效/未安装
        pytest.skip(f"聚宽数据源不可用: {exc}")
    return DataLayer(
        source=src,
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )


@pytest.fixture(scope="session")
def real_daily(real_layer):
    """真实日线(含 warmup 段), MultiIndex(date, symbol)."""
    return real_layer.daily(REG_SYMBOLS, REG_WARMUP, REG_END)


@pytest.fixture(scope="session")
def real_close_wide(real_daily):
    """后复权收盘价宽表 columns=symbols."""
    return real_daily["close"].unstack("symbol")


@pytest.fixture(scope="session")
def real_universe():
    """覆盖 REG_SYMBOLS × 目标区间的 Universe."""
    from qlab.core.calendar import get_default_calendar
    from qlab.data.universe import Universe, UniverseSpec

    cal = get_default_calendar()
    dates = cal.trading_days(pd.Timestamp(REG_START), pd.Timestamp(REG_END))
    idx = pd.MultiIndex.from_product(
        [dates, REG_SYMBOLS], names=["date", "symbol"]
    )
    panel = pd.DataFrame(
        {"in_universe": True, "weight": 1.0 / len(REG_SYMBOLS)}, index=idx
    )
    return Universe(panel, UniverseSpec("regression"))


@pytest.fixture(scope="session")
def real_labels(real_daily, real_close_wide):
    """真实标签集(CUSUM 采样 + 三重障碍), index=event_start, 含 symbol/t1/bin/ret."""
    from qlab.core.calendar import get_default_calendar
    from qlab.labeling import CUSUMFilter, TripleBarrier, to_event_dataframe
    from qlab.labeling.triple_barrier import label_events

    cal = get_default_calendar()
    pairs = CUSUMFilter(h=0.04).sample_per_symbol(real_close_wide)
    pairs = pairs[
        (pairs["timestamp"] >= pd.Timestamp(REG_START))
        & (pairs["timestamp"] <= pd.Timestamp(REG_END))
    ]
    events = to_event_dataframe(pairs, target=0.04, t1_days=10, calendar=cal)
    labels = label_events(
        events, real_daily[["close"]], TripleBarrier(pt=1.5, sl=1.0)
    )
    labels["t1"] = pd.to_datetime(labels["touch_time"])
    return labels
