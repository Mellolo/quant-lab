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
        # qlab/tests/conftest.py → parents[2] = quant-lab/ 仓根
        str(__import__("pathlib").Path(__file__).resolve().parents[2] / ".jqcache"),
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
def real_open_wide(real_daily):
    """后复权开盘价宽表 columns=symbols."""
    return real_daily["open"].unstack("symbol")


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
def real_labels(real_daily, real_close_wide, real_open_wide):
    """真实标签：CUSUM 确认日 → 次日开盘 + TB 1.5:1 / v10（SampleSpec 合约）."""
    from qlab.labeling import CUSUMFilter, ExitSettings, SampleSpec

    confirm = CUSUMFilter(h=0.04).sample_per_symbol(real_close_wide)
    confirm = confirm[
        (confirm["timestamp"] >= pd.Timestamp(REG_START))
        & (confirm["timestamp"] <= pd.Timestamp(REG_END))
    ]

    class _FixedConfirm:
        def sample_per_symbol(self, prices):
            return confirm.copy()

    exit_ = ExitSettings(pt=1.5, sl=1.0, vertical_days=10)
    labels = SampleSpec(entry=_FixedConfirm(), exit=exit_).run(
        real_close_wide,
        open=real_open_wide,
        target=0.04,
        label_prices=real_daily[["open", "close"]],
        drop_no_data=True,
    )
    labels["t1"] = pd.to_datetime(labels["touch_time"])
    return labels
