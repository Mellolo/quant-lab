"""ParquetShardedBarStore / InMemoryShardedBarStore 测试 — §7.3 分片增量更新口径."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from qlab.data import (
    DataLayer,
    InMemoryShardedBarStore,
    ParquetShardedBarStore,
)
from qlab.data.sources import FakeDataSource


@pytest.fixture
def fake_bars() -> pd.DataFrame:
    src = FakeDataSource(seed=99, n_symbols=4, start_year=2022)
    syms = src.all_symbols
    return src.fetch_bars(syms, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-06-30"))


# ---- 共享行为：两种实现都要满足 ------------------------------------------


@pytest.mark.parametrize("store_factory", [
    pytest.param(lambda: InMemoryShardedBarStore(), id="memory"),
    pytest.param(lambda: ParquetShardedBarStore(tempfile.mkdtemp()), id="parquet"),
])
def test_put_get_roundtrip(store_factory, fake_bars):
    store = store_factory()
    syms = sorted(fake_bars.index.get_level_values("symbol").unique())
    store.put_range(
        fake_bars, kind="daily", source_version="v1",
        freq="1d", adjust="backward",
    )
    out = store.get_range(
        kind="daily", symbols=syms,
        start=pd.Timestamp("2023-01-01"), end=pd.Timestamp("2023-06-30"),
        source_version="v1", freq="1d", adjust="backward",
    )
    assert not out.empty
    # roundtrip 行数一致（允许排序不同）
    assert len(out) == len(fake_bars)


@pytest.mark.parametrize("store_factory", [
    pytest.param(lambda: InMemoryShardedBarStore(), id="memory"),
    pytest.param(lambda: ParquetShardedBarStore(tempfile.mkdtemp()), id="parquet"),
])
def test_missing_ranges(store_factory, fake_bars):
    store = store_factory()
    syms = sorted(fake_bars.index.get_level_values("symbol").unique())
    # 只写入 1-3 月
    sub = fake_bars.loc[
        (fake_bars.index.get_level_values("date") >= pd.Timestamp("2023-01-01")) &
        (fake_bars.index.get_level_values("date") <= pd.Timestamp("2023-03-31"))
    ]
    store.put_range(sub, kind="daily", source_version="v1",
                    freq="1d", adjust="backward")

    # 查询 1-6 月，缺 4-6 月 × 全部 symbol = 4*3 = 12 个分片
    missing = store.missing_ranges(
        kind="daily", symbols=syms,
        start=pd.Timestamp("2023-01-01"), end=pd.Timestamp("2023-06-30"),
        source_version="v1", freq="1d", adjust="backward",
    )
    expected = {(s, ym) for s in syms for ym in ("2023-04", "2023-05", "2023-06")}
    assert set(missing) == expected


@pytest.mark.parametrize("store_factory", [
    pytest.param(lambda: InMemoryShardedBarStore(), id="memory"),
    pytest.param(lambda: ParquetShardedBarStore(tempfile.mkdtemp()), id="parquet"),
])
def test_source_version_isolation(store_factory, fake_bars):
    """source_version 不同 → 视为完全不同的缓存."""
    store = store_factory()
    syms = sorted(fake_bars.index.get_level_values("symbol").unique())
    store.put_range(fake_bars, kind="daily", source_version="v1",
                    freq="1d", adjust="backward")
    # 用 v2 查同一时段——应当全部缺失
    missing = store.missing_ranges(
        kind="daily", symbols=syms,
        start=pd.Timestamp("2023-01-01"), end=pd.Timestamp("2023-06-30"),
        source_version="v2", freq="1d", adjust="backward",
    )
    assert len(missing) == len(syms) * 6  # 4 symbols × 6 months


# ---- DataLayer 集成：分片走通 + 二次取数命中缓存 -----------------------


def test_datalayer_uses_sharded_store():
    src = FakeDataSource(seed=11, n_symbols=3, start_year=2022)
    bar_store = InMemoryShardedBarStore()
    data = DataLayer(source=src, bar_store=bar_store)

    syms = src.all_symbols
    df1 = data.daily(syms, "2023-02-01", "2023-04-30", validate=False)
    df2 = data.daily(syms, "2023-03-01", "2023-03-31", validate=False)
    # df2 完全包含在 df1 拉取范围内，应当命中缓存（不再有 missing）
    missing_after = bar_store.missing_ranges(
        kind="daily", symbols=syms,
        start=pd.Timestamp("2023-03-01"), end=pd.Timestamp("2023-03-31"),
        source_version=src.source_version,
        freq="1d", adjust="backward",
    )
    assert missing_after == []
    assert not df1.empty and not df2.empty
    assert len(df2) < len(df1)


def test_parquet_sharded_layout_on_disk():
    """落盘后目录结构符合 §7.3 设计."""
    src = FakeDataSource(seed=7, n_symbols=2, start_year=2023)
    with tempfile.TemporaryDirectory() as tmp:
        bar_store = ParquetShardedBarStore(tmp)
        data = DataLayer(source=src, bar_store=bar_store)
        syms = src.all_symbols
        data.daily(syms, "2023-01-01", "2023-02-28", validate=False)

        # 期望路径：tmp/daily/fake-v1/adjust=backward/freq=1d/<sym>/<YYYY-MM>.parquet
        files = list(Path(tmp).rglob("*.parquet"))
        assert files, "no parquet files written"
        names = sorted({f.name for f in files})
        assert any(n.startswith("2023-01") for n in names)
        assert any(n.startswith("2023-02") for n in names)
        # 路径层级里出现 source_version 和 symbol 段
        sample = str(files[0])
        assert "fake-v1" in sample
        assert any(s.replace(".", "_") in sample or s in sample for s in syms)


# ======================================================================
# 58 缓存损坏恢复 —— "跑一半被 Ctrl-C" 的真实场景
# ======================================================================


def test_corrupted_shard_is_treated_as_missing(tmp_path):
    """损坏的 parquet 分片应被判为缺失并重取, 而非抛 ArrowInvalid.

    研究流程中断(Ctrl-C / OOM)会留下半写文件。早前 get_range 直接
    pd.read_parquet 会崩在 "Parquet magic bytes not found", 且报错
    不告诉用户该删哪个文件 —— 整个环境就卡住了。
    """
    import numpy as np
    import pandas as pd

    from qlab.data.store import ParquetShardedBarStore

    store = ParquetShardedBarStore(tmp_path)
    idx = pd.MultiIndex.from_product(
        [pd.bdate_range("2024-06-03", periods=5), ["600519.SH"]],
        names=["date", "symbol"],
    )
    df = pd.DataFrame({"close": np.arange(len(idx), dtype=float)}, index=idx)
    store.put_range(df, kind="daily", source_version="v1")

    shards = list(tmp_path.rglob("*.parquet"))
    assert shards, "应已写入分片"
    # 模拟半写: 写入非 parquet 内容
    shards[0].write_bytes(b"HALF_WRITTEN_GARBAGE")

    # 1) missing_ranges 必须把它算作缺失(否则不会重取)
    missing = store.missing_ranges(
        kind="daily", symbols=["600519.SH"],
        start=pd.Timestamp("2024-06-03"), end=pd.Timestamp("2024-06-07"),
        source_version="v1",
    )
    assert missing, "损坏分片应被判为缺失"

    # 2) get_range 不得抛异常(跳过坏分片)
    got = store.get_range(
        kind="daily", symbols=["600519.SH"],
        start=pd.Timestamp("2024-06-03"), end=pd.Timestamp("2024-06-07"),
        source_version="v1",
    )
    assert isinstance(got, pd.DataFrame)

    # 3) 重新写入应能覆盖损坏文件并恢复完整
    store.put_range(df, kind="daily", source_version="v1")
    healed = store.get_range(
        kind="daily", symbols=["600519.SH"],
        start=pd.Timestamp("2024-06-03"), end=pd.Timestamp("2024-06-07"),
        source_version="v1",
    )
    assert len(healed) == len(df), f"覆写后应恢复完整: {len(healed)} vs {len(df)}"
    assert np.allclose(
        healed["close"].sort_index().to_numpy(), df["close"].sort_index().to_numpy()
    )


def test_corrupted_kv_cache_treated_as_absent(tmp_path):
    """key-based 缓存损坏时 has() 应返回 False(触发重取覆写)."""
    import numpy as np
    import pandas as pd

    from qlab.data.store import ParquetBarStore

    store = ParquetBarStore(tmp_path)
    df = pd.DataFrame({"a": np.arange(3.0)})
    store.put("k1", df)
    assert store.has("k1")

    # 损坏后 has() 必须为 False —— 否则上层会 get() 然后崩
    (tmp_path / "k1.parquet").write_bytes(b"GARBAGE")
    assert not store.has("k1"), "损坏文件应视为不存在"

    # 覆写后恢复
    store.put("k1", df)
    assert store.has("k1")
    assert len(store.get("k1")) == 3


def test_read_only_cache_dir_raises_actionable_error(tmp_path):
    """缓存目录不可写时, 报错要点明"缓存写不进去"而不是裸的 PermissionError."""
    import os
    import stat

    import numpy as np

    root = tmp_path / "ro"
    root.mkdir()
    store = ParquetShardedBarStore(root)
    idx = pd.MultiIndex.from_product(
        [pd.date_range("2024-06-03", periods=3, freq="D"), ["600519.SH"]],
        names=["date", "symbol"],
    )
    df = pd.DataFrame({"close": np.arange(3.0)}, index=idx)
    os.chmod(root, stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(OSError, match="分片缓存写入失败"):
            store.put_range(df, kind="daily", source_version="v1")
    finally:
        os.chmod(root, 0o755)


def test_datalayer_rejects_swapped_store_args(tmp_path):
    """store= / bar_store= 传反了必须 fail-loud —— 静默退化会全量重拉远程."""
    from qlab.data.store import ParquetBarStore

    src = FakeDataSource(seed=1, n_symbols=2)
    with pytest.raises(TypeError, match="传反了"):
        DataLayer(source=src, store=ParquetShardedBarStore(tmp_path / "a"))
    with pytest.raises(TypeError, match="传反了"):
        DataLayer(source=src, bar_store=ParquetBarStore(tmp_path / "b"))
    # 正确用法不受影响
    DataLayer(
        source=src,
        store=ParquetBarStore(tmp_path / "kv"),
        bar_store=ParquetShardedBarStore(tmp_path / "bars"),
    )
