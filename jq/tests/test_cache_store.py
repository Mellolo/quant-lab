"""``jq.cache_store`` 单元测试 — 纯本地, 不触网.

覆盖缓存正确性的关键路径: 日期归一化、月份计算、连续月合并、
长 key 收敛、原子写、月分片读写、缺失月判定、空月标记.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from jq.cache_store import (
    CACHE_VERSION,
    MonthlyShardStore,
    SnapshotStore,
    atomic_write,
    current_month,
    is_today_or_future,
    merge_consecutive_months,
    month_bounds,
    month_of,
    months_between,
    norm_date,
    read_pkl,
    safe_key,
)

# ======================================================================
# 日期归一化 (④)
# ======================================================================


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-07-01", "2026-07-01"),
        ("2026-7-1", "2026-07-01"),          # 非零填充
        ("2026/07/01", "2026-07-01"),
        (dt.date(2026, 7, 1), "2026-07-01"),
        (dt.datetime(2026, 7, 1, 15, 30), "2026-07-01"),
        (pd.Timestamp("2026-07-01"), "2026-07-01"),
        (pd.Timestamp("2026-07-01 09:30:00"), "2026-07-01"),
    ],
)
def test_norm_date_accepts_common_forms(value, expected):
    assert norm_date(value) == expected


@pytest.mark.parametrize("bad", [None, "", "not-a-date", "2026-13-45"])
def test_norm_date_rejects_garbage(bad):
    with pytest.raises(ValueError, match="日期"):
        norm_date(bad)


def test_norm_date_makes_string_compare_safe():
    """归一化后字符串比较等价于日期比较(旧实现的 bug 根源)."""
    assert norm_date("2026-7-9") < norm_date("2026-07-10")
    assert norm_date(pd.Timestamp("2026-07-01")) == norm_date("2026-07-01")


def test_is_today_or_future():
    today = dt.date.today()
    assert is_today_or_future(today)
    assert is_today_or_future(today + dt.timedelta(days=30))
    assert not is_today_or_future(today - dt.timedelta(days=1))
    assert not is_today_or_future("garbage")


# ======================================================================
# 月份计算 (⑥)
# ======================================================================


def test_month_of_and_bounds():
    assert month_of("2024-02-15") == "2024-02"
    assert month_bounds("2024-02") == ("2024-02-01", "2024-02-29")  # 闰年
    assert month_bounds("2023-02") == ("2023-02-01", "2023-02-28")
    assert month_bounds("2024-12") == ("2024-12-01", "2024-12-31")


def test_months_between():
    assert months_between("2024-01-15", "2024-04-02") == [
        "2024-01", "2024-02", "2024-03", "2024-04",
    ]
    assert months_between("2024-03-01", "2024-03-31") == ["2024-03"]
    assert months_between("2024-03-05", "2024-01-01") == []  # 逆序 → 空


def test_months_between_crosses_year():
    assert months_between("2023-11-20", "2024-02-03") == [
        "2023-11", "2023-12", "2024-01", "2024-02",
    ]


def test_merge_consecutive_months():
    assert merge_consecutive_months([]) == []
    assert merge_consecutive_months(["2024-01"]) == [("2024-01-01", "2024-01-31")]
    # 连续 → 合并成一段
    assert merge_consecutive_months(["2024-01", "2024-02", "2024-03"]) == [
        ("2024-01-01", "2024-03-31")
    ]
    # 有断档 → 拆成多段(⑥: 不再跨越空档重取)
    assert merge_consecutive_months(["2024-01", "2024-02", "2024-05"]) == [
        ("2024-01-01", "2024-02-29"),
        ("2024-05-01", "2024-05-31"),
    ]
    # 乱序 + 重复 → 归一
    assert merge_consecutive_months(["2024-05", "2024-01", "2024-05"]) == [
        ("2024-01-01", "2024-01-31"),
        ("2024-05-01", "2024-05-31"),
    ]


def test_merge_consecutive_months_crosses_year():
    assert merge_consecutive_months(["2023-12", "2024-01"]) == [
        ("2023-12-01", "2024-01-31")
    ]


# ======================================================================
# 长 key 收敛 (③)
# ======================================================================


def test_safe_key_short_passthrough():
    assert safe_key("600519.XSHG") == "600519_XSHG"
    assert safe_key(["a", "b"]) == "a_b"
    assert safe_key("") == "empty"


def test_safe_key_long_list_is_hashed():
    """5000 只股票的列表不得撑爆文件名(旧实现会 OSError)."""
    codes = [f"{i:06d}.XSHE" for i in range(5000)]
    key = safe_key(codes)
    assert len(key) <= 80
    # 不同输入 → 不同 key
    assert key != safe_key(codes[:-1])
    # 同一输入 → 稳定 key
    assert key == safe_key(list(codes))


def test_safe_key_is_filename_safe():
    key = safe_key("a/b\\c:d*e?f.g")
    assert not set(key) & set("/\\:*?.")


# ======================================================================
# 原子写 / 容错读 (⑤⑪)
# ======================================================================


def test_atomic_write_and_read(tmp_path: Path):
    p = tmp_path / "x.pkl"
    df = pd.DataFrame({"a": [1, 2]})
    atomic_write(p, df)
    assert p.exists()
    pd.testing.assert_frame_equal(read_pkl(p), df)


def test_atomic_write_leaves_no_tmp(tmp_path: Path):
    p = tmp_path / "x.pkl"
    atomic_write(p, pd.DataFrame({"a": [1]}))
    assert list(tmp_path.glob("*.tmp*")) == []


def test_atomic_write_overwrite_is_not_torn(tmp_path: Path):
    p = tmp_path / "x.pkl"
    atomic_write(p, pd.DataFrame({"a": [1]}))
    atomic_write(p, pd.DataFrame({"a": [1, 2, 3]}))
    assert len(read_pkl(p)) == 3


def test_read_pkl_missing_returns_none(tmp_path: Path):
    assert read_pkl(tmp_path / "nope.pkl") is None


def test_read_pkl_corrupt_warns_and_returns_none(tmp_path: Path):
    p = tmp_path / "bad.pkl"
    p.write_bytes(b"\x80\x04not-a-valid-pickle")
    with pytest.warns(RuntimeWarning, match="缓存文件损坏"):
        assert read_pkl(p) is None


def test_atomic_write_creates_parent_dirs(tmp_path: Path):
    p = tmp_path / "deep" / "nested" / "x.pkl"
    atomic_write(p, {"k": 1})
    assert p.exists()
    assert read_pkl(p) == {"k": 1}


# ======================================================================
# 月分片存储 (⑤⑥⑧⑩⑫)
# ======================================================================


@pytest.fixture
def store(tmp_path: Path) -> MonthlyShardStore:
    return MonthlyShardStore(tmp_path)


def _bars(start: str, end: str) -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="D")
    return pd.DataFrame({"close": range(len(idx))}, index=idx)


def test_shard_path_contains_version(store: MonthlyShardStore):
    p = store.shard_path("get_price", "600519.XSHG", "2024-01", "daily__post")
    assert CACHE_VERSION in p.parts
    assert p.name == "2024-01.pkl"


def test_write_then_read_roundtrip(store: MonthlyShardStore):
    df = _bars("2024-01-01", "2024-02-29")
    store.write_range(df, "get_price", "600519.XSHG", "2024-01-01", "2024-02-29")
    got = store.read_range("get_price", "600519.XSHG", "2024-01-01", "2024-02-29")
    assert len(got) == len(df)
    # 分片按月切开
    assert store.shard_path("get_price", "600519.XSHG", "2024-01").exists()
    assert store.shard_path("get_price", "600519.XSHG", "2024-02").exists()


def test_read_range_slices_precisely(store: MonthlyShardStore):
    store.write_range(
        _bars("2024-01-01", "2024-03-31"),
        "get_price", "X", "2024-01-01", "2024-03-31",
    )
    got = store.read_range("get_price", "X", "2024-02-10", "2024-02-20")
    assert got.index.min() == pd.Timestamp("2024-02-10")
    assert got.index.max() == pd.Timestamp("2024-02-20")


def test_missing_months_before_any_write(store: MonthlyShardStore):
    assert store.missing_months("get_price", "X", "2024-01-01", "2024-03-31") == [
        "2024-01", "2024-02", "2024-03",
    ]


def test_missing_months_after_partial_write(store: MonthlyShardStore):
    store.write_range(
        _bars("2024-02-01", "2024-02-29"),
        "get_price", "X", "2024-02-01", "2024-02-29",
    )
    assert store.missing_months("get_price", "X", "2024-01-01", "2024-03-31") == [
        "2024-01", "2024-03",
    ]


def test_empty_month_is_marked_as_fetched(store: MonthlyShardStore):
    """⑧ 无数据的月份也要落盘, 否则上市前区间会被反复远程查询."""
    store.write_range(
        pd.DataFrame(), "get_price", "X", "1999-01-01", "1999-03-31"
    )
    assert store.missing_months("get_price", "X", "1999-01-01", "1999-03-31") == []
    assert len(store.read_range("get_price", "X", "1999-01-01", "1999-03-31")) == 0


def test_current_month_never_cached(store: MonthlyShardStore):
    """⑫ 当月数据不完整, 既不写盘也永远视为缺失."""
    cur = current_month()
    lo, hi = month_bounds(cur)
    store.write_range(_bars(lo, hi), "get_price", "X", lo, hi)
    assert not store.shard_path("get_price", "X", cur).exists()
    assert store.missing_months("get_price", "X", lo, hi) == [cur]


def test_future_month_never_cached(store: MonthlyShardStore):
    future = str(pd.Period(current_month(), freq="M") + 6)
    lo, hi = month_bounds(future)
    store.write_range(_bars(lo, hi), "get_price", "X", lo, hi)
    assert not store.shard_path("get_price", "X", future).exists()


def test_variant_isolation(store: MonthlyShardStore):
    """不同 variant(频率/复权口径) 互不干扰."""
    store.write_range(
        _bars("2024-01-01", "2024-01-31"),
        "get_price", "X", "2024-01-01", "2024-01-31", "daily__post",
    )
    assert store.missing_months(
        "get_price", "X", "2024-01-01", "2024-01-31", "minute__post"
    ) == ["2024-01"]


def test_symbol_isolation(store: MonthlyShardStore):
    store.write_range(
        _bars("2024-01-01", "2024-01-31"), "get_price", "A", "2024-01-01", "2024-01-31"
    )
    assert store.missing_months(
        "get_price", "B", "2024-01-01", "2024-01-31"
    ) == ["2024-01"]


def test_read_range_dedupes_overlapping_shards(store: MonthlyShardStore):
    """同一日期重复写入时保留最后一条(不产生重复行)."""
    store.write_range(
        _bars("2024-01-01", "2024-01-31"), "get_price", "X", "2024-01-01", "2024-01-31"
    )
    store.write_range(
        _bars("2024-01-01", "2024-01-31"), "get_price", "X", "2024-01-01", "2024-01-31"
    )
    got = store.read_range("get_price", "X", "2024-01-01", "2024-01-31")
    assert not got.index.duplicated().any()


def test_read_range_tolerates_missing_middle(store: MonthlyShardStore):
    """只读已有月, 缺失月不报错(由上层决定是否补查)."""
    store.write_range(
        _bars("2024-01-01", "2024-01-31"), "get_price", "X", "2024-01-01", "2024-01-31"
    )
    store.write_range(
        _bars("2024-03-01", "2024-03-31"), "get_price", "X", "2024-03-01", "2024-03-31"
    )
    got = store.read_range("get_price", "X", "2024-01-01", "2024-03-31")
    assert len(got) > 0
    assert store.missing_months("get_price", "X", "2024-01-01", "2024-03-31") == [
        "2024-02"
    ]


def test_long_symbol_list_as_key(store: MonthlyShardStore):
    """③ get_extras 类函数会把整个标的列表当 key."""
    codes = [f"{i:06d}.XSHE" for i in range(3000)]
    store.write_range(
        _bars("2024-01-01", "2024-01-31"),
        "get_extras", codes, "2024-01-01", "2024-01-31", "is_st",
    )
    got = store.read_range(
        "get_extras", codes, "2024-01-01", "2024-01-31", "is_st"
    )
    assert len(got) == 31


# ======================================================================
# 快照存储
# ======================================================================


def test_snapshot_roundtrip(tmp_path: Path):
    s = SnapshotStore(tmp_path)
    assert s.get("get_concepts", "all") is None
    s.put("get_concepts", "all", {"x": 1})
    assert s.get("get_concepts", "all") == {"x": 1}


def test_snapshot_key_parts_and_version(tmp_path: Path):
    s = SnapshotStore(tmp_path)
    p = s.path("get_industry", ("600519.XSHG", "2024-01-01"))
    assert CACHE_VERSION in p.parts
    s.put("get_industry", ("600519.XSHG", "2024-01-01"), [1, 2])
    assert s.get("get_industry", ("600519.XSHG", "2024-01-01")) == [1, 2]
    # 不同 key → 不串味
    assert s.get("get_industry", ("600519.XSHG", "2024-01-02")) is None


def test_snapshot_long_key_is_hashed(tmp_path: Path):
    s = SnapshotStore(tmp_path)
    codes = [f"{i:06d}.XSHE" for i in range(5000)]
    s.put("get_factor_values", (codes, "size"), {"size": 1})
    assert s.get("get_factor_values", (codes, "size")) == {"size": 1}
    assert all(len(part) <= 100 for part in s.path("get_factor_values", (codes, "size")).parts)


def test_snapshot_survives_process_boundary(tmp_path: Path):
    """写入的快照能被另一个 Store 实例读到(纯磁盘, 无内存状态)."""
    SnapshotStore(tmp_path).put("get_concepts", "all", [1, 2, 3])
    assert SnapshotStore(tmp_path).get("get_concepts", "all") == [1, 2, 3]
