"""宇宙规格与板块/ST 过滤单元测试."""

from __future__ import annotations

import pandas as pd
import pytest

from qlab.data import DataLayer, UniverseSpec, apply_board_filters, is_bj_symbol, is_star_symbol
from qlab.data.sources import FakeDataSource
from qlab.data.universe import parse_broad_universe


def test_board_symbol_helpers():
    assert is_bj_symbol("430047.BJ")
    assert is_bj_symbol("920000.BJ")
    assert not is_bj_symbol("600519.SH")
    assert is_star_symbol("688981.SH")
    assert is_star_symbol("688981.XSHG")
    assert not is_star_symbol("600519.SH")
    assert not is_star_symbol("300750.SZ")

    kept = apply_board_filters(
        ["600519.SH", "688981.SH", "430047.BJ", "000001.SZ"],
        exclude_bj=True,
        exclude_star=True,
    )
    assert kept == ["600519.SH", "000001.SZ"]


def test_parse_broad_universe():
    assert parse_broad_universe("main_a") == (True, True)
    assert parse_broad_universe("hs_a") == (False, True)
    assert parse_broad_universe("csi500") is None


def test_fake_main_a_excludes_star_and_old_all_a_removed():
    src = FakeDataSource(seed=2, n_symbols=40, start_year=2022)
    # 构造列表里应含 688
    assert any(is_star_symbol(s) for s in src.all_symbols)

    layer = DataLayer(source=src)
    main = layer.universe(UniverseSpec.main_a(), "2023-01-03", "2023-01-31")
    hs = layer.universe(UniverseSpec.hs_a(), "2023-01-03", "2023-01-31")
    main_syms = set(main.all_symbols())
    hs_syms = set(hs.all_symbols())

    assert not any(is_star_symbol(s) for s in main_syms)
    assert any(is_star_symbol(s) for s in hs_syms)
    assert main_syms <= hs_syms

    # 任一日成员不含科创（main_a）
    d = pd.Timestamp("2023-01-10")
    assert not any(is_star_symbol(s) for s in main.members(d))

    with pytest.raises(ValueError, match="已移除"):
        layer.universe("all_a", "2023-01-03", "2023-01-10")


def test_index_universe_members_can_differ_by_day_on_fake():
    """指数池按日可查；Fake 内成分固定，但 API 仍是按日 members()."""
    layer = DataLayer(source=FakeDataSource(seed=1, n_symbols=60))
    uni = layer.universe("csi500", "2023-03-01", "2023-03-10")
    d0, d1 = uni.date_range()
    m0 = set(uni.members(d0))
    m1 = set(uni.members(d1))
    assert m0 == m1  # Fake 简化为固定成分
    assert 0 < len(m0) <= 50
