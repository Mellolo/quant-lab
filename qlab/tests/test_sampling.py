"""采样合约单元测试：确认日 → 入场 + 经典三重屏障出场."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qlab.core.calendar import get_default_calendar
from qlab.core.enums import EntryAt, EntryTiming
from qlab.labeling import (
    EXIT_RESEARCH_DEFAULT,
    EXIT_TB_1_5_1_V20,
    CUSUMFilter,
    ExitSettings,
    NewHighBreakoutSampler,
    SampleSpec,
    VolumeCUSUMFilter,
    confirmation_to_entry,
    daily_event_pairs,
    to_event_dataframe,
)


class _GridEntry:
    def __init__(self, symbols, dates):
        self._pairs = daily_event_pairs(symbols, dates)

    def sample_per_symbol(self, prices):
        return self._pairs.copy()


def _synth_ohlc(n: int = 40, symbols: list[str] | None = None):
    symbols = symbols or ["AAA.SZ"]
    days = pd.bdate_range("2024-01-02", periods=n)
    close = pd.DataFrame({s: 100.0 + i for i, s in enumerate(symbols)}, index=days)
    open_ = close * 0.99
    return days, open_, close


def test_confirmation_to_entry_maps_each_day_to_next_session():
    cal = get_default_calendar()
    days = pd.bdate_range("2024-01-02", periods=5)
    pairs = daily_event_pairs(["A.SZ"], days[:3])
    out = confirmation_to_entry(pairs, cal)
    expect = [cal.next_trading_day(pd.Timestamp(d), 1) for d in days[:3]]
    assert list(pd.DatetimeIndex(out["timestamp"]).normalize()) == [
        pd.Timestamp(t).normalize() for t in expect
    ]


def test_sample_spec_default_is_next_open():
    days, open_, close = _synth_ohlc()
    spec = SampleSpec(entry=_GridEntry(["AAA.SZ"], days[:3]), exit=EXIT_TB_1_5_1_V20)
    assert spec._entry_at() == EntryAt.NEXT_OPEN
    confirm = spec.confirmation_pairs(close)
    entry = spec.sample_pairs(close)
    cal = get_default_calendar()
    for c, e in zip(confirm["timestamp"], entry["timestamp"]):
        assert pd.Timestamp(e).normalize() == cal.next_trading_day(
            pd.Timestamp(c).normalize(), 1
        )
    events = spec.build_events(entry, target=0.02)
    assert (events["entry_timing"] == EntryTiming.OPEN.value).all()


def test_sample_spec_confirm_close_keeps_confirm_day():
    days, _, close = _synth_ohlc()
    confirm_days = days[:2]
    spec = SampleSpec(
        entry=_GridEntry(["AAA.SZ"], confirm_days),
        exit=EXIT_TB_1_5_1_V20,
        entry_at=EntryAt.CONFIRM_CLOSE,
    )
    pairs = spec.sample_pairs(close)
    assert set(pd.DatetimeIndex(pairs["timestamp"]).normalize()) == {
        confirm_days[0].normalize(),
        confirm_days[1].normalize(),
    }
    events = spec.build_events(pairs, target=0.02)
    assert (events["entry_timing"] == EntryTiming.CLOSE.value).all()


def test_grid_is_daily_confirmation_then_next_open():
    """网格：每天确认 → 次日开盘入场."""
    days, open_, close = _synth_ohlc(n=15, symbols=["X.SH", "Y.SZ"])
    spec = SampleSpec(
        entry=_GridEntry(["X.SH", "Y.SZ"], days[:3]),
        exit=EXIT_RESEARCH_DEFAULT,
    )
    labs = spec.run(close, open=open_, target=0.02, drop_no_data=False)
    # 3 confirm × 2 sym → 3 entry days × 2
    assert len(labs) == 6
    cal = get_default_calendar()
    entry_days = {
        cal.next_trading_day(pd.Timestamp(d).normalize(), 1) for d in days[:3]
    }
    assert set(pd.DatetimeIndex(labs.index).normalize()) == entry_days
    assert (labs["entry_timing"] == "open").all()


def test_new_high_emits_confirm_day_only():
    n = 40
    idx = pd.bdate_range("2024-01-02", periods=n)
    px = np.full(n, 100.0)
    px[19] = 110.0
    px[20:] = 110.0 + np.arange(n - 20)
    s = pd.Series(px, index=idx)
    confirm = NewHighBreakoutSampler(window=20, cooldown_days=30).sample(s)
    assert confirm[0] == idx[19]

    wide = s.to_frame("S.SZ")
    spec = SampleSpec(
        entry=NewHighBreakoutSampler(window=20, cooldown_days=30),
        exit=EXIT_RESEARCH_DEFAULT,
    )
    entry = spec.sample_pairs(wide)
    cal = get_default_calendar()
    assert pd.Timestamp(entry["timestamp"].iloc[0]).normalize() == cal.next_trading_day(
        idx[19].normalize(), 1
    )


def test_run_next_open_requires_open_panel():
    days, _, close = _synth_ohlc()
    spec = SampleSpec(entry=_GridEntry(["AAA.SZ"], days[:2]))
    with pytest.raises(ValueError, match="open"):
        spec.run(close, target=0.02)


def test_run_confirm_close_accepts_close_only():
    days, _, close = _synth_ohlc()
    # 制造后续上涨，便于触上屏障
    close.iloc[1:] = 105.0
    spec = SampleSpec(
        entry=_GridEntry(["AAA.SZ"], days[:1]),
        exit=EXIT_TB_1_5_1_V20,
        entry_at=EntryAt.CONFIRM_CLOSE,
    )
    labs = spec.run(close, target=0.01, drop_no_data=False)
    assert len(labs) == 1
    assert labs["entry_timing"].iloc[0] == "close"


def test_exit_settings_drives_t1_and_barrier():
    days = pd.bdate_range("2024-01-02", periods=5)
    pairs = daily_event_pairs(["A.SZ"], days[:1])
    # 入场日 pairs（已映射）
    entry = confirmation_to_entry(pairs)
    exit_ = ExitSettings(pt=3.0, sl=1.0, vertical_days=20)
    ev = to_event_dataframe(entry, target=0.02, exit=exit_)
    cal = get_default_calendar()
    t0 = pd.Timestamp(entry["timestamp"].iloc[0]).normalize()
    assert ev["t1"].iloc[0] == cal.next_trading_day(t0, 20)
    assert exit_.barrier().pt == 3.0
    assert exit_.barrier().sl == 1.0
    assert EXIT_RESEARCH_DEFAULT.name() == "TB_3_1_v20"


def test_volume_cusum_requires_volume_kwarg():
    days, _, close = _synth_ohlc()
    vol = close * 0 + 1e6
    spec = SampleSpec(entry=VolumeCUSUMFilter(h=0.5))
    with pytest.raises(ValueError, match="volume"):
        spec.sample_pairs(close)
    pairs = spec.sample_pairs(close, volume=vol)
    assert list(pairs.columns) == ["timestamp", "symbol"]


def test_cusum_through_sample_spec_pipeline():
    days = pd.bdate_range("2024-01-02", periods=60)
    rng = np.random.default_rng(1)
    close = pd.DataFrame(
        {"A.SZ": 100 + np.cumsum(rng.normal(0, 1.5, len(days)))},
        index=days,
    )
    open_ = close * 0.995
    spec = SampleSpec(entry=CUSUMFilter(h=0.03), exit=EXIT_RESEARCH_DEFAULT)
    confirm = spec.confirmation_pairs(close)
    entry = spec.sample_pairs(close)
    if len(confirm) == 0:
        pytest.skip("合成路径未触发 CUSUM")
    assert len(entry) == len(confirm)
    labs = spec.run(close, open=open_, target=0.02, drop_no_data=False)
    assert len(labs) > 0
    assert labs["event_id"].is_unique
    assert set(labs["touch_type"]).issubset({"upper", "lower", "vertical", "no_data"})
