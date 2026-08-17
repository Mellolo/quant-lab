"""复盘阈值。改档位只改这里，不要在 market/style 里散落魔法数。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReviewSpec:
    """①市场 + ②风格的可复现口径。"""

    universe: str = "hs_a"

    volume_window: int = 20
    volume_dry: float = 0.70
    volume_hot: float = 1.30
    volume_climax: float = 2.00
    climax_lookback: int = 5
    climax_fade: float = 0.80

    beta_window: int = 60
    vol_window: int = 20
    mom_window: int = 20
    short_mom_window: int = 5
    long_mom_window: int = 60
    strength_window: int = 5
    quintile: float = 0.20
    min_names: int = 5
    min_beta_obs: int = 20

    score_soft: float = 0.003
    score_hard: float = 0.008
    # 比率类（晋级超额、二板占比）对照自身近窗，用 sigma 不是写死家数
    z_soft: float = 0.75
    z_hard: float = 1.50
    short_lookback: int = 20
    board_min: int = 2

    stall_ratio: float = 1.50
    stall_abs_ret: float = 0.003
    panic_ld_min: int = 5
    panic_ld_frac: float = 0.004
    mania_lu_min: int = 10
    mania_lu_frac: float = 0.015
    hollow_gap: float = 0.008
    ice_advance: float = 0.30
    ice_ret: float = -0.01
    mania_advance: float = 0.70
    optimistic_advance: float = 0.60
    confirm_advance: float = 0.52
    repair_advance: float = 0.45

    growth_codes: tuple[str, ...] = ("801080", "801150", "801880")
    value_codes: tuple[str, ...] = ("801780", "801790")

    crowding_top: float = 0.10
    crowding_top1: float = 0.01
    crowding_delta_gather: float = 0.015
    crowding_delta_fade: float = -0.015
    clock_hot_turnover: float = 1.30
    clock_calm_turnover: float = 1.10
    clock_confirm_turnover: float = 1.00
    fund_lookback_days: int = 400
    use_fundamentals: bool = True
    use_margin: bool = True
    use_industry: bool = True
    active_turnover: float = 0.01
    # 指南针 0AMV 代理：个股活筹比例 = 1-exp(-换手/τ)，不写死 4 亿 / 1% 一刀切
    active_tau: float = 0.02
    active_frac_min: float = 0.50

    @property
    def warmup(self) -> int:
        """run() 往前多取的交易日数（含 β / 长动量）。"""
        return max(self.beta_window, self.long_mom_window, self.volume_window) + 1
