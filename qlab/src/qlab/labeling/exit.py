"""采样出场设定 — 第一版仅经典三重屏障.

已实现
------
``ExitSettings(pt, sl, vertical_days)`` → :class:`~qlab.labeling.triple_barrier.TripleBarrier`
+ 定日 ``t1``。止盈/止损宽度 = 倍数 × ``event.target``（通常为波动）。

扩展点（未实现，勿在本模块加逻辑）
----------------------------------
后续若增加出场变体，建议仍挂在同一合约层（与 :class:`~qlab.labeling.sample_spec.SampleSpec`
配对），例如独立 ``ExitKind`` / 策略对象，由 ``label_events`` 或其后继分发：

- 移动止盈（activate + trail 宽度）
- 分批止盈后再 trail
- 前高 / 结构位止盈
- 均线跌破出场

研究脚本里的非 TB 模拟不属于本库 API。
"""

from __future__ import annotations

from dataclasses import dataclass

from qlab.labeling.triple_barrier import TripleBarrier


@dataclass(frozen=True)
class ExitSettings:
    """出场：止盈 / 止损（× event.target）+ 定日垂直屏障.

    Parameters
    ----------
    pt :
        止盈倍数；``0`` = 无上屏障。
    sl :
        止损倍数；``0`` = 无下屏障。
    vertical_days :
        自入场日起若干**交易日**后的垂直屏障。
    """

    pt: float = 3.0
    sl: float = 1.0
    vertical_days: int = 20

    def __post_init__(self) -> None:
        if self.pt < 0:
            raise ValueError("pt 必须 >= 0")
        if self.sl < 0:
            raise ValueError("sl 必须 >= 0")
        if self.vertical_days < 1:
            raise ValueError("vertical_days 必须 >= 1")

    def barrier(self) -> TripleBarrier:
        """水平屏障倍数（垂直屏障见 ``vertical_days`` → event.t1）."""
        return TripleBarrier(pt=self.pt, sl=self.sl)

    def name(self) -> str:
        """短名，例如 ``TB_3_1_v20``."""
        return f"TB_{_fmt_mult(self.pt)}_{_fmt_mult(self.sl)}_v{self.vertical_days}"


def _fmt_mult(x: float) -> str:
    if float(x) == int(x):
        return str(int(x))
    return f"{x:g}".replace(".", "p")


EXIT_RESEARCH_DEFAULT = ExitSettings(pt=3.0, sl=1.0, vertical_days=20)
"""研究默认：TB 3:1，垂直 20 交易日."""

EXIT_TB_1_5_1_V20 = ExitSettings(pt=1.5, sl=1.0, vertical_days=20)
"""较紧止盈预设（测试 / 对照）."""
