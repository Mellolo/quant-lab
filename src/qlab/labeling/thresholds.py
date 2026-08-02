"""动态阈值估计 — 书 Ch3 Snippet 3.1.

EWM 日波动率，用于 triple-barrier 的屏障宽度基准。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def daily_ewm_vol(close: pd.Series, span: int = 100) -> pd.Series:
    """每日 EWM 标准差（**单只标的**）.

    Args:
        close: indexed by date 的 Series。多标的 panel 请用
            :func:`daily_ewm_vol_panel`。
        span: EWM 窗口。

    Returns:
        相同索引的 Series。

    Raises:
        TypeError: 传入 DataFrame 时 —— 不拦的话 pandas 会在
            ``.rename("ewm_vol")`` 处报 ``'str' object is not callable``,
            完全看不出真实原因。
    """
    if isinstance(close, pd.DataFrame):
        raise TypeError(
            f"daily_ewm_vol 只接**单只标的**的 Series, 传入了 DataFrame"
            f"(shape={close.shape})。\n"
            "  多标的 panel(columns=symbols) 请用 daily_ewm_vol_panel()。"
        )
    log_ret = np.log(close).diff()
    return log_ret.ewm(span=span).std().rename("ewm_vol")


def daily_ewm_vol_panel(close: pd.DataFrame, span: int = 100) -> pd.DataFrame:
    """对 panel（columns=symbols）逐列计算 EWM vol.

    Raises:
        TypeError: 传入 Series 时 —— 单只标的请用 :func:`daily_ewm_vol`。
    """
    if isinstance(close, pd.Series):
        raise TypeError(
            "daily_ewm_vol_panel 只接 panel(columns=symbols) 的 DataFrame, "
            "传入了 Series。\n  单只标的请用 daily_ewm_vol()。"
        )
    log_ret = np.log(close).diff()
    return log_ret.ewm(span=span).std()
