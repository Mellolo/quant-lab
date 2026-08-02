"""DD / TuW — 书 Ch14 §14.5.2 Snippet 14.4."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_dd_tuw(
    series: pd.Series,
    dollars: bool = False,
) -> tuple[pd.Series, pd.Series]:
    """计算 Drawdown 与 Time under Water.

    参数
    ----
    series : NAV 或累计 PnL 序列（按时间索引）
    dollars : True → 输出绝对 DD；False → 相对 DD

    返回
    ----
    (dd, tuw)
        dd : Series indexed by HWM 时间，值为该 HWM 到下一个低谷的回撤
        tuw : Series indexed by HWM 时间，值为该 HWM 到再次突破前的年数
    """
    df0 = series.to_frame("pnl")
    df0["hwm"] = series.expanding().max()
    df1 = df0.groupby("hwm").min().reset_index()
    df1.columns = ["hwm", "min"]
    # 找每个 hwm 的首次出现时间
    df1.index = df0["hwm"].drop_duplicates(keep="first").index
    df1 = df1[df1["hwm"] > df1["min"]]

    dd = df1["hwm"] - df1["min"] if dollars else 1 - df1["min"] / df1["hwm"]

    if len(df1.index) >= 2:
        # 用 365.25 天表示 1 年（新版 pandas 拒绝 timedelta64[Y] 的歧义单位）
        diffs = (df1.index[1:] - df1.index[:-1])
        tuw_vals = diffs / np.timedelta64(int(365.25 * 24 * 3600), "s")
        tuw = pd.Series(tuw_vals, index=df1.index[:-1])
    else:
        tuw = pd.Series(dtype=float)

    return dd, tuw
