"""动态仓位 + 限价单 — 书 Ch10 §10.6."""

from __future__ import annotations

import math


def _bet_size_sigmoid(w: float, x: float) -> float:
    """Sigmoid bet size: m = x / sqrt(w + x^2)."""
    return x / (w + x ** 2) ** 0.5


def _inv_price_sigmoid(f: float, w: float, m: float) -> float:
    """sigmoid 的反函数（对 m → price）."""
    return f - m * (w / (1 - m ** 2)) ** 0.5


def calibrate_sigmoid_w(x: float, m: float) -> float:
    """已知 (x, m) → 反推 w. 用于校准."""
    return x ** 2 * (m ** -2 - 1)


def dynamic_position(
    current_position: int,
    max_position: int,
    forecast_price: float,
    market_price: float,
    w: float,
) -> int:
    """根据市价偏离目标价计算动态目标仓位.

    参数
    ----
    current_position : 当前持仓
    max_position : 最大绝对仓位
    forecast_price : 预测目标价
    market_price : 当前市价
    w : sigmoid 宽度系数（用 calibrate_sigmoid_w 校准）

    返回
    ----
    目标仓位（整数）
    """
    x = forecast_price - market_price
    m = _bet_size_sigmoid(w, x)
    return int(m * max_position)


def limit_price(
    target_position: int,
    current_position: int,
    forecast_price: float,
    w: float,
    max_position: int,
) -> float:
    """计算保本限价（从 current → target 的过程中不会实际亏损） — 书 Ch10 Snippet 10.4.

    返回各中间仓位反函数价格的**算术平均**:
    ``sum(inv_price(j)) / |target - current|``。
    目标与当前相同时返回 NaN（避免除零）。
    """
    if target_position == current_position:
        return math.nan

    delta = target_position - current_position
    sgn = 1 if delta > 0 else -1
    lp = 0.0
    # 书 Snippet 10.4: range(abs(pos+sgn), abs(targetPos)+1)
    for j in range(abs(current_position + sgn), abs(target_position) + 1):
        m = j / float(max_position)
        if abs(m) >= 1.0:
            # |m|=1 时反函数奇异; 跳过边界点
            continue
        lp += _inv_price_sigmoid(forecast_price, w, m)
    return lp / abs(delta)
