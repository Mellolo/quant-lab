"""动态仓位 + 限价单 — 书 Ch10 §10.6."""

from __future__ import annotations


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
    """计算保本限价（从 current → target 的过程中不会实际亏损） — 书 Ch10 Snippet 10.4."""
    if target_position == current_position:
        return forecast_price

    sgn = 1 if target_position >= current_position else -1
    lp = 0.0
    count = 0
    start = abs(current_position + sgn)
    end = abs(target_position) + 1
    if start >= end:
        return forecast_price
    for j in range(start, end):
        lp += _inv_price_sigmoid(forecast_price, w, j / max_position)
        count += 1
    if count == 0:
        return forecast_price
    return lp / (target_position - current_position) * count
