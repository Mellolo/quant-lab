"""特征库 — 按因子族组织.

每个文件按主题分（动量、波动率、量价、基本面、微观结构、日内派生）。
所有特征通过装饰器自动注册到全局 registry。

按需 import 触发注册：

    from qlab.features.library import momentum, volatility
    # 之后 registry 中就有这些特征了
"""

# 触发各模块的注册副作用
from qlab.features.library import (  # noqa: F401
    auction,
    fundamental,
    intraday,
    momentum,
    price,
    trend,
    volatility,
    volume,
)
from qlab.features.library.auction import AuctionPremium, AuctionVolumeRatio
from qlab.features.library.fundamental import PB, PE_TTM
from qlab.features.library.intraday import IntradayVolSlope, MorningReturn
from qlab.features.library.momentum import Momentum, MomentumResidual, SmoothMomentum
from qlab.features.library.price import FractionalDiff, LogPrice, PriceMA
from qlab.features.library.trend import DistToHigh, IsStage2, StageLabel
from qlab.features.library.volatility import EwmVol, RealizedVol
from qlab.features.library.volume import TurnoverRatio, VolumeRatio

__all__ = [
    # momentum
    "Momentum", "MomentumResidual", "SmoothMomentum",
    # volatility
    "EwmVol", "RealizedVol",
    # volume
    "TurnoverRatio", "VolumeRatio",
    # price
    "LogPrice", "PriceMA", "FractionalDiff",
    # fundamental
    "PE_TTM", "PB",
    # trend / stage
    "StageLabel", "IsStage2", "DistToHigh",
    # intraday
    "IntradayVolSlope", "MorningReturn",
    # auction (today_open)
    "AuctionPremium", "AuctionVolumeRatio",
]
