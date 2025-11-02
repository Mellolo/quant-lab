from queue import Queue
from backtest.strategy.strategy import AbstractStrategy

class ManualSignal:
    Open, Close, Continue = range(3)

    def __init__(self, signal_type, **kwargs):
        self.signal_type = signal_type
        self.args = {}
        for key, val in iter(kwargs.items()):
            self.args[key] = val

    def get_arg(self, key):
        return self.args.get(key, None)

class ManualStrategyInfo:
    def __init__(self, **kwargs):
        self.args = {}
        for key, val in iter(kwargs.items()):
            self.args[key] = val

    def get_arg(self, key):
        return self.args.get(key, None)

class ManualStrategy(AbstractStrategy):
    def __init__(self, signal_queue: Queue[ManualSignal], info_queue: Queue[ManualStrategyInfo]):
        super().__init__()
        self.signal_queue = signal_queue
        self.info_queue = info_queue

    def handle_data(self):
        # 行情信息
        data = self.datas[0]
        market_index = self.datas[1] if len(self.datas) > 1 else None
        industry_index = self.datas[2] if len(self.datas) > 2 else None

        # 持仓信息
        my_position_id = None
        order_refs = self.get_my_position_id_by_data(data, skip_closed=True)
        if len(order_refs) > 1:
            my_position_id = order_refs[0]

        # 输出策略信息
        info = ManualStrategyInfo(data=data, market_index=market_index, industry_index=industry_index, my_position=my_position_id)
        self.info_queue.put(info)

        # 接收外部信号
        signal = self.signal_queue.get()

        # 处理外部信号
        if signal.signal_type == ManualSignal.Open:
            if my_position_id is not None:
                raise ValueError("已有仓位，不允许重复开仓")

            # 根据输入信号的参数开仓
            price = signal.get_arg("price")
            target_price = signal.get_arg("target_price")
            stop_price = signal.get_arg("stop_price")
            is_buy = signal.get_arg("is_buy") if signal.get_arg("is_buy") is not None else True
            if price is None:
                self.open_market(data, is_buy=is_buy, target_price=target_price, stop_price=stop_price)
            else:
                self.open_limit(data, price, is_buy=is_buy, target_price=target_price, stop_price=stop_price)

        elif signal.signal_type == ManualSignal.Close:
            if my_position_id is None:
                raise ValueError("没有仓位，不允许执行平仓")

            # 平仓
            self.close_position(my_position_id)

        elif signal.signal_type == ManualSignal.Continue:
            pass


