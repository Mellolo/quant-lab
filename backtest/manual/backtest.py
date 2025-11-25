import datetime
import json
from queue import Queue
import backtrader as bt
import pandas as pd
from backtrader.feeds import PandasData
import threading
from typing import Dict

from backtest.broker.aStockBroker import AStockBroker
from backtest.feeds.clean import check_index_consistency
from backtest.strategy.strategy import AbstractStrategy, SinglePosition

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

class ManualStrategyPosition:
    def __init__(self, pos: SinglePosition, **kwargs):
        self.args = {}
        for key, val in iter(kwargs.items()):
            self.args[key] = val

        # 开仓信息
        open_order = pos.get_open_order()
        self.args["is_buy"] = open_order.isbuy()
        self.args["open_created_price"] = open_order.created.price
        self.args["open_created_size"] = open_order.created.size
        self.args["open_created_dt"] = open_order.created.dt
        if open_order.status == open_order.Completed:
            self.args["is_executed"] = True
            self.args["open_executed_price"] = open_order.executed.price
            self.args["open_executed_size"] = open_order.executed.size
            self.args["open_executed_dt"] = open_order.executed.dt
        else:
            self.args["is_executed"] = False

        # 仓位状态
        if pos.is_completed():
            # 已平仓
            self.args["completed"] = True
            completed_type = pos.get_completed_type()
            self.args["completed_type"] = completed_type
            # 平仓信息
            if completed_type == "close":
                self.args["completed_executed_price"] = pos.get_close_order().executed.price
                self.args["completed_executed_dt"] = pos.get_close_order().executed.dt
            elif completed_type == "take_profit":
                self.args["completed_executed_price"] = pos.get_take_profit_order().executed.price
                self.args["completed_executed_dt"] = pos.get_take_profit_order().executed.dt
            elif completed_type == "stop_loss":
                self.args["completed_executed_price"] = pos.get_stop_loss_order().executed.price
                self.args["completed_executed_dt"] = pos.get_stop_loss_order().executed.dt
        elif pos.is_cancelled():
            # 已取消
            self.args["cancelled"] = True

    def get_arg(self, key):
        return self.args.get(key, None)

    @classmethod
    def from_dict(cls, data_dict: Dict):
        return cls(**data_dict)

    def to_dict(self):
        return self.args.copy()

class ManualStrategy(AbstractStrategy):
    def __init__(self, signal_queue: Queue[ManualSignal], info_queue: Queue[ManualStrategyInfo]):
        super().__init__()
        self.signal_queue = signal_queue
        self.info_queue = info_queue
        self.cash_before_position = 0.
        self.position_info = {}

    def get_info(self) -> ManualStrategyInfo:
        # 行情信息
        data_df = self.get_data_as_df(self.datas[0])
        market_index_df = self.get_data_as_df(self.datas[1]) if len(self.datas) > 1 else None
        industry_index_df = self.get_data_as_df(self.datas[2]) if len(self.datas) > 2 else None

        # 持仓信息
        position_running = None
        completed_positions = []
        order_refs = self.get_my_position_id_by_data(self.datas[0])
        for order_ref in order_refs:
            pos = self.get_my_position(order_ref)
            manual_strategy_position = ManualStrategyPosition(pos, **self.position_info[order_ref])
            if pos.is_completed() or pos.is_cancelled():
                completed_positions.append(manual_strategy_position)
            else:
                position_running = manual_strategy_position

        sorted(completed_positions, key=lambda x: x.get_position().get_open_order().created)

        # 输出策略信息
        info = ManualStrategyInfo(current_time=data_df.loc[len(data_df) - 1, "datetime"],
                                  data=data_df, market_index=market_index_df, industry_index=industry_index_df,
                                  position_running=position_running, completed_positions=completed_positions)
        return info

    def handle_data(self):
        # 行情
        data = self.datas[0]

        # 持仓信息
        my_position_id = None
        order_refs = self.get_my_position_id_by_data(data, skip_closed=True)
        if len(order_refs) >= 1:
            my_position_id = order_refs[0]
        else:
            # 无仓位情况下，记录开仓前的现金
            self.cash_before_position = self.broker.getcash()

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
                order = self.open_market(data, is_buy=is_buy, target_price=target_price, stop_price=stop_price)
            else:
                order = self.open_limit(data, price, is_buy=is_buy, target_price=target_price, stop_price=stop_price)

            self.position_info[order.ref] = {
                "open_reason": signal.get_arg("reason")
            }

        elif signal.signal_type == ManualSignal.Close:
            if my_position_id is None:
                raise ValueError("没有仓位，不允许执行平仓")

            if my_position_id in self.position_info:
                self.position_info[my_position_id]["close_reason"] = signal.get_arg("reason")
            else:
                self.position_info[my_position_id] = {
                    "close_reason": signal.get_arg("reason")
                }

            # 平仓
            self.close_position(my_position_id)

        elif signal.signal_type == ManualSignal.Continue:
            pass

    def stop(self):
        self.info_queue.put(ManualStrategyInfo(stop=True))

class ManualBacktestEngine:
    def __init__(self, broker = AStockBroker(), cash = 1000000.0, commission=0.001, from_datetime: pd.Timestamp = None):
        # 回测开始的时间
        self.from_datetime = from_datetime

        # 回测引擎
        self.cerebro = bt.Cerebro()

        # 回测引擎信息交互队列
        self.signal_queue = Queue[ManualSignal](1)
        self.info_queue = Queue[ManualStrategyInfo](1)

        # 回测行情数据
        self.df_dict = {}

        # 初始化回测引擎
        self.cerebro.broker = broker
        self.cerebro.broker.setcash(cash)
        self.cerebro.broker.setcommission(commission=commission)
        self.cerebro.addsizer(bt.sizers.PercentSizer, percents=99)

        # 回测引擎添加策略
        self.cerebro.addstrategy(ManualStrategy, signal_queue=self.signal_queue, info_queue=self.info_queue)

        # 回测引擎添加分析指标
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

        # 回测过程中的当前阶段状态信息
        self.info = None

        # 回测结果
        self.result = None

    def add_data(self, df: pd.DataFrame):
        self.df_dict["data"] = df

    def add_market_index(self, df: pd.DataFrame):
        self.df_dict["market_index"] = df

    def add_industry_index(self, df: pd.DataFrame):
        self.df_dict["add_industry_index"] = df

    def get_info(self) -> ManualStrategyInfo:
       return self.info

    def send_signal(self, signal: ManualSignal):
        self.signal_queue.put(signal)
        self.info = self.info_queue.get()

    def run(self):
        check_index_consistency(self.df_dict)
        # 添加数据到引擎
        for symbol, df in self.df_dict.items():
            data = PandasData(dataname=df, name=symbol)
            self.cerebro.adddata(data)

        # 运行策略
        def _backtest():
            results = self.cerebro.run()
            self.result = results[0]
        threading.Thread(target=_backtest).start()
        self.info = self.info_queue.get()

        if self.from_datetime is not None:
            while True:
                data_df = self.info.get_arg("data")
                if data_df.loc[len(data_df) - 1, "datetime"] >= self.from_datetime:
                    break
                self.send_signal(ManualSignal(ManualSignal.Continue))

    def stop(self):
        if self.get_info() is None:
            return

        position_running = self.get_info().get_arg("position_running")
        if position_running is None:
            return

        self.send_signal(ManualSignal(ManualSignal.Close))
        while True:
            position_running = self.get_info().get_arg("position_running")
            if position_running is not None:
                self.send_signal(ManualSignal(ManualSignal.Continue))
            else:
                break

    def plot(self):
        if self.result is not None:
            self.cerebro.plot()

    def get_sharpe(self):
        if self.result:
            return self.result.analyzers.sharpe.get_analysis()
        return None

    def get_drawdown(self):
        if self.result:
            return self.result.analyzers.drawdown.get_analysis()
        return None

    def get_trade_analysis(self):
        if self.result:
            return self.result.analyzers.trades.get_analysis()
        return None