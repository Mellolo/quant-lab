from queue import Queue
import backtrader as bt
import pandas as pd
from backtrader.feeds import PandasData
import threading

from backtest.broker.aStockBroker import AStockBroker
from backtest.manual.strategy import ManualStrategy, ManualSignal
from backtest.feeds.clean import check_index_consistency


class ManualBacktest:
    def __init__(self, broker = AStockBroker(), cash = 10000.0, commission=0.001):
        # 回测引擎
        self.cerebro = bt.Cerebro()

        # 回测引擎信息交互队列
        self.signal_queue = Queue(1)
        self.info_queue = Queue(1)

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

    def get_info(self):
       return self.info

    def send_signal(self, signal_type, **kwargs):
        signal = ManualSignal(signal_type, **kwargs)
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
        threading.Thread(target=_backtest()).start()

        self.info = self.info_queue.get()

    def plot(self):
        if self.result:
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