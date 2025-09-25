import datetime

import backtrader as bt
from backtrader.utils.date import num2date
import pandas as pd
from backtest.broker.t1broker import T1Broker
from backtest.barfeeds.astock import data_feed_astock
from backtest.strategy.strategy import AbstractStrategy
import os

# 定义策略类
class MA5Strategy(AbstractStrategy):
    params = (
        ('ma_period', 48),  # 48周期，相当于4小时均线（5分钟*48=240分钟=4小时）
    )

    def __init__(self):
        # 为每个数据源初始化移动平均线指标和交叉检测指标
        self.ma48 = {}
        self.crossover = {}
        
        for i, data in enumerate(self.datas):
            # 初始化移动平均线指标
            self.ma48[data] = bt.indicators.SimpleMovingAverage(
                data, period=self.params.ma_period)
            
            # 使用backtrader自带的交叉检测指标
            self.crossover[data] = bt.indicators.CrossOver(data.close, self.ma48[data])

    def next(self):
        # 遍历所有数据源（标的）
        for i, data in enumerate(self.datas):
            # 检查是否是交叉点
            if self.crossover[data] > 0:  # 上穿
                self.open_market(data=data, size=300, target_price=data.close[0]*1.05, stop_price=data.close[0] * 0.95)  # 保存订单引用
            
            elif self.crossover[data] < 0:  # 下穿
                for order_ref in self.position:
                    if self.position[order_ref]["open_order"].data == data:
                        self.close_position(order_ref)

def main():
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(MA5Strategy)
    
    # 读取test目录下的所有数据文件
    test_dir = 'test'
    df_dict = {}
    if os.path.exists(test_dir):
        data_files = [f for f in os.listdir(test_dir) if f.endswith('.csv')]
        
        for data_file in data_files:
            file_path = os.path.join(test_dir, data_file)
            # 读取数据
            data_df = pd.read_csv(file_path)
            
            # 获取股票代码作为数据名称
            symbol = data_file.replace('.csv', '')

            df_dict[symbol] = data_df

    datas = data_feed_astock(df_dict, datetime.date(2025, 8, 1), datetime.date(2025, 8, 15), "5m")
    # 添加数据到引擎
    for data in datas:
        cerebro.adddata(data)
    
    # 设置自定义broker
    cerebro.broker = T1Broker()
    
    # 设置初始资金
    cerebro.broker.setcash(10000.0)
    
    # 设置佣金 - 0.1%
    cerebro.broker.setcommission(commission=0.001)
    
    # 设置仓位大小 - 每个标的使用可用资金的相等份额
    cerebro.addsizer(bt.sizers.PercentSizer, percents=99 // len(cerebro.datas) if cerebro.datas else 99)
    
    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    
    # 打印初始资金
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    
    # 运行策略
    results = cerebro.run()
    strat = results[0]
    
    # 打印最终资金
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    
    # 打印分析结果
    print('夏普比率:', strat.analyzers.sharpe.get_analysis())
    print('最大回撤:', strat.analyzers.drawdown.get_analysis())
    print('交易分析:', strat.analyzers.trades.get_analysis())
    
    # 显示结果图表
    cerebro.plot()

if __name__ == '__main__':
    main()