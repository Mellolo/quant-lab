import datetime
import backtrader as bt
import pandas as pd
from backtest.broker.t1broker import T1Broker
from backtest.feeds.clean import data_clean_with_merge, check_index_consistency
from backtrader.feeds import PandasData
from backtest.strategy.strategy import AbstractStrategy
import os

# 定义策略类
class MA5Strategy(AbstractStrategy):
    params = (
        ('ma_period', 48),  # 48周期，相当于4小时均线（5分钟*48=240分钟=4小时）
    )

    def __init__(self):
        super().__init__(t1=True)
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
        self.next_open()

        # 遍历所有数据源（标的）
        for i, data in enumerate(self.datas):
            order_refs = self.get_my_position_id_by_data(data)
            for order_ref in order_refs:
                order = self.get_my_position_open_order(order_ref)
                if order.status == order.Completed:
                    if self.crossover[data] < 0:  # 下穿
                        #self.close_position(order_ref)
                        pass

            # 检查是否是交叉点
            if len(order_refs) == 0:
                if self.crossover[data] > 0:  # 上穿
                    self.open_market(data=data, target_price=data.close[0] * 1.03, stop_price=data.close[0] * 0.98)


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
            data_df = data_clean_with_merge(data_df,
                                datetime.date(2025, 5, 1),
                                datetime.date(2025, 9, 15),
                                [("09:30", "11:30"), ("13:00", "15:00")],
                                "5m", "30m")
            # 获取股票代码作为数据名称
            symbol = data_file.replace('.csv', '')
            df_dict[symbol] = data_df

    check_index_consistency(df_dict)
    # 添加数据到引擎
    for symbol, df in df_dict.items():
        data = PandasData(dataname=df, fromdate=datetime.date(2025, 5, 1), todate=datetime.date(2025, 9, 15), name=symbol)
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