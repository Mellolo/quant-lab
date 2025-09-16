import backtrader as bt
from backtrader.utils.date import num2date
import pandas as pd
from datetime import datetime
from backtest.t1broker import T1Broker
import os

# 定义策略类
class MA5Strategy(bt.Strategy):
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
        
        # 用于存储订单信息
        self.order_dict = {}  # 用于存储订单ID和订单信息的字典

    def notify_order(self, order):
        if order.status in [order.Created, order.Submitted, order.Accepted]:
            self.order_dict[order.ref] = {
                'type': 'buy' if order.isbuy() else 'sell',
                'created_price': order.created.price,
                'created_size': order.created.size,
                'executed_price': order.executed.price,
                'executed_size': order.executed.size,
                'datetime': num2date(order.data.datetime[0]),
                'status': order.status
            }

    def get_order_info(self, order_id):
        """根据订单ID查询订单信息"""
        return self.order_dict.get(order_id, None)

    def next(self):
        # 遍历所有数据源（标的）
        for i, data in enumerate(self.datas):
            # 获取当前具体时间
            current_time = num2date(data.datetime[0])
            data_name = data._name or f'data_{i}'
            
            # 检查是否是交叉点
            if self.crossover[data] > 0:  # 上穿
                order = self.buy(data=data)  # 保存订单引用
                if order:
                    print(f'{data_name} 买入订单ID({order.ref})发送, 订单状态: {order.status}, 价格: {data.close[0]:.2f}, 时间: {current_time}')
            
            elif self.crossover[data] < 0:  # 下穿
                position = self.getposition(data)
                if position:  # 只有在有持仓时才卖出
                    order = self.sell(data=data)  # 保存订单引用
                    if order:
                        print(f'{data_name} 卖出订单ID({order.ref})发送, 订单状态: {order.status}, 价格: {data.close[0]:.2f}, 时间: {current_time}')

def main():
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(MA5Strategy)
    
    # 读取test目录下的所有数据文件
    test_dir = 'test'
    if os.path.exists(test_dir):
        data_files = [f for f in os.listdir(test_dir) if f.endswith('.csv')]
        
        for data_file in data_files:
            file_path = os.path.join(test_dir, data_file)
            # 读取数据
            data_df = pd.read_csv(file_path)
            
            # 将第一列设为时间索引
            data_df['datetime'] = pd.to_datetime(data_df.iloc[:, 0])
            data_df.set_index('datetime', inplace=True)
            
            # 获取股票代码作为数据名称
            symbol = data_file.replace('.csv', '')
            
            # 创建数据源（5分钟级别）
            data = bt.feeds.PandasData(dataname=data_df,
                                       fromdate=datetime(2025, 8, 1),
                                       todate=datetime(2025, 9, 12),
                                       name=symbol)
            
            # 添加数据到引擎
            cerebro.adddata(data)
            print(f'已加载数据: {symbol}')
    
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