import backtrader as bt
from backtrader.utils.date import num2date
import pandas as pd
from datetime import datetime
from backtest.t1broker import T1Broker

# 定义策略类
class MA5Strategy(bt.Strategy):
    params = (
        ('ma_period', 48),  # 48周期，相当于4小时均线（5分钟*48=240分钟=4小时）
    )

    def __init__(self):
        # 初始化移动平均线指标
        self.ma48 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.ma_period)
        
        # 使用backtrader自带的交叉检测指标
        self.crossover = bt.indicators.CrossOver(self.data.close, self.ma48)
        
        # 用于记录当天是否已经交易过
        self.last_trade_date = None
        
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
                'datetime': num2date(self.data.datetime[0]),
                'status': order.status
            }

        if order.status in [order.Completed]:
            # 订单已完成（完全成交）
            if order.isbuy():
                print(f"买入订单成交: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 时间 {num2date(self.data.datetime[0])}")
            elif order.issell():
                print(f"卖出订单成交: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 时间 {num2date(self.data.datetime[0])}")
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # 订单被取消、保证金不足、被拒绝或过期
            print(f"订单未成交: {order.Status[order.status]}")

    def get_order_info(self, order_id):
        """根据订单ID查询订单信息"""
        return self.order_dict.get(order_id, None)

    def next(self):
        # 获取当前具体时间
        current_time = num2date(self.data.datetime[0])
        
        # 展示历史未完成订单信息
        # pending_orders = [order for order in self.order_dict.values()
        #                  if order['status'] in ['Submitted', 'Accepted']]
        #
        # print(f"\n=== 未完成订单 ===")
        # if pending_orders:
        #     for i, order in enumerate(pending_orders):
        #         price = order['price'] if order['price'] is not None else 0
        #         print(f"{i+1}. {order['type']} - 价格: {price:.2f}, 数量: {order['size']}, 时间: {order['datetime']}, 状态: {order['status']}")
        # else:
        #     print("暂无未完成订单")
        #
        # print("=" * 30)

        # 获取过去几根K线的数据
        # 索引0表示当前K线，-1表示前一根K线，-2表示前两根K线，以此类推
        current_close = self.data.close[0]      # 当前收盘价
        previous_close = self.data.close[-1]    # 前一根K线收盘价
        two_ago_close = self.data.close[-2]     # 前两根K线收盘价
        
        current_open = self.data.open[0]        # 当前开盘价
        previous_open = self.data.open[-1]      # 前一根K线开盘价
        
        current_high = self.data.high[0]        # 当前最高价
        previous_high = self.data.high[-1]      # 前一根K线最高价
        
        current_low = self.data.low[0]          # 当前最低价
        previous_low = self.data.low[-1]        # 前一根K线最低价
        
        # 打印示例数据
        # print(f"时间: {current_time}")
        # print(f"当前K线 - 开盘价: {current_open:.2f}, 收盘价: {current_close:.2f}, 最高价: {current_high:.2f}, 最低价: {current_low:.2f}")
        # print(f"前一根K线 - 开盘价: {previous_open:.2f}, 收盘价: {previous_close:.2f}, 最高价: {previous_high:.2f}, 最低价: {previous_low:.2f}")
        # print(f"前两根K线 - 收盘价: {two_ago_close:.2f}")
        # print("-" * 50)
        
        # 检查是否是交叉点
        if self.crossover > 0:  # 上穿
            order = self.buy()  # 保存订单引用
            if order:
                print(f'BUY CREATE, Order status: {order.status}, Price: {self.data.close[0]:.2f}, Date: {current_time}, Order ID: {order.ref}')
        
        elif self.crossover < 0:  # 下穿
            if self.position:  # 只有在有持仓时才卖出
                order = self.sell()  # 保存订单引用
                if order:
                    print(f'SELL CREATE, Order status: {order.status}, Price: {self.data.close[0]:.2f}, Date: {current_time}, Order ID: {order.ref}')

def main():
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(MA5Strategy)
    
    # 读取prices.csv数据（5分钟级别数据）
    data_df = pd.read_csv('prices.csv')
    
    # 将第一列设为时间索引
    data_df['datetime'] = pd.to_datetime(data_df.iloc[:, 0])
    data_df.set_index('datetime', inplace=True)
    
    # 创建数据源（5分钟级别）
    data = bt.feeds.PandasData(dataname=data_df,
                               fromdate=datetime(2025, 8, 1),
                               todate=datetime(2025, 9, 12))
    
    # 添加数据到引擎
    cerebro.adddata(data)
    
    # 设置自定义broker
    cerebro.broker = T1Broker()
    
    # 设置初始资金
    cerebro.broker.setcash(10000.0)
    
    # 设置佣金 - 0.1%
    cerebro.broker.setcommission(commission=0.001)
    
    # 设置仓位大小
    cerebro.addsizer(bt.sizers.PercentSizer, percents=99)
    
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