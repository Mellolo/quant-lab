import backtrader as bt
from backtrader.utils.date import num2date
import pandas as pd
from datetime import datetime

# 自定义broker，实现A股T+1交易规则
class T1Broker(bt.brokers.BackBroker):
    def __init__(self):
        super(T1Broker, self).__init__()
        self._buy_orders = {}  # 记录买入订单的日期

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        # 执行买入操作
        order = super(T1Broker, self).buy(
            owner, data, size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )
        
        # 记录买入订单的日期
        data_name = data._name or 'default'
        if data_name not in self._buy_orders:
            self._buy_orders[data_name] = []
            
        self._buy_orders[data_name].append({
            'date': data.datetime.date(0),
            'size': abs(size),  # 确保为正数
            'price': price or data.close[0]
        })
        
        return order

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        # 检查是否可以卖出
        # 根据T+1规则，当天买入的股票不能当天卖出
        data_name = data._name or 'default'
        current_date = data.datetime.date(0)
        available_size = size
        
        # 计算当天买入的股票数量（不能卖出）
        today_buys = 0
        if data_name in self._buy_orders:
            for order in self._buy_orders[data_name]:
                if order['date'] == current_date:
                    today_buys += order['size']
        
        if today_buys > 0:
            # 如果当天有买入，则限制卖出数量
            position = self.getposition(data)
            # 可用卖出数量为持仓量减去当日买入量
            available_size = max(0, position.size - today_buys)
            
            # 如果尝试卖出当天买入的股票，打印提醒日志
            restricted_size = min(size, today_buys)
            if restricted_size > 0:
                current_datetime = num2date(data.datetime[0])
                print(f"T+1限制提醒: {current_datetime} {data_name} 有 {restricted_size} 股当天买入的股票无法当天卖出")
            
            if available_size <= 0:
                # 如果没有可卖数量，返回无效订单
                return super(T1Broker, self).sell(
                    owner, data, 0, price, plimit, exectype, valid, tradeid, oco,
                    trailamount, trailpercent, **kwargs
                )
        
        # 执行卖出操作
        return super(T1Broker, self).sell(
            owner, data, available_size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )

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
            print(f"订单已发送: Order ID{order.ref} Order Status {order.status} 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 时间 {num2date(self.data.datetime[0])}")
            # 订单已提交/已接受，但尚未成交
            self.order_dict[order.ref] = {
                'type': 'buy' if order.isbuy() else 'sell',
                'price': order.executed.price,
                'size': order.executed.size,
                'datetime': num2date(self.data.datetime[0]),
                'status': 'completed'
            }

        if order.status in [order.Completed]:
            # 订单已完成（完全成交）
            if order.isbuy():
                print(f"买入订单成交: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 时间 {num2date(self.data.datetime[0])}")
                self.order_dict[order.ref] = {
                    'type': 'buy',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'datetime': num2date(self.data.datetime[0]),
                    'status': 'completed'
                }
            elif order.issell():
                print(f"卖出订单成交: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 时间 {num2date(self.data.datetime[0])}")
                self.order_dict[order.ref] = {
                    'type': 'sell',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'datetime': num2date(self.data.datetime[0]),
                    'status': 'completed'
                }
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # 订单被取消、保证金不足、被拒绝或过期
            print(f"订单未成交: {order.Status[order.status]}")
            self.order_dict[order.ref] = {
                'type': 'buy' if order.isbuy() else 'sell',
                'price': order.price,
                'size': order.size,
                'datetime': num2date(self.data.datetime[0]),
                'status': order.Status[order.status]
            }

    def get_order_info(self, order_id):
        """根据订单ID查询订单信息"""
        return self.order_dict.get(order_id, None)

    def next(self):
        # 获取当前具体时间
        current_time = num2date(self.data.datetime[0])
        
        # 播报仓位信息
        # position = self.getposition()
        # print(f"\n=== 仓位信息 ===")
        # print(f"时间: {current_time}")
        # print(f"当前持仓数量: {position.size}")
        # print(f"当前持仓成本: {position.price:.2f}")
        # print(f"当前账户价值: {self.broker.getvalue():.2f}")
        # print(f"可用资金: {self.broker.getcash():.2f}")
        
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
            print(f'BUY CREATE, Order status: {order.status}, Price: {self.data.close[0]:.2f}, Date: {current_time}, Order ID: {order.ref}')
        
        elif self.crossover < 0:  # 下穿
            if self.position:  # 只有在有持仓时才卖出
                order = self.sell()  # 保存订单引用
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
                               fromdate=datetime(2025, 9, 1),
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