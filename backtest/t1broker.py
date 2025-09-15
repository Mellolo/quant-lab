import backtrader as bt
from backtrader.utils.date import num2date

class T1Broker(bt.brokers.BackBroker):
    def __init__(self):
        super(T1Broker, self).__init__()
        # 记录买入成交记录的日期
        self._buy_executions = {}

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        # 检查买入数量是否是100的倍数
        if size % 100 != 0:
            print(f"买入数量必须是100的倍数，当前数量: {size}，已调整为: {size - (size % 100)}")
            size = size - (size % 100)
            
            # 如果调整后数量为0，则不执行买入
            if size <= 0:
                print("买入数量不足100股，无法买入")
                return None
        
        # 执行买入操作
        order = super(T1Broker, self).buy(
            owner, data, size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )
        
        return order

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        # 检查卖出数量是否是100的倍数
        if size % 100 != 0:
            print(f"卖出数量必须是100的倍数，当前数量: {size}，已调整为: {size - (size % 100)}")
            size = size - (size % 100)
            
            # 如果调整后数量为0，则不执行卖出
            if size <= 0:
                print("卖出数量不足100股，无法卖出")
                return None
        
        # 检查是否可以卖出
        # 根据T+1规则，当天买入成交的股票不能当天卖出
        data_name = data._name or 'default'
        current_date = data.datetime.date(0)
        available_size = size
        
        # 计算当天买入成交的股票数量（不能卖出）
        today_buys = 0
        if data_name in self._buy_executions:
            for execution in self._buy_executions[data_name]:
                if execution['date'] == current_date:
                    today_buys += execution['size']
        
        if today_buys > 0:
            # 如果当天有买入成交，则限制卖出数量
            position = self.getposition(data)
            # 可用卖出数量为持仓量减去当日买入成交量
            available_size = max(0, position.size - today_buys)
            
            # 再次检查调整后的可用数量是否是100的倍数
            if available_size % 100 != 0:
                available_size = (available_size // 100) * 100
                print(f"可用卖出数量调整为100的倍数: {available_size}")
            
            # 如果尝试卖出当天买入成交的股票，打印提醒日志
            restricted_size = min(size, today_buys)
            if restricted_size > 0:
                current_datetime = num2date(data.datetime[0])
                print(f"T+1限制提醒: {current_datetime} {data_name} 有 {restricted_size} 股当天买入成交的股票无法当天卖出")
            
            if available_size <= 0:
                # 如果没有可卖数量，返回无效订单
                print("没有可卖出的股票")
                return None
        
        # 执行卖出操作
        return super(T1Broker, self).sell(
            owner, data, available_size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )

    def notify_order(self, order):
        # 处理订单状态更新
        super(T1Broker, self).notify_order(order)
        
        # 只有在订单完全成交时才记录买入成交记录
        if order.status == order.Completed and order.isbuy():
            data_name = order.data._name or 'default'
            execution_date = order.data.datetime.date(0)
            
            # 初始化该标的的买入成交记录列表
            if data_name not in self._buy_executions:
                self._buy_executions[data_name] = []
            
            # 添加买入成交记录
            self._buy_executions[data_name].append({
                'date': execution_date,
                'size': order.executed.size,
                'price': order.executed.price
            })