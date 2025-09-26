import backtrader as bt
from backtest.logger import log_backtest

class CommonBroker(bt.brokers.BackBroker):
    def __init__(self, name="CommonBroker"):
        super(CommonBroker, self).__init__()
        # 记录买入成交记录的日期
        self._buy_executions = {}
        self.name = name

    def log(self, data, message):
        log_backtest(data, message, source=self.name)

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        messages = []
            
        # 如果调整后数量为0，则不执行买入
        if size <= 0:
            messages.insert(0, "买入订单无法发送(数量为0)")
            self.log(data, "，".join(messages))
            return None
        
        # 执行买入操作
        order = super(CommonBroker, self).buy(
            owner, data, size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )

        messages.insert(0, f'买入订单({order.ref})发送，价格: {price if price else f'市价参考({data.close[0]})'}，数量: {size}')
        self.log(data, "，".join(messages))
        
        return order

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        messages = []

        # 如果调整后数量为0，则不执卖出
        if size <= 0:
            messages.insert(0, "卖出订单无法发送(数量为0)")
            self.log(data, "，".join(messages))
            return None
        
        # 执行卖出操作
        order = super(CommonBroker, self).sell(
            owner, data, size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )

        messages.insert(0, f'卖出订单({order.ref})发送，价格: {price if price else f'市价参考({data.close[0]})'}，数量: {size}')
        self.log(data, "，".join(messages))

        return order

    def notify(self, order):
        if order.status in [order.Completed]:
            # 订单已完成（完全成交）
            if order.isbuy():
                self.log(order.data,
                             f"买入订单({order.ref})成交: 价格({order.executed.price:.2f}), 数量({order.executed.size})")
            elif order.issell():
                self.log(order.data,
                             f"卖出订单({order.ref})成交: 价格({order.executed.price:.2f}), 数量({order.executed.size})")
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # 订单被取消、保证金不足、被拒绝或过期
            self.log(order.data, f"订单({order.ref})未成交:{order.Status[order.status]}")

        # 处理订单状态更新
        super(CommonBroker, self).notify(order)
