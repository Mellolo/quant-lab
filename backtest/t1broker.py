import logging

import backtrader as bt
from .logger import log_backtest

class T1Broker(bt.brokers.BackBroker):
    def __init__(self):
        super(T1Broker, self).__init__()
        # 记录买入成交记录的日期
        self._buy_executions = {}

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        actual_size = size
        # 检查买入数量是否是100的倍数
        if size % 100 != 0:
            actual_size = (actual_size // 100) * 100
            message = f"买入数量必须是100的倍数，当前数量: {size}，已调整为: {actual_size}"
            if actual_size <= 0:
                message += "，买入数量不足100股，无法卖出"
            log_backtest(data, message)
            
        # 如果调整后数量为0，则不执行买入
        if actual_size <= 0:
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
        data_name = data._name or 'default'
        current_date = data.datetime.date(0)

        actual_size = size
        # 检查是否可以卖出
        # 根据T+1规则，当天买入成交的股票不能当天卖出
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
            actual_size = min(size, available_size)
            # 如果尝试卖出当天买入成交的股票，打印提醒日志
            log_backtest(data, f"T+1限制提醒: {data_name} 有 {today_buys} 股当天买入成交后无法卖出，实际可卖出数量: {available_size}, 本订单卖出数量调整为:{min(size, available_size)}")

        # 检查卖出数量是否是100的倍数
        if actual_size % 100 != 0:
            actual_size = (actual_size // 100) * 100
            message = f"卖出数量必须是100的倍数，当前数量: {size}，已调整为: {actual_size}"
            if actual_size <= 0:
                message += "，卖出数量不足100股，无法卖出"
            log_backtest(data, message)

        # 如果调整后数量为0，则不执卖出
        if actual_size <= 0:
            return None
        
        # 执行卖出操作
        return super(T1Broker, self).sell(
            owner, data, actual_size, price, plimit, exectype, valid, tradeid, oco,
            trailamount, trailpercent, **kwargs
        )

    def notify(self, order):
        data_name = order.data._name or 'default'

        if order.status in [order.Completed]:
            # 订单已完成（完全成交）
            if order.isbuy():
                # 初始化该标的的买入成交记录列表
                if data_name not in self._buy_executions:
                    self._buy_executions[data_name] = []

                # 添加买入成交记录
                self._buy_executions[data_name].append({
                    'date': order.data.datetime.date(0),
                    'size': order.executed.size,
                    'price': order.executed.price
                })
                log_backtest(order.data,
                             f"买入订单ID({order.ref})成交: 价格({order.executed.price:.2f}), 数量({order.executed.size}))")
            elif order.issell():
                log_backtest(order.data,
                             f"卖出订单ID({order.ref})成交: 价格({order.executed.price:.2f}), 数量({order.executed.size}))")
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # 订单被取消、保证金不足、被拒绝或过期
            log_backtest(order.data, f"订单ID({order.ref})未成交:{order.Status[order.status]}")

        # 处理订单状态更新
        super(T1Broker, self).notify(order)
