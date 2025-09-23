import backtrader as bt
from backtrader import BuyOrder
from black.concurrency import cancel


class AbstractStrategy(bt.Strategy):
    params = (
        ('position', {}),
    )

    def open_market(self, data, size, is_buy=True):
        if is_buy:
            order =  self.buy(data=data, size=size, exectype=bt.Order.Market)
        else:
            order = self.sell(data=data, size=size, exectype=bt.Order.Market)

        self.position[order.ref] = {
            "open_order": order,
        }
        return order

    def open_limit(self, data, size, price, is_buy=True):
        if is_buy:
            order = self.buy(data=data, size=size, price=price, exectype=bt.Order.Limit)
        else:
            order = self.sell(data=data, size=size, price=price, exectype=bt.Order.Limit)

        self.position[order.ref] = {
            "open_order": order,
        }
        return order

    def open_break(self, data, size, break_price, is_buy=True):
        if is_buy:
            order =  self.buy(data=data, size=size, price=break_price, exectype=bt.Order.Stop)
        else:
            order = self.sell(data=data, size=size, price=break_price, exectype=bt.Order.Stop)

        self.position[order.ref] = {
            "open_order": order,
        }
        return order

    def take_profit(self, order_ref, target_price):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"设置止盈单但未找到仓位，开仓订单编号: {order_ref}")
        self.position[order_ref]["mark_take_profit"] = target_price # 标记止盈

        order = self.position[order_ref]["open_order"]
        if not order.status in [order.Completed, order.Partial]:
            return None

        # 取消之前设置的止盈单
        self.cancel_take_profit(order_ref)

        # 创建新止盈单
        data = order.data # 交易品种
        size = order.executed.size # 该仓位持有的数量，用作于止盈数量
        if order.isbuy():
            take_profit_order = self.sell(data=data, size=size, price=target_price, exectype=bt.Order.Limit)
        else:
            take_profit_order = self.buy(data=data, size=size, price=target_price, exectype=bt.Order.Limit)
        self.position[order_ref]["take_profit_order"] = take_profit_order

        return take_profit_order

    def cancel_take_profit(self, order_ref):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"取消止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 取消之前设置的止盈单，并把取消掉的订单也保存下来
        if self.position[order_ref]["take_profit_order"]:
            take_profit_order = self.position[order_ref]["take_profit_order"]
            self.cancel(take_profit_order)
            if not self.position[order_ref]["last_take_profit_orders"]:
                self.position[order_ref]["last_take_profit_orders"] = []
            self.position[order_ref]["last_take_profit_orders"].append(take_profit_order)
            self.position[order_ref]["take_profit_order"] = None

    def stop_loss(self, order_ref, stop_price):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"设置止损单但未找到仓位，开仓订单编号: {order_ref}")
        self.position[order_ref]["mark_stop_loss"] = stop_price # 标记止损

        order = self.position[order_ref]["open_order"]
        if not order.status in [order.Completed, order.Partial]:
            return None

        # 取消之前设置的止损单
        self.cancel_stop_loss(order_ref)

        # 创建新止损单
        data = order.data  # 交易品种
        size = order.executed.size  # 该仓位持有的数量，用作于止损数量
        if order.isbuy():
            stop_loss_order = self.sell(data=data, size=size, price=stop_price, exectype=bt.Order.Stop)
        else:
            stop_loss_order = self.buy(data=data, size=size, price=stop_price, exectype=bt.Order.Stop)
        self.position[order_ref]["stop_loss_order"] = stop_loss_order

        return stop_loss_order

    def cancel_stop_loss(self, order_ref):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"取消止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 取消之前设置的止损单，并把取消掉的订单也保存下来
        if self.position[order_ref]["stop_loss_order"]:
            stop_loss_order = self.position[order_ref]["stop_loss_order"]
            self.cancel(stop_loss_order)
            if not self.position[order_ref]["last_stop_loss_orders"]:
                self.position[order_ref]["last_stop_loss_orders"] = []
            self.position[order_ref]["last_stop_loss_orders"].append(stop_loss_order)
            self.position[order_ref]["stop_loss_order"] = None

    def cancel_order(self, order_ref):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"取消开仓但未找到仓位，开仓订单编号: {order_ref}")
        self.cancel(self.position[order_ref]["open_order"])

    def close_position(self, order_ref):
        if not self.position[order_ref] or self.position[order_ref]["open_order"]:
            raise ValueError(f"平仓但未找到仓位，开仓订单编号: {order_ref}")

        # 取消止盈止损单
        if self.position[order_ref]["stop_loss_order"]:
            self.cancel(self.position[order_ref]["stop_loss_order"])
        if self.position[order_ref]["take_profit_order"]:
            self.cancel(self.position[order_ref]["take_profit_order"])

        # 平仓
        order = self.position[order_ref]["open_order"]
        data = order.data  # 交易品种
        size = order.executed.size  # 该仓位持有的数量，用作于平仓数量
        if order.isbuy():
            close_order = self.sell(data=data, size=size)
        else:
            close_order = self.buy(data=data, size=size)
        self.position["close_order"] = close_order

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交/已接受 - 无需操作
            return

        # 订单已完成
        if order.status in [order.Completed]:
            # 开仓订单成交后的处理
            if order.ref in self.position:
                if "mark_stop_loss" in self.position[order.ref]:
                    self.stop_loss(order.ref, self.position[order.ref]["mark_stop_loss"])
                if "mark_take_profit" in self.position[order.ref]:
                    self.take_profit(order.ref, self.position[order.ref]["mark_take_profit"])

            # 平仓订单成交后的处理
            for order_ref in self.position:
                close_order = self.position[order_ref].get("close_order", None)
                if close_order and close_order.ref == order.ref:
                    self.cancel(order_ref)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            # 检查是否是止盈或止损订单被取消
            order_type = ""
            if self.take_profit_order and order.ref == self.take_profit_order.ref:
                order_type = "止盈"
            elif self.stop_loss_order and order.ref == self.stop_loss_order.ref:
                order_type = "止损"
            elif self.order and order.ref == self.order.ref:
                order_type = "主"

            self.log(f'{order_type}订单取消/保证金不足/被拒绝')