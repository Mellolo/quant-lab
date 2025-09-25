import backtrader as bt
from backtrader import num2date


class AbstractStrategy(bt.Strategy):
    def __init__(self, t1 = False):
        self.my_position = {}
        self.t1 = t1

    def next_open(self):
        if self.t1:
            for order_ref in self.my_position:
                mark_take_profit_t1 = self.my_position[order_ref].get("mark_take_profit_t1", None)
                if mark_take_profit_t1 is not None:
                    self.take_profit(order_ref, mark_take_profit_t1)
                    self.my_position[order_ref]["mark_take_profit_t1"] = None

                mark_stop_loss_t1 = self.my_position[order_ref].get("mark_stop_loss_t1", None)
                if mark_stop_loss_t1 is not None:
                    self.stop_loss(order_ref, mark_stop_loss_t1)
                    self.my_position[order_ref]["mark_stop_loss_t1"] = None

    def open_market(self, data, size, is_buy=True, target_price = None, stop_price = None):
        if is_buy:
            order =  self.buy(data=data, size=size, exectype=bt.Order.Market)
        else:
            order = self.sell(data=data, size=size, exectype=bt.Order.Market)

        self.my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self.my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self.my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def open_limit(self, data, size, price, is_buy=True, target_price = None, stop_price = None):
        if is_buy:
            order = self.buy(data=data, size=size, price=price, exectype=bt.Order.Limit)
        else:
            order = self.sell(data=data, size=size, price=price, exectype=bt.Order.Limit)

        self.my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self.my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self.my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def open_break(self, data, size, break_price, is_buy=True, target_price = None, stop_price = None):
        if is_buy:
            order =  self.buy(data=data, size=size, price=break_price, exectype=bt.Order.Stop)
        else:
            order = self.sell(data=data, size=size, price=break_price, exectype=bt.Order.Stop)

        self.my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self.my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self.my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def take_profit(self, order_ref, target_price):
        # 获取开仓订单
        order = self.my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"设置止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止盈单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止盈单
        close_order = self.my_position.get(order_ref, {}).get("close_order", None)
        mark_close = self.my_position.get(order_ref, {}).get("mark_close", False)
        if close_order or mark_close:
            raise ValueError(f"仓位({order_ref})已经进入平仓状态，请勿设置止盈单")

        # 如果t+1且是开仓当天，无法下止盈单
        if self.t1 and num2date(order.executed.dt) == order.data.datetime.date():
            raise ValueError(f"该开仓订单是t+1且是开仓当天，无法下止盈单")

        # 取消之前设置的止盈单
        is_cancel = self.cancel_take_profit(order_ref)
        # 如果之前就没有止盈单，那么直接发送
        if not is_cancel:
            # 创建新止盈单
            data = order.data # 交易品种
            size = order.executed.size # 该仓位持有的数量，用作于止盈数量
            if order.isbuy():
                take_profit_order = self.sell(data=data, size=size, price=target_price, exectype=bt.Order.Limit)
            else:
                take_profit_order = self.buy(data=data, size=size, price=target_price, exectype=bt.Order.Limit)
            self.my_position[order_ref]["take_profit_order"] = take_profit_order
        else:
            # 设置止盈目标，等待notify_order中取消完成后再创建
            self.my_position[order_ref]["mark_take_profit"] = target_price

    def cancel_take_profit(self, order_ref):
        if not self.my_position.get(order_ref, {}).get("open_order", None):
            raise ValueError(f"取消止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 不再需要notify处理
        self.my_position[order_ref]["mark_take_profit"] = None

        # 取消之前设置的止盈单
        take_profit_order = self.my_position.get(order_ref, {}).get("take_profit_order", None)
        if take_profit_order:
            self.cancel(take_profit_order)
            return True

        return False

    def stop_loss(self, order_ref, stop_price):
        # 获取开仓订单
        order = self.my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"设置止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止损单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止损单
        close_order = self.my_position.get(order_ref, {}).get("close_order", None)
        mark_close = self.my_position.get(order_ref, {}).get("mark_close", False)
        if close_order or mark_close:
            raise ValueError(f"仓位({order_ref})已经进入平仓状态，请勿设置止损单")

        # 如果t+1且是开仓当天，无法下止损单
        if self.t1 and num2date(order.executed.dt) == order.data.datetime.date():
            raise ValueError(f"该开仓订单是t+1且是开仓当天，无法下止损单")

        # 取消之前设置的止损单
        is_cancel = self.cancel_stop_loss(order_ref)
        # 如果之前就没有止损单，那么直接发送
        if not is_cancel:
            # 创建新止损单
            data = order.data  # 交易品种
            size = order.executed.size  # 该仓位持有的数量，用作于止损数量
            if order.isbuy():
                stop_loss_order = self.sell(data=data, size=size, price=stop_price, exectype=bt.Order.Stop)
            else:
                stop_loss_order = self.buy(data=data, size=size, price=stop_price, exectype=bt.Order.Stop)
            self.my_position[order_ref]["stop_loss_order"] = stop_loss_order
        else:
            # 设置止损目标，等待notify_order中取消完成后再创建
            self.my_position[order_ref]["mark_stop_loss"] = stop_price

    def cancel_stop_loss(self, order_ref):
        if not self.my_position.get(order_ref, {}).get("open_order", None):
            raise ValueError(f"取消止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 不再需要notify处理
        self.my_position[order_ref]["mark_stop_loss"] = None

        # 取消之前设置的止损单
        stop_loss_order = self.my_position.get(order_ref, {}).get("stop_loss_order", None)
        if stop_loss_order:
            self.cancel(stop_loss_order)
            return True

        return False

    def cancel_order(self, order_ref):
        # 获取开仓订单
        order = self.my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"取消开仓但未找到仓位，开仓订单编号: {order_ref}")
        self.cancel(order)

    def close_position(self, order_ref):
        # 获取开仓订单
        order = self.my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"平仓但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许平仓
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许平仓
        close_order = self.my_position.get(order_ref, {}).get("close_order", None)
        if close_order:
            raise ValueError(f"仓位({order_ref})已经发送平仓订单，请勿平仓")

        # 如果t+1且是开仓当天，无法平仓
        if self.t1 and num2date(order.executed.dt) == order.data.datetime.date():
            raise ValueError(f"该开仓订单是t+1且是开仓当天，无法平仓")

        # 取消止盈止损单
        take_profit_cancel = self.cancel_take_profit(order_ref)
        stop_loss_cancel = self.cancel_stop_loss(order_ref)

        # 如果没有止盈止损单，那么直接平仓
        if not take_profit_cancel and not stop_loss_cancel:
            # 平仓
            order = self.my_position[order_ref]["open_order"]
            data = order.data  # 交易品种
            size = order.executed.size # 该仓位持有的数量，用作于平仓数量
            if order.isbuy():
                close_order = self.sell(data=data, size=size, exectype=bt.Order.Market)
            else:
                close_order = self.buy(data=data, size=size, exectype=bt.Order.Market)
            self.my_position[order_ref]["close_order"] = close_order
        else:
            # 设置平仓目标，等待notify_order中该仓位所有的止盈止损都取消后再创建
            self.my_position[order_ref]["mark_close"] = True

    def _notify_order_type(self, order):
        if order.ref in self.my_position:
            # 开仓订单
            return "open", order.ref
        for order_ref in self.my_position:
            # 平仓订单
            close_order = self.my_position[order_ref].get("close_order", None)
            if close_order and close_order.ref == order.ref:
                return "close", order_ref
            # 止盈订单
            take_profit_order = self.my_position[order_ref].get("take_profit_order", None)
            if take_profit_order and take_profit_order.ref == order.ref:
                return "take_profit", order_ref
            # 止损订单
            stop_loss_order = self.my_position[order_ref].get("stop_loss_order", None)
            if stop_loss_order and stop_loss_order.ref == order.ref:
                return "stop_loss", order_ref
        # 未知订单
        return "unknown", None

    def notify_order(self, order):
        order_type, position_ref = self._notify_order_type(order)
        if order.status in [order.Completed]:
            # 订单已完成
            if order_type == "open":
                # 非t+1
                if not self.t1:
                    # 设置止盈
                    take_profit_price = self.my_position.get(position_ref, {}).get("mark_take_profit", None)
                    if take_profit_price is not None:
                        self.take_profit(position_ref, take_profit_price)
                    # 设置止损
                    stop_loss_price = self.my_position.get(position_ref, {}).get("mark_stop_loss", None)
                    if stop_loss_price is not None:
                        self.stop_loss(position_ref, stop_loss_price)
                else:
                    # t+1
                    # 设置止盈
                    take_profit_price = self.my_position.get(position_ref, {}).get("mark_take_profit", None)
                    if take_profit_price is not None:
                        self.my_position[position_ref]["mark_take_profit"] = None
                        self.my_position[position_ref]["mark_take_profit_t1"] = take_profit_price
                    # 设置止损
                    stop_loss_price = self.my_position.get(position_ref, {}).get("mark_stop_loss", None)
                    if stop_loss_price is not None:
                        self.my_position[position_ref]["mark_stop_loss"] = None
                        self.my_position[position_ref]["mark_stop_loss_t1"] = stop_loss_price

            elif order_type in ["close", "take_profit", "stop_loss"]:
                self.cancel_take_profit(position_ref)
                self.cancel_stop_loss(position_ref)
                del self.my_position[position_ref]
        elif order.status in [order.Partial]:
            # 订单部分成交
            raise ValueError(f"订单部分成交的场景，回测暂不支持，订单编号: {order.ref}")
        elif order.status in [order.Canceled]:
            # 订单被取消
            if order_type == "take_profit":
                self.my_position[position_ref]["take_profit_order"] = None
                # 若有，设置新止盈
                take_profit_price = self.my_position.get(position_ref, {}).get("mark_take_profit", None)
                if take_profit_price is not None:
                    self.take_profit(position_ref, take_profit_price)

            if order_type == "stop_loss":
                self.my_position[position_ref]["stop_loss_order"] = None
                # 若有，设置新止损
                stop_loss_price = self.my_position.get(position_ref, {}).get("mark_stop_loss", None)
                if stop_loss_price is not None:
                    self.stop_loss(position_ref, stop_loss_price)

            if order_type in ["take_profit", "stop_loss"]:
                # 若止盈止损单都被取消，且设置了平仓目标，则平仓
                take_profit_order = self.my_position.get(position_ref, {}).get("take_profit_order", None)
                stop_loss_order = self.my_position.get(position_ref, {}).get("stop_loss_order", None)
                if not take_profit_order and not stop_loss_order:
                    if self.my_position.get(position_ref, {}).get("mark_close", False):
                        self.close_position(position_ref)

        elif order.status in [order.Expired]:
            # 订单已过期
            if order_type == "open":
                del self.my_position[position_ref]
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)
        elif order.status in [order.Margin, order.Rejected]:
            # 交易所拒绝了订单
            if order_type == "open":
                del self.my_position[position_ref]
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)
