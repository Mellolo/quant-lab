import backtrader as bt
from backtrader import num2date
import pandas as pd


class AbstractStrategy(bt.Strategy):
    def __init__(self, loss_tolerant = 0.02):
        self._my_position = {}
        self._loss_tolerant = loss_tolerant

    def get_all_my_position_id(self):
        return list(self._my_position.keys())

    def get_my_position_id_by_data(self, data, skip_closed =  False):
        result = []
        for order_ref in self._my_position:
            open_order = self._my_position[order_ref]["open_order"]
            if skip_closed:
                if self._my_position[order_ref].get("completed", False) or self._my_position[order_ref].get("cancelled", False):
                    continue

            if open_order.data == data:
                result.append(order_ref)

        return result

    def get_my_position(self, order_ref):
        if order_ref in self._my_position:
            return self._my_position[order_ref]
        else:
            return None

    def get_my_position_open_order(self, order_ref):
        return self._my_position.get(order_ref, {}).get("open_order", None)

    def get_my_position_take_profit_order(self, order_ref):
        return self._my_position.get(order_ref, {}).get("take_profit_order", None)

    def get_my_position_stop_loss_order(self, order_ref):
        return self._my_position.get(order_ref, {}).get("stop_loss_order", None)

    @staticmethod
    def get_data_as_df(data):
        df = pd.DataFrame({
            'datetime': [data.datetime.datetime(i) for i in range(1-len(data), 1)],
            'open': [data.open[i] for i in range(1-len(data), 1)],
            'high': [data.high[i] for i in range(1-len(data), 1)],
            'low': [data.low[i] for i in range(1-len(data), 1)],
            'close': [data.close[i] for i in range(1-len(data), 1)],
            'volume': [data.volume[i] for i in range(1-len(data), 1)],
        })
        return df

    def next(self):
        if self.broker.__dict__.get("_close_t1", False):
            for order_ref in self._my_position:
                open_order = self._my_position.get(order_ref, {}).get("open_order", None)
                if open_order.data.datetime.date(0) <= num2date(open_order.executed.dt).date():
                    continue

                mark_take_profit_t1 = self._my_position[order_ref].get("mark_take_profit_t1", None)
                if mark_take_profit_t1 is not None:
                    self.take_profit(order_ref, mark_take_profit_t1)
                    self._my_position[order_ref]["mark_take_profit_t1"] = None

                mark_stop_loss_t1 = self._my_position[order_ref].get("mark_stop_loss_t1", None)
                if mark_stop_loss_t1 is not None:
                    self.stop_loss(order_ref, mark_stop_loss_t1)
                    self._my_position[order_ref]["mark_stop_loss_t1"] = None

        self.handle_data()

    def _get_sizing_by_loss(self, data, price, stop_price):
        # 根据止损计算的开仓size
        loss_per_unit = abs(price - stop_price)
        size_by_loss = int(self.broker.getvalue() * self._loss_tolerant / loss_per_unit)

        # 根据保证金计算最大开仓size
        margin = self.broker.getcommissioninfo(data).margin if self.broker.getcommissioninfo(data).margin else 1.0
        margin_cash = self.broker.getcash() / margin * 0.99
        margin_size = margin_cash / price

        return min(size_by_loss, margin_size)

    def open_market(self, data, size = None, is_buy=True, target_price = None, stop_price = None):
        if size is None:
            if stop_price is None:
                size = self.getsizing(data, isbuy=is_buy)
            else:
                size = self._get_sizing_by_loss(data, data.close[0], stop_price)

        if is_buy:
            order = self.buy(data=data, size=size, exectype=bt.Order.Market)
        else:
            order = self.sell(data=data, size=size, exectype=bt.Order.Market)

        if order is None:
            return None

        self._my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self._my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self._my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def open_limit(self, data, price, size = None, is_buy=True, target_price = None, stop_price = None):
        if size is None:
            if stop_price is None:
                size = self.getsizing(data, isbuy=is_buy)
            else:
                size = self._get_sizing_by_loss(data, price, stop_price)

        if is_buy:
            order = self.buy(data=data, size=size, price=price, exectype=bt.Order.Limit)
        else:
            order = self.sell(data=data, size=size, price=price, exectype=bt.Order.Limit)

        if order is None:
            return None

        self._my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self._my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self._my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def open_break(self, data, break_price, size = None, is_buy=True, target_price = None, stop_price = None):
        if size is None:
            if stop_price is None:
                size = self.getsizing(data, isbuy=is_buy)
            else:
                size = self._get_sizing_by_loss(data, break_price, stop_price)

        if is_buy:
            order =  self.buy(data=data, size=size, price=break_price, exectype=bt.Order.Stop)
        else:
            order = self.sell(data=data, size=size, price=break_price, exectype=bt.Order.Stop)

        if order is None:
            return None

        self._my_position[order.ref] = {
            "open_order": order,
        }

        if target_price:
            self._my_position[order.ref]["mark_take_profit"] = target_price

        if stop_price:
            self._my_position[order.ref]["mark_stop_loss"] = stop_price

        return order

    def take_profit(self, order_ref, target_price):
        # 获取开仓订单
        order = self._my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"设置止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止盈单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止盈单
        close_order = self._my_position.get(order_ref, {}).get("close_order", None)
        mark_close = self._my_position.get(order_ref, {}).get("mark_close", False)
        if close_order or mark_close:
            raise ValueError(f"仓位({order_ref})已经进入平仓状态，请勿设置止盈单")

        # 如果t+1且是开仓当天，无法下止盈单
        if self.broker.__dict__.get("_close_t1", False) and num2date(order.executed.dt).date() == order.data.datetime.date():
            raise ValueError(f"在当前T+1规则下，该开仓订单为当天({num2date(order.executed.dt)})成交，现在({order.data.datetime.datetime()})无法创建止盈单")

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
            self._my_position[order_ref]["take_profit_order"] = take_profit_order
        else:
            # 设置止盈目标，等待notify_order中取消完成后再创建
            self._my_position[order_ref]["mark_take_profit"] = target_price

    def cancel_take_profit(self, order_ref):
        if not self._my_position.get(order_ref, {}).get("open_order", None):
            raise ValueError(f"取消止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 不再需要notify处理
        self._my_position[order_ref]["mark_take_profit"] = None

        # 取消之前设置的止盈单
        take_profit_order = self._my_position.get(order_ref, {}).get("take_profit_order", None)
        if take_profit_order:
            self.cancel(take_profit_order)
            return True

        return False

    def stop_loss(self, order_ref, stop_price):
        # 获取开仓订单
        order = self._my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"设置止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止损单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止损单
        close_order = self._my_position.get(order_ref, {}).get("close_order", None)
        mark_close = self._my_position.get(order_ref, {}).get("mark_close", False)
        if close_order or mark_close:
            raise ValueError(f"仓位({order_ref})已经进入平仓状态，请勿设置止损单")

        # 如果t+1且是开仓当天，无法下止损单
        if self.broker.__dict__.get("_close_t1", False) and num2date(order.executed.dt).date() == order.data.datetime.date():
            raise ValueError(f"在当前T+1规则下，该开仓订单为当天({num2date(order.executed.dt)})成交，现在({order.data.datetime.datetime()})无法创建止损单")

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
            self._my_position[order_ref]["stop_loss_order"] = stop_loss_order
        else:
            # 设置止损目标，等待notify_order中取消完成后再创建
            self._my_position[order_ref]["mark_stop_loss"] = stop_price

    def cancel_stop_loss(self, order_ref):
        if not self._my_position.get(order_ref, {}).get("open_order", None):
            raise ValueError(f"取消止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 不再需要notify处理
        self._my_position[order_ref]["mark_stop_loss"] = None

        # 取消之前设置的止损单
        stop_loss_order = self._my_position.get(order_ref, {}).get("stop_loss_order", None)
        if stop_loss_order:
            self.cancel(stop_loss_order)
            return True

        return False

    def cancel_order(self, order_ref):
        # 获取开仓订单
        order = self._my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"取消开仓但未找到仓位，开仓订单编号: {order_ref}")
        self.cancel(order)

    def close_position(self, order_ref):
        # 获取开仓订单
        order = self._my_position.get(order_ref, {}).get("open_order", None)
        if not order:
            raise ValueError(f"平仓但未找到仓位，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许平仓
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许平仓
        close_order = self._my_position.get(order_ref, {}).get("close_order", None)
        if close_order:
            raise ValueError(f"仓位({order_ref})已经发送平仓订单，请勿平仓")

        # 如果t+1且是开仓当天，无法平仓
        if self.broker.__dict__.get("_close_t1", False) and num2date(order.executed.dt).date() == order.data.datetime.date():
            raise ValueError(f"在当前T+1规则下，该开仓订单为当天({num2date(order.executed.dt)})成交，现在({order.data.datetime.datetime()})无法平仓")

        # 取消止盈止损单
        take_profit_cancel = self.cancel_take_profit(order_ref)
        stop_loss_cancel = self.cancel_stop_loss(order_ref)

        # 如果没有止盈止损单，那么直接平仓
        if not take_profit_cancel and not stop_loss_cancel:
            # 平仓
            order = self._my_position[order_ref]["open_order"]
            data = order.data  # 交易品种
            size = order.executed.size # 该仓位持有的数量，用作于平仓数量
            if order.isbuy():
                close_order = self.sell(data=data, size=size, exectype=bt.Order.Market)
            else:
                close_order = self.buy(data=data, size=size, exectype=bt.Order.Market)
            self._my_position[order_ref]["close_order"] = close_order
        else:
            # 设置平仓目标，等待notify_order中该仓位所有的止盈止损都取消后再创建
            self._my_position[order_ref]["mark_close"] = True

    def _notify_order_type(self, order):
        if order.ref in self._my_position:
            # 开仓订单
            return "open", order.ref
        for order_ref in self._my_position:
            # 平仓订单
            close_order = self._my_position[order_ref].get("close_order", None)
            if close_order and close_order.ref == order.ref:
                return "close", order_ref
            # 止盈订单
            take_profit_order = self._my_position[order_ref].get("take_profit_order", None)
            if take_profit_order and take_profit_order.ref == order.ref:
                return "take_profit", order_ref
            # 止损订单
            stop_loss_order = self._my_position[order_ref].get("stop_loss_order", None)
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
                if not self.broker.__dict__.get("_close_t1", False):
                    # 设置止盈
                    take_profit_price = self._my_position.get(position_ref, {}).get("mark_take_profit", None)
                    if take_profit_price is not None:
                        self.take_profit(position_ref, take_profit_price)
                    # 设置止损
                    stop_loss_price = self._my_position.get(position_ref, {}).get("mark_stop_loss", None)
                    if stop_loss_price is not None:
                        self.stop_loss(position_ref, stop_loss_price)
                else:
                    # t+1
                    # 设置止盈
                    take_profit_price = self._my_position.get(position_ref, {}).get("mark_take_profit", None)
                    if take_profit_price is not None:
                        self._my_position[position_ref]["mark_take_profit"] = None
                        self._my_position[position_ref]["mark_take_profit_t1"] = take_profit_price
                    # 设置止损
                    stop_loss_price = self._my_position.get(position_ref, {}).get("mark_stop_loss", None)
                    if stop_loss_price is not None:
                        self._my_position[position_ref]["mark_stop_loss"] = None
                        self._my_position[position_ref]["mark_stop_loss_t1"] = stop_loss_price

            elif order_type in ["close", "take_profit", "stop_loss"]:
                self.cancel_take_profit(position_ref)
                self.cancel_stop_loss(position_ref)

                self._my_position[position_ref]["completed"] = True
                self._my_position[position_ref]["completed_order"] = order

        elif order.status in [order.Partial]:
            # 订单部分成交
            raise ValueError(f"订单部分成交的场景，回测暂不支持，订单编号: {order.ref}")
        elif order.status in [order.Canceled]:
            # 订单被取消
            if order_type == "open":
                self._my_position[position_ref]["cancelled"] = True

            if order_type == "take_profit":
                self._my_position[position_ref]["take_profit_order"] = None
                # 若有，设置新止盈
                take_profit_price = self._my_position.get(position_ref, {}).get("mark_take_profit", None)
                if take_profit_price is not None:
                    self.take_profit(position_ref, take_profit_price)

            if order_type == "stop_loss":
                self._my_position[position_ref]["stop_loss_order"] = None
                # 若有，设置新止损
                stop_loss_price = self._my_position.get(position_ref, {}).get("mark_stop_loss", None)
                if stop_loss_price is not None:
                    self.stop_loss(position_ref, stop_loss_price)

            if order_type in ["take_profit", "stop_loss"]:
                # 若止盈止损单都被取消，且设置了平仓目标，则平仓
                take_profit_order = self._my_position.get(position_ref, {}).get("take_profit_order", None)
                stop_loss_order = self._my_position.get(position_ref, {}).get("stop_loss_order", None)
                if not take_profit_order and not stop_loss_order:
                    if self._my_position.get(position_ref, {}).get("mark_close", False):
                        self.close_position(position_ref)

        elif order.status in [order.Expired]:
            # 订单已过期
            if order_type == "open":
                self._my_position[position_ref]["cancelled"] = True
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)
        elif order.status in [order.Margin, order.Rejected]:
            # 交易所拒绝了订单
            if order_type == "open":
                self._my_position[position_ref]["cancelled"] = True
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)
