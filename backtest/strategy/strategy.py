import backtrader as bt
from backtrader import num2date
import pandas as pd
from typing import Dict
import copy

class SinglePosition:
    def __init__(self, open_order: bt.Order, mark_take_profit: float = None, mark_stop_loss: float = None):
        self.open_order = open_order
        self.take_profit_order = None
        self.stop_loss_order = None
        self.close_order = None

        self.mark_take_profit = mark_take_profit
        self.mark_take_profit_t1 = None
        self.mark_stop_loss = mark_stop_loss
        self.mark_stop_loss_t1 = None
        self.mark_close = False

        self.completed = False
        self.completed_type = None
        self.cancelled = False

    ############ 订单 ############
    def get_open_order(self):
        return self.open_order

    def get_take_profit_order(self):
        return self.take_profit_order

    def get_stop_loss_order(self):
        return self.stop_loss_order

    def get_close_order(self):
        return self.close_order

    def set_take_profit_order(self, order: bt.Order):
        self.take_profit_order = order

    def set_stop_loss_order(self, order: bt.Order):
        self.stop_loss_order = order

    def set_close_order(self, order: bt.Order):
        self.close_order = order

    def clear_take_profit_order(self):
        self.take_profit_order = None

    def clear_stop_loss_order(self):
        self.take_profit_order = None

    def clear_close_order(self):
        self.take_profit_order = None
    ############ 订单 ############

    ############ 标记 ############
    def get_mark_take_profit(self):
        return self.mark_take_profit

    def get_mark_take_profit_t1(self):
        return self.mark_take_profit_t1

    def get_mark_stop_loss(self):
        return self.mark_stop_loss

    def get_mark_stop_loss_t1(self):
        return self.mark_stop_loss_t1

    def get_mark_close(self):
        return self.mark_close

    def set_mark_take_profit(self, price: float):
        self.mark_take_profit = price

    def clear_mark_take_profit(self):
        self.mark_take_profit = None

    def set_mark_take_profit_t1(self, price: float):
        self.mark_take_profit_t1 = price

    def clear_mark_take_profit_t1(self):
        self.mark_take_profit_t1 = None

    def set_mark_stop_loss(self, price: float):
        self.mark_stop_loss = price

    def clear_mark_stop_loss(self):
        self.mark_stop_loss = None

    def set_mark_stop_loss_t1(self, price: float):
        self.mark_stop_loss_t1 = price

    def clear_mark_stop_loss_t1(self):
        self.mark_stop_loss_t1 = None

    def set_mark_close(self, is_close: bool):
        self.mark_close = is_close
    ############ 标记 ############

    ############ 状态 ############
    def set_completed(self, completed_type):
        self.completed = True
        self.completed_type = completed_type

    def set_cancelled(self):
        self.cancelled = True

    def is_completed(self):
        return self.completed

    def is_cancelled(self):
        return self.cancelled
    ############ 状态 ############

class AbstractStrategy(bt.Strategy):
    def __init__(self, loss_tolerant = 0.02):
        self._my_position:Dict[str, SinglePosition] = {}
        self._loss_tolerant = loss_tolerant

    def get_all_my_position_id(self):
        return list(self._my_position.keys())

    def get_my_position_id_by_data(self, data, skip_closed =  False):
        result = []
        for order_ref in self._my_position:
            pos = self._my_position[order_ref]
            if skip_closed:
                if pos.is_completed() or pos.is_cancelled():
                    continue

            if pos.get_open_order().data == data:
                result.append(order_ref)

        return result

    def get_my_position(self, order_ref):
        if order_ref in self._my_position:
            return self._my_position[order_ref]
        else:
            return None

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
                # 获取仓位
                pos = self.get_my_position(order_ref)

                # T+1规则下，需要判断开仓订单成交的后一天才可以设置止盈止损
                open_order = pos.get_open_order()
                if open_order.data.datetime.date(0) <= num2date(open_order.executed.dt).date():
                    continue

                mark_take_profit_t1 = pos.get_mark_take_profit_t1()
                if mark_take_profit_t1 is not None:
                    self.take_profit(order_ref, mark_take_profit_t1)
                    pos.clear_mark_take_profit_t1()

                mark_stop_loss_t1 = pos.get_mark_stop_loss_t1()
                if mark_stop_loss_t1 is not None:
                    self.stop_loss(order_ref, mark_stop_loss_t1)
                    pos.clear_mark_stop_loss_t1()

                self._my_position[order_ref] = pos

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

    def open_market(self, data, size = None, is_buy: bool = True, target_price: float = None, stop_price: float = None):
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

        self._my_position[order.ref] = SinglePosition(order, mark_take_profit=target_price, mark_stop_loss=stop_price)
        return order

    def open_limit(self, data, price, size = None, is_buy: bool = True, target_price: float = None, stop_price: float = None):
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

        self._my_position[order.ref] = SinglePosition(order, mark_take_profit=target_price, mark_stop_loss=stop_price)
        return order

    def open_break(self, data, break_price, size = None, is_buy: bool = True, target_price: float = None, stop_price: float = None):
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

        self._my_position[order.ref] = SinglePosition(order, mark_take_profit=target_price, mark_stop_loss=stop_price)
        return order

    def take_profit(self, order_ref, target_price):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"设置止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"设置止盈单但未找到开仓订单，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止盈单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止盈单
        close_order = pos.get_close_order()
        mark_close = pos.get_mark_close()
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
            pos.set_take_profit_order(take_profit_order)
        else:
            # 设置止盈目标，等待notify_order中取消完成后再创建
            pos.set_mark_take_profit(target_price)

        # 更新仓位
        self._my_position[order_ref] = pos

    def cancel_take_profit(self, order_ref):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"取消止盈单但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"取消止盈单但未找到开仓订单，开仓订单编号: {order_ref}")

        # 不再需要notify处理标记
        pos.clear_mark_take_profit()

        # 取消之前设置的止盈单
        take_profit_order = pos.get_take_profit_order()
        if take_profit_order is not None:
            self.cancel(take_profit_order)
            # 更新仓位
            self._my_position[order_ref] = pos
            return True
        else:
            # 更新仓位
            self._my_position[order_ref] = pos
            return False

    def stop_loss(self, order_ref, stop_price):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"设置止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"设置止损单但未找到开仓订单，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许下止损单
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许下止损单
        close_order = pos.get_close_order()
        mark_close = pos.get_mark_close()
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
            pos.set_stop_loss_order(stop_loss_order)
        else:
            # 设置止损目标，等待notify_order中取消完成后再创建
            pos.set_mark_stop_loss(stop_price)

        # 更新仓位
        self._my_position[order_ref] = pos

    def cancel_stop_loss(self, order_ref):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"取消止损单但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"取消止损单但未找到开仓订单，开仓订单编号: {order_ref}")

        # 不再需要notify处理标记
        pos.clear_mark_stop_loss()

        # 取消之前设置的止损单
        stop_loss_order = pos.get_stop_loss_order()
        if stop_loss_order is not None:
            self.cancel(stop_loss_order)
            # 更新仓位
            self._my_position[order_ref] = pos
            return True
        else:
            # 更新仓位
            self._my_position[order_ref] = pos
            return False

    def cancel_order(self, order_ref):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"取消开仓但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"取消开仓但未找到开仓订单，开仓订单编号: {order_ref}")

        self.cancel(order)

    def close_position(self, order_ref):
        # 获取仓位
        pos = self.get_my_position(order_ref)
        if not pos:
            raise ValueError(f"平仓但未找到仓位，开仓订单编号: {order_ref}")

        # 获取开仓订单
        order = pos.get_open_order()
        if order is None:
            raise ValueError(f"平仓但未找到开仓订单，开仓订单编号: {order_ref}")

        # 如果没有成交，不允许平仓
        if not order.status in [order.Completed]:
            raise ValueError(f"该开仓订单还未成交，开仓订单编号: {order_ref}")

        # 如果已进入平仓状态，不允许平仓
        close_order = pos.get_close_order()
        if close_order is not None:
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
            data = order.data  # 交易品种
            size = order.executed.size # 该仓位持有的数量，用作于平仓数量
            if order.isbuy():
                close_order = self.sell(data=data, size=size, exectype=bt.Order.Market)
            else:
                close_order = self.buy(data=data, size=size, exectype=bt.Order.Market)
            pos.set_close_order(close_order)
        else:
            # 设置平仓目标，等待notify_order中该仓位所有的止盈止损都取消后再创建
            pos.set_mark_close(True)

        # 更新仓位
        self._my_position[order_ref] = pos

    def _notify_order_type(self, order):
        if order.ref in self._my_position:
            # 开仓订单
            return "open", order.ref
        for order_ref in self._my_position:
            pos = self.get_my_position(order_ref)
            # 平仓订单
            close_order = pos.get_close_order()
            if close_order and close_order.ref == order.ref:
                return "close", order_ref
            # 止盈订单
            take_profit_order = pos.get_take_profit_order()
            if take_profit_order and take_profit_order.ref == order.ref:
                return "take_profit", order_ref
            # 止损订单
            stop_loss_order = pos.get_stop_loss_order()
            if stop_loss_order and stop_loss_order.ref == order.ref:
                return "stop_loss", order_ref
        # 未知订单
        return "unknown", None

    def notify_order(self, order):
        # 获取订单类型
        order_type, position_ref = self._notify_order_type(order)
        # 获取仓位
        pos = self.get_my_position(position_ref)
        if order.status in [order.Completed]:
            # 订单已完成
            if order_type == "open":
                # 非t+1
                if not self.broker.__dict__.get("_close_t1", False):
                    # 设置止盈
                    take_profit_price = pos.get_mark_take_profit()
                    if take_profit_price is not None:
                        self.take_profit(position_ref, take_profit_price)
                    # 设置止损
                    stop_loss_price = pos.get_mark_stop_loss()
                    if stop_loss_price is not None:
                        self.stop_loss(position_ref, stop_loss_price)
                else:
                    # t+1
                    # 设置止盈
                    take_profit_price = pos.get_mark_take_profit()
                    if take_profit_price is not None:
                        pos.clear_mark_take_profit()
                        pos.set_mark_take_profit_t1(take_profit_price)
                    # 设置止损
                    stop_loss_price = pos.get_mark_stop_loss()
                    if stop_loss_price is not None:
                        pos.clear_mark_stop_loss()
                        pos.set_mark_stop_loss_t1(stop_loss_price)

            elif order_type in ["close", "take_profit", "stop_loss"]:
                self.cancel_take_profit(position_ref)
                self.cancel_stop_loss(position_ref)
                pos.set_completed(order_type)

        elif order.status in [order.Partial]:
            # 订单部分成交
            raise ValueError(f"订单部分成交的场景，回测暂不支持，订单编号: {order.ref}")
        elif order.status in [order.Canceled]:
            # 订单被取消
            if order_type == "open":
                pos.set_cancelled()

            if order_type == "take_profit":
                pos.clear_take_profit_order()
                # 若有，设置新止盈
                take_profit_price = pos.get_mark_take_profit()
                if take_profit_price is not None:
                    self.take_profit(position_ref, take_profit_price)

            if order_type == "stop_loss":
                pos.clear_stop_loss_order()
                # 若有，设置新止损
                stop_loss_price = pos.get_mark_stop_loss()
                if stop_loss_price is not None:
                    self.stop_loss(position_ref, stop_loss_price)

            if order_type in ["take_profit", "stop_loss"]:
                # 若止盈止损单都被取消，且设置了平仓目标，则平仓
                take_profit_order = pos.get_take_profit_order()
                stop_loss_order = pos.get_stop_loss_order()
                if not take_profit_order and not stop_loss_order:
                    if pos.get_mark_close():
                        self.close_position(position_ref)

        elif order.status in [order.Expired]:
            # 订单已过期
            if order_type == "open":
                pos.set_cancelled()
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)
        elif order.status in [order.Margin, order.Rejected]:
            # 交易所拒绝了订单
            if order_type == "open":
                pos.set_cancelled()
            if order_type in ["take_profit", "stop_loss", "close"]:
                self.close_position(order.ref)

        # 更新仓位信息
        self._my_position[position_ref] = pos
