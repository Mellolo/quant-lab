import logging

class BacktestLogger:
    """
    回测日志记录器，用于在回测过程中记录带有行情时间的日志
    """
    def __init__(self, name='Backtest'):
        self.logger = logging.getLogger(name)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(message)s')#logging.Formatter('[%(asctime)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

    def log_with_time(self, data, message, source="", level=logging.INFO):
        """
        使用数据时间记录日志
        
        Args:
            data: backtrader数据对象
            message: 日志消息
            level: 日志级别
            source: 日志发生源
        """
        data_name = data._name or 'default'
        current_time = data.datetime.datetime(0)
        log_message = f"[{current_time}] [{data_name}] {message}"
        if source:
            log_message = f"[{source}] {log_message}"
        self.logger.log(level, log_message)

# 创建全局日志记录器实例
backtest_logger = BacktestLogger()

def log_backtest(data, message, source="", level=logging.INFO):
    """
    全局函数，用于在回测过程中记录带有行情时间的日志
    
    Args:
        data: backtrader数据对象
        message: 日志消息
        level: 日志级别
        source: 日志发生源
    """
    backtest_logger.log_with_time(data, message, source, level)