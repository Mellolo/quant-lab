import pandas as pd
from typing import List, Tuple, Union


def _validate_datetime_index_24(index: pd.Index, freq: str):
    pass

def _validate_datetime_index(index: pd.Index, trading_periods: List[Tuple[str, str]], freq: str):
    # 生成预期的完整时间序列
    # 获取索引中实际存在的日期并去重
    unique_dates = sorted(set([d.date().strftime('%Y-%m-%d') for d in index]))
    # 创建完整的预期时间序列
    expected_index = pd.DatetimeIndex([])
    for current_date in unique_dates:
        # 为每一天创建交易时间段
        for trading_period in trading_periods:
            datetime_index = _generate_trading_times(current_date, trading_period, freq)
            expected_index = expected_index.union(datetime_index)
    expected_index = expected_index.sort_values()

    # 检查实际索引是否包含所有预期的时间点
    if not index.equals(expected_index):
        raise ValueError(f"数据索引在{freq}频率下的当前交易时段{trading_periods}无效，请检查数据")

def _generate_trading_times(
        date: Union[str, pd.Timestamp],
        trading_period: Tuple[str, str],
        frequency: str
) -> pd.DatetimeIndex:
    # 构造完整的开始和结束时间戳
    start_time, end_time = trading_period
    start_dt = pd.Timestamp(f"{date} {start_time}")
    end_dt = pd.Timestamp(f"{date} {end_time}")
    # 解析行情频率
    freq_td = pd.Timedelta(frequency)

    # 手动按频率生成时间点
    times = []
    current_time = start_dt
    while current_time < end_dt:
        current_time += freq_td
        times.append(current_time)

    if current_time > end_dt:
        raise ValueError(f"行情频率{frequency}不符合交易时间段要求{start_time}-{end_time}")

    # 转换为 DatetimeIndex 并返回，根据项目规范返回Series类型
    return pd.DatetimeIndex(times)