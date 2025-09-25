import pandas as pd
from typing import List, Tuple, Union

__all__ = [
    '_validate_datetime_index',
    '_generate_trading_time_index',
    '_validate_datetime_index_24',
    '_generate_trading_time_index_24'
]

def _validate_datetime_index_24(index: pd.Index, freq: str):
    if len(index) <= 1:
        return
    # 将频率转换为pandas的Timedelta对象
    freq_td = pd.Timedelta(freq)
    
    # 计算实际的频率间隔
    actual_diffs = pd.Series(index).diff().dropna()
    
    # 检查所有间隔是否等于期望频率
    if not (actual_diffs == freq_td).all():
        # 找出不符合预期频率的位置
        mismatched = actual_diffs[actual_diffs != freq_td]
        raise ValueError(f"数据索引频率无效。期望频率: {freq}, 发现不匹配项: {mismatched.to_dict()}")

def _generate_trading_time_index_24(
        dates: Union[List[str], List[pd.Timestamp]],
        freq: str
) -> pd.DatetimeIndex:
    # 创建完整的预期时间序列
    expected_index = pd.DatetimeIndex([])
    for current_date in dates:
        # 为每一天创建交易时间段
        start_dt = pd.Timestamp(f"{current_date} 00:00")
        end_dt = start_dt + pd.Timedelta(days=1)
        # 解析行情频率
        freq_td = pd.Timedelta(freq)

        # 手动按频率生成时间点
        times = []
        current_time = start_dt
        while current_time < end_dt:
            times.append(current_time)
            current_time += freq_td

        if current_time > end_dt:
            raise ValueError(f"行情频率{freq}不符合24小时交易时段要求")

        # 转换为 DatetimeIndex 并返回，根据项目规范返回Series类型
        return pd.DatetimeIndex(times)

    expected_index = expected_index.sort_values()
    return expected_index

def _validate_datetime_index(index: pd.Index, trading_periods: List[Tuple[str, str]], freq: str):
    # 首先获取索引中实际存在的日期并去重
    unique_dates = sorted(set([d.date().strftime('%Y-%m-%d') for d in index]))
    # 创建完整的预期时间序列
    expected_index = _generate_trading_time_index(unique_dates, trading_periods, freq)

    # 检查实际索引是否包含所有预期的时间点
    if not index.equals(expected_index):
        raise ValueError(f"数据索引在{freq}频率下的当前交易时段{trading_periods}无效，请检查数据")

def _generate_trading_time_index(
        dates: Union[List[str], List[pd.Timestamp]],
        trading_periods: List[Tuple[str, str]],
        freq: str
) -> pd.DatetimeIndex:
    # 创建完整的预期时间序列
    expected_index = pd.DatetimeIndex([])
    for current_date in dates:
        # 为每一天创建交易时间段
        for trading_period in trading_periods:
            datetime_index = _generate_trading_time_index_for_period(current_date, trading_period, freq)
            expected_index = expected_index.union(datetime_index)
    expected_index = expected_index.sort_values()
    return expected_index

def _generate_trading_time_index_for_period(
        date: Union[str, pd.Timestamp],
        trading_period: Tuple[str, str],
        freq: str
) -> pd.DatetimeIndex:
    # 构造完整的开始和结束时间戳
    start_time, end_time = trading_period
    start_dt = pd.Timestamp(f"{date} {start_time}")
    end_dt = pd.Timestamp(f"{date} {end_time}")
    # 解析行情频率
    freq_td = pd.Timedelta(freq)

    # 手动按频率生成时间点
    times = []
    current_time = start_dt
    while current_time < end_dt:
        current_time += freq_td
        times.append(current_time)

    if current_time > end_dt:
        raise ValueError(f"行情频率{freq}不符合交易时间段要求{start_time}-{end_time}")

    # 转换为 DatetimeIndex 并返回，根据项目规范返回Series类型
    return pd.DatetimeIndex(times)