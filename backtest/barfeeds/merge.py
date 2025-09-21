import pandas as pd
from typing import List, Tuple, Dict

from anaconda_project.internal.conda_api import result

from .util import *

def _get_freq_change(from_freq: str, to_freq: str) -> int:
    # 验证输入的bar频率合并变更是否合法
    from_freq_td = pd.Timedelta(from_freq)
    to_freq_td = pd.Timedelta(to_freq)
    if to_freq_td % from_freq_td != pd.Timedelta(0):
        raise ValueError(f"输入的频率转换无效，不是倍数关系({from_freq}->{to_freq})")
    freq_times = to_freq_td // from_freq_td
    if freq_times <= 0:
        raise ValueError(f"输入的频率转换无效，倍数不合理({from_freq}->{to_freq})")
    return freq_times

def bar_merge_24(df: pd.DataFrame, from_freq: str, to_freq: str) -> pd.DataFrame:
    # 首先验证输入的行情是否合法
    _validate_datetime_index_24(df.index, from_freq)

    # 验证输入的bar频率合并变更是否合法
    freq_times = _get_freq_change(from_freq, to_freq)
    if freq_times == 1:
        # 如果频率不变，则直接返回
        return df.copy()

    # 首先获取索引中实际存在的日期并去重
    unique_dates = sorted(set([d.date().strftime('%Y-%m-%d') for d in df.index]))

    expected_index = _generate_trading_time_index_24(unique_dates, to_freq)
    # 行情合并
    from_freq_td = pd.Timedelta(from_freq)
    result_list = []
    for expected_index_time in expected_index:
        df_to_merge = df[(df.index <= expected_index_time) & (df.index > expected_index_time - from_freq_td * freq_times), :]
        if df_to_merge.empty:
            continue
        # 收集合并后的结果
        merged_data = _bar_merge(df_to_merge)
        merged_df = pd.DataFrame([merged_data], index=[expected_index_time])
        result_list.append(merged_df)

    # 最后一次性合并所有结果
    if result_list:
        processed_df = pd.concat(result_list)
    else:
        processed_df = pd.DataFrame()
    processed_df.sort_index(inplace=True)

    return processed_df

def bar_merge(df: pd.DataFrame, trading_periods: List[Tuple[str, str]], from_freq: str, to_freq: str) -> pd.DataFrame:
    # 首先验证输入的行情是否合法
    _validate_datetime_index(df.index, trading_periods, from_freq)

    # 验证输入的bar频率合并变更是否合法
    freq_times = _get_freq_change(from_freq, to_freq)
    if freq_times == 1:
        # 如果频率不变，则直接返回
        return df.copy()

    # 首先获取索引中实际存在的日期并去重
    unique_dates = sorted(set([d.date().strftime('%Y-%m-%d') for d in df.index]))
    # 创建完整的预期时间序列
    expected_index = _generate_trading_time_index(unique_dates, trading_periods, to_freq)

    # 行情合并
    from_freq_td = pd.Timedelta(from_freq)
    result_list = []  # 收集所有要合并的DataFrame
    for expected_index_time in expected_index:
        df_to_merge = df.loc[(df.index <= expected_index_time) & (df.index > expected_index_time - from_freq_td * freq_times), :]
        if df_to_merge.empty:
            continue
        # 收集合并后的结果
        merged_data = _bar_merge(df_to_merge)
        merged_df = pd.DataFrame([merged_data], index=[expected_index_time])
        result_list.append(merged_df)

    # 最后一次性合并所有结果
    if result_list:
        processed_df = pd.concat(result_list)
    else:
        processed_df = pd.DataFrame()
    processed_df.sort_index(inplace=True)

    return processed_df


def _bar_merge(df: pd.DataFrame) -> Dict:
    if df.empty:
        # 如果输入为空，返回空的DataFrame
        return pd.DataFrame()

    # 创建一个新的DataFrame来存储合并结果
    merged_data = {}

    # 对于OHLC数据，使用特定的合并逻辑
    for col in df.columns:
        if col == 'open':
            # 开盘价取第一行的开盘价
            merged_data[col] = df[col].iloc[0]
        elif col == 'high':
            # 最高价取所有行中的最高价
            merged_data[col] = df[col].max()
        elif col == 'low':
            # 最低价取所有行中的最低价
            merged_data[col] = df[col].min()
        elif col == 'close':
            # 收盘价取最后一行的收盘价
            merged_data[col] = df[col].iloc[-1]
        elif col == 'volume':
            # 成交量取所有行的成交量之和
            merged_data[col] = df[col].sum()
        elif col == 'money':
            # 成交额取所有行的成交额之和
            merged_data[col] = df[col].sum()
        else:
            # 其他列取最后一行的值
            merged_data[col] = df[col].iloc[-1]

    return merged_data
