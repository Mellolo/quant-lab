import datetime
import pandas as pd
from typing import List, Tuple, Dict
from .merge import bar_merge, bar_merge_24
from .util import *

def data_clean(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date, trading_periods: List[Tuple[str, str]], freq: str) -> pd.DataFrame:
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date) + datetime.timedelta(days=1)

    # 必需字段（注意第一列是索引列，不在columns中）
    required_fields = {'open', 'close', 'high', 'low', 'volume', 'datetime'}

    # 检查每个DataFrame是否包含必需字段
    missing_fields = required_fields - set(df.columns)
    if missing_fields:
        raise ValueError(f"DataFrame缺少以下必需字段: {missing_fields}")

    # 复制DataFrame避免修改原始数据
    processed_df = df.copy()

    # 将datetime列转换为datetime格式
    processed_df['datetime'] = pd.to_datetime(processed_df['datetime'])

    # 设置datetime为索引
    processed_df.set_index('datetime', inplace=True, drop=False)

    processed_df = processed_df.loc[(start_datetime <= processed_df.index) & (processed_df.index < end_datetime), :]

    # 按索引排序
    processed_df.sort_index(inplace=True)
    _validate_datetime_index(processed_df.index, trading_periods, freq)

    return processed_df

def data_clean_with_merge(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date, trading_periods: List[Tuple[str, str]], from_freq: str, to_freq: str) -> pd.DataFrame:
    df = data_clean(df, start_date, end_date, trading_periods, from_freq)
    return bar_merge(df, trading_periods, from_freq, to_freq)

def data_clean_24(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    # 必需字段（注意第一列是索引列，不在columns中）
    required_fields = {'open', 'close', 'high', 'low', 'volume', 'datetime'}

    # 检查每个DataFrame是否包含必需字段
    missing_fields = required_fields - set(df.columns)
    if missing_fields:
        raise ValueError(f"DataFrame缺少以下必需字段: {missing_fields}")

    # 复制DataFrame避免修改原始数据
    processed_df = df.copy()

    # 将datetime列转换为datetime格式
    processed_df['datetime'] = pd.to_datetime(processed_df['datetime'])

    # 设置datetime为索引
    processed_df.set_index('datetime', inplace=True)

    # 按索引排序
    processed_df.sort_index(inplace=True)
    _validate_datetime_index_24(processed_df.index, freq)

    return processed_df

def data_clean_with_merge_24(df: pd.DataFrame, from_freq: str, to_freq: str) -> pd.DataFrame:
    df = data_clean_24(df, from_freq)
    return bar_merge_24(df, from_freq, to_freq)

def check_index_consistency(dfs: Dict[str, pd.DataFrame]):
    # 检查所有DataFrame的索引是否完全一致
    if dfs:
        keys = list(dfs.keys())
        first_index = dfs[keys[0]].index
        for key in keys[1:]:
            if not first_index.equals(dfs[key].index):
                raise ValueError(f"DataFrame {key} 的索引与 DataFrame {keys[0]} 的索引不匹配")
