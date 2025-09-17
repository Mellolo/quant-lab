import datetime
from backtrader.feeds import PandasData
import pandas as pd
from typing import List, Dict
from .util import _validate_datetime_index

def data_feed_astock(dfs: Dict[str, pd.DataFrame], start_date: datetime.date, end_date: datetime.date, freq: str) -> List[PandasData]:
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date) + datetime.timedelta(days=1)

    # 必需字段（注意第一列是索引列，不在columns中）
    required_fields = {'open', 'close', 'high', 'low', 'volume', 'datetime'}

    # 检查每个DataFrame是否包含必需字段
    for symbol, df in dfs.items():
        missing_fields = required_fields - set(df.columns)
        if missing_fields:
            raise ValueError(f"DataFrame {symbol} 缺少以下必需字段: {missing_fields}")

    # 处理每个DataFrame:转换datetime为datetime格式并设置为index
    processed_dfs = {}
    for symbol, df in dfs.items():
        # 复制DataFrame避免修改原始数据
        df_copy = df.copy()

        # 将datetime列转换为datetime格式
        df_copy['datetime'] = pd.to_datetime(df_copy['datetime'])

        # 设置datetime为索引
        df_copy.set_index('datetime', inplace=True)

        df_copy = df_copy.iloc[(start_datetime <= df_copy.index) & (df_copy.index < end_datetime), :]

        # 按索引排序
        df_copy.sort_index(inplace=True)
        _validate_datetime_index(df_copy.index, [("09:30", "11:30"), ("13:00", "15:00")], freq)

        processed_dfs[symbol] = df_copy

    # 检查所有DataFrame的索引是否完全一致
    if processed_dfs:
        keys = list(processed_dfs.keys())
        first_index = processed_dfs[keys[0]].index
        for key in keys[1:]:
            if not first_index.equals(processed_dfs[key].index):
                raise ValueError(f"DataFrame {key} 的索引与 DataFrame {keys[0]} 的索引不匹配")

    return [PandasData(dataname=df, fromdate=start_date, todate=end_date, name=symbol) for symbol, df in processed_dfs.items()]
