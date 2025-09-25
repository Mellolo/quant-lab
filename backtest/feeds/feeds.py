import datetime
from backtrader.feeds import PandasData
import pandas as pd
from typing import List, Dict
from .clean import data_clean, data_clean_24

def data_feed_astock(dfs: Dict[str, pd.DataFrame], start_date: datetime.date, end_date: datetime.date, freq: str) -> List[PandasData]:
    processed_dfs = {}
    for symbol, df in dfs.items():
        processed_dfs[symbol] = data_clean(df, start_date, end_date, [("09:30", "11:30"), ("13:00", "15:00")], freq)

    # 检查所有DataFrame的索引是否完全一致
    if processed_dfs:
        keys = list(processed_dfs.keys())
        first_index = processed_dfs[keys[0]].index
        for key in keys[1:]:
            if not first_index.equals(processed_dfs[key].index):
                raise ValueError(f"DataFrame {key} 的索引与 DataFrame {keys[0]} 的索引不匹配")

    return [PandasData(dataname=df, fromdate=start_date, todate=end_date, name=symbol) for symbol, df in processed_dfs.items()]

def data_feed_crypto(dfs: Dict[str, pd.DataFrame], freq: str) -> List[PandasData]:
    processed_dfs = {}
    for symbol, df in dfs.items():
        processed_dfs[symbol] = data_clean_24(df, freq)

    # 检查所有DataFrame的索引是否完全一致
    if processed_dfs:
        keys = list(processed_dfs.keys())
        first_index = processed_dfs[keys[0]].index
        for key in keys[1:]:
            if not first_index.equals(processed_dfs[key].index):
                raise ValueError(f"DataFrame {key} 的索引与 DataFrame {keys[0]} 的索引不匹配")

    return [PandasData(dataname=df, name=symbol) for symbol, df in processed_dfs.items()]