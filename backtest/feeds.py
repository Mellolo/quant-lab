import datetime
from backtrader.feeds import PandasData
import pandas as pd
from typing import List, Tuple, Union, Dict

def data_feed_astock(dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, freq: str) -> List[PandasData]:
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) + datetime.timedelta(days=1)

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

        df_copy = df_copy.iloc[(start_date <= df_copy.index) & (df_copy.index <= end_date), :]

        # 按索引排序
        df_copy.sort_index(inplace=True)
        validate_datetime_index(df_copy.index, [("09:30", "11:30"), ("13:00", "15:00")], freq)

        processed_dfs[symbol] = df_copy

    # 检查所有DataFrame的索引是否完全一致
    # if processed_dfs:
    #     keys = processed_dfs.keys()
    #     first_index = processed_dfs[keys[0]].index
    #     for i, df in enumerate(processed_df_list[1:], start=1):
    #         if not first_index.equals(df.index):
    #             raise ValueError(f"DataFrame {i} 的索引与 DataFrame 0 的索引不匹配")

    return [PandasData(dataname=df, fromdate=start_date, todate=end_date, symbol=symbol) for symbol, df in processed_dfs]


def validate_datetime_index(index: pd.Index, trading_periods: List[Tuple[str, str]], freq: str) -> bool:
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

    return True

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

