from .merge import bar_merge
import os
import pandas as pd

def test_bar_merge_24():
    assert False


def test_bar_merge():
    # 读取数据
    file_path = os.path.join("test", "000001.XSHE.csv")
    df = pd.read_csv(file_path)
    # 将datetime列转换为datetime格式
    df['datetime'] = pd.to_datetime(df['datetime'])
    # 设置datetime为索引
    df.set_index('datetime', inplace=True)
    processed_df = bar_merge(df, [("09:30", "11:30"), ("13:00", "15:00")], "5m", "15m")
    print(processed_df)
