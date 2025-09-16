import pandas as pd


def data_check(df: pd.DataFrame):
    """
    检查数据框中是否包含必要的列：open, close, high, low, volume, money, symbol, datetime
    
    Args:
        df (pd.DataFrame): 要检查的数据框
        
    Raises:
        ValueError: 如果缺少任何必需的列
    """
    required_columns = ['open', 'close', 'high', 'low', 'volume', 'money', 'symbol', 'datetime']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"数据框缺少以下必需的列: {missing_columns}")