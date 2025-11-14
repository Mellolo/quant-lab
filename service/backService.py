import json
import pandas as pd
from dao.backSpace import BackSpaceRecord
from backtest.feeds.clean import data_clean
from backtest.manual.backtest import ManualBacktestEngine
from dao.backSpace import BackSpaceRepository
from models.backSqace import BackSpaceStatus


def create_back_space_astock(df: pd.DataFrame):
    start_date = pd.to_datetime(df["datetime"].min()).date()
    end_date = pd.to_datetime(df["datetime"].max()).date()
    df = data_clean(df, start_date, end_date, [("09:30", "11:30"), ("13:00", "15:00")], "5m")

    record = BackSpaceRecord(data=df.to_json(), status="{}")
    repository = BackSpaceRepository()
    repository.create(record)

def get_back_space(space_id: int):
    repository = BackSpaceRepository()
    record = repository.get_by_id(space_id)
    data_df = pd.read_json(record.data)
    status = BackSpaceStatus.from_dict(json.loads(record.status))

    # 回测
    engine = ManualBacktestEngine(cash=100000)
    engine.add_data(data_df)
    engine.run()

    # 还原回测状态

    return data_df, status
