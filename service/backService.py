import json
import pandas as pd
from dao.backSpace import BackSpaceRecord
from backtest.feeds.clean import data_clean
from backtest.manual.backtest import ManualBacktestEngine, ManualStrategyInfo
from dao.backSpace import BackSpaceRepository
from models.backSqace import BackSpace, BackSpaceStatus


def create_back_space_astock(df: pd.DataFrame) -> BackSpace:
    start_date = pd.to_datetime(df["datetime"].min()).date()
    end_date = pd.to_datetime(df["datetime"].max()).date()
    df = data_clean(df, start_date, end_date, [("09:30", "11:30"), ("13:00", "15:00")], "5m")
    status = BackSpaceStatus(dt=start_date, cash=1000000, positions=[])

    # 写入数据库
    repository = BackSpaceRepository()
    repository.create(BackSpaceRecord(data=df.to_json(), status=json.dumps(status.to_dict())))

    return BackSpace(data=df, status=status)

def get_back_space(space_id: int) -> BackSpace:
    repository = BackSpaceRepository()
    record = repository.get_by_id(space_id)
    data_df = pd.read_json(record.data)
    status = BackSpaceStatus.from_dict(json.loads(record.status))

    return BackSpace(data=data_df, status=status)

def create_back_engine_astock(back_space: BackSpace) -> ManualBacktestEngine:
    engine = ManualBacktestEngine(cash=back_space.status.cash, from_datetime=pd.to_datetime(back_space.status.dt))
    engine.add_data(back_space.data)
    engine.run()

    return engine

def update_back_space(space_id: int, status: BackSpaceStatus):
    repository = BackSpaceRepository()
    record = repository.get_by_id(space_id)
    record.status = status.to_dict()
    repository.update(record)