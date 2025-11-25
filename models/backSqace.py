from typing import Dict, List
import pandas as pd

from backtest.manual.backtest import ManualBacktestEngine

class BackSpaceStatus:
    def __init__(self, name="default",dt=None, cash=None, positions: List[Dict]=None):
        self.name = name
        self.dt = dt
        self.cash = cash
        self.positions = positions

    def to_dict(self):
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, data_dict: Dict):
        return cls(
            name=data_dict.get('name'),
            dt=data_dict.get('dt'),
            cash=data_dict.get('cash'),
            positions=data_dict.get('positions'),
        )

class BackSpace:
    def __init__(self, id=None, data: pd.DataFrame=None, status: BackSpaceStatus =None):
        self.id = id
        self.data = data
        self.status = status

    def to_dict(self):
        return {
            'id': self.id,
            'data': self.data.to_dict(),
            'status': self.status.to_dict()
        }


class BackSpaceConnect:
    def __init__(self, sid: str, space_id: int, engine: ManualBacktestEngine=None):
        self.sid = sid
        self.space_id = space_id
        self.engine = engine