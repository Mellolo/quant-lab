from typing import Dict
import pandas as pd

class BackSpaceStatus:
    def __init__(self, dt=None, cash=None, positions=None):
        self.dt = dt
        self.cash = cash
        self.positions = positions

    def to_dict(self):
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, data_dict: Dict):
        return cls(
            dt=data_dict.get('dt'),
            cash=data_dict.get('cash'),
            positions=data_dict.get('positions'),
        )

class BackSpace:
    def __init__(self, data: pd.DataFrame=None, status: BackSpaceStatus =None):
        self.data = data
        self.status = status