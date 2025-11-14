class BackSpaceStatus:
    def __init__(self, dt=None, positions=None):
        self.dt = dt
        self.positions = positions

    def to_dict(self):
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, data_dict):
        return cls(
            dt=data_dict.get('dt'),
            positions=data_dict.get('positions'),
        )