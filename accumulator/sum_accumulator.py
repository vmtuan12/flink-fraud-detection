from accumulator.base_accumulator import BaseAccumulator

class AvgAccumulator(BaseAccumulator):
    def __init__(self) -> None:
        super().__init__()
        self.sum = 0.00

    def add(self, value: float):
        self.sum += value

    def get_local_value(self):
        return self.sum

    def reset_local_value(self):
        self.sum = 0