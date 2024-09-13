from accumulator.base_accumulator import BaseAccumulator
from accumulator.avg_accumulator import AvgAccumulator
from accumulator.max_accumulator import MaxAccumulator
from accumulator.sum_accumulator import SumAccumulator

class AccumulatorCreator():
    @classmethod
    def get_accumulator(cls, type: str) -> BaseAccumulator:
        match type:
            case "MAX":
                return MaxAccumulator()
            case "AVG":
                return AvgAccumulator()
            case "SUM":
                return SumAccumulator()
            case default:
                return AvgAccumulator()