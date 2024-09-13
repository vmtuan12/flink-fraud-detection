from abc import ABC, abstractmethod

class Accumulator(ABC):
    @abstractmethod
    def add(value):
        pass

    @abstractmethod
    def get_local_value(value):
        pass

    @abstractmethod
    def reset_local_value(value):
        pass