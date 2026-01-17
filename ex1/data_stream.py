

from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    """A base Data stream class for streaming data."""

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    @abstractmethod
    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    pass


class TransactionStream(DataStream):
    pass


class EventStream(DataStream):
    pass