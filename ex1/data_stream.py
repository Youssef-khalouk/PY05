

from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    """A base Data stream class for streaming data."""
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.status = "active"

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        if not criteria:
            return data_batch
        return [data for data in data_batch if criteria in data]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "id": self.stream_id,
            "status": self.status,
            "type": self.__class__.__name__
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.__sensor_report = 0
        self.__avg_t = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if isinstance(data_batch, List) is False:
                raise Exception("Error data is not a List, data type "
                                + f"-> {type(data_batch)}")
            for data in data_batch:
                if isinstance(data, tuple) is False:
                    raise Exception("Error, invalid data type")
                if data[0] not in ["temp", "humidity", "pressure"]:
                    raise Exception("Error, invalid data type")
                float(data[1])
                self.__sensor_report += 1
                self.__avg_t.append(data[1])
        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.__sensor_report} readings"


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        pass


class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        pass


class StreamProcessor:

    def processing_batchs() -> None:
        pass

    def filter_data() -> None:
        pass

    def transformation() -> None:
        pass
