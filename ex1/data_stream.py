
from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    """A base Data stream class for streaming data."""
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.status = "active"

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return the result."""
        pass

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        """
        filter data_batch using criteria, and return the list of filtered data.
        """
        if not criteria:
            return data_batch
        return [data for data in data_batch if criteria in data]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """
        get the stats of data stream that is a dictionary of informations
        """
        return {
            "id": self.stream_id,
            "status": self.status,
            "type": self.__class__.__name__
        }


class SensorStream(DataStream):
    """A class for streaming the sensor readings."""
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.__sensor_report = 0
        self.__avg_t = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return the result."""
        try:
            if isinstance(data_batch, List) is False:
                raise Exception("Error data is not a List, data type "
                                + f"-> {type(data_batch)}")
            for data in data_batch:
                if isinstance(data, str) is False:
                    raise Exception("Error, invalid data type")
                data_s = data.split(":")
                if len(data_s) <= 1:
                    raise Exception(f"Error: this data '{data}' is not valid")
                float(data_s[1])
                self.__sensor_report += 1
                if data_s[0] == "temp":
                    self.__avg_t = float(data_s[1])
        except (Exception, ValueError) as e:
            print(e)
            return "Sensor analysis: 0 readings."
        else:
            return (f"Sensor analysis: {self.__sensor_report} "
                    + f"readings processed, avg temp: {self.__avg_t:.1f}°C")


class TransactionStream(DataStream):
    """A class for streaming Transactions operation."""
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.__buys = 0
        self.__sells = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return the result."""
        try:
            if isinstance(data_batch, List) is False:
                raise Exception("Error: data is not a List, data type ->"
                                + type(data_batch))
            for data in data_batch:
                if isinstance(data, str) is False:
                    raise Exception("Error: invalid data type!")
                data_s = data.split(":")
                if data_s[0] == "buy":
                    self.__buys += int(data_s[1])
                elif data_s[0] == "sell":
                    self.__sells += int(data_s[1])
                else:
                    raise Exception(f"Error: invalid data type '{data_s[0]}'")
        except (Exception, ValueError) as e:
            print("Error:", e)
            return "ransaction analysis: 0 operations."
        else:
            n_f = self.__buys - self.__sells
            return (f"ransaction analysis: {len(data_batch)} operations, net "
                    + f"flow: {f"+{n_f}" if n_f >= 0 else n_f} units")

    def filter_data(
            self,
            data_batch: List[Any],
            criteria: Optional[str] = None
            ) -> List[Any]:
        """
        filter data_batch using criteria, and return the list of filtered data.
        """
        if criteria == "large":
            return [
                item for item in data_batch
                if isinstance(item, str)
                and ":" in item
                and int(item.split(":")[1]) >= 100
            ]
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    """A class for streaming events batch."""
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.__events = 0
        self.__error = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return the result."""
        try:
            if isinstance(data_batch, List) is False:
                raise Exception("Error: data is not a List, data type ->"
                                + type(data_batch))
            for data in data_batch:
                if isinstance(data, str) is False:
                    raise Exception("Error, invalid data type!")
                self.__events += 1
                if data == "error":
                    self.__error += 1
        except Exception as error:
            print(error)
            return "Event analysis: 0 events"
        else:
            return (f"Event analysis: {self.__events} events,"
                    + f" {self.__error} error detected")


class StreamProcessor:
    """Handles multiple DataStream types polymorphically."""
    def __init__(self) -> None:
        self.streams = []

    def add_streams(self, data_streams: List[DataStream]) -> None:
        """adding List of streams to the stream processor."""
        for stream in data_streams:
            self.add_stream(stream)

    def add_stream(self, stream: DataStream) -> None:
        """adding stream to the stream processor."""
        if isinstance(stream, DataStream):
            self.streams.append(stream)
        else:
            raise Exception(
                f"'{stream.__class__.__name__}' is not stream")

    def process_all(self, batches: Dict[str, Any]) -> None:
        """process all the batches using matched stream."""
        for stream in self.streams:
            try:
                data = batches.get(stream.stream_id)
                if not data:
                    continue
                stream.process_batch(data)
                if isinstance(stream, SensorStream):
                    print(f"- Sensor data: {len(data)} readings processed")
                if isinstance(stream, TransactionStream):
                    print(f"- Transaction data: {len(data)}"
                          + " operations processed")
                if isinstance(stream, EventStream):
                    print(f"- Event data: {len(data)} events processed")
            except Exception as e:
                print(e)

    @staticmethod
    def process_stream_batch(stream: DataStream, data: List[Any]) -> None:
        """Process a batch of data using a polymorphic DataStream instance."""

        if not isinstance(stream, DataStream):
            print(f"[ERROR] Invalid stream type: {type(stream)}")
            return

        try:
            result = stream.process_batch(data)
            print(result)
        except Exception as e:
            print(f"[ERROR] Stream processing failed: {e}")


if __name__ == "__main__":

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: Environmental Data")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {str(sensor_data).replace("'", "")}")
    StreamProcessor.process_stream_batch(sensor, sensor_data)

    print("\nInitializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction.stream_id}, type: Financial Data")
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing event batch: {str(transaction_data).replace("'", "")}")
    StreamProcessor.process_stream_batch(transaction, transaction_data)

    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, type: System Events")
    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {str(event_data).replace("'", "")}")
    StreamProcessor.process_stream_batch(event, event_data)

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\nBatch 1 Results:")
    manager = StreamProcessor()
    data_streams = [
        SensorStream("SENSOR_001"),
        TransactionStream("TRANS_001"),
        EventStream("EVENT_001"),
    ]
    batches = {
        "SENSOR_001": ["temp:20.0", "temp:25.0"],
        "TRANS_001": ["buy:200", "sell:50", "buy:90", "sell:25"],
        "EVENT_001": ["login", "logout", "error"],
    }
    try:
        manager.add_streams(data_streams)
        manager.process_all(batches)
    except Exception as error:
        print("Error:", error)

    print("\nStream filtering active: High-priority data only")
    sensors = len(sensor.filter_data(batches["SENSOR_001"], "temp"))
    transactions = len(transaction.filter_data(batches["TRANS_001"], "large"))
    print(f"Filtered results: {sensors} critical sensor alerts,"
          + f" {transactions} large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")
