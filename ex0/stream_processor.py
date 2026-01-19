
from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """A class for data processor."""
    def __init__(self):
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


class NumericProcessor(DataProcessor):
    """A class for Numeric processor."""
    sum_data: int = 0
    avg_data: Optional[float] = None

    def process(self, data: Any) -> str:
        """process the data that given as a list."""
        if self.validate(data) is True:
            NumericProcessor.sum_data += sum(data)
            if NumericProcessor.avg_data is None:
                NumericProcessor.avg_data = sum(data) / len(data)
            else:
                NumericProcessor.avg_data = (
                    (
                        NumericProcessor.avg_data
                        + (sum(data)) / len(data)
                    ) / 2
                    )
            return (f"Processed {len(data)} numeric values, sum={sum(data)},"
                    + f"vg={sum(data) / len(data)}")
        else:
            return "Error: data was not validate, please verify your input"

    def validate(self, data: Any) -> bool:
        """Validate the data that passed in the process."""
        try:
            if isinstance(data, list) is False:
                raise Exception(
                    "data is not list, data type is "
                    + f"'{type(data)}'"
                    )
            if len(data) == 0:
                raise Exception("data is empty!")
            for num in data:
                int(num)
        except (ValueError, Exception) as error:
            print("Error:", error)
            return False
        else:
            return True

    @classmethod
    def get_data(cls) -> Dict[str, Union[int, float]]:
        return {"total sum": cls.sum_data, "total avg": cls.avg_data}


class TextProcessor(DataProcessor):
    """A class for test processor."""

    def process(self, data: Any) -> str:
        """process the data that given as a string."""
        if self.validate(data) is True:
            return (f"Processed text: {len(data)} characters,"
                    + f" {len(data.split())} words")
        else:
            return "Error: data was not validate, please verify you input"

    def validate(self, data: Any) -> bool:
        """Validate the data that passed in the process."""
        try:
            if isinstance(data, str) is False:
                raise Exception(
                    "data is not string, data type is "
                    + f"'{type(data)}'"
                    )
            if len(data) == 0:
                raise Exception("data is empty!")
        except (Exception) as error:
            print("Error:", error)
            return False
        else:
            return True


class LogProcessor(DataProcessor):
    """A class for log processor."""

    def process(self, data: Any) -> str:
        if self.validate(data) is True:
            log = data.split(":")
        if (log[0] == "ERROR"):
            return (f"[ALERT] {log[0]} level detected:{log[1]}")
        if (log[0] == "INFO"):
            return (f"[INFO] {log[0]} level detected:{log[1]}")
        return "Error: data was not validate, please verify your input"

    def validate(self, data: Any) -> bool:
        try:
            if (isinstance(data, str) is False):
                raise Exception("Error data is not a log_str, data type "
                                + f"-> {type(data)}")
            if (len(data) == 0):
                raise Exception("Error data is empty")
            if (len(data.split(":")) == 0):
                raise Exception("Error data is not a log_str")
        except (Exception) as e:
            print(e)
            return False
        else:
            return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric_processor = NumericProcessor()
    data: List[int] = [n for n in range(1, 6)]
    print("Processing data:", data)
    string = numeric_processor.process(data)
    if numeric_processor.validate(data) is True:
        print("Validation: Numeric data verified")
    print(numeric_processor.format_output(string))

    print("\nInitializing Text Processor...")
    text_processor = TextProcessor()
    data = "Hello Nexus World"
    print(f"Processing data: \"{data}\"")
    string = text_processor.process(data)
    if text_processor.validate(data) is True:
        print("Validation: text data verified")
    print(text_processor.format_output(string))

    print("\nInitializing Text Processor...")
    log_processor = LogProcessor()
    data = "ERROR: Connection timeout"
    print(f"Processing data: \"{data}\"")
    string = log_processor.process(data)
    if log_processor.validate(data) is True:
        print("Validation: log entry verified")
    print(log_processor.format_output(string))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    types = [NumericProcessor(), TextProcessor(), LogProcessor()]
    datas = [[2, 2, 2], "Hello World", "INFO: System ready"]
    for i in range(3):
        print(f"Result {i + 1}: {types[i].process(datas[i])}")

    print("\nFoundation systems online. Nexus redy for advanced streams.")
