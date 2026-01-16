
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
        return (f"the output is : {result}")


class NumericProcessor(DataProcessor):
    """A class for Numeric processor."""

    def process(self, data: list[int]) -> str:
        """process the data that given as a list."""
        if (self.validate(data) is True):
            return (f"Processed {len(data)} numeric values, sum={sum(data)}, vg={sum(data) / len(data)}")
        else:
            return "Error: data was not validate, please verify your input"

    def validate(self, data: list[int]) -> bool:
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


class TextProcessor(DataProcessor):
    """A class for test processor."""
    def __init__(self, text: str) -> None:
        self.text = text

    def process(self, data: Any) -> str:
        pass

    def validate(self, data: Any) -> bool:
        pass


class LogProcessor(DataProcessor):
    """A class for log processor."""
    def __init__(self, log: str) -> None:
        self.log = log

    def process(self, data: Any) -> str:
        pass

    def validate(self, data: Any) -> bool:
        pass


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric_processor = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print("Processing data:", data)
    str = numeric_processor.process(data)
    if numeric_processor.validate(data) is True:
        print("Validation: Numeric data verified")
    print(str)
