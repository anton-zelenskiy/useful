import csv
from abc import ABC, abstractmethod
from typing import Any


class FileWriter(ABC):
    """Abstract base class for file writers."""

    @abstractmethod
    def write(self, file_path: str, data: list[dict[str, Any]]) -> None:
        """
        Write data to file.

        Args:
            file_path: Path to output file
            data: List of dictionaries to write
        """
        pass


class CSVWriter(FileWriter):
    """CSV file writer with standardized columns."""

    def __init__(self, fieldnames: list[str], encoding: str = 'utf-8') -> None:
        self.fieldnames = fieldnames
        self.encoding = encoding

    def write(self, file_path: str, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        with open(file_path, 'w', encoding=self.encoding, newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(data)
