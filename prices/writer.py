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

    def __init__(self, fieldnames: list[str]) -> None:
        """
        Initialize CSV writer with fieldnames.

        Args:
            fieldnames: List of column names for CSV output
        """
        self.fieldnames = fieldnames

    def write(self, file_path: str, data: list[dict[str, Any]]) -> None:
        """
        Write products to CSV file with standardized columns.

        Args:
            file_path: Path to output CSV file
            data: List of product dictionaries
            skip_processing: If True, skip data processing (data is already processed)
        """
        if not data:
            print(f'No data to write to {file_path}')
            return

        # Write to CSV
        with open(file_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f'Successfully wrote {len(data)} records to {file_path}')
