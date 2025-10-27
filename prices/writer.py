"""
File readers and writers for different input/output formats.
Follows SOLID principles with separate interfaces and implementations.
"""

import csv
import json
from abc import ABC, abstractmethod
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List


class FileReader(ABC):
    """Abstract base class for file readers."""

    @abstractmethod
    def read(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read data from file.

        Args:
            file_path: Path to input file

        Returns:
            List of dictionaries read from file
        """
        pass


class CSVReader(FileReader):
    """CSV file reader."""

    def __init__(self, encoding: str = 'utf-8'):
        """
        Initialize CSV reader.

        Args:
            encoding: File encoding
        """
        self.encoding = encoding

    def read(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read CSV file.

        Args:
            file_path: Path to CSV file

        Returns:
            List of dictionaries from CSV
        """
        data = []
        try:
            with open(file_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f)
                data = list(reader)
            print(f'Successfully read {len(data)} records from {file_path}')
        except Exception as e:
            print(f'Error reading CSV file {file_path}: {e}')

        return data


class FileWriter(ABC):
    """Abstract base class for file writers."""

    @abstractmethod
    def write(self, file_path: str, data: List[Dict[str, Any]]) -> None:
        """
        Write data to file.

        Args:
            file_path: Path to output file
            data: List of dictionaries to write
        """
        pass


class CSVWriter(FileWriter):
    """CSV file writer with standardized columns."""

    def __init__(self, fieldnames: List[str] = None):
        """
        Initialize CSV writer with fieldnames.

        Args:
            fieldnames: List of column names for CSV output
        """
        self.fieldnames = fieldnames or [
            'original_name',
            'normalized_name',
            'volume',
            'volume_unit',
            'package_count',
            'price',
            'price_total',
        ]

    def write(self, file_path: str, data: List[Dict[str, Any]]) -> None:
        """
        Write products to CSV file with standardized columns.

        Args:
            file_path: Path to output CSV file
            data: List of product dictionaries
        """
        if not data:
            print(f'No data to write to {file_path}')
            return

        # Process data for CSV output
        processed_data = self._process_data(data)

        # Write to CSV
        with open(file_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)

        print(f'Successfully wrote {len(processed_data)} records to {file_path}')

    def _process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw data for CSV output.

        Args:
            data: Raw product data

        Returns:
            Processed data ready for CSV writing
        """
        processed_data = []

        for item in data:
            # Parse package information
            package_count, package_volume, package_unit = self._parse_package_info(
                item.get('package', '')
            )

            # Parse price as Decimal
            price_str = item.get('price', '')
            try:
                price = Decimal(str(price_str)) if price_str else Decimal('0')
            except (InvalidOperation, ValueError):
                price = Decimal('0')

            # Calculate total price: price_per_liter * package_volume * package_count
            try:
                package_volume_decimal = Decimal(package_volume) if package_volume else Decimal('0')
                package_count_int = int(package_count) if package_count else 1
                price_total = price * package_volume_decimal * package_count_int
            except (InvalidOperation, ValueError):
                price_total = Decimal('0')

            processed_item = {
                'original_name': item.get('original_name', ''),
                'normalized_name': item.get('normalized_name', ''),
                'volume': package_volume,
                'volume_unit': package_unit,
                'package_count': package_count,
                'price': f'{price:.2f}',
                'price_total': f'{price_total:.2f}',
            }
            processed_data.append(processed_item)

        return processed_data

    def _parse_package_info(self, package_str: str) -> tuple[int, str, str]:
        """
        Parse package information from strings like:
        - "30 L" -> (1, "30", "L")
        - "6 x 5 L" -> (6, "5", "L")
        - "12 x 500 ML" -> (12, "500", "ML")

        Args:
            package_str: Package string to parse

        Returns:
            Tuple of (package_count, package_volume, package_unit)
        """
        import re

        if not package_str:
            return 1, '', ''

        package_str = str(package_str).strip().upper()

        # Pattern 1: "6 x 5 L" or "12 x 500 ML" (multi-package)
        multi_pattern = r'(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(L|ML)'
        multi_match = re.search(multi_pattern, package_str, re.IGNORECASE)
        if multi_match:
            count = int(multi_match.group(1))
            volume = multi_match.group(2)
            unit = multi_match.group(3).upper()
            return count, volume, unit

        # Pattern 2: "30 L" (single package)
        single_pattern = r'(\d+(?:\.\d+)?)\s*(L|ML)'
        single_match = re.search(single_pattern, package_str, re.IGNORECASE)
        if single_match:
            volume = single_match.group(1)
            unit = single_match.group(2).upper()
            return 1, volume, unit

        # If no pattern matches, return default
        return 1, '', ''


class ReaderFactory:
    """Factory class for creating appropriate readers."""

    @staticmethod
    def create_reader(file_type: str) -> FileReader:
        """
        Create appropriate reader based on file type.

        Args:
            file_type: Type of file ('csv', 'json')

        Returns:
            Appropriate reader instance

        Raises:
            ValueError: If file type is not supported
        """
        readers = {
            'csv': CSVReader,
        }

        if file_type.lower() not in readers:
            raise ValueError(f'Unsupported file type: {file_type}')

        return readers[file_type.lower()]()

    @staticmethod
    def create_csv_reader(encoding: str = 'utf-8') -> CSVReader:
        """
        Create CSV reader with custom encoding.

        Args:
            encoding: File encoding

        Returns:
            CSVReader instance
        """
        return CSVReader(encoding)


class WriterFactory:
    """Factory class for creating appropriate writers."""

    @staticmethod
    def create_writer(file_type: str) -> FileWriter:
        """
        Create appropriate writer based on file type.

        Args:
            file_type: Type of file ('csv', 'json', 'excel')

        Returns:
            Appropriate writer instance

        Raises:
            ValueError: If file type is not supported
        """
        writers = {
            'csv': CSVWriter,
        }

        if file_type.lower() not in writers:
            raise ValueError(f'Unsupported file type: {file_type}')

        return writers[file_type.lower()]()

    @staticmethod
    def create_csv_writer(fieldnames: List[str] = None) -> CSVWriter:
        """
        Create CSV writer with custom fieldnames.

        Args:
            fieldnames: Custom column names

        Returns:
            CSVWriter instance
        """
        return CSVWriter(fieldnames)
