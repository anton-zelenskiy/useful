import csv
from abc import ABC, abstractmethod
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List


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
            package_count = item.get('package_count', 1)
            volume = item.get('volume', '')
            volume_unit = item.get('volume_unit', '')

            # Parse price as Decimal
            price_str = item.get('price', '')
            try:
                price = Decimal(str(price_str)) if price_str else Decimal('0')
            except (InvalidOperation, ValueError):
                price = Decimal('0')

            # Calculate total price: price_per_liter * package_volume * package_count
            try:
                package_volume_decimal = Decimal(volume) if volume else Decimal('0')
                package_count_int = int(package_count) if package_count else 1
                price_total = price * package_volume_decimal * package_count_int
            except (InvalidOperation, ValueError):
                price_total = Decimal('0')

            processed_item = {
                'original_name': item.get('original_name', ''),
                'normalized_name': item.get('normalized_name', ''),
                'volume': volume,
                'volume_unit': volume_unit,
                'package_count': package_count,
                'price': f'{price:.2f}',
                'price_total': f'{price_total:.2f}',
            }
            processed_data.append(processed_item)

        return processed_data



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
