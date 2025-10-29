import csv
import logging
from abc import ABC, abstractmethod
from typing import Any

from normalizers import (
    normalize_product_name,
    normalize_viscosity_grades,
    parse_volume_from_string,
    remove_russian_characters,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler())
logger.propagate = False


class FileReader(ABC):
    """Base class for file readers."""

    @abstractmethod
    def read(self, file_path: str) -> list[dict[str, Any]]:
        # TODO: return generator
        """
        Read data from file.

        Args:
            file_path: Path to input file

        returns: list of dictionaries read from file
        """
        pass


class CSVReader(FileReader):
    """CSV file reader."""

    def __init__(self, encoding: str = 'utf-8') -> None:
        self.encoding = encoding

    def read(self, file_path: str) -> list[dict[str, Any]]:
        data = []
        try:
            with open(file_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f)
                data = list(reader)
        except Exception as e:
            logger.error(f'Error reading CSV file {file_path}: {e}')

        return data


class ProductFilter(ABC):
    @abstractmethod
    def should_include(self, row: dict[str, Any]) -> bool:
        """
        Checks if row should be included based on filter criteria.
        """
        pass


class ValvolineProductFilter(ProductFilter):
    def __init__(self, match_word: str = 'valvoline') -> None:
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        """Check if row contains Valvoline products."""
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class RosneftProductFilter(ProductFilter):
    def __init__(self, match_word: str = 'rosneft') -> None:
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        """Check if row contains Rosneft products."""
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class ForsageProductFilter(ProductFilter):
    def __init__(self, match_word: str = 'forsage') -> None:
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class ProductProcessor(ABC):
    @abstractmethod
    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Process a single row and return normalized data.

        Args:
            row: Original row data

        Returns:
            Processed row data
        """
        pass


class ValvolineProductProcessor(ProductProcessor):
    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        original_name = row.get('name', '')

        normalized_name = normalize_product_name(original_name)
        normalized_name, volume_number, volume_unit = parse_volume_from_string(normalized_name)

        normalized_name = remove_russian_characters(normalized_name)
        normalized_name = normalize_viscosity_grades(normalized_name)
        normalized_name = normalized_name.strip()

        normalized_name = f'{normalized_name} {volume_number} {volume_unit}'.lower()

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': volume_number,
            'volume_unit': volume_unit,
        }


class RosneftProductProcessor(ProductProcessor):
    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        original_name = row.get('name', '')

        normalized_name = normalize_product_name(original_name)
        normalized_name, volume_number, volume_unit = parse_volume_from_string(normalized_name)

        normalized_name = remove_russian_characters(normalized_name)

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': volume_number,
            'volume_unit': volume_unit,
        }


class ForsageProductProcessor(ProductProcessor):
    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        original_name = row.get('name', '')

        normalized_name = normalize_product_name(original_name)
        normalized_name, volume_number, volume_unit = parse_volume_from_string(normalized_name)

        normalized_name = normalized_name.replace('forsage lubricants', 'forsage')

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': volume_number,
            'volume_unit': volume_unit,
        }


class CSVProductFilter:
    def __init__(
        self,
        reader: FileReader,
        filter_: ProductFilter,
        processor: ProductProcessor,
    ) -> None:
        """
        Initialize CSV product filter.

        Args:
            reader: File reader instance
            filter_: Product filter instance
            processor: Product processor instance
        """
        self.reader = reader
        self.filter = filter_
        self.processor = processor

    def filter_data(self, input_file: str) -> list[dict[str, Any]]:
        """
        Filter products and return filtered data.

        Args:
            input_file: Path to input CSV file

        Returns:
            List of filtered and processed data
        """
        all_data = self.reader.read(input_file)

        filtered_data = []
        for row in all_data:
            if self.filter.should_include(row):
                processed_row = self.processor.process_row(row)
                filtered_data.append(processed_row)

        return filtered_data


def filter_valvoline_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'valvoline'
) -> list[dict[str, Any]]:
    reader = CSVReader(encoding)
    filter_ = ValvolineProductFilter(match_word)
    processor = ValvolineProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)


def filter_rosneft_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'rosneft'
) -> list[dict[str, Any]]:
    reader = CSVReader(encoding)
    filter_ = RosneftProductFilter(match_word)
    processor = RosneftProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)


def filter_forsage_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'forsage'
) -> list[dict[str, Any]]:
    reader = CSVReader(encoding)
    filter_ = ForsageProductFilter(match_word)
    processor = ForsageProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)
