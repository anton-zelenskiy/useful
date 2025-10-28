import csv
import re
from abc import ABC, abstractmethod
from typing import Any

from constants import VOLUME_MAP


class FileReader(ABC):
    """Abstract base class for file readers."""

    @abstractmethod
    def read(self, file_path: str) -> list[dict[str, Any]]:
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

    def __init__(self, encoding: str = 'utf-8') -> None:
        """
        Initialize CSV reader.

        Args:
            encoding: File encoding
        """
        self.encoding = encoding

    def read(self, file_path: str) -> list[dict[str, Any]]:
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


class ProductFilter(ABC):
    """Abstract base class for product filters."""

    @abstractmethod
    def should_include(self, row: dict[str, Any]) -> bool:
        """
        Check if row should be included based on filter criteria.

        Args:
            row: Row data dictionary

        Returns:
            True if row should be included
        """
        pass


class ValvolineProductFilter(ProductFilter):
    """Filter for Valvoline products."""

    def __init__(self, match_word: str = 'valvoline') -> None:
        """
        Initialize Valvoline filter.

        Args:
            match_word: Word to match in product name or brand
        """
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        """Check if row contains Valvoline products."""
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class RosneftProductFilter(ProductFilter):
    """Filter for Rosneft products."""

    def __init__(self, match_word: str = 'rosneft') -> None:
        """
        Initialize Rosneft filter.

        Args:
            match_word: Word to match in product name or brand
        """
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        """Check if row contains Rosneft products."""
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class ForsageProductFilter(ProductFilter):
    """Filter for Forsage products."""

    def __init__(self, match_word: str = 'forsage') -> None:
        """
        Initialize Forsage filter.

        Args:
            match_word: Word to match in product name or brand
        """
        self.match_word = match_word.lower()

    def should_include(self, row: dict[str, Any]) -> bool:
        """Check if row contains Forsage products."""
        name = str(row.get('name', '')).lower()
        brand = str(row.get('brand', '')).lower()
        return self.match_word in name or self.match_word in brand


class ProductProcessor(ABC):
    """Abstract base class for product processors."""

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
    """Processor for Valvoline products using the old normalization logic."""

    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Process Valvoline product row."""
        original_name = row.get('name', '')
        normalized_name, volume_number, volume_unit = normalize_product_name(original_name)

        normalized_name = remove_russian_characters(normalized_name)
        normalized_name = normalize_viscosity_grades(normalized_name)

        normalized_name = f'{normalized_name} {volume_number} {volume_unit}'.lower()

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': volume_number,
            'volume_unit': volume_unit,
        }


class RosneftProductProcessor(ProductProcessor):
    """Processor for Rosneft products with simple normalization."""

    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Process Rosneft product row."""
        original_name = row.get('name', '')

        # Simple normalization for Rosneft
        normalized_name = str(original_name).strip().lower()
        normalized_name = re.sub(r'\s+', ' ', normalized_name).strip()

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': '',
            'volume_unit': '',
        }


class ForsageProductProcessor(ProductProcessor):
    """Processor for Forsage products with simple normalization."""

    def process_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Process Forsage product row."""
        original_name = row.get('name', '')

        normalized_name, volume_number, volume_unit = normalize_product_name(original_name)

        return {
            'original_name': original_name,
            'normalized_name': normalized_name,
            'volume': volume_number,
            'volume_unit': volume_unit,
        }


class CSVProductFilter:
    """Main class for filtering CSV products."""

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
            writer: File writer instance
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
        # Read all data
        all_data = self.reader.read(input_file)

        # Filter and process data
        filtered_data = []
        for row in all_data:
            if self.filter.should_include(row):
                processed_row = self.processor.process_row(row)
                filtered_data.append(processed_row)

        print(f'First 10 rows of filtered data: {filtered_data[:10]}')

        return filtered_data


def remove_duplicate_words(text: str) -> str:
    """
    Remove duplicate words from text while preserving order.

    Args:
        text: Input text

    Returns:
        Text with duplicate words removed
    """
    if not text:
        return ''

    words = text.split()
    seen_words = set()
    unique_words = []

    for word in words:
        word_lower = word.lower()
        if word_lower not in seen_words:
            seen_words.add(word_lower)
            unique_words.append(word)

    return ' '.join(unique_words)


def normalize_viscosity_grades(text: str) -> str:
    """
    Remove hyphens from viscosity grades (e.g., "75W-80" -> "75W80", "5W-40" -> "5W40").

    Args:
        text: Input text

    Returns:
        Text with viscosity grade hyphens removed
    """
    if not text:
        return ''

    # Remove hyphens from viscosity grades pattern: digits + W + hyphen + digits
    normalized = re.sub(r'(\d+w)-(\d+)', r'\1\2', text)

    return normalized


def parse_volume_from_string(text: str) -> tuple[str, str, str]:
    """
    Parse volume information from product name string.

    Args:
        text: Product name string

    Returns:
        Tuple of (cleaned_name, volume_number, volume_unit)
        - cleaned_name: Name with volume removed and cleaned
        - volume_number: Extracted volume number (e.g., "1", "500", "4.5")
        - volume_unit: Extracted volume unit (e.g., "l", "ml", "kg")
    """
    if not text:
        return '', '', ''

    text = str(text).strip()
    volume_number = ''
    volume_unit = ''

    end_patterns = [
        r'\s+(\d+(?:\.\d+)?)\s*([а-яё]+)\.?\s*$',  # " 4 л"
        r'(\d+(?:\.\d+)?)\s*([а-яё]+)\.?\s*$',  # "4л."
    ]

    for pattern in end_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match and not volume_number:  # Only if we haven't found volume yet
            volume_number = match.group(1)
            volume_unit_raw = match.group(2).lower()
            if volume_unit_raw in VOLUME_MAP:
                volume_unit = VOLUME_MAP[volume_unit_raw]
                text = re.sub(pattern, '', text, flags=re.IGNORECASE)
                break

    return text, volume_number, volume_unit


def remove_russian_characters(text: str) -> str:
    return re.sub(r'[а-яё]', '', text, flags=re.IGNORECASE)

def normalize_product_name(name: str) -> tuple[str, str, str]:
    if not name:
        return '', '', ''

    # Step 1: Apply common normalization first
    name = str(name).strip()

    # Remove digits from the end (for cases like "866904")
    # This handles cases where there are product codes at the end
    name = re.sub(r'\s+\d+\s*$', '', name)

    # Clean up the text
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'^,\s*|,\s*$', '', name)  # Remove leading/trailing commas

    # replace "(" and ")" to backspace
    name = re.sub(r'\(|\)', ' ', name)

    # Step 2: Parse volume and get cleaned name
    name, volume_number, volume_unit = parse_volume_from_string(name)

    if not name:
        return '', volume_number, volume_unit


    normalized = re.sub(r'\band\b', '&', name, flags=re.IGNORECASE)

    normalized = remove_duplicate_words(normalized)

    normalized = normalized.lower()

    normalized = re.sub(r'[.,]', '', normalized)

    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized, volume_number, volume_unit


def filter_valvoline_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'valvoline'
) -> list[dict[str, Any]]:
    """
    Filter Valvoline products from CSV file using SOLID architecture.

    Args:
        input_file: Path to input CSV file
        encoding: File encoding (default: cp1251)
        match_word: Word to match in product name or brand

    Returns:
        List of filtered Valvoline products
    """
    reader = CSVReader(encoding)
    filter_ = ValvolineProductFilter(match_word)
    processor = ValvolineProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)


def filter_rosneft_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'rosneft'
) -> list[dict[str, Any]]:
    """
    Filter Rosneft products from CSV file using SOLID architecture.

    Args:
        input_file: Path to input CSV file
        encoding: File encoding (default: cp1251)
        match_word: Word to match in product name or brand

    Returns:
        List of filtered Rosneft products
    """
    reader = CSVReader(encoding)
    filter_ = RosneftProductFilter(match_word)
    processor = RosneftProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)


def filter_forsage_products(
    input_file: str, encoding: str = 'cp1251', match_word: str = 'forsage'
) -> list[dict[str, Any]]:
    """
    Filter Forsage products from CSV file using SOLID architecture.

    Args:
        input_file: Path to input CSV file
        encoding: File encoding (default: cp1251)
        match_word: Word to match in product name or brand

    Returns:
        List of filtered Forsage products
    """
    reader = CSVReader(encoding)
    filter_ = ForsageProductFilter(match_word)
    processor = ForsageProductProcessor()
    csv_filter = CSVProductFilter(reader, filter_, processor)
    return csv_filter.filter_data(input_file)
