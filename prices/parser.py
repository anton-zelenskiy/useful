import csv
import re
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


VOLUME_MAP = {
    'л': 'l',
    'мл': 'ml',
    'кг': 'kg',
    'г': 'g',
}


def common_normalize(text: str) -> str:
    """
    Apply common normalization rules to text.

    Args:
        text: Input text

    Returns:
        Normalized text
    """
    if not text:
        return ''

    text = str(text).strip()

    # Remove digits from the end (for cases like "866904")
    # This handles cases where there are product codes at the end
    text = re.sub(r'\s+\d+\s*$', '', text)

    # Clean up the text
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^,\s*|,\s*$', '', text)  # Remove leading/trailing commas

    # replace "(" and ")" to backspace
    text = re.sub(r'\(|\)', ' ', text)

    return text


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
    normalized = re.sub(r'(\d+W)-(\d+)', r'\1\2', text)

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
        r',\s*(\d+(?:\.\d+)?)\s*([а-яё]+)\.?\s*$',  # ", 4 л"
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

    # Apply common normalization to cleaned text
    text = common_normalize(text)

    return text, volume_number, volume_unit


def normalize_product_name(name: str) -> tuple[str, str, str]:
    """
    Normalize product name according to specified rules:
    - Apply common normalization first
    - Parse and extract volume information
    - Remove russian characters
    - Convert to uppercase
    - Replace 'and' to '&' if 'and' is single word (not part of other word)

    Args:
        name: Original product name

    Returns:
        Tuple of (normalized_name, volume_number, volume_unit)
    """
    if not name:
        return '', '', ''

    # Step 1: Apply common normalization first
    normalized = common_normalize(name)

    # Step 2: Parse volume and get cleaned name
    cleaned_name, volume_number, volume_unit = parse_volume_from_string(normalized)

    if not cleaned_name:
        return '', volume_number, volume_unit

    # Step 3: Remove russian characters (keep only latin letters, digits, spaces, and common symbols)
    normalized = re.sub(r'[а-яё]', '', cleaned_name, flags=re.IGNORECASE)

    # Step 4: Replace 'and' to '&' if it's a single word (not part of other word)
    normalized = re.sub(r'\band\b', '&', normalized, flags=re.IGNORECASE)

    # Step 5: Remove duplicate words
    normalized = remove_duplicate_words(normalized)

    # Step 6: Normalize viscosity grades
    normalized = normalize_viscosity_grades(normalized)

    # Step 7: Convert to uppercase
    normalized = normalized.upper()

    # Step 8: Remove ".", ","
    normalized = re.sub(r'[.,]', '', normalized)

    # Step 9: Clean up multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized, volume_number, volume_unit


def filter_valvoline_products(
    input_file: str, output_file: str, encoding: str = 'cp1251',
    match_word: str = 'valvoline'
) -> None:
    """
    Read CSV file, filter rows where 'name' or 'brand' columns contain 'valvoline',
    and write filtered data to new CSV file.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        encoding: File encoding (default: utf-8)
    """
    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(f'File not found: {input_path}')

    try:
        with open(input_path, 'r', encoding=encoding, newline='') as infile:
            reader = csv.DictReader(infile)

            # Check if required columns exist
            if 'name' not in reader.fieldnames or 'brand' not in reader.fieldnames:
                raise ValueError("CSV file must contain 'name' and 'brand' columns")

            # Filter rows and collect data
            filtered_rows = []
            for row in reader:
                name = str(row.get('name', '')).lower()
                brand = str(row.get('brand', '')).lower()

                if match_word in name or match_word in brand:
                    # Store original name and normalized name with volume info
                    original_name = row.get('name', '')
                    normalized_name, volume_number, volume_unit = normalize_product_name(
                        original_name
                    )
                    normalized_name = f"{normalized_name} {volume_number} {volume_unit}".upper()
                    filtered_rows.append(
                        {
                            'original_name': original_name,
                            'normalized_name': normalized_name,
                            'volume': volume_number,
                            'volume_unit': volume_unit,
                        }
                    )

            # Write filtered data to output file
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                if filtered_rows:
                    fieldnames = ['original_name', 'normalized_name', 'volume', 'volume_unit']
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(filtered_rows)
                    print(
                        f"Filtered {len(filtered_rows)} rows containing 'valvoline' to {output_file}"
                    )
                else:
                    print("No rows found containing 'valvoline'")

    except UnicodeDecodeError as e:
        raise Exception(f'Error reading CSV file: {e}')
