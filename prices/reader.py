"""
XLSX file readers for different Valvoline product files with various structures.
"""

import csv
import re
from abc import ABC, abstractmethod
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def parse_package_info(package_str: str) -> tuple[int, str, str]:
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


def normalize_valvoline_brand(name: str) -> str:
    """
    Normalize product name to ensure it starts with VALVOLINE.

    Rules:
    - If product name starts with 'VAL' (case insensitive), replace with 'VALVOLINE'
    - If product name already starts with 'valvoline' (case insensitive), keep as is
    - If product name doesn't start with 'valvoline', add 'VALVOLINE ' at the beginning

    Args:
        name: Original product name

    Returns:
        Product name with VALVOLINE brand prefix
    """
    if not name:
        return 'VALVOLINE'

    name = str(name).strip()
    name_lower = name.lower()

    # Check if already starts with valvoline
    if name_lower.startswith('valvoline'):
        return name

    # Check if starts with 'val' (but not valvoline)
    if name_lower.startswith('val') and not name_lower.startswith('valvoline'):
        # Replace 'val' with 'valvoline'
        return 'VALVOLINE' + name[3:]  # Keep original case for rest

    # If doesn't start with valvoline, add it
    return 'VALVOLINE ' + name


def normalize_product_name_simple(name: str) -> tuple[str, str, str]:
    """
    Simple normalization without complex parsing.
    Just adds VALVOLINE brand and extracts basic volume info.

    Args:
        name: Original product name

    Returns:
        Tuple of (normalized_name, volume_number, volume_unit)
    """
    if not name:
        return '', '', ''

    # Add VALVOLINE brand
    normalized = normalize_valvoline_brand(name)

    volume_number = ''
    volume_unit = ''

    # Pattern: digits followed L, ML
    volume_match = re.search(r'(\d+(?:\.\d+)?)\s*(L|ML)', normalized, re.IGNORECASE)
    if volume_match:
        volume_number = volume_match.group(1)
        volume_unit_raw = volume_match.group(2).lower()
        if volume_unit_raw in ['л', 'l']:
            volume_unit = 'L'
        elif volume_unit_raw in ['мл', 'ml']:
            volume_unit = 'ML'

    return normalized, volume_number, volume_unit


class BaseXlsxReader(ABC):
    """Base class for XLSX file readers."""

    def __init__(self, file_path: str):
        """
        Initialize reader with XLSX file path.

        Args:
            file_path: Path to XLSX file
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f'File not found: {self.file_path}')

    @abstractmethod
    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """
        Parse XLSX file and extract Valvoline products.

        Returns:
            List of dictionaries with product data
        """
        pass

    def save_to_csv(self, output_file: str, products: List[Dict[str, Any]]) -> None:
        """
        Save products to CSV file with standardized columns.

        Args:
            output_file: Path to output CSV file
            products: List of product dictionaries
        """
        if not products:
            print(f'No Valvoline products found in {self.file_path.name}')
            return

        # Create simplified output with standardized columns
        simplified_products = []
        for product in products:
            # Parse package information
            package_count, package_volume, package_unit = parse_package_info(
                product.get('package', '')
            )

            # Parse price as Decimal
            price_str = product.get('price', '')
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

            simplified_products.append(
                {
                    'original_name': product.get('original_name', ''),
                    'normalized_name': product.get('normalized_name', ''),
                    'volume': package_volume,
                    'volume_unit': package_unit,
                    'package_count': package_count,
                    'price': f'{price:.2f}',
                    'price_total': f'{price_total:.2f}',
                }
            )

        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            fieldnames = [
                'original_name',
                'normalized_name',
                'volume',
                'volume_unit',
                'package_count',
                'price',
                'price_total',
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(simplified_products)

        print(
            f'Found {len(products)} Valvoline products in {self.file_path.name}, saved to {output_file}'
        )


class ValsarXlsxReader(BaseXlsxReader):
    """
    Reader for VALSAR price file: `Прайс ВАЛСАР с 01.09.2025_new.xlsx`

    Structure:
    - Rows 1-2: Not actual data
    - Row 3: Russian headers
    - Row 4: English headers
    - Row 5+: Data starts
    - Columns: 'Наименование', 'Упаковка', 'Окончательная цена с НДС за 1л.'
    """

    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """Parse VALSAR XLSX file."""
        try:
            print(f'\n=== DEBUG: Reading {self.file_path.name} ===')

            # Read with pandas, skip first 2 rows, use row 3 as header
            df = pd.read_excel(self.file_path, skiprows=2, header=0)

            print(f'DataFrame shape: {df.shape}')
            print(f'Columns: {list(df.columns)}')
            print(f'First 5 rows:')
            print(df.head())
            print(f'Data types:')
            print(df.dtypes)

            products = []
            valvoline_count = 0
            total_rows = 0

            for idx, row in df.iterrows():
                total_rows += 1

                # Get the name column (should be 'Наименование')
                name_cols = [col for col in df.columns if 'Наименование' in str(col)]
                if not name_cols:
                    print(f'Row {idx}: No "Наименование" column found')
                    continue

                name = str(row[name_cols[0]]).strip()
                if pd.isna(row[name_cols[0]]) or not name or name.lower() == 'nan':
                    continue

                # Debug: print first few product names
                if idx < 10:
                    print(f'Row {idx}: Product name = "{name}"')

                # Process all products (not just valvoline ones)
                valvoline_count += 1
                print(f'PROCESSING PRODUCT #{valvoline_count}: "{name}"')

                # Get other columns
                package_cols = [col for col in df.columns if 'Упаковка' in str(col)]
                price_cols = [
                    col for col in df.columns if 'Окончательная цена с НДС за 1л' in str(col)
                ]

                package = (
                    str(row[package_cols[0]]).strip()
                    if package_cols and not pd.isna(row[package_cols[0]])
                    else ''
                )
                price = (
                    str(row[price_cols[0]]).strip()
                    if price_cols and not pd.isna(row[price_cols[0]])
                    else ''
                )

                normalized_name, volume_number, volume_unit = normalize_product_name_simple(name)

                product = {
                    'original_name': name,
                    'normalized_name': normalized_name,
                    'volume': volume_number,
                    'volume_unit': volume_unit,
                    'price': price,
                    'package': package,
                }
                products.append(product)

            print(f'\n=== SUMMARY ===')
            print(f'Total rows processed: {total_rows}')
            print(f'Products processed: {valvoline_count}')
            print(f'Products to save: {len(products)}')

            return products

        except Exception as e:
            print(f'Error reading VALSAR file {self.file_path.name}: {e}')
            import traceback

            traceback.print_exc()
            return []


class DistributorsXlsxReader(BaseXlsxReader):
    """
    Reader for distributors file: `Дистрибьюторы_РФ_фасовка_август_2025.xlsm`

    This file structure needs to be analyzed first.
    """

    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """Parse distributors XLSX file."""
        try:
            # Try to read the file and analyze its structure
            df = pd.read_excel(self.file_path)

            print(f'Analyzing {self.file_path.name}:')
            print(f'Shape: {df.shape}')
            print(f'Columns: {list(df.columns)}')
            print(f'First few rows:')
            print(df.head())

            products = []

            # Look for columns that might contain product names
            name_columns = []
            for col in df.columns:
                col_str = str(col).lower()
                if any(
                    keyword in col_str
                    for keyword in ['наименование', 'название', 'product', 'name', 'товар']
                ):
                    name_columns.append(col)

            print(f'Potential name columns: {name_columns}')

            # Process rows
            for _, row in df.iterrows():
                for name_col in name_columns:
                    name = str(row[name_col]).strip()
                    if pd.isna(row[name_col]) or not name or name.lower() == 'nan':
                        continue

                    # Process all products (not just valvoline ones)
                    normalized_name, volume_number, volume_unit = normalize_product_name_simple(
                        name
                    )

                    product = {
                        'original_name': name,
                        'normalized_name': normalized_name,
                        'volume': volume_number,
                        'volume_unit': volume_unit,
                        'price': '',
                        'package': '',
                    }
                    products.append(product)

            return products

        except Exception as e:
            print(f'Error reading distributors file {self.file_path.name}: {e}')
            return []


class CostXlsxReader(BaseXlsxReader):
    """
    Reader for cost file: `Прайс себестоимость от 01.10.2025г..xlsx`

    This file structure needs to be analyzed first.
    """

    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """Parse cost XLSX file."""
        try:
            # Try to read the file and analyze its structure
            df = pd.read_excel(self.file_path)

            print(f'Analyzing {self.file_path.name}:')
            print(f'Shape: {df.shape}')
            print(f'Columns: {list(df.columns)}')
            print(f'First few rows:')
            print(df.head())

            products = []

            # Look for columns that might contain product names
            name_columns = []
            for col in df.columns:
                col_str = str(col).lower()
                if any(
                    keyword in col_str
                    for keyword in ['наименование', 'название', 'product', 'name', 'товар']
                ):
                    name_columns.append(col)

            print(f'Potential name columns: {name_columns}')

            # Process rows
            for _, row in df.iterrows():
                for name_col in name_columns:
                    name = str(row[name_col]).strip()
                    if pd.isna(row[name_col]) or not name or name.lower() == 'nan':
                        continue

                    # Process all products (not just valvoline ones)
                    normalized_name, volume_number, volume_unit = normalize_product_name_simple(
                        name
                    )

                    product = {
                        'original_name': name,
                        'normalized_name': normalized_name,
                        'volume': volume_number,
                        'volume_unit': volume_unit,
                        'price': '',
                        'package': '',
                    }
                    products.append(product)

            return products

        except Exception as e:
            print(f'Error reading cost file {self.file_path.name}: {e}')
            return []


def test_package_parsing():
    """Test the package parsing functionality."""
    test_cases = [
        ('30 L', (1, '30', 'L')),
        ('6 x 5 L', (6, '5', 'L')),
        ('12 × 500 ML', (12, '500', 'ML')),  # Using × symbol
        ('6X5L', (6, '5', 'L')),  # No spaces
        ('1 x 1 L', (1, '1', 'L')),
        ('', (1, '', '')),
        ('invalid package', (1, '', '')),
    ]

    print('\n=== Testing Package Parsing ===')
    for package_str, expected in test_cases:
        result = parse_package_info(package_str)
        status = '✓' if result == expected else '✗'
        print(f"{status} '{package_str}' -> {result} (expected: {expected})")


def test_valvoline_normalization():
    """Test the VALVOLINE brand normalization."""
    test_cases = [
        ('Motor Oil 5W-40', 'VALVOLINE Motor Oil 5W-40'),
        ('VAL Motor Oil', 'VALVOLINE Motor Oil'),
        ('valvoline Gear Oil', 'valvoline Gear Oil'),  # Already has valvoline
        ('VALVOLINE ATF', 'VALVOLINE ATF'),  # Already has VALVOLINE
        ('', 'VALVOLINE'),
        ('Val Motor Oil', 'VALVOLINE Motor Oil'),
    ]

    print('\n=== Testing VALVOLINE Brand Normalization ===')
    for original, expected in test_cases:
        result = normalize_valvoline_brand(original)
        status = '✓' if result == expected else '✗'
        print(f"{status} '{original}' -> '{result}' (expected: '{expected}')")


if __name__ == '__main__':
    # Test the VALVOLINE normalization
    test_valvoline_normalization()

    # Test package parsing
    test_package_parsing()

    # Process the VALSAR file
    data_dir = Path('data')
    file_to_read = 'Прайс ВАЛСАР с 01.09.2025_new.xlsx'
    reader = ValsarXlsxReader(file_path=str(data_dir / file_to_read))
    products = reader.parse_xlsx()
    reader.save_to_csv(str(data_dir / 'valvoline_products_valsar.csv'), products)
