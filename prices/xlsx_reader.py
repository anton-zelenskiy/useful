import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
from constants import VOLUME_MAP
from csv_reader import normalize_product_name


class BaseNormalizer(ABC):
    """Abstract base class for product name normalizers."""

    @abstractmethod
    def normalize(self, name: str) -> tuple[str, str, str]:
        """
        Normalize product name.

        Returns:
            Tuple of (normalized_name, volume_number, volume_unit)
        """
        pass


class ValvolineNormalizer(BaseNormalizer):
    def normalize(self, name: str) -> tuple[str, str, str]:
        if not name:
            return '', '', ''

        # Add VALVOLINE brand
        normalized = self._normalize_valvoline_brand(name)

        # Simple volume extraction - look for patterns like "1л", "500мл", "4л", "1L", "500ML"
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

    def _normalize_valvoline_brand(self, name: str) -> str:
        """Normalize product name to ensure it starts with VALVOLINE."""
        if not name:
            return 'VALVOLINE'

        name = str(name).strip().lower()

        # Check if already starts with valvoline
        if name.startswith('valvoline'):
            return name

        # Check if starts with 'val' (but not valvoline)
        if name.startswith('val') and not name.startswith('valvoline'):
            # Replace 'val' with 'valvoline'
            return 'VALVOLINE' + name[3:]  # Keep original case for rest

        # If doesn't start with valvoline, add it
        return 'VALVOLINE ' + name


class ForsageNormalizer(BaseNormalizer):
    """Normalizer for Forsage products."""

    def normalize(self, name: str) -> tuple[str, str, str]:
        """Normalize Forsage product name."""
        if not name:
            return '', '', ''

        # Clean up the name
        normalized = str(name).strip()

        # Remove extra spaces
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        normalized = normalized.lower()

        # No volume extraction from name since volume comes from package column
        return normalized, '', ''


class RosneftNormalizer(BaseNormalizer):
    def normalize(self, name: str) -> tuple[str, str, str]:
        if not name:
            return '', '', ''

        # Basic normalization
        normalized = str(name).strip()
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        normalized = normalized.lower()

        return normalized, '', ''


class BaseXlsxReader(ABC):
    """Base class for XLSX file readers."""

    def __init__(self, file_path: str, normalizer: BaseNormalizer) -> None:
        """
        Initialize reader with XLSX file path, normalizer.

        Args:
            file_path: Path to XLSX file
            normalizer: Normalizer instance for this reader
        """
        self.file_path = Path(file_path)
        self.normalizer = normalizer

    @abstractmethod
    def parse_xlsx(self) -> list[dict[str, Any]]:
        """
        Parse XLSX file and extract products.

        Returns:
            List of dictionaries with product data
        """
        pass


class ValvolineXlsxReader(BaseXlsxReader):
    """
    Reader for VALSAR price file: `Прайс ВАЛСАР с 01.09.2025_new.xlsx`

    Structure:
    - Rows 1-2: Not actual data
    - Row 3: Russian headers
    - Row 4: English headers
    - Row 5+: Data starts
    - Columns: 'Наименование', 'Упаковка', 'Окончательная цена с НДС за 1л.'
    """

    def parse_xlsx(self) -> list[dict[str, Any]]:
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

                package_count, volume, volume_unit = self._parse_package_info(package)

                price = (
                    str(row[price_cols[0]]).strip()
                    if price_cols and not pd.isna(row[price_cols[0]])
                    else ''
                )

                normalized_name = self._normalize_name(name)

                product = {
                    'original_name': name,
                    'normalized_name': normalized_name,
                    'volume': volume,
                    'volume_unit': volume_unit,
                    'package_count': package_count,
                    'price': price,
                }
                products.append(product)

            print(f'\n=== SUMMARY ===')
            print(f'Total rows processed: {total_rows}')
            print(f'Products to save: {len(products)}')

            return products

        except Exception as e:
            print(f'Error reading VALSAR file {self.file_path.name}: {e}')
            import traceback

            traceback.print_exc()
            return []

    def _normalize_name(self, name: str) -> str:
        """Normalize product name to ensure it starts with VALVOLINE."""
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
        if not package_str:
            return 1, '', ''

        package_str = str(package_str).strip()

        # Pattern 1: "6 x 5 L" or "12 x 500 ML" (multi-package)
        multi_pattern = r'(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(l|lm)'
        multi_match = re.search(multi_pattern, package_str, re.IGNORECASE)
        if multi_match:
            count = int(multi_match.group(1))
            volume = multi_match.group(2)
            unit = multi_match.group(3)
            return count, volume, unit

        # Pattern 2: "30 L" (single package)
        single_pattern = r'(\d+(?:\.\d+)?)\s*(l|ml)'
        single_match = re.search(single_pattern, package_str, re.IGNORECASE)
        if single_match:
            volume = single_match.group(1)
            unit = single_match.group(2)
            return 1, volume, unit

        # If no pattern matches, return default
        return 1, '', ''


class ForsageXlsxReader(BaseXlsxReader):
    """
    Reader for cost file: `Прайс себестоимость от 01.10.2025г..xlsx`

    This file structure needs to be analyzed first.
    """

    def parse_xlsx(self) -> list[dict[str, Any]]:
        """Parse cost XLSX file."""
        try:
            print(f'\n=== DEBUG: Reading {self.file_path.name} ===')

            # Read only the specific columns we need
            columns_to_read = ['Forsage', 'Фасовка', 'Себестоимость с НДС']
            df = pd.read_excel(self.file_path, usecols=columns_to_read)

            print(f'DataFrame shape: {df.shape}')
            print(f'Columns: {list(df.columns)}')
            print(f'Data types:')
            print(df.dtypes)
            print(f'\nFirst 10 rows:')
            print(df.head(10))

            products = []
            current_product_name = None

            # Process rows with hierarchical structure
            for idx, row in df.iterrows():
                # Check if this row has a product name
                forsage_val = row['Forsage']
                if not pd.isna(forsage_val) and str(forsage_val).strip():
                    current_product_name = str(forsage_val).strip()
                    print(f'\nFound new product: "{current_product_name}"')

                # If we have a current product and package info, create product entry
                if current_product_name:
                    package_val = row['Фасовка']
                    price_val = row['Себестоимость с НДС']

                    if (
                        not pd.isna(package_val)
                        and str(package_val).strip()
                        and not pd.isna(price_val)
                        and str(price_val).strip()
                    ):
                        package_str = str(package_val).strip()
                        price_str = str(price_val).strip()

                        # Parse package info using writer's method
                        package_count, volume, volume_unit = (
                            self._parse_package_info(package_str)
                        )

                        product = {
                            'original_name': current_product_name,
                            'normalized_name': self._normalize_name(current_product_name),
                            'volume': volume,
                            'volume_unit': volume_unit,
                            'package_count': package_count,
                            'price': price_str,
                            'package': package_str,
                        }
                        products.append(product)

                        print(f'  Added product: {package_str} - {price_str}')

            print(f'\n=== SUMMARY ===')
            print(f'Total rows processed: {len(df)}')
            print(f'Products found: {len(products)}')

            return products

        except Exception as e:
            print(f'Error reading cost file {self.file_path.name}: {e}')
            import traceback

            traceback.print_exc()
            return []

    def _normalize_name(self, name: str) -> str:
        name = normalize_product_name(name)
        name = name.replace('forsage lubricants', 'forsage')

        return name

    def _parse_package_info(self, package_str: str) -> tuple[int, str, str]:
        """
        Parse package information from strings like:
        - 1кг
        - 1000мл
        - 1л
        """
        if not package_str:
            return 1, '', ''

        package_str = str(package_str).strip()

        pattern = r'(\d+(?:\.\d+)?)\s*(кг|мл|л|г)'
        match = re.search(pattern, package_str, re.IGNORECASE)
        if match:
            volume = match.group(1)
            volume_unit_raw = match.group(2).lower()
            if volume_unit_raw in VOLUME_MAP:
                volume_unit = VOLUME_MAP[volume_unit_raw]
                return 1, volume, volume_unit

        return 1, '', ''

class RosneftXlsxReader(BaseXlsxReader):
    """
    Reader for Rosneft distributors file: `Дистрибьюторы_РФ_фасовка_август_2025.xlsm`

    Structure:
    - Header starts from 10th row (row 9 in 0-based indexing)
    - Header consists of 4 rows from 10 to 13 (rows 9-12 in 0-based indexing)
    - Data starts from row 15 (row 14 in 0-based indexing)
    """

    def parse_xlsx(self) -> list[dict[str, Any]]:
        """Parse Rosneft distributors XLSX file."""
        try:
            print(f'\n=== DEBUG: Reading {self.file_path.name} ===')

            sheet_name = 'РНПК'
            print(f'\n=== READING SHEET: {sheet_name} ===')

            print(f'Reading with simple header (skiprows=9, header=0)...')
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=9, header=0)


            # Find the specific columns we need
            name_column = 'Наименование'
            price_column = df.columns[8]
            package_column = 'Упаковка'

            products = []
            current_product_name = None

            for idx, row in df.iterrows():
                # Check if this row has a product name
                name_val = row[name_column]
                if not pd.isna(name_val) and str(name_val).strip():
                    current_product_name = str(name_val).strip()
                    logging.debug(f'Found new product: "{current_product_name}"')

                package_val = row[package_column]
                price_val = row[price_column]

                if pd.isna(package_val) and pd.isna(price_val):
                    continue

                # If we have a current product and package info, create product entry
                if current_product_name:
                    package_val = row[package_column]
                    price_val = row[price_column]

                    if (
                        not pd.isna(package_val)
                        and str(package_val).strip()
                        and not pd.isna(price_val)
                        and str(price_val).strip()
                    ):
                        package_str = str(package_val).strip()
                        price_str = str(price_val).strip()

                        # Parse package information using writer's method
                        package_count, volume, volume_unit = self._parse_package_info(
                            package_str
                        )

                        normalized_name = normalize_product_name(current_product_name)
                        product = {
                            'original_name': current_product_name,
                            'normalized_name': normalized_name,
                            'volume': volume,
                            'volume_unit': volume_unit,
                            'package_count': package_count,
                            'price': price_str,
                            'package': package_str,
                        }
                        products.append(product)

                        logging.debug(f'  Added product: {package_str} - {price_str}')

            print(f'\n=== SUMMARY ===')
            print(f'Total products found: {len(products)}')

            return products

        except Exception as e:
            print(f'Error reading Rosneft file {self.file_path.name}: {e}')
            import traceback

            traceback.print_exc()
            return []

    def _parse_package_info(self, package_str: str) -> tuple[int, str, str]:
        """
        Parse package information from strings like:
        - "канистра 1 л" -> (1, "1", "л")
        - "канистра 4 л" -> (1, "4", "л")
        - "бочка 175 кг" -> (1, "175", "кг")

        Args:
            package_str: Package string to parse

        Returns:
            Tuple of (package_count, package_volume, package_unit)
        """
        if not package_str:
            return 1, '', ''

        package_str = str(package_str).strip().lower()

        pattern = r'(\d+(?:\.\d+)?)\s*(л|кг)'
        match = re.search(pattern, package_str, re.IGNORECASE)
        if match:
            volume = match.group(1)
            volume_unit_raw = match.group(2).lower()
            if volume_unit_raw in VOLUME_MAP:
                volume_unit = VOLUME_MAP[volume_unit_raw]
                return 1, volume, volume_unit

        return 1, '', ''
