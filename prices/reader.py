import re
from abc import ABC, abstractmethod
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from Levenshtein import distance
from parser import ReaderFactory
from writer import FileWriter, WriterFactory


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

        # Convert to uppercase for consistency
        normalized = normalized.upper()

        # No volume extraction from name since volume comes from package column
        return normalized, '', ''


class RosneftNormalizer(BaseNormalizer):
    def normalize(self, name: str) -> tuple[str, str, str]:
        if not name:
            return '', '', ''

        # Basic normalization
        normalized = str(name).strip()
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        normalized = normalized.upper()

        return normalized, '', ''


def match_products_by_name(csv_file1: str, csv_file2: str, max_distance: int = 3) -> None:
    """
    Match products from two CSV files using Levenshtein distance.

    Args:
        csv_file1: Path to first CSV file (e.g., valvoline_products.csv)
        csv_file2: Path to second CSV file (e.g., valvoline_products_valsar.csv)
        max_distance: Maximum Levenshtein distance for a match
    """
    print(f'\n=== MATCHING PRODUCTS ===')
    print(f'CSV file 1: {csv_file1}')
    print(f'CSV file 2: {csv_file2}')
    print(f'Max distance: {max_distance}')

    # Use CSV reader to read both files
    csv_reader = ReaderFactory.create_csv_reader()

    # Read CSV file 1 products
    csv1_products = csv_reader.read(csv_file1)

    # Read CSV file 2 products
    csv2_products = csv_reader.read(csv_file2)

    # Match products
    matches_found = 0
    print(f'\n=== MATCHING RESULTS ===')

    for csv1_product in csv1_products:
        csv1_name = csv1_product.get('normalized_name', '').strip()
        if not csv1_name:
            continue

        best_match = None
        best_distance = float('inf')
        best_match_product = None

        for csv2_product in csv2_products:
            csv2_name = csv2_product.get('normalized_name', '').strip()
            if not csv2_name:
                continue

            dist = distance(csv1_name.lower(), csv2_name.lower())
            if dist < best_distance:
                best_distance = dist
                best_match = csv2_name
                best_match_product = csv2_product

        if best_match and best_distance <= max_distance:
            matches_found += 1
            print(f'✓ MATCH (distance: {best_distance})')
            print(f'  File 1: "{csv1_name}"')
            print(f'  File 2: "{best_match}"')
            print(f'  Price 1: {csv1_product.get("price", "N/A")}')
            print(f'  Price 2: {best_match_product.get("price", "N/A")}')
            print()
        else:
            print(f'✗ NO MATCH (best distance: {best_distance})')
            print(f'  File 1: "{csv1_name}"')
            print()

    print(f'=== SUMMARY ===')
    print(f'Total products in file 1: {len(csv1_products)}')
    print(f'Total products in file 2: {len(csv2_products)}')
    print(f'Matches found: {matches_found}')
    print(f'Match rate: {matches_found / len(csv1_products) * 100:.1f}%')


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

    def __init__(self, file_path: str, normalizer: BaseNormalizer, writer: FileWriter = None):
        """
        Initialize reader with XLSX file path, normalizer, and writer.

        Args:
            file_path: Path to XLSX file
            normalizer: Normalizer instance for this reader
            writer: Writer instance for output (defaults to CSVWriter)
        """
        self.file_path = Path(file_path)
        self.normalizer = normalizer
        self.writer = writer or WriterFactory.create_writer('csv')
        if not self.file_path.exists():
            raise FileNotFoundError(f'File not found: {self.file_path}')

    @abstractmethod
    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """
        Parse XLSX file and extract products.

        Returns:
            List of dictionaries with product data
        """
        pass

    def save_to_file(self, output_file: str, products: List[Dict[str, Any]]) -> None:
        """
        Save products to file using the configured writer.

        Args:
            output_file: Path to output file
            products: List of product dictionaries
        """
        if not products:
            print(f'No products found in {self.file_path.name}')
            return

        self.writer.write(output_file, products)


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
            print(f'Products processed: {valvoline_count}')
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

            # This file has a hierarchical structure:
            # - "Forsage" column contains product names (spans multiple rows)
            # - "Фасовка" column contains package sizes
            # - "Себестоимость с НДС" contains prices

            print(f'\nDetected hierarchical structure:')
            print(f'- Product names: "Forsage" column')
            print(f'- Package sizes: "Фасовка" column')
            print(f'- Prices: "Себестоимость с НДС" column')

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

                        # Normalize the product name
                        normalized_name, volume_number, volume_unit = self.normalizer.normalize(
                            current_product_name
                        )

                        # Parse package info using writer's method
                        package_count, package_volume, package_unit = (
                            self.writer._parse_package_info(package_str)
                        )

                        product = {
                            'original_name': current_product_name,
                            'normalized_name': normalized_name,
                            'volume': package_volume,  # Use package volume, not name volume
                            'volume_unit': package_unit,
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


class RosneftXlsxReader(BaseXlsxReader):
    """
    Reader for Rosneft distributors file: `Дистрибьюторы_РФ_фасовка_август_2025.xlsm`

    Structure:
    - Header starts from 10th row (row 9 in 0-based indexing)
    - Header consists of 4 rows from 10 to 13 (rows 9-12 in 0-based indexing)
    - Data starts from row 15 (row 14 in 0-based indexing)
    """

    def parse_xlsx(self) -> List[Dict[str, Any]]:
        """Parse Rosneft distributors XLSX file."""
        try:
            print(f'\n=== DEBUG: Reading {self.file_path.name} ===')

            # Read the specific "РНПК" sheet
            sheet_name = 'РНПК'
            print(f'\n=== READING SHEET: {sheet_name} ===')

            # Read with simple header structure
            # Skip first 12 rows, use row 12 as header (where "за штуку" is)
            print(f'Reading with simple header (skiprows=9, header=0)...')
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=9, header=0)

            print(f'DataFrame shape: {df.shape}')
            print(f'Columns: {df.columns.tolist()}')
            print(f'\nFirst 50 rows:')
            print(df.head(50))

            # Find the specific columns we need
            name_column = 'Наименование'
            price_column = df.columns[8]
            package_column = 'Упаковка'

            products = []
            current_product_name = None

            # Process rows with hierarchical structure
            print(f'\n=== PROCESSING PRODUCTS ===')
            for idx, row in df.iterrows():
                # Check if this row has a product name
                name_val = row[name_column]
                if not pd.isna(name_val) and str(name_val).strip():
                    current_product_name = str(name_val).strip()
                    print(f'\nFound new product: "{current_product_name}"')


                package_val = row[package_column]
                price_val = row[price_column]

                if (
                    pd.isna(package_val)
                    and pd.isna(price_val)
                ):
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
                        package_count, package_volume, package_unit = self._parse_package_info(package_str)

                        # Normalize the product name
                        normalized_name, volume_number, volume_unit = self.normalizer.normalize(current_product_name)

                        product = {
                            'original_name': current_product_name,
                            'normalized_name': normalized_name,
                            'volume': package_volume,  # Use package volume, not name volume
                            'volume_unit': package_unit,
                            'package_count': package_count,
                            'price': price_str,
                            'package': package_str,
                        }
                        products.append(product)

                        print(f'  Added product: {package_str} - {price_str}')

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

        # Pattern 1: "канистра 1 л" or "канистра 4 л"
        pattern = r'(\d+(?:\.\d+)?)\s*(л|кг)'
        match = re.search(pattern, package_str, re.IGNORECASE)
        if match:
            volume = match.group(1)
            unit = match.group(2).upper()
            return 1, volume, unit

        return 1, '', ''


if __name__ == '__main__':
    data_dir = Path('data')
    # file_to_read = 'Прайс ВАЛСАР с 01.09.2025_new.xlsx'
    # valvoline_normalizer = ValvolineNormalizer()
    # reader = ValvolineXlsxReader(
    #     file_path=str(data_dir / file_to_read), normalizer=valvoline_normalizer
    # )
    # products = reader.parse_xlsx()
    # reader.save_to_file(str(data_dir / 'valvoline_products_xlsx.csv'), products)

    # # Analyze distance distribution to help choose max_distance
    # csv_file1 = str(data_dir / 'valvoline_products.csv')
    # csv_file2 = str(data_dir / 'valvoline_products_xlsx.csv')
    # match_products_by_name(csv_file1, csv_file2, max_distance=5)

    # # Test CostXlsxReader
    # forsage_file = 'Прайс себестоимость от 01.10.2025г..xlsx'
    # forsage_normalizer = ForsageNormalizer()
    # cost_writer = WriterFactory.create_writer('csv')
    # cost_reader = ForsageXlsxReader(
    #     file_path=str(data_dir / forsage_file),
    #     normalizer=forsage_normalizer,
    #     writer=cost_writer
    # )
    # cost_products = cost_reader.parse_xlsx()
    # cost_reader.save_to_file(str(data_dir / 'forsage_products_xlsx.csv'), cost_products)

    # # Match products from two CSV files
    # csv_file1 = str(data_dir / 'forsage_products.csv')
    # csv_file2 = str(data_dir / 'forsage_products_xlsx.csv')
    # match_products_by_name(csv_file1, csv_file2, max_distance=5)

    # Test RosneftXlsxReader - analyze file structure first
    rosneft_file = 'Дистрибьюторы_РФ_фасовка_август_2025.xlsm'
    rosneft_normalizer = RosneftNormalizer()
    rosneft_writer = WriterFactory.create_writer('csv')
    rosneft_reader = RosneftXlsxReader(
        file_path=str(data_dir / rosneft_file), normalizer=rosneft_normalizer, writer=rosneft_writer
    )
    rosneft_products = rosneft_reader.parse_xlsx()
    rosneft_reader.save_to_file(str(data_dir / 'rosneft_products_xlsx.csv'), rosneft_products)

    # Match products from two CSV files
    # csv_file1 = str(data_dir / 'rosneft_products.csv')
    # csv_file2 = str(data_dir / 'rosneft_products_xlsx.csv')
    # match_products_by_name(csv_file1, csv_file2, max_distance=5)
