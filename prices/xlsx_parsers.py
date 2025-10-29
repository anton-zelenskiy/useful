import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
from constants import VOLUME_MAP
from normalizers import normalize_product_name


class BaseXlsxParser(ABC):
    """Base class for XLSX file parsers."""

    SHEET_NAME: str | None = None

    def __init__(self, file_path: str) -> None:
        """
        Initialize reader with XLSX file path.

        Args:
            file_path: Path to XLSX file
        """
        self.file_path = Path(file_path)

    def parse_xlsx(self) -> list[dict[str, Any]]:
        try:
            return self._parse_xlsx()
        except Exception as e:
            logging.error(f'Error reading file {self.file_path.name}: {e}')
            return []

    @abstractmethod
    def _parse_xlsx(self) -> list[dict[str, Any]]:
        """
        Parse XLSX file and extract products.

        Returns:
            List of dictionaries with product data
        """

    def _find_column_by_pattern(self, df: pd.DataFrame, pattern: str) -> str | None:
        """
        Find the first column that matches the given pattern.
        """
        matching_cols = [col for col in df.columns if pattern in str(col)]
        if matching_cols:
            return matching_cols[0]
        return None

    def _get_column_value(self, row: pd.Series, column_name: str | None) -> str:
        """
        Get column value from row, handling NaN and empty values.
        """
        if not column_name or column_name not in row.index:
            return ''

        value = row[column_name]
        if pd.isna(value):
            return ''

        return str(value).strip()


class ValvolineXlsxParser(BaseXlsxParser):
    NAME_COLUMN = 'Наименование'
    PACKAGE_COLUMN = 'Упаковка'
    PRICE_COLUMN = 'Окончательная цена с НДС за 1л'

    def _parse_xlsx(self) -> list[dict[str, Any]]:
        df = pd.read_excel(self.file_path, skiprows=2, header=0)

        name_column = self._find_column_by_pattern(df, self.NAME_COLUMN)
        package_column = self._find_column_by_pattern(df, self.PACKAGE_COLUMN)
        price_column = self._find_column_by_pattern(df, self.PRICE_COLUMN)

        if not name_column:
            logging.warning('Name column not found in Valvoline file')
            return []

        products = []

        for _, row in df.iterrows():
            name = self._get_column_value(row, name_column)
            if not name or name.lower() == 'nan':
                continue

            package = self._get_column_value(row, package_column)
            price = self._get_column_value(row, price_column)

            normalized_name = self._normalize_name(name)
            package_count, volume, volume_unit = self._parse_package_info(package)

            product = {
                'original_name': name,
                'normalized_name': normalized_name,
                'volume': volume,
                'volume_unit': volume_unit,
                'package_count': package_count,
                'price': price,
            }
            products.append(product)

        return products

    def _normalize_name(self, name: str) -> str:
        name = normalize_product_name(name)

        if not name:
            return 'valvoline'

        name = self._replace_volume_unit(name)

        if name.startswith('valvoline'):
            return name
        elif name.startswith('val'):
            return 'valvoline' + name[3:]

        return 'valvoline ' + name

    def _replace_volume_unit(self, name: str) -> str:
        """
        Replace volume unit patterns like "4/5 l" with "5 l".

        Examples:
        - "valvoline hybrid vehicle c5 0w20 4/5 l nsw" -> "valvoline hybrid vehicle c5 0w20 5 l nsw"
        - "12/1 l" -> "1 l"
        - "500/1 ml" -> "1 ml"

        Args:
            name: Product name to clean

        Returns:
            Cleaned product name
        """

        def replace_match(match: re.Match) -> str:
            second_num = match.group(2)
            unit = match.group(3)

            return f'{second_num} {unit}'

        if not name:
            return name

        pattern = r'(\d+)\s*/\s*(\d+)\s*(l|ml|kg|g)\b'

        cleaned_name = re.sub(pattern, replace_match, name, flags=re.IGNORECASE)

        return cleaned_name

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

        package_str = str(package_str).strip().lower()

        multi_pattern = r'(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(l|ml|kg)'
        multi_match = re.search(multi_pattern, package_str, re.IGNORECASE)
        if multi_match:
            count = int(multi_match.group(1))
            volume = multi_match.group(2)
            unit = multi_match.group(3)
            return count, volume, unit

        single_pattern = r'(\d+(?:\.\d+)?)\s*(l|ml|kg)'
        single_match = re.search(single_pattern, package_str, re.IGNORECASE)
        if single_match:
            volume = single_match.group(1)
            unit = single_match.group(2)
            return 1, volume, unit

        return 1, '', ''


class ForsageXlsxParser(BaseXlsxParser):
    NAME_COLUMN = 'Forsage'
    PACKAGE_COLUMN = 'Фасовка'
    PRICE_COLUMN = 'Себестоимость с НДС'

    def _parse_xlsx(self) -> list[dict[str, Any]]:
        columns_to_read = [self.NAME_COLUMN, self.PACKAGE_COLUMN, self.PRICE_COLUMN]
        df = pd.read_excel(self.file_path, usecols=columns_to_read)

        products = []
        current_product_name = None

        for _, row in df.iterrows():
            product_name = self._get_column_value(row, self.NAME_COLUMN)
            if product_name:
                current_product_name = product_name
                logging.debug(f'found new product: "{current_product_name}"')

            if current_product_name:
                package_str = self._get_column_value(row, self.PACKAGE_COLUMN)
                price_str = self._get_column_value(row, self.PRICE_COLUMN)

                if package_str and price_str:
                    package_count, volume, volume_unit = self._parse_package_info(package_str)

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

                    logging.debug(f'added product: {package_str} - {price_str}')

        logging.info(f'total products found: {len(products)}')

        return products

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


class RosneftXlsxParser(BaseXlsxParser):
    NAME_COLUMN = 'Наименование'
    PACKAGE_COLUMN = 'Упаковка'

    SHEET_NAME = 'РНПК'

    def _parse_xlsx(self) -> list[dict[str, Any]]:
        logging.info(f'reading sheet: {self.SHEET_NAME}')

        df = pd.read_excel(self.file_path, sheet_name=self.SHEET_NAME, skiprows=9, header=0)

        name_column = self._find_column_by_pattern(df, self.NAME_COLUMN)
        package_column = self._find_column_by_pattern(df, self.PACKAGE_COLUMN)
        price_column = df.columns[8]

        if not name_column:
            logging.warning('Name column not found in Rosneft file')
            return []

        products = []
        current_product_name = None

        for _, row in df.iterrows():
            name_val = self._get_column_value(row, name_column)
            if name_val:
                current_product_name = name_val
                logging.debug(f'Found new product: "{current_product_name}"')

            package_val = self._get_column_value(row, package_column)
            price_val = self._get_column_value(row, price_column)

            if not package_val and not price_val:
                continue

            if current_product_name and package_val and price_val:
                package_count, volume, volume_unit = self._parse_package_info(package_val)

                normalized_name = normalize_product_name(current_product_name)
                product = {
                    'original_name': current_product_name,
                    'normalized_name': normalized_name,
                    'volume': volume,
                    'volume_unit': volume_unit,
                    'package_count': package_count,
                    'price': price_val,
                    'package': package_val,
                }
                products.append(product)

                logging.debug(f'added product: {package_val} - {price_val}')

        logging.info(f'total products found: {len(products)}')

        return products

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
