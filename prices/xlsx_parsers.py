import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
from constants import VOLUME_MAP
from csv_reader import normalize_product_name


class BaseXlsxParser(ABC):
    """Base class for XLSX file parsers."""

    def __init__(self, file_path: str) -> None:
        """
        Initialize reader with XLSX file path.

        Args:
            file_path: Path to XLSX file
            normalizer: Normalizer instance for this reader
        """
        self.file_path = Path(file_path)

    @abstractmethod
    def parse_xlsx(self) -> list[dict[str, Any]]:
        """
        Parse XLSX file and extract products.

        Returns:
            List of dictionaries with product data
        """
        pass


class ValvolineXlsxParser(BaseXlsxParser):
    def parse_xlsx(self) -> list[dict[str, Any]]:
        try:
            df = pd.read_excel(self.file_path, skiprows=2, header=0)

            products = []
            total_rows = 0

            for idx, row in df.iterrows():
                total_rows += 1

                name_cols = [col for col in df.columns if 'Наименование' in str(col)]
                if not name_cols:
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

                normalized_name = self._normalize_name(name)
                package_count, volume, volume_unit = self._parse_package_info(package)

                price = (
                    str(row[price_cols[0]]).strip()
                    if price_cols and not pd.isna(row[price_cols[0]])
                    else ''
                )

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

        except Exception as e:
            logging.error(f'Error reading file {self.file_path.name}: {e}')
            return []

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
    def parse_xlsx(self) -> list[dict[str, Any]]:
        try:
            columns_to_read = ['Forsage', 'Фасовка', 'Себестоимость с НДС']
            df = pd.read_excel(self.file_path, usecols=columns_to_read)

            products = []
            current_product_name = None

            for idx, row in df.iterrows():
                product_name = row['Forsage']
                if not pd.isna(product_name) and str(product_name).strip():
                    current_product_name = str(product_name).strip()
                    logging.debug(f'found new product: "{current_product_name}"')

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

        except Exception as e:
            logging.error(f'Error reading file {self.file_path.name}: {e}')
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


class RosneftXlsxParser(BaseXlsxParser):
    def parse_xlsx(self) -> list[dict[str, Any]]:
        try:
            sheet_name = 'РНПК'
            logging.info(f'reading sheet: {sheet_name}')

            df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=9, header=0)

            name_column = 'Наименование'
            price_column = df.columns[8]
            package_column = 'Упаковка'

            products = []
            current_product_name = None

            for idx, row in df.iterrows():
                name_val = row[name_column]
                if not pd.isna(name_val) and str(name_val).strip():
                    current_product_name = str(name_val).strip()
                    logging.debug(f'Found new product: "{current_product_name}"')

                package_val = row[package_column]
                price_val = row[price_column]

                if pd.isna(package_val) and pd.isna(price_val):
                    continue

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

                        package_count, volume, volume_unit = self._parse_package_info(package_str)

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

                        logging.debug(f'added product: {package_str} - {price_str}')

            logging.info(f'total products found: {len(products)}')

            return products

        except Exception as e:
            logging.error(f'Error reading Rosneft file {self.file_path.name}: {e}')
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
