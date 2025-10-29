import logging
from abc import ABC, abstractmethod
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from constants import VolumeUnit
from csv_reader import (
    filter_forsage_products,
    filter_rosneft_products,
    filter_valvoline_products,
)
from Levenshtein import distance
from writer import CSVWriter
from xlsx_parsers import (
    ForsageXlsxParser,
    RosneftXlsxParser,
    ValvolineXlsxParser,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler())
logger.propagate = False


OUTPUT_COLUMNS = [
    'name',
    'price',
    'csv_name',
    'xlsx_name',
    'distance',
    'csv_volume',
    'csv_volume_unit',
    'xlsx_volume',
    'xlsx_volume_unit',
    'xlsx_price',
]


class ProductFilter(Protocol):
    """Protocol for filtering products from CSV files."""

    def __call__(self, csv_file: str, encoding: str = 'utf-8') -> list[dict[str, Any]]:
        """Filter products from CSV file."""
        ...


class XlsxParser(Protocol):
    """Protocol for parsing XLSX files."""

    def parse_xlsx(self) -> list[dict[str, Any]]:
        """Parse XLSX file and return products."""
        ...


class ReportGenerator(ABC):
    """Abstract base class for generating product matching reports."""

    def __init__(self, csv_encoding: str = 'utf-8') -> None:
        self.csv_encoding = csv_encoding

    @abstractmethod
    def get_csv_filter(self) -> ProductFilter:
        """Get the CSV product filter for this brand."""
        pass

    @abstractmethod
    def get_xlsx_parser(self, xlsx_file: str) -> XlsxParser:
        """Get the XLSX parser for this brand."""
        pass

    @abstractmethod
    def get_fieldnames(self) -> list[str]:
        """Get the fieldnames for CSV output."""
        pass

    def generate_report(
        self,
        csv_file: str,
        xlsx_file: str,
        output_file: str = None,
        max_distance: int = 3,
    ) -> None:
        """
        Generate matching report for products.

        Args:
            csv_file: Path to CSV file with products
            xlsx_file: Path to XLSX file with products
            output_file: Path to output CSV file for matched results (optional)
            max_distance: Maximum Levenshtein distance for a match
        """
        # Get CSV products
        csv_filter = self.get_csv_filter()
        csv_products = csv_filter(csv_file, encoding=self.csv_encoding)

        # Get XLSX products
        xlsx_parser = self.get_xlsx_parser(xlsx_file)
        xlsx_products = xlsx_parser.parse_xlsx()

        # Match products
        matched_results = self._match_products(csv_products, xlsx_products, max_distance)

        # Process results if needed
        processed_results = self.process_results(matched_results)

        # Write results if output file specified
        if output_file and processed_results:
            fieldnames = self.get_fieldnames()
            writer = CSVWriter(fieldnames)
            writer.write(output_file, processed_results)

    def _match_products(
        self,
        csv_products: list[dict[str, Any]],
        xlsx_products: list[dict[str, Any]],
        max_distance: int,
    ) -> list[dict[str, Any]]:
        """
        Internal function to match products between CSV and XLSX data.

        Args:
            csv_products: List of products from CSV
            xlsx_products: List of products from XLSX
            max_distance: Maximum Levenshtein distance for a match

        Returns:
            List of matched products with comparison data
        """
        matches_found = 0
        matched_results = []

        for csv_product in csv_products:
            csv_name = csv_product.get('normalized_name', '').strip()
            if not csv_name:
                continue

            best_match = None
            best_distance = float('inf')
            best_match_product = None

            for xlsx_product in xlsx_products:
                xlsx_name = xlsx_product.get('normalized_name', '').strip()
                if not xlsx_name:
                    continue

                if csv_product.get('volume', '') != xlsx_product.get('volume', ''):
                    logger.debug(
                        f'volume mismatch: "{csv_product.get("volume", "")}",'
                        f'"{xlsx_product.get("volume", "")}"'
                    )
                    continue
                if csv_product.get('volume_unit', '') != xlsx_product.get('volume_unit', ''):
                    logger.debug(
                        f'volume unit mismatch: "{csv_product.get("volume_unit", "")}",'
                        f'"{xlsx_product.get("volume_unit", "")}"'
                    )
                    continue

                dist = distance(
                    ' '.join(sorted(csv_name.split())), ' '.join(sorted(xlsx_name.split()))
                )
                if dist < best_distance:
                    best_distance = dist
                    best_match = xlsx_name
                    best_match_product = xlsx_product

            if best_match and best_distance <= max_distance:
                matches_found += 1
                logger.info(
                    f'match found: "{csv_name}", (distance: {best_distance})\n'
                    f'volume: "{best_match_product.get("volume", "")}")\n'
                    f'volume unit: "{best_match_product.get("volume_unit", "")}")\n'
                    f'price: "{best_match_product.get("price", "")}")\n'
                    f'package count: "{best_match_product.get("package_count", "")}")\n\n'
                )

                price = best_match_product.get('price', '')
                price = Decimal(price) if price else Decimal('0')

                matched_results.append(
                    {
                        'name': csv_product.get('original_name', ''),
                        'price': f'{price:.2f}',
                        'csv_name': csv_name,
                        'xlsx_name': best_match,
                        'distance': best_distance,
                        'csv_volume': csv_product.get('volume', ''),
                        'csv_volume_unit': csv_product.get('volume_unit', ''),
                        'xlsx_volume': best_match_product.get('volume', ''),
                        'xlsx_volume_unit': best_match_product.get('volume_unit', ''),
                        'xlsx_price': f'{price:.2f}',
                    }
                )
            else:
                logger.debug(f'no match found for "{csv_name}"')

        logger.debug(f'Matches found: {matches_found}')

        return matched_results

    def process_results(self, matched_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Process matched results before writing to CSV.
        Override in subclasses for custom processing.

        Args:
            matched_results: List of matched products

        Returns:
            Processed results ready for CSV output
        """
        return matched_results


class ValvolineReportGenerator(ReportGenerator):
    """Report generator for Valvoline products."""

    def get_csv_filter(self) -> ProductFilter:
        return filter_valvoline_products

    def get_xlsx_parser(self, xlsx_file: str) -> XlsxParser:
        return ValvolineXlsxParser(file_path=xlsx_file)

    def get_fieldnames(self) -> list[str]:
        return OUTPUT_COLUMNS

    def process_results(self, matched_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process Valvoline-specific data with price calculations."""
        return self._calculate_price(matched_results)

    def _calculate_price(self, matched_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Calculate total price based on volume and unit conversion.

        All prices are per 1 liter. If volume_unit is ml, convert to liters.

        Args:
            matched_results: List of matched products

        Returns:
            Processed data with calculated total prices
        """
        processed_data = []

        for item in matched_results:
            xlsx_price_str = item.get('price', '')
            xlsx_volume = item.get('xlsx_volume', '')
            xlsx_volume_unit = VolumeUnit(item.get('xlsx_volume_unit', VolumeUnit.L))

            try:
                xlsx_price = Decimal(str(xlsx_price_str)) if xlsx_price_str else Decimal('0')
                xlsx_volume_decimal = Decimal(xlsx_volume) if xlsx_volume else Decimal('0')

                # Convert volume to liters for price calculation
                volume_in_liters = self._convert_volume_to_liters(
                    xlsx_volume_decimal, xlsx_volume_unit
                )
                xlsx_price_total = xlsx_price * volume_in_liters
            except (InvalidOperation, ValueError):
                xlsx_price_total = Decimal('0')

            processed_item = {
                **item,
                'price': f'{xlsx_price_total:.2f}',
            }
            processed_data.append(processed_item)

        return processed_data

    def _convert_volume_to_liters(self, volume: Decimal, unit: VolumeUnit) -> Decimal:
        """
        Convert volume to liters based on unit.

        Args:
            volume: Volume value
            unit: VolumeUnit

        Returns:
            Volume in liters
        """
        if unit == VolumeUnit.ML:
            return volume / Decimal('1000')

        return volume


class RosneftReportGenerator(ReportGenerator):
    """Report generator for Rosneft products."""

    def get_csv_filter(self) -> ProductFilter:
        return filter_rosneft_products

    def get_xlsx_parser(self, xlsx_file: str) -> XlsxParser:
        return RosneftXlsxParser(file_path=xlsx_file)

    def get_fieldnames(self) -> list[str]:
        return OUTPUT_COLUMNS


class ForsageReportGenerator(ReportGenerator):
    """Report generator for Forsage products."""

    def get_csv_filter(self) -> ProductFilter:
        return filter_forsage_products

    def get_xlsx_parser(self, xlsx_file: str) -> XlsxParser:
        return ForsageXlsxParser(file_path=xlsx_file)

    def get_fieldnames(self) -> list[str]:
        return OUTPUT_COLUMNS


def match_valvoline_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'utf-8',
) -> None:
    """Match Valvoline products from CSV file with XLSX file using Levenshtein distance."""
    generator = ValvolineReportGenerator(csv_encoding=encoding)
    generator.generate_report(csv_file, xlsx_file, output_file, max_distance)


def match_rosneft_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """Match Rosneft products from CSV file with XLSX file using Levenshtein distance."""
    generator = RosneftReportGenerator(csv_encoding=encoding)
    generator.generate_report(csv_file, xlsx_file, output_file, max_distance)


def match_forsage_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """Match Forsage products from CSV file with XLSX file using Levenshtein distance."""
    generator = ForsageReportGenerator(csv_encoding=encoding)
    generator.generate_report(csv_file, xlsx_file, output_file, max_distance)


if __name__ == '__main__':
    match_valvoline_products(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Прайс ВАЛСАР с 01.09.2025_new.xlsx',
        output_file='data/valvoline_products_matched.csv',
        max_distance=3,
        encoding='cp1251',
    )

    match_forsage_products(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Прайс себестоимость от 01.10.2025г..xlsx',
        output_file='data/forsage_products_matched.csv',
        max_distance=3,
        encoding='cp1251',
    )

    match_rosneft_products(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Дистрибьюторы_РФ_фасовка_август_2025.xlsm',
        output_file='data/rosneft_products_matched.csv',
        max_distance=3,
        encoding='cp1251',
    )
