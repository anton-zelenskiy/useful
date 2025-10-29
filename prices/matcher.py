import logging
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from constants import VolumeUnit
from csv_reader import (
    CSVReader,
    ForsageProductFilter,
    ForsageProductProcessor,
    ProductReader,
    RosneftProductFilter,
    RosneftProductProcessor,
    ValvolineProductFilter,
    ValvolineProductProcessor,
)
from Levenshtein import distance
from writer import CSVWriter
from xlsx_parsers import BaseXlsxParser, ForsageXlsxParser, RosneftXlsxParser, ValvolineXlsxParser

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


class ReportGenerator:
    """Base class for generating product matching reports."""

    def __init__(
        self, product_reader: ProductReader, xlsx_parser_cls: type[BaseXlsxParser]
    ) -> None:
        self.product_reader = product_reader
        self.xlsx_parser_cls = xlsx_parser_cls

    def get_fieldnames(self) -> list[str]:
        return OUTPUT_COLUMNS

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
        csv_products = self.product_reader.read_data(csv_file)

        # Get XLSX products
        xlsx_parser = self.xlsx_parser_cls(file_path=xlsx_file)
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
                price = Decimal(str(price)) if price else Decimal('0')
                price = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                matched_results.append(
                    {
                        'name': csv_product.get('original_name', ''),
                        'price': str(price),
                        'csv_name': csv_name,
                        'xlsx_name': best_match,
                        'distance': best_distance,
                        'csv_volume': csv_product.get('volume', ''),
                        'csv_volume_unit': csv_product.get('volume_unit', ''),
                        'xlsx_volume': best_match_product.get('volume', ''),
                        'xlsx_volume_unit': best_match_product.get('volume_unit', ''),
                        'xlsx_price': str(price),
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
                xlsx_price_total = (xlsx_price * volume_in_liters).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
            except (InvalidOperation, ValueError):
                xlsx_price_total = Decimal('0')

            processed_item = {
                **item,
                'price': str(xlsx_price_total),
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


if __name__ == '__main__':
    csv_encoding = 'cp1251'

    reader = CSVReader(encoding=csv_encoding)
    filter_ = ValvolineProductFilter()
    processor = ValvolineProductProcessor()
    product_reader = ProductReader(reader, filter_, processor)
    generator = ValvolineReportGenerator(product_reader, ValvolineXlsxParser)
    generator.generate_report(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Прайс ВАЛСАР с 01.09.2025_new.xlsx',
        output_file='data/valvoline_products_matched.csv',
        max_distance=3,
    )

    reader = CSVReader(encoding=csv_encoding)
    filter_ = ForsageProductFilter()
    processor = ForsageProductProcessor()
    product_reader = ProductReader(reader, filter_, processor)
    generator = ReportGenerator(product_reader, ForsageXlsxParser)
    generator.generate_report(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Прайс себестоимость от 01.10.2025г..xlsx',
        output_file='data/forsage_products_matched.csv',
        max_distance=3,
    )

    reader = CSVReader(encoding=csv_encoding)
    filter_ = RosneftProductFilter()
    processor = RosneftProductProcessor()
    product_reader = ProductReader(reader, filter_, processor)
    generator = ReportGenerator(product_reader, RosneftXlsxParser)
    generator.generate_report(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Дистрибьюторы_РФ_фасовка_август_2025.xlsm',
        output_file='data/rosneft_products_matched.csv',
        max_distance=3,
    )
