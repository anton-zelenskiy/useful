import logging
from decimal import Decimal, InvalidOperation
from typing import Any

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


# TODO: refactoring needed
def match_valvoline_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'utf-8',
) -> None:
    """
    Match Valvoline products from CSV file with XLSX file using Levenshtein distance.

    Args:
        csv_file: Path to CSV file with Valvoline products
        xlsx_file: Path to XLSX file with Valvoline products
        output_file: Path to output CSV file for matched results (optional)
        max_distance: Maximum Levenshtein distance for a match
        encoding: CSV file encoding
    """
    csv_products = filter_valvoline_products(csv_file, encoding=encoding)

    xlsx_reader = ValvolineXlsxParser(file_path=xlsx_file)
    xlsx_products = xlsx_reader.parse_xlsx()

    matched_results = _match_products(csv_products, xlsx_products, max_distance)
    processed_results = _calculate_price(matched_results)

    if output_file and processed_results:
        fieldnames = [
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
        writer = CSVWriter(fieldnames)
        writer.write(output_file, processed_results)


def _calculate_price(matched_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            volume_in_liters = _convert_volume_to_liters(xlsx_volume_decimal, xlsx_volume_unit)
            xlsx_price_total = xlsx_price * volume_in_liters
        except (InvalidOperation, ValueError):
            xlsx_price_total = Decimal('0')

        processed_item = {
            **item,
            'price': f'{xlsx_price_total:.2f}',
        }
        processed_data.append(processed_item)

    return processed_data


def _convert_volume_to_liters(volume: Decimal, unit: VolumeUnit) -> Decimal:
    """
    Convert volume to liters based on unit.

    Args:
        volume: Volume value
        unit: VolumeUNit

    Returns:
        Volume in liters
    """

    if unit == VolumeUnit.ML:
        return volume / Decimal('1000')

    return volume


def match_rosneft_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """
    Match Rosneft products from CSV file with XLSX file using Levenshtein distance.
    """
    csv_products = filter_rosneft_products(csv_file, encoding=encoding)

    xlsx_reader = RosneftXlsxParser(file_path=xlsx_file)
    xlsx_products = xlsx_reader.parse_xlsx()

    matched_results = _match_products(csv_products, xlsx_products, max_distance)

    if output_file and matched_results:
        fieldnames = [
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
        writer = CSVWriter(fieldnames)
        writer.write(output_file, matched_results)


def match_forsage_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """
    Match Forsage products from CSV file with XLSX file using Levenshtein distance.
    """
    csv_products = filter_forsage_products(csv_file, encoding=encoding)

    xlsx_reader = ForsageXlsxParser(file_path=xlsx_file)
    xlsx_products = xlsx_reader.parse_xlsx()

    matched_results = _match_products(csv_products, xlsx_products, max_distance)

    if output_file and matched_results:
        fieldnames = [
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
        writer = CSVWriter(fieldnames)
        writer.write(output_file, matched_results)


def _match_products(
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
                ' '.join(sorted(csv_name.split())),
                ' '.join(sorted(xlsx_name.split()))
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
