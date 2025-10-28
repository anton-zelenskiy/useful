from decimal import Decimal, InvalidOperation
from typing import Any

from csv_reader import (
    filter_forsage_products,
    filter_rosneft_products,
    filter_valvoline_products,
)
from Levenshtein import distance
from writer import CSVWriter
from xlsx_reader import (
    ForsageNormalizer,
    ForsageXlsxReader,
    RosneftNormalizer,
    RosneftXlsxReader,
    ValvolineNormalizer,
    ValvolineXlsxReader,
)


def match_valvoline_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
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
    print(f'\n=== MATCHING VALVOLINE PRODUCTS ===')
    print(f'CSV file: {csv_file}')
    print(f'XLSX file: {xlsx_file}')
    print(f'Max distance: {max_distance}')

    # Read CSV products
    csv_products = filter_valvoline_products(csv_file, encoding=encoding)
    print(f'Found {len(csv_products)} Valvoline products in CSV')

    # Read XLSX products
    valvoline_normalizer = ValvolineNormalizer()
    xlsx_reader = ValvolineXlsxReader(file_path=xlsx_file, normalizer=valvoline_normalizer)
    xlsx_products = xlsx_reader.parse_xlsx()
    print(f'Found {len(xlsx_products)} Valvoline products in XLSX')

    # Match products
    matched_results = _match_products(csv_products, xlsx_products, max_distance, 'Valvoline')

    # Process Valvoline-specific data for CSV output
    if output_file and matched_results:
        processed_results = _process_valvoline_data(matched_results)
        fieldnames = [
            'csv_name',
            'xlsx_name',
            'distance',
            'csv_price',
            'xlsx_price',
            'csv_volume',
            'csv_volume_unit',
            'xlsx_volume',
            'xlsx_volume_unit',
            'csv_price_total',
            'xlsx_price_total',
        ]
        writer = CSVWriter(fieldnames)
        writer.write(output_file, processed_results)
        print(f'Matched results saved to {output_file}')


def _process_valvoline_data(matched_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Process Valvoline-specific data for CSV output.

    Args:
        matched_results: List of matched products

    Returns:
        Processed data with Valvoline-specific calculations
    """
    processed_data = []

    for item in matched_results:
        # Process CSV data
        csv_price_str = item.get('csv_price', '')
        csv_volume = item.get('csv_volume', '')
        csv_volume_unit = item.get('csv_volume_unit', '')

        try:
            csv_price = Decimal(str(csv_price_str)) if csv_price_str else Decimal('0')
            csv_volume_decimal = Decimal(csv_volume) if csv_volume else Decimal('0')
            csv_price_total = csv_price * csv_volume_decimal
        except (InvalidOperation, ValueError):
            csv_price = Decimal('0')
            csv_price_total = Decimal('0')

        # Process XLSX data
        xlsx_price_str = item.get('xlsx_price', '')
        xlsx_volume = item.get('xlsx_volume', '')
        xlsx_volume_unit = item.get('xlsx_volume_unit', '')

        try:
            xlsx_price = Decimal(str(xlsx_price_str)) if xlsx_price_str else Decimal('0')
            xlsx_volume_decimal = Decimal(xlsx_volume) if xlsx_volume else Decimal('0')
            xlsx_price_total = xlsx_price * xlsx_volume_decimal
        except (InvalidOperation, ValueError):
            xlsx_price = Decimal('0')
            xlsx_price_total = Decimal('0')

        processed_item = {
            'csv_name': item.get('csv_name', ''),
            'xlsx_name': item.get('xlsx_name', ''),
            'distance': item.get('distance', ''),
            'csv_price': f'{csv_price:.2f}',
            'xlsx_price': f'{xlsx_price:.2f}',
            'csv_volume': csv_volume,
            'csv_volume_unit': csv_volume_unit,
            'xlsx_volume': xlsx_volume,
            'xlsx_volume_unit': xlsx_volume_unit,
            'csv_price_total': f'{csv_price_total:.2f}',
            'xlsx_price_total': f'{xlsx_price_total:.2f}',
        }
        processed_data.append(processed_item)

    return processed_data


def match_rosneft_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """
    Match Rosneft products from CSV file with XLSX file using Levenshtein distance.

    Args:
        csv_file: Path to CSV file with Rosneft products
        xlsx_file: Path to XLSX file with Rosneft products
        output_file: Path to output CSV file for matched results (optional)
        max_distance: Maximum Levenshtein distance for a match
        encoding: CSV file encoding
    """
    print(f'\n=== MATCHING ROSNEFT PRODUCTS ===')
    print(f'CSV file: {csv_file}')
    print(f'XLSX file: {xlsx_file}')
    print(f'Max distance: {max_distance}')

    # Read CSV products
    csv_products = filter_rosneft_products(csv_file, encoding=encoding)
    print(f'Found {len(csv_products)} Rosneft products in CSV')

    # Read XLSX products
    rosneft_normalizer = RosneftNormalizer()
    xlsx_reader = RosneftXlsxReader(file_path=xlsx_file, normalizer=rosneft_normalizer)
    xlsx_products = xlsx_reader.parse_xlsx()
    print(f'Found {len(xlsx_products)} Rosneft products in XLSX')

    # Match products
    matched_results = _match_products(csv_products, xlsx_products, max_distance, 'Rosneft')

    # Write results to CSV if output file specified
    if output_file and matched_results:
        fieldnames = [
            'csv_name',
            'xlsx_name',
            'distance',
            'csv_price',
            'xlsx_price',
            'csv_volume',
            'csv_volume_unit',
            'xlsx_volume',
            'xlsx_volume_unit',
        ]
        writer = CSVWriter(fieldnames)
        writer.write(output_file, matched_results)
        print(f'Matched results saved to {output_file}')


def match_forsage_products(
    csv_file: str,
    xlsx_file: str,
    output_file: str = None,
    max_distance: int = 3,
    encoding: str = 'cp1251',
) -> None:
    """
    Match Forsage products from CSV file with XLSX file using Levenshtein distance.

    Args:
        csv_file: Path to CSV file with Forsage products
        xlsx_file: Path to XLSX file with Forsage products
        output_file: Path to output CSV file for matched results (optional)
        max_distance: Maximum Levenshtein distance for a match
        encoding: CSV file encoding
    """
    print(f'\n=== MATCHING FORSAGE PRODUCTS ===')
    print(f'CSV file: {csv_file}')
    print(f'XLSX file: {xlsx_file}')
    print(f'Max distance: {max_distance}')

    # Read CSV products
    csv_products = filter_forsage_products(csv_file, encoding=encoding)
    print(f'Found {len(csv_products)} Forsage products in CSV')

    # Read XLSX products
    forsage_normalizer = ForsageNormalizer()
    xlsx_reader = ForsageXlsxReader(file_path=xlsx_file, normalizer=forsage_normalizer)
    xlsx_products = xlsx_reader.parse_xlsx()
    print(f'Found {len(xlsx_products)} Forsage products in XLSX')

    # Match products
    matched_results = _match_products(csv_products, xlsx_products, max_distance, 'Forsage')

    # Write results to CSV if output file specified
    if output_file and matched_results:
        fieldnames = [
            'csv_name',
            'xlsx_name',
            'distance',
            'csv_price',
            'xlsx_price',
            'csv_volume',
            'csv_volume_unit',
            'xlsx_volume',
            'xlsx_volume_unit',
        ]
        writer = CSVWriter(fieldnames)
        writer.write(output_file, matched_results)
        print(f'Matched results saved to {output_file}')


def _match_products(
    csv_products: list[dict[str, Any]],
    xlsx_products: list[dict[str, Any]],
    max_distance: int,
    brand_name: str,
) -> list[dict[str, Any]]:
    """
    Internal function to match products between CSV and XLSX data.

    Args:
        csv_products: List of products from CSV
        xlsx_products: List of products from XLSX
        max_distance: Maximum Levenshtein distance for a match
        brand_name: Brand name for display purposes

    Returns:
        List of matched products with comparison data
    """
    matches_found = 0
    matched_results = []
    print(f'\n=== MATCHING RESULTS FOR {brand_name} ===')

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

            dist = distance(csv_name.lower(), xlsx_name.lower())
            if dist < best_distance:
                best_distance = dist
                best_match = xlsx_name
                best_match_product = xlsx_product

        if best_match and best_distance <= max_distance:
            matches_found += 1
            print(f'MATCH (distance: {best_distance})')
            print(f'CSV: "{csv_name}"')
            print(f'XLSX: "{best_match}"')
            print(f'CSV Price: {csv_product.get("price", "N/A")}')
            print(f'XLSX Price: {best_match_product.get("price", "N/A")}')

            # Add to results for CSV output
            matched_results.append(
                {
                    'csv_name': csv_name,
                    'xlsx_name': best_match,
                    'distance': best_distance,
                    'csv_price': csv_product.get('price', ''),
                    'xlsx_price': best_match_product.get('price', ''),
                    'csv_volume': csv_product.get('volume', ''),
                    'csv_volume_unit': csv_product.get('volume_unit', ''),
                    'xlsx_volume': best_match_product.get('volume', ''),
                    'xlsx_volume_unit': best_match_product.get('volume_unit', ''),
                }
            )
        else:
            print(f'NO MATCH (best distance: {best_distance})')

    print(f'=== {brand_name} SUMMARY ===')
    print(f'Total products in CSV: {len(csv_products)}')
    print(f'Total products in XLSX: {len(xlsx_products)}')
    print(f'Matches found: {matches_found}')
    if len(csv_products) > 0:
        print(f'Match rate: {matches_found / len(csv_products) * 100:.1f}%')

    return matched_results


if __name__ == '__main__':
    match_valvoline_products(
        csv_file='data/ms_ozon_product_202510261943.csv',
        xlsx_file='data/Прайс ВАЛСАР с 01.09.2025_new.xlsx',
        output_file='data/valvoline_products_matched.csv',
        max_distance=3,
        encoding='cp1251',
    )

    # match_forsage_products(
    #     csv_file='data/ms_ozon_product_202510261943.csv',
    #     xlsx_file='data/Прайс себестоимость от 01.10.2025г..xlsx',
    #     output_file='data/forsage_products_matched.csv',
    #     max_distance=3,
    #     encoding='cp1251',
    # )

    # match_rosneft_products(
    #     csv_file='data/ms_ozon_product_202510261943.csv',
    #     xlsx_file='data/Дистрибьюторы_РФ_фасовка_август_2025.xlsm',
    #     output_file='data/rosneft_products_matched.csv',
    #     max_distance=3,
    #     encoding='cp1251',
    # )
