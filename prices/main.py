from parser import filter_valvoline_products, normalize_product_name, parse_volume_from_string
from pathlib import Path

if __name__ == '__main__':
    # Process the Ozon products file
    input_file = 'data/ms_ozon_product_202510261943.csv'
    output_file = 'data/valvoline_products.csv'

    try:
        filter_valvoline_products(input_file, output_file)
    except Exception as e:
        print(f'Error: {e}')


    # Test volume parsing examples
    print('\n=== Testing Volume Parsing ===')
    test_names = [
        'Масло Valvoline DCT (1л)',
        'Valvoline valvoline 0W-30 Масло моторное, Синтетическое, 4 л',
        'Трансмиссионное масло VALVOLINE AXLE OIL 75W-90 LS кан. 1 л., 866904',
        'Масло трансмиссионное Valvoline LIGHT and HD ATF/CVT (1 л)',
        'Valvoline 5W-40 Масло моторное, Полусинтетическое, 60 л',
        'Защита контактов и электрики Valvoline Electro Protect, 500мл',
        'Valvoline Motor Oil 4л.',
        'Valvoline ATF 1.5л',
        'Valvoline Gear Oil (750мл)',
        'Valvoline Synthetic 5.5 л',
        'Valvoline Valvoline Motor Oil Synthetic Synthetic',  # Test duplicate words
        'Valvoline 5W-40 Motor Oil 75W-80 Gear Oil',  # Test viscosity grades
    ]

    for original in test_names:
        cleaned_name, volume_number, volume_unit = parse_volume_from_string(original)
        normalized_name, vol_num, vol_unit = normalize_product_name(original)
        print(f'Original:  {original}')
        print(f'Cleaned:   {cleaned_name}')
        print(f'Volume:    {volume_number} {volume_unit}')
        print(f'Normalized: {normalized_name}')
        print(f'Final Volume: {vol_num} {vol_unit}')
        print()
