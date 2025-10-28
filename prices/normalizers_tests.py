import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from normalizers import (
    normalize_product_name,
    normalize_viscosity_grades,
    parse_volume_from_string,
    remove_duplicate_words,
    remove_russian_characters,
)


def test_remove_duplicate_words_preserves_order():
    assert remove_duplicate_words('valvoline valvoline 5w40 5w40 oil') == 'valvoline 5w40 oil'


def test_remove_duplicate_words_empty():
    assert remove_duplicate_words('') == ''


def test_normalize_viscosity_grades_removes_hyphen():
    assert normalize_viscosity_grades('5W-40') == '5W40'
    assert normalize_viscosity_grades('75W-80 GL-4') == '75W80 GL-4'


def test_normalize_viscosity_grades_empty():
    assert normalize_viscosity_grades('') == ''


@pytest.mark.parametrize(
    'text,expected_name,expected_num,expected_unit',
    [
        ('Масло моторное 4 л', 'Масло моторное', '4', 'l'),
        ('Антифриз 500мл', 'Антифриз', '500', 'ml'),
        ('Масло трансмиссионное 4л.', 'Масло трансмиссионное', '4', 'l'),
        ('Смазка 175 кг', 'Смазка', '175', 'kg'),
        ('Без объема', 'Без объема', '', ''),
    ],
)
def test_parse_volume_from_string(text, expected_name, expected_num, expected_unit):
    name, num, unit = parse_volume_from_string(text)
    assert name == expected_name
    assert num == expected_num
    assert unit == expected_unit


def test_remove_russian_characters():
    assert remove_russian_characters('масло OIL 5W40') == 'OIL 5W40'


def test_normalize_product_name_basic_rules():
    assert normalize_product_name('Valvoline Oil 866904') == 'valvoline oil'
    assert normalize_product_name(' (Valvoline, Oil and Grease) ') == 'valvoline oil & grease'
    assert normalize_product_name('Valvoline  ,  Valvoline.. Oil') == 'valvoline oil'


