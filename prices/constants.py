import enum


class VolumeUnit(enum.StrEnum):
    L = 'l'
    ML = 'ml'
    KG = 'kg'
    G = 'g'


VOLUME_MAP = {
    'л': 'l',
    'мл': 'ml',
    'кг': 'kg',
    'г': 'g',
}
