import json
import logging
import requests
import datetime

def fill_yandex_region_key_yandex_suggest():
    """Заполняет для городов значение региона из экосистемы Яндекса. """
    from project.apps.regions.models import Region
    import time

    cities = Region.objects.get(slug='russia').get_leafnodes()

    result = []
    for city in cities[:10]:
        data = suggest_geo(city.title)
        result.append({
            city.title: data['results']
        })

        # time.sleep(0.5)

    return result


def suggest_geo(city='Пекин'):
    """Парсит region_key с api Яндекса. """
    from urllib.parse import quote
    import json

    region = quote(city)

    url = (
        f'https://suggest-maps.yandex.ru/suggest-geo'
        f'?search_type=tune&'
        f'v=9&'
        f'results=15&'
        f'lang=ru&'
        f'callback=jQuery183024443942322682188_1559192475463'
        f'&part={region}'
    )
    r = requests.get(url=url)
    r.raise_for_status()

    result = r.content.decode('utf-8')

    # Удаляем лишнее
    result = result.replace('jQuery183024443942322682188_1559192475463(', '')
    result = result[:-1]

    return json.loads(result)


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox
    import os
    import csv

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')

    output_fn = '/tmp/{}.csv'.format(file)
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    data = fill_yandex_region_key_topvisor()

    for item in data:

        our_writer.writerow([
            item['id'],
            item['title'],
            item['region_key'],
        ])

    out_f.close()

    f = open(output_fn, 'rb')
    client.files_upload(f.read(), output_fn.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(output_fn)


def fill_yandex_region_key_topvisor():
    """Заполняет для городов значение региона из экосистемы Яндекса. """
    from project.apps.regions.models import Region
    from tqdm import tqdm

    countries = Region.objects.filter(title__in=[
        'Азербайджан',
        'Армения',
        'Беларусь',
        'Грузия',
        'Казахстан',
        'Киргизия',
        'Китай',
        'Россия',
        'Таджикистан',
        'Узбекистан',
        'Украина',
    ])
    cities = []

    for country in countries:
        cities.extend(country.get_leafnodes())

    result = []
    for city in tqdm(cities):
        full_title = city.title.split(' ', 1)

        title = full_title[0]
        try:
            district = full_title[1]
            district = district.replace('(', '').replace(')', '')
        except IndexError:
            district = None
        district_2 = city.parent.title

        data = parse_ya_regions_from_topvisor(title)

        concrete_city = find_city(data, title, district, district_2)

        result.append({
            'id': city.id,
            'title': city.title,
            'region_key': concrete_city['id'] if concrete_city else -1,
        })

    return result


def find_city(data, title, district=None, district_2=None):
    """Ищет нужный город из спарсенных данных, т.к. по названию
    города может найтись несколько городов. """

    if district:
        result = [
            item for item in data
            if item['name_ru'] == title and item['type'] == 'CITY' and district in item['areaName_ru']
        ]
        if result:
            return result[0]

        else:
            result = [
                item for item in data
                if item['name_ru'] == title and item['type'] == 'CITY' and district_2 in item['areaName_ru']
            ]
            if result:
                return result[0]

    else:
        result = [
            item for item in data
            if item['name_ru'] == title and item['type'] == 'CITY'
        ]

        if result:
            return result[0]


def parse_ya_regions_from_topvisor(city='Пушкин'):
    """Парсит region_key с api topvisor. """
    from urllib.parse import quote

    region = quote(city)

    url = (
        f'https://topvisor.com/ajax/get.php?'
        f'ssi=ece4512da9e5ee9411e5de68f6f5bc57&'
        f'module=mod_projects&'
        f'fget=regions&'
        f'getList=name&'
        f'limit=10&'
        f'searcher=0&'
        f'term={region}'
    )
    r = requests.get(
        url=url,
        cookies={'ssi': 'ece4512da9e5ee9411e5de68f6f5bc57'}
    )
    r.raise_for_status()

    result = r.json()

    return result
